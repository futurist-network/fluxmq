---
title: 3-node cluster with TLS
group: howto
order: 1
summary: Bring up a three-node cluster on one machine or in Docker — with replication, mutual TLS between nodes, HTTPS and TLS for clients.
---

## 1. Build the binary {#build}

```bash
cd ui && pnpm install && pnpm build && cd ..
cargo build --release -p fluxmq-server -p fluxmq-client
```

## 2. Issue certificates {#certs}

For development use the built-in command (in production, issue certificates from your own CA):

```bash
./target/release/fluxmq tls gen-dev --out ./certs --nodes n1,n2,n3 --hosts localhost,127.0.0.1
```

| File | Used for |
|---|---|
| `ca.pem` / `ca-key.pem` | development CA: `--tls-client-ca`, `--cluster-tls-ca`, the clients' CA |
| `server.pem` / `server-key.pem` | client and HTTP ports (`--tls-cert` / `--tls-key`) |
| `client.pem` / `client-key.pem` | client certificate for mTLS (CN `client`) |
| `node-<id>.pem` / `node-<id>-key.pem` | a cluster node certificate (CN = node id) |

Keys are written with mode `0600`; existing files are not overwritten without `--force`. `--valid-days N` sets the validity (default 397 days).

## 3. Start three nodes {#start}

One terminal per node. Ports are shifted (17420…) so the cluster doesn't clash with a single broker.

```bash
export FLUXMQ_CLUSTER_SECRET=dev-secret
PEERS=n1@127.0.0.1:17422,n2@127.0.0.1:17432,n3@127.0.0.1:17442
TLS="--tls-cert ./certs/server.pem --tls-key ./certs/server-key.pem --tls-client-ca ./certs/ca.pem --cluster-tls-ca ./certs/ca.pem"

./target/release/fluxmq --node-id n1 --peers $PEERS --data-dir ./data/n1 \
  --client-addr 127.0.0.1:17420 --http-addr 127.0.0.1:17421 \
  --raft-addr 127.0.0.1:17422 --replication-addr 127.0.0.1:17423 \
  --cluster-tls-cert ./certs/node-n1.pem --cluster-tls-key ./certs/node-n1-key.pem $TLS

./target/release/fluxmq --node-id n2 --peers $PEERS --data-dir ./data/n2 \
  --client-addr 127.0.0.1:17430 --http-addr 127.0.0.1:17431 \
  --raft-addr 127.0.0.1:17432 --replication-addr 127.0.0.1:17433 \
  --cluster-tls-cert ./certs/node-n2.pem --cluster-tls-key ./certs/node-n2-key.pem $TLS

./target/release/fluxmq --node-id n3 --peers $PEERS --data-dir ./data/n3 \
  --client-addr 127.0.0.1:17440 --http-addr 127.0.0.1:17441 \
  --raft-addr 127.0.0.1:17442 --replication-addr 127.0.0.1:17443 \
  --cluster-tls-cert ./certs/node-n3.pem --cluster-tls-key ./certs/node-n3-key.pem $TLS
```

Start the nodes in any order; the node with the lowest id initialises the cluster once a majority is up.

- `--node-id` and `--cluster-secret` are required with `--peers`; the secret is the same on every node. Certificates complement it, they don't replace it.
- **Configure every node the same way**: a node without cluster TLS can't join a TLS cluster, and vice versa.
- HTTPS on the HTTP port of a cluster requires cluster TLS — nodes call each other's `/internal/*` through that port.
- On several machines always set `--advertise-host` — the address other nodes and clients use to reach the node.

## 4. Check it {#verify}

```bash
C="--cacert ./certs/ca.pem"
curl -s $C https://127.0.0.1:17421/api/cluster/metadata | jq '.controller, [.nodes[] | {id, status, is_voter}]'
curl -s $C -X POST https://127.0.0.1:17431/api/channels -H 'content-type: application/json' \
  -d '{"name":"orders","mode":"stream","partitions":6}'
curl -s $C https://127.0.0.1:17441/api/cluster/metadata | jq -c '.channels.orders[] | {partition, leader, replicas, isr}'
curl -s $C https://127.0.0.1:17421/api/cluster/status | jq -c '.raft, .under_replicated_partitions, .offline_partitions'
```

Open any node's console — `https://127.0.0.1:17421/nodes`. The browser warns about the untrusted (`gen-dev`) CA — fine for development. You should see three Up nodes, a controller, 0 under-replicated and 0 offline partitions. The [Security](/docs/security) tab shows TLS on all three surfaces.

Demo traffic with the mTLS client certificate:

```bash
./target/release/fluxmq-demo --seeds tls://127.0.0.1:17420,tls://127.0.0.1:17430,tls://127.0.0.1:17440 \
  --tls-ca ./certs/ca.pem --tls-cert ./certs/client.pem --tls-key ./certs/client-key.pem
```

## Alternative: Docker {#docker}

The `cluster` profile does all of this itself: an init container writes `gen-dev` certificates and a random cluster secret into the `certs` volume, then `n1`–`n3` start with RF=3, `--min-isr 2`, client-port TLS, HTTPS and mTLS between nodes.

```bash
dc() { docker compose -f deploy/docker-compose.yml "$@"; }
dc --profile cluster up -d                     # n1 console: https://localhost:37431
dc --profile cluster --profile demo up -d      # + demo traffic
dc --profile cluster cp n1:/certs/ca.pem ./ca.pem && curl --cacert ca.pem https://localhost:37431/api/health
```

`FLUXMQ_CLUSTER_TLS=off` gives a cluster without TLS, `FLUXMQ_CLIENT_MTLS=on` requires mTLS from clients. Node addresses in the metadata (`n1:7420` …) only resolve inside the compose network, so run cluster SDK clients in that network.

## Resilience check {#resilience}

```bash
FLUXMQ_CLIENT_TLS_CA=certs/ca.pem FLUXMQ_CLIENT_TLS_CERT=certs/client.pem \
FLUXMQ_CLIENT_TLS_KEY=certs/client-key.pem RESILIENCE_PARTITIONS=6 \
  cargo run --release -p fluxmq-client --example resilience -- \
  tls://127.0.0.1:17420,tls://127.0.0.1:17430,tls://127.0.0.1:17440 120
# in another terminal: kill -9 <node pid>, start it again with the same command after 15 s
```

The example publishes numbered messages and finally checks that no acknowledged one was lost: `acked but missing 0`. What the console shows during a failure: [Nodes and cluster](/docs/nodes#failure).

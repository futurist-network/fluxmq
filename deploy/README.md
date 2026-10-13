# FluxMQ in Docker

Everything here runs from the repository root. You need Docker with Compose v2.24+
(Docker Desktop 4.x or Engine 25+) and BuildKit (the default).

| File | What it is |
|---|---|
| `../Dockerfile` | Multi-stage image: UI (pnpm + Vite), Rust release build (BuildKit cache mounts), Debian slim runtime |
| `../.dockerignore` | Keeps `target/`, `node_modules`, screenshots, `graphify-out` etc. out of the build context |
| `build.sh` | Builds `fluxmq:<tag>` (and `fluxmq-harness:<tag>`) from the working tree or any git ref |
| `docker-compose.yml` | Profiles `single`, `cluster`, `demo`, `kafka`, `kafka-cluster`, `rabbitmq`, `rabbitmq-cluster`, `bench` |
| `docker-entrypoint.sh` | Wires gen-dev certificates and the cluster secret from env; drops empty `FLUXMQ_*` vars |
| `healthcheck.sh` | `GET /api/health` over HTTPS or HTTP |
| `rabbitmq/` | RabbitMQ cluster config and start script (Erlang cookie from a volume) |
| `install.sh` | Installs or upgrades the latest release as a systemd service (Linux, no Docker) |
| `fluxmq.toml` | Example config file (`fluxmq --config`), installed to `/etc/fluxmq/` |
| `fluxmq.service` | systemd unit: runs as `fluxmq`, reloads TLS on `systemctl reload`, hardened |

## Without Docker (systemd)

```sh
curl -fsSL https://raw.githubusercontent.com/futurist-network/fluxmq/master/deploy/install.sh | sudo sh
```

Downloads `fluxmq-linux-<arch>.tar.gz` from the latest GitHub release (`FLUXMQ_VERSION=v0.11.0`
pins one), checks its SHA-256, installs `/usr/local/bin/fluxmq`, creates the `fluxmq` user,
`/var/lib/fluxmq`, `/etc/fluxmq/fluxmq.toml` and a random `/etc/fluxmq/admin-token`, and starts
the `fluxmq` service. Re-running it upgrades the binary and keeps the config, token and data.
Releases come from `.github/workflows/release.yml` on a `v*` tag (image and tarballs for
amd64 and arm64).

## The image

```sh
deploy/build.sh                 # working tree -> fluxmq:dev
deploy/build.sh --ref HEAD      # last commit (git archive + this packaging) -> fluxmq:dev
docker build -t fluxmq:dev .    # the same as the first line, without the label
```

* Contents: `/usr/local/bin/fluxmq` (broker, `fluxmq tls gen-dev`, `backup`, `restore`), `fluxmq-bench`,
  `fluxmq-demo`. The console is built into `fluxmq` (`FLUXMQ_UI=off` turns it off).
* Runs as the non-root user `fluxmq` (uid/gid 10001). Data volume `/var/lib/fluxmq`
  (`FLUXMQ_DATA_DIR`). Ports 7420 (client), 7421 (HTTP/WS + console), 7422 (Raft), 7423 (replication).
* `HEALTHCHECK` = `/api/health` (HTTPS with `-k` first, then HTTP), every 5 s.
* `tini` is PID 1, so `docker stop` sends SIGTERM to the broker and it shuts down gracefully (in a
  cluster it hands its leaderships over first).
* Size: about 150 MB (Debian bookworm-slim + curl + ca-certificates + tini + the stripped binaries + UI).
* Build: deps and the target dir are BuildKit cache mounts (`fluxmq-cargo-registry`, `fluxmq-target`,
  `fluxmq-pnpm-store`), so after the first build only changed crates recompile (about 2 minutes with
  thin LTO on an M1 Pro). A cold build compiles every dependency (about 6–10 minutes). The image
  build sets `CARGO_PROFILE_RELEASE_DEBUG=0` and strips symbols; `Cargo.toml` is unchanged.
* Build from a commit when the working tree is mid-edit: `--ref <commit>` takes the sources from
  `git archive` and the packaging files from the working tree. Sources are re-touched inside the build,
  so the shared target cache never reuses artifacts from different sources.

Plain `docker run`:

```sh
docker run --rm -p 127.0.0.1:37420:7420 -p 127.0.0.1:37421:7421 -v fluxmq-data:/var/lib/fluxmq fluxmq:dev
open http://localhost:37421
docker run --rm fluxmq:dev fluxmq --help        # any command works
```

## Compose profiles

```sh
dc() { docker compose -f deploy/docker-compose.yml "$@"; }   # bash and zsh
```

| Profile | Command | Open |
|---|---|---|
| `single` | `dc --profile single up -d` | <http://localhost:37421> |
| `single` with TLS | `FLUXMQ_SINGLE_TLS=on dc --profile single up -d` | <https://localhost:37421> |
| `cluster` (3 nodes, full TLS) | `dc --profile cluster up -d` | <https://localhost:37431> (also 37441, 37451) |
| `cluster` without TLS | `FLUXMQ_CLUSTER_TLS=off dc --profile cluster up -d` | <http://localhost:37431> |
| `demo` (load generator) | `dc --profile single --profile demo up -d` or `--profile cluster --profile demo` | |
| `kafka` (KRaft, 1 node) | `dc --profile kafka up -d` | `localhost:39092` |
| `kafka-cluster` (3 nodes, RF=3, min ISR 2) | `dc --profile kafka-cluster up -d` | `localhost:39192/39292/39392` |
| `rabbitmq` (1 node) | `dc --profile rabbitmq up -d` | `amqp://guest:guest@localhost:35672` |
| `rabbitmq-cluster` (3 nodes, quorum queues) | `dc --profile rabbitmq-cluster up -d` | `localhost:35673-35675` |
| `bench` | used by `bench/run.sh` (see `bench/README.md`) | |

Stop and delete everything (containers, networks, **data volumes, certificates**):

```sh
dc --profile '*' down -v
```

`up -d --wait` blocks until the healthchecks pass. Logs: `dc --profile cluster logs -f n1`.

### single

One broker, plaintext by default, data in the `fluxmq-data` volume. The `certs` init service always
runs (it takes a second) so that `FLUXMQ_SINGLE_TLS=on` and the demo can use TLS without another step.

### cluster

* `certs` (init) runs `fluxmq tls gen-dev --out /certs --nodes n1,n2,n3 --hosts n1,n2,n3,fluxmq,localhost,127.0.0.1`
  into the `certs` volume, and writes a random cluster secret to `/certs/cluster-secret`. It only
  generates what is missing, so restarts keep the same CA. Change the SANs with `FLUXMQ_CERT_HOSTS`
  (before the volume exists).
* `n1`, `n2`, `n3` start after `certs` completed, with `--peers n1@n1:7422,n2@n2:7422,n3@n3:7422`,
  `--advertise-host <service name>`, `--replication-factor 3`, `--min-isr 2` (override with
  `FLUXMQ_MIN_ISR`), and full TLS: client port TLS, HTTPS console, node-to-node mTLS (Raft,
  replication, internal HTTP). The entrypoint maps `FLUXMQ_TLS_DIR=/certs` to the `--tls-*` and
  `--cluster-tls-*` flags.
* Client mTLS is optional: `FLUXMQ_CLIENT_MTLS=on` requires a client certificate on the client
  port (`client.pem` from the same volume; the demo then presents it automatically).
* The console is served by every node over HTTPS with the gen-dev CA. Trust it in curl with
  `dc --profile cluster cp n1:/certs/ca.pem ./ca.pem && curl --cacert ca.pem https://localhost:37431/api/health`;
  a browser shows a warning for the self-signed CA (accept it, or import `ca.pem`).
* Clients outside Docker: cluster metadata advertises `n1:7420` etc., which only resolve inside the
  compose network. Run SDK clients in the network (as `demo` and `bench` do), or add `n1 n2 n3` to
  `/etc/hosts` pointing at 127.0.0.1 and use the mapped client ports with a single-seed client.

### demo

`fluxmq-demo` against the cluster when `n1` resolves (cluster profile running), otherwise against
the single node; TLS follows `FLUXMQ_CLUSTER_TLS` / `FLUXMQ_SINGLE_TLS`. `DEMO_SPEED=2` doubles the
rate. Within a minute the console shows the demo's slow consumers, stalls and dead letters.

### kafka / kafka-cluster

Official `apache/kafka` image (default `3.7.0`, override `KAFKA_IMAGE`), KRaft combined
broker+controller nodes, no ZooKeeper. Inside the network: `kafka:9092` or `kafka1..3:9092`; from
the host: the `EXTERNAL` listener on 39092 (single) or 39192/39292/39392. The cluster defaults to
RF=3 and `min.insync.replicas=2`; topic auto-creation is off. Heap 1 GiB (`KAFKA_HEAP_OPTS`).

### rabbitmq / rabbitmq-cluster

Official `rabbitmq` image (default `4.1`, override `RABBITMQ_IMAGE`). The cluster forms through
static peer discovery (`rabbitmq/cluster.conf`); `quorum_queue.initial_cluster_size = 3`. The
Erlang cookie is random per stack, generated by the `rabbitmq-cookie` init service into a volume.
The image allows `guest`/`guest` from the network: development only.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `FLUXMQ_IMAGE` | `fluxmq:dev` | Broker image |
| `FLUXMQ_SINGLE_TLS` | `off` | TLS (client port + HTTPS) on the single node |
| `FLUXMQ_CLUSTER_TLS` | `on` | Full TLS in the cluster (`off` = plaintext everywhere) |
| `FLUXMQ_CLIENT_MTLS` | `off` | Require client certificates on the client port |
| `FLUXMQ_MIN_ISR` / `FLUXMQ_REPLICATION_FACTOR` | `2` / `3` | Cluster durability |
| `FLUXMQ_FSYNC` | `group-commit` | `group-commit` or `os` |
| `FLUXMQ_ADMIN_TOKEN` | unset (open dev mode) | Bootstrap admin token (pass from your shell, never commit it) |
| `FLUXMQ_LOG` | `info` | Log filter |
| `BROKER_CPUS` / `BROKER_MEM` | `2` / `2g` | Limits for **every** broker container (FluxMQ, Kafka, RabbitMQ) |
| `BENCH_CPUS` / `BENCH_MEM` | `2` / `2g` | Limits for the demo / bench client containers |
| `FLUXMQ_CLIENT_PORT`, `FLUXMQ_HTTP_PORT` | 37420, 37421 | Single-node host ports |
| `N1_CLIENT_PORT`, `N1_HTTP_PORT` (`N2_`, `N3_`) | 37430/37431, 37440/37441, 37450/37451 | Cluster host ports |
| `KAFKA_PORT`, `KAFKA1_PORT`..`KAFKA3_PORT` | 39092, 39192, 39292, 39392 | Kafka host ports |
| `RABBITMQ_PORT`, `RMQ1_PORT`..`RMQ3_PORT` | 35672, 35673-35675 | RabbitMQ host ports |
| `KAFKA_IMAGE`, `RABBITMQ_IMAGE`, `HARNESS_IMAGE` | see above | Image overrides |

All host ports bind to 127.0.0.1. No secrets live in these files: the cluster secret and the
Erlang cookie are generated per stack into volumes, and the certificates are development ones
(`gen-dev`, the CA key sits next to them) that never leave the `certs` volume. For production,
mount your own PEM files and set `FLUXMQ_TLS_CERT` / `FLUXMQ_CLUSTER_TLS_*` explicitly (they win over
`FLUXMQ_TLS_DIR`), and pass `FLUXMQ_CLUSTER_SECRET` from a secret store.

## Troubleshooting

* `fluxmq-n1-1 is unhealthy`: `dc --profile cluster logs n1`. The usual cause is a stale `certs`
  volume from a different `FLUXMQ_CERT_HOSTS`; `down -v` and start again.
* Port already in use: override the host port variable, e.g. `FLUXMQ_HTTP_PORT=38421`.
* Disk: Kafka/RabbitMQ images are 260–400 MB each, the build cache (cargo registry + target, pnpm
  store) a few GB. `docker buildx du --verbose` lists it; remove only the FluxMQ entries (the
  `fluxmq-*` cache mounts) with `docker buildx prune --filter id=<id>`; the next build is then cold.

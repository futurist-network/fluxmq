---
title: Flags and environment
group: reference
order: 1
summary: Every fluxmq start-up option (from crates/fluxmq-server/src/config.rs) — flag, environment variable, default — plus the backup, restore and tls subcommands.
---

Every option is a flag or an environment variable; the flag wins. An empty variable (`FLUXMQ_TLS_CERT=`) counts as unset. The full list with descriptions: `fluxmq --help`.

## General {#general}

| Flag | Variable | Default | Description |
|---|---|---|---|
| `--data-dir` | `FLUXMQ_DATA_DIR` | `./data` | data directory: messages, offsets, DLQ, audit log |
| `--client-addr` | `FLUXMQ_CLIENT_ADDR` | `0.0.0.0:7420` | client protocol (TCP) listen address |
| `--http-addr` | `FLUXMQ_HTTP_ADDR` | `0.0.0.0:7421` | HTTP listen address: REST, WebSocket telemetry, console |
| `--cluster-name` | `FLUXMQ_CLUSTER_NAME` | `local` | cluster name shown in the console |
| `--ui-dir` | `FLUXMQ_UI_DIR` | `./ui/dist` | directory of the built console |
| `--admin-token` | `FLUXMQ_ADMIN_TOKEN` | — | bootstrap admin token; enforces auth on REST, WebSocket and the client protocol |
| `--log` | `FLUXMQ_LOG` | `RUST_LOG`, then `info` | log filter, e.g. `info,fluxmq_core=debug` |
| `--public-url` | `FLUXMQ_PUBLIC_URL` | — | public console URL (`https://mq.example.com`) for links in notifications |
| `--webhooks-allow-private` | `FLUXMQ_WEBHOOKS_ALLOW_PRIVATE` | unset | `true`/`false`: let webhooks target loopback, private and link-local addresses. Unset — allowed in open mode, denied once auth is enforced |

## Storage and performance {#storage}

| Flag | Variable | Default | Description |
|---|---|---|---|
| `--fsync` | `FLUXMQ_FSYNC` | `group-commit` | `group-commit` — ack after fsync (batched up to 2 ms); `os` — ack after the write to the OS, faster but loses data on power failure |
| `--segment-bytes` | `FLUXMQ_SEGMENT_BYTES` | `67108864` (64 MiB) | segment size, 1 MiB to 2 GiB; each partition rolls within ±10 %; applies to new segments |
| `--fsync-threads` | `FLUXMQ_FSYNC_THREADS` | `0` | size of the shared fsync thread pool; 0 — twice the CPUs, 4 to 32 |
| `--commit-pipeline` | `FLUXMQ_COMMIT_PIPELINE` | `on` | `on` (default since 0.9): write and submit the next batches while earlier ones are fsynced; each partition is acked only after its own fsync, in order. `off` — wait for every batch's fsync (the 0.8 behaviour) |
| `--commit-pipeline-depth` | `FLUXMQ_COMMIT_PIPELINE_DEPTH` | `1` | max group-commit batches in flight per channel when the pipeline is on (1–16) |
| `--partition-cache-bytes` | `FLUXMQ_PARTITION_CACHE_BYTES` | `16777216` (16 MiB) | bytes of recent batches kept in memory per partition, so caught-up consumers read without disk; 0 disables |
| `--delivery-threads` | `FLUXMQ_DELIVERY_THREADS` | `2` | threads of the dedicated delivery runtime (encoding and socket writes), so consumers keep CPU when producers saturate the broker; 0 — shared runtime (0.6 behaviour) |


## Live subscriptions (v0.10) {#live}

| Flag | Variable | Default | Description |
|---|---|---|---|
| `--live-max-connections` | `FLUXMQ_LIVE_MAX_CONNECTIONS` | `10000` | live subscription connections (WebSocket `/live` + SSE `/events`) per node; over → 503 |
| `--live-max-connections-per-token` | `FLUXMQ_LIVE_MAX_CONNECTIONS_PER_TOKEN` | `1000` | live connections per token; over → 429 (bootstrap admin and open mode exempt) |
| `--live-max-connections-per-channel` | `FLUXMQ_LIVE_MAX_CONNECTIONS_PER_CHANNEL` | `10000` | live subscriptions per channel on this node; over → 429 |
| `--live-max-subscriptions` | `FLUXMQ_LIVE_MAX_SUBSCRIPTIONS` | `16` | subscriptions per live WebSocket connection |
| `--live-max-msgs-per-sec` | `FLUXMQ_LIVE_MAX_MSGS_PER_SEC` | `10000` | messages per second per live connection; throttles, never drops; 0 — unlimited |
| `--live-max-bytes-per-sec` | `FLUXMQ_LIVE_MAX_BYTES_PER_SEC` | `16777216` (16 MiB) | bytes per second per live connection; throttles; 0 — unlimited |
| `--live-max-payload-bytes` | `FLUXMQ_LIVE_MAX_PAYLOAD_BYTES` | `65536` | larger payloads are truncated in live events (`truncated: true`, full `size` given) |
| `--live-max-batch` | `FLUXMQ_LIVE_MAX_BATCH` | `256` | messages per live `messages` event |
| `--live-send-buffer` | `FLUXMQ_LIVE_SEND_BUFFER` | `64` | outbound events queued per live connection |
| `--live-send-timeout` | `FLUXMQ_LIVE_SEND_TIMEOUT` | `10` | seconds a live client may not drain its connection before it is closed as slow |
| `--live-heartbeat` | `FLUXMQ_LIVE_HEARTBEAT` | `15` | WebSocket ping / SSE heartbeat interval, seconds |
| `--live-idle-timeout` | `FLUXMQ_LIVE_IDLE_TIMEOUT` | `60` | WebSocket: close after this many seconds without anything received (not even a pong) |
| `--live-fanout-buffer` | `FLUXMQ_LIVE_FANOUT_BUFFER` | `1024` | batches buffered per channel in the shared live fan-out |
| `--live-allowed-origins` | `FLUXMQ_LIVE_ALLOWED_ORIGINS` | — | browser origins allowed besides same-origin, comma-separated (`https://app.example.com`) or `*` |

## Cluster {#cluster}

Without `--peers` the broker is a single node.

| Flag | Variable | Default | Description |
|---|---|---|---|
| `--node-id` | `FLUXMQ_NODE_ID` | — | node id; required with `--peers`, overrides the id persisted in the data dir |
| `--peers` | `FLUXMQ_PEERS` | — | initial voters `id@raft_host:port,…` (may include this node); a node not in the list joins through them as a learner |
| `--raft-addr` | `FLUXMQ_RAFT_ADDR` | `0.0.0.0:7422` | Raft and internal RPC |
| `--replication-addr` | `FLUXMQ_REPLICATION_ADDR` | `0.0.0.0:7423` | partition replication |
| `--advertise-host` | `FLUXMQ_ADVERTISE_HOST` | the listen IP, `127.0.0.1` for `0.0.0.0` | the address other nodes and clients use; always set it on several machines |
| `--replication-factor` | `FLUXMQ_REPLICATION_FACTOR` | `3` | replicas of new channels (capped at the live node count) |
| `--min-isr` | `FLUXMQ_MIN_ISR` | `1` | minimum in-sync replicas to acknowledge a publish, otherwise `Overloaded` |
| `--cluster-secret` | `FLUXMQ_CLUSTER_SECRET` | — | shared secret for node-to-node traffic; required with `--peers`, the same on every node |
| `--auto-rebalance` | `FLUXMQ_AUTO_REBALANCE` | `full` | `off` / `repair` / `full` — the rebalancing mode while the cluster has no stored config; afterwards `PUT /api/cluster/rebalance/config` is the source of truth |

## TLS {#tls}

Without these flags every port is plaintext.

| Flag | Variable | Description |
|---|---|---|
| `--tls-cert` | `FLUXMQ_TLS_CERT` | PEM chain: TLS on the client port and (unless overridden or `--http-tls off`) on HTTP; needs `--tls-key` |
| `--tls-key` | `FLUXMQ_TLS_KEY` | private key for `--tls-cert` |
| `--tls-client-ca` | `FLUXMQ_TLS_CLIENT_CA` | CA for client certificates: require mTLS on the client port |
| `--http-tls-cert` | `FLUXMQ_HTTP_TLS_CERT` | a separate chain for the HTTP port (default `--tls-cert`) |
| `--http-tls-key` | `FLUXMQ_HTTP_TLS_KEY` | key for `--http-tls-cert` |
| `--http-tls` | `FLUXMQ_HTTP_TLS` | `on` / `off`; default TLS when a certificate is configured; `off` keeps HTTP plaintext (behind a TLS proxy) |
| `--cluster-tls-cert` | `FLUXMQ_CLUSTER_TLS_CERT` | this node's certificate for node-to-node mutual TLS; needs the key and CA too, the same on every node |
| `--cluster-tls-key` | `FLUXMQ_CLUSTER_TLS_KEY` | key for `--cluster-tls-cert` |
| `--cluster-tls-ca` | `FLUXMQ_CLUSTER_TLS_CA` | the CA every node certificate must chain to |

## Subcommands {#subcommands}

Without a subcommand `fluxmq` runs the broker.

| Command | What it does |
|---|---|
| `fluxmq backup list` | list backups on a target, newest first (`--json` for JSON) |
| `fluxmq backup run` | back up the data directory of a **stopped** broker: `--data-dir`, `--encryption-key`, `--no-messages` |
| `fluxmq restore` | restore a backup into an empty directory: `--backup <id\|latest>`, `--data-dir`, `--encryption-key` |
| `fluxmq tls gen-dev` | issue a development CA and server, node and client certificates (development only) |

**Target for `backup` and `restore`:**

| Flag | Variable | Default | Description |
|---|---|---|---|
| `--fs-path` | `FLUXMQ_BACKUP_FS_PATH` | — | a filesystem directory (instead of S3) |
| `--s3-bucket` | `FLUXMQ_BACKUP_S3_BUCKET` | — | S3 bucket |
| `--s3-endpoint` | `FLUXMQ_BACKUP_S3_ENDPOINT` | — | custom endpoint (MinIO, R2, B2…); empty — AWS |
| `--s3-region` | `FLUXMQ_BACKUP_S3_REGION` | `us-east-1` | region (`auto` for R2) |
| `--s3-prefix` | `FLUXMQ_BACKUP_S3_PREFIX` | empty | key prefix inside the bucket |
| `--s3-path-style` | `FLUXMQ_BACKUP_S3_PATH_STYLE` | off | path-style addressing (MinIO) |
| `--s3-access-key-id` | `AWS_ACCESS_KEY_ID` | — | access key id |
| `--s3-secret-access-key` | `AWS_SECRET_ACCESS_KEY` | — | secret access key |
| `--encryption-key` | `FLUXMQ_BACKUP_ENCRYPTION_KEY` | — | encryption key (base64, 32 bytes) for `backup run` and `restore` |

**`fluxmq tls gen-dev`:** `--out <dir>` (required), `--nodes n1,n2,…` (node certificates `node-<id>.pem`, CN = id), `--hosts` (SANs, default `localhost,127.0.0.1`), `--force` (overwrite files), `--valid-days N` (default 397), `--reuse-ca` (issue from the existing `ca.pem` without changing the CA).

## Clients: demo and bench {#tools}

`fluxmq-demo` and `fluxmq-bench` accept `--addr` (`FLUXMQ_ADDR`, default `127.0.0.1:7420`), `--seeds` (`FLUXMQ_SEEDS`), `--token` (`FLUXMQ_TOKEN`), `--tls-ca`, `--tls-cert`, `--tls-key` (`FLUXMQ_CLIENT_TLS_CA`, `FLUXMQ_CLIENT_TLS_CERT`, `FLUXMQ_CLIENT_TLS_KEY`) and `tls://` addresses.

## Ports {#ports}

| Port | Protocol | Used for |
|---|---|---|
| 7420 | TCP, binary protocol (TLS with `--tls-cert`) | producers and consumers |
| 7421 | HTTP or HTTPS | REST `/api/*`, WebSocket `/ws`, `/metrics`, the console |
| 7422 | HTTP+JSON (mTLS with cluster TLS) | Raft and internal RPC — cluster only |
| 7423 | TCP (mTLS with cluster TLS) | partition replication — cluster only |

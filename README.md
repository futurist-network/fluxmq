# FluxMQ

[![CI](https://github.com/futurist-network/fluxmq/actions/workflows/ci.yml/badge.svg)](https://github.com/futurist-network/fluxmq/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/futurist-network/fluxmq)](https://github.com/futurist-network/fluxmq/releases/latest)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

FluxMQ is a single-binary message broker written in Rust with observability built in. It
offers three delivery modes (replayable streams, competing-consumer queues and per-key
ordered channels) with at-least-once delivery, retries with backoff, and a dead-letter
queue. It ships with a web console that shows live aggregates (health, traffic, lag,
errors), not a raw firehose: the broker computes rates, percentiles and alerts itself,
and the UI follows them over one WebSocket. You don't need a separate Prometheus,
Grafana or admin tool to see what is wrong and act on it.

> Status: **v0.11.0** — single node or a replicated, self-balancing cluster with node
> decommissioning, batched delivery, TLS on every port (client mTLS with certificate-to-role
> bindings, HTTPS, node-to-node mTLS), access tokens/roles with live-session revocation,
> content-based routing, notifications, HTTP push webhooks, backups, a Docker image and a
> comparative benchmark against Kafka and RabbitMQ. Usable for development and evaluation; see
> [Status and roadmap](#status-and-roadmap) for what is not done yet, and [CHANGELOG.md](CHANGELOG.md) for
> what changed in each version.

![FluxMQ dashboard, served by a real broker running the demo traffic](ui/screenshots/v0.10/dashboard.png)

## Features

- **Channels** in three modes: `stream` (partitioned, replayable log; partitions are spread
  across a group's instances), `queue` (competing consumers), `keyed` (strict order per key,
  other keys keep flowing). Fan-out to any number of consumer groups.
- **Durable storage:** segmented append-only log with CRC'd records, group commit (2 ms,
  default) or OS-buffered writes, and retention by age or size. Channel configs, group
  offsets (1 s checkpoints) and the DLQ survive restarts.
- **Delivery:** per-message ack/nack, prefetch, ack timeouts, retries with a backoff
  schedule, dead-lettering with the failure reason, DLQ retry/discard/export, replay of stored
  messages, consumer-group offset reset (earliest / latest / timestamp).
- **Web console** (`ui/`, served by the broker): dashboard with an alert center, channels,
  per-group and per-instance consumer health (CPU, processing p99, last error from the SDK),
  message search by key, offset or time, live tail, message inspector with the per-group
  retry timeline, an error inbox grouped by reason, nodes, a create-channel wizard and a ⌘K
  command palette.
- **Built-in telemetry:** rates and HDR latency percentiles, health rules and alerts (lag
  growth, stalled consumers, slow instance, DLQ growth, high publish latency, disk), events
  feed, and history (1 s → 1 m → 30 m rollups, up to 7 days).
- **Delayed messages:** deliver at a time or after a delay, up to 30 days (`flux-delay-ms` /
  `flux-deliver-at` headers, REST `delay_ms` / `deliver_at_ms`, SDK `.delay()` / `.deliver_at()`). They are
  stored on disk and in replicas, survive restarts and node failures, and can be cancelled or sent early from
  the console's Scheduled page.
- **Priority queues:** channel mode `priority` with 2–10 levels; consumers always take the most urgent level
  first (`flux-priority` header, SDK `.priority(n)`).
- **Idempotent publishing:** a retried batch is stored once, also across restarts and leader changes (on by
  default in the SDK).
- **Rust SDK** (`fluxmq-client`) over a compact binary TCP protocol, plus a REST API for
  everything the console does.
- **Ops basics:** bootstrap admin token plus access tokens (v0.2), an audit log of every
  mutating call, and graceful shutdown.
- **Tooling:** `fluxmq-demo` (an e-commerce story that exercises every screen) and
  `fluxmq-bench` (closed- and open-loop throughput/latency); see
  [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Install

```sh
# Docker (amd64 / arm64)
docker run -d --name fluxmq -p 7420:7420 -p 7421:7421 -v fluxmq:/var/lib/fluxmq ghcr.io/futurist-network/fluxmq

# Linux server with systemd (x86_64 / aarch64, glibc 2.36+): binary, /etc/fluxmq/fluxmq.toml,
# a generated admin token and the `fluxmq` service; re-run to upgrade
curl -fsSL https://raw.githubusercontent.com/futurist-network/fluxmq/master/deploy/install.sh | sudo sh
```

Then open <http://localhost:7421>. The console is built into the binary; settings live in
[`fluxmq.toml`](deploy/fluxmq.toml) (see [Running](#running)).

## 60-second quickstart (from source)

Needs Rust ≥ 1.88, Node ≥ 20.19 and pnpm 9.

```sh
cd ui && pnpm install && pnpm build && cd ..        # web console → ui/dist (built into the binary)
cargo build --release -p fluxmq-server -p fluxmq-client
./target/release/fluxmq --data-dir ./data &          # broker: TCP 7420, HTTP 7421
./target/release/fluxmq-demo &                       # realistic demo traffic
open http://localhost:7421                           # (xdg-open on Linux)
```

Within a minute the demo produces real problems for the console to show: a slow email
worker, an inventory consumer that stalls every few minutes, and payments that get
dead-lettered with `Invalid schema`. For a development loop with hot reload, run
`scripts/dev.sh`, which starts a debug broker, the demo and the Vite dev server on
<http://localhost:5173>.

**3-node cluster on one machine:** see [docs/CLUSTER.md](docs/CLUSTER.md) (English) or section 19 of
[docs/GUIDE.ru.md](docs/GUIDE.ru.md) (Russian user guide) — three `fluxmq --node-id nN --peers …`
commands, then `fluxmq-demo --seeds 127.0.0.1:17420,127.0.0.1:17430,127.0.0.1:17440`.

## Running

```sh
cargo run --release -p fluxmq-server -- --data-dir ./data
```

The broker listens on `7420` (client protocol, TCP) and `7421` (REST API, WebSocket telemetry
and the web UI at <http://localhost:7421/>). The UI is embedded from `ui/dist` at build time:
build it first with `cd ui && pnpm install && pnpm build`, otherwise `/` shows a hint page.

Every flag can also be set by its `FLUXMQ_*` environment variable or in a TOML file passed with
`--config` (`FLUXMQ_CONFIG`), keyed by flag name: see [deploy/fluxmq.toml](deploy/fluxmq.toml).
A flag beats the environment, which beats the file. Unknown keys are an error.

| Flag | Env | Default |
|---|---|---|
| `--data-dir` | `FLUXMQ_DATA_DIR` | `./data` |
| `--client-addr` | `FLUXMQ_CLIENT_ADDR` | `0.0.0.0:7420` |
| `--http-addr` | `FLUXMQ_HTTP_ADDR` | `0.0.0.0:7421` |
| `--cluster-name` | `FLUXMQ_CLUSTER_NAME` | `local` |
| `--config` | `FLUXMQ_CONFIG` | none |
| `--ui` | `FLUXMQ_UI` | `on` (`off`: API only) |
| `--ui-dir` | `FLUXMQ_UI_DIR` | none (the built-in console) |
| `--admin-token` | `FLUXMQ_ADMIN_TOKEN` | none (dev mode: everything open) |
| `--admin-token-file` | `FLUXMQ_ADMIN_TOKEN_FILE` | none (reads the token from a file) |
| `--fsync` | `FLUXMQ_FSYNC` | `group-commit` (or `os`) |
| `--log` | `FLUXMQ_LOG` / `RUST_LOG` | `info` |

With `--admin-token` or once any access token exists, REST and `/ws` need
`Authorization: Bearer <token>` (`/ws` also accepts `?token=`) and clients must send it in
`Hello`. `/api/health` stays open. Cluster flags (`--node-id`, `--peers`, `--raft-addr`,
`--replication-addr`, `--advertise-host`, `--replication-factor`, `--min-isr`,
`--cluster-secret` / `--cluster-secret-file`) are described in [docs/CLUSTER.md](docs/CLUSTER.md).

Development: `scripts/dev.sh` runs a debug broker on `./data-dev`, the demo traffic generator
(`fluxmq-demo`) and the UI dev server together; Ctrl-C stops everything.
SIGINT/SIGTERM shut the broker down gracefully (in a cluster it first hands its partition
leaderships over and leaves the ISRs; clients get `Goodbye`, data is flushed).
## Documentation

The full user guide is built into the console (**Docs** in the sidebar, English and Russian): quick start,
concepts, how-to guides (channels, clients, TLS, clusters, backups, webhooks), troubleshooting and the
reference for every flag, REST endpoint and metric. Its sources are in
[`ui/src/docs/content`](ui/src/docs/content). More in [docs/](docs/): [cluster](docs/CLUSTER.md),
[architecture](docs/ARCHITECTURE.md), [benchmarks](docs/BENCHMARKS.md), [roadmap](docs/ROADMAP.md) and the
Russian [user guide](docs/GUIDE.ru.md).

## SDK

```rust
use fluxmq_client::{Client, ClientOptions, StartFrom, SubscribeOptions, message};
use fluxmq_proto::{ChannelConfig, ChannelMode};

let client = Client::connect(ClientOptions::new("127.0.0.1:7420", "order-api")).await?;
client.ensure_channel(ChannelConfig::new("orders", ChannelMode::Stream)).await?;

// Produce
let producer = client.producer("orders");
let id = producer
    .send(message(r#"{"order_id":"ord-1","amount":42}"#)
        .key("ord-1")
        .event_type("OrderCreated")
        .content_type("application/json"))
    .await?;

// Consume (group "billing"). `delivery.nack(reason, true)` retries with backoff, then dead-letters.
let mut consumer = client
    .subscribe("orders", "billing", SubscribeOptions::default().prefetch(32).start(StartFrom::Earliest))
    .await?;
while let Some(delivery) = consumer.next().await {
    let delivery = delivery?;
    println!("{:?}: {}", delivery.key(), String::from_utf8_lossy(delivery.payload()));
    delivery.ack().await?;
}
```

Runnable versions: `cargo run -p fluxmq-client --example producer` and `--example consumer`.

### Reconnect and clusters

The client reconnects by itself (exponential backoff + jitter), resubscribes consumers and
retries publishes. Given several seeds, it fetches cluster metadata, partitions messages
client-side with the broker's own function (`crc32(key) % partitions`, round-robin without a
key), publishes directly to partition leaders and consumes from every leader node through one
merged stream. A broker without cluster support answers `Metadata` with `BadRequest`, and the
client then routes everything through one connection (v0.1 behaviour).

```rust
use std::time::Duration;
use fluxmq_client::{Client, ClientOptions, ReconnectPolicy};

let client = Client::connect(
    ClientOptions::with_seeds(["n1:7420", "n2:7420", "n3:7420"], "order-api")
        .reconnect(ReconnectPolicy::default().backoff(Duration::from_millis(100), Duration::from_secs(5)))
        .publish_timeout(Duration::from_secs(30)),
).await?;
let mut state = client.state(); // watch: Connecting / Connected / Reconnecting / Closed
```

Publishes are retried on connection loss, timeouts, `NotLeader` and `Overloaded` until
`publish_timeout`; `BadRequest`/`Forbidden` are never retried. **A retried publish can be stored
twice** (delivery is at-least-once), so keep consumers idempotent. `BatchingOptions { ordered:
true, .. }` keeps per-key order across retries. `cargo run -p fluxmq-client --example resilience`
checks for lost acknowledged publishes while you `kill -9` and restart the broker.

## Architecture

```text
 producers / consumers ──TCP :7420──► fluxmq-server ──► fluxmq-core (storage, groups, retry, DLQ)
                                          │                   │ pull: cumulative stats
 browser ◄── HTTP/WS :7421 ── fluxmq-api ◄┘   fluxmq-telemetry ◄┘ (rates, percentiles, alerts, history)
```

The data plane never waits for the observability plane. Core exposes pull-based counters and
histograms, and push paths (events, live tail) use bounded `try_send` channels. The UI
receives a snapshot and then JSON merge-patch deltas per subscribed topic.

| Crate | Role |
|---|---|
| `fluxmq-proto` | Shared contract: model, wire protocol, REST bodies, telemetry documents |
| `fluxmq-core` | Broker engine: segmented log, group commit, consumer groups, retry/DLQ |
| `fluxmq-telemetry` | Collector, health rules, alerts, history rollups, WS sessions |
| `fluxmq-api` | axum REST + WebSocket + static UI, permission table, cluster fan-out |
| `fluxmq-auth` | Access tokens and roles (RBAC) |
| `fluxmq-router` | Content-based routing and the filter DSL |
| `fluxmq-meta` | Revisioned metadata store (local `meta.json` or Raft) |
| `fluxmq-cluster` | Raft (openraft), controller, failover, rebalancing planner + executor, internal RPC |
| `fluxmq-replication` | Partition replication (followers fetch from leaders, ISR, HW) |
| `fluxmq-notify` | Notifications (rules, grouping, senders) and the `$system.events` feed |
| `fluxmq-webhooks` | HTTP push subscriptions (`$webhook:<name>` groups, signatures, SSRF guard) |
| `fluxmq-backup` | Incremental backups to S3/filesystem, encryption, retention, restore |
| `fluxmq-tls` | rustls TLS for every surface: reloadable certificates, mTLS, cluster TLS, `gen-dev` |
| `fluxmq-server` | The `fluxmq` binary: config, TCP client protocol, shutdown, `backup`/`restore` CLI |
| `fluxmq-client` | Rust SDK, `fluxmq-bench`, `fluxmq-demo` |

More: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) covers routes, semantics and the telemetry
contract, [docs/CLUSTER.md](docs/CLUSTER.md) running a cluster, [docs/GUIDE.ru.md](docs/GUIDE.ru.md)
is the full user guide (Russian), [docs/BENCHMARKS.md](docs/BENCHMARKS.md) has the benchmarks, and
[ui/README.md](ui/README.md) explains the console and its mock server.

## Project layout

```text
crates/       Rust workspace (see the table above)
ui/           web console (Vite + React + TypeScript), mock broker in ui/mock
docs/         ARCHITECTURE.md, CLUSTER.md, GUIDE.ru.md, ROADMAP.md, BENCHMARKS.md
scripts/      dev.sh: broker + demo + UI dev server
```

## Status and roadmap

v0.1 covered roadmap stages 0–6 on a single node. v0.2 adds stage 7 (RBAC), most of stage 8
(routing, filter search — scan-based, no payload index yet) and the core of stage 9 (cluster
with replication, Nodes page, Prometheus). v0.3 adds notifications + the events feed, HTTP push
webhooks and backups/restore (single node and cluster). v0.4 (stage 11) adds TLS on every
surface and automatic rebalancing with safe replica moves. v0.5 (stage 12) adds batched
delivery, live-session revocation and certificate bindings, node decommission / forget,
persistent notification history, Docker packaging and the comparative benchmark. Not done:
payload field indexing, OTel export, rack awareness, a byte-aware planner, Raft leadership
transfer, decommissioning several nodes at once, CA reload without restart / CRL / OCSP,
point-in-time restore into a cluster, OIDC/SSO. Details in
[docs/ROADMAP.md](docs/ROADMAP.md), limitations in section 17 of
[docs/GUIDE.ru.md](docs/GUIDE.ru.md) and in [docs/BENCHMARKS.md](docs/BENCHMARKS.md#caveats)
(including the replicated numbers).

## License

Apache-2.0 (see [LICENSE](LICENSE) and [NOTICE](NOTICE)). Copyright 2026 Futurist Network.

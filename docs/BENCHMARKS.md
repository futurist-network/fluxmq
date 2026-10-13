# FluxMQ benchmarks (v0.1 single node, v0.2 cluster, v0.3 webhooks and backups, v0.4 TLS and replication over TLS, v0.5 batched delivery, v0.6 hot path, v0.7 batches and reliability proof, v0.8 zero-copy hot path)

> **v0.8: zero-copy hot path, follower fetch-ahead, paced retention.** Report:
> [`bench/report/index.html`](../bench/report/index.html) (set `20261010-v0.8`). It opens with key answers (RU/EN) and the
> v0.8 goals, met or not met. Same laptop and limits as v0.7 (brokers 2 CPUs / 2 GiB). The client now gets 4 CPUs on 3 nodes
> too (v0.7: 2, which left Kafka's 3-node runs client-bound). FluxMQ runs group commit (fsync before every ack). Consumed
> msg/s; median of 2 for 1 KiB, otherwise 1 run. Kafka rows marked ◦ and every RabbitMQ row are reused from v0.7 (not re-run).
>
> | | FluxMQ 0.7 | **FluxMQ 0.8** | Kafka 3.7 default | Kafka fsync per message | RabbitMQ 4.1 ◦ |
> |---|---:|---:|---:|---:|---:|
> | 1 node, 1 KiB, 1×1 / 4×4 | 231k / 320k | **489k / 547k** | 470k / 538k | 302k / 395k | 11.8k / 15.6k |
> | 1 node, 100 B, 1×1 / 4×4 | 427k / 632k | **702k / 748k** | 643k ◦ / 472k | 673k ◦ / 734k | 11.4k / 38.3k |
> | 3 nodes RF=3, 1 KiB, 1×1 / 4×4 | 217k / 207k | **278k / 273k** | 253k ◦ / 251k | 160k ◦ / 120k ◦ | 6.3k / 10.4k |
> | 3 nodes RF=3, 100 B, 1×1 / 4×4 | 622k / 328k | **586k / 1.09M** | 617k ◦ / client-bound ◦ | 594k ◦ / client-bound ◦ | 7.3k / 10.7k |
> | p50 / p99 at 1k / 10k / 50k, 1 node | 1.5/3.6, 1.7/3.5, 2.4/8.2 ms | **1.9/18, 1.8/10, 2.8/15 ms** | 3.0/12 ◦, 2.1/12, 2.3/18 ms | 3.1/40 ◦, 12/38, 17/104 ms | 1.1/2.6, 4.1/49 ms, n.r. |
> | p50 / p99 at 1k / 10k / 50k, 3 nodes | 2.7/7.5, 7.6/19, 14/131 ms | **2.9/6.7, 6.7/15, 17/84 ms** | 2.8/8.3 ◦, 2.4/94, 2.6/81 ms | 3.8/100 ◦, 123/259 ◦, 197/1391 ms | – |
> | Acked-but-lost, kill -9 + power cut (1 node, leader, all 3) | 0 in 12 crashes | **0 in 6 crashes (560k acked)** | 90,094 / 119,178 on power cut ◦ | 0 ◦ | – |
> | Failover: kill -9 leader under 5k msg/s, no acks for | 5.9 s | **5.6 s** | 11.4 s ◦ | – | – |
> | TLS cost, 1 KiB (1 node 1×1 / 3 nodes 4×4) | −47 % / −47 % | **−56 % / −16 %** | – | – | – |
> | Data dir vs cap (4 GiB/broker) at saturation | overshoot by GBs | **≤ cap + 2 segments in 21/21 runs** | up to 9.3 GiB | | |
>
> - **At least as fast as Kafka's default on one node at 1 KiB** (1.04× / 1.02×, measured the same day) while fsyncing every
>   ack. Against Kafka with fsync per message (equal durability): 1.4–1.6× on 1 node, 1.7–2.3× on 3 nodes.
> - **Broker CPU per message is 3.9× lower than v0.7** on one node (0.85 cores at 511k msg/s). At 1 KiB the consumers now keep up.
>   Catch-up is 2–3× faster (1 node 1 KiB 1.28M msg/s). Under overload 100 B the consumers get 710k (1 node) and 1.16M (3 nodes).
> - **Durability unchanged:** 0 acked lost in every crash. Kafka's default loses 90–119k per pair of power cuts (v0.7 data).
> - **Goals not met:** 3-node p99 ≤ 20 ms at 50k (84 / 142 / 203 ms in three runs; the v0.7 image on the same host gives 86 /
>   136 / 366, a tie with Kafka's 81 ms). TLS ≤ 15 % (−56 % at 1 KiB on one node; no TLS code changed; v0.10 kTLS did not help, see docs/design/v0.10-ktls.md).
> - **Latency caveat:** the host's macOS storage indexer ran at 50–90 % CPU all session, which gave random 30–380 ms tails to the
>   v0.7 image as well. That is why the 1-node p99 is above v0.7's published numbers. Retention and fetch-ahead were ruled out as
>   the cause of the earlier "publish p99 spikes" (see the set's Host health section).

> **v0.7: FluxMQ vs Kafka vs RabbitMQ, plus a crash-durability matrix.** The report is
> [`bench/report/index.html`](../bench/report/index.html) (set `20261009-v0.7`), opening with "Key questions — answers"
> in Russian and English. Docker Desktop on one MacBook, 2 CPUs / 2 GiB per broker container, the v0.6 harness settings.
> Consumed msg/s, median of 2 repeats. FluxMQ in its default group commit (fsync before every ack).
>
> | | FluxMQ 0.6 | **FluxMQ 0.7** | Kafka 3.7 default | Kafka fsync per message | RabbitMQ 4.1 |
> |---|---:|---:|---:|---:|---:|
> | 1 node, 1 KiB, 1×1 / 4×4 | 256k / 355k | **231k / 320k** | 522k / 579k | 286k / 436k | 11.8k / 15.6k |
> | 1 node, 100 B, 1×1 / 4×4 | 404k / 36k | **427k / 632k** | 643k / 210k | 673k / 638k | 11.4k / 38.3k |
> | 3 nodes RF=3, 1 KiB, 1×1 / 4×4 | 202k / 307k | **217k / 207k** | 253k / 80k | 160k / 120k | 6.3k / 10.4k |
> | 3 nodes RF=3, 100 B, 1×1 / 4×4 | – / 11.5k | **622k / 328k** | 617k / client-bound | 594k / client-bound | 7.3k / 10.7k |
> | p50 / p99 at 1k / 10k / 50k msg/s, 1 node | 1.96/4.2, 2.7/6.4, 4.4/155 ms | **1.5/3.6, 1.7/3.5, 2.4/8.2 ms** | 3.0/12, 2.2/16, 2.2/22 ms | 3.1/40, 7.7/44, 15/57 ms | 1.1/2.6, 4.1/49 ms, n.r. |
> | Acked-but-lost after a power cut (1 node / all 3 nodes) | – | **0 / 0** | 90,094 / 119,178 | 0 / 0 | not measured |
> | Failover: kill -9 leader under 5k msg/s, no acks for | – | **5.9 s** | 11.4 s | – | – |
>
> - **More reliable than Kafka as shipped:** 0 acked messages lost in 12 kill -9 and power-cut runs (1.16M acked ids), where
>   Kafka's default loses 90–119k per pair of power cuts. Kafka with fsync per message also loses 0.
> - **Faster than Kafka:** latency tails, 3-node throughput with 4 consumers (2.6× at 1 KiB), consumers under overload
>   (100 B: 606k vs 201k), catch-up at 100 B (2×), failover (2×) and restart (0.4 s vs 9–22 s), memory (28 MiB idle vs 436 MiB).
> - **Slower than Kafka:** one-node saturation at 1 KiB (0.44–0.55× Kafka default, 0.73–0.81× Kafka fsync) and 100 B 1×1.
>   Engineering gap, not durability: the broker copies bytes to consumers and runs out of its 2 CPUs; see "Improvements".
> - **Data-loss bug found by this run and fixed before publishing** (a lagging group could skip stored, undelivered
>   messages when retention overtook it); every FluxMQ row is from the fixed build.

> **v0.6: FluxMQ vs Kafka vs RabbitMQ.** The comparative report is
> [`bench/report/index.html`](../bench/report/index.html). It shows set `20261009-v0.6`, a **partial** set stopped by
> the decision to start v0.7, plus the history of every set and a v0.5 → v0.6 section. Methodology:
> [`bench/README.md`](../bench/README.md). Docker Desktop on one MacBook, 2 CPUs / 2 GiB per broker container.
> Consumed msg/s; medians of 3 repeats (1 node) and 2 (3 nodes). FluxMQ uses group commit (fsync before every ack) and no
> producer header (`stamp_producer=false`). The harness keeps a window of 20000 unacknowledged messages per producer for every system.
>
> | | FluxMQ 0.5 (window 2000) | FluxMQ 0.6 | Kafka 3.7 | RabbitMQ 4.1 |
> |---|---:|---:|---:|---:|
> | 1 node, 1 KiB, 1×1 / 4×4 | 150k / 169k | **256k / 355k** | 534k / 587k | 11.2k / 10.6k |
> | 1 node, 100 B, 1×1 | 188k | **404k** | 696k | 11.2k |
> | 1 node, 100 B, 4×4 | 204k | **36k** (consumers starved, 1.49M published) | 374k (1.31M published) | 29.5k |
> | 3 nodes RF=3, 1 KiB, 1×1 / 4×4 | 94k / 120k | **202k / 307k** | not run in v0.6 | not run in v0.6 |
> | E2E p50 / p99 at 1k msg/s, 1 node | 3.9 / 6.4 ms | **1.96 / 4.2 ms** | 3.1 / 6.6 ms | 1.7 / 3.5 ms |
> | E2E p99 at 10k / 50k msg/s, 1 node | 13.0 / 593 ms | **6.4 / 155 ms** | 6.4 / 9.0 ms | 40.7 ms / n.r. |
>
> - **v0.6 roughly doubles FluxMQ's throughput** (+71 to +155 % at 1 KiB). Part of that is the fairer harness. A broker-only
>   check with the old harness at window 2000 gave +21 %. Kafka is still 1.7–2.1× faster on one node.
> - **Broker CPU at 10k msg/s dropped 45 %** (1.24 → 0.67 cores, 8 partitions): the fsync pool and the sticky partitioner.
> - **The retention stall at 50k msg/s is gone.** With the same 128 MiB/partition cap, p99 went from 572 / 1208 ms (v0.5) to
>   12.6–64 ms (v0.6). Random tails of 0.1–0.3 s remain in some runs.
> - **Open weakness:** under producer overload at 100 B 4×4, FluxMQ's consumers starve (29–43k msg/s on 1 node). Kafka
>   degrades much less.
> - **No loss:** every FluxMQ id that did not arrive was either still stored or provably removed by size retention (audit
>   plus the broker's retention-skip counter).
> - **SDK bug found by this run, fixed in 0.6:** `PublishFuture` used tokio's oneshot, which consumes the coop budget.
>   With 20k sends outstanding, throughput fell to 50k msg/s. It was already present in v0.5.
> - Kafka and RabbitMQ on 3 nodes, FluxMQ 3-node `fsync=os` and the commit-pipeline A/B were not run. Details are in the set's notes.
>
> v0.5 (set `20261009-full2`, window 2000): Kafka led throughput by 2.5–5×, and FluxMQ's p99 at 50k msg/s was 593 ms. That
> tail was retention deleting 128 MiB segments. Erratum (v0.6): the one "not drained" v0.5 run (3 nodes TLS, 1 KiB 4×4: 250 of
> 3.27 M messages) was a real retention bug that deleted the newest closed segment. It is fixed in v0.6, with a regression test.

These numbers come from a single developer laptop, with broker and clients on the same
machine, using `fluxmq-bench` (in `crates/fluxmq-client`) over the real TCP client
protocol. Treat them as a sanity check and a regression baseline, not a capacity plan. We
have not run comparative benchmarks against Kafka, RabbitMQ, NATS or others; that is
future work, and this document makes no claims about them.

## Machine

| | |
|---|---|
| Model | MacBook Pro `MacBookPro18,3` (`sysctl hw.model`) |
| CPU | Apple M1 Pro, 8 cores (6 performance + 2 efficiency) |
| RAM | 32 GB |
| Disk | Internal Apple SSD (APFS, Apple Fabric), **96 % full** during the runs |
| OS | macOS 15.7.7 (24G720) |
| Build | `cargo build --release` (thin LTO, `codegen-units = 1`), rustc 1.99.0 |

## Method

Each run starts a **fresh broker on an empty data directory**, runs one `fluxmq-bench`
invocation and then deletes the data directory. The bench creates a `stream` channel `bench`
with 8 partitions, starts the consumers in a fresh group at the tail, runs the producers for the
measured duration, then waits up to 10 s for the consumers to drain.

```sh
fluxmq --data-dir <fresh dir> --fsync <os|group-commit>

# closed loop: maximum throughput (each producer keeps 4 requests of 100 messages in flight)
fluxmq-bench --producers 4 --inflight 4 --batch 100 --consumers 4 --size <100|1024> --duration 10

# open loop: fixed 50k msg/s, latency measured from the scheduled send time
fluxmq-bench --producers 4 --inflight 32 --batch 10 --consumers 4 --size <100|1024> --rate 50000 --duration 20

# light load: 1k msg/s, one message per request
fluxmq-bench --producers 4 --inflight 32 --batch 1 --consumers 4 --size 1024 --rate 1000 --duration 20
```

* **Publish latency** is the round trip of one publish *request* (batch of N messages), from
  send until the broker acks it. Under `group-commit` the ack comes after the data is fsynced.
  Under `os` it comes after the write reaches the OS page cache.
* **E2E latency** runs from send (or, in open loop, the *scheduled* send) until a consumer
  receives the message. The timestamp is embedded in the payload, and producers and consumers
  share one process clock.
* **Consume msg/s** is the rate measured during the run window. "Drained" means every
  published message was consumed by the end of the 10 s grace period.

## Results

| Run | fsync | Size | Publish msg/s | Publish MB/s | Consume msg/s | Publish p50 / p99 / p99.9 (ms) | E2E p50 / p99 / p99.9 (ms) |
|---|---|---|---:|---:|---:|---|---|
| closed loop | os | 100 B | 2,744,000 | 274 | 255,000 (not drained) | 0.47 / 0.76 / 41 | 9,167 / 18,203 / 18,366 ¹ |
| closed loop | os | 1 KiB | 793,000 | 812 | 248,000 (not drained) | 1.11 / 34.6 / 92 | 7,684 / 14,533 / 14,680 ¹ |
| closed loop | group-commit | 100 B | 165,000 | 16.5 | 165,000 (drained) | 9.9 / 15.5 / 18.9 | 11.5 / 17.5 / 21.4 |
| closed loop | group-commit | 1 KiB | 191,000 | 196 | 191,000 (drained) | 7.1 / 15.0 / 39.2 | 11.8 / 19.8 / 41.2 |
| open loop 50k/s | os | 100 B | 50,000 | 5.0 | 50,000 | 1.22 / 2.81 / 8.1 | 1.49 / 3.98 / 10.4 |
| open loop 50k/s | os | 1 KiB | 50,000 | 51.2 | 50,000 | 1.27 / 6.24 / 20.3 | 1.52 / 7.92 / 22.6 |
| open loop 50k/s | group-commit | 100 B | 50,000 | 5.0 | 50,000 | 9.1 / 18.3 / 24.6 | 10.3 / 20.6 / 27.1 |
| open loop 50k/s | group-commit | 1 KiB | 50,000 | 51.2 | 50,000 | 9.4 / 18.5 / 22.8 | 10.5 / 20.5 / 25.1 |
| light 1k/s | group-commit | 1 KiB | 1,000 | 1.0 | 1,000 | 9.0 / 19.0 / 24.0 | 9.0 / 19.1 / 24.0 |
| light 1k/s | os | 1 KiB | 1,000 | 1.0 | 1,000 | 1.25 / 8.4 / 14.5 | 1.26 / 9.5 / 15.1 |

¹ In closed-loop `os` runs the producers outrun the consumers, so these numbers measure
queueing delay (a growing backlog), not delivery latency. Use the open-loop rows for E2E
latency.

How to read them:

* **The consumer side is the throughput ceiling.** One group of 4 consumers peaks at
  ~250k msg/s whatever the message size. Core dispatch takes the group lock once per message
  (about 200k msg/s per group over TCP at 1 KiB). With `--fsync os` the producers write about
  3× faster than one group can consume. Batched dispatch is the obvious next optimization.
* **Group commit costs about one device flush per commit.** On macOS that flush is
  `F_FULLFSYNC`, roughly 7–10 ms on this SSD, so commit latency sits at ~9 ms p50 at any
  load. Throughput comes from batching: 165–191k msg/s with 1,600 messages in flight. Under
  group commit, publishers need enough requests in flight to cover the flush time (Little's
  law). With 4 producers × 4 in flight × 10 messages, the same 50k msg/s open-loop run
  only reached ~15.5k msg/s, a client concurrency limit rather than a broker limit, which is
  why the open-loop rows use `--inflight 32`.
* **The 1 KiB closed loop reaches ~800 MB/s** of appends with `os`. It wrote 8.1 GB in 10 s,
  so set retention (`max_bytes`) before running long benchmarks.

## Known performance issue: fsync contention across channels

Each channel has its own writer thread and issues its own durability barrier. Within a channel,
all dirty partitions share one `F_FULLFSYNC`. Across channels nothing is shared, and
`F_FULLFSYNC` flushes the whole device cache, so concurrent barriers from different channels
serialize. Measured on the same machine with group commit, 1 KiB messages, one message per
request:

| Load | Publish p50 / p99 (ms) |
|---|---|
| 1 channel at 1,000 msg/s | 9.0 / 19.0 |
| 6 channels at 300 msg/s each (1,800 msg/s total) | 46–51 / 90–105 |

The demo, which writes to 6 channels, therefore shows publish p99 above 100 ms and keeps the
`high_publish_latency` alert open on this laptop. The planned fix is a broker-wide barrier
coordinator: channel writers do a plain `fsync`, then wait on one shared `F_FULLFSYNC` that
covers every channel's pending commits (group commit across channels). On Linux each channel's
`fdatasync` is also a device flush, so the same coordinator should help there too, but we
have not measured it.

## Replicated: 3-node cluster, RF = 3 (v0.2)

Three release `fluxmq` processes on the same laptop (the local cluster from `docs/CLUSTER.md`:
loopback, one SSD, default `--fsync group-commit` on every node, `--replication-factor 3`,
`--min-isr 1`), bench through the seeds (`--seeds 127.0.0.1:17420,127.0.0.1:17430,127.0.0.1:17440`,
client-side partitioning, direct-to-leader publishes), 8 partitions, 1 KiB, one run each,
demo traffic stopped. A publish is acknowledged once it is fsynced on the leader **and**
replicated to every ISR member (`acks=all`).

| Run | Flags | Publish msg/s | Consume msg/s | Publish p50 / p99 (ms) | E2E p50 / p99 (ms) |
|---|---|---|---|---|---|
| closed loop | `--producers 4 --consumers 4 --duration 10` (batch 100, inflight 4) | 23,350 (23.9 MB/s) | 23,323 (drained) | 63.7 / 204 (per 100-msg request) | 58.3 / 176 |
| open loop 1k/s | `--producers 2 --consumers 2 --rate 1000 --batch 1 --inflight 64` | 1,003 | 1,003 | 80.7 / 388 | 80.8 / 388 |
| open loop 5k/s | `--producers 4 --consumers 4 --rate 5000 --batch 10` (inflight 4) | 2,224 — **could not keep up** | 2,221 | 3,631 / 5,583 ¹ | 3,627 / 5,571 ¹ |

¹ In-flight-bound: with ~60 ms per replicated ack, 4 producers × 4 in-flight requests × 10
messages cap the client at ~2.5k msg/s; the open-loop latency then measures the queue. Raise
`--inflight` / `--batch` for throughput.

Compared with the single-node group-commit rows above (~10 ms p50), replication costs a lot of
latency on this machine. Likely reasons (not profiled in detail), none addressed in v0.2:

* **One SSD for three nodes.** Every node's group commit is an `F_FULLFSYNC` of the same
  device (mean 8.3 ms per call during the runs, `fluxmq_node_fsync_latency_seconds`), so the
  three processes' flushes queue behind each other. Separate machines/disks would
  not share this.
* **The ack needs a second fetch round:** followers append, then the leader learns
  their new log end offset on their *next* fetch before it can advance the high watermark.
* The cross-channel fsync contention described above, multiplied by three processes.
* Loopback and a shared 8-core CPU for 3 brokers + bench.

Treat these as a functional smoke test of replication, not as cluster performance numbers;
Linux on separate hosts with NVMe has not been measured.

## HTTP push webhook delivery (v0.3)

`cargo test -p fluxmq-webhooks --release --test webhooks bench_deliveries_per_second --
--ignored --nocapture`: an in-process broker (8 partitions, 200-byte payloads, 20 000 messages)
delivering to an in-process axum receiver on loopback that answers 204 immediately. v0.3.0,
same laptop:

| `max_in_flight` | `batch_max` | Throughput | Requests | Request latency p50 / p99 |
|---:|---:|---:|---:|---:|
| 1 | 1 | ~25.6 k msg/s | 20 000 | 0.03 / 0.08 ms |
| 16 | 1 | ~64.8 k msg/s | 20 000 | 0.20 / 0.34 ms |
| 64 | 1 | ~68.5 k msg/s | 20 000 | 0.61 / 1.22 ms |
| 4 | 100 | ~505 k msg/s | 200 | 0.07 / 0.18 ms |

Caveats: the receiver does no work and sits on loopback, so this is the broker-side ceiling
(delivery loop, envelope serialization, signing off, HTTP/1.1 keep-alive pool). A real endpoint
adds its own latency: with `max_in_flight = 1` (strict order) throughput per subscription is
bounded by `1 / endpoint latency` (e.g. 20 ms → ~50 msg/s); raise `max_in_flight` or batch.
One run per configuration; the in-memory broker skips fsync.

Live check (release server, `fluxmq-demo` traffic, Python receiver): signed deliveries verified
by the receiver; a 503-answering endpoint retried 3× per message (backoff 300/600 ms) and both
messages landed in the DLQ under `HTTP 503`; with auth enforced, a loopback target went to the
DLQ after one attempt as `blocked by policy`. In a 3-node cluster a 6-partition channel was
delivered by all three leaders (5 / 10 / 15 messages) with every delivery id exactly once.

## Backup and restore (v0.3)

Live runs on the release server with `fluxmq-demo` traffic (9 channels, 26 partitions), v0.3.0,
same laptop. "Logical" = bytes the backup describes (incl. reused segments).

| Run | Target | Logical size | Uploaded | Messages | Time |
|---|---|---:|---:|---:|---:|
| backup #1 (full) | filesystem (same disk) | 34.1 MB | 34.1 MB | 119 k | 0.71 s |
| backup #2 (1 s later) | filesystem | 34.3 MB | 34.3 MB (2 of 25 segments reused) | 120 k | 0.55 s |
| backup #1 | S3 emulator (moto, loopback) | 41.3 MB | 41.3 MB | 144 k | 0.44 s |
| backup #2 | S3 emulator | 41.6 MB | 41.5 MB (2 reused) | 145 k | 0.39 s |
| backup, AES-256-GCM | filesystem | 49.6 MB | 49.6 MB | 181 k | 0.88 s |
| 3-node cluster backup (3 leaders) | S3 emulator | 5.5 KB | — | 34 | 0.08 s |
| `fluxmq restore` | S3 emulator → empty dir | 41.3 MB | — | 145 k | 4.5 s (8.8 MiB/s) |
| `fluxmq restore`, encrypted | filesystem → empty dir | 49.6 MB | — | 181 k | 4.0 s (12.4 MiB/s) |

Caveats:

* **Incremental savings depend on sealed segments.** Only sealed (rolled) segments are reused;
  the active segment of every partition is re-uploaded up to the cut on each run. With the demo
  data every partition still has a single active segment, so almost nothing is reused (2 of 25
  segments — unchanged partitions). Channels with many rolled segments save proportionally more.
* **Restore is fsync-bound on macOS** (`F_FULLFSYNC` per segment plus record-by-record
  verification); Linux will differ. Restore downloads and verifies sequentially.
* **The S3 numbers use a local emulator** (moto) on loopback; real S3/R2 adds network
  bandwidth and per-request latency (multipart above 5 MiB, ≤ 3 segments in parallel).
* Backups read the page cache of a warm broker; a cold cache adds disk reads. One run each.

## TLS vs plaintext (v0.4)

Same machine and release build as above, single node, `stream` channel with 8 partitions,
1 KiB messages, broker and bench on loopback. TLS = `--tls-cert/--tls-key` with `gen-dev`
certificates (ECDSA P-256), the bench connecting with `--addr tls://… --tls-ca ca.pem`
(rustls/aws-lc-rs, TLS 1.3, AES-GCM). **The data directory was on a 6 GB RAM disk**
(`hdiutil attach -nomount ram://…`): the laptop's SSD had under 2 GB free, and a RAM disk
removes the `F_FULLFSYNC` floor, so the runs are CPU-bound — the worst case for TLS overhead.
That is why the plaintext numbers here are higher than in the SSD table above. Plaintext and
TLS runs alternated, three closed-loop rounds per fsync mode, 4 s each (other builds were
running on the machine, so expect ±15 % run-to-run noise).

```sh
fluxmq --data-dir <ramdisk>/data --fsync <group-commit|os> [--tls-cert server.pem --tls-key server-key.pem]
fluxmq-bench [--addr tls://127.0.0.1:18420 --tls-ca ca.pem] \
  --producers 4 --inflight 4 --batch 100 --consumers 4 --size 1024 --duration 4
```

Closed loop, msg/s per run (every run drained):

| fsync | | Publish, runs 1 / 2 / 3 | Publish mean | Consume, runs 1 / 2 / 3 | Consume mean |
|---|---|---|---:|---|---:|
| group-commit | plaintext | 523,844 / 402,044 / 535,597 | 487,162 | 371,138 / 301,953 / 371,605 | 348,232 |
| group-commit | TLS | 421,308 / 468,279 / 505,332 | 464,973 (**−4.6 %**) | 315,103 / 338,605 / 361,741 | 338,483 (**−2.8 %**) |
| os | plaintext | 805,839 / 705,158 / 984,205 | 831,734 | 298,418 / 287,598 / 328,324 | 304,780 |
| os | TLS | 652,223 / 706,485 / 865,098 | 741,269 (**−10.9 %**) | 303,957 / 290,112 / 319,595 | 304,555 (**−0.1 %**) |

Open loop at 50k msg/s (`--producers 4 --inflight 32 --batch 10 --consumers 4 --rate 50000`,
group-commit), ms, two runs each:

| | Publish p50 / p99 / p99.9 | E2E p50 / p99 / p99.9 |
|---|---|---|
| plaintext | 1.74 / 3.17 / 3.84 · 1.67 / 3.16 / 3.46 | 1.73 / 3.21 / 3.93 · 1.67 / 3.21 / 3.60 |
| TLS | 1.79 / 3.25 / 4.03 · 1.69 / 3.15 / 3.43 | 1.78 / 3.29 / 4.16 · 1.67 / 3.19 / 3.50 |

* **Gate met.** Publish + consume over TLS stays within 15 % of plaintext at 1 KiB: −3…−5 %
  with group commit, −11 % publish / ±0 consume in the fully CPU-bound `os` mode (where the
  consumer side, not TLS, is the ceiling).
* **Latency is unchanged** at a realistic load: TLS adds no measurable time at p50 – p99.9.
* Where the cost goes: the publish path encrypts ~800 MB/s on the broker's connection tasks
  plus a lock per read/write on the split TLS stream (`tokio::io::split`); plaintext keeps
  `TcpStream::into_split`. A non-TLS broker runs exactly the v0.3 code path.
* Replication over mTLS: see the next section.

## Replication over TLS: 3-node cluster, RF = 2 (v0.4 integration)

Same laptop, release build, three `fluxmq` processes on loopback and **the internal SSD**
(APFS, 95 % full; at least 12 GB stayed free during the runs). Each run used a fresh
3-node cluster with `--replication-factor 2 --min-isr 1`, 6 partitions, 1 KiB messages,
closed loop (`--producers 4 --consumers 4 --inflight 4 --batch 100`), seeds on all three nodes.
Publishes are acknowledged after the ISR (leader + follower) has the batch (`acks=all`).

* **TLS** means everything encrypted: client port TLS + mTLS (`tls://` seeds with a client
  certificate), HTTPS, and cluster mTLS. Replication fetches, Raft and internal HTTP run over
  rustls (`gen-dev` ECDSA P-256 certificates, TLS 1.3).
* **Plaintext** means the same cluster without any TLS flag (the v0.3 code path).

Runs alternated plaintext / TLS. The `os` runs are 8 s long, because at ~400k msg/s × RF 2 a
20 s run writes about 12 GB. The `group-commit` runs are 20 s. Everything drained.

```sh
fluxmq --node-id nX --peers … --replication-factor 2 --fsync <os|group-commit> \
  [--tls-cert server.pem --tls-key server-key.pem --tls-client-ca ca.pem \
   --cluster-tls-cert node-nX.pem --cluster-tls-key node-nX-key.pem --cluster-tls-ca ca.pem]
fluxmq-bench --seeds [tls://]127.0.0.1:17410,… [--tls-ca ca.pem --tls-cert client.pem --tls-key client-key.pem] \
  --channel bench --partitions 6 --producers 4 --consumers 4 --inflight 4 --batch 100 --size 1024 --duration <8|20>
```

| fsync | | Publish msg/s per run | Mean | Publish p50 / p99 per 100-msg request (ms) | E2E p50 / p99 (ms) |
|---|---|---|---:|---|---|
| os | plaintext | 340,633 / 406,734 / 407,344 | 384,904 | 2.9–3.0 / 23–42 | 3.4–3.5 / 20–34 |
| os | TLS | 356,457 / 353,350 / 350,458 | 353,422 (**−8.2 %**) | 3.5–3.6 / 24–27 | 3.9–4.0 / 20–22 |
| group-commit | plaintext | 23,171 / 21,726 | 22,449 | 67–71 / 119–126 | 54–57 / 108–113 |
| group-commit | TLS | 22,504 / 22,236 | 22,370 (**−0.4 %**) | 70 / 115–116 | 55–57 / 107–110 |

Consume equalled publish in every run, within 0.1 %.

* **With real durability (group commit) TLS costs nothing measurable.** The replicated ack is
  bounded by three processes sharing one SSD's `F_FULLFSYNC`, not by crypto: ~70 ms per
  replicated batch, the same with or without TLS. The difference is within the 7 %
  run-to-run spread of the plaintext runs.
* **CPU-bound (`os`), TLS costs about 8 %.** The node encrypts each byte up to three times:
  client → leader, leader → follower fetch, and leader → consumer. p50 latency rises by about
  0.5 ms. The first plaintext run was 16 % slower than the next two (a cold start of the
  laptop's page cache / SSD); without it the gap is about 13 %. Both are within the 15 % gate,
  which the design set for single-node publish+consume.
* Not measured: separate hosts with a real network (TLS record overhead is then hidden behind
  the RTT), Linux, larger clusters, and RSA certificates (ECDSA keeps handshakes cheap).
  Handshakes are not in these numbers: connections are long-lived and the replication stream
  and HTTP pool reuse theirs.

## Batched delivery (v0.5)

Before = the `4e411e8` release build (broker and bench); after = the v0.5 batched-delivery
build (wire v3 `DeliverBatch`, batch pull in core, SDK ack coalescing; see ARCHITECTURE.md
"Client protocol and batched delivery"). Same machine as above, single node on loopback,
`stream` channel with 8 partitions, 4 consumers (prefetch 2000, the bench default),
`--fsync os`, data directory on a 4 GB RAM disk (the SSD had < 6 GB free). Before and after
runs were **interleaved**, a fresh broker and data directory per run.

**Noise warning.** Other engineers were compiling and running test suites on the same laptop
throughout (load average 5–13 on 8 cores). Every configuration was repeated (3 rounds for
throughput, 8 for latency) and the tables give the **median** with the full range. Read the
throughput gain as "large and consistent", the latency rows as "no regression at 5k msg/s,
within noise at 50k msg/s".

### Consume throughput

Drain mode (new in v0.5, `fluxmq-bench --drain N`): N messages are published first with no
consumer attached, then 4 consumers start at the earliest offset of a fresh group and the
time to consume all N is measured. This isolates the consumer path. The old bench has no
drain mode, so both rows use the new bench (with the new SDK); the "before" broker sends
one `Deliver` per message, the "after" broker sends `DeliverBatch`.

```sh
fluxmq --data-dir <ramdisk>/data --fsync os --client-addr 127.0.0.1:20100 --http-addr 127.0.0.1:20101
fluxmq-bench --addr 127.0.0.1:20100 --channel dr --drain <1000000|3000000> --consumers 4 \
  --size <1024|100> --producers 4 --inflight 4 --batch 100
```

| Size | N | Before, msg/s (runs) | Before median | After, msg/s (runs) | After median | Change |
|---|---:|---|---:|---|---:|---:|
| 1 KiB | 1 M | 300,980 / 354,023 / 379,883 | 354,000 (363 MB/s) | 245,134 / 586,002 / 705,231 | 586,000 (600 MB/s) | **+66 %** |
| 100 B | 3 M | 426,666 / 426,828 / 493,104 | 427,000 | 613,525 / 762,135 / 860,381 | 762,000 | **+79 %** |

Closed loop (publish and consume concurrently, old bench + old broker vs new bench + new
broker; `--producers 4 --inflight 4 --batch 100 --consumers 4 --duration 6`). At 100 B the
producers outrun one group of 4 consumers, so the consume rate during the run is the consumer
ceiling:

| Size | Consume msg/s during run, before (runs) | median | after (runs) | median | Change |
|---|---|---:|---|---:|---:|
| 100 B | 337,929 / 342,024 / 368,458 | 342,000 | 279,096 / 477,279 / 504,431 | 477,000 | **+40 %** (not drained in either) |
| 1 KiB | 224,785 / 159,291 / 256,346 | 225,000 | 232,328 / 177,217 / 300,016 | 232,000 | consumers kept up (drained) in both; bounded by publish, not a consumer ceiling |

The v0.1 note above ("core dispatch takes the group lock once per message … batched dispatch
is the obvious next optimization") is what this addresses: one group-lock critical section
and one frame now carry up to 256 deliveries / 1 MiB, the codec serializes straight into the
socket buffer, and the SDK sends one `Ack` per 64 deliveries instead of waking an ack task per
message.

### Latency at a fixed rate (open loop, CO-corrected)

`--producers 4 --inflight 32 --consumers 4 --size <1024|100>`; 5k msg/s: `--batch 1 --rate 5000
--duration 15`; 50k msg/s: `--batch 10 --rate 50000 --duration 10`. End-to-end = scheduled
publish → delivery in the consumer, ms; median over 8 interleaved runs, range in brackets.

| Rate | Size | E2E p50 before | E2E p50 after | E2E p99 before | E2E p99 after |
|---|---|---:|---:|---:|---:|
| 5k/s | 1 KiB | 1.35 | 1.32 | 7.69 [2.35–43.2] | **4.89** [2.30–11.1] |
| 5k/s | 100 B | 1.32 | 1.31 | 5.17 [2.35–7.12] | **3.48** [2.28–5.77] |
| 50k/s | 1 KiB | 1.50 | 1.44 | 4.67 [3.40–19.8] | 6.30 [2.79–15.4] |
| 50k/s | 100 B | 1.45 | 1.44 | 3.33 [2.64–46.0] | 4.64 [2.54–98.7] |

* **No added latency.** p50 is unchanged or slightly better at both rates: the broker never
  waits to fill a batch (a lone ready delivery goes out at once as a plain `Deliver`).
* **p99 at 5k msg/s is not worse** (the acceptance gate); its median is lower in both sizes.
* At 50k msg/s the p99 medians are ~1.3 ms higher, but the before/after ranges overlap almost
  completely and both contain multi-10 ms outliers from the background load; this is not a
  measurable regression on this machine. Worth re-checking on a quiet box.

Caveats specific to this section:

* Drain throughput depends on prefetch: batches are bounded by the remaining prefetch credit,
  so small prefetch values (e.g. the SDK default 64) gain less.
* The SDK caps its ack batch at prefetch / 2 so delayed acks never starve the flow; acks are
  also flushed after `ack_linger` (1 ms default), on unsubscribe, drop and client close.
* Single node only; cluster (replicated) delivery uses the same pump and was not re-measured.

## Caveats

* **This is a laptop.** Thermal state, background processes, and a nearly full APFS volume
  (96 %) all affect the numbers. Each configuration was run **once**, so run-to-run variance
  was not measured.
* **macOS durability semantics:** Rust's `sync_data` on macOS is `F_FULLFSYNC`, a real
  device-cache flush, which is slower than Linux `fdatasync` on most NVMe drives. Linux
  server numbers will differ, probably with lower group-commit latency. They have not been
  measured.
* **Clients share the host with the broker**, so TCP goes over loopback and the producers,
  consumers and broker compete for the same 8 cores. Over a real network, latencies would rise
  by at least the network round trip.
* **Coordinated omission:** closed-loop latencies under-sample stalls by construction. The
  open-loop runs measure from the scheduled send time, which corrects for this, but only up
  to the client's own scheduling accuracy.
* **Short runs:** 10–20 s per configuration, a single partition layout (8), one consumer
  group and no retention pressure. Segment rolling, retention deletes, multi-group fan-out
  and long-run steady state were not benchmarked.
* **Comparative benchmarks** against Kafka and RabbitMQ are in a separate report, see below.

## Reproducing

```sh
cargo build --release -p fluxmq-server -p fluxmq-client
./target/release/fluxmq --data-dir /tmp/fluxmq-bench --fsync group-commit &
./target/release/fluxmq-bench --producers 4 --inflight 32 --batch 10 --consumers 4 \
    --size 1024 --rate 50000 --duration 20
kill %1 && rm -rf /tmp/fluxmq-bench
```

# Comparative benchmarks: FluxMQ vs Apache Kafka vs RabbitMQ

One command runs every system and scenario in Docker on one machine with the same resources and the
same client harness, then writes one self-contained HTML report.

```sh
bench/run.sh                                        # full run, ~1.5 h, report at the end
bench/run.sh --smoke                                # every scenario for a few seconds (~10 min)
bench/run.sh --systems fluxmq,kafka --scenarios saturation
bench/run.sh --systems fluxmq-single,fluxmq-single-tls --scenarios saturation,latency
python3 bench/report/generate.py bench/results/<timestamp>   # re-render a report
open bench/report/index.html
```

Needs Docker (Compose v2.24+), Python 3.9+ and about 10 GB of free disk for a full run (images,
build cache, and up to ~2 GB of broker data per run in the VM; each run's topic is deleted afterwards
and each system's volumes are removed with `down -v`). The runner refuses to start a run when the
host has less than `--min-free-gb` (default 8) free.

## Layout

| Path | What |
|---|---|
| `run.sh` → `runner.py` | Orchestration: build images if missing, per system `compose up --wait`, readiness, `docker stats` sampler, scenarios, `down -v`, report |
| `harness/` | `mq-harness`, the common Rust load/latency harness (FluxMQ SDK, rdkafka, lapin). Its own crate, **not** a workspace member; built in Docker (`harness/Dockerfile`, librdkafka compiled from source). `cargo build --release --no-default-features` builds a FluxMQ-only harness natively (no cmake), e.g. to run against a local cluster |
| `crash/` | Crash-durability test (kill -9 / simulated power cut, acked-but-lost per system): `crash/build.sh`, `crash/crash.py`, see `crash/README.md` |
| `report/generate.py` | Results dir → `report/index.html` (inline CSS/JS/SVG, light and dark themes) |
| `results/<timestamp>/` | `env.json`, `runs/*.json` (one per run), `runs/*.txt` (fluxmq-bench output), `stats.jsonl`, `index.json` |

## Systems

| Name | Stack (deploy/docker-compose.yml profile) | Durability |
|---|---|---|
| `fluxmq-single` | `single`, plaintext | group commit (fsync before ack) |
| `fluxmq-single-tls` | `single`, client TLS + HTTPS | same |
| `fluxmq-cluster` | `cluster`, `FLUXMQ_CLUSTER_TLS=off` | RF=3, min ISR 2, acks=all, group commit on every replica |
| `fluxmq-cluster-tls` | `cluster`, full TLS (client, HTTPS, node mTLS) | same |
| `fluxmq-single-os` | `single`, plaintext, `FLUXMQ_FSYNC=os` | ack after `write(2)`, no fsync: **durability parity with Kafka defaults** |
| `fluxmq-cluster-os` | `cluster`, `FLUXMQ_CLUSTER_TLS=off`, `FLUXMQ_FSYNC=os` | RF=3, min ISR 2, acks=all, no fsync (parity with Kafka defaults) |
| `kafka-single` | `kafka`, KRaft | `acks=all`, idempotent producer, default flush (page cache, no fsync per message) |
| `kafka-cluster` | `kafka-cluster`, KRaft ×3 | RF=3, `min.insync.replicas=2`, `acks=all`, default flush |
| `kafka-single-fsync` | `kafka`, `KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=1` | as `kafka-single`, plus broker `log.flush.interval.messages=1`: **fsync before ack** |
| `kafka-cluster-fsync` | `kafka-cluster`, `KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=1` | as `kafka-cluster`, plus fsync before ack on the leader and on every follower |
| `fluxmq-single-lz4`, `fluxmq-cluster-lz4` | as `fluxmq-single` / `fluxmq-cluster` | v0.7 extra row: producer `--fluxmq-compression lz4` |
| `fluxmq-single-cumulative`, `fluxmq-cluster-cumulative` | as `fluxmq-single` / `fluxmq-cluster` | v0.7 extra row: consumers `--fluxmq-ack-mode cumulative` |
| `rabbitmq-single` | `rabbitmq` | durable classic queues, persistent messages, publisher confirms |
| `rabbitmq-cluster` | `rabbitmq-cluster` | quorum queues (3 members), persistent, publisher confirms |

Aliases for `--systems`: `all`, `fluxmq`, `kafka`, `rabbitmq`, `single`, `cluster`, and `v07`.
`v07` selects only the v0.7 additions (the `-fsync`, `-lz4` and `-cumulative` rows), so they can
be added to an existing result set with `--out`.

**Kafka "fsync before ack" (`kafka-*-fsync`).** The compose variable
`KAFKA_LOG_FLUSH_INTERVAL_MESSAGES` (default Long.MAX_VALUE, which is Kafka's own default) becomes
the broker setting `log.flush.interval.messages`. Topics inherit it as `flush.messages`.

* With 1, `UnifiedLog.append` flushes the partition log (fsync of the segment and its index)
  whenever an append leaves at least one unflushed message. That is every produce batch, inside
  the append, before the produce response.
* With `acks=all`, followers apply the same setting on their fetch appends. A follower's next
  fetch, which is what advances the high watermark, therefore comes after its fsync, and every
  ISR member has fsynced an acked batch.
* `log.flush.interval.ms` is left at its default.

The runner records the effective value from `kafka-configs --describe --all` in
`runs/<system>-system.json` (`kafka_log_flush_interval_messages`). The crash test
(`bench/crash/`) shows the difference: with the default flush a power cut loses acknowledged
messages, and with the fsync variant it does not.

**FluxMQ v0.7 fast-path flags (harness).** The headline `fluxmq-*` rows use the SDK defaults:
protocol v4 batches, individual acks, no compression, raw records and CRC verification on.

| Flag | SDK setting | Default |
|---|---|---|
| `--fluxmq-compression none\|lz4\|zstd` | `ClientOptions::compression` | `none` |
| `--fluxmq-ack-mode individual\|cumulative` | `SubscribeOptions::ack_mode` | `individual` |
| `--fluxmq-raw-records true\|false` | `SubscribeOptions::raw_records` | `true` |
| `--fluxmq-verify-crc true\|false` | `SubscribeOptions::verify_crc` | `true` |

Every flag is recorded in the run JSON (`params.fluxmq_*`).

The `-os` variants are the like-for-like comparison with Kafka, whose default flush policy also
acknowledges from the page cache. The headline numbers and the report's conclusions stay on group
commit (FluxMQ's default); the report shows the `-os` rows in a lighter colour, marked ‡, and in
their own "fsync=os" section next to their group-commit counterparts.

Every broker container gets `--broker-cpus` (2) and `--broker-mem` (2g); the client container gets
`--bench-cpus` (2) and `--bench-mem` (2g). Systems never run at the same time. The limits are
recorded in every run's JSON, so two invocations with different client limits can share one
results directory (`--out`), e.g. the published report:

```sh
OUT=bench/results/$(date +%Y%m%d)-full
bench/run.sh --systems single  --bench-cpus 4 --repeat 3 --out $OUT --no-report            # 2 + 4 of 8 VM CPUs
bench/run.sh --systems cluster --bench-cpus 2 --repeat 3 --scenarios idle,saturation,latency --out $OUT   # 3×2 + 2
```

Use the same client limits for every system in one comparison (single-node systems with each
other, clusters with each other), and keep brokers + client within the VM's CPUs.

## Scenarios

| Scenario | What | Defaults (full run) |
|---|---|---|
| `idle` | broker CPU and memory at rest (after 30 s settle, 30 s window) | |
| `saturation` | closed loop, max publish + consume; CPU/RSS "under load" from the same window | 1 KiB and 100 B × (1p×1c, 4p×4c); 10 s warm-up + 30 s measured |
| `latency` | open loop at fixed rates, end-to-end p50/p90/p99/p99.9/max | 1k, 10k, 50k msg/s, 1 KiB, 4p×4c, 30 s; a rate is skipped (and reported) once a lower one was not sustained or the system's 1 KiB saturation throughput is below it |
| `catchup` | Preload `--catchup-gb` with no consumer reading, then start the consumers and measure consume-only throughput until every preloaded message arrived (checked by id; bounded by `--catchup-timeout`, default 600 s). Retention is raised for the run to 2× the preload per partition so nothing is trimmed (the volume guard still applies) | 1 GiB at each `--sizes`, `--catchup-clients 4x4`. JSON: `consume_rate` (from consumer start, including subscribe / group join), `preload.consume_rate_after_first`, `preload.first_receipt_s`, `preload.catchup_s`, `preload.publish_rate` |
| `overload` | Producers unthrottled (closed loop, same window as saturation) for a longer window: consumed msg/s and how fast consumer lag grows. No drain check (a backlog is the point); retention drops under the size cap are counted by the brokers | `--overload-duration 60`, `--overload-clients 4x4`, each size. JSON: `consume_rate`, `lag_start`, `lag_end`, `lag_growth_per_s`, per-second `lag` in `timeline` |
| `batching` | FluxMQ only, single node, v0.4 baseline image vs the current image, `--batch-repeat` rounds (default 3) interleaved, a fresh broker per round | each image's own `fluxmq-bench`: closed loop 1 KiB and 100 B, 1k msg/s 1 msg/request; the **current** bench's drain mode against both brokers (1 M × 1 KiB, 3 M × 100 B prefilled, then 4 consumers drain) to isolate the consumer path |

Drain misses are classified per run (`drain_miss.classification`): **dropped by retention** (consumers fell further
behind than the cap; for FluxMQ confirmed by the audit plus the broker's retention-skip log, for Kafka/RabbitMQ inferred
from consumer lag vs the cap), **stored, not yet delivered** (FluxMQ audit found the id in the log) and **missing**
(unexplained, true loss). A FluxMQ run with missing messages stops the matrix (`--no-stop-on-missing` to continue);
after a failed harness run the runner deletes orphaned `bench-*` channels/topics. `fluxmq-single-pipeline` (not in any
alias) is FluxMQ with `FLUXMQ_COMMIT_PIPELINE=on` for A/B runs (the broker default since v0.9; `off` is the
v0.8 behaviour, `FLUXMQ_COMMIT_PIPELINE_DEPTH` the per-partition depth, see docs/design/v0.9-commit-path.md).
`fluxmq-{single,cluster}-tls-ktls` (alias `ktls`, with the plain and `-tls` rows) run kernel TLS
(`FLUXMQ_TLS_IMPL=ktls`, opt-in since v0.10; see docs/design/v0.10-ktls.md).

Options: `--repeat N` (saturation repeats per configuration, interleaved; the report shows the
median and min–max), `--batch-repeat N`, `--duration`, `--latency-duration`, `--warmup`, `--sizes 1024,100`, `--clients 1x1,4x4`,
`--rates 1000,10000,50000`, `--partitions 8`, `--window 20000`, `--max-in-flight 32`,
`--linger-ms 5` (saturation), `--latency-linger-ms 1`, `--retention-mb 128`, `--segment-mb 16`,
`--late-drain 45`, `--idle-settle 30`, `--ref <git ref>` (package a commit instead of the working tree),
`--baseline-ref 3a6bb81`, `--build auto|always|never`, `--out <dir>`, `--no-report`.

Fairness settings (since the v0.6 runs; result sets up to `20261009-full2` used window 2000, the
SDK's default 8 in-flight requests and Kafka segments of retention/8):

* `--window 20000` unacknowledged publishes per producer, for every system. By Little's law
  in-flight = throughput × ack latency; with the 20–50 ms acks seen under saturation, 20k allows
  0.4–1 M msg/s from one producer, above every system's measured ceiling (Kafka 1 node 1×1:
  383k msg/s), so the window no longer caps anyone. 2000 capped FluxMQ 1×1 at ~143k msg/s
  (2000 ÷ 14 ms). Larger only adds queueing latency and client memory (20 MB per producer at
  1 KiB).
* `--max-in-flight 32`: FluxMQ `BatchingOptions::max_in_flight` (pipelined `Publish` requests
  per producer; SDK default 8). librdkafka's idempotent producer pipelines up to 5 requests per
  broker connection, i.e. 15 per producer on 3 brokers; AMQP confirms have no such limit.
* `--segment-mb 16` for FluxMQ (broker-wide `FLUXMQ_SEGMENT_BYTES`, via compose) and Kafka
  (topic `segment.bytes`); `--retention-mb` is the same cap for all three (RabbitMQ:
  `x-max-length-bytes`; it has no segment setting). Brokers without `FLUXMQ_SEGMENT_BYTES`
  ignore it.

## Why one common harness

The official tools were considered and rejected for the comparison:

* `kafka-producer-perf-test.sh` reports producer (ack) latency only and `kafka-consumer-perf-test.sh`
  reports no latency; there is no end-to-end number to compare.
* RabbitMQ `perf-test` does measure end-to-end latency, but with its own Java client, batching and
  timestamp scheme, so a difference between systems would partly be a difference between tools.

`mq-harness` runs **the same engine** for all three: identical producer loop (async publish with a
bounded window of unacknowledged messages), identical consumer accounting, identical HDR histograms,
identical open-loop scheduling, the send timestamp in the first 8 payload bytes and producers and
consumers in one process (no clock skew). Each system is reached through its standard native client:
the FluxMQ Rust SDK (`BatchingProducer`), librdkafka 2.12 (the C client behind most non-Java Kafka
clients, built from source with rdkafka's `cmake-build`, no system packages) and lapin (AMQP 0-9-1).
Client-side batching is each library's own, with the same linger for FluxMQ and Kafka. The harness
image is ~120 MB; its build cache ~1 GB.

Rules that keep it honest:

* Fresh topic / channel / queues per run, deleted afterwards; the systems' stacks are fresh per system.
* Warm-up discarded; throughput counted in the measured window only; latency recorded only for
  messages *sent* in the window; "drained" = every message sent in the window and acknowledged
  by the broker was received within 15 s, checked **by message id** (producer id + sequence
  number in the payload; a duplicate counts once).
* On a miss the harness keeps the consumers running up to `--late-drain` (45 s, more than the
  30 s FluxMQ/AMQP ack timeout, so an unacked delivery that gets redelivered shows up) and
  reports `drain_miss` in the run JSON: `late` (arrived in the extra time, with partition,
  offset, attempt and node of each late arrival) vs `missing`, the missing ids (and ranges),
  and for FluxMQ an `audit`: a fresh consumer group re-reads the channel from the earliest
  offset and says, per missing id, whether it is stored (partition/offset) or not in the log.
  The channel / topic is deleted only after that.
* Open loop measures from the scheduled send time (coordinated omission corrected); a rate that is
  not reached is reported as not sustained, never silently.
* Resource usage comes from `docker stats` sampled during the same windows; the client container is
  sampled too, so a client-bound run is visible.

## Results format

`runs/<system>-sat-1024B-4p4c.json` (abridged):

```json
{ "kind": "harness", "system": "kafka", "label": "kafka-cluster", "scenario": "saturation",
  "params": {"size": 1024, "producers": 4, "consumers": 4, "partitions": 8, "rate": 0, ...},
  "durability": "Kafka: topic 8 partitions, RF=3, min.insync.replicas=2; producer acks=all ...",
  "measure_start_ms": 1791468600000, "measure_end_ms": 1791468630000,
  "publish_rate": 123456, "consume_rate": 123400, "publish_mb_s": 126.4, "drained": true,
  "sustained": null, "e2e_ms": {"p50": 4.1, "p90": 7.9, "p99": 15.2, "p999": 30.1, "max": 52.0, ...},
  "publish_ms": {...}, "errors": 0, "timeline": [{"t": 1, "phase": "warmup", "pub": 98000, "con": 97000}, ...] }
```

`stats.jsonl`: `{"ts", "system", "service", "cpu_pct", "mem_bytes", "mem_limit"}` per container per sample.
`env.json`: host CPU/RAM/OS, Docker VM CPUs/RAM, limits, image ids/revisions, git sha, the plan.

## Published result sets

Never rewritten (`env.json` has `"frozen": true`; the runner refuses to write into them):

| Set | What |
|---|---|
| `20261008-171456-smoke` | smoke run, seconds per scenario (harness proof only) |
| `20261009-full` | first full run, aborted at kafka-single when the host disk filled (Kafka kept deleted segments 60 s); valid: FluxMQ 1 node ± TLS; Kafka records reconstructed from the runner log and marked invalid |
| `20261009-full2` | complete v0.5 comparison (window 2000); `notes.html` = the analysis shown in the report, `investigation/` = the 50k msg/s retention A/B runs |
| `20261009-v0.6` | v0.6, **partial** (stopped to start v0.7): every 1-node system plus FluxMQ 3 nodes ± TLS. `investigation/` holds the retention-stall and broker-CPU A/B runs, the window sweep that found the SDK coop bug (`sdk-coop-diag/`), and the rows measured with that bug (`sdk-coop-bug-window20000/`). Shown in `index.html` |

Findings that come after a set was frozen go to `report/errata/<set>.html`, which the report shows
above that set's notes. For example, the `20261009-full2` drain miss turned out to be a retention bug,
fixed in v0.6.

`python3 bench/report/generate.py --all` renders `bench/report/<set>.html` for every set and
`bench/report/index.html` = the latest complete set + a history / before→after section. A new run
(e.g. after optimizations) is just another `--out` directory.

Disk safety: Kafka brokers delete segments after 1 s (`log.segment.delete.delay.ms`,
`file.delete.delay.ms`), 16 MiB segments, retention checked every second; after every run the
runner logs each broker's data-dir size (`volumes.jsonl`) and aborts the whole matrix when one
exceeds `--max-volume-gb` (5) or host free disk drops below `--min-free-gb` (10); Kafka topics are
verified deleted; every system's stack is removed with `down -v`.

## Caveats

macOS Docker VM (virtual disk, bridge network), one laptop shared by brokers and clients, three
"nodes" on one VM and one SSD, one run per configuration, near-default broker settings, different
durability semantics per system. The report repeats these next to the numbers.

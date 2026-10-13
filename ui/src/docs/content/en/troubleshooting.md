---
title: Troubleshooting and FAQ
group: howto
order: 11
summary: Common symptoms and their causes — start-up, auth, latency, clusters, TLS, clients — and answers to frequent questions.
---

## Start-up and the console {#startup}

| Symptom | Cause and fix |
|---|---|
| `Address already in use` | port 7420 or 7421 is taken (often by a second instance or `pnpm mock`). Find it with `lsof -i :7421`, or change `--http-addr` / `--client-addr` |
| The console address shows "UI is not built" | the console isn't built: `cd ui && pnpm install && pnpm build`, then check `--ui-dir` |
| A "Connection lost" banner | the broker is stopped or unreachable; the console reconnects by itself |
| Write errors, "restart the broker" | the disk is full; free space and restart — the broker repairs its files at start |
| `cargo` complains about the version | Rust 1.88+ is required: `rustup update stable` |

## Auth {#auth}

| Symptom | Cause and fix |
|---|---|
| The console asks for a token, REST answers 401 | auth is enforced: enter a token in the console or send `Authorization: Bearer …` |
| Client: `Unauthorized` on connect | pass a token (`auth_token`, `--token`, `FLUXMQ_TOKEN`) or set up a certificate binding |
| 403 `forbidden` / `Forbidden` in a client | the token is valid but its role or scopes aren't enough. See the role with `curl -s $H/api/auth/me -H "Authorization: Bearer $TOKEN"` |
| "Session token revoked or expired", WebSocket closed with 4401 | the token was revoked or expired; sign in with a new one |
| Can't revoke a token: `409 last_admin` | it's the last admin token — issue another one first |

## Performance {#performance}

| Symptom | Cause and fix |
|---|---|
| Slow publishing on macOS, "High publish latency" alerts | that's `F_FULLFSYNC` in `group-commit` mode (7–10 ms per flush). For development use `--fsync os`; in production Linux with NVMe |
| Consumed on the Dashboard exceeds Published | normal: Consumed sums the acks of every group |
| Lag grows, a "Slow instance" alert | open the instance drawer on Consumers: CPU, processing time, last error. See [Investigate lag](/docs/howto-investigate-lag) |
| Messages pile up in the DLQ | open Dead Letters, fix the cause, Retry all. See [Handle the DLQ](/docs/howto-handle-dlq) |
| Message search says "scan limit reached" | there is no payload index; up to 200,000 messages are scanned per query — narrow the time range, partition or key |

## Cluster {#cluster}

| Symptom | Cause and fix |
|---|---|
| 503 "partitions … are changing leaders; retry" | a leader change is in progress (failover, drain, reassign). Retry in a couple of seconds; the SDK does it itself |
| A node shows Down after a graceful stop | expected: after it starts again it turns Up within ~10 s |
| `Overloaded: not enough replicas` | the partition's ISR is smaller than `--min-isr`; bring the failed nodes back |
| A node won't start: "a cluster secret is required" | with `--peers`, `--node-id` and `--cluster-secret` (the same on every node) are required |
| Decommission refused with `409` | another rebalance or decommission is running, too few nodes for the replication factor, a partition has no leader, or the quorum would break — the response says which |
| Clients can't find nodes in Docker | addresses like `n1:7420` only resolve inside the compose network — run cluster clients there |

## TLS {#tls}

| Symptom | Cause and fix |
|---|---|
| Client: `this port requires TLS` | the port uses TLS but the client connects in plaintext — use `tls://host:port` or `ClientOptions::tls` |
| HTTP request: 400 "This port speaks HTTPS" | open the console with `https://` |
| `TLS handshake with …` in a client | a foreign CA or an expired certificate; check `ca_file` and the expiry on [Security](/docs/security) |
| The broker won't start with TLS flags | an incomplete combination (certificate without key, `--tls-client-ca` without `--tls-cert`, cluster TLS without all three files) or a broken PEM — the error message says which |
| A certificate binding is "inert" | the client port has no mTLS — start the broker with `--tls-client-ca` |

## Questions {#faq}

**Are messages lost when the broker crashes?** Acknowledged ones aren't: by default the ack comes only after fsync (and ISR replication in a cluster). After a crash the broker checks the last segment's tail by CRC and cuts off the incomplete part — it was never acknowledged. Group offsets are saved every second, so up to ~1 s of already processed messages can be redelivered.

**Why did I get a duplicate?** Delivery is at-least-once: a publish retry after a dropped connection, a broker crash, a group rewind or a DLQ retry can deliver a message again. Make handlers idempotent.

**Can I change the partition count?** No, only at creation. Create a new channel and move the flow.

**How is FluxMQ different from Kafka and RabbitMQ?** One binary with a built-in console and alerts; stream (Kafka-like), queue (RabbitMQ-like) and strict per-key modes; per-message acks, retries and a DLQ out of the box; fsync before ack by default.

**Are there SDKs for other languages?** Rust only for now. Others publish over REST and consume through [webhooks](/docs/webhooks).

**Where are detailed logs?** `--log debug` or `RUST_LOG=info,fluxmq_core=debug,fluxmq_api=debug`.

---
title: Connect a client (Rust SDK)
group: howto
order: 3
summary: A producer and a consumer with the Rust SDK — publishing, batching and compression (0.7), acknowledgements, ack modes, tokens, TLS and clusters.
---

The official SDK is `fluxmq-client` (Rust, tokio). It speaks the binary protocol on port 7420, reconnects by itself, publishes straight to partition leaders in a cluster and reports processing stats to the broker every 2 seconds — visible on [Consumers](/docs/consumers). For languages without an SDK, publish over REST (`POST /api/channels/{name}/publish`) and consume with [webhooks](/docs/webhooks).

## Dependencies {#deps}

The SDK is used by path for now (not on crates.io yet):

```toml
[dependencies]
fluxmq-client = { path = "../FluxMQ/crates/fluxmq-client" }
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

## Producer {#producer}

```rust
use fluxmq_client::{message, ChannelConfig, ChannelMode, Client, ClientOptions};

#[tokio::main]
async fn main() -> fluxmq_client::Result<()> {
    let mut opts = ClientOptions::new("127.0.0.1:7420", "order-api-1");
    opts.auth_token = std::env::var("FLUXMQ_TOKEN").ok();     // when auth is enforced
    let client = Client::connect(opts).await?;

    client.ensure_channel(ChannelConfig::new("orders", ChannelMode::Stream)).await?;

    let producer = client.producer("orders");
    let id = producer
        .send(
            message(r#"{"order_id":8842,"amount":129.9}"#)
                .key("8842")                  // partition and order follow the key
                .event_type("OrderCreated")   // flux-type header, shown in the console
                .content_type("application/json")
                .new_trace_id(),              // flux-trace-id
        )
        .await?;
    println!("partition {} offset {}", id.partition, id.offset);
    Ok(())
}
```

`send` returns **after** the message is on disk (and, in a cluster, replicated to the ISR). The `flux-producer` header with the client name is set automatically (turn it off with `ClientOptions::stamp_producer(false)`).

## High throughput: batching and compression {#batching}

`BatchingProducer` collects each partition's messages into batches and returns a future per message. The linger is adaptive: when nothing of a partition is in flight a message goes out at once; a batch only accumulates while the partition's previous request is unacknowledged — no added latency at low rates.

```rust
use fluxmq_client::{message, BatchingOptions, Client, ClientOptions, Compression};
use futures::stream::{FuturesUnordered, StreamExt};

let client = Client::connect(
    ClientOptions::new("127.0.0.1:7420", "ingest-1").compression(Compression::Lz4),
).await?;
let producer = client.batching_producer("clicks", BatchingOptions::default());

let mut pending = FuturesUnordered::new();
for i in 0..100_000u32 {
    // send() only waits for queueing; the future resolves once the message is durable
    pending.push(producer.send(message(format!(r#"{{"n":{i}}}"#)).key(format!("user-{}", i % 500))).await?);
}
while let Some(r) = pending.next().await {
    r?; // MessageId; an error means the message was not stored
}
producer.close().await; // wait until everything queued is published
```

| `BatchingOptions` | Default | Meaning |
|---|---|---|
| `linger` | 1 ms | how long a batch may wait for company while its partition already has a batch in flight |
| `max_batch` | 1000 | messages per batch |
| `max_batch_bytes` | 1 MiB | bytes per batch (keep well under the 16 MiB frame limit) |
| `max_in_flight` | 8 | requests in flight across all partitions |
| `queue_capacity` | 16,384 | per-partition queue; `send` waits when it is full (backpressure) |
| `ordered` | `false` | `true` — at most one batch in flight per partition: per-key order survives retries |

With a 0.7+ broker (protocol v4) each batch is encoded once into a **record batch** (`PublishBatch`) that the broker appends to the log as is. Durability is unchanged: the answer comes after fsync and replication.

**Compression** — `ClientOptions::compression(…)`, per batch:

| Option | When | Cost |
|---|---|---|
| `Compression::None` (default) | small or already compressed/encrypted payloads, lowest latency | — |
| `Compression::Lz4` | JSON, logs, text; network or disk is the bottleneck | a little producer CPU, very cheap decompression |
| `Compression::Zstd` | maximum traffic and storage savings (replication, long retention) | more CPU on both sides |

The broker stores, replicates and delivers the batch compressed, decompressing only for filters, the inspector and search. A batch that doesn't shrink is sent uncompressed. Brokers before 0.7 ignore the setting.

## Duplicate protection when publishing {#idempotent}

When the broker's answer is lost (network drop, timeout, leader change), the SDK retries. Such a retry used to store the batch twice. With a v0.12 broker it doesn't: the SDK marks every batch with its client id and batch number (the `flux-pseq` header on the first message, about 30 bytes per batch), and the broker answers a retry of a stored batch with the same message ids and writes nothing.

- On by default; turn off with `ClientOptions::idempotent(false)`.
- Works across a leader change: the new leader rebuilds its table from the last minute of its log.
- A retry that arrives while the first send is still being written waits for it and gets the same answer.
- It protects against the SDK's own retries. If your application sends the same message again, that is a new batch — recognize such repeats by an id of your own in a header.
- Retries dropped so far: the `fluxmq_channel_publish_duplicates_total` metric.

## Consumer {#consumer}

```rust
use fluxmq_client::{Client, ClientOptions, StartFrom, SubscribeOptions};
use futures::StreamExt;

let client = Client::connect(ClientOptions::new("127.0.0.1:7420", "billing-worker-1")).await?;
let mut consumer = client
    .subscribe("orders", "billing",
        SubscribeOptions::default().prefetch(32).start(StartFrom::Earliest))
    .await?;

while let Some(delivery) = consumer.next().await {
    let delivery = delivery?;   // Err only for terminal errors (Forbidden, channel deleted, client closed)
    match handle(delivery.payload()).await {
        Ok(()) => delivery.ack().await?,
        // the reason shows in the DLQ and groups failures — keep it short and stable
        Err(e) if e.is_transient() => delivery.nack("PostgreSQL timeout", true).await?,   // retry
        Err(_) => delivery.nack("Invalid schema", false).await?,                           // straight to the DLQ
    }
}
```

- `"billing"` is the **consumer group**. Run several instances with the same group and they share the work; another group receives every message again.
- `prefetch` (default 64) — unacknowledged messages an instance may hold at once.
- `start` applies only to a **new** group: `Earliest`, `Latest` (default) or `TimestampMs(…)`.
- A delivery has `key()`, `payload()`, `headers()`, `header(…)`, `attempt()` (the attempt number).
- Acks are coalesced: every `ack_batch` acks (64, but at most half of `prefetch`) or after `ack_linger` (1 ms). `nack` goes out at once. Tune with `SubscribeOptions::default().ack_batch(128).ack_linger(Duration::from_millis(2))`.

## Ack modes {#ack-modes}

| Mode | How | What works | Choose when |
|---|---|---|---|
| `AckMode::Individual` (default) | — | per-message ack/nack, retries, ack timeout, DLQ | almost always |
| `AckMode::Cumulative` | `SubscribeOptions::default().cumulative()` | a committed position per partition; **no** nack, ack timeout or DLQ (`nack` returns `BadRequest`) | `stream` channels only; analytics and stream processing that need maximum speed |

In `Individual` mode consecutive acks travel as ranges (`AckRanges`) — acking a whole batch costs a few bytes. In `Cumulative` mode `delivery.ack()` commits everything up to and including that message in its partition, and `consumer.commit().await` commits everything received so far and waits for the broker. After a crash delivery resumes from the last committed position — everything after it is redelivered. Ack a message only after processing every earlier one in its partition.

```rust
let mut consumer = client
    .subscribe("clicks", "analytics", SubscribeOptions::default().prefetch(2000).cumulative())
    .await?;
while let Some(d) = consumer.next().await {
    let d = d?;
    process(&d).await?;
    d.ack().await?;   // commits the partition up to and including d
}
```

Other options: `prefetch_bytes(n)` — also bound unacknowledged data in bytes; `verify_crc(false)` — skip the batches' CRC-32C check (hardware-accelerated and nearly free); `raw_records(false)` — receive messages one by one, as in 0.6.

## Token, TLS and clusters {#auth-tls}

```rust
use std::time::Duration;
use fluxmq_client::{Client, ClientOptions, ReconnectPolicy, TlsOptions};

let tls = TlsOptions::new()
    .ca_file("certs/ca.pem")                                       // the broker's CA
    .identity_files("certs/client.pem", "certs/client-key.pem");   // for mTLS
let client = Client::connect(
    ClientOptions::with_seeds(["tls://n1:7420", "tls://n2:7420", "tls://n3:7420"], "order-api-1")
        .auth_token(std::env::var("FLUXMQ_TOKEN").unwrap())      // not needed with a certificate binding
        .tls(tls)
        .reconnect(ReconnectPolicy::default().backoff(Duration::from_millis(100), Duration::from_secs(5)))
        .publish_timeout(Duration::from_secs(30)),
).await?;
```

- **Several seeds**: the client loads cluster metadata, publishes straight to partition leaders and consumes from every leader node as one stream.
- **Reconnects** with exponential backoff and jitter; `client.state()` is the connection state (`Connecting` → `Connected` ⇄ `Reconnecting` → `Closed`).
- **Publish retries**: connection loss, timeouts, `NotLeader` and `Overloaded` are retried until `publish_timeout`; `BadRequest`, `Forbidden`, `Unauthorized` never.
- `tls://host:port` without `TlsOptions` uses TLS with the system certificate store (a public CA).

> [!WARNING]
> **A retried publish can create a duplicate**: the broker may have stored the batch while the answer was lost with the connection. Delivery is at-least-once — make handlers idempotent, for example by a business key.

## Version compatibility {#compat}

SDK 0.7 works with brokers 0.4–0.7: it offers protocol v4 and, against an older broker, switches straight to the version that broker speaks (remembered for 5 minutes). The API is the same everywhere; record batches, compression and cumulative acks pay off only with a 0.7+ broker.

Runnable examples: `cargo run -p fluxmq-client --example producer -- 127.0.0.1:7420 10` and `--example consumer -- 127.0.0.1:7420`.

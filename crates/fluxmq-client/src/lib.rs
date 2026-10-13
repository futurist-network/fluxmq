//! # FluxMQ Rust client
//!
//! Async (tokio) SDK for the FluxMQ broker's binary protocol
//! ([`fluxmq_proto::wire`]: `u32` big-endian length + postcard frames, default port 7420).
//!
//! * [`Client`] — a connection manager, cheap to clone: a bootstrap connection plus lazily
//!   opened connections to cluster partition leaders (`TCP_NODELAY`), each re-established
//!   automatically. Per connection a writer task coalesces queued frames into as few socket
//!   writes as possible; a reader task routes responses to waiting requests by `req_id` and
//!   deliveries to consumers by subscription id.
//! * [`Producer`] / [`MessageBuilder`] — publish with keys, headers and trace ids; the
//!   `flux-producer` header is set to the client name automatically (unless
//!   [`ClientOptions::stamp_producer`] is off).
//! * [`BatchingProducer`] — auto-batching with per-message futures, for high throughput. The
//!   linger is adaptive: a message goes out at once when its partition has nothing in flight.
//!   On v0.7+ brokers each batch is encoded once into a record batch (`PublishBatch`),
//!   optionally compressed ([`ClientOptions::compression`]), and stored by the broker as is.
//! * [`Consumer`] — a [`futures::Stream`] of [`Delivery`]s with `ack` / `nack`. Acks are
//!   coalesced (~1 ms) into one fire-and-forget frame. Processing time (receipt → ack/nack),
//!   the last nack reason and process CPU/memory are reported to the broker every 2 s, which
//!   powers the Consumers view of the UI.
//!
//! ## Producing
//!
//! ```no_run
//! use fluxmq_client::{Client, ClientOptions, message};
//! use fluxmq_proto::{ChannelConfig, ChannelMode};
//!
//! # async fn run() -> fluxmq_client::Result<()> {
//! let client = Client::connect(ClientOptions::new("127.0.0.1:7420", "order-api-1")).await?;
//! client.ensure_channel(ChannelConfig::new("orders.events", ChannelMode::Stream)).await?;
//!
//! let producer = client.producer("orders.events");
//! let id = producer
//!     .send(message(r#"{"order_id":"8842"}"#).key("8842").event_type("OrderCreated").new_trace_id())
//!     .await?;
//! println!("stored at partition {} offset {}", id.partition, id.offset);
//! # Ok(()) }
//! ```
//!
//! ## Consuming
//!
//! ```no_run
//! use fluxmq_client::{Client, ClientOptions, SubscribeOptions};
//! use futures::StreamExt;
//!
//! # async fn run() -> fluxmq_client::Result<()> {
//! let client = Client::connect(ClientOptions::new("127.0.0.1:7420", "order-processing-1")).await?;
//! let mut consumer = client
//!     .subscribe("orders.events", "order-processing", SubscribeOptions::default().prefetch(128))
//!     .await?;
//! while let Some(delivery) = consumer.next().await {
//!     let delivery = delivery?; // Err only for terminal errors (Forbidden, deleted, closed)
//!     match std::str::from_utf8(delivery.payload()) {
//!         Ok(_) => delivery.ack().await?,
//!         Err(_) => delivery.nack("Invalid schema", false).await?, // straight to DLQ
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! ## Fast path (protocol v4, brokers v0.7+): compression and ack modes
//!
//! The SDK negotiates the newest protocol the broker speaks (v4 → v3 → v2, cached per address)
//! and keeps the same API on every version.
//!
//! * **Record batches.** Producers send one pre-encoded records section per partition and
//!   batch; the broker appends it without per-message work (durability unchanged: the answer
//!   still comes after fsync / ISR replication). Consumers receive stored batches verbatim
//!   ([`SubscribeOptions::raw_records`], on by default) and decode them lazily; payloads are
//!   slices of the received frame. Each batch's CRC-32C is verified
//!   ([`SubscribeOptions::verify_crc`], on by default; measured cost is within noise).
//! * **Compression** ([`ClientOptions::compression`]: [`Compression::None`] (default),
//!   `Lz4`, `Zstd`). Per batch, done once by the producer; stored, replicated and delivered
//!   compressed. Trade-off: less network, disk and replication traffic for producer CPU and
//!   consumer decompression. `Lz4` is the cheap choice for JSON-like payloads, `Zstd` compresses
//!   better at more CPU. Batches that don't shrink are sent uncompressed. Payloads that are
//!   already compressed or encrypted gain nothing.
//! * **Ack modes** ([`SubscribeOptions::ack_mode`]).
//!   [`AckMode::Individual`] (default): every message is acked or nacked on its own, with
//!   per-message redelivery, ack timeouts, nack and DLQ. In-order acks travel as id ranges
//!   (`AckRanges`), so acking a whole batch costs a few bytes.
//!   [`AckMode::Cumulative`] (stream channels): [`Delivery::ack`] commits the position up to and
//!   including that message in its partition, and [`Consumer::commit`] commits everything
//!   received so far and waits for the broker. It is the cheapest mode, but nack, ack timeouts
//!   and the DLQ don't apply, and after a crash or rebalance delivery resumes from the last
//!   committed position. That is still at-least-once, but every message after it is
//!   redelivered. Ack only after processing everything before it in the partition.
//!
//! ```no_run
//! use fluxmq_client::{AckMode, Client, ClientOptions, Compression, SubscribeOptions};
//!
//! # async fn run() -> fluxmq_client::Result<()> {
//! let client = Client::connect(
//!     ClientOptions::new("127.0.0.1:7420", "ingest-1").compression(Compression::Lz4),
//! )
//! .await?;
//! let mut consumer = client
//!     .subscribe("clicks", "analytics", SubscribeOptions::default().prefetch(2000).cumulative())
//!     .await?;
//! while let Some(d) = consumer.next().await {
//!     let d = d?;
//!     // ... process ...
//!     d.ack().await?; // commits this partition up to and including d
//! }
//! # Ok(()) }
//! ```
//!
//! ## Errors, reconnect and clusters
//!
//! Every call returns [`ClientError`]: `Io`, `Protocol`, `Server(WireError)`, `Timeout`
//! (per-request deadline, [`ClientOptions::request_timeout`]) or `Closed`.
//!
//! * **Reconnect.** Lost connections are re-established automatically with exponential backoff
//!   and jitter ([`ReconnectPolicy`]); [`Client::state`] is a watch of [`ConnectionState`]
//!   (`Connecting` → `Connected` ⇄ `Reconnecting` → `Closed`). Requests in flight on a dropped
//!   connection fail with `Closed`, except publishes, which are retried (below).
//! * **Seeds and clusters.** [`ClientOptions::seeds`] lists bootstrap addresses. The client
//!   fetches cluster metadata, partitions messages client-side exactly like the broker
//!   (`crc32(key) % partitions`, round-robin without a key), publishes straight to partition
//!   leaders and subscribes on every node that leads a partition of the channel. Brokers
//!   without cluster support answer `Metadata` with `BadRequest`; the client then works in
//!   single-node mode (everything via one connection, broker-side partitioning).
//! * **Publish retries.** Connection loss, request timeouts, `NotLeader` and `Overloaded` are
//!   retried, with a metadata refresh in between, until [`ClientOptions::publish_timeout`].
//!   `BadRequest`, `Forbidden`, `Unauthorized` and `NotFound` are never retried (in a cluster
//!   `NotFound` is retried while the metadata still lists the channel). **A retry can duplicate
//!   a message** (the broker may have stored it before the connection dropped): delivery is
//!   at-least-once end to end, so consumers should be idempotent.
//! * **Consumers** resubscribe transparently (same group) after reconnects and leadership
//!   changes. Deliveries buffered from the old subscription are dropped and acking a delivery
//!   handed out before is a no-op — the broker redelivers those. The stream yields `Err` and
//!   ends only on terminal errors (`Forbidden`, channel deleted, client closed / gave up).
//!
//! ```no_run
//! use std::time::Duration;
//! use fluxmq_client::{Client, ClientOptions, ConnectionState, ReconnectPolicy};
//!
//! # async fn run() -> fluxmq_client::Result<()> {
//! let opts = ClientOptions::with_seeds(["10.0.0.1:7420", "10.0.0.2:7420"], "order-api-1")
//!     .reconnect(ReconnectPolicy::default().backoff(Duration::from_millis(100), Duration::from_secs(5)))
//!     .publish_timeout(Duration::from_secs(30));
//! let client = Client::connect(opts).await?;
//! let mut state = client.state();
//! tokio::spawn(async move {
//!     while state.changed().await.is_ok() {
//!         let s: ConnectionState = *state.borrow();
//!         println!("fluxmq connection: {s:?}");
//!     }
//! });
//! # Ok(()) }
//! ```
//!
//! ## TLS
//!
//! [`ClientOptions::tls`] (or a `tls://host:port` address, which uses the OS trust store)
//! wraps every connection — seeds, reconnects and cluster nodes learned from metadata — in TLS
//! (rustls, aws-lc-rs). A CA file pins a private broker CA; a client certificate enables mTLS.
//! The wire protocol inside is unchanged.
//!
//! ```no_run
//! use fluxmq_client::{Client, ClientOptions, TlsOptions};
//!
//! # async fn run() -> fluxmq_client::Result<()> {
//! let tls = TlsOptions::new()
//!     .ca_file("certs/ca.pem")
//!     .identity_files("certs/client.pem", "certs/client-key.pem");
//! let client = Client::connect(
//!     ClientOptions::with_seeds(["tls://n1:7420", "tls://n2:7420"], "order-api-1").tls(tls),
//! )
//! .await?;
//! # Ok(()) }
//! ```

mod acks;
mod client;
mod conn;
mod consumer;
mod error;
mod metadata;
mod node;
mod options;
mod procstats;
mod producer;
mod records;
mod util;

pub use client::{Client, ConnectionState};
pub use conn::ServerInfo;
pub use consumer::{Consumer, Delivery, SubscribeOptions};
pub use error::{ClientError, Result};
pub use fluxmq_tls::PemSource;
pub use options::{ClientOptions, ReconnectPolicy, TLS_SCHEME, TlsOptions};
pub use producer::{
    BatchingOptions, BatchingProducer, MessageBuilder, Producer, PublishFuture, message,
    new_trace_id,
};

pub use fluxmq_proto;
pub use fluxmq_proto::batch::Compression;
pub use fluxmq_proto::cluster::ClusterMetadata;
pub use fluxmq_proto::wire::{AckMode, ErrorCode, WireError};
pub use fluxmq_proto::{ChannelConfig, ChannelMode, MessageId, PublishMessage, StartFrom};

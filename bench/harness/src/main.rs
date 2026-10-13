//! `mq-harness`: one load / latency harness for FluxMQ, Apache Kafka and RabbitMQ.
//!
//! Every system is driven by the same engine, so the numbers differ only by the broker and
//! its client library:
//!
//! * `--producers` producer connections, each sending messages one by one through the
//!   system's asynchronous publish API with at most `--window` unacknowledged messages
//!   (FluxMQ `BatchingProducer`, librdkafka `FutureProducer`, lapin publisher confirms).
//!   Client-side batching is the library's own: FluxMQ and Kafka coalesce for up to
//!   `--linger-ms`; AMQP has no producer batching (lapin pipelines frames).
//! * `--consumers` consumer connections in one fresh group / over the run's queues,
//!   acknowledging every message (FluxMQ ack, AMQP basic.ack, Kafka auto-commit of offsets).
//! * The first 8 bytes of every payload carry the (intended) send time in µs on the harness's
//!   monotonic clock; producers and consumers live in this process, so end-to-end latency =
//!   receive time − send time with no clock skew.
//! * `--rate 0` is a closed loop (saturation). `--rate R` is an open loop: sends are scheduled
//!   at fixed intervals and latency is measured from the scheduled time (coordinated-omission
//!   corrected). A run that cannot keep up is reported as not sustained.
//! * `--warmup` seconds are discarded, then `--duration` seconds are measured; afterwards the
//!   consumers get `--drain` seconds to receive everything sent in the window.
//! * Every payload also carries its producer id and a per-producer sequence number, so the
//!   drain check is by message id, not by count: every message *acknowledged* by the broker and
//!   sent in the window must be received (duplicates count once). On a miss the harness keeps
//!   waiting up to `--late-drain` more seconds (past the brokers' redelivery timeouts) and
//!   reports each missed message as `late` (arrived eventually) or `missing`, with the missing
//!   ids; FluxMQ additionally re-reads the channel with a fresh group (`audit`) to tell
//!   "in the log but not delivered" from "not in the log". The channel/topic is deleted only
//!   after all of that.
//!
//! The result is one JSON document (`--out`), see `bench/README.md`.

mod fluxmq_sys;
#[cfg(feature = "kafka")]
mod kafka_sys;
#[cfg(feature = "rabbitmq")]
mod rabbit_sys;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use clap::{Parser, ValueEnum};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum System {
    Fluxmq,
    Kafka,
    Rabbitmq,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueueType {
    Classic,
    Quorum,
}

#[derive(Debug, Clone, Parser, Serialize)]
#[command(
    name = "mq-harness",
    about = "Common FluxMQ / Kafka / RabbitMQ load and latency harness"
)]
pub struct Args {
    /// System under test.
    #[arg(long, value_enum)]
    pub system: System,
    /// Broker addresses, comma-separated. FluxMQ: `host:port` or `tls://host:port` seeds;
    /// Kafka: bootstrap `host:port`; RabbitMQ: `amqp://user:pass@host:5672/%2f` URIs.
    #[arg(long, value_delimiter = ',', required = true)]
    pub brokers: Vec<String>,
    /// Free-form label of the system variant (e.g. `fluxmq-cluster-tls`), copied to the JSON.
    #[arg(long, default_value = "")]
    pub label: String,
    /// Scenario name copied to the JSON (`saturation`, `latency`, ...).
    #[arg(long, default_value = "adhoc")]
    pub scenario: String,
    /// Run id copied to the JSON (the runner uses `<system>-<scenario>-<params>`).
    #[arg(long, default_value = "")]
    pub run_id: String,
    /// Payload size in bytes (>= 16).
    #[arg(long, default_value_t = 1024)]
    pub size: usize,
    #[arg(long, default_value_t = 4)]
    pub producers: usize,
    #[arg(long, default_value_t = 4)]
    pub consumers: usize,
    /// Partitions (FluxMQ channel / Kafka topic) or number of queues (RabbitMQ).
    #[arg(long, default_value_t = 8)]
    pub partitions: u32,
    /// Replication factor for the Kafka topic (FluxMQ takes it from the broker's
    /// `--replication-factor`, RabbitMQ quorum queues from the cluster size).
    #[arg(long, default_value_t = 1)]
    pub replication: u32,
    /// Kafka topic `min.insync.replicas` (only with replication > 1).
    #[arg(long, default_value_t = 2)]
    pub min_insync: u32,
    /// RabbitMQ queue type.
    #[arg(long, value_enum, default_value_t = QueueType::Classic)]
    pub queue_type: QueueType,
    /// Target total publish rate (msg/s); 0 = closed loop (as fast as the window allows).
    #[arg(long, default_value_t = 0)]
    pub rate: u64,
    /// Measured seconds.
    #[arg(long, default_value_t = 30)]
    pub duration: u64,
    /// Warm-up seconds before measuring (discarded).
    #[arg(long, default_value_t = 10)]
    pub warmup: u64,
    /// Max seconds to wait after the window for consumers to receive everything
    /// (`drained` in the JSON).
    #[arg(long, default_value_t = 15)]
    pub drain: u64,
    /// After a missed `--drain`: keep waiting up to this many more seconds and classify every
    /// missed message as late or missing. Default 45 s, above the FluxMQ / AMQP default ack
    /// timeout (30 s), so an unacked delivery that is redelivered shows up as late.
    #[arg(long, default_value_t = 45)]
    pub late_drain: u64,
    /// At most this many missing ids listed in the JSON (they are also summarized as ranges).
    #[arg(long, default_value_t = 1000)]
    pub dump_missing: usize,
    /// Harness self-test (FluxMQ only): consumer 0 drops its 1000th delivery without acking
    /// (redelivered after the ack timeout: must be reported late) and dead-letters its 2000th
    /// (never redelivered: must be reported missing, and found by the audit).
    #[arg(long, hide = true)]
    pub self_test_faults: bool,
    /// Max unacknowledged publishes per producer connection.
    #[arg(long, default_value_t = 20000)]
    pub window: usize,
    /// FluxMQ `BatchingOptions::max_in_flight` (Publish requests pipelined per producer).
    #[arg(long, default_value_t = 32)]
    pub max_in_flight: usize,
    /// Client-side batching linger (FluxMQ `BatchingOptions::linger`, Kafka `linger.ms`).
    #[arg(long, default_value_t = 5)]
    pub linger_ms: u64,
    /// Consumer prefetch (FluxMQ prefetch, AMQP basic.qos).
    #[arg(long, default_value_t = 2000)]
    pub prefetch: u32,
    /// Retention cap per partition in bytes for the run's topic/channel (keeps disk bounded).
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    pub retention_bytes: u64,
    /// Log segment size in bytes: Kafka topic `segment.bytes`. FluxMQ's is broker-wide
    /// (`FLUXMQ_SEGMENT_BYTES`, set by the runner); the value is recorded in the JSON.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub segment_bytes: u64,
    /// FluxMQ broker fsync mode, informational (it is a broker setting): `group-commit` or `os`.
    #[arg(long, default_value = "group-commit")]
    pub fluxmq_fsync: String,
    /// FluxMQ v0.7 producer compression (`ClientOptions::compression`): none | lz4 | zstd.
    /// Per batch, on protocol v4 brokers; older brokers ignore it.
    #[arg(long, default_value = "none")]
    pub fluxmq_compression: String,
    /// FluxMQ v0.7 consumer ack mode (`SubscribeOptions::ack_mode`): individual (default,
    /// every message acked) | cumulative (position commits; stream channels).
    #[arg(long, default_value = "individual")]
    pub fluxmq_ack_mode: String,
    /// FluxMQ v0.7: receive stored batches verbatim (`SubscribeOptions::raw_records`).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub fluxmq_raw_records: bool,
    /// FluxMQ v0.7: verify each delivered batch's CRC-32C (`SubscribeOptions::verify_crc`).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub fluxmq_verify_crc: bool,
    /// Catch-up scenario: publish this many messages first with no consumer reading (closed
    /// loop), then start the consumers and measure consume-only throughput until every
    /// preloaded message was received (bounded by `--drain`). 0 = normal run.
    #[arg(long, default_value_t = 0)]
    pub preload: u64,
    /// FluxMQ client TLS: CA bundle (enables TLS).
    #[arg(long, env = "FLUXMQ_CLIENT_TLS_CA")]
    pub tls_ca: Option<std::path::PathBuf>,
    /// FluxMQ client TLS: client certificate (mTLS).
    #[arg(long, env = "FLUXMQ_CLIENT_TLS_CERT")]
    pub tls_cert: Option<std::path::PathBuf>,
    /// FluxMQ client TLS: client key (mTLS).
    #[arg(long, env = "FLUXMQ_CLIENT_TLS_KEY")]
    pub tls_key: Option<std::path::PathBuf>,
    /// Kafka over TLS: CA bundle that signs the brokers' certificate (librdkafka
    /// `security.protocol=ssl`, `ssl.ca.location`; hostname verification stays on).
    #[arg(long)]
    pub kafka_tls_ca: Option<std::path::PathBuf>,
    /// Keep the topic / channel / queues after the run (default: delete them).
    #[arg(long)]
    pub keep: bool,
    /// Output JSON path (stdout when omitted).
    #[arg(long)]
    pub out: Option<std::path::PathBuf>,
}

// ---------------------------------------------------------------------------------------
// Message ids

/// Payload layout: `[0..8)` send time µs, `[8..12)` producer id, `[12..20)` sequence number.
pub const ID_BYTES: usize = 20;

/// `(producer, sequence)` of a harness payload.
pub fn payload_id(payload: &[u8]) -> Option<(u32, u64)> {
    if payload.len() < ID_BYTES {
        return None;
    }
    let p = u32::from_be_bytes(payload[8..12].try_into().ok()?);
    let k = u64::from_be_bytes(payload[12..20].try_into().ok()?);
    Some((p, k))
}

/// Growable bitset of sequence numbers.
#[derive(Default, Clone)]
pub struct Bits(Vec<u64>);

impl Bits {
    /// Sets bit `i`; true if it was not set before.
    pub fn insert(&mut self, i: u64) -> bool {
        let w = (i / 64) as usize;
        if w >= self.0.len() {
            self.0.resize(w + 1, 0);
        }
        let m = 1u64 << (i % 64);
        let fresh = self.0[w] & m == 0;
        self.0[w] |= m;
        fresh
    }

    pub fn contains(&self, i: u64) -> bool {
        self.0
            .get((i / 64) as usize)
            .is_some_and(|w| w & (1u64 << (i % 64)) != 0)
    }

    pub fn count(&self) -> u64 {
        self.0.iter().map(|w| u64::from(w.count_ones())).sum()
    }

    /// `self ∖ other`.
    pub fn minus(&self, other: &Bits) -> Bits {
        Bits(
            self.0
                .iter()
                .enumerate()
                .map(|(i, w)| w & !other.0.get(i).copied().unwrap_or(0))
                .collect(),
        )
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.iter().enumerate().flat_map(|(i, &w)| {
            (0..64)
                .filter(move |b| w & (1u64 << b) != 0)
                .map(move |b| i as u64 * 64 + b)
        })
    }
}

/// Compact `[first, last]` runs of a bitset (for the JSON), at most `max` runs (the last
/// one then ends with the highest id, so the overall span stays visible).
fn ranges(b: &Bits, max: usize) -> Vec<[u64; 2]> {
    let mut out: Vec<[u64; 2]> = Vec::new();
    for i in b.iter() {
        let full = out.len() >= max.max(1);
        match out.last_mut() {
            Some(r) if r[1] + 1 == i || full => r[1] = i,
            _ => out.push([i, i]),
        }
    }
    out
}

/// Result of a system's audit hook: given the missing `(producer, seq)` ids, re-reads the
/// stored data and reports where (and whether) they are.
/// Gets the missing ids as one bitset of sequence numbers per producer (compact even for tens
/// of millions of ids).
pub type Audit = Box<dyn FnOnce(Vec<Bits>) -> BoxFuture<'static, serde_json::Value> + Send>;

// ---------------------------------------------------------------------------------------
// Shared measurement state

pub fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 600_000_000, 3).expect("valid bounds")
}

pub struct Shared {
    base: Instant,
    /// Window bounds in µs on `base` (u64::MAX = not yet).
    from_us: AtomicU64,
    until_us: AtomicU64,
    pub stop: AtomicBool,
    /// Messages handed to the client (all producers), for `--preload`.
    pub_sent_total: AtomicU64,
    /// First send of any producer / first receipt of any consumer (µs on `base`; MAX = none).
    first_send_us: AtomicU64,
    first_recv_us: AtomicU64,
    /// `--preload` limit (0 = none).
    preload: u64,
    /// Consumers start reading once this is true (immediately unless `--preload`).
    gate: tokio::sync::watch::Sender<bool>,
    pub_acked_total: AtomicU64,
    pub_acked_window: AtomicU64,
    pub_sent_in_window: AtomicU64,
    con_total: AtomicU64,
    con_window: AtomicU64,
    con_sent_in_window: AtomicU64,
    /// Receipts of a window message already received before (redelivery / duplicate).
    duplicates: AtomicU64,
    /// Deliveries the broker flagged as a redelivery (FluxMQ attempt > 1, AMQP redelivered).
    redelivered: AtomicU64,
    /// Per producer: sequence numbers sent in the window and acknowledged by the broker.
    acked: Vec<Mutex<Bits>>,
    /// Per producer: sequence numbers sent in the window and received (at least once).
    received: Vec<Mutex<Bits>>,
    /// Set after a missed drain: first receipts of window messages are logged in `late_log`
    /// (with the system's delivery metadata: partition, offset, attempt, node).
    late_phase: AtomicBool,
    late_log: Mutex<Vec<serde_json::Value>>,
    errors: AtomicU64,
    last_error: Mutex<Option<String>>,
    e2e: Mutex<Vec<Arc<Mutex<Histogram<u64>>>>>,
    publish: Mutex<Vec<Arc<Mutex<Histogram<u64>>>>>,
    size: usize,
}

impl Shared {
    fn new(size: usize, producers: usize, preload: u64) -> Self {
        Self {
            pub_sent_total: AtomicU64::new(0),
            first_send_us: AtomicU64::new(u64::MAX),
            first_recv_us: AtomicU64::new(u64::MAX),
            preload,
            gate: tokio::sync::watch::channel(preload == 0).0,
            base: Instant::now(),
            from_us: AtomicU64::new(u64::MAX),
            until_us: AtomicU64::new(u64::MAX),
            stop: AtomicBool::new(false),
            pub_acked_total: AtomicU64::new(0),
            pub_acked_window: AtomicU64::new(0),
            pub_sent_in_window: AtomicU64::new(0),
            con_total: AtomicU64::new(0),
            con_window: AtomicU64::new(0),
            con_sent_in_window: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            redelivered: AtomicU64::new(0),
            acked: (0..producers)
                .map(|_| Mutex::new(Bits::default()))
                .collect(),
            received: (0..producers)
                .map(|_| Mutex::new(Bits::default()))
                .collect(),
            late_phase: AtomicBool::new(false),
            late_log: Mutex::new(Vec::new()),
            errors: AtomicU64::new(0),
            last_error: Mutex::new(None),
            e2e: Mutex::new(Vec::new()),
            publish: Mutex::new(Vec::new()),
            size,
        }
    }

    /// Consumers call this before subscribing: returns at once, or (catch-up scenario) when
    /// the preload is complete.
    pub async fn consume_gate(&self) {
        let mut rx = self.gate.subscribe();
        let _ = rx.wait_for(|open| *open).await;
    }

    pub fn now_us(&self) -> u64 {
        self.base.elapsed().as_micros() as u64
    }

    fn in_window(&self, t: u64) -> bool {
        t >= self.from_us.load(Ordering::Relaxed) && t < self.until_us.load(Ordering::Relaxed)
    }

    pub fn error(&self, e: impl std::fmt::Display) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let mut last = self.last_error.lock();
        if last.is_none() || self.errors.load(Ordering::Relaxed) % 1000 == 1 {
            *last = Some(e.to_string());
        }
    }

    pub fn payload(&self, sent_us: u64, producer: u32, seq: u64) -> Vec<u8> {
        let mut v = vec![0xABu8; self.size];
        v[..8].copy_from_slice(&sent_us.to_be_bytes());
        v[8..12].copy_from_slice(&producer.to_be_bytes());
        v[12..20].copy_from_slice(&seq.to_be_bytes());
        v
    }

    /// Per producer: acknowledged window messages not received yet.
    fn missing(&self) -> Vec<Bits> {
        self.acked
            .iter()
            .zip(&self.received)
            .map(|(a, r)| {
                let a = a.lock().clone();
                a.minus(&r.lock())
            })
            .collect()
    }

    fn missing_count(&self) -> u64 {
        self.acked
            .iter()
            .zip(&self.received)
            .map(|(a, r)| a.lock().minus(&r.lock()).count())
            .sum()
    }

    /// A consumer-side recorder (one histogram per consumer task, no contention).
    pub fn recorder(self: &Arc<Self>) -> Recorder {
        let h = Arc::new(Mutex::new(hist()));
        self.e2e.lock().push(h.clone());
        Recorder {
            shared: self.clone(),
            hist: h,
        }
    }
}

pub struct Recorder {
    shared: Arc<Shared>,
    hist: Arc<Mutex<Histogram<u64>>>,
}

impl Recorder {
    /// Records one received message.
    pub fn record(&self, payload: &[u8]) {
        self.record_ext(payload, || serde_json::Value::Null);
    }

    /// [`Recorder::record`] plus delivery metadata, logged for messages that arrive late.
    pub fn record_ext(&self, payload: &[u8], meta: impl FnOnce() -> serde_json::Value) {
        let s = &self.shared;
        let now = s.now_us();
        s.first_recv_us.fetch_min(now, Ordering::Relaxed);
        s.con_total.fetch_add(1, Ordering::Relaxed);
        if s.in_window(now) {
            s.con_window.fetch_add(1, Ordering::Relaxed);
        }
        if payload.len() >= 8 {
            let sent = u64::from_be_bytes(payload[..8].try_into().expect("8 bytes"));
            if s.in_window(sent) {
                let fresh = match payload_id(payload) {
                    Some((p, k)) if (p as usize) < s.received.len() => {
                        s.received[p as usize].lock().insert(k)
                    }
                    _ => true,
                };
                if fresh {
                    if s.late_phase.load(Ordering::Relaxed) {
                        let mut log = s.late_log.lock();
                        if log.len() < 1000 {
                            let (p, k) = payload_id(payload).unwrap_or((u32::MAX, 0));
                            log.push(json!({
                                "t_ms": now / 1000, "producer": p, "seq": k,
                                "sent_ms": sent / 1000, "meta": meta(),
                            }));
                        }
                    }
                    s.con_sent_in_window.fetch_add(1, Ordering::Relaxed);
                    self.hist
                        .lock()
                        .saturating_record(now.saturating_sub(sent).max(1));
                } else {
                    s.duplicates.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// The broker marked a delivery as redelivered.
    pub fn redelivered(&self) {
        self.shared.redelivered.fetch_add(1, Ordering::Relaxed);
    }
}

/// Enqueues one message; resolves once it is accepted by the client library, yielding a
/// future that resolves on the broker's acknowledgement.
pub type Enqueue = BoxFuture<'static, anyhow::Result<BoxFuture<'static, anyhow::Result<()>>>>;

/// System-independent producer loop.
pub async fn producer_loop<F>(shared: Arc<Shared>, args: Args, id: u32, mut enqueue: F)
where
    F: FnMut(Vec<u8>, u64) -> Enqueue,
{
    let hist = Arc::new(Mutex::new(hist()));
    shared.publish.lock().push(hist.clone());
    let mut inflight = FuturesUnordered::new();
    let per_producer_rate = args.rate as f64 / args.producers as f64;
    let interval_us = (args.rate > 0).then(|| 1e6 / per_producer_rate);
    let start_us = shared.now_us();
    // Stagger producers so open-loop sends don't fire in lockstep.
    let offset_us = interval_us
        .map(|iv| iv * id as f64 / args.producers as f64)
        .unwrap_or(0.0);
    let mut k: u64 = 0;
    let window = args.window.max(1);

    let on_done = |(sent, seq): (u64, u64), r: anyhow::Result<()>| {
        let now = shared.now_us();
        match r {
            Ok(()) => {
                shared.pub_acked_total.fetch_add(1, Ordering::Relaxed);
                if shared.in_window(now) {
                    shared.pub_acked_window.fetch_add(1, Ordering::Relaxed);
                }
                if shared.in_window(sent) {
                    shared.pub_sent_in_window.fetch_add(1, Ordering::Relaxed);
                    if let Some(a) = shared.acked.get(id as usize) {
                        a.lock().insert(seq);
                    }
                    hist.lock()
                        .saturating_record(now.saturating_sub(sent).max(1));
                }
            }
            Err(e) => shared.error(format!("publish: {e:#}")),
        }
    };

    while !shared.stop.load(Ordering::Relaxed) {
        let sent = match interval_us {
            Some(iv) => {
                let due = start_us + (offset_us + iv * k as f64) as u64;
                let now = shared.now_us();
                if due > now {
                    let wake = tokio::time::Instant::now() + Duration::from_micros(due - now);
                    tokio::select! {
                        _ = tokio::time::sleep_until(wake) => {}
                        Some((s, r)) = inflight.next(), if !inflight.is_empty() => {
                            on_done(s, r);
                            continue;
                        }
                    }
                }
                due
            }
            None => shared.now_us(),
        };
        while inflight.len() >= window {
            match inflight.next().await {
                Some((s, r)) => on_done(s, r),
                None => break,
            }
        }
        if shared.stop.load(Ordering::Relaxed) {
            break;
        }
        if shared.preload > 0 && shared.pub_sent_total.fetch_add(1, Ordering::Relaxed) >= shared.preload {
            break;
        }
        shared.first_send_us.fetch_min(sent, Ordering::Relaxed);
        let payload = shared.payload(sent, id, k);
        match enqueue(payload, k).await {
            Ok(ack) => inflight.push(async move { ((sent, k), ack.await) }),
            Err(e) => {
                shared.error(format!("enqueue: {e:#}"));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        k += 1;
        // Closed loop with a huge window: yield now and then so acks get polled.
        if interval_us.is_none() && k.is_multiple_of(256) {
            while let Some(Some((s, r))) = futures::FutureExt::now_or_never(inflight.next()) {
                on_done(s, r);
            }
        }
    }
    // Collect outstanding acks (bounded by --drain).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.drain.max(1));
    // (With --preload the producers stop on their own; their acks are awaited the same way.)
    while !inflight.is_empty() {
        match tokio::time::timeout_at(deadline, inflight.next()).await {
            Ok(Some((s, r))) => on_done(s, r),
            Ok(None) => break,
            Err(_) => {
                shared.error(format!(
                    "{} publishes unacknowledged at shutdown",
                    inflight.len()
                ));
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------------------

pub(crate) fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn pct(h: &Histogram<u64>) -> serde_json::Value {
    if h.is_empty() {
        return serde_json::Value::Null;
    }
    let ms = |v: u64| (v as f64 / 1000.0 * 1000.0).round() / 1000.0;
    json!({
        "count": h.len(),
        "min": ms(h.min()),
        "mean": ms(h.mean() as u64),
        "p50": ms(h.value_at_quantile(0.50)),
        "p90": ms(h.value_at_quantile(0.90)),
        "p99": ms(h.value_at_quantile(0.99)),
        "p999": ms(h.value_at_quantile(0.999)),
        "max": ms(h.max()),
    })
}

fn merged(list: &Mutex<Vec<Arc<Mutex<Histogram<u64>>>>>) -> Histogram<u64> {
    let mut total = hist();
    for h in list.lock().iter() {
        let _ = total.add(&*h.lock());
    }
    total
}

/// What each system module returns from `setup`.
pub struct Started {
    pub producers: Vec<tokio::task::JoinHandle<()>>,
    pub consumers: Vec<tokio::task::JoinHandle<()>>,
    pub durability: String,
    pub client: String,
    pub broker_version: Option<String>,
    pub cleanup: Option<BoxFuture<'static, anyhow::Result<()>>>,
    /// Re-reads the stored data for missing ids (FluxMQ only).
    pub audit: Option<Audit>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.size >= ID_BYTES, "--size must be >= {ID_BYTES}");
    anyhow::ensure!(args.producers > 0, "--producers must be > 0");
    let topic = format!(
        "bench-{}-{}",
        if args.run_id.is_empty() { "adhoc" } else { "r" },
        epoch_ms() % 100_000_000
    );
    let shared = Arc::new(Shared::new(args.size, args.producers, args.preload));
    if args.preload > 0 {
        // Catch-up: every preloaded message belongs to the "window" (tracked by id).
        shared.from_us.store(0, Ordering::Relaxed);
    }
    let started_at = epoch_ms();
    eprintln!(
        "mq-harness: {:?} [{}] {} scenario={} size={} producers={} consumers={} rate={} warmup={}s duration={}s topic={}",
        args.system,
        args.label,
        args.brokers.join(","),
        args.scenario,
        args.size,
        args.producers,
        args.consumers,
        args.rate,
        args.warmup,
        args.duration,
        topic
    );

    let started = match args.system {
        System::Fluxmq => fluxmq_sys::start(&args, &topic, shared.clone()).await,
        #[cfg(feature = "kafka")]
        System::Kafka => kafka_sys::start(&args, &topic, shared.clone()).await,
        #[cfg(feature = "rabbitmq")]
        System::Rabbitmq => rabbit_sys::start(&args, &topic, shared.clone()).await,
        #[allow(unreachable_patterns)]
        other => Err(anyhow::anyhow!(
            "{other:?} support not compiled in (cargo features `kafka` / `rabbitmq`)"
        )),
    }
    .context("setup")?;

    // Timeline sampler.
    let timeline = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let sampler = {
        let shared = shared.clone();
        let timeline = timeline.clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            let mut last = (0u64, 0u64);
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.tick().await;
            loop {
                tick.tick().await;
                let cur = (
                    shared.pub_acked_total.load(Ordering::Relaxed),
                    shared.con_total.load(Ordering::Relaxed),
                );
                let phase = if shared.from_us.load(Ordering::Relaxed) == u64::MAX {
                    "warmup"
                } else if shared.until_us.load(Ordering::Relaxed) == u64::MAX {
                    "measure"
                } else {
                    "drain"
                };
                timeline.lock().push(json!({
                    "t": t0.elapsed().as_secs(),
                    "phase": phase,
                    "pub": cur.0 - last.0,
                    "con": cur.1 - last.1,
                    // Consumer lag: acknowledged publishes not consumed yet (all producers).
                    "lag": cur.0.saturating_sub(cur.1),
                }));
                last = cur;
            }
        })
    };

    let lag_now = |s: &Shared| {
        s.pub_acked_total
            .load(Ordering::Relaxed)
            .saturating_sub(s.con_total.load(Ordering::Relaxed))
    };
    let (measure_start_ms, measure_end_ms, until, window_s, lag_start, lag_end);
    let mut preload_doc = serde_json::Value::Null;
    if args.preload > 0 {
        // Catch-up: preload with no consumer reading, then open the gate; the drain below
        // measures consume-only throughput.
        eprintln!("mq-harness: preloading {} messages", args.preload);
        measure_start_ms = epoch_ms();
        let t = Instant::now();
        for p in started.producers {
            let _ = p.await;
        }
        until = shared.now_us();
        shared.until_us.store(until, Ordering::Relaxed);
        shared.stop.store(true, Ordering::Relaxed);
        let _ = t;
        // From the first send (producers may start during setup) to the last ack.
        let first = shared.first_send_us.load(Ordering::Relaxed).min(until);
        let preload_s = ((until - first) as f64 / 1e6).max(1e-3);
        let acked = shared.pub_sent_in_window.load(Ordering::Relaxed);
        preload_doc = json!({
            "messages": args.preload, "acked": acked,
            "bytes": acked * args.size as u64,
            "seconds": (preload_s * 1000.0).round() / 1000.0,
            "publish_rate": (acked as f64 / preload_s).round(),
        });
        eprintln!("mq-harness: preloaded {acked} in {preload_s:.1}s; starting consumers");
        lag_start = lag_now(&shared);
        let _ = shared.gate.send(true);
        measure_end_ms = epoch_ms();
        window_s = preload_s;
        lag_end = lag_start;
    } else {
        tokio::time::sleep(Duration::from_secs(args.warmup)).await;
        measure_start_ms = epoch_ms();
        let from = shared.now_us();
        shared.from_us.store(from, Ordering::Relaxed);
        lag_start = lag_now(&shared);
        eprintln!("mq-harness: measuring for {}s", args.duration);
        tokio::time::sleep(Duration::from_secs(args.duration)).await;
        until = shared.now_us();
        shared.until_us.store(until, Ordering::Relaxed);
        lag_end = lag_now(&shared);
        measure_end_ms = epoch_ms();
        window_s = (until - from) as f64 / 1e6;
        shared.stop.store(true, Ordering::Relaxed);
        for p in started.producers {
            let _ = p.await;
        }
    }
    // Drain: wait until every message sent in the window and acknowledged by the broker was
    // received, checked by message id.
    let drain_start = Instant::now();
    let deadline = drain_start + Duration::from_secs(args.drain);
    let mut drained = args.consumers == 0;
    let mut drain_s = None;
    while args.consumers > 0 && !drained {
        // Cheap count check first (necessary condition); the id check only once it holds.
        if shared.con_sent_in_window.load(Ordering::Relaxed)
            >= shared.pub_sent_in_window.load(Ordering::Relaxed)
            && shared.missing_count() == 0
        {
            drained = true;
            drain_s = Some(drain_start.elapsed().as_secs_f64());
        } else if Instant::now() >= deadline {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    // A miss: keep the consumers running up to --late-drain more seconds and classify.
    let mut miss = serde_json::Value::Null;
    if !drained && args.consumers > 0 {
        let at_deadline = shared.missing();
        let missed_at_drain: u64 = at_deadline.iter().map(Bits::count).sum();
        eprintln!(
            "mq-harness: {missed_at_drain} acknowledged message(s) not received after {}s; \
             waiting up to {}s more",
            args.drain, args.late_drain
        );
        shared.late_phase.store(true, Ordering::Relaxed);
        let late_deadline = Instant::now() + Duration::from_secs(args.late_drain);
        let mut late_complete_s = None;
        let mut timeline_late = Vec::new();
        let mut last_tick = Instant::now();
        loop {
            let left = shared.missing_count();
            if last_tick.elapsed() >= Duration::from_secs(1) {
                timeline_late.push(json!([drain_start.elapsed().as_secs(), left]));
                last_tick = Instant::now();
            }
            if left == 0 {
                late_complete_s = Some(drain_start.elapsed().as_secs_f64());
                break;
            }
            if Instant::now() >= late_deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let still = shared.missing();
        let missing_n: u64 = still.iter().map(Bits::count).sum();
        let mut missing_ids = Vec::new();
        let mut missing_ranges = serde_json::Map::new();
        for (p, b) in still.iter().enumerate() {
            if b.count() > 0 {
                missing_ranges.insert(p.to_string(), json!(ranges(b, args.dump_missing)));
            }
            for k in b
                .iter()
                .take(args.dump_missing.saturating_sub(missing_ids.len()))
            {
                missing_ids.push((p as u32, k));
            }
        }
        let audit = match (started.audit, missing_n == 0 || args.late_drain == 0) {
            (Some(a), false) => {
                eprintln!("mq-harness: auditing {missing_n} missing id(s) in the stored data");
                a(still.clone()).await
            }
            _ => serde_json::Value::Null,
        };
        let mut at_ranges = serde_json::Map::new();
        for (p, b) in at_deadline.iter().enumerate() {
            if b.count() > 0 {
                at_ranges.insert(p.to_string(), json!(ranges(b, args.dump_missing)));
            }
        }
        miss = json!({
            "missed_at_drain": missed_at_drain,
            "missed_at_drain_ranges": at_ranges,
            "late": missed_at_drain.saturating_sub(missing_n),
            "missing": missing_n,
            "late_drain_s": args.late_drain,
            "all_received_after_s": late_complete_s,
            "missing_left_per_s": timeline_late,
            "window_end_ms": until / 1000,
            "late_arrivals": std::mem::take(&mut *shared.late_log.lock()),
            "missing_ranges": missing_ranges,
            "missing_ids": missing_ids.iter().take(args.dump_missing)
                .map(|(p, k)| json!([p, k])).collect::<Vec<_>>(),
            "audit": audit,
        });
        eprintln!(
            "mq-harness: drain miss: {} late, {} missing ({})",
            missed_at_drain.saturating_sub(missing_n),
            missing_n,
            {
                let r = serde_json::to_string(&miss["missing_ranges"]).unwrap_or_default();
                if r.len() > 400 {
                    format!("{}... (see the JSON)", &r[..400])
                } else {
                    r
                }
            }
        );
    }
    sampler.abort();
    for c in &started.consumers {
        c.abort();
    }
    let cleanup_error = match (args.keep, started.cleanup) {
        (false, Some(c)) => match tokio::time::timeout(Duration::from_secs(30), c).await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("{e:#}")),
            Err(_) => Some("cleanup timed out".into()),
        },
        _ => None,
    };

    let (published, consumed, pub_rate, con_rate) = if args.preload > 0 {
        // Catch-up: consume rate over the time from opening the gate until everything was
        // received (or the drain deadline), publish rate over the preload.
        let p = shared.pub_sent_in_window.load(Ordering::Relaxed);
        let c = shared.con_sent_in_window.load(Ordering::Relaxed);
        let secs = drain_s.unwrap_or_else(|| drain_start.elapsed().as_secs_f64()).max(1e-3);
        if let Some(pd) = preload_doc.as_object_mut() {
            // Subscribe / group join time, and the rate once data flows (both reported; the
            // headline consume_rate includes the join, as an operator would see it).
            let gate_us = until;
            let first = shared.first_recv_us.load(Ordering::Relaxed);
            if first != u64::MAX && first >= gate_us {
                let join_s = (first - gate_us) as f64 / 1e6;
                pd.insert("first_receipt_s".into(), json!((join_s * 1000.0).round() / 1000.0));
                pd.insert("consume_rate_after_first".into(),
                          json!((c as f64 / (secs - join_s).max(1e-3)).round()));
            }
            pd.insert("catchup_s".into(), json!((secs * 1000.0).round() / 1000.0));
        }
        (p, c, p as f64 / window_s, c as f64 / secs)
    } else {
        let p = shared.pub_acked_window.load(Ordering::Relaxed);
        let c = shared.con_window.load(Ordering::Relaxed);
        (p, c, p as f64 / window_s, c as f64 / window_s)
    };
    let mb = |r: f64| (r * args.size as f64 / 1e6 * 100.0).round() / 100.0;
    let sustained = if args.rate == 0 {
        serde_json::Value::Null
    } else {
        let t = args.rate as f64;
        json!(pub_rate >= 0.95 * t && con_rate >= 0.95 * t && drained)
    };
    let e2e = merged(&shared.e2e);
    let publish = merged(&shared.publish);
    let doc = json!({
        "schema": 2,
        "kind": "harness",
        "run_id": args.run_id,
        "label": args.label,
        "system": args.system,
        "scenario": args.scenario,
        "topic": topic,
        "params": {
            "size": args.size,
            "producers": args.producers,
            "consumers": args.consumers,
            "partitions": args.partitions,
            "replication": args.replication,
            "queue_type": if args.system == System::Rabbitmq { json!(args.queue_type) } else { serde_json::Value::Null },
            "rate": args.rate,
            "warmup_s": args.warmup,
            "duration_s": args.duration,
            "window": args.window,
            "max_in_flight": if args.system == System::Fluxmq { json!(args.max_in_flight) } else { serde_json::Value::Null },
            "linger_ms": args.linger_ms,
            "prefetch": args.prefetch,
            "retention_bytes": args.retention_bytes,
            "segment_bytes": args.segment_bytes,
            "tls": args.tls_ca.is_some()
                || args.kafka_tls_ca.is_some()
                || args.brokers.iter().any(|b| b.starts_with("tls://")),
            "mtls": args.tls_cert.is_some(),
            "brokers": args.brokers,
            "preload": args.preload,
            "fluxmq_compression": if args.system == System::Fluxmq { json!(args.fluxmq_compression) } else { serde_json::Value::Null },
            "fluxmq_ack_mode": if args.system == System::Fluxmq { json!(args.fluxmq_ack_mode) } else { serde_json::Value::Null },
            "fluxmq_raw_records": if args.system == System::Fluxmq { json!(args.fluxmq_raw_records) } else { serde_json::Value::Null },
            "fluxmq_verify_crc": if args.system == System::Fluxmq { json!(args.fluxmq_verify_crc) } else { serde_json::Value::Null },
        },
        "durability": started.durability,
        "client": started.client,
        "broker_version": started.broker_version,
        "started_at_ms": started_at,
        "measure_start_ms": measure_start_ms,
        "measure_end_ms": measure_end_ms,
        "window_s": (window_s * 1000.0).round() / 1000.0,
        "published": published,
        "consumed": consumed,
        "publish_rate": pub_rate.round(),
        "consume_rate": con_rate.round(),
        "publish_mb_s": mb(pub_rate),
        "consume_mb_s": mb(con_rate),
        "sent_in_window": shared.pub_sent_in_window.load(Ordering::Relaxed),
        "received_of_window": shared.con_sent_in_window.load(Ordering::Relaxed),
        "drained": drained,
        "drain_s": drain_s.map(|s| (s * 1000.0).round() / 1000.0),
        "drain_miss": miss,
        "duplicates": shared.duplicates.load(Ordering::Relaxed),
        "redelivered": shared.redelivered.load(Ordering::Relaxed),
        "sustained": sustained,
        "preload": preload_doc,
        "lag_start": lag_start,
        "lag_end": lag_end,
        "lag_growth_per_s": if args.preload > 0 { serde_json::Value::Null } else {
            json!(((lag_end as f64 - lag_start as f64) / window_s).round())
        },
        "e2e_ms": pct(&e2e),
        "publish_ms": pct(&publish),
        "errors": shared.errors.load(Ordering::Relaxed),
        "last_error": shared.last_error.lock().clone(),
        "cleanup_error": cleanup_error,
        "timeline": *timeline.lock(),
    });
    let text = serde_json::to_string_pretty(&doc)?;
    match &args.out {
        Some(p) => {
            if let Some(dir) = p.parent() {
                std::fs::create_dir_all(dir).ok();
            }
            std::fs::write(p, &text).with_context(|| format!("write {}", p.display()))?
        }
        None => println!("{text}"),
    }
    eprintln!(
        "mq-harness: pub {:.0} msg/s ({:.1} MB/s), con {:.0} msg/s, e2e p50/p99/p999 = {}, drained={}, errors={}",
        pub_rate,
        mb(pub_rate),
        con_rate,
        if e2e.is_empty() {
            "-".to_string()
        } else {
            format!(
                "{:.2}/{:.2}/{:.2} ms",
                e2e.value_at_quantile(0.5) as f64 / 1000.0,
                e2e.value_at_quantile(0.99) as f64 / 1000.0,
                e2e.value_at_quantile(0.999) as f64 / 1000.0
            )
        },
        drained,
        shared.errors.load(Ordering::Relaxed)
    );
    // Library background threads (librdkafka, lapin) may linger; exit explicitly.
    std::process::exit(0);
}

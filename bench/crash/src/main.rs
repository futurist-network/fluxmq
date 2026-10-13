//! `mq-crash`: the client half of the crash-durability test (bench/crash/README.md).
//!
//! * `write` creates a fresh topic / channel, signals readiness (`--ready-file`), waits for
//!   `--go-file`, then publishes at a steady open-loop rate with broker acknowledgements and
//!   records **every acknowledged message id** (producer, sequence, partition, offset, ack
//!   time). It keeps publishing through the crash (failed publishes are counted, never
//!   recorded as acked) until `--stop-file` appears, then waits up to `--settle` seconds for
//!   outstanding acks and writes the acked list (`--out`, JSON lines) and a summary.
//! * `verify` re-reads the whole topic / channel from the earliest offset with a fresh
//!   consumer and reports every acknowledged id that is not in the log: **acked-but-lost**.
//!
//! The crash itself (kill -9 / simulated power cut) is done by bench/crash/crash.py with
//! `crashctl` inside the broker containers. Payload layout: `[0..4)` b"FXCR", `[4..8)`
//! producer, `[8..16)` sequence, `[16..24)` send time (epoch µs), rest filler.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, bail};
use clap::{Parser, Subcommand, ValueEnum};
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::json;

const MAGIC: &[u8; 4] = b"FXCR";
const HEADER: usize = 24;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum System {
    Fluxmq,
    Kafka,
}

#[derive(Parser, Debug)]
#[command(name = "mq-crash", about = "Crash-durability client: write acked ids, verify after a crash")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args, Debug, Clone)]
struct Common {
    #[arg(long, value_enum)]
    system: System,
    /// FluxMQ seeds `host:port` / Kafka bootstrap servers, comma-separated.
    #[arg(long, value_delimiter = ',', required = true)]
    brokers: Vec<String>,
    #[arg(long)]
    topic: String,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    Write(WriteArgs),
    Verify(VerifyArgs),
}

#[derive(clap::Args, Debug, Clone)]
struct WriteArgs {
    #[command(flatten)]
    c: Common,
    #[arg(long, default_value_t = 4)]
    partitions: u32,
    /// Kafka topic replication factor (FluxMQ: broker setting).
    #[arg(long, default_value_t = 1)]
    replication: u32,
    /// Kafka topic min.insync.replicas (with replication > 1).
    #[arg(long, default_value_t = 2)]
    min_insync: u32,
    /// Kafka topic segment.bytes (the bench's 16 MiB; FluxMQ: FLUXMQ_SEGMENT_BYTES).
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    segment_bytes: u64,
    /// Total target publish rate, msg/s (open loop).
    #[arg(long, default_value_t = 5000)]
    rate: u64,
    #[arg(long, default_value_t = 1024)]
    size: usize,
    #[arg(long, default_value_t = 2)]
    producers: usize,
    /// Max unacknowledged messages per producer.
    #[arg(long, default_value_t = 5000)]
    window: usize,
    #[arg(long, default_value_t = 5)]
    linger_ms: u64,
    /// Publish timeout (FluxMQ publish_timeout, Kafka message.timeout.ms), seconds. A
    /// message not acknowledged within it fails and is not counted as acked.
    #[arg(long, default_value_t = 30)]
    publish_timeout: u64,
    /// Created once the topic exists and the producers are connected.
    #[arg(long)]
    ready_file: Option<PathBuf>,
    /// Publishing starts when this file exists (immediately when omitted).
    #[arg(long)]
    go_file: Option<PathBuf>,
    /// Publishing stops when this file exists.
    #[arg(long)]
    stop_file: Option<PathBuf>,
    /// Hard cap on publishing time after go, seconds.
    #[arg(long, default_value_t = 600)]
    max_seconds: u64,
    /// After stop: wait this long for outstanding acks, seconds.
    #[arg(long, default_value_t = 45)]
    settle: u64,
    /// Acked ids, JSON lines `[producer, seq, partition, offset, ack_epoch_ms]`.
    #[arg(long)]
    out: PathBuf,
    /// Summary JSON.
    #[arg(long)]
    summary: PathBuf,
    /// Also run a live consumer (fresh group, from the earliest offset) during the run and
    /// record when each of our messages arrived, in 10 ms buckets since go
    /// (`consumer.buckets_10ms` in the summary): the failover scenario's consumer gap.
    #[arg(long)]
    live_consume: bool,
}

/// Live consumer accounting (`--live-consume`).
struct LiveRx {
    go_ms: u64,
    seen: std::sync::Mutex<(HashSet<(u32, u64)>, Vec<u32>)>,
    received: AtomicU64,
    duplicates: AtomicU64,
    errors: AtomicU64,
}

impl LiveRx {
    fn new(go_ms: u64) -> Self {
        Self {
            go_ms,
            seen: std::sync::Mutex::new((HashSet::new(), Vec::new())),
            received: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
    fn add(&self, payload: &[u8]) {
        let Some(id) = payload_id(payload) else {
            return;
        };
        let bucket = (epoch_ms().saturating_sub(self.go_ms) / 10) as usize;
        let mut g = self.seen.lock().unwrap();
        if !g.0.insert(id) {
            self.duplicates.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if g.1.len() <= bucket {
            g.1.resize(bucket + 1, 0);
        }
        g.1[bucket] += 1;
        self.received.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(clap::Args, Debug, Clone)]
struct VerifyArgs {
    #[command(flatten)]
    c: Common,
    /// The writer's acked-ids file.
    #[arg(long)]
    acked: PathBuf,
    /// Stop when nothing new arrived for this long (after the end of the log), seconds.
    #[arg(long, default_value_t = 15)]
    idle: u64,
    /// Hard cap, seconds.
    #[arg(long, default_value_t = 300)]
    max_seconds: u64,
    /// Crash instant (epoch ms), to classify lost acks as before / after it.
    #[arg(long)]
    crash_ms: Option<u64>,
    #[arg(long)]
    out: PathBuf,
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn payload(size: usize, producer: u32, seq: u64) -> Vec<u8> {
    let mut v = vec![0xABu8; size.max(HEADER)];
    v[0..4].copy_from_slice(MAGIC);
    v[4..8].copy_from_slice(&producer.to_be_bytes());
    v[8..16].copy_from_slice(&seq.to_be_bytes());
    let us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    v[16..24].copy_from_slice(&us.to_be_bytes());
    v
}

fn payload_id(p: &[u8]) -> Option<(u32, u64)> {
    if p.len() < HEADER || &p[0..4] != MAGIC {
        return None;
    }
    Some((
        u32::from_be_bytes(p[4..8].try_into().ok()?),
        u64::from_be_bytes(p[8..16].try_into().ok()?),
    ))
}

async fn wait_file(path: &Path, max: Duration) -> anyhow::Result<()> {
    let t = Instant::now();
    while !path.exists() {
        if t.elapsed() > max {
            bail!("timed out waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// write

/// Resolves to the (partition, offset) the broker acknowledged.
type AckFut = BoxFuture<'static, anyhow::Result<(i64, i64)>>;
/// Hands one payload to the client library; resolves once queued.
type Enqueue = Box<dyn Fn(Vec<u8>) -> BoxFuture<'static, anyhow::Result<AckFut>> + Send + Sync>;

struct Setup {
    producers: Vec<Enqueue>,
    durability: String,
    client: String,
    /// Keeps clients alive.
    _keep: Vec<Box<dyn std::any::Any + Send + Sync>>,
}

#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    acked: AtomicU64,
    failed: AtomicU64,
    enqueue_errors: AtomicU64,
    first_error_ms: AtomicU64,
    last_error: std::sync::Mutex<Option<String>>,
}

impl Counters {
    fn error(&self, e: String) {
        let _ = self.first_error_ms.compare_exchange(
            0,
            epoch_ms(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        *self.last_error.lock().unwrap() = Some(e);
    }
}

async fn write(a: WriteArgs) -> anyhow::Result<()> {
    anyhow::ensure!(a.size >= HEADER, "--size must be >= {HEADER}");
    let setup = match a.c.system {
        System::Fluxmq => fluxmq::setup(&a).await?,
        #[cfg(feature = "kafka")]
        System::Kafka => kafka::setup(&a).await?,
        #[cfg(not(feature = "kafka"))]
        System::Kafka => bail!("built without the `kafka` feature"),
    };
    if let Some(f) = &a.ready_file {
        std::fs::write(f, b"ready").with_context(|| format!("write {}", f.display()))?;
    }
    eprintln!("mq-crash write: {} ready, topic {}", setup.client, a.c.topic);
    if let Some(f) = &a.go_file {
        wait_file(f, Duration::from_secs(600)).await?;
    }
    let go_ms = epoch_ms();
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(Counters::default());
    let live = Arc::new(LiveRx::new(go_ms));
    let live_stop = Arc::new(AtomicBool::new(false));
    let live_task = a.live_consume.then(|| {
        let (a, live, live_stop) = (a.clone(), live.clone(), live_stop.clone());
        tokio::spawn(async move {
            let r = match a.c.system {
                System::Fluxmq => fluxmq::live_consume(&a, &live, &live_stop).await,
                #[cfg(feature = "kafka")]
                System::Kafka => kafka::live_consume(&a, &live, &live_stop).await,
                #[cfg(not(feature = "kafka"))]
                System::Kafka => Ok(()),
            };
            if let Err(e) = r {
                eprintln!("mq-crash write: live consumer failed: {e:#}");
                live.errors.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    // Stop watcher.
    {
        let stop = stop.clone();
        let file = a.stop_file.clone();
        let max = Duration::from_secs(a.max_seconds);
        tokio::spawn(async move {
            let t = Instant::now();
            loop {
                if t.elapsed() >= max || file.as_ref().is_some_and(|f| f.exists()) {
                    stop.store(true, Ordering::Relaxed);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
    }
    // Progress / timeline.
    let timeline = Arc::new(std::sync::Mutex::new(Vec::new()));
    let progress = {
        let c = counters.clone();
        let tl = timeline.clone();
        tokio::spawn(async move {
            let mut last = (0u64, 0u64, 0u64);
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.tick().await;
            let mut t = 0u64;
            loop {
                tick.tick().await;
                t += 1;
                let cur = (
                    c.sent.load(Ordering::Relaxed),
                    c.acked.load(Ordering::Relaxed),
                    c.failed.load(Ordering::Relaxed) + c.enqueue_errors.load(Ordering::Relaxed),
                );
                let row = json!({"t": t, "ms": epoch_ms(), "sent": cur.0 - last.0,
                                 "acked": cur.1 - last.1, "errors": cur.2 - last.2});
                eprintln!("mq-crash write: {row}");
                tl.lock().unwrap().push(row);
                last = cur;
            }
        })
    };

    let per_rate = a.rate.max(1) as f64 / a.producers.max(1) as f64;
    let mut tasks = Vec::new();
    for (p, enqueue) in setup.producers.into_iter().enumerate() {
        let stop = stop.clone();
        let c = counters.clone();
        let a = a.clone();
        tasks.push(tokio::spawn(async move {
            producer_loop(p as u32, enqueue, per_rate, a, stop, c).await
        }));
    }
    let mut acked = Vec::new();
    let mut unacked_at_end = 0u64;
    for t in tasks {
        let (list, left) = t.await?;
        acked.extend(list);
        unacked_at_end += left;
    }
    progress.abort();
    let stop_ms = epoch_ms();
    if let Some(t) = live_task {
        // Let the consumer catch up with the last acks (bounded), then stop it.
        let acked_n = acked.len() as u64;
        let t0 = Instant::now();
        while live.received.load(Ordering::Relaxed) < acked_n && t0.elapsed() < Duration::from_secs(20)
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        live_stop.store(true, Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
    }
    let consumer = a.live_consume.then(|| {
        let g = live.seen.lock().unwrap();
        json!({
            "received": live.received.load(Ordering::Relaxed),
            "duplicates": live.duplicates.load(Ordering::Relaxed),
            "errors": live.errors.load(Ordering::Relaxed),
            "buckets_10ms": g.1,
        })
    });

    let mut text = String::with_capacity(acked.len() * 32);
    for r in &acked {
        text.push_str(&format!("[{},{},{},{},{}]\n", r.0, r.1, r.2, r.3, r.4));
    }
    std::fs::write(&a.out, text).with_context(|| format!("write {}", a.out.display()))?;
    let first_err = counters.first_error_ms.load(Ordering::Relaxed);
    let summary = json!({
        "system": format!("{:?}", a.c.system).to_lowercase(),
        "topic": a.c.topic,
        "brokers": a.c.brokers,
        "partitions": a.partitions,
        "replication": a.replication,
        "rate": a.rate, "size": a.size, "producers": a.producers, "window": a.window,
        "linger_ms": a.linger_ms, "publish_timeout_s": a.publish_timeout,
        "durability": setup.durability,
        "client": setup.client,
        "go_ms": go_ms, "stop_ms": stop_ms,
        "sent": counters.sent.load(Ordering::Relaxed),
        "acked": acked.len(),
        "failed": counters.failed.load(Ordering::Relaxed),
        "enqueue_errors": counters.enqueue_errors.load(Ordering::Relaxed),
        "unacked_at_end": unacked_at_end,
        "first_error_ms": (first_err > 0).then_some(first_err),
        "last_error": counters.last_error.lock().unwrap().clone(),
        "timeline": *timeline.lock().unwrap(),
        "consumer": consumer,
    });
    std::fs::write(&a.summary, serde_json::to_string_pretty(&summary)?)?;
    eprintln!(
        "mq-crash write: sent {} acked {} failed {} unacked {}",
        counters.sent.load(Ordering::Relaxed),
        acked.len(),
        counters.failed.load(Ordering::Relaxed),
        unacked_at_end
    );
    Ok(())
}

type AckedRec = (u32, u64, i64, i64, u64);

async fn producer_loop(
    p: u32,
    enqueue: Enqueue,
    rate: f64,
    a: WriteArgs,
    stop: Arc<AtomicBool>,
    c: Arc<Counters>,
) -> (Vec<AckedRec>, u64) {
    let iv = Duration::from_secs_f64(1.0 / rate);
    let start = tokio::time::Instant::now() + iv.mul_f64(p as f64 / a.producers.max(1) as f64);
    let mut inflight = FuturesUnordered::new();
    let mut acked = Vec::new();
    let mut k = 0u64;
    let on_done = |acked: &mut Vec<AckedRec>, (seq, r): (u64, anyhow::Result<(i64, i64)>)| match r
    {
        Ok((part, off)) => {
            c.acked.fetch_add(1, Ordering::Relaxed);
            acked.push((p, seq, part, off, epoch_ms()));
        }
        Err(e) => {
            c.failed.fetch_add(1, Ordering::Relaxed);
            c.error(format!("publish: {e:#}"));
        }
    };
    while !stop.load(Ordering::Relaxed) {
        let due = start + iv.mul_f64(k as f64);
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(due) => break,
                Some(done) = inflight.next(), if !inflight.is_empty() => on_done(&mut acked, done),
            }
        }
        while inflight.len() >= a.window.max(1) {
            match inflight.next().await {
                Some(done) => on_done(&mut acked, done),
                None => break,
            }
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let seq = k;
        k += 1;
        c.sent.fetch_add(1, Ordering::Relaxed);
        // A stuck enqueue (client queue full while the broker is down) must not block the
        // stop; bound it.
        let enq = tokio::time::timeout(
            Duration::from_secs(a.publish_timeout),
            enqueue(payload(a.size, p, seq)),
        );
        match enq.await {
            Ok(Ok(fut)) => inflight.push(fut.map(move |r| (seq, r))),
            Ok(Err(e)) => {
                c.enqueue_errors.fetch_add(1, Ordering::Relaxed);
                c.error(format!("enqueue: {e:#}"));
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(_) => {
                c.enqueue_errors.fetch_add(1, Ordering::Relaxed);
                c.error("enqueue timed out".into());
            }
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(a.settle);
    while !inflight.is_empty() {
        match tokio::time::timeout_at(deadline, inflight.next()).await {
            Ok(Some(done)) => on_done(&mut acked, done),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    (acked, inflight.len() as u64)
}

// ---------------------------------------------------------------------------------------
// verify

struct Seen {
    ids: HashSet<(u32, u64)>,
    scanned: u64,
    foreign: u64,
    duplicates: u64,
    /// partition -> (min offset, max offset) seen.
    offsets: BTreeMap<i64, (i64, i64)>,
}

impl Seen {
    fn new() -> Self {
        Self {
            ids: HashSet::new(),
            scanned: 0,
            foreign: 0,
            duplicates: 0,
            offsets: BTreeMap::new(),
        }
    }
    fn add(&mut self, payload: &[u8], part: i64, off: i64) {
        self.scanned += 1;
        self.offsets
            .entry(part)
            .and_modify(|(lo, hi)| {
                *lo = (*lo).min(off);
                *hi = (*hi).max(off);
            })
            .or_insert((off, off));
        match payload_id(payload) {
            Some(id) => {
                if !self.ids.insert(id) {
                    self.duplicates += 1;
                }
            }
            None => self.foreign += 1,
        }
    }
}

fn load_acked(path: &Path) -> anyhow::Result<Vec<AckedRec>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut v = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let r: (u32, u64, i64, i64, u64) = serde_json::from_str(line)?;
        v.push(r);
    }
    Ok(v)
}

async fn verify(a: VerifyArgs) -> anyhow::Result<()> {
    let acked = load_acked(&a.acked)?;
    let want: HashSet<(u32, u64)> = acked.iter().map(|r| (r.0, r.1)).collect();
    let started = Instant::now();
    let (seen, end_reason, extra) = match a.c.system {
        System::Fluxmq => fluxmq::read_all(&a, &want).await?,
        #[cfg(feature = "kafka")]
        System::Kafka => kafka::read_all(&a, &want).await?,
        #[cfg(not(feature = "kafka"))]
        System::Kafka => bail!("built without the `kafka` feature"),
    };
    let lost: Vec<&AckedRec> = acked
        .iter()
        .filter(|r| !seen.ids.contains(&(r.0, r.1)))
        .collect();
    let mut by_part: BTreeMap<i64, (u64, i64, i64)> = BTreeMap::new();
    for r in &lost {
        by_part
            .entry(r.2)
            .and_modify(|(n, lo, hi)| {
                *n += 1;
                *lo = (*lo).min(r.3);
                *hi = (*hi).max(r.3);
            })
            .or_insert((1, r.3, r.3));
    }
    let (before, after) = match a.crash_ms {
        Some(c) => (
            json!(lost.iter().filter(|r| r.4 <= c).count()),
            json!(lost.iter().filter(|r| r.4 > c).count()),
        ),
        None => (serde_json::Value::Null, serde_json::Value::Null),
    };
    let last_acked_before_crash = a
        .crash_ms
        .and_then(|c| acked.iter().filter(|r| r.4 <= c).map(|r| r.4).max());
    let unacked_seen = seen.ids.iter().filter(|id| !want.contains(id)).count();
    let doc = json!({
        "system": format!("{:?}", a.c.system).to_lowercase(),
        "topic": a.c.topic,
        "acked": acked.len(),
        "acked_unique": want.len(),
        "found": want.len() - lost.len(),
        "lost": lost.len(),
        "lost_acked_before_crash": before,
        "lost_acked_after_crash": after,
        "last_ack_before_crash_ms": last_acked_before_crash,
        "lost_ack_ms_range": if lost.is_empty() { serde_json::Value::Null } else {
            json!([lost.iter().map(|r| r.4).min(), lost.iter().map(|r| r.4).max()])
        },
        "lost_by_partition": by_part.iter().map(|(p, (n, lo, hi))|
            (p.to_string(), json!({"count": n, "acked_offsets": [lo, hi]}))).collect::<serde_json::Map<_, _>>(),
        "lost_sample": lost.iter().take(200).map(|r| json!(r)).collect::<Vec<_>>(),
        "scanned": seen.scanned,
        "unacked_but_stored": unacked_seen,
        "duplicates": seen.duplicates,
        "foreign": seen.foreign,
        "offsets_seen": seen.offsets.iter().map(|(p, (lo, hi))|
            (p.to_string(), json!([lo, hi]))).collect::<serde_json::Map<_, _>>(),
        "end_reason": end_reason,
        "extra": extra,
        "seconds": (started.elapsed().as_secs_f64() * 10.0).round() / 10.0,
    });
    std::fs::write(&a.out, serde_json::to_string_pretty(&doc)?)?;
    eprintln!(
        "mq-crash verify: acked {} found {} LOST {} (scanned {}, {})",
        want.len(),
        want.len() - lost.len(),
        lost.len(),
        seen.scanned,
        end_reason
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------
// FluxMQ

mod fluxmq {
    use super::*;
    use fluxmq_client::{
        BatchingOptions, ChannelConfig, ChannelMode, Client, ClientOptions, PublishMessage,
        ReconnectPolicy, StartFrom, SubscribeOptions,
    };

    fn options(c: &Common, name: &str, publish_timeout: u64) -> ClientOptions {
        let mut o = ClientOptions::new(c.brokers[0].clone(), name);
        if c.brokers.len() > 1 {
            o = o.seeds(c.brokers.clone());
        }
        o.request_timeout = Duration::from_secs(10);
        o.publish_timeout = Duration::from_secs(publish_timeout);
        o.stamp_producer = false;
        o.reconnect = ReconnectPolicy::infinite()
            .backoff(Duration::from_millis(100), Duration::from_secs(2))
            .retry_initial_connect(true);
        o
    }

    pub async fn setup(a: &WriteArgs) -> anyhow::Result<Setup> {
        let admin = Client::connect(options(&a.c, "mq-crash-admin", a.publish_timeout))
            .await
            .context("connect")?;
        let mut cfg = ChannelConfig::new(a.c.topic.clone(), ChannelMode::Stream);
        cfg.partitions = a.partitions.max(1);
        cfg.retention.max_bytes = None;
        cfg.retention.max_age_secs = Some(7 * 24 * 3600);
        admin.ensure_channel(cfg).await.context("create channel")?;
        let version = admin.server_info().broker_version.clone();
        let mut producers: Vec<Enqueue> = Vec::new();
        let mut keep: Vec<Box<dyn std::any::Any + Send + Sync>> = vec![Box::new(admin)];
        for p in 0..a.producers {
            let client =
                Client::connect(options(&a.c, &format!("mq-crash-producer-{p}"), a.publish_timeout))
                    .await?;
            let producer = Arc::new(client.batching_producer(
                &a.c.topic,
                BatchingOptions {
                    linger: Duration::from_millis(a.linger_ms),
                    queue_capacity: a.window.max(1024),
                    ..BatchingOptions::default()
                },
            ));
            keep.push(Box::new(client));
            producers.push(Box::new(move |payload: Vec<u8>| {
                let producer = producer.clone();
                async move {
                    let msg = PublishMessage {
                        key: None,
                        headers: Default::default(),
                        payload: payload.into(),
                    };
                    let fut = producer.send(msg).await?;
                    Ok(async move {
                        let id = fut.await?;
                        Ok((i64::from(id.partition), id.offset as i64))
                    }
                    .boxed())
                }
                .boxed()
            }));
        }
        Ok(Setup {
            producers,
            durability: "FluxMQ stream channel; publish acknowledged per the broker's \
                         FLUXMQ_FSYNC policy (group-commit: after fsync; os: after write(2)) \
                         and, in a cluster, after every ISR member has it"
                .into(),
            client: format!(
                "fluxmq-client BatchingProducer linger={}ms (broker {version})",
                a.linger_ms
            ),
            _keep: keep,
        })
    }

    pub async fn live_consume(
        a: &WriteArgs,
        live: &LiveRx,
        stop: &AtomicBool,
    ) -> anyhow::Result<()> {
        let client = Client::connect(options(&a.c, "mq-crash-live", 30)).await?;
        let group = format!("{}-live", a.c.topic);
        let mut consumer = client
            .subscribe(
                &a.c.topic,
                &group,
                SubscribeOptions::default()
                    .prefetch(2000)
                    .start(StartFrom::Earliest),
            )
            .await
            .context("subscribe")?;
        while !stop.load(Ordering::Relaxed) {
            match tokio::time::timeout(Duration::from_millis(200), consumer.next()).await {
                Ok(Some(Ok(d))) => {
                    live.add(d.payload());
                    let _ = d.ack().await;
                }
                Ok(Some(Err(e))) => {
                    live.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("mq-crash write: live consume: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(None) => bail!("live consumer stream ended"),
                Err(_) => {}
            }
        }
        let _ = consumer.unsubscribe().await;
        Ok(())
    }

    pub async fn read_all(
        a: &VerifyArgs,
        want: &HashSet<(u32, u64)>,
    ) -> anyhow::Result<(Seen, String, serde_json::Value)> {
        let client = Client::connect(options(&a.c, "mq-crash-verify", 30)).await?;
        let group = format!("{}-verify-{}", a.c.topic, epoch_ms() % 1_000_000);
        let mut consumer = client
            .subscribe(
                &a.c.topic,
                &group,
                SubscribeOptions::default()
                    .prefetch(2000)
                    .start(StartFrom::Earliest),
            )
            .await
            .context("subscribe")?;
        let mut seen = Seen::new();
        let t = Instant::now();
        let mut found = 0usize;
        let reason;
        loop {
            if t.elapsed() > Duration::from_secs(a.max_seconds) {
                reason = "max time";
                break;
            }
            // Everything acked was found: read a little longer for the tail, then stop.
            let idle = if found >= want.len() {
                Duration::from_secs(2)
            } else {
                Duration::from_secs(a.idle)
            };
            match tokio::time::timeout(idle, consumer.next()).await {
                Ok(Some(Ok(d))) => {
                    let id = d.id();
                    let before = seen.ids.len();
                    seen.add(d.payload(), i64::from(id.partition), id.offset as i64);
                    if seen.ids.len() > before
                        && payload_id(d.payload()).is_some_and(|i| want.contains(&i))
                    {
                        found += 1;
                    }
                    let _ = d.ack().await;
                }
                Ok(Some(Err(e))) => bail!("consume: {e}"),
                Ok(None) => {
                    reason = "stream ended";
                    break;
                }
                Err(_) => {
                    reason = if found >= want.len() {
                        "all acked found"
                    } else {
                        "idle timeout"
                    };
                    break;
                }
            }
        }
        let _ = consumer.unsubscribe().await;
        Ok((seen, reason.into(), json!({"group": group})))
    }
}

// ---------------------------------------------------------------------------------------
// Kafka

#[cfg(feature = "kafka")]
mod kafka {
    use super::*;
    use std::collections::HashMap;

    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::message::Message;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};

    fn base(c: &Common) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", c.brokers.join(","));
        cfg
    }

    pub async fn setup(a: &WriteArgs) -> anyhow::Result<Setup> {
        let admin: AdminClient<DefaultClientContext> = base(&a.c).create()?;
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
        let min_isr = if a.replication > 1 {
            a.min_insync.min(a.replication)
        } else {
            1
        };
        let min_isr_s = min_isr.to_string();
        let segment_s = a.segment_bytes.to_string();
        let topic = NewTopic::new(
            &a.c.topic,
            a.partitions.max(1) as i32,
            TopicReplication::Fixed(a.replication.max(1) as i32),
        )
        .set("min.insync.replicas", &min_isr_s)
        .set("segment.bytes", &segment_s);
        for r in admin.create_topics(&[topic], &opts).await? {
            if let Err((t, code)) = r
                && code != RDKafkaErrorCode::TopicAlreadyExists
            {
                bail!("create topic {t}: {code}");
            }
        }
        // Wait for leaders on every partition.
        let probe: BaseConsumer = base(&a.c).create()?;
        for _ in 0..120 {
            let ok = probe
                .fetch_metadata(Some(&a.c.topic), Duration::from_secs(5))
                .ok()
                .and_then(|m| {
                    m.topics().first().map(|t| {
                        t.error().is_none()
                            && t.partitions().len() == a.partitions as usize
                            && t.partitions().iter().all(|p| p.leader() >= 0)
                    })
                })
                .unwrap_or(false);
            if ok {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let mut producers: Vec<Enqueue> = Vec::new();
        let mut keep: Vec<Box<dyn std::any::Any + Send + Sync>> = vec![Box::new(admin)];
        for p in 0..a.producers {
            let producer: FutureProducer = base(&a.c)
                .set("client.id", format!("mq-crash-producer-{p}"))
                .set("acks", "all")
                .set("enable.idempotence", "true")
                .set("linger.ms", a.linger_ms.to_string())
                .set("compression.type", "none")
                .set("message.timeout.ms", (a.publish_timeout * 1000).to_string())
                .create()?;
            let producer = Arc::new(producer);
            keep.push(Box::new(producer.clone()));
            let topic = a.c.topic.clone();
            producers.push(Box::new(move |payload: Vec<u8>| {
                let producer = producer.clone();
                let topic = topic.clone();
                async move {
                    loop {
                        let rec = FutureRecord::<(), [u8]>::to(&topic).payload(&payload);
                        match producer.send_result(rec) {
                            Ok(fut) => {
                                return Ok(async move {
                                    match fut.await {
                                        Ok(Ok(d)) => Ok((i64::from(d.partition), d.offset)),
                                        Ok(Err((e, _))) => Err(anyhow::anyhow!("{e}")),
                                        Err(_) => Err(anyhow::anyhow!("delivery canceled")),
                                    }
                                }
                                .boxed());
                            }
                            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            Err((e, _)) => return Err(anyhow::anyhow!("{e}")),
                        }
                    }
                }
                .boxed()
            }));
        }
        let (_, version) = rdkafka::util::get_rdkafka_version();
        Ok(Setup {
            producers,
            durability: format!(
                "Kafka topic RF={}, min.insync.replicas={min_isr}; producer acks=all, \
                 enable.idempotence=true; broker flush policy per its config \
                 (log.flush.interval.messages)",
                a.replication
            ),
            client: format!("librdkafka {version} FutureProducer linger.ms={}", a.linger_ms),
            _keep: keep,
        })
    }

    pub async fn live_consume(
        a: &WriteArgs,
        live: &LiveRx,
        stop: &AtomicBool,
    ) -> anyhow::Result<()> {
        let consumer: StreamConsumer = base(&a.c)
            .set("group.id", format!("{}-live", a.c.topic))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[a.c.topic.as_str()])?;
        while !stop.load(Ordering::Relaxed) {
            match tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await {
                Ok(Ok(m)) => {
                    if let Some(p) = m.payload() {
                        live.add(p);
                    }
                }
                Ok(Err(e)) => {
                    live.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("mq-crash write: live consume: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(_) => {}
            }
        }
        Ok(())
    }

    pub async fn read_all(
        a: &VerifyArgs,
        want: &HashSet<(u32, u64)>,
    ) -> anyhow::Result<(Seen, String, serde_json::Value)> {
        let group = format!("{}-verify-{}", a.c.topic, epoch_ms() % 1_000_000);
        let consumer: StreamConsumer = base(&a.c)
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        // Partitions and their high watermarks (retry while the cluster recovers).
        let mut parts = Vec::new();
        let t = Instant::now();
        while parts.is_empty() {
            if let Ok(m) = consumer.fetch_metadata(Some(&a.c.topic), Duration::from_secs(5))
                && let Some(tm) = m.topics().first()
                && tm.error().is_none()
                && tm.partitions().iter().all(|p| p.leader() >= 0)
            {
                parts = tm.partitions().iter().map(|p| p.id()).collect();
            }
            if parts.is_empty() {
                if t.elapsed() > Duration::from_secs(120) {
                    bail!("topic metadata unavailable");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        let mut high: HashMap<i32, (i64, i64)> = HashMap::new();
        for &p in &parts {
            let mut wm = None;
            for _ in 0..60 {
                match consumer.fetch_watermarks(&a.c.topic, p, Duration::from_secs(5)) {
                    Ok(w) => {
                        wm = Some(w);
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
                }
            }
            high.insert(p, wm.context("watermarks")?);
        }
        let mut tpl = TopicPartitionList::new();
        for &p in &parts {
            tpl.add_partition_offset(&a.c.topic, p, Offset::Beginning)?;
        }
        consumer.assign(&tpl)?;
        let mut seen = Seen::new();
        let mut next: HashMap<i32, i64> = parts.iter().map(|&p| (p, 0)).collect();
        let at_end = |next: &HashMap<i32, i64>| {
            high.iter()
                .all(|(p, (lo, hi))| hi <= lo || next.get(p).copied().unwrap_or(0) >= *hi)
        };
        let mut found = 0usize;
        let reason;
        loop {
            if at_end(&next) {
                reason = "reached high watermarks";
                break;
            }
            if t.elapsed() > Duration::from_secs(a.max_seconds) {
                reason = "max time";
                break;
            }
            match tokio::time::timeout(Duration::from_secs(a.idle), consumer.recv()).await {
                Ok(Ok(m)) => {
                    let before = seen.ids.len();
                    if let Some(p) = m.payload() {
                        seen.add(p, i64::from(m.partition()), m.offset());
                        if seen.ids.len() > before
                            && payload_id(p).is_some_and(|i| want.contains(&i))
                        {
                            found += 1;
                        }
                    }
                    next.insert(m.partition(), m.offset() + 1);
                }
                Ok(Err(e)) => {
                    eprintln!("mq-crash verify: consume error: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => {
                    reason = "idle timeout";
                    break;
                }
            }
        }
        let _ = found;
        let hw: serde_json::Map<_, _> = high
            .iter()
            .map(|(p, (lo, hi))| (p.to_string(), json!([lo, hi])))
            .collect();
        Ok((seen, reason.into(), json!({"group": group, "watermarks": hw})))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Write(a) => write(a).await?,
        Cmd::Verify(a) => verify(a).await?,
    }
    // librdkafka threads may linger.
    std::process::exit(0);
}

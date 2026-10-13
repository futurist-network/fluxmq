//! FluxMQ load / latency benchmark.
//!
//! Runs N producers and M consumers (each on its own connection) against one channel and prints
//! per-second publish/consume rates, throughput, publish round-trip latency and end-to-end
//! latency (publish → delivery, from a timestamp embedded in the payload).
//!
//! ```text
//! cargo run --release -p fluxmq-client --bin fluxmq-bench -- \
//!     --producers 4 --consumers 4 --size 1024 --batch 100 --duration 30
//! # against a cluster (publishes/consumes directly on partition leaders):
//! cargo run --release -p fluxmq-client --bin fluxmq-bench -- --seeds n1:7420,n2:7420,n3:7420
//! ```
//!
//! `--drain N` measures pure consume throughput instead: it first publishes N messages with
//! no consumer attached, then starts the consumers at the earliest offset of a fresh group
//! and times how long they take to consume all N (use a fresh `--channel`).
//!
//! Clients reconnect automatically; the per-second table notes reconnects and the summary
//! prints the total.
//!
//! Latency methodology:
//! * `--rate 0` (default) is a closed loop: each producer worker sends the next batch as soon as
//!   the previous one is acknowledged. Latencies are honest per request, but a closed loop
//!   suffers from *coordinated omission* (when the broker stalls, fewer samples are taken).
//! * `--rate R` is an open loop: sends are scheduled at fixed intervals and latency is measured
//!   from the *intended* send time, so stalls are charged to every request that should have
//!   been sent meanwhile (coordinated-omission corrected).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, ValueEnum};
use fluxmq_client::{Client, ClientOptions, StartFrom, SubscribeOptions, TlsOptions};
use fluxmq_proto::{ChannelConfig, ChannelMode, PublishMessage};
use hdrhistogram::Histogram;
use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Stream,
    Queue,
    Keyed,
}

impl From<Mode> for ChannelMode {
    fn from(m: Mode) -> Self {
        match m {
            Mode::Stream => ChannelMode::Stream,
            Mode::Queue => ChannelMode::Queue,
            Mode::Keyed => ChannelMode::Keyed,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "fluxmq-bench", about = "FluxMQ throughput / latency benchmark")]
struct Args {
    /// Broker client address.
    #[arg(long, default_value = "127.0.0.1:7420", env = "FLUXMQ_ADDR")]
    addr: String,
    /// Bootstrap addresses of a cluster (comma-separated); overrides --addr. The client learns
    /// partition leaders from metadata and publishes/consumes on them directly.
    #[arg(long, env = "FLUXMQ_SEEDS", value_delimiter = ',')]
    seeds: Vec<String>,
    /// Auth token (if the broker has one).
    #[arg(long, env = "FLUXMQ_TOKEN")]
    token: Option<String>,
    /// CA certificate (PEM) the broker certificate must chain to; enables TLS (so does a
    /// `tls://` address, which uses the OS trust store).
    #[arg(long, env = "FLUXMQ_CLIENT_TLS_CA")]
    tls_ca: Option<std::path::PathBuf>,
    /// Client certificate (PEM) for mTLS; needs `--tls-key`.
    #[arg(long, env = "FLUXMQ_CLIENT_TLS_CERT")]
    tls_cert: Option<std::path::PathBuf>,
    /// Client private key (PEM) for `--tls-cert`.
    #[arg(long, env = "FLUXMQ_CLIENT_TLS_KEY")]
    tls_key: Option<std::path::PathBuf>,
    /// Channel to use (created if missing).
    #[arg(long, default_value = "bench")]
    channel: String,
    /// Channel mode when creating it.
    #[arg(long, value_enum, default_value_t = Mode::Stream)]
    mode: Mode,
    /// Partitions when creating the channel.
    #[arg(long, default_value_t = 8)]
    partitions: u32,
    /// Producer connections.
    #[arg(long, default_value_t = 1)]
    producers: usize,
    /// Concurrent in-flight publish requests per producer connection.
    #[arg(long, default_value_t = 4)]
    inflight: usize,
    /// Consumer connections (all in one fresh group).
    #[arg(long, default_value_t = 1)]
    consumers: usize,
    /// Prefetch per consumer.
    #[arg(long, default_value_t = 2000)]
    prefetch: u32,
    /// Payload size in bytes (min 8: the send timestamp lives in the first 8 bytes).
    #[arg(long, default_value_t = 1024)]
    size: usize,
    /// Target total publish rate in msgs/s; 0 = as fast as possible (closed loop).
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Measurement duration in seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,
    /// Messages per publish request.
    #[arg(long, default_value_t = 100)]
    batch: usize,
    /// Distinct message keys; 0 = no keys (round-robin partitioning). Keyed mode needs keys and
    /// uses 1024 when this is 0.
    #[arg(long, default_value_t = 0)]
    keys: u64,
    /// Consumer ack batch size (`SubscribeOptions::ack_batch`).
    #[arg(long, default_value_t = 64)]
    ack_batch: u32,
    /// Consumer ack linger in µs (`SubscribeOptions::ack_linger`); default: the client's
    /// `ack_linger` (1 ms).
    #[arg(long)]
    ack_linger_us: Option<u64>,
    /// Drain mode: prefill N messages, then measure how fast the consumers drain them.
    #[arg(long, default_value_t = 0)]
    drain: u64,
}

impl Args {
    fn subscribe_options(&self, start: StartFrom) -> SubscribeOptions {
        let mut o = SubscribeOptions::default()
            .prefetch(self.prefetch)
            .start(start)
            .ack_batch(self.ack_batch);
        if let Some(us) = self.ack_linger_us {
            o = o.ack_linger(Duration::from_micros(us));
        }
        o
    }
}

/// Microsecond clock shared by producers and consumers (same process ⇒ comparable).
#[derive(Clone, Copy)]
struct Clock {
    base: Instant,
}

impl Clock {
    fn us(&self, t: Instant) -> u64 {
        t.saturating_duration_since(self.base).as_micros() as u64
    }
    fn now_us(&self) -> u64 {
        self.us(Instant::now())
    }
}

type Hist = Arc<Mutex<Histogram<u64>>>;

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000, 3).expect("valid bounds")
}

#[derive(Default)]
struct Counters {
    published: AtomicU64,
    pub_bytes: AtomicU64,
    consumed: AtomicU64,
    con_bytes: AtomicU64,
    errors: AtomicU64,
}

struct Shared {
    c: Counters,
    stop: AtomicBool,
    pub_hists: Mutex<Vec<Hist>>,
    e2e_hists: Mutex<Vec<Hist>>,
    last_error: Mutex<Option<String>>,
}

impl Shared {
    fn new_hist(list: &Mutex<Vec<Hist>>) -> Hist {
        let h = Arc::new(Mutex::new(hist()));
        list.lock().push(h.clone());
        h
    }
    fn error(&self, e: impl std::fmt::Display) {
        self.c.errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock() = Some(e.to_string());
    }
}

fn opts(args: &Args, name: String) -> ClientOptions {
    let mut o = ClientOptions::new(args.addr.clone(), name).seeds(args.seeds.clone());
    o.auth_token = args.token.clone();
    o.tls = TlsOptions::from_files(
        args.tls_ca.clone(),
        args.tls_cert.clone(),
        args.tls_key.clone(),
    )
    .expect("validated in main");
    o.request_timeout = Duration::from_secs(30);
    o.publish_timeout = Duration::from_secs(30);
    o
}

fn total_reconnects(clients: &Mutex<Vec<Client>>) -> u64 {
    clients.lock().iter().map(Client::reconnects).sum()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.size >= 8, "--size must be >= 8");
    TlsOptions::from_files(
        args.tls_ca.clone(),
        args.tls_cert.clone(),
        args.tls_key.clone(),
    )
    .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        args.producers > 0 && args.batch > 0 && args.inflight > 0,
        "invalid producer settings"
    );
    let clock = Clock {
        base: Instant::now(),
    };

    let admin = Client::connect(opts(&args, "fluxmq-bench".into())).await?;
    let clients: Arc<Mutex<Vec<Client>>> = Arc::new(Mutex::new(vec![admin.clone()]));
    let mut cfg = ChannelConfig::new(&args.channel, args.mode.into());
    cfg.partitions = args.partitions.max(1);
    let created = admin.ensure_channel(cfg).await?;
    let target = if args.seeds.is_empty() {
        args.addr.clone()
    } else {
        args.seeds.join(",")
    };
    let topology = match admin.metadata() {
        Some(m) => format!("cluster '{}' of {} nodes", m.cluster_name, m.nodes.len()),
        None => "single node".to_string(),
    };
    println!(
        "fluxmq-bench → {} ({topology}, broker {}), channel '{}' {} [{:?}, {} partitions]",
        target,
        admin.server_info().broker_version,
        args.channel,
        if created {
            "created"
        } else {
            "already existed (its config wins)"
        },
        args.mode,
        args.partitions
    );
    println!(
        "producers {} × inflight {} × batch {}, consumers {} (prefetch {}), size {} B, rate {}, duration {}s",
        args.producers,
        args.inflight,
        args.batch,
        args.consumers,
        args.prefetch,
        args.size,
        if args.rate == 0 {
            "max (closed loop)".to_string()
        } else {
            format!("{}/s (open loop)", args.rate)
        },
        args.duration
    );

    if args.drain > 0 {
        return drain_mode(&args, &clients).await;
    }

    let shared = Arc::new(Shared {
        c: Counters::default(),
        stop: AtomicBool::new(false),
        pub_hists: Mutex::new(Vec::new()),
        e2e_hists: Mutex::new(Vec::new()),
        last_error: Mutex::new(None),
    });

    // Consumers first, in a fresh group starting at the tail.
    let group = format!("bench-{}-{}", std::process::id(), fluxmq_proto::now_ms());
    let mut consumer_tasks = Vec::new();
    for i in 0..args.consumers {
        let client = Client::connect(opts(&args, format!("bench-consumer-{}", i + 1))).await?;
        clients.lock().push(client.clone());
        let mut consumer = client
            .subscribe(
                &args.channel,
                &group,
                args.subscribe_options(StartFrom::Latest),
            )
            .await?;
        let hist = Shared::new_hist(&shared.e2e_hists);
        let shared = shared.clone();
        consumer_tasks.push(tokio::spawn(async move {
            while let Some(d) = consumer.next().await {
                let d = match d {
                    Ok(d) => d,
                    Err(e) => {
                        if !shared.stop.load(Ordering::Relaxed) {
                            shared.error(e);
                        }
                        break;
                    }
                };
                let p = d.payload();
                if p.len() >= 8 {
                    let sent = u64::from_be_bytes(p[..8].try_into().expect("8 bytes"));
                    let now = clock.now_us();
                    hist.lock()
                        .saturating_record(now.saturating_sub(sent).max(1));
                }
                shared.c.consumed.fetch_add(1, Ordering::Relaxed);
                shared
                    .c
                    .con_bytes
                    .fetch_add(p.len() as u64, Ordering::Relaxed);
                if let Err(e) = d.ack().await {
                    shared.error(e);
                    break;
                }
            }
            drop(client);
        }));
    }

    // Producers.
    let workers_total = (args.producers * args.inflight) as f64;
    let batch_interval = (args.rate > 0)
        .then(|| Duration::from_secs_f64(args.batch as f64 * workers_total / args.rate as f64));
    let keys = match (args.mode, args.keys) {
        (Mode::Keyed, 0) => 1024,
        (_, k) => k,
    };
    let keyed = keys > 0;
    let filler = Bytes::from(vec![0xABu8; args.size - 8]);
    let start = Instant::now();
    let mut producer_tasks = Vec::new();
    for p in 0..args.producers {
        let client = Client::connect(opts(&args, format!("bench-producer-{}", p + 1))).await?;
        clients.lock().push(client.clone());
        for w in 0..args.inflight {
            let client = client.clone();
            let shared = shared.clone();
            let hist = Shared::new_hist(&shared.pub_hists);
            let channel = args.channel.clone();
            let filler = filler.clone();
            let batch = args.batch;
            let size = args.size as u64;
            // Stagger open-loop workers so they don't fire in lockstep.
            let offset = batch_interval
                .map(|iv| iv.mul_f64((p * args.inflight + w) as f64 / workers_total))
                .unwrap_or_default();
            producer_tasks.push(tokio::spawn(async move {
                let mut seq: u64 = (p * 1_000_003 + w * 7919) as u64;
                let mut k: u32 = 0;
                while !shared.stop.load(Ordering::Relaxed) {
                    let intended = match batch_interval {
                        Some(iv) => {
                            let t = start + offset + iv * k;
                            k += 1;
                            tokio::time::sleep_until(t.into()).await;
                            t
                        }
                        None => Instant::now(),
                    };
                    let ts = clock.us(intended).to_be_bytes();
                    let msgs: Vec<PublishMessage> = (0..batch)
                        .map(|_| {
                            seq = seq.wrapping_add(1);
                            let mut b = BytesMut::with_capacity(8 + filler.len());
                            b.put_slice(&ts);
                            b.put_slice(&filler);
                            PublishMessage {
                                key: keyed.then(|| format!("k{}", seq % keys)),
                                headers: Default::default(),
                                payload: b.freeze(),
                            }
                        })
                        .collect();
                    match client.publish_batch(&channel, msgs).await {
                        Ok(ids) => {
                            let lat = Instant::now().saturating_duration_since(intended);
                            hist.lock()
                                .saturating_record((lat.as_micros() as u64).max(1));
                            shared
                                .c
                                .published
                                .fetch_add(ids.len() as u64, Ordering::Relaxed);
                            shared
                                .c
                                .pub_bytes
                                .fetch_add(ids.len() as u64 * size, Ordering::Relaxed);
                        }
                        Err(e) => {
                            // Connection loss / NotLeader / Overloaded were already retried
                            // for publish_timeout inside the SDK.
                            shared.error(&e);
                            if client.is_closed() {
                                break;
                            }
                        }
                    }
                }
            }));
        }
    }

    // Reporter.
    let mut pub_total = hist();
    let mut e2e_total = hist();
    let mut tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    let mut last = (0u64, 0u64, 0u64, 0u64);
    let mut last_t = Instant::now();
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
    println!(
        "{:>5} {:>12} {:>9} {:>12} {:>9} | {:>27} | {:>27}",
        "t",
        "pub msg/s",
        "pub MB/s",
        "con msg/s",
        "con MB/s",
        "publish rtt p50/p99/p999 ms",
        "end-to-end p50/p99/p999 ms"
    );
    let mut interrupted = false;
    let mut last_reconnects = 0u64;
    for sec in 1..=args.duration {
        tokio::select! {
            _ = tick.tick() => {}
            _ = &mut ctrl_c => { interrupted = true; }
        }
        let now = Instant::now();
        let dt = now.duration_since(last_t).as_secs_f64();
        last_t = now;
        let cur = (
            shared.c.published.load(Ordering::Relaxed),
            shared.c.pub_bytes.load(Ordering::Relaxed),
            shared.c.consumed.load(Ordering::Relaxed),
            shared.c.con_bytes.load(Ordering::Relaxed),
        );
        let ph = drain(&shared.pub_hists, &mut pub_total);
        let eh = drain(&shared.e2e_hists, &mut e2e_total);
        println!(
            "{:>4}s {:>12} {:>9.1} {:>12} {:>9.1} | {:>27} | {:>27}",
            sec,
            fmt_rate((cur.0 - last.0) as f64 / dt),
            (cur.1 - last.1) as f64 / dt / 1e6,
            fmt_rate((cur.2 - last.2) as f64 / dt),
            (cur.3 - last.3) as f64 / dt / 1e6,
            pct(&ph),
            pct(&eh),
        );
        last = cur;
        let rc = total_reconnects(&clients);
        if rc != last_reconnects {
            println!("      ↻ {} reconnect(s) so far", rc);
            last_reconnects = rc;
        }
        if interrupted {
            break;
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    shared.stop.store(true, Ordering::Relaxed);
    for t in producer_tasks {
        let _ = t.await;
    }
    let published = shared.c.published.load(Ordering::Relaxed);
    let pub_bytes = shared.c.pub_bytes.load(Ordering::Relaxed);
    let consumed_at_stop = shared.c.consumed.load(Ordering::Relaxed);

    // Drain: give consumers up to 10 s to catch up with everything published.
    let mut drained = args.consumers == 0;
    if args.consumers > 0 {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if shared.c.consumed.load(Ordering::Relaxed) >= published {
                drained = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    drain(&shared.pub_hists, &mut pub_total);
    drain(&shared.e2e_hists, &mut e2e_total);
    let consumed = shared.c.consumed.load(Ordering::Relaxed);
    for t in &consumer_tasks {
        t.abort();
    }

    println!("\n=== summary ({elapsed:.1}s measured) ===");
    println!(
        "published   {published} msgs, {:.0} msg/s, {:.1} MB/s",
        published as f64 / elapsed,
        pub_bytes as f64 / elapsed / 1e6
    );
    if args.consumers > 0 {
        println!(
            "consumed    {consumed} msgs ({consumed_at_stop} during the run, {:.0} msg/s); {}",
            consumed_at_stop as f64 / elapsed,
            if drained {
                "all published messages consumed".to_string()
            } else {
                format!(
                    "NOT drained: {} missing after 10 s",
                    published.saturating_sub(consumed)
                )
            }
        );
    }
    println!(
        "publish rtt (per {}-msg request) ms: {}",
        args.batch,
        full(&pub_total)
    );
    if args.consumers > 0 {
        println!("end-to-end (publish→delivery) ms:   {}", full(&e2e_total));
    }
    println!("reconnects  {}", total_reconnects(&clients));
    let errors = shared.c.errors.load(Ordering::Relaxed);
    if errors > 0 {
        println!(
            "errors      {errors} (last: {})",
            shared.last_error.lock().clone().unwrap_or_default()
        );
    }
    if args.rate == 0 {
        println!(
            "note: closed-loop run — latencies are subject to coordinated omission; use --rate for \
             open-loop, CO-corrected latency at a fixed load."
        );
    } else {
        println!(
            "note: open-loop at {} msg/s — latencies measured from the scheduled send time \
             (coordinated-omission corrected). If 'pub msg/s' is below target, the broker or \
             client could not keep up.",
            args.rate
        );
    }
    Ok(())
}

/// `--drain N`: prefill, then time consumers draining the backlog from the earliest offset.
async fn drain_mode(args: &Args, clients: &Arc<Mutex<Vec<Client>>>) -> anyhow::Result<()> {
    let total = args.drain;
    let filler = Bytes::from(vec![0xABu8; args.size - 8]);
    let keys = match (args.mode, args.keys) {
        (Mode::Keyed, 0) => 1024,
        (_, k) => k,
    };
    // Prefill: producers × inflight workers claim batches until N messages are published.
    let claimed = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for p in 0..args.producers {
        let client = Client::connect(opts(args, format!("bench-producer-{}", p + 1))).await?;
        clients.lock().push(client.clone());
        for _ in 0..args.inflight {
            let (client, claimed, filler) = (client.clone(), claimed.clone(), filler.clone());
            let (channel, batch) = (args.channel.clone(), args.batch as u64);
            tasks.push(tokio::spawn(async move {
                loop {
                    let from = claimed.fetch_add(batch, Ordering::Relaxed);
                    if from >= total {
                        return Ok::<(), anyhow::Error>(());
                    }
                    let n = batch.min(total - from);
                    let msgs = (from..from + n)
                        .map(|seq| {
                            let mut b = BytesMut::with_capacity(8 + filler.len());
                            b.put_u64(0);
                            b.put_slice(&filler);
                            PublishMessage {
                                key: (keys > 0).then(|| format!("k{}", seq % keys)),
                                headers: Default::default(),
                                payload: b.freeze(),
                            }
                        })
                        .collect();
                    client.publish_batch(&channel, msgs).await?;
                }
            }));
        }
    }
    for t in tasks {
        t.await??;
    }
    let fill = t0.elapsed().as_secs_f64();
    println!(
        "prefilled {total} msgs in {fill:.2}s ({:.0} msg/s)",
        total as f64 / fill
    );

    let group = format!(
        "bench-drain-{}-{}",
        std::process::id(),
        fluxmq_proto::now_ms()
    );
    let consumed = Arc::new(AtomicU64::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let mut consumers = Vec::new();
    for i in 0..args.consumers.max(1) {
        let client = Client::connect(opts(args, format!("bench-consumer-{}", i + 1))).await?;
        clients.lock().push(client.clone());
        consumers.push((client, i));
    }
    let start = Instant::now();
    let mut tasks = Vec::new();
    for (client, _) in consumers {
        let mut consumer = client
            .subscribe(
                &args.channel,
                &group,
                args.subscribe_options(StartFrom::Earliest),
            )
            .await?;
        let (consumed, done) = (consumed.clone(), done.clone());
        tasks.push(tokio::spawn(async move {
            while let Some(Ok(d)) = consumer.next().await {
                let _ = d.ack().await;
                if consumed.fetch_add(1, Ordering::Relaxed) + 1 == total {
                    done.notify_one();
                }
            }
            drop(client);
        }));
    }
    let mut tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    let mut last = 0u64;
    let finished = loop {
        tokio::select! {
            _ = done.notified() => break true,
            _ = tick.tick() => {
                let cur = consumed.load(Ordering::Relaxed);
                println!("{:>5.1}s  con {:>12} msg/s", start.elapsed().as_secs_f64(), fmt_rate((cur - last) as f64));
                last = cur;
                if start.elapsed() > Duration::from_secs(args.duration.max(60)) {
                    break false;
                }
            }
        }
    };
    let secs = start.elapsed().as_secs_f64();
    let n = consumed.load(Ordering::Relaxed);
    for t in &tasks {
        t.abort();
    }
    println!("\n=== drain summary ===");
    println!(
        "consumed    {n} of {total} msgs in {secs:.3}s: {:.0} msg/s, {:.1} MB/s{}",
        n as f64 / secs,
        n as f64 * args.size as f64 / secs / 1e6,
        if finished { "" } else { " (TIMED OUT)" }
    );
    println!("reconnects  {}", total_reconnects(clients));
    Ok(())
}

/// Moves every per-task interval histogram into one interval histogram and the run total.
fn drain(list: &Mutex<Vec<Hist>>, total: &mut Histogram<u64>) -> Histogram<u64> {
    let mut interval = hist();
    for h in list.lock().iter() {
        let mut h = h.lock();
        let _ = interval.add(&*h);
        h.reset();
    }
    let _ = total.add(&interval);
    interval
}

fn ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

fn pct(h: &Histogram<u64>) -> String {
    if h.is_empty() {
        return "-".into();
    }
    format!(
        "{:.2}/{:.2}/{:.2}",
        ms(h.value_at_quantile(0.5)),
        ms(h.value_at_quantile(0.99)),
        ms(h.value_at_quantile(0.999))
    )
}

fn full(h: &Histogram<u64>) -> String {
    if h.is_empty() {
        return "no samples".into();
    }
    format!(
        "min {:.2}  p50 {:.2}  p90 {:.2}  p99 {:.2}  p99.9 {:.2}  max {:.2}  (n={})",
        ms(h.min()),
        ms(h.value_at_quantile(0.5)),
        ms(h.value_at_quantile(0.9)),
        ms(h.value_at_quantile(0.99)),
        ms(h.value_at_quantile(0.999)),
        ms(h.max()),
        h.len()
    )
}

fn fmt_rate(r: f64) -> String {
    let digits = (r.round() as u64).to_string();
    let groups: Vec<&str> = digits
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(|c| std::str::from_utf8(c).expect("ascii digits"))
        .collect();
    groups.join(",")
}

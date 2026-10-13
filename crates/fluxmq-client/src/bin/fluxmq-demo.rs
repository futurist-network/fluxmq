//! FluxMQ demo traffic generator: a story-driven e-commerce scenario that makes the UI look
//! alive. Everything goes through the real SDK over TCP.
//!
//! ```text
//! cargo run -p fluxmq-client --bin fluxmq-demo -- --addr 127.0.0.1:7420 --speed 1
//! cargo run -p fluxmq-client --bin fluxmq-demo -- --seeds n1:7420,n2:7420,n3:7420
//! ```
//!
//! Every client uses the SDK's automatic reconnect: restart the broker while the demo runs
//! and producers/consumers pause, then carry on (publishes are retried, consumers
//! resubscribe). The demo also waits for the broker if it isn't up yet.
//!
//! The story:
//! * `orders.events` (stream, 8 partitions): `order-api-1..3` publish OrderCreated →
//!   PaymentStarted → PaymentCompleted/PaymentFailed → OrderConfirmed/OrderCancelled, keyed by
//!   order id. Groups `order-processing` (8 healthy instances) and `analytics` (2 instances).
//! * `payments.events` (keyed by customer id): group `payment-processor`; ~1.5 % of messages
//!   fail with "PostgreSQL timeout" (retried), ~0.2 % with "Invalid schema" (straight to DLQ).
//! * `email` (queue): group `email-service` with 4 workers; `worker-03` is slow and sometimes
//!   answers "HTTP 503". The producer rate follows a sine wave, so lag builds up during bursts
//!   and recovers in the troughs, and the slow-instance alert fires for worker-03.
//! * `notifications` (stream) fans out to `push-service` and `sms-service`.
//! * `inventory.updates` (stream): `inventory-indexer` stalls for ~40 s every 3 minutes
//!   (lag / stall alerts).
//! * `audit.logs` is created ~20 s after start (a "channel created" event in the UI).
//!
//! Payloads come in several formats, as in a real system, so the console shows each one:
//! JSON (most), form fields for SMS (`to=…&text=…`, like an SMS gateway), XML from the legacy
//! `NYC-3` warehouse, HTML newsletters and CSV audit exports. `content-type` says which.
//!
//! `--speed` scales every producer rate and divides every processing time, so the health
//! picture stays the same at any speed.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use fluxmq_client::{
    ChannelConfig, ChannelMode, Client, ClientError, ClientOptions, ConnectionState, Delivery,
    MessageBuilder, ReconnectPolicy, StartFrom, SubscribeOptions, message,
};
use fluxmq_proto::RetryPolicy;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(
    name = "fluxmq-demo",
    about = "Realistic synthetic traffic for the FluxMQ UI"
)]
struct Args {
    /// Broker client address.
    #[arg(long, default_value = "127.0.0.1:7420", env = "FLUXMQ_ADDR")]
    addr: String,
    /// Cluster bootstrap addresses (comma-separated); overrides --addr.
    #[arg(long, env = "FLUXMQ_SEEDS", value_delimiter = ',')]
    seeds: Vec<String>,
    /// Rate multiplier (2 = twice the traffic, processing twice as fast).
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// Auth token if the broker has an admin token.
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
}

// ---------------------------------------------------------------------------------------------
// Small deterministic-enough RNG (SplitMix64) so the demo needs no extra dependency.

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ (fluxmq_proto::now_ms().wrapping_mul(0x9E37_79B9_7F4A_7C15)))
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in [0, 1).
    fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.f64()
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        self.f64() < p
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
    /// Log-normal-ish duration around `median_ms` with a long right tail.
    fn latency_ms(&mut self, median_ms: f64, spread: f64) -> f64 {
        // Sum of uniforms ≈ normal; exp gives the tail.
        let n = (0..4).map(|_| self.f64()).sum::<f64>() - 2.0;
        median_ms * (n * spread).exp()
    }
}

// ---------------------------------------------------------------------------------------------

struct Ctx {
    addr: String,
    seeds: Vec<String>,
    token: Option<String>,
    tls: Option<fluxmq_client::TlsOptions>,
    speed: f64,
    cancel: CancellationToken,
    start: Instant,
    produced: AtomicU64,
    consumed: AtomicU64,
    nacked: AtomicU64,
    errors: AtomicU64,
    reconnects: AtomicU64,
}

impl Ctx {
    fn opts(&self, name: &str) -> ClientOptions {
        let mut o = ClientOptions::new(self.addr.clone(), name)
            .seeds(self.seeds.clone())
            .reconnect(
                ReconnectPolicy::default()
                    .backoff(Duration::from_millis(200), Duration::from_secs(3))
                    .retry_initial_connect(true),
            )
            .publish_timeout(Duration::from_secs(60));
        o.auth_token = self.token.clone();
        o.tls = self.tls.clone();
        o
    }
    /// Connects (waiting for the broker if needed) and counts this client's reconnects.
    async fn connect(self: &Arc<Self>, name: &str) -> Result<Client, ClientError> {
        let client = tokio::select! {
            c = Client::connect(self.opts(name)) => c?,
            _ = self.cancel.cancelled() => return Err(ClientError::Closed("demo stopping".into())),
        };
        let mut state = client.state();
        let ctx = self.clone();
        tokio::spawn(async move {
            let mut was_down = false;
            while state.changed().await.is_ok() {
                match *state.borrow_and_update() {
                    ConnectionState::Reconnecting => was_down = true,
                    ConnectionState::Connected if was_down => {
                        was_down = false;
                        ctx.reconnects.fetch_add(1, Ordering::Relaxed);
                    }
                    ConnectionState::Closed => break,
                    _ => {}
                }
            }
        });
        Ok(client)
    }
    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
    /// Processing time scaled by `--speed`.
    fn scaled(&self, ms: f64) -> Duration {
        Duration::from_secs_f64((ms / self.speed).max(0.0) / 1000.0)
    }
    async fn sleep(&self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => true,
            _ = self.cancel.cancelled() => false,
        }
    }
}

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Runs `f` until cancelled, restarting it after errors. The SDK reconnects by itself; this
/// only catches what it surfaces as terminal (e.g. a publish still failing after
/// `publish_timeout`, a deleted channel).
async fn supervise<F, Fut>(ctx: Arc<Ctx>, name: String, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), BoxErr>>,
{
    while !ctx.cancel.is_cancelled() {
        match f().await {
            Ok(()) => break,
            Err(e) => {
                if ctx.cancel.is_cancelled() {
                    break;
                }
                ctx.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(worker = %name, "{e}; retrying in 2 s");
                if !ctx.sleep(Duration::from_secs(2)).await {
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Consumers

#[derive(Clone)]
struct Failure {
    probability: f64,
    reason: &'static str,
    retry: bool,
}

#[derive(Clone, Copy)]
struct Stall {
    first_after: Duration,
    every: Duration,
    length: Duration,
}

#[derive(Clone)]
struct ConsumerSpec {
    channel: &'static str,
    group: &'static str,
    instance: String,
    prefetch: u32,
    /// Deliveries processed concurrently by this instance.
    concurrency: usize,
    median_ms: f64,
    spread: f64,
    failures: Vec<Failure>,
    stall: Option<Stall>,
}

impl ConsumerSpec {
    fn new(channel: &'static str, group: &'static str, instance: impl Into<String>) -> Self {
        Self {
            channel,
            group,
            instance: instance.into(),
            prefetch: 64,
            concurrency: 16,
            median_ms: 3.0,
            spread: 0.4,
            failures: Vec::new(),
            stall: None,
        }
    }
    fn timing(mut self, median_ms: f64, spread: f64) -> Self {
        self.median_ms = median_ms;
        self.spread = spread;
        self
    }
    fn flow(mut self, prefetch: u32, concurrency: usize) -> Self {
        self.prefetch = prefetch;
        self.concurrency = concurrency;
        self
    }
    fn fail(mut self, probability: f64, reason: &'static str, retry: bool) -> Self {
        self.failures.push(Failure {
            probability,
            reason,
            retry,
        });
        self
    }
    fn stall(mut self, stall: Stall) -> Self {
        self.stall = Some(stall);
        self
    }
}

fn spawn_consumer(ctx: &Arc<Ctx>, spec: ConsumerSpec) -> tokio::task::JoinHandle<()> {
    let ctx = ctx.clone();
    tokio::spawn(async move {
        let name = spec.instance.clone();
        let c = ctx.clone();
        supervise(ctx, name, || run_consumer(c.clone(), spec.clone())).await;
    })
}

async fn run_consumer(ctx: Arc<Ctx>, spec: ConsumerSpec) -> Result<(), BoxErr> {
    let client = ctx.connect(&spec.instance).await?;
    let mut consumer = match client
        .subscribe(
            spec.channel,
            spec.group,
            SubscribeOptions {
                prefetch: spec.prefetch,
                start: StartFrom::Latest,
                ..Default::default()
            },
        )
        .await
    {
        Ok(c) => c,
        Err(e) => return Err(format!("subscribe {}/{}: {e}", spec.channel, spec.group).into()),
    };
    tracing::debug!(instance = %spec.instance, "subscribed to {}/{}", spec.channel, spec.group);
    let permits = Arc::new(Semaphore::new(spec.concurrency.max(1)));
    let mut rng = Rng::new(spec.instance.len() as u64 * 7919 + spec.prefetch as u64);
    loop {
        if let Some(stall) = spec.stall
            && let Some(remaining) = stall_remaining(ctx.elapsed(), stall)
        {
            tracing::info!(instance = %spec.instance, "stalling for {:.0}s (simulated GC/deadlock)", remaining.as_secs_f64());
            if !ctx.sleep(remaining).await {
                break;
            }
            tracing::info!(instance = %spec.instance, "recovered from stall");
        }
        let next = tokio::select! {
            n = consumer.next() => n,
            _ = ctx.cancel.cancelled() => break,
        };
        let delivery = match next {
            Some(Ok(d)) => d,
            Some(Err(e)) => return Err(e.into()),
            None => return Err("subscription ended".into()),
        };
        let permit = tokio::select! {
            p = permits.clone().acquire_owned() => p.expect("semaphore never closed"),
            _ = ctx.cancel.cancelled() => break,
        };
        let work = ctx.scaled(rng.latency_ms(spec.median_ms, spec.spread));
        let failure = spec
            .failures
            .iter()
            .find(|f| rng.chance(f.probability))
            .cloned();
        let ctx2 = ctx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            tokio::time::sleep(work).await;
            finish(&ctx2, delivery, failure).await;
        });
    }
    // Graceful: wait for in-flight work, then leave the group.
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        permits.acquire_many(spec.concurrency.max(1) as u32),
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(2), consumer.unsubscribe()).await;
    client.close();
    Ok(())
}

async fn finish(ctx: &Ctx, delivery: Delivery, failure: Option<Failure>) {
    let r = match failure {
        Some(f) => {
            ctx.nacked.fetch_add(1, Ordering::Relaxed);
            delivery.nack(f.reason, f.retry).await
        }
        None => {
            ctx.consumed.fetch_add(1, Ordering::Relaxed);
            delivery.ack().await
        }
    };
    if let Err(e) = r
        && !matches!(e, ClientError::Closed(_))
    {
        tracing::warn!("ack/nack failed: {e}");
    }
}

fn stall_remaining(elapsed: Duration, s: Stall) -> Option<Duration> {
    if elapsed < s.first_after {
        return None;
    }
    let into = (elapsed - s.first_after).as_secs_f64() % s.every.as_secs_f64();
    let left = s.length.as_secs_f64() - into;
    (left > 0.5).then(|| Duration::from_secs_f64(left))
}

// ---------------------------------------------------------------------------------------------
// Producers

const TICK: Duration = Duration::from_millis(20);

/// A generic rate-driven producer: every tick, `rate(t)` msgs/s worth of credit accrues and
/// `make` builds that many messages, published as one batch.
fn spawn_producer<R, M>(
    ctx: &Arc<Ctx>,
    name: &str,
    channel: &'static str,
    rate: R,
    make: M,
) -> tokio::task::JoinHandle<()>
where
    R: Fn(f64) -> f64 + Send + Sync + Clone + 'static,
    M: FnMut(&mut Rng) -> MessageBuilder + Send + Clone + 'static,
{
    let ctx = ctx.clone();
    let name = name.to_string();
    tokio::spawn(async move {
        let c = ctx.clone();
        let n = name.clone();
        supervise(ctx, name, move || {
            run_producer(c.clone(), n.clone(), channel, rate.clone(), make.clone())
        })
        .await;
    })
}

async fn run_producer<R, M>(
    ctx: Arc<Ctx>,
    name: String,
    channel: &'static str,
    rate: R,
    mut make: M,
) -> Result<(), BoxErr>
where
    R: Fn(f64) -> f64,
    M: FnMut(&mut Rng) -> MessageBuilder,
{
    let client = ctx.connect(&name).await?;
    let producer = client.producer(channel);
    let mut rng = Rng::new(name.len() as u64 * 31 + channel.len() as u64);
    let mut credit = rng.f64();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = Instant::now();
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = ctx.cancel.cancelled() => break,
        }
        let now = Instant::now();
        let dt = now.duration_since(last).as_secs_f64();
        last = now;
        credit += rate(ctx.elapsed().as_secs_f64()).max(0.0) * ctx.speed * dt;
        let n = credit.floor() as usize;
        if n == 0 {
            continue;
        }
        credit -= n as f64;
        let batch: Vec<_> = (0..n).map(|_| make(&mut rng)).collect();
        producer.send_batch(batch).await?;
        ctx.produced.fetch_add(n as u64, Ordering::Relaxed);
    }
    client.close();
    Ok(())
}

const CURRENCIES: &[&str] = &["EUR", "EUR", "EUR", "USD", "USD", "GBP", "PLN", "SEK"];
const COUNTRIES: &[&str] = &[
    "DE", "DE", "FR", "NL", "US", "US", "GB", "PL", "ES", "IT", "SE", "MD", "RO",
];

struct PendingOrder {
    due: Instant,
    step: usize,
    failed: bool,
    order_id: String,
    customer_id: String,
    amount: f64,
    currency: &'static str,
    country: &'static str,
    trace_id: String,
}

/// `order-api-N`: each order emits a 4-event sequence spread over ~0.3–2 s.
fn spawn_order_api(ctx: &Arc<Ctx>, n: usize, orders_per_sec: f64) -> tokio::task::JoinHandle<()> {
    let ctx = ctx.clone();
    tokio::spawn(async move {
        let name = format!("order-api-{n}");
        let c = ctx.clone();
        let nm = name.clone();
        supervise(ctx, name, move || {
            run_order_api(c.clone(), nm.clone(), n, orders_per_sec)
        })
        .await;
    })
}

async fn run_order_api(
    ctx: Arc<Ctx>,
    name: String,
    n: usize,
    orders_per_sec: f64,
) -> Result<(), BoxErr> {
    let client = ctx.connect(&name).await?;
    let producer = client.producer("orders.events");
    let mut rng = Rng::new(n as u64 * 104_729);
    let mut seq: u64 = rng.below(1_000_000);
    let mut pending: Vec<PendingOrder> = Vec::new();
    let mut credit = 0.0;
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = Instant::now();
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = ctx.cancel.cancelled() => break,
        }
        let now = Instant::now();
        let dt = now.duration_since(last).as_secs_f64();
        last = now;
        // Gentle diurnal-ish wobble so charts are not flat.
        let t = ctx.elapsed().as_secs_f64();
        let wobble = 1.0 + 0.25 * (t / 47.0 + n as f64).sin() + 0.1 * (t / 7.0).sin();
        credit += orders_per_sec * wobble * ctx.speed * dt;
        while credit >= 1.0 {
            credit -= 1.0;
            seq += 1;
            pending.push(PendingOrder {
                due: now,
                step: 0,
                failed: false,
                order_id: format!("ord-{n}{seq:07}"),
                customer_id: format!("cus-{:05}", rng.below(40_000)),
                amount: (rng.latency_ms(60.0, 0.8) * 100.0).round() / 100.0,
                currency: rng.pick(CURRENCIES),
                country: rng.pick(COUNTRIES),
                trace_id: fluxmq_client::new_trace_id(),
            });
        }
        let mut batch = Vec::new();
        for o in pending.iter_mut().filter(|o| o.due <= now) {
            let event = match (o.step, o.failed) {
                (0, _) => "OrderCreated",
                (1, _) => "PaymentStarted",
                (2, _) => {
                    o.failed = rng.chance(0.04);
                    if o.failed {
                        "PaymentFailed"
                    } else {
                        "PaymentCompleted"
                    }
                }
                (_, true) => "OrderCancelled",
                (_, false) => "OrderConfirmed",
            };
            let mut payload = json!({
                "order_id": o.order_id,
                "customer_id": o.customer_id,
                "amount": o.amount,
                "currency": o.currency,
                "country": o.country,
                "event": event,
                // Same as `event`; lets the documented filter `type = "OrderCreated"` match.
                "type": event,
                "ts": fluxmq_proto::now_ms(),
            });
            if o.step == 0 {
                payload["items"] = json!(1 + rng.below(5));
            }
            if event == "PaymentFailed" {
                payload["reason"] =
                    json!(rng.pick(&["card_declined", "insufficient_funds", "3ds_timeout"]));
            }
            batch.push(
                message(payload.to_string())
                    .key(&o.order_id)
                    .event_type(event)
                    .content_type("application/json")
                    .trace_id(&o.trace_id),
            );
            o.step += 1;
            let delay_ms = match o.step {
                1 => rng.range(50.0, 300.0),
                2 => rng.latency_ms(500.0, 0.5),
                _ => rng.range(50.0, 200.0),
            };
            o.due = now + Duration::from_secs_f64(delay_ms / 1000.0 / ctx.speed);
        }
        pending.retain(|o| o.step < 4);
        if !batch.is_empty() {
            let k = batch.len() as u64;
            producer.send_batch(batch).await?;
            ctx.produced.fetch_add(k, Ordering::Relaxed);
        }
    }
    client.close();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Scenario

fn channel(name: &str, mode: ChannelMode, partitions: u32) -> ChannelConfig {
    let mut c = ChannelConfig::new(name, mode);
    c.partitions = partitions;
    c
}

async fn create_channels(admin: &Client) -> Result<(), ClientError> {
    let mut orders = channel("orders.events", ChannelMode::Stream, 8);
    orders.ordering_key = Some("order_id".into());

    let mut payments = channel("payments.events", ChannelMode::Keyed, 4);
    payments.ordering_key = Some("customer_id".into());
    payments.retry = RetryPolicy {
        max_attempts: 4,
        backoff_ms: vec![500, 2_000, 10_000],
        ack_timeout_ms: 30_000,
    };

    let mut email = channel("email", ChannelMode::Queue, 1);
    email.retry = RetryPolicy {
        max_attempts: 5,
        backoff_ms: vec![1_000, 5_000, 15_000],
        ack_timeout_ms: 30_000,
    };

    let notifications = channel("notifications", ChannelMode::Stream, 4);

    let mut inventory = channel("inventory.updates", ChannelMode::Stream, 4);
    inventory.ordering_key = Some("sku".into());
    // The indexer stalls for ~40 s; don't let the ack timeout turn that into retries.
    inventory.retry.ack_timeout_ms = 120_000;

    for cfg in [orders, payments, email, notifications, inventory] {
        let name = cfg.name.clone();
        let created = admin.ensure_channel(cfg).await?;
        tracing::info!(
            "channel {name} {}",
            if created { "created" } else { "already exists" }
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,fluxmq_client=error".into()),
        )
        .init();
    let args = Args::parse();
    anyhow::ensure!(args.speed > 0.0, "--speed must be > 0");

    let ctx = Arc::new(Ctx {
        addr: args.addr.clone(),
        seeds: args.seeds.clone(),
        token: args.token.clone(),
        tls: fluxmq_client::TlsOptions::from_files(
            args.tls_ca.clone(),
            args.tls_cert.clone(),
            args.tls_key.clone(),
        )
        .map_err(anyhow::Error::msg)?,
        speed: args.speed,
        cancel: CancellationToken::new(),
        start: Instant::now(),
        produced: AtomicU64::new(0),
        consumed: AtomicU64::new(0),
        nacked: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        reconnects: AtomicU64::new(0),
    });

    let admin = ctx.connect("fluxmq-demo-admin").await?;
    tracing::info!(
        "connected to {} ({:?}, {})",
        if args.seeds.is_empty() {
            args.addr.clone()
        } else {
            args.seeds.join(",")
        },
        admin.server_info(),
        match admin.metadata() {
            Some(m) => format!("cluster of {} nodes", m.nodes.len()),
            None => "single node".into(),
        }
    );
    // Log broker outages/recoveries once (the admin connection stands for all clients).
    {
        let mut state = admin.state();
        tokio::spawn(async move {
            while state.changed().await.is_ok() {
                let s = *state.borrow_and_update();
                match s {
                    ConnectionState::Reconnecting => {
                        tracing::warn!("broker connection lost — clients are reconnecting")
                    }
                    ConnectionState::Connected => tracing::info!("broker connection restored"),
                    ConnectionState::Closed => break,
                    ConnectionState::Connecting => {}
                }
            }
        });
    }
    loop {
        match create_channels(&admin).await {
            Ok(()) => break,
            Err(e) if e.is_connection_error() => {
                tracing::warn!("creating channels: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    let mut tasks = Vec::new();

    // --- consumers first, so nothing published by the demo is skipped (they start at Latest).
    for i in 1..=8 {
        tasks.push(spawn_consumer(
            &ctx,
            ConsumerSpec::new(
                "orders.events",
                "order-processing",
                format!("order-processing-{i}"),
            )
            .timing(4.0, 0.45)
            .flow(64, 16),
        ));
    }
    for i in 1..=2 {
        tasks.push(spawn_consumer(
            &ctx,
            ConsumerSpec::new("orders.events", "analytics", format!("analytics-{i}"))
                .timing(1.5, 0.3)
                .flow(256, 64),
        ));
    }
    for i in 1..=3 {
        tasks.push(spawn_consumer(
            &ctx,
            ConsumerSpec::new(
                "payments.events",
                "payment-processor",
                format!("payment-processor-{i}"),
            )
            .timing(9.0, 0.5)
            .flow(64, 16)
            .fail(0.015, "PostgreSQL timeout", true)
            .fail(0.002, "Invalid schema", false),
        ));
    }
    for i in 1..=4 {
        let spec = ConsumerSpec::new("email", "email-service", format!("worker-{i:02}")).flow(4, 1);
        let spec = if i == 3 {
            spec.timing(380.0, 0.35).fail(0.06, "HTTP 503", true)
        } else {
            spec.timing(18.0, 0.3).fail(0.002, "SMTP 421", true)
        };
        tasks.push(spawn_consumer(&ctx, spec));
    }
    for i in 1..=2 {
        tasks.push(spawn_consumer(
            &ctx,
            ConsumerSpec::new("notifications", "push-service", format!("push-service-{i}"))
                .timing(6.0, 0.4)
                .flow(64, 16),
        ));
    }
    tasks.push(spawn_consumer(
        &ctx,
        ConsumerSpec::new("notifications", "sms-service", "sms-service-1")
            .timing(12.0, 0.5)
            .flow(64, 32)
            .fail(0.003, "Twilio rate limit", true),
    ));
    tasks.push(spawn_consumer(
        &ctx,
        ConsumerSpec::new(
            "inventory.updates",
            "inventory-indexer",
            "inventory-indexer-1",
        )
        .timing(8.0, 0.4)
        .flow(32, 8)
        .stall(Stall {
            first_after: Duration::from_secs(60),
            every: Duration::from_secs(180),
            length: Duration::from_secs(40),
        }),
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- producers
    for n in 1..=3 {
        tasks.push(spawn_order_api(&ctx, n, 30.0));
    }
    for n in 1..=2 {
        tasks.push(spawn_producer(
            &ctx,
            &format!("payment-gateway-{n}"),
            "payments.events",
            move |t| 80.0 + 15.0 * (t / 33.0 + n as f64).sin(),
            |rng| {
                let customer = format!("cus-{:05}", rng.below(40_000));
                let kind = *rng.pick(&[
                    "PaymentAuthorized",
                    "PaymentCaptured",
                    "PaymentCaptured",
                    "RefundIssued",
                ]);
                let payload = json!({
                    "payment_id": format!("pay-{:012x}", rng.next_u64() >> 16),
                    "customer_id": customer,
                    "amount": (rng.latency_ms(60.0, 0.8) * 100.0).round() / 100.0,
                    "currency": rng.pick(CURRENCIES),
                    "psp": rng.pick(&["stripe", "adyen", "paypal"]),
                    "ts": fluxmq_proto::now_ms(),
                });
                message(payload.to_string())
                    .key(customer)
                    .event_type(kind)
                    .content_type("application/json")
                    .new_trace_id()
            },
        ));
    }
    tasks.push(spawn_producer(
        &ctx,
        "notification-scheduler",
        "email",
        // Bursty: peaks (~180/s) exceed the group's capacity (~150/s), troughs let it recover.
        |t| 100.0 * (1.0 + 0.8 * (2.0 * std::f64::consts::PI * t / 180.0).sin()),
        |rng| {
            let template = *rng.pick(&[
                "order_confirmation",
                "shipping_update",
                "password_reset",
                "newsletter",
                "invoice",
            ]);
            let to = format!("user{}@example.com", rng.below(100_000));
            if template == "newsletter" {
                // Marketing renders the newsletter itself: the message is the HTML body.
                let html = format!(
                    "<!DOCTYPE html><html><head><title>Autumn sale</title></head><body>\
                     <h1>Up to {}% off</h1><p>Hi {to}, the autumn sale is on.</p>\
                     <p><a href=\"https://shop.example.com/sale\">Shop now</a></p></body></html>",
                    10 + rng.below(5) * 10
                );
                return message(html)
                    .event_type(template)
                    .content_type("text/html; charset=utf-8");
            }
            let payload = json!({
                "to": to,
                "template": template,
                "locale": rng.pick(&["en", "de", "fr", "pl"]),
                "ts": fluxmq_proto::now_ms(),
            });
            message(payload.to_string())
                .event_type(template)
                .content_type("application/json")
        },
    ));
    tasks.push(spawn_producer(
        &ctx,
        "notification-api",
        "notifications",
        |t| 120.0 + 30.0 * (t / 21.0).sin(),
        |rng| {
            let kind = *rng.pick(&["push", "push", "push", "sms", "in_app"]);
            let user = format!("usr-{:06}", rng.below(500_000));
            if kind == "sms" {
                // SMS go out in the gateway's own format: form fields.
                let phone = format!("%2B3736{:07}", rng.below(10_000_000));
                let body = format!(
                    "to={phone}&sender=FluxShop&text=Your+order+is+on+the+way%21+Track%3A+https%3A%2F%2Fshop.example.com%2Ft%2F{}&user_id={user}",
                    rng.below(1_000_000)
                );
                return message(body)
                    .key(user)
                    .event_type(kind)
                    .content_type("application/x-www-form-urlencoded");
            }
            let payload = json!({ "user_id": user, "kind": kind, "title": "Your order is on the way", "ts": fluxmq_proto::now_ms() });
            message(payload.to_string()).key(user).event_type(kind).content_type("application/json")
        },
    ));
    tasks.push(spawn_producer(
        &ctx,
        "warehouse-sync",
        "inventory.updates",
        |_| 40.0,
        |rng| {
            let sku = format!("SKU-{:05}", rng.below(20_000));
            let warehouse = *rng.pick(&["BER-1", "AMS-2", "WAW-1", "NYC-3"]);
            let delta = rng.below(40) as i64 - 20;
            if warehouse == "NYC-3" {
                // The legacy NYC warehouse system still speaks XML.
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><stockAdjustment xmlns=\"urn:wms:v2\" \
                     warehouse=\"{warehouse}\"><sku>{sku}</sku><delta>{delta}</delta>\
                     <reason>{}</reason><ts>{}</ts></stockAdjustment>",
                    rng.pick(&["cycle-count", "damaged", "return", "receiving"]),
                    fluxmq_proto::now_ms()
                );
                return message(xml)
                    .key(sku)
                    .event_type("StockAdjusted")
                    .content_type("application/xml");
            }
            let payload = json!({
                "sku": sku,
                "warehouse": warehouse,
                "delta": delta,
                "ts": fluxmq_proto::now_ms(),
            });
            message(payload.to_string())
                .key(sku)
                .event_type("StockAdjusted")
                .content_type("application/json")
        },
    ));

    // --- audit.logs appears a little later ("Channel created" event in the UI).
    {
        let ctx2 = ctx.clone();
        let admin = admin.clone();
        let mut late = Vec::new();
        tasks.push(tokio::spawn(async move {
            if !ctx2.sleep(Duration::from_secs(20)).await {
                return;
            }
            match admin.ensure_channel(channel("audit.logs", ChannelMode::Stream, 2)).await {
                Ok(_) => tracing::info!("channel audit.logs created"),
                Err(e) => tracing::warn!("creating audit.logs: {e}"),
            }
            late.push(spawn_consumer(
                &ctx2,
                ConsumerSpec::new("audit.logs", "compliance-archiver", "compliance-archiver-1")
                    .timing(5.0, 0.3)
                    .flow(64, 8),
            ));
            ctx2.sleep(Duration::from_millis(300)).await;
            late.push(spawn_producer(
                &ctx2,
                "audit-writer",
                "audit.logs",
                |_| 25.0,
                |rng| {
                    let actor = format!("svc-{}", rng.pick(&["checkout", "admin-portal", "billing", "iam"]));
                    let action = *rng.pick(&["login", "update_price", "refund", "export_report", "role_change"]);
                    if action == "export_report" {
                        // Report exports are archived as the CSV the user downloaded.
                        let mut csv = String::from("date,orders,revenue_eur,refunds\n");
                        for d in 1..=5 {
                            csv.push_str(&format!(
                                "2026-10-0{d},{},{:.2},{}\n",
                                800 + rng.below(400),
                                40_000.0 + rng.range(0.0, 20_000.0),
                                rng.below(30)
                            ));
                        }
                        return message(csv).key(actor).event_type(action).content_type("text/csv");
                    }
                    let payload = json!({ "actor": actor, "action": action, "ip": format!("10.0.{}.{}", rng.below(255), rng.below(255)), "ts": fluxmq_proto::now_ms() });
                    message(payload.to_string()).key(actor).event_type(action).content_type("application/json")
                },
            ));
            for t in late {
                let _ = t.await;
            }
        }));
    }

    // --- status line every 10 s, until Ctrl-C.
    tracing::info!("demo running (speed {}x) — Ctrl-C to stop", args.speed);
    let mut status = tokio::time::interval(Duration::from_secs(10));
    status.tick().await;
    let mut last = (0u64, 0u64);
    loop {
        tokio::select! {
            _ = status.tick() => {
                let p = ctx.produced.load(Ordering::Relaxed);
                let c = ctx.consumed.load(Ordering::Relaxed);
                tracing::info!(
                    "produced {p} ({:.0}/s), acked {c} ({:.0}/s), nacked {}, worker errors {}, reconnects {}",
                    (p - last.0) as f64 / 10.0,
                    (c - last.1) as f64 / 10.0,
                    ctx.nacked.load(Ordering::Relaxed),
                    ctx.errors.load(Ordering::Relaxed),
                    ctx.reconnects.load(Ordering::Relaxed),
                );
                last = (p, c);
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    tracing::info!("shutting down…");
    ctx.cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(8), futures::future::join_all(tasks)).await;
    admin.close();
    tracing::info!(
        "done: produced {}, acked {}, nacked {}",
        ctx.produced.load(Ordering::Relaxed),
        ctx.consumed.load(Ordering::Relaxed),
        ctx.nacked.load(Ordering::Relaxed)
    );
    Ok(())
}

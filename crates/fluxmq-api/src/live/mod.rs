//! Live subscription gateway (v0.10): `GET /api/channels/{name}/live` (WebSocket) and
//! `GET /api/channels/{name}/events` (SSE). Design: `docs/design/v0.10-live-gateway.md`.
//!
//! * [`feed`] — one shared pump per channel broadcasting newly visible batches;
//! * [`sub`] — a subscription: catch-up from storage, then the shared fan-out (holes and
//!   broadcast lag are filled from storage, so nothing visible is ever skipped);
//! * [`ws`] / [`sse`] — transports; [`handler`] — authentication, origin check, limits.

pub mod cursor;
pub mod envelope;
pub mod feed;
pub mod handler;
pub mod params;
pub mod sse;
pub mod sub;
pub mod ws;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use fluxmq_telemetry::prometheus::Writer;
use parking_lot::Mutex;

use envelope::Transport;
use feed::Feed;

/// Limits and knobs (flags `--live-*`, env `FLUXMQ_LIVE_*`).
#[derive(Debug, Clone)]
pub struct LiveConfig {
    /// Live connections per node (WS + SSE); over → 503.
    pub max_connections: usize,
    /// Live connections per token id (bootstrap admin / open mode exempt); over → 429.
    pub max_connections_per_token: usize,
    /// Live subscriptions per channel on this node; over → 429.
    pub max_connections_per_channel: usize,
    /// Subscriptions per WS connection.
    pub max_subscriptions: usize,
    /// Per-connection message rate (token bucket, throttles); 0 = unlimited.
    pub max_msgs_per_sec: u64,
    /// Per-connection byte rate (token bucket, throttles); 0 = unlimited.
    pub max_bytes_per_sec: u64,
    /// Payloads above this are truncated in the envelope.
    pub max_payload_bytes: usize,
    /// Messages per `messages` event.
    pub max_batch: usize,
    /// About this many bytes of envelopes per `messages` event.
    pub max_frame_bytes: usize,
    /// Outbound frames queued per connection.
    pub send_buffer: usize,
    /// A write (or a full send queue) not progressing for this long → `slow` close.
    pub send_timeout: Duration,
    /// WS ping / SSE comment interval.
    pub heartbeat: Duration,
    /// WS: nothing received (not even a pong) for this long → close.
    pub idle_timeout: Duration,
    /// Batches buffered per channel in the shared broadcast ring.
    pub fanout_buffer: usize,
    /// Exact origins (`https://app.example.com`) or `*` allowed besides same-origin.
    pub allowed_origins: Vec<String>,
    /// Min interval between `progress` events of a filtered subscription.
    pub progress_interval: Duration,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_connections_per_token: 1_000,
            max_connections_per_channel: 10_000,
            max_subscriptions: 16,
            max_msgs_per_sec: 10_000,
            max_bytes_per_sec: 16 << 20,
            max_payload_bytes: 64 << 10,
            max_batch: 256,
            max_frame_bytes: 256 << 10,
            send_buffer: 64,
            send_timeout: Duration::from_secs(10),
            heartbeat: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(60),
            fanout_buffer: 1024,
            allowed_origins: Vec::new(),
            progress_interval: Duration::from_secs(2),
        }
    }
}

/// Why a connection or subscription was refused (metrics label + HTTP status).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    NodeLimit,
    TokenLimit,
    ChannelLimit,
    SubscriptionLimit,
    Origin,
    Forbidden,
    Unauthorized,
}

impl Reject {
    pub fn label(self) -> &'static str {
        match self {
            Reject::NodeLimit => "node_limit",
            Reject::TokenLimit => "token_limit",
            Reject::ChannelLimit => "channel_limit",
            Reject::SubscriptionLimit => "subscription_limit",
            Reject::Origin => "origin",
            Reject::Forbidden => "forbidden",
            Reject::Unauthorized => "unauthorized",
        }
    }

    const ALL: [Reject; 7] = [
        Reject::NodeLimit,
        Reject::TokenLimit,
        Reject::ChannelLimit,
        Reject::SubscriptionLimit,
        Reject::Origin,
        Reject::Forbidden,
        Reject::Unauthorized,
    ];
}

/// Counters behind `/metrics` (`fluxmq_live_*`).
#[derive(Default)]
pub struct LiveMetrics {
    pub conns_ws: AtomicI64,
    pub conns_sse: AtomicI64,
    pub subs_catchup: AtomicI64,
    pub subs_live: AtomicI64,
    pub msgs_ws: AtomicU64,
    pub msgs_sse: AtomicU64,
    pub bytes_ws: AtomicU64,
    pub bytes_sse: AtomicU64,
    pub slow_ws: AtomicU64,
    pub slow_sse: AtomicU64,
    pub gaps: AtomicU64,
    rejections: [AtomicU64; 7],
}

impl LiveMetrics {
    pub fn reject(&self, r: Reject) {
        let i = Reject::ALL.iter().position(|x| *x == r).unwrap_or(0);
        self.rejections[i].fetch_add(1, Ordering::Relaxed);
    }

    pub fn rejections(&self, r: Reject) -> u64 {
        let i = Reject::ALL.iter().position(|x| *x == r).unwrap_or(0);
        self.rejections[i].load(Ordering::Relaxed)
    }

    pub fn sent(&self, t: Transport, msgs: u32, bytes: usize) {
        let (m, b) = match t {
            Transport::Ws => (&self.msgs_ws, &self.bytes_ws),
            Transport::Sse => (&self.msgs_sse, &self.bytes_sse),
        };
        if msgs > 0 {
            m.fetch_add(u64::from(msgs), Ordering::Relaxed);
        }
        b.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn slow(&self, t: Transport) {
        match t {
            Transport::Ws => &self.slow_ws,
            Transport::Sse => &self.slow_sse,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn conns(&self, t: Transport) -> &AtomicI64 {
        match t {
            Transport::Ws => &self.conns_ws,
            Transport::Sse => &self.conns_sse,
        }
    }
}

struct Inner {
    cfg: LiveConfig,
    feeds: Mutex<HashMap<String, Weak<Feed>>>,
    conns: Mutex<Counts>,
    metrics: LiveMetrics,
}

#[derive(Default)]
struct Counts {
    total: usize,
    per_token: HashMap<String, usize>,
    per_channel: HashMap<String, usize>,
}

/// The gateway's shared state. Cheap to clone.
#[derive(Clone)]
pub struct LiveService {
    inner: Arc<Inner>,
}

impl Default for LiveService {
    fn default() -> Self {
        Self::new(LiveConfig::default())
    }
}

impl LiveService {
    pub fn new(cfg: LiveConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                cfg,
                feeds: Mutex::new(HashMap::new()),
                conns: Mutex::new(Counts::default()),
                metrics: LiveMetrics::default(),
            }),
        }
    }

    pub fn config(&self) -> &LiveConfig {
        &self.inner.cfg
    }

    pub fn metrics(&self) -> &LiveMetrics {
        &self.inner.metrics
    }

    /// Live connections on this node (WS + SSE).
    pub fn connections(&self) -> usize {
        self.inner.conns.lock().total
    }

    /// Channels with an active shared feed.
    pub fn feeds(&self) -> usize {
        self.inner
            .feeds
            .lock()
            .values()
            .filter(|w| w.strong_count() > 0)
            .count()
    }

    /// Takes a connection slot (node + per-token). `token` = the token id (`None` for the
    /// bootstrap admin / open mode: exempt from the per-token cap).
    pub fn acquire_conn(&self, token: Option<&str>, t: Transport) -> Result<ConnSlot, Reject> {
        let cfg = &self.inner.cfg;
        let mut c = self.inner.conns.lock();
        if c.total >= cfg.max_connections {
            drop(c);
            self.inner.metrics.reject(Reject::NodeLimit);
            return Err(Reject::NodeLimit);
        }
        if let Some(tok) = token {
            let n = c.per_token.get(tok).copied().unwrap_or(0);
            if n >= cfg.max_connections_per_token {
                drop(c);
                self.inner.metrics.reject(Reject::TokenLimit);
                return Err(Reject::TokenLimit);
            }
            c.per_token.insert(tok.to_string(), n + 1);
        }
        c.total += 1;
        drop(c);
        self.inner.metrics.conns(t).fetch_add(1, Ordering::Relaxed);
        Ok(ConnSlot {
            svc: self.clone(),
            token: token.map(str::to_string),
            transport: t,
        })
    }

    /// Takes a per-channel subscription slot.
    pub fn acquire_channel(&self, channel: &str) -> Result<ChannelSlot, Reject> {
        let mut c = self.inner.conns.lock();
        let n = c.per_channel.get(channel).copied().unwrap_or(0);
        if n >= self.inner.cfg.max_connections_per_channel {
            drop(c);
            self.inner.metrics.reject(Reject::ChannelLimit);
            return Err(Reject::ChannelLimit);
        }
        c.per_channel.insert(channel.to_string(), n + 1);
        Ok(ChannelSlot {
            svc: self.clone(),
            channel: channel.to_string(),
        })
    }

    /// The shared feed of `reader`'s channel (started on first use).
    pub fn feed(&self, reader: &fluxmq_core::LiveReader) -> Arc<Feed> {
        let mut feeds = self.inner.feeds.lock();
        if let Some(f) = feeds.get(reader.channel()).and_then(Weak::upgrade)
            && !f.is_closed()
        {
            return f;
        }
        let f = Feed::start(reader.clone(), &self.inner.cfg);
        feeds.retain(|_, w| w.strong_count() > 0);
        feeds.insert(reader.channel().to_string(), Arc::downgrade(&f));
        f
    }

    /// This node's gateway for `GET /api/live/status`.
    pub fn status(&self, node: &str) -> fluxmq_proto::live::LiveNodeStatus {
        use fluxmq_proto::live::{LiveChannelCount, LiveLimits, LiveNodeStatus, LiveRejection};
        let cfg = &self.inner.cfg;
        let m = &self.inner.metrics;
        let g = |a: &AtomicI64| a.load(Ordering::Relaxed).max(0) as u64;
        let c = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut channels: Vec<LiveChannelCount> = self
            .inner
            .conns
            .lock()
            .per_channel
            .iter()
            .map(|(ch, n)| LiveChannelCount {
                channel: ch.clone(),
                subscriptions: *n as u64,
            })
            .collect();
        channels.sort_by(|a, b| {
            b.subscriptions
                .cmp(&a.subscriptions)
                .then_with(|| a.channel.cmp(&b.channel))
        });
        LiveNodeStatus {
            node: node.to_string(),
            limits: LiveLimits {
                max_connections: cfg.max_connections as u64,
                max_connections_per_token: cfg.max_connections_per_token as u64,
                max_connections_per_channel: cfg.max_connections_per_channel as u64,
                max_subscriptions: cfg.max_subscriptions as u64,
                max_msgs_per_sec: cfg.max_msgs_per_sec,
                max_bytes_per_sec: cfg.max_bytes_per_sec,
                max_payload_bytes: cfg.max_payload_bytes as u64,
                max_batch: cfg.max_batch as u64,
                send_buffer: cfg.send_buffer as u64,
                send_timeout_secs: cfg.send_timeout.as_secs(),
                heartbeat_secs: cfg.heartbeat.as_secs(),
                idle_timeout_secs: cfg.idle_timeout.as_secs(),
                fanout_buffer: cfg.fanout_buffer as u64,
                allowed_origins: cfg.allowed_origins.clone(),
            },
            connections_ws: g(&m.conns_ws),
            connections_sse: g(&m.conns_sse),
            subscriptions_catchup: g(&m.subs_catchup),
            subscriptions_live: g(&m.subs_live),
            feeds: self.feeds() as u64,
            messages_sent: c(&m.msgs_ws) + c(&m.msgs_sse),
            bytes_sent: c(&m.bytes_ws) + c(&m.bytes_sse),
            slow_disconnects: c(&m.slow_ws) + c(&m.slow_sse),
            gaps: c(&m.gaps),
            rejections: Reject::ALL
                .iter()
                .map(|r| (r.label(), m.rejections(*r)))
                .filter(|(_, n)| *n > 0)
                .map(|(reason, count)| LiveRejection {
                    reason: reason.to_string(),
                    count,
                })
                .collect(),
            channels,
        }
    }

    /// `fluxmq_live_*` Prometheus families.
    pub fn prometheus(&self, node: &str) -> String {
        let m = &self.inner.metrics;
        let mut w = Writer::default();
        let g = |a: &AtomicI64| a.load(Ordering::Relaxed).max(0) as f64;
        let c = |a: &AtomicU64| a.load(Ordering::Relaxed) as f64;
        w.family(
            "fluxmq_live_connections",
            "gauge",
            "Open live subscription connections.",
        );
        for (ep, v) in [("ws", g(&m.conns_ws)), ("sse", g(&m.conns_sse))] {
            w.sample(
                "fluxmq_live_connections",
                &[("node", node), ("endpoint", ep)],
                v,
            );
        }
        w.family(
            "fluxmq_live_subscriptions",
            "gauge",
            "Live subscriptions by state: catchup (reading storage) or live (shared fan-out).",
        );
        for (st, v) in [("catchup", g(&m.subs_catchup)), ("live", g(&m.subs_live))] {
            w.sample(
                "fluxmq_live_subscriptions",
                &[("node", node), ("state", st)],
                v,
            );
        }
        w.family(
            "fluxmq_live_feeds",
            "gauge",
            "Channels with a shared live feed.",
        );
        w.sample("fluxmq_live_feeds", &[("node", node)], self.feeds() as f64);
        for (name, help, ws, sse) in [
            (
                "fluxmq_live_messages_sent_total",
                "Messages sent to live subscribers.",
                c(&m.msgs_ws),
                c(&m.msgs_sse),
            ),
            (
                "fluxmq_live_bytes_sent_total",
                "Bytes of events sent to live subscribers.",
                c(&m.bytes_ws),
                c(&m.bytes_sse),
            ),
            (
                "fluxmq_live_slow_disconnects_total",
                "Live connections closed because the client did not keep up.",
                c(&m.slow_ws),
                c(&m.slow_sse),
            ),
        ] {
            w.family(name, "counter", help);
            w.sample(name, &[("node", node), ("endpoint", "ws")], ws);
            w.sample(name, &[("node", node), ("endpoint", "sse")], sse);
        }
        w.family(
            "fluxmq_live_rejections_total",
            "counter",
            "Live connections/subscriptions refused, by reason.",
        );
        for r in Reject::ALL {
            w.sample(
                "fluxmq_live_rejections_total",
                &[("node", node), ("reason", r.label())],
                m.rejections(r) as f64,
            );
        }
        w.family(
            "fluxmq_live_gaps_total",
            "counter",
            "Ranges reported to live subscribers as removed by retention.",
        );
        w.sample("fluxmq_live_gaps_total", &[("node", node)], c(&m.gaps));
        w.finish()
    }
}

/// A node/token connection slot; released on drop.
pub struct ConnSlot {
    svc: LiveService,
    token: Option<String>,
    transport: Transport,
}

impl Drop for ConnSlot {
    fn drop(&mut self) {
        let mut c = self.svc.inner.conns.lock();
        c.total = c.total.saturating_sub(1);
        if let Some(t) = &self.token
            && let Some(n) = c.per_token.get_mut(t)
        {
            *n = n.saturating_sub(1);
            if *n == 0 {
                c.per_token.remove(t);
            }
        }
        drop(c);
        self.svc
            .inner
            .metrics
            .conns(self.transport)
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// A per-channel subscription slot; released on drop.
pub struct ChannelSlot {
    svc: LiveService,
    channel: String,
}

impl Drop for ChannelSlot {
    fn drop(&mut self) {
        let mut c = self.svc.inner.conns.lock();
        if let Some(n) = c.per_channel.get_mut(&self.channel) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                c.per_channel.remove(&self.channel);
            }
        }
    }
}

/// Per-connection token bucket over messages and bytes (burst = one second). Throttles by
/// sleeping; never drops.
pub struct RateLimiter {
    msgs_rate: u64,
    bytes_rate: u64,
    state: Mutex<(f64, f64, tokio::time::Instant)>,
}

impl RateLimiter {
    pub fn new(msgs_rate: u64, bytes_rate: u64) -> Self {
        Self {
            msgs_rate,
            bytes_rate,
            state: Mutex::new((
                msgs_rate as f64,
                bytes_rate as f64,
                tokio::time::Instant::now(),
            )),
        }
    }

    /// Debits `msgs`/`bytes`; returns how long the caller must wait first.
    fn debit(&self, msgs: u64, bytes: u64) -> Duration {
        if self.msgs_rate == 0 && self.bytes_rate == 0 {
            return Duration::ZERO;
        }
        let mut st = self.state.lock();
        let now = tokio::time::Instant::now();
        let dt = now.duration_since(st.2).as_secs_f64();
        st.2 = now;
        let mut wait = 0f64;
        if self.msgs_rate > 0 {
            let r = self.msgs_rate as f64;
            st.0 = (st.0 + dt * r).min(r) - msgs as f64;
            if st.0 < 0.0 {
                wait = wait.max(-st.0 / r);
            }
        }
        if self.bytes_rate > 0 {
            let r = self.bytes_rate as f64;
            st.1 = (st.1 + dt * r).min(r) - bytes as f64;
            if st.1 < 0.0 {
                wait = wait.max(-st.1 / r);
            }
        }
        Duration::from_secs_f64(wait)
    }

    pub async fn acquire(&self, msgs: u64, bytes: u64) {
        let wait = self.debit(msgs, bytes);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_enforce_limits() {
        let svc = LiveService::new(LiveConfig {
            max_connections: 3,
            max_connections_per_token: 2,
            max_connections_per_channel: 1,
            ..Default::default()
        });
        let a = svc.acquire_conn(Some("t"), Transport::Ws).unwrap();
        let _b = svc.acquire_conn(Some("t"), Transport::Sse).unwrap();
        assert_eq!(
            svc.acquire_conn(Some("t"), Transport::Ws).err(),
            Some(Reject::TokenLimit)
        );
        let _c = svc.acquire_conn(None, Transport::Ws).unwrap();
        assert_eq!(
            svc.acquire_conn(None, Transport::Ws).err(),
            Some(Reject::NodeLimit)
        );
        drop(a);
        assert_eq!(svc.connections(), 2);
        let _d = svc.acquire_conn(Some("t"), Transport::Ws).unwrap();
        let ch = svc.acquire_channel("x").unwrap();
        assert_eq!(svc.acquire_channel("x").err(), Some(Reject::ChannelLimit));
        drop(ch);
        assert!(svc.acquire_channel("x").is_ok());
        assert_eq!(svc.metrics().rejections(Reject::NodeLimit), 1);
        let text = svc.prometheus("n1");
        assert!(text.contains("fluxmq_live_connections{node=\"n1\",endpoint=\"ws\"} 2"));
        assert!(text.contains("reason=\"token_limit\"} 1"));
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_throttles() {
        let l = RateLimiter::new(100, 0);
        let t0 = tokio::time::Instant::now();
        l.acquire(100, 0).await; // the burst
        assert!(t0.elapsed() < Duration::from_millis(5));
        l.acquire(50, 0).await;
        let e = t0.elapsed();
        assert!(
            e >= Duration::from_millis(490) && e < Duration::from_millis(600),
            "{e:?}"
        );
        assert!(RateLimiter::new(0, 0).debit(1 << 40, 1 << 40).is_zero());
    }
}

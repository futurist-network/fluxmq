//! One live subscription (design §3.3): start positions, catch-up from storage, then the
//! shared fan-out. It subscribes to the feed **before** catching up and fills every hole (a
//! late join, a lagged receiver) from storage, so nothing visible is ever skipped and nothing
//! is delivered twice. Frames go to the connection's bounded queue; a queue that stays full
//! for `send_timeout` ends the subscription as slow.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use fluxmq_core::LiveReader;
use fluxmq_proto::Message;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, MissedTickBehavior};

use super::cursor::{self, CursorError};
use super::envelope::{self, Frame, Transport};
use super::feed::{Batch, Feed, READ_BYTES, READ_RECORDS, read_range};
use super::params::{Filter, StartFrom, SubSpec};
use super::{LiveService, RateLimiter};

/// How often a live subscription re-checks unserved partitions and channel deletion.
const RECHECK: Duration = Duration::from_secs(1);

/// Why a subscription ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubEnd {
    /// The connection's queue is gone (client closed, connection ended).
    Gone,
    /// The client did not drain its queue within `send_timeout`.
    Slow,
    /// The channel was deleted.
    Deleted,
}

/// Where a subscription starts, resolved against the channel (before any frame is sent, so
/// errors can still become HTTP statuses).
#[derive(Debug, Clone)]
pub struct Start {
    pub partitions: Vec<u32>,
    pub next: BTreeMap<u32, u64>,
    /// `(partition, requested, actual)`: a resume position beyond the log end.
    pub resets: Vec<(u32, u64, u64)>,
}

/// Resolution failures (400 / `error` event).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartError {
    BadPartition(u32),
    Cursor(CursorError),
    Io(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartError::BadPartition(p) => write!(f, "partition {p} does not exist"),
            StartError::Cursor(e) => write!(f, "{e}"),
            StartError::Io(e) => write!(f, "storage error: {e}"),
        }
    }
}

/// Resolves the start position of every partition of the subscription.
pub async fn resolve(reader: &LiveReader, spec: &SubSpec) -> Result<Start, StartError> {
    let n = reader.partitions();
    let partitions: Vec<u32> = match &spec.partitions {
        Some(ps) => {
            if let Some(&bad) = ps.iter().find(|&&p| p >= n) {
                return Err(StartError::BadPartition(bad));
            }
            ps.clone()
        }
        None => (0..n).collect(),
    };
    let from_cursor: BTreeMap<u32, u64> = match &spec.cursor {
        Some(c) => cursor::decode(reader.channel(), c)
            .map_err(StartError::Cursor)?
            .positions
            .into_iter()
            .collect(),
        None => BTreeMap::new(),
    };
    let mut next = BTreeMap::new();
    let mut resets = Vec::new();
    for &p in &partitions {
        // Latest of a partition this node does not serve: its local log end (carried in the
        // cursor; the client may resume it on another node).
        let latest = reader.visible_end(p).unwrap_or_else(|| reader.log_end(p));
        let (pos, explicit) = match (from_cursor.get(&p), &spec.from) {
            (Some(&o), _) => (o, true),
            (None, StartFrom::Latest) => (latest, false),
            (None, StartFrom::Earliest) => (reader.low(p), false),
            (None, StartFrom::Time(ts)) => {
                let r = reader.clone();
                let ts = *ts;
                let o = tokio::task::spawn_blocking(move || r.offset_for_time(p, ts))
                    .await
                    .map_err(|e| StartError::Io(e.to_string()))?
                    .map_err(|e| StartError::Io(e.to_string()))?;
                (o, false)
            }
            (None, StartFrom::Offsets(m)) => match m.get(&p) {
                Some(&o) => (o, true),
                None => (latest, false),
            },
        };
        let pos = match reader.visible_end(p) {
            Some(end) if explicit && pos > end => {
                resets.push((p, pos, end));
                end
            }
            _ => pos,
        };
        next.insert(p, pos);
    }
    Ok(Start {
        partitions,
        next,
        resets,
    })
}

/// Decrements a state gauge on drop.
struct Gauge(LiveService, fn(&super::LiveMetrics) -> &AtomicI64);

impl Gauge {
    fn new(svc: &LiveService, which: fn(&super::LiveMetrics) -> &AtomicI64) -> Self {
        which(svc.metrics()).fetch_add(1, Ordering::Relaxed);
        Gauge(svc.clone(), which)
    }
}

impl Drop for Gauge {
    fn drop(&mut self) {
        (self.1)(self.0.metrics()).fetch_sub(1, Ordering::Relaxed);
    }
}

/// A running subscription.
pub struct Sub {
    pub id: String,
    pub transport: Transport,
    pub reader: LiveReader,
    pub svc: LiveService,
    pub filter: Filter,
    pub out: mpsc::Sender<Frame>,
    pub rate: Arc<RateLimiter>,
    next: BTreeMap<u32, u64>,
    partitions: Vec<u32>,
    unavailable: BTreeSet<u32>,
    crc: u32,
    /// Filtered-out messages moved the position since the last event.
    progress_due: bool,
    last_event: Instant,
}

impl Sub {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        transport: Transport,
        reader: LiveReader,
        svc: LiveService,
        filter: Filter,
        out: mpsc::Sender<Frame>,
        rate: Arc<RateLimiter>,
        start: &Start,
    ) -> Self {
        let crc = cursor::channel_crc(reader.channel());
        Sub {
            id,
            transport,
            reader,
            svc,
            filter,
            out,
            rate,
            next: start.next.clone(),
            partitions: start.partitions.clone(),
            unavailable: BTreeSet::new(),
            crc,
            progress_due: false,
            last_event: Instant::now(),
        }
    }

    fn cursor(&self) -> String {
        cursor::encode(self.crc, self.next.iter().map(|(&p, &o)| (p, o)))
    }

    async fn send(&mut self, frame: Frame) -> Result<(), SubEnd> {
        let len = frame.len();
        let msgs = frame.msgs;
        if msgs > 0 {
            self.rate.acquire(u64::from(msgs), len as u64).await;
        }
        let timeout = self.svc.config().send_timeout;
        match tokio::time::timeout(timeout, self.out.send(frame)).await {
            Ok(Ok(())) => {
                self.svc.metrics().sent(self.transport, msgs, len);
                self.last_event = Instant::now();
                Ok(())
            }
            Ok(Err(_)) => Err(SubEnd::Gone),
            Err(_) => Err(SubEnd::Slow),
        }
    }

    async fn event(
        &mut self,
        ty: &str,
        mut body: serde_json::Value,
        with_cursor: bool,
    ) -> Result<(), SubEnd> {
        let cursor = with_cursor.then(|| self.cursor());
        body["type"] = json!(ty);
        body["sub"] = json!(self.id);
        if let Some(c) = &cursor {
            body["cursor"] = json!(c);
        }
        let frame = envelope::frame(self.transport, ty, cursor.as_deref(), body.to_string(), 0);
        self.send(frame).await
    }

    /// Runs until the connection goes away, the client is too slow or the channel is deleted.
    pub async fn run(mut self, feed: Arc<Feed>, resets: Vec<(u32, u64, u64)>) -> SubEnd {
        // First: no batch published from now on can be missed.
        let mut rx = feed.subscribe();
        match self.run_inner(&feed, &mut rx, resets).await {
            Ok(()) => SubEnd::Gone,
            Err(e) => e,
        }
    }

    async fn run_inner(
        &mut self,
        feed: &Feed,
        rx: &mut broadcast::Receiver<Arc<Batch>>,
        resets: Vec<(u32, u64, u64)>,
    ) -> Result<(), SubEnd> {
        let cfg = self.svc.config().clone();
        let hello = json!({
            "channel": self.reader.channel(),
            "partitions": self.partitions,
            "limits": {
                "max_batch": cfg.max_batch,
                "max_payload_bytes": cfg.max_payload_bytes,
                "max_msgs_per_sec": cfg.max_msgs_per_sec,
                "max_bytes_per_sec": cfg.max_bytes_per_sec,
                "heartbeat_secs": cfg.heartbeat.as_secs(),
            },
        });
        self.event("hello", hello, true).await?;
        for (p, requested, actual) in resets {
            let body = json!({ "partition": p, "requested": requested, "actual": actual });
            self.event("reset", body, true).await?;
        }
        self.check_unavailable().await?;
        {
            let _g = Gauge::new(&self.svc, |m| &m.subs_catchup);
            self.catch_up_all().await?;
        }
        let _g = Gauge::new(&self.svc, |m| &m.subs_live);
        let mut tick = tokio::time::interval_at(Instant::now() + RECHECK, RECHECK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                r = rx.recv() => match r {
                    Ok(b) => self.on_batch(&b).await?,
                    // The ring overran us: storage has everything, read it from there.
                    Err(broadcast::error::RecvError::Lagged(_)) => self.catch_up_all().await?,
                    Err(broadcast::error::RecvError::Closed) => return Err(SubEnd::Deleted),
                },
                _ = tick.tick() => {
                    if self.reader.is_closed() || feed.is_closed() {
                        return Err(SubEnd::Deleted);
                    }
                    self.check_unavailable().await?;
                    self.flush_progress(cfg.progress_interval).await?;
                }
                () = self.out.closed() => return Err(SubEnd::Gone),
            }
        }
    }

    /// Reports partitions that stopped (or started) being served here; newly served ones
    /// catch up from storage.
    async fn check_unavailable(&mut self) -> Result<(), SubEnd> {
        let mut newly_down = Vec::new();
        let mut back = Vec::new();
        for &p in &self.partitions {
            let served = self.reader.visible_end(p).is_some();
            match (served, self.unavailable.contains(&p)) {
                (false, false) => newly_down.push(p),
                (true, true) => back.push(p),
                _ => {}
            }
        }
        for p in &back {
            self.unavailable.remove(p);
        }
        if !newly_down.is_empty() {
            self.unavailable.extend(newly_down.iter().copied());
            let parts: Vec<serde_json::Value> = newly_down
                .iter()
                .map(|&p| {
                    json!({ "partition": p, "leader": self.reader.leader_hint(p), "leader_http": null })
                })
                .collect();
            self.event("unavailable", json!({ "partitions": parts }), true)
                .await?;
        }
        for p in back {
            if let Some(end) = self.reader.visible_end(p) {
                self.catch_up(p, end).await?;
            }
        }
        Ok(())
    }

    /// Catches every served partition up to its visible end, one chunk per partition in turn.
    async fn catch_up_all(&mut self) -> Result<(), SubEnd> {
        loop {
            let mut progressed = false;
            for p in self.partitions.clone() {
                if self.unavailable.contains(&p) {
                    continue;
                }
                let Some(end) = self.reader.visible_end(p) else {
                    continue;
                };
                if self.next[&p] < end && self.read_chunk(p, end).await? {
                    progressed = true;
                }
            }
            if !progressed {
                return Ok(());
            }
        }
    }

    /// Delivers `[next[p], to)` of `p` from storage.
    async fn catch_up(&mut self, p: u32, to: u64) -> Result<(), SubEnd> {
        while self.next[&p] < to {
            if !self.read_chunk(p, to).await? {
                break;
            }
        }
        Ok(())
    }

    /// One bounded storage read of `p` below `to`; `false` when nothing more is readable.
    async fn read_chunk(&mut self, p: u32, to: u64) -> Result<bool, SubEnd> {
        let next = self.next[&p];
        let low = self.reader.low(p);
        if next < low {
            self.gap(p, next, low.min(to)).await?;
            if self.next[&p] >= to {
                return Ok(true);
            }
        }
        let from = self.next[&p];
        let msgs = match read_range(&self.reader, p, from, READ_RECORDS, READ_BYTES).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(channel = self.reader.channel(), partition = p, error = %e, "live catch-up read failed");
                return Ok(false);
            }
        };
        let msgs: Vec<Arc<Message>> = msgs.into_iter().take_while(|m| m.offset < to).collect();
        if msgs.is_empty() {
            return Ok(false);
        }
        self.deliver(p, &msgs, None).await?;
        Ok(true)
    }

    async fn on_batch(&mut self, b: &Batch) -> Result<(), SubEnd> {
        let p = b.partition;
        let Some(&next) = self.next.get(&p) else {
            return Ok(());
        };
        if self.unavailable.contains(&p) || b.next <= next {
            return Ok(());
        }
        if b.from > next {
            // A hole before this batch (joined late or lagged): fill it from storage.
            self.catch_up(p, b.from).await?;
        }
        let next = self.next[&p];
        let skip = b.messages.partition_point(|m| m.offset < next);
        if skip < b.messages.len() {
            let envs = b.envelopes();
            self.deliver(p, &b.messages[skip..], Some(&envs[skip..]))
                .await?;
        }
        Ok(())
    }

    async fn gap(&mut self, p: u32, from: u64, to: u64) -> Result<(), SubEnd> {
        if to <= from {
            return Ok(());
        }
        self.next.insert(p, to);
        self.svc.metrics().gaps.fetch_add(1, Ordering::Relaxed);
        let body = json!({ "partition": p, "from": from, "to": to, "lost": to - from });
        self.event("gap", body, true).await
    }

    /// Sends `msgs` (consecutive, ascending, all ≥ `next[p]`) that pass the filter, in events
    /// of at most `max_batch` messages / about `max_frame_bytes`.
    async fn deliver(
        &mut self,
        p: u32,
        msgs: &[Arc<Message>],
        envs: Option<&[String]>,
    ) -> Result<(), SubEnd> {
        let Some(first) = msgs.first() else {
            return Ok(());
        };
        if first.offset > self.next[&p] {
            let from = self.next[&p];
            self.gap(p, from, first.offset).await?;
        }
        let (max_batch, max_frame, max_payload) = {
            let c = self.svc.config();
            (c.max_batch.max(1), c.max_frame_bytes, c.max_payload_bytes)
        };
        let mut chunk: Vec<Cow<'_, str>> = Vec::new();
        let mut chunk_bytes = 0usize;
        for (i, m) in msgs.iter().enumerate() {
            if !self.filter.is_empty() && !self.filter.matches(m) {
                self.next.insert(p, m.offset + 1);
                self.progress_due = true;
                continue;
            }
            let env = match envs {
                Some(e) => Cow::Borrowed(e[i].as_str()),
                None => Cow::Owned(envelope::envelope_json(m, max_payload)),
            };
            chunk_bytes += env.len() + 1;
            chunk.push(env);
            self.next.insert(p, m.offset + 1);
            if chunk.len() >= max_batch || chunk_bytes >= max_frame {
                self.send_messages(std::mem::take(&mut chunk), chunk_bytes)
                    .await?;
                chunk_bytes = 0;
            }
        }
        if !chunk.is_empty() {
            self.send_messages(chunk, chunk_bytes).await?;
        }
        Ok(())
    }

    async fn send_messages(
        &mut self,
        chunk: Vec<Cow<'_, str>>,
        bytes: usize,
    ) -> Result<(), SubEnd> {
        let cursor = self.cursor();
        let n = chunk.len() as u32;
        let json =
            envelope::messages_json(&self.id, &cursor, chunk.iter().map(|c| c.as_ref()), bytes);
        let frame = envelope::frame(self.transport, "messages", Some(&cursor), json, n);
        self.progress_due = false;
        self.send(frame).await
    }

    async fn flush_progress(&mut self, every: Duration) -> Result<(), SubEnd> {
        if self.progress_due && self.last_event.elapsed() >= every {
            self.progress_due = false;
            self.event("progress", json!({}), true).await?;
        }
        Ok(())
    }
}

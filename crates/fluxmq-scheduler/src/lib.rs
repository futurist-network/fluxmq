//! Delayed messages (v0.12). Contract and guarantees: [`fluxmq_proto::scheduled`].
//!
//! * [`SchedulerService::publish`] is the publish entry point of the client protocol and REST:
//!   messages without delay headers go straight to [`Cluster::publish`] (one map lookup per
//!   message, nothing else); delayed ones are appended to `$scheduled` instead (same
//!   durability), with their channel and due time in headers.
//! * One worker per `$scheduled` partition this node leads (reconciled every 500 ms, so a new
//!   leader takes over after a failover). It follows the partition from the lowest pending
//!   offset (persisted in the meta store as `scheduler/low/<p>` every few seconds), keeps
//!   `(offset, due, channel)` per pending entry in memory, publishes due entries to their
//!   channels in batches and appends a `done` control record for them.
//! * Cancel / release now append a control record to the entry's partition (through its
//!   leader); the worker applies it.

pub mod state;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fluxmq_cluster::Cluster;
use fluxmq_core::Broker;
use fluxmq_meta::MetaStoreExt;
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::model::{ChannelConfig, ChannelMode, RetentionPolicy, headers};
use fluxmq_proto::scheduled::{
    DELAY_HEADER, DELIVER_AT_HEADER, DUE_HEADER, NodeScheduled, OP_HEADER, REF_HEADER,
    SCHEDULED_CHANNEL, SCHEDULED_FOR_HEADER, SCHEDULED_ID_HEADER, SCHEDULED_PARTITIONS,
    SCHEDULED_RETENTION_SECS, ScheduledChannel, ScheduledMessage, TARGET_HEADER, Timing,
    is_delayed, timing,
};
use fluxmq_proto::wire::{ErrorCode, WireError};
use fluxmq_proto::{Message, MessageId, PublishMessage, now_ms};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::state::PartitionState;

/// Entries released per publish round.
const RELEASE_BATCH: usize = 256;
/// Follow / wake cadence when nothing is due sooner.
const TICK: Duration = Duration::from_millis(100);
/// Records read per follow step.
const READ_BATCH: u32 = 1000;
/// Wait before retrying a failed release.
const RETRY_AFTER_MS: u64 = 1000;
const RECONCILE: Duration = Duration::from_millis(500);
const WATERMARK_EVERY: Duration = Duration::from_secs(5);

fn watermark_key(p: u32) -> String {
    format!("scheduler/low/{p}")
}

pub fn channel_config() -> ChannelConfig {
    let mut c = ChannelConfig::new(SCHEDULED_CHANNEL, ChannelMode::Stream);
    c.partitions = SCHEDULED_PARTITIONS;
    c.retention = RetentionPolicy {
        max_age_secs: Some(SCHEDULED_RETENTION_SECS),
        max_bytes: None,
    };
    c.dlq_enabled = false;
    c
}

struct Part {
    state: Arc<Mutex<PartitionState>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct Inner {
    broker: Broker,
    cluster: Cluster,
    parts: Mutex<HashMap<u32, Part>>,
    released: AtomicU64,
    cancelled: AtomicU64,
    stop: watch::Sender<bool>,
    enabled: bool,
}

/// Cheap to clone.
#[derive(Clone)]
pub struct SchedulerService {
    inner: Arc<Inner>,
}

/// Why a control operation (cancel / release now) failed.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("no pending delayed message with this id")]
    NotFound,
    /// This node doesn't lead the entry's partition (the API forwards to the leader).
    #[error("not the leader of {SCHEDULED_CHANNEL} partition {0}")]
    NotLeader(u32),
    #[error("{0}")]
    Failed(String),
}

impl SchedulerService {
    /// Starts the workers (reconcile loop). The `$scheduled` channel is created on demand.
    pub fn start(broker: Broker, cluster: Cluster) -> Self {
        let (stop, rx) = watch::channel(false);
        let svc = Self {
            inner: Arc::new(Inner {
                broker,
                cluster,
                parts: Mutex::new(HashMap::new()),
                released: AtomicU64::new(0),
                cancelled: AtomicU64::new(0),
                stop,
                enabled: true,
            }),
        };
        tokio::spawn(reconcile_loop(svc.inner.clone(), rx));
        svc
    }

    /// Publishes without scheduling support (delay headers are refused). For tests and tools.
    pub fn disabled(broker: Broker) -> Self {
        let (stop, _) = watch::channel(true);
        Self {
            inner: Arc::new(Inner {
                cluster: Cluster::standalone(broker.clone()),
                broker,
                parts: Mutex::new(HashMap::new()),
                released: AtomicU64::new(0),
                cancelled: AtomicU64::new(0),
                stop,
                enabled: false,
            }),
        }
    }

    /// Like [`Cluster::publish`], with delayed delivery for messages carrying
    /// [`DELIVER_AT_HEADER`] / [`DELAY_HEADER`]. Ids keep the input order; a delayed message's
    /// id is its schedule id (in `$scheduled`).
    pub async fn publish(
        &self,
        channel: &str,
        partition: Option<u32>,
        messages: Vec<PublishMessage>,
    ) -> Result<Vec<MessageId>, WireError> {
        if !messages.iter().any(|m| is_delayed(&m.headers)) {
            return self
                .inner
                .cluster
                .publish(channel, partition, messages)
                .await;
        }
        if !self.inner.enabled {
            return Err(WireError::new(
                ErrorCode::BadRequest,
                "delayed delivery is not available on this broker",
            ));
        }
        let now = now_ms();
        let mut now_msgs = Vec::new();
        let mut later = Vec::new();
        let mut order = Vec::with_capacity(messages.len());
        for mut m in messages {
            let t =
                timing(&m.headers, now).map_err(|e| WireError::new(ErrorCode::BadRequest, e))?;
            m.headers.remove(DELIVER_AT_HEADER);
            m.headers.remove(DELAY_HEADER);
            match t {
                Timing::Now => {
                    order.push(false);
                    now_msgs.push(m);
                }
                Timing::At(at) => {
                    m.headers.insert(TARGET_HEADER.into(), channel.to_string());
                    m.headers.insert(DUE_HEADER.into(), at.to_string());
                    order.push(true);
                    later.push(m);
                }
            }
        }
        let mut later_ids = Vec::new();
        if !later.is_empty() {
            if !self.channel_exists(channel) {
                return Err(WireError::new(
                    ErrorCode::NotFound,
                    format!("channel '{channel}' not found"),
                ));
            }
            ensure_channel(&self.inner)
                .await
                .map_err(|e| WireError::new(ErrorCode::Overloaded, e))?;
            later_ids = self
                .inner
                .cluster
                .publish(SCHEDULED_CHANNEL, None, later)
                .await?;
        }
        let mut now_ids = Vec::new();
        if !now_msgs.is_empty() {
            now_ids = self
                .inner
                .cluster
                .publish(channel, partition, now_msgs)
                .await?;
        }
        let (mut a, mut b) = (now_ids.into_iter(), later_ids.into_iter());
        Ok(order
            .into_iter()
            .filter_map(|delayed| if delayed { b.next() } else { a.next() })
            .collect())
    }

    fn channel_exists(&self, name: &str) -> bool {
        self.inner.broker.channel(name).is_some()
            || (self.inner.cluster.is_clustered()
                && self
                    .inner
                    .cluster
                    .metadata(&[name.to_string()])
                    .channels
                    .contains_key(name))
    }

    /// This node's pending entries (partitions it leads): the soonest `limit` (of `channel`
    /// when given) with their details, and counts per channel.
    pub async fn local(&self, channel: Option<String>, limit: usize) -> NodeScheduled {
        let states: Vec<(u32, Arc<Mutex<PartitionState>>)> = self
            .inner
            .parts
            .lock()
            .iter()
            .map(|(p, part)| (*p, part.state.clone()))
            .collect();
        let mut picked = Vec::new();
        let mut channels: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        for (p, st) in &states {
            let st = st.lock();
            for (off, due, ch) in st.upcoming(channel.as_deref(), limit) {
                picked.push((*p, off, due, ch.to_string()));
            }
            for (ch, n, next) in st.channels() {
                let e = channels.entry(ch.to_string()).or_insert((0, u64::MAX));
                e.0 += n;
                e.1 = e.1.min(next);
            }
        }
        picked.sort_by_key(|&(p, off, due, _)| (due, p, off));
        picked.truncate(limit);
        let broker = self.inner.broker.clone();
        let ids: Vec<MessageId> = picked
            .iter()
            .map(|&(partition, offset, ..)| MessageId { partition, offset })
            .collect();
        let msgs = tokio::task::spawn_blocking(move || broker.read_by_ids(SCHEDULED_CHANNEL, &ids))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        let by_id: HashMap<(u32, u64), Message> = msgs
            .into_iter()
            .map(|m| ((m.partition, m.offset), m))
            .collect();
        let node = self.inner.broker.node_id().to_string();
        let items = picked
            .into_iter()
            .map(|(partition, offset, due, ch)| {
                let m = by_id.get(&(partition, offset));
                ScheduledMessage {
                    id: MessageId { partition, offset },
                    channel: ch,
                    key: m.and_then(|m| m.key.clone()),
                    due_ms: due,
                    scheduled_at_ms: m.map_or(0, |m| m.timestamp_ms),
                    size: m.map_or(0, |m| m.payload.len() as u32),
                    event_type: m.and_then(|m| m.headers.get(headers::EVENT_TYPE).cloned()),
                    node: node.clone(),
                }
            })
            .collect();
        NodeScheduled {
            items,
            channels: channels
                .into_iter()
                .filter(|(c, _)| channel.as_deref().is_none_or(|w| w == c))
                .map(|(channel, (count, next_due_ms))| ScheduledChannel {
                    channel,
                    count,
                    next_due_ms,
                })
                .collect(),
            released: self.inner.released.load(Ordering::Relaxed),
            cancelled: self.inner.cancelled.load(Ordering::Relaxed),
        }
    }

    /// Cancels a pending entry (`now = false`) or releases it at once (`now = true`). Served
    /// by the leader of the entry's partition.
    pub async fn control(&self, id: MessageId, now: bool) -> Result<(), ControlError> {
        let state = {
            let parts = self.inner.parts.lock();
            match parts.get(&id.partition) {
                Some(p) => p.state.clone(),
                None => {
                    return Err(if id.partition < SCHEDULED_PARTITIONS {
                        ControlError::NotLeader(id.partition)
                    } else {
                        ControlError::NotFound
                    });
                }
            }
        };
        if !state.lock().contains(id.offset) {
            return Err(ControlError::NotFound);
        }
        let op = if now { "now" } else { "cancel" };
        append_control(&self.inner, id.partition, &[(id.offset, op)])
            .await
            .map_err(ControlError::Failed)?;
        let mut st = state.lock();
        if now {
            st.release_now(id.offset);
        } else if st.remove(id.offset) {
            self.inner.cancelled.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Stops the workers.
    pub async fn shutdown(&self) {
        let _ = self.inner.stop.send(true);
        let parts: Vec<Part> = self.inner.parts.lock().drain().map(|(_, p)| p).collect();
        for p in parts {
            let _ = p.stop.send(true);
            let _ = p.task.await;
        }
    }
}

/// Creates `$scheduled` when missing (any node may ask; the cluster's controller applies it).
async fn ensure_channel(inner: &Inner) -> Result<(), String> {
    if inner.broker.channel(SCHEDULED_CHANNEL).is_some() {
        return Ok(());
    }
    if inner.cluster.is_clustered()
        && inner
            .cluster
            .metadata(&[SCHEDULED_CHANNEL.to_string()])
            .channels
            .contains_key(SCHEDULED_CHANNEL)
    {
        return Ok(());
    }
    match tokio::time::timeout(
        Duration::from_secs(15),
        inner.cluster.create_channel(channel_config()),
    )
    .await
    {
        Ok(Ok(_)) => {
            info!("created the {SCHEDULED_CHANNEL} system channel");
            Ok(())
        }
        Ok(Err(e)) if e.to_string().contains("already exists") => Ok(()),
        Ok(Err(e)) => Err(format!("cannot create {SCHEDULED_CHANNEL}: {e}")),
        Err(_) => Err(format!("creating {SCHEDULED_CHANNEL} timed out")),
    }
}

async fn reconcile_loop(inner: Arc<Inner>, mut stop: watch::Receiver<bool>) {
    loop {
        reconcile(&inner);
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(RECONCILE) => {}
        }
    }
}

/// Starts a worker for every `$scheduled` partition this node leads, forgets finished ones.
fn reconcile(inner: &Arc<Inner>) {
    let exists = inner.broker.channel(SCHEDULED_CHANNEL).is_some();
    let mut parts = inner.parts.lock();
    parts.retain(|_, p| !p.task.is_finished());
    if !exists {
        return;
    }
    for p in 0..SCHEDULED_PARTITIONS {
        if parts.contains_key(&p) || !inner.broker.leads_partition(SCHEDULED_CHANNEL, p) {
            continue;
        }
        let state = Arc::new(Mutex::new(PartitionState::default()));
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run_partition(inner.clone(), p, state.clone(), rx));
        debug!(partition = p, "delayed messages: worker started");
        parts.insert(p, Part { state, stop, task });
    }
}

/// Appends control records `(entry offset, op)` to partition `p` (this node must lead it).
async fn append_control(inner: &Inner, p: u32, ops: &[(u64, &str)]) -> Result<(), String> {
    let msgs = ops
        .iter()
        .map(|&(offset, op)| PublishMessage {
            key: None,
            headers: BTreeMap::from([
                (OP_HEADER.to_string(), op.to_string()),
                (REF_HEADER.to_string(), offset.to_string()),
            ]),
            payload: bytes::Bytes::new(),
        })
        .collect();
    inner
        .cluster
        .publish(SCHEDULED_CHANNEL, Some(p), msgs)
        .await
        .map(drop)
        .map_err(|e| e.message)
}

async fn run_partition(
    inner: Arc<Inner>,
    p: u32,
    state: Arc<Mutex<PartitionState>>,
    mut stop: watch::Receiver<bool>,
) {
    let meta = inner.cluster.meta();
    let start = meta
        .get_as::<u64>(&watermark_key(p))
        .ok()
        .flatten()
        .unwrap_or(0);
    let mut next_read = start;
    let mut saved_low = start;
    let mut last_save = tokio::time::Instant::now();
    info!(
        partition = p,
        from = start,
        "delayed messages: following {SCHEDULED_CHANNEL}"
    );
    loop {
        if *stop.borrow() || !inner.broker.leads_partition(SCHEDULED_CHANNEL, p) {
            break;
        }
        // 1. Follow new entries and control records.
        loop {
            match read_from(&inner.broker, p, next_read).await {
                Ok(batch) => {
                    let n = batch.len();
                    if let Some(last) = batch.last() {
                        next_read = last.offset + 1;
                    }
                    apply(&inner, &state, batch);
                    if n < READ_BATCH as usize {
                        break;
                    }
                }
                Err(e) => {
                    debug!(partition = p, error = %e, "delayed messages: read failed");
                    break;
                }
            }
        }
        // 2. Release what is due.
        loop {
            let due = state.lock().due(now_ms(), RELEASE_BATCH);
            if due.is_empty() {
                break;
            }
            let full = due.len() == RELEASE_BATCH;
            release(&inner, p, &state, due).await;
            if !full {
                break;
            }
        }
        // 3. Persist the low watermark now and then.
        if last_save.elapsed() >= WATERMARK_EVERY {
            let low = state.lock().low_offset().unwrap_or(next_read);
            if low != saved_low && meta.put_as(&watermark_key(p), &low).await.is_ok() {
                saved_low = low;
            }
            last_save = tokio::time::Instant::now();
        }
        // 4. Sleep until the next due time (or the next follow tick).
        let wait = state
            .lock()
            .next_wake()
            .map(|t| Duration::from_millis(t.saturating_sub(now_ms())))
            .unwrap_or(TICK)
            .min(TICK);
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }
    debug!(partition = p, "delayed messages: worker stopped");
}

async fn read_from(broker: &Broker, p: u32, offset: u64) -> Result<Vec<Message>, String> {
    let broker = broker.clone();
    tokio::task::spawn_blocking(move || {
        let q = MessageQuery {
            partition: Some(p),
            offset: Some(offset),
            limit: Some(READ_BATCH),
            ..Default::default()
        };
        broker
            .read_messages(SCHEDULED_CHANNEL, &q)
            .map(|(mut m, _, _)| {
                m.sort_by_key(|m| m.offset);
                m
            })
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

fn apply(inner: &Inner, state: &Mutex<PartitionState>, batch: Vec<Message>) {
    let mut st = state.lock();
    for m in batch {
        if let Some(op) = m.headers.get(OP_HEADER) {
            let Some(r) = m
                .headers
                .get(REF_HEADER)
                .and_then(|r| r.parse::<u64>().ok())
            else {
                continue;
            };
            match op.as_str() {
                "now" => {
                    st.release_now(r);
                }
                "cancel" => {
                    if st.remove(r) {
                        inner.cancelled.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ => {
                    st.remove(r);
                }
            }
            continue;
        }
        let (Some(target), Some(due)) = (
            m.headers.get(TARGET_HEADER),
            m.headers
                .get(DUE_HEADER)
                .and_then(|d| d.parse::<u64>().ok()),
        ) else {
            continue;
        };
        st.insert(m.offset, due, target);
    }
}

/// Publishes due entries to their channels, then marks them done (or dropped / retried).
async fn release(
    inner: &Inner,
    p: u32,
    state: &Mutex<PartitionState>,
    due: Vec<(u64, u64, Arc<str>)>,
) {
    let ids: Vec<MessageId> = due
        .iter()
        .map(|&(offset, ..)| MessageId {
            partition: p,
            offset,
        })
        .collect();
    let broker = inner.broker.clone();
    let read = tokio::task::spawn_blocking(move || broker.read_by_ids(SCHEDULED_CHANNEL, &ids))
        .await
        .ok()
        .and_then(Result::ok);
    let Some(msgs) = read else {
        let at = now_ms() + RETRY_AFTER_MS;
        let mut st = state.lock();
        for &(off, ..) in &due {
            st.retry_later(off, at);
        }
        return;
    };
    let by_off: HashMap<u64, Message> = msgs.into_iter().map(|m| (m.offset, m)).collect();
    let mut per_channel: BTreeMap<Arc<str>, (Vec<u64>, Vec<PublishMessage>)> = BTreeMap::new();
    let mut ops: Vec<(u64, &str)> = Vec::new();
    for (off, due_ms, ch) in &due {
        let Some(m) = by_off.get(off) else {
            // Gone from the log (retention ran while the broker was down for a week).
            warn!(partition = p, offset = off, channel = %ch, "delayed message lost to retention");
            ops.push((*off, "drop"));
            continue;
        };
        let mut h = m.headers.clone();
        h.remove(TARGET_HEADER);
        h.remove(DUE_HEADER);
        h.insert(SCHEDULED_ID_HEADER.into(), format!("{p}/{off}"));
        h.insert(SCHEDULED_FOR_HEADER.into(), due_ms.to_string());
        let e = per_channel.entry(ch.clone()).or_default();
        e.0.push(*off);
        e.1.push(PublishMessage {
            key: m.key.clone(),
            headers: h,
            payload: m.payload.clone(),
        });
    }
    let mut retry = Vec::new();
    for (ch, (offs, msgs)) in per_channel {
        match inner.cluster.publish(&ch, None, msgs).await {
            Ok(_) => {
                inner
                    .released
                    .fetch_add(offs.len() as u64, Ordering::Relaxed);
                ops.extend(offs.into_iter().map(|o| (o, "done")));
            }
            Err(e) if e.code == ErrorCode::NotFound => {
                warn!(channel = %ch, count = offs.len(), "delayed messages dropped: channel no longer exists");
                ops.extend(offs.into_iter().map(|o| (o, "drop")));
            }
            Err(e) => {
                debug!(channel = %ch, error = %e.message, "delayed messages: release failed, retrying");
                retry.extend(offs);
            }
        }
    }
    {
        let mut st = state.lock();
        for &(off, _) in &ops {
            st.remove(off);
        }
        let at = now_ms() + RETRY_AFTER_MS;
        for off in retry {
            st.retry_later(off, at);
        }
    }
    // A lost control record means the entry is released again after a failover (at least
    // once; the copies share `flux-scheduled-id`).
    if !ops.is_empty()
        && let Err(e) = append_control(inner, p, &ops).await
    {
        debug!(partition = p, error = %e, "delayed messages: marking released entries failed");
    }
}

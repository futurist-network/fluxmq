//! The [`Broker`] handle — the only public entry point into the core.
//!
//! Signatures and doc contracts are fixed (other crates compile against them).
//!
//! Threading: publishes go to a per-channel writer thread; metadata (DLQ, offsets) to the
//! broker's meta writer thread; read-ahead for lagging consumers runs on `spawn_blocking`.
//! Admin operations (`create_channel`, `update_channel`, `delete_channel`) do a few small
//! metadata file operations inline (mkdir, an atomic config write, a directory rename);
//! bulk data removal happens on the writer thread. `read_messages` / `get_message` perform
//! positional reads on the calling thread; async callers should wrap them in
//! `spawn_blocking` when scanning large ranges.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use fluxmq_proto::api::{DlqReasonGroup, DlqSummary, MessageQuery, UpdateChannelRequest};
use fluxmq_proto::model::{
    ChannelConfig, DeadLetter, DeliveryAttempt, Message, MessageId, PublishMessage, StartFrom,
    validate_channel_name,
};
use fluxmq_proto::wire::{ClientStats, Delivery};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::BrokerEvent;
use crate::channel::{self, Channel, Shared};
use crate::config::BrokerConfig;
use crate::error::{CoreError, Result};
use crate::group::{BatchPoll, Group, Poll, RawDelivery, SharedDelivery, SubMode};
use crate::persist::{self, CONFIG_FILE, MetaOp};
use crate::replica::{IsrUpdate, ReplicaHandle};
use crate::stats::{BrokerStats, ChannelStats, GroupStats, PartitionStats};
use crate::util::{atomic_write, now_ms, now_us};

/// Hard cap on partitions per channel.
pub const MAX_PARTITIONS: u32 = 1024;
/// Header added to replayed messages: `{channel}/{partition}/{offset}`.
pub const REPLAYED_FROM_HEADER: &str = "flux-replayed-from";

const REAPER_TICK: Duration = Duration::from_millis(10);
/// Background retention pass interval (5 s before v0.6, 1 s in v0.6–v0.7). Short so that a
/// segment is removed soon after it becomes due whatever the ingest rate, keeping the
/// overshoot over the byte budget bounded (v0.8); a pass with nothing due only counts
/// segments under read locks (no I/O), and channels without retention are skipped.
const RETENTION_TICK: Duration = Duration::from_millis(50);
/// Minimum pause between two removals of a paced pass: caps the deletion rate at roughly
/// 200 segments/s (3 GiB/s at 16 MiB segments, far above any ingest rate we sustain) while
/// never deleting segments back to back. A pass spreads its removals evenly over
/// [`RETENTION_TICK`] when it has few, and goes at this pace when it has many.
const RETENTION_MIN_GAP: Duration = Duration::from_millis(5);
/// A paced pass stops after this long (the next one starts a tick later, with freshly
/// computed due counts).
const RETENTION_PASS_BUDGET: Duration = Duration::from_secs(1);
/// Paced passes truncate an unreferenced segment in steps of this size before unlinking it.
const RETENTION_SHRINK_STEP: u64 = 16 << 20;
const RETENTION_SHRINK_PAUSE: Duration = Duration::from_millis(2);
const CAPACITY_CACHE: Duration = Duration::from_secs(10);

/// A stored message plus its delivery history per consumer group (see [`Broker::get_message`]).
pub type MessageWithDeliveries = (Message, BTreeMap<String, Vec<DeliveryAttempt>>);

/// Cheap to clone; all clones share one broker.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

struct Inner {
    node_id: String,
    started_at_ms: u64,
    shared: Arc<Shared>,
    channels: RwLock<BTreeMap<String, Arc<Channel>>>,
    /// Serializes create/update/delete.
    admin: Mutex<()>,
    channels_dir: PathBuf,
    trash_dir: PathBuf,
    connections: AtomicU32,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    capacity: Mutex<Option<(Instant, u64)>>,
    shut_down: AtomicBool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Without an explicit shutdown, still stop writer threads (they commit what they have).
        for t in self.tasks.get_mut().drain(..) {
            t.abort();
        }
        if !self.shut_down.load(Ordering::Acquire) {
            for ch in self.channels.get_mut().values() {
                drop(ch.close_writer(None));
                ch.close_consumers();
            }
        }
    }
}

/// Who is subscribing — surfaced in the Consumers UI.
#[derive(Debug, Clone)]
pub struct SubscriberInfo {
    pub client_name: String,
    pub remote_addr: String,
    pub connection_id: u64,
}

/// Selects DLQ entries for retry/discard.
#[derive(Debug, Clone)]
pub enum DlqSelector {
    Ids(Vec<(String, u64)>),
    Reason {
        reason: String,
        channel: Option<String>,
    },
    Channel(String),
    All,
}

fn validate_config(c: &ChannelConfig) -> Result<()> {
    validate_channel_name(&c.name).map_err(CoreError::InvalidArgument)?;
    if c.partitions == 0 || c.partitions > MAX_PARTITIONS {
        return Err(CoreError::InvalidArgument(format!(
            "partitions must be between 1 and {MAX_PARTITIONS}"
        )));
    }
    if c.mode == fluxmq_proto::model::ChannelMode::Priority
        && !fluxmq_proto::model::PRIORITY_LEVELS.contains(&c.partitions)
    {
        return Err(CoreError::InvalidArgument(format!(
            "a priority channel has {}-{} levels (partitions), got {}",
            fluxmq_proto::model::PRIORITY_LEVELS.start(),
            fluxmq_proto::model::PRIORITY_LEVELS.end(),
            c.partitions
        )));
    }
    validate_retry(&c.retry)
}

fn validate_retry(r: &fluxmq_proto::model::RetryPolicy) -> Result<()> {
    if r.max_attempts == 0 {
        return Err(CoreError::InvalidArgument(
            "max_attempts must be >= 1".into(),
        ));
    }
    if r.ack_timeout_ms == 0 {
        return Err(CoreError::InvalidArgument(
            "ack_timeout_ms must be >= 1".into(),
        ));
    }
    Ok(())
}

/// Reads `ids` from `ch`, skipping missing / not yet readable offsets.
fn read_ids(ch: &Channel, ids: &[MessageId]) -> Result<Vec<Message>> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(log) = ch.logs.get(id.partition as usize)
            && id.offset < ch.read_limit(id.partition as usize)
            && let Some(m) = log.get(id.offset)?
        {
            out.push(m);
        }
    }
    Ok(out)
}

/// Re-publishable copies of `msgs` (from channel `source`) carrying [`REPLAYED_FROM_HEADER`].
pub fn replay_copies(source: &str, msgs: Vec<Message>) -> Vec<PublishMessage> {
    msgs.into_iter()
        .map(|m| {
            let mut headers = m.headers;
            headers.insert(
                REPLAYED_FROM_HEADER.to_string(),
                format!("{source}/{}/{}", m.partition, m.offset),
            );
            PublishMessage {
                key: m.key,
                headers,
                payload: m.payload,
            }
        })
        .collect()
}

fn join_err(e: tokio::task::JoinError) -> CoreError {
    CoreError::Io(std::io::Error::other(format!(
        "background task failed: {e}"
    )))
}

/// Blocking part of `open`: directories, node id, channel recovery.
fn open_blocking(cfg: &BrokerConfig) -> Result<(String, Vec<channel::Recovered>)> {
    let channels_dir = cfg.data_dir.join("channels");
    let trash_dir = cfg.data_dir.join("trash");
    fs::create_dir_all(&channels_dir)?;
    if trash_dir.exists() {
        fs::remove_dir_all(&trash_dir)?;
    }
    fs::create_dir_all(&trash_dir)?;
    let node_id = match &cfg.node_id {
        Some(id) => id.clone(),
        None => {
            let path = cfg.data_dir.join("node_id");
            match fs::read_to_string(&path) {
                Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
                _ => {
                    let id = format!(
                        "n-{:012x}",
                        (now_us() ^ (u64::from(std::process::id()) << 40)) & 0xffff_ffff_ffff
                    );
                    atomic_write(&path, id.as_bytes())?;
                    id
                }
            }
        }
    };
    let mut recovered = Vec::new();
    for entry in fs::read_dir(&channels_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let dir = entry.path();
        let cfg_path = dir.join(CONFIG_FILE);
        let bytes = match fs::read(&cfg_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                warn!(
                    ?dir,
                    "removing incomplete channel directory (no channel.json)"
                );
                fs::remove_dir_all(&dir)?;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let config: ChannelConfig = serde_json::from_slice(&bytes)
            .map_err(|e| CoreError::Corrupt(format!("{}: {e}", cfg_path.display())))?;
        recovered.push(channel::recover(&dir, config, cfg)?);
    }
    Ok((node_id, recovered))
}

/// Storage capacity of the filesystem holding `path` (total bytes).
fn fs_capacity(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statvfs` is plain old data; zeroed is a valid initial value, and the pointer
    // arguments are valid for the duration of the call.
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    let r = unsafe { libc::statvfs(c.as_ptr(), &mut s) };
    if r != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)] // field widths differ across platforms
    Some((s.f_blocks as u64).saturating_mul(s.f_frsize as u64))
}

impl Broker {
    /// Opens (or creates) the data directory, recovers channels, segments, group offsets and
    /// DLQ, truncates torn tail writes, and starts background tasks (writer/fsync, retry
    /// scheduler, ack-timeout reaper, retention, offset checkpointing). Must be called inside a
    /// Tokio runtime.
    pub async fn open(config: BrokerConfig) -> Result<Broker> {
        let cfg = config.clone();
        let (node_id, recovered) = tokio::task::spawn_blocking(move || open_blocking(&cfg))
            .await
            .map_err(join_err)??;
        let (meta, _meta_thread) = persist::spawn_meta_writer()?;
        let first_dlq_id = recovered
            .iter()
            .filter_map(|r| r.dlq.keys().next_back().copied())
            .max()
            .map_or(1, |m| m + 1);
        let channels_dir = config.data_dir.join("channels");
        let trash_dir = config.data_dir.join("trash");
        let shared = Arc::new(Shared::new(config, meta, first_dlq_id, node_id.clone()));
        let mut channels = BTreeMap::new();
        for rec in recovered {
            let ch = Channel::start(shared.clone(), rec)?;
            channels.insert(ch.name.clone(), ch);
        }
        info!(node_id, channels = channels.len(), "broker opened");
        let inner = Arc::new(Inner {
            node_id,
            started_at_ms: now_ms(),
            shared,
            channels: RwLock::new(channels),
            admin: Mutex::new(()),
            channels_dir,
            trash_dir,
            connections: AtomicU32::new(0),
            tasks: Mutex::new(Vec::new()),
            capacity: Mutex::new(None),
            shut_down: AtomicBool::new(false),
        });
        let broker = Broker { inner };
        broker.spawn_background();
        Ok(broker)
    }

    fn spawn_background(&self) {
        let weak = Arc::downgrade(&self.inner);
        let mut tasks = Vec::new();
        // Ack-timeout reaper.
        tasks.push(spawn_ticker(weak.clone(), REAPER_TICK, |inner| {
            let now = Instant::now();
            for ch in inner.channel_list() {
                let groups: Vec<Arc<Group>> = ch.groups.read().values().cloned().collect();
                for g in groups {
                    g.reap(&ch, now);
                }
            }
        }));
        // Batched DeadLettered events.
        tasks.push(spawn_ticker(
            weak.clone(),
            Duration::from_secs(1),
            |inner| inner.shared.flush_dead_letter_events(),
        ));
        // Offset checkpoints (skipped when unchanged).
        let last: Mutex<BTreeMap<String, Vec<u8>>> = Mutex::new(BTreeMap::new());
        let interval = self
            .inner
            .shared
            .config
            .offset_checkpoint_interval
            .max(Duration::from_millis(10));
        tasks.push(spawn_ticker(weak.clone(), interval, move |inner| {
            let mut last = last.lock();
            for ch in inner.channel_list() {
                let bytes = ch.offsets_snapshot();
                if last.get(&ch.name) != Some(&bytes) {
                    last.insert(ch.name.clone(), bytes.clone());
                    let _ = inner.shared.meta.send(MetaOp::Offsets {
                        files: ch.files.clone(),
                        bytes,
                    });
                }
            }
        }));
        // Retention.
        let weak_r = weak.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(RETENTION_TICK).await;
                let Some(inner) = weak_r.upgrade() else { break };
                let channels = inner.channel_list();
                drop(inner);
                let _ =
                    tokio::task::spawn_blocking(move || enforce_retention_paced(&channels)).await;
            }
        }));
        self.inner.tasks.lock().extend(tasks);
    }

    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.inner.shared.config
    }

    fn get(&self, name: &str) -> Result<Arc<Channel>> {
        self.inner
            .channels
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| CoreError::ChannelNotFound(name.to_string()))
    }

    fn check_running(&self) -> Result<()> {
        if self.inner.shared.shutting_down.load(Ordering::Acquire) {
            Err(CoreError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    /// Validates (name, partitions >= 1, max_attempts >= 1) and persists the channel.
    pub fn create_channel(&self, config: ChannelConfig) -> Result<ChannelConfig> {
        self.check_running()?;
        let mut config = config;
        validate_config(&config)?;
        if config.created_at_ms == 0 {
            config.created_at_ms = now_ms();
        }
        let _admin = self.inner.admin.lock();
        if self.inner.channels.read().contains_key(&config.name) {
            return Err(CoreError::ChannelExists(config.name));
        }
        let dir = self.inner.channels_dir.join(&config.name);
        if dir.exists() {
            // Not registered, so it is a leftover of an interrupted create.
            fs::remove_dir_all(&dir)?;
        }
        let rec = channel::recover(&dir, config.clone(), &self.inner.shared.config)?;
        let json =
            serde_json::to_vec_pretty(&config).map_err(|e| CoreError::Corrupt(e.to_string()))?;
        atomic_write(&dir.join(CONFIG_FILE), &json)?;
        let ch = Channel::start(self.inner.shared.clone(), rec)?;
        self.inner.channels.write().insert(config.name.clone(), ch);
        self.inner.shared.emit(BrokerEvent::ChannelCreated {
            channel: config.name.clone(),
        });
        Ok(config)
    }

    pub fn update_channel(
        &self,
        name: &str,
        update: UpdateChannelRequest,
    ) -> Result<ChannelConfig> {
        self.check_running()?;
        let _admin = self.inner.admin.lock();
        let ch = self.get(name)?;
        let mut cfg = ch.config.read().clone();
        if let Some(r) = update.retention {
            cfg.retention = r;
        }
        if let Some(r) = update.retry {
            validate_retry(&r)?;
            cfg.retry = r;
        }
        if let Some(d) = update.dlq_enabled {
            cfg.dlq_enabled = d;
        }
        let json =
            serde_json::to_vec_pretty(&cfg).map_err(|e| CoreError::Corrupt(e.to_string()))?;
        ch.files.write_config(&json)?;
        *ch.config.write() = cfg.clone();
        self.inner.shared.emit(BrokerEvent::ChannelUpdated {
            channel: name.to_string(),
        });
        Ok(cfg)
    }

    /// Removes the channel and its data. Active subscriptions end (`next()` returns `None`).
    pub fn delete_channel(&self, name: &str) -> Result<()> {
        let _admin = self.inner.admin.lock();
        let ch = self.get(name)?;
        let trash = self.inner.trash_dir.join(format!("{name}.{}", now_us()));
        // `false`: the storage was released (no directory on this node, nothing to move).
        let moved = ch.files.delete_with(|dir| fs::rename(dir, &trash))?;
        ch.deleted.store(true, Ordering::Release);
        self.inner.channels.write().remove(name);
        for r in ch.repl.iter() {
            r.wake();
        }
        ch.close_consumers();
        drop(ch.close_writer(moved.then_some(trash)));
        self.inner.shared.emit(BrokerEvent::ChannelDeleted {
            channel: name.to_string(),
        });
        Ok(())
    }

    pub fn channels(&self) -> Vec<ChannelConfig> {
        self.inner
            .channel_list()
            .iter()
            .map(|c| c.config.read().clone())
            .collect()
    }

    pub fn channel(&self, name: &str) -> Option<ChannelConfig> {
        self.inner
            .channels
            .read()
            .get(name)
            .map(|c| c.config.read().clone())
    }

    /// Appends messages; returns once durable per [`crate::FsyncPolicy`]. Partition is chosen by
    /// key hash (stable: `crc32(key) % partitions`) or round-robin when the key is `None`. The
    /// broker stamps `timestamp_ms`. Concurrent publishers are batched by the writer.
    pub async fn publish(
        &self,
        channel: &str,
        messages: Vec<PublishMessage>,
    ) -> Result<Vec<MessageId>> {
        let received = Instant::now();
        self.check_running()?;
        let ch = self.get(channel)?;
        ch.publish(messages, None, received).await
    }

    /// Like [`Broker::publish`] but every message goes to `partition` (same durability and, in
    /// replicated mode, the same high-watermark-gated ack). `InvalidArgument` when `partition`
    /// is out of range; in replicated mode `NotLeader` when this node does not lead it.
    pub async fn publish_to_partition(
        &self,
        channel: &str,
        partition: u32,
        messages: Vec<PublishMessage>,
    ) -> Result<Vec<MessageId>> {
        let received = Instant::now();
        self.check_running()?;
        let ch = self.get(channel)?;
        if partition >= ch.partitions() {
            return Err(CoreError::InvalidArgument(format!(
                "partition {partition} out of range (channel '{channel}' has {})",
                ch.partitions()
            )));
        }
        ch.publish(messages, Some(partition), received).await
    }

    /// Appends one producer record batch (`ClientRequest::PublishBatch`, protocol v4) as one
    /// envelope, stored verbatim (compressed batches stay compressed). Returns
    /// `(partition, base_offset, record_count)`: the records hold the contiguous offsets
    /// `base_offset .. base_offset + record_count`. Same durability as [`Broker::publish`]
    /// (fsync policy; in replicated mode acknowledged by the ISR, `NotLeader` when this node
    /// doesn't lead the partition).
    ///
    /// `partition: None` routes the whole batch: to the partition all keyed records map to
    /// (`fluxmq_proto::batch::key_partition`), or a sticky partition when no record has a
    /// key. Keyed records spanning partitions → `InvalidArgument`. Malformed sections
    /// (malformed records, wrong count, nonzero deltas, bad compression, too large) →
    /// `InvalidArgument`.
    pub async fn publish_batch(
        &self,
        channel: &str,
        partition: Option<u32>,
        records: fluxmq_proto::batch::RecordsSection,
    ) -> Result<(u32, u64, u32)> {
        let received = Instant::now();
        self.check_running()?;
        let ch = self.get(channel)?;
        ch.publish_batch(partition, records, received).await
    }

    /// Whether [`Broker::publish_to_partition`] would pass its leadership check right now:
    /// always outside replicated mode, else when this node is `partition`'s active leader
    /// (false for unknown channels / partitions). A hint only — the role may change at once.
    pub fn leads_partition(&self, channel: &str, partition: u32) -> bool {
        let Ok(ch) = self.get(channel) else {
            return false;
        };
        if partition >= ch.partitions() {
            return false;
        }
        !ch.replicated.load(Ordering::Acquire) || ch.repl[partition as usize].is_leader()
    }

    /// Joins (creating if needed) consumer group `group`. `start` applies only when the group
    /// is new. At most `prefetch` (>= 1) deliveries are outstanding for this subscription.
    pub fn subscribe(
        &self,
        channel: &str,
        group: &str,
        subscriber: SubscriberInfo,
        prefetch: u32,
        start: StartFrom,
    ) -> Result<Subscription> {
        self.subscribe_with(
            channel,
            group,
            subscriber,
            prefetch,
            start,
            SubMode::default(),
        )
    }

    /// [`Broker::subscribe`] with v4 options. `mode.cumulative` (`AckMode::Cumulative`) is
    /// only valid on stream channels (`InvalidArgument` otherwise): the subscription has no
    /// ack deadline and settles with [`Subscription::commit`]. `mode.raw_records` lets
    /// [`Subscription::next_frames`] hand out stored envelopes verbatim.
    pub fn subscribe_with(
        &self,
        channel: &str,
        group: &str,
        subscriber: SubscriberInfo,
        prefetch: u32,
        start: StartFrom,
        mode: SubMode,
    ) -> Result<Subscription> {
        self.check_running()?;
        if prefetch == 0 {
            return Err(CoreError::InvalidArgument("prefetch must be >= 1".into()));
        }
        if group.is_empty() || group.len() > 256 {
            return Err(CoreError::InvalidArgument(
                "group name must be 1-256 characters".into(),
            ));
        }
        let ch = self.get(channel)?;
        if mode.cumulative && ch.config.read().mode != fluxmq_proto::model::ChannelMode::Stream {
            return Err(CoreError::InvalidArgument(
                "cumulative ack mode is only available on stream channels".into(),
            ));
        }
        let g = ch.group(group, start);
        let id = self
            .inner
            .shared
            .next_sub_id
            .fetch_add(1, Ordering::Relaxed);
        let (instance, wake) = g.add_sub(id, subscriber, prefetch, mode);
        self.inner.shared.emit(BrokerEvent::ConsumerConnected {
            channel: channel.to_string(),
            group: group.to_string(),
            instance: instance.to_string(),
        });
        Ok(Subscription {
            id,
            channel: ch,
            group: g,
            instance,
            wake,
        })
    }

    /// Reads stored messages for the Messages browser (see [`MessageQuery`] precedence rules).
    /// Returns `(messages, scanned, has_more)`. Never blocks the write path.
    pub fn read_messages(
        &self,
        channel: &str,
        query: &MessageQuery,
    ) -> Result<(Vec<Message>, u64, bool)> {
        let ch = self.get(channel)?;
        crate::reads::read_messages(&ch, query)
    }

    /// One message plus its delivery history per group (empty if evicted from the bounded
    /// attempt history).
    pub fn get_message(
        &self,
        channel: &str,
        id: MessageId,
    ) -> Result<Option<MessageWithDeliveries>> {
        let ch = self.get(channel)?;
        let Some(log) = ch.logs.get(id.partition as usize) else {
            return Ok(None);
        };
        if id.offset >= ch.read_limit(id.partition as usize) {
            return Ok(None);
        }
        Ok(log.get(id.offset)?.map(|m| (m, ch.history.get(id))))
    }

    /// Re-publishes stored messages to `target` (same channel when `None`), adding header
    /// `flux-replayed-from: {channel}/{partition}/{offset}`.
    pub async fn replay(
        &self,
        channel: &str,
        ids: &[MessageId],
        target: Option<&str>,
    ) -> Result<Vec<MessageId>> {
        self.check_running()?;
        let src = self.get(channel)?;
        let dst = match target {
            Some(t) => self.get(t)?,
            None => src.clone(),
        };
        let ids = ids.to_vec();
        let src_r = src.clone();
        let msgs = tokio::task::spawn_blocking(move || read_ids(&src_r, &ids))
            .await
            .map_err(join_err)??;
        let publish = replay_copies(&src.name, msgs);
        let count = publish.len() as u64;
        let out = dst.publish(publish, None, Instant::now()).await?;
        self.record_replayed(&dst.name, count);
        Ok(out)
    }

    /// Reads the given messages (blocking). Ids that don't exist, or are not readable yet
    /// (at or above the high watermark in replicated mode), are skipped. Used by the cluster
    /// replay path, where each partition leader reads its own partitions.
    pub fn read_by_ids(&self, channel: &str, ids: &[MessageId]) -> Result<Vec<Message>> {
        let ch = self.get(channel)?;
        read_ids(&ch, ids)
    }

    /// Emits the `MessagesReplayed` event for a replay published outside [`Broker::replay`]
    /// (cluster replay publishes through the partition leaders).
    pub fn record_replayed(&self, channel: &str, count: u64) {
        if count > 0 {
            self.inner.shared.emit(BrokerEvent::MessagesReplayed {
                channel: channel.to_string(),
                count,
            });
        }
    }

    /// Moves the group's position (affects only not-yet-delivered messages).
    /// Whether consumer group `group` exists on `channel` (it may have no instances).
    pub fn has_group(&self, channel: &str, group: &str) -> Result<bool> {
        Ok(self.get(channel)?.groups.read().contains_key(group))
    }

    /// Active subscriptions of consumer group `group` on this node; `None` when the group
    /// doesn't exist here.
    pub fn group_subscribers(&self, channel: &str, group: &str) -> Result<Option<usize>> {
        Ok(self
            .get(channel)?
            .groups
            .read()
            .get(group)
            .map(|g| g.subscriber_count()))
    }

    /// Moves `group`'s read position to `to`; an unknown group is created there (lets an
    /// operator pre-position a group before its consumers connect). The REST API only resets
    /// existing groups (see [`Broker::has_group`]).
    pub fn reset_offsets(&self, channel: &str, group: &str, to: StartFrom) -> Result<()> {
        let ch = self.get(channel)?;
        if ch.groups.read().get(group).is_none() {
            // A new group simply starts at `to`.
            ch.group(group, to);
            return Ok(());
        }
        ch.group(group, to).reset(&ch, to);
        Ok(())
    }

    /// Removes consumer group `group` (offsets, retry queue, in-flight state) and its
    /// persisted offsets. Fails with `InvalidArgument` while it has active subscriptions and
    /// `GroupNotFound` when absent. In replicated mode the removal replicates with the
    /// partition leaders' offset snapshots; in a cluster call it on every node that leads a
    /// partition of the channel.
    pub fn delete_group(&self, channel: &str, group: &str) -> Result<()> {
        let ch = self.get(channel)?;
        {
            let mut groups = ch.groups.write();
            let g = groups.get(group).ok_or_else(|| CoreError::GroupNotFound {
                channel: channel.to_string(),
                group: group.to_string(),
            })?;
            let subs = g.subscriber_count();
            if subs > 0 {
                return Err(CoreError::InvalidArgument(format!(
                    "consumer group '{group}' has {subs} active subscription(s)"
                )));
            }
            // Closed under the registry lock: a subscriber racing with the removal ends
            // immediately instead of consuming from a detached group.
            g.close();
            groups.remove(group);
        }
        let _ = self.inner.shared.meta.send(MetaOp::Offsets {
            files: ch.files.clone(),
            bytes: ch.offsets_snapshot(),
        });
        Ok(())
    }

    /// Newest first. Returns `(entries, total_matching)`.
    pub fn dlq_list(
        &self,
        channel: Option<&str>,
        reason: Option<&str>,
        before: Option<u64>,
        limit: usize,
    ) -> (Vec<DeadLetter>, u64) {
        let mut total = 0u64;
        let mut out: Vec<DeadLetter> = Vec::new();
        for ch in self.inner.channel_list() {
            if channel.is_some_and(|c| c != ch.name) {
                continue;
            }
            let mut taken = 0usize;
            ch.dlq.for_each_newest_first(|dl| {
                if before.is_some_and(|b| dl.id >= b) || reason.is_some_and(|r| r != dl.reason) {
                    return true;
                }
                total += 1;
                if taken < limit {
                    out.push(dl.clone());
                    taken += 1;
                }
                true
            });
        }
        out.sort_by_key(|d| std::cmp::Reverse(d.id));
        out.truncate(limit);
        (out, total)
    }

    pub fn dlq_summary(&self) -> DlqSummary {
        let mut groups: BTreeMap<String, DlqReasonGroup> = BTreeMap::new();
        let mut total = 0u64;
        for ch in self.inner.channel_list() {
            ch.dlq.for_each_newest_first(|dl| {
                total += 1;
                let g = groups
                    .entry(dl.reason.clone())
                    .or_insert_with(|| DlqReasonGroup {
                        reason: dl.reason.clone(),
                        count: 0,
                        channels: BTreeMap::new(),
                        first_ms: dl.dead_at_ms,
                        last_ms: dl.dead_at_ms,
                    });
                g.count += 1;
                *g.channels.entry(dl.channel.clone()).or_default() += 1;
                g.first_ms = g.first_ms.min(dl.dead_at_ms);
                g.last_ms = g.last_ms.max(dl.dead_at_ms);
                true
            });
        }
        let mut groups: Vec<DlqReasonGroup> = groups.into_values().collect();
        groups.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));
        DlqSummary { total, groups }
    }

    /// Selected DLQ entries; in replicated mode only those of partitions this node leads (the
    /// leader owns a partition's DLQ — followers mirror it).
    fn dlq_select(&self, selector: &DlqSelector) -> Vec<(Arc<Channel>, Vec<u64>)> {
        let mut sel = self.dlq_select_all(selector);
        if self.replication_enabled() {
            for (ch, ids) in &mut sel {
                ids.retain(|&id| {
                    ch.dlq.partition_of(id).is_some_and(|p| {
                        (p as usize) < ch.logs.len() && ch.dispatch_limit(p as usize).is_some()
                    })
                });
            }
        }
        sel
    }

    fn dlq_select_all(&self, selector: &DlqSelector) -> Vec<(Arc<Channel>, Vec<u64>)> {
        let channels = self.inner.channel_list();
        match selector {
            DlqSelector::Ids(ids) => {
                let mut by_ch: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
                for (c, id) in ids {
                    by_ch.entry(c.as_str()).or_default().push(*id);
                }
                channels
                    .into_iter()
                    .filter_map(|ch| {
                        let ids: Vec<u64> = by_ch
                            .get(ch.name.as_str())?
                            .iter()
                            .copied()
                            .filter(|&id| ch.dlq.is_visible(id))
                            .collect();
                        Some((ch, ids))
                    })
                    .collect()
            }
            DlqSelector::Reason { reason, channel } => channels
                .into_iter()
                .filter(|ch| channel.as_ref().is_none_or(|c| *c == ch.name))
                .map(|ch| {
                    let ids = ch.dlq.select(|dl| dl.reason == *reason);
                    (ch, ids)
                })
                .collect(),
            DlqSelector::Channel(c) => channels
                .into_iter()
                .filter(|ch| ch.name == *c)
                .map(|ch| {
                    let ids = ch.dlq.select(|_| true);
                    (ch, ids)
                })
                .collect(),
            DlqSelector::All => channels
                .into_iter()
                .map(|ch| {
                    let ids = ch.dlq.select(|_| true);
                    (ch, ids)
                })
                .collect(),
        }
    }

    /// Re-delivers selected dead letters to their original group with a fresh attempt budget.
    /// Returns the number of entries affected.
    pub fn dlq_retry(&self, selector: DlqSelector) -> Result<u64> {
        self.check_running()?;
        let mut count = 0u64;
        for (ch, ids) in self.dlq_select(&selector) {
            for id in ids {
                if let Some(dl) = ch.dlq.begin_retry(id) {
                    let g = ch.group(&dl.group, StartFrom::Latest);
                    g.inject_dlq_retry(dl);
                    count += 1;
                }
            }
        }
        if count > 0 {
            self.inner.shared.emit(BrokerEvent::DlqRetried { count });
        }
        Ok(count)
    }

    pub fn dlq_discard(&self, selector: DlqSelector) -> Result<u64> {
        let mut count = 0u64;
        for (ch, ids) in self.dlq_select(&selector) {
            for id in ids {
                if ch.dlq.remove(id).is_some() {
                    count += 1;
                }
            }
        }
        if count > 0 {
            self.inner.shared.emit(BrokerEvent::DlqDiscarded { count });
        }
        Ok(count)
    }

    /// Point-in-time snapshot for the observability plane. Cost is O(channels + groups +
    /// instances) histogram clones; intended to be called every 100–500 ms by one collector.
    pub fn stats(&self) -> BrokerStats {
        let mut storage_bytes = 0u64;
        let channels: Vec<ChannelStats> = self
            .inner
            .channel_list()
            .iter()
            .map(|ch| {
                let partitions: Vec<PartitionStats> = ch
                    .logs
                    .iter()
                    .map(|log| PartitionStats {
                        partition: log.partition,
                        low_offset: log.low(),
                        high_offset: log.high(),
                        storage_bytes: log.storage_bytes(),
                        segments: log.segment_count(),
                        published_total: log.published_total.load(Ordering::Relaxed),
                    })
                    .collect();
                storage_bytes += partitions.iter().map(|p| p.storage_bytes).sum::<u64>();
                let groups: Vec<Arc<Group>> = ch.groups.read().values().cloned().collect();
                let m = &ch.metrics;
                // Lock order (see lib.rs): group state before channel histograms. Take every
                // group snapshot first, then each histogram in its own statement, so no
                // guard is alive while another lock is acquired.
                let group_stats: Vec<GroupStats> = groups.iter().map(|g| g.stats(ch)).collect();
                let publish_latency_us = m.publish_latency.lock().clone();
                let delivery_latency_us = m.delivery_latency.lock().clone();
                let dlq_size = ch.dlq.len();
                let config = ch.config.read().clone();
                let oldest_timestamp_ms = ch.logs.iter().filter_map(|l| l.oldest_timestamp()).min();
                // Plain values only from here on: no lock is taken inside the literal.
                ChannelStats {
                    config,
                    oldest_timestamp_ms,
                    partitions,
                    published_total: m.published.get(),
                    duplicates_total: ch.duplicates.load(Ordering::Relaxed),
                    published_bytes_total: m.published_bytes.get(),
                    delivered_total: m.delivered.get(),
                    delivered_bytes_total: m.delivered_bytes.get(),
                    acked_total: m.acked.get(),
                    nacked_total: m.nacked.get(),
                    timed_out_total: m.timed_out.get(),
                    retries_total: m.retries.get(),
                    dead_lettered_total: m.dead_lettered.get(),
                    dlq_size,
                    publish_latency_us,
                    delivery_latency_us,
                    groups: group_stats,
                }
            })
            .collect();
        let fsync_latency_us = self.inner.shared.fsync_latency.lock().clone();
        let commit_latency_us = self.inner.shared.commit_latency.lock().clone();
        BrokerStats {
            taken_at_ms: now_ms(),
            node_id: self.inner.node_id.clone(),
            cluster_name: self.inner.shared.config.cluster_name.clone(),
            started_at_ms: self.inner.started_at_ms,
            connections: self.inner.connections.load(Ordering::Relaxed),
            channels,
            fsync_latency_us,
            commit_latency_us,
            storage_bytes,
            storage_capacity_bytes: self.storage_capacity(storage_bytes),
        }
    }

    fn storage_capacity(&self, used: u64) -> u64 {
        let configured = self.inner.shared.config.storage_capacity_bytes;
        if configured > 0 {
            return configured;
        }
        let mut cache = self.inner.capacity.lock();
        if let Some((at, v)) = *cache
            && at.elapsed() < CAPACITY_CACHE
        {
            return v;
        }
        let v = fs_capacity(&self.inner.shared.config.data_dir)
            .unwrap_or_else(|| used.saturating_mul(2).max(10 << 30));
        *cache = Some((Instant::now(), v));
        v
    }

    pub fn events(&self) -> broadcast::Receiver<BrokerEvent> {
        self.inner.shared.events.subscribe()
    }

    /// Live tail: a sampled copy of newly published messages, rate-limited to
    /// `BrokerConfig::tail_max_rate` per channel. Zero cost when no receiver exists.
    pub fn tail(&self, channel: &str) -> Result<TailReceiver> {
        let ch = self.get(channel)?;
        let rx = ch.tail.subscribe();
        let last = ch.metrics.published.get();
        Ok(TailReceiver {
            rx,
            channel: ch,
            last_published: last,
        })
    }

    /// Number of client connections (maintained by the network layer).
    pub fn connection_opened(&self) {
        self.inner.connections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn connection_closed(&self) {
        let c = &self.inner.connections;
        let mut cur = c.load(Ordering::Relaxed);
        while cur > 0 {
            match c.compare_exchange_weak(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Applies retention now (normally runs every few seconds in the background): deletes
    /// whole non-active segments older than `max_age_secs`, beyond `max_bytes` (split evenly
    /// across partitions), and — for queue channels with at least one group — segments every
    /// group has fully consumed.
    pub async fn enforce_retention(&self) {
        let channels = self.inner.channel_list();
        let _ = tokio::task::spawn_blocking(move || enforce_retention_unpaced(&channels)).await;
    }

    /// fsync pool threads started so far (test hook).
    #[doc(hidden)]
    pub fn debug_fsync_threads(&self) -> usize {
        self.inner.shared.fsync_pool.threads()
    }

    /// Runs one paced retention pass, exactly like the background task (test hook). Returns
    /// the number of segments this pass removed (the background task may run concurrently,
    /// so tests must not infer it from segment counts).
    #[doc(hidden)]
    pub async fn debug_enforce_retention_paced(&self) -> usize {
        let channels = self.inner.channel_list();
        tokio::task::spawn_blocking(move || enforce_retention_paced(&channels).removed)
            .await
            .unwrap_or(0)
    }

    /// Consistent backup cut of `channel` (see [`crate::backup`]): per partition the pinned
    /// segments holding the committed records up to the committed end — in replicated mode
    /// only partitions this node leads, capped at the high watermark — then the groups'
    /// committed offsets and the DLQ export, both captured after the segment cut. Pins hold
    /// back retention of the referenced segments until the cut is dropped; the writer is never
    /// blocked. Blocking (a short positional scan per segment): call from `spawn_blocking`.
    pub fn backup_cut(&self, channel: &str) -> Result<crate::backup::ChannelCut> {
        let ch = self.get(channel)?;
        let replicated = ch.replicated.load(Ordering::Acquire);
        let mut partitions = Vec::with_capacity(ch.logs.len());
        let mut included = vec![false; ch.logs.len()];
        for (p, log) in ch.logs.iter().enumerate() {
            let end = if replicated {
                if ch.dispatch_limit(p).is_none() {
                    continue;
                }
                ch.read_limit(p)
            } else {
                log.high()
            };
            let (segments, end) = log.backup_cut(end)?;
            let low = segments.first().map_or(end, |s| s.base_offset);
            included[p] = true;
            partitions.push(crate::backup::PartitionCut {
                partition: p as u32,
                low,
                end,
                segments,
            });
        }
        // Lock order: the registry guard is released before any group lock is taken.
        let groups: Vec<(String, Arc<Group>)> = ch
            .groups
            .read()
            .iter()
            .map(|(n, g)| (n.clone(), g.clone()))
            .collect();
        let group_offsets = groups
            .into_iter()
            .map(|(n, g)| {
                let mut c = g.committed();
                for (p, off) in c.iter_mut().enumerate() {
                    if !included.get(p).copied().unwrap_or(false) {
                        *off = crate::backup::UNINIT_OFFSET;
                    }
                }
                (n, c)
            })
            .collect();
        let dlq = ch
            .dlq
            .export(|p| included.get(p as usize).copied().unwrap_or(false));
        let config = ch.config.read().clone();
        Ok(crate::backup::ChannelCut {
            config,
            taken_at_ms: now_ms(),
            partitions,
            group_offsets,
            dlq,
        })
    }

    // -----------------------------------------------------------------------------------
    // Replication hooks (stage 9). Unused in single-node mode.
    // -----------------------------------------------------------------------------------

    /// Switches the broker to replicated mode (idempotent, irreversible for this process).
    /// Every partition of every channel — existing and created later — becomes `Offline`
    /// (publishes fail with [`CoreError::NotLeader`], consumers get nothing from it) until a
    /// role is set through [`Broker::replica`]. Call before accepting client traffic.
    pub fn enable_replication(&self) {
        if self.inner.shared.replicated.swap(true, Ordering::AcqRel) {
            return;
        }
        let _admin = self.inner.admin.lock();
        let channels = self.inner.channel_list();
        // DLQ ids become `(seq << 10) | partition` (unique cluster-wide).
        let max_id = channels.iter().map(|c| c.dlq.max_id()).max().unwrap_or(0);
        self.inner
            .shared
            .set_dlq_seq((max_id >> crate::replica::DLQ_PARTITION_BITS) + 1);
        for ch in channels {
            ch.enable_replication();
        }
        info!("replicated mode enabled");
    }

    /// Whether [`Broker::enable_replication`] was called.
    pub fn replication_enabled(&self) -> bool {
        self.inner.shared.replicated.load(Ordering::Acquire)
    }

    /// A read-only, HW-bounded reader of `channel` for live subscriptions (v0.10).
    pub fn live_reader(&self, channel: &str) -> Result<crate::live::LiveReader> {
        Ok(crate::live::LiveReader::new(self.get(channel)?))
    }

    /// Replication handle of one partition (replicated mode only).
    pub fn replica(&self, channel: &str, partition: u32) -> Result<ReplicaHandle> {
        if !self.replication_enabled() {
            return Err(CoreError::InvalidArgument(
                "replication is not enabled on this broker".into(),
            ));
        }
        let ch = self.get(channel)?;
        if partition >= ch.partitions() {
            return Err(CoreError::InvalidArgument(format!(
                "partition {partition} out of range"
            )));
        }
        Ok(ReplicaHandle::new(ch, partition))
    }

    /// Handles of every partition of every channel (replicated mode; empty otherwise).
    pub fn replicas(&self) -> Vec<ReplicaHandle> {
        if !self.replication_enabled() {
            return Vec::new();
        }
        self.inner
            .channel_list()
            .into_iter()
            .flat_map(|ch| (0..ch.partitions()).map(move |p| ReplicaHandle::new(ch.clone(), p)))
            .collect()
    }

    /// Test hook: enqueues a pause of `channel`'s writer thread; it blocks when it reaches the
    /// pause (mid-batch, before committing) until the returned sender is dropped or signalled.
    #[doc(hidden)]
    pub async fn debug_pause_writer(&self, channel: &str) -> Result<std::sync::mpsc::Sender<()>> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.get(channel)?
            .send_cmd(crate::channel::WriteCmd::Pause(rx))
            .await?;
        Ok(tx)
    }

    /// Test hook: the next commit's fsync of `partition` sleeps `delay`, then fails with `EIO`
    /// (single node or replicated).
    #[doc(hidden)]
    pub fn debug_fail_next_fsync(
        &self,
        channel: &str,
        partition: u32,
        delay: std::time::Duration,
    ) -> Result<()> {
        let ch = self.get(channel)?;
        let log = ch.logs.get(partition as usize).ok_or_else(|| {
            CoreError::InvalidArgument(format!("partition {partition} out of range"))
        })?;
        log.fail_next_fsync_for_tests_after(delay);
        Ok(())
    }

    /// Test hook: every later commit's fsync of `partition` first sleeps `delay` (a slow
    /// device for that partition); `Duration::ZERO` stops it.
    #[doc(hidden)]
    pub fn debug_delay_fsyncs(
        &self,
        channel: &str,
        partition: u32,
        delay: std::time::Duration,
    ) -> Result<()> {
        let ch = self.get(channel)?;
        let log = ch.logs.get(partition as usize).ok_or_else(|| {
            CoreError::InvalidArgument(format!("partition {partition} out of range"))
        })?;
        log.delay_fsyncs_for_tests(delay);
        Ok(())
    }

    /// Test hook: the next commit's fsync of `partition` first sleeps `delay` (one-shot).
    #[doc(hidden)]
    pub fn debug_delay_next_fsync(
        &self,
        channel: &str,
        partition: u32,
        delay: std::time::Duration,
    ) -> Result<()> {
        let ch = self.get(channel)?;
        let log = ch.logs.get(partition as usize).ok_or_else(|| {
            CoreError::InvalidArgument(format!("partition {partition} out of range"))
        })?;
        log.delay_next_fsync_for_tests(delay);
        Ok(())
    }

    /// Test hook: one stored message regardless of the high watermark (replica comparison).
    #[doc(hidden)]
    pub fn debug_get_uncapped(&self, channel: &str, id: MessageId) -> Result<Option<Message>> {
        let ch = self.get(channel)?;
        match ch.logs.get(id.partition as usize) {
            Some(log) => Ok(log.get(id.offset)?),
            None => Ok(None),
        }
    }

    /// Test hook: commands waiting in `channel`'s writer queue.
    #[doc(hidden)]
    pub fn debug_writer_queue_len(&self, channel: &str) -> Result<usize> {
        Ok(self.get(channel)?.writer_queue_len())
    }

    /// ISR changes made by partitions this node leads.
    pub fn isr_updates(&self) -> broadcast::Receiver<IsrUpdate> {
        self.inner.shared.isr_events.subscribe()
    }

    /// Stops background tasks, flushes and fsyncs everything, checkpoints offsets.
    pub async fn shutdown(&self) {
        if self.inner.shut_down.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner
            .shared
            .shutting_down
            .store(true, Ordering::Release);
        for t in self.inner.tasks.lock().drain(..) {
            t.abort();
        }
        let channels = self.inner.channel_list();
        for ch in &channels {
            for r in ch.repl.iter() {
                r.wake();
            }
        }
        let closers: Vec<_> = channels.iter().map(|ch| ch.close_writer(None)).collect();
        for c in closers {
            let _ = c.await;
        }
        for ch in &channels {
            ch.close_consumers();
            let _ = self.inner.shared.meta.send(MetaOp::Offsets {
                files: ch.files.clone(),
                bytes: ch.offsets_snapshot(),
            });
        }
        let meta = self.inner.shared.meta.clone();
        let _ = tokio::task::spawn_blocking(move || persist::flush_blocking(&meta)).await;
        self.inner.shared.flush_dead_letter_events();
        info!("broker shut down");
    }
}

impl Inner {
    fn channel_list(&self) -> Vec<Arc<Channel>> {
        self.channels.read().values().cloned().collect()
    }
}

fn spawn_ticker(
    weak: Weak<Inner>,
    every: Duration,
    f: impl Fn(&Inner) + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(inner) = weak.upgrade() else { break };
            f(&inner);
        }
    })
}

/// Retention predicate of one partition: a segment goes when it is older than `max_age`,
/// when the partition still holds at least its byte budget without it (`total` = its current
/// size, decreased by each segment accepted), or when every group of a queue channel
/// consumed it.
fn retention_pred<'a>(
    retention: &'a fluxmq_proto::model::RetentionPolicy,
    now: u64,
    budget: Option<u64>,
    floor: Option<u64>,
    mut total: u64,
) -> impl FnMut(&crate::storage::segment::Segment) -> bool + 'a {
    move |seg| {
        let expired = retention.max_age_secs.is_some_and(|age| {
            seg.max_ts()
                .is_some_and(|t| t.saturating_add(age.saturating_mul(1000)) < now)
        });
        // Kafka's rule: drop a segment for size only if what remains still holds the
        // budget. (v0.5 used `total > budget`, which right after a roll deleted the only
        // closed segment and kept far less than the budget, so groups within the budget lost
        // unconsumed messages.)
        let oversize = budget.is_some_and(|b| total.saturating_sub(seg.visible_len()) >= b);
        let consumed = floor.is_some_and(|f| seg.next_offset() <= f);
        if expired || oversize || consumed {
            total = total.saturating_sub(seg.visible_len());
            true
        } else {
            false
        }
    }
}

/// Per-channel inputs of the retention predicate, captured once per pass.
struct RetentionPlan<'a> {
    ch: &'a Arc<Channel>,
    retention: fluxmq_proto::model::RetentionPolicy,
    budget: Option<u64>,
    queue_floor: Option<Vec<u64>>,
}

impl<'a> RetentionPlan<'a> {
    /// `None` when nothing of `ch` can be due (deleted, or no age/size limit and no queue
    /// groups).
    fn new(ch: &'a Arc<Channel>) -> Option<Self> {
        if ch.deleted.load(Ordering::Acquire) {
            return None;
        }
        let (retention, queue) = {
            let cfg = ch.config.read();
            (cfg.retention.clone(), cfg.mode.is_queue())
        };
        let queue_floor = if queue {
            let groups = ch.groups.read();
            if groups.is_empty() {
                None
            } else {
                let mut floor = vec![u64::MAX; ch.logs.len()];
                for g in groups.values() {
                    for (p, c) in g.committed().into_iter().enumerate() {
                        floor[p] = floor[p].min(c);
                    }
                }
                Some(floor)
            }
        } else {
            None
        };
        if retention.max_age_secs.is_none()
            && retention.max_bytes.is_none()
            && queue_floor.is_none()
        {
            return None;
        }
        let budget = retention
            .max_bytes
            .map(|b| b / u64::from(ch.partitions().max(1)));
        Some(Self {
            ch,
            retention,
            budget,
            queue_floor,
        })
    }

    /// A fresh predicate for partition `p` (its running total starts at the partition's
    /// current size, so each call sees removals made since).
    fn pred(
        &self,
        p: usize,
        now: u64,
    ) -> impl FnMut(&crate::storage::segment::Segment) -> bool + '_ {
        let floor = self.queue_floor.as_ref().map(|f| f[p]);
        retention_pred(
            &self.retention,
            now,
            self.budget,
            floor,
            self.ch.logs[p].storage_bytes(),
        )
    }
}

/// Unpaced retention (`Broker::enforce_retention`): every eligible segment goes now.
/// Returns the number of segments removed.
fn enforce_retention_unpaced(channels: &[Arc<Channel>]) -> usize {
    let now = now_ms();
    let mut removed = 0usize;
    for plan in channels.iter().filter_map(RetentionPlan::new) {
        for (p, log) in plan.ch.logs.iter().enumerate() {
            match log.remove_oldest(usize::MAX, plan.pred(p, now), None) {
                Ok((n, _)) => removed += n as usize,
                Err(e) => {
                    warn!(channel = %plan.ch.name, partition = p, error = %e, "retention failed")
                }
            }
        }
    }
    removed
}

/// Outcome of one paced retention pass.
#[derive(Debug, Default, Clone, Copy)]
struct RetentionPass {
    /// Segments removed.
    removed: usize,
    /// Segments still due when the pass ended (its time budget ran out).
    remaining: usize,
}

/// Paced retention (the background pass). Deleting several large segments at once (every
/// partition of a channel crossing its budget together) stalled concurrent group-commit
/// fsyncs for 0.5–1 s in the v0.5 benchmark, so removals are spread in time: one segment at
/// a time, from the partition (of any channel) with the most due, each unreferenced segment
/// truncated in steps before the unlink, with a pause between removals —
/// [`RETENTION_TICK`] / due, at least [`RETENTION_MIN_GAP`].
///
/// v0.8: the pass removes *every* due segment (within [`RETENTION_PASS_BUDGET`]) instead of
/// at most 16 per channel and second, and passes run every [`RETENTION_TICK`] (50 ms)
/// instead of every second. The removal rate therefore follows the ingest rate: a segment
/// is removed within about two ticks of becoming due, which keeps each partition within its
/// byte budget plus about two segments (one from Kafka's rule — a segment goes only if the
/// remainder still covers the budget — and at most about one rolled since the last pass) at
/// any sustained ingest the broker accepts. v0.7 removed at most 16 segments/s per channel
/// and fell behind above ~250 MB/s per channel (16 MiB segments).
fn enforce_retention_paced(channels: &[Arc<Channel>]) -> RetentionPass {
    let started = Instant::now();
    let now = now_ms();
    let plans: Vec<RetentionPlan<'_>> = channels.iter().filter_map(RetentionPlan::new).collect();
    // (plan, partition, due)
    let mut due: Vec<(usize, usize, usize)> = Vec::new();
    for (i, plan) in plans.iter().enumerate() {
        for p in 0..plan.ch.logs.len() {
            let n = plan.ch.logs[p].count_removable(plan.pred(p, now));
            if n > 0 {
                due.push((i, p, n));
            }
        }
    }
    let mut remaining: usize = due.iter().map(|d| d.2).sum();
    let mut pass = RetentionPass::default();
    if remaining == 0 {
        return pass;
    }
    let gap =
        (RETENTION_TICK / u32::try_from(remaining).unwrap_or(u32::MAX)).max(RETENTION_MIN_GAP);
    let shrink = Some((RETENTION_SHRINK_STEP, RETENTION_SHRINK_PAUSE));
    while remaining > 0 {
        if pass.removed > 0 {
            if started.elapsed() + gap > RETENTION_PASS_BUDGET {
                break;
            }
            std::thread::sleep(gap);
        }
        // Most due first; ties go to the earliest channel/partition.
        let Some(slot) = due
            .iter_mut()
            .filter(|d| d.2 > 0)
            .max_by_key(|d| (d.2, std::cmp::Reverse((d.0, d.1))))
        else {
            break;
        };
        let (i, p) = (slot.0, slot.1);
        let plan = &plans[i];
        // The predicate is re-evaluated against the partition as it is now, so the Kafka rule
        // holds even if the partition changed since the count.
        match plan.ch.logs[p].remove_oldest(1, plan.pred(p, now), shrink) {
            Ok((n, _)) if n > 0 => {
                slot.2 -= 1;
                remaining -= 1;
                pass.removed += n as usize;
            }
            Ok(_) => {
                remaining -= slot.2;
                slot.2 = 0;
            }
            Err(e) => {
                warn!(channel = %plan.ch.name, partition = p, error = %e, "retention failed");
                remaining -= slot.2;
                slot.2 = 0;
            }
        }
    }
    pass.remaining = remaining;
    pass
}

/// An active consumer-group membership. Dropping it unsubscribes and returns its in-flight
/// deliveries to the group (they are redelivered as a new attempt).
pub struct Subscription {
    id: u64,
    channel: Arc<Channel>,
    group: Arc<Group>,
    instance: Arc<str>,
    /// Wakes only this subscription (acks freeing its prefetch credit).
    wake: Arc<tokio::sync::Notify>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.group.remove_sub(&self.channel, self.id);
        self.channel.shared.emit(BrokerEvent::ConsumerDisconnected {
            channel: self.channel.name.clone(),
            group: self.group.name.to_string(),
            instance: self.instance.to_string(),
        });
    }
}

impl Subscription {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Next delivery. Waits while `prefetch` deliveries are outstanding or nothing is
    /// available. Returns `None` when the channel is deleted or the broker shuts down.
    /// Cancel-safe.
    pub async fn next(&mut self) -> Option<Delivery> {
        loop {
            // Register interest before polling so a notification racing with the poll is
            // never lost. Polling only mutates state when it returns a delivery, which is then
            // returned without any further await — dropping this future is always safe.
            let notified = self.group.notify.notified();
            let woken = self.wake.notified();
            tokio::pin!(notified, woken);
            notified.as_mut().enable();
            woken.as_mut().enable();
            let (poll, fetch) = self.group.poll(&self.channel, self.id);
            if !fetch.is_empty() {
                self.group.spawn_fetches(&self.channel, fetch);
            }
            match poll {
                Poll::Ready(d) => return Some(d.into_wire()),
                Poll::Closed => return None,
                Poll::Wait(Some(until)) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut woken => {}
                        _ = tokio::time::sleep_until(until.into()) => {}
                    }
                }
                Poll::Wait(None) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut woken => {}
                    }
                }
            }
        }
    }

    /// Batch variant of [`Subscription::next`]: waits until at least one delivery is
    /// available, then appends everything ready right now to `out` — up to `max` deliveries,
    /// the remaining prefetch credit and `max_bytes` of estimated wire size (with `out` empty
    /// the first one is always taken; appending to a non-empty `out` keeps the bound strict) —
    /// in one critical section, without waiting for more. Returns the estimated
    /// bytes appended, or `None` (nothing appended) when the channel is deleted or the broker
    /// shuts down. Cancel-safe, like `next`.
    pub async fn next_batch(
        &mut self,
        out: &mut Vec<SharedDelivery>,
        max: usize,
        max_bytes: usize,
    ) -> Option<usize> {
        if max == 0 {
            return Some(0);
        }
        loop {
            // Same notification protocol as `next`: state changes only on `Ready`, which
            // returns without a further await.
            let notified = self.group.notify.notified();
            let woken = self.wake.notified();
            tokio::pin!(notified, woken);
            notified.as_mut().enable();
            woken.as_mut().enable();
            match self.poll_batch(out, max, max_bytes) {
                BatchPoll::Ready { bytes } => return Some(bytes),
                BatchPoll::Closed => return None,
                BatchPoll::Wait(Some(until)) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut woken => {}
                        _ = tokio::time::sleep_until(until.into()) => {}
                    }
                }
                BatchPoll::Wait(None) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut woken => {}
                    }
                }
            }
        }
    }

    /// Non-blocking [`Subscription::next_batch`]: appends whatever is ready now (possibly
    /// nothing) and returns the estimated bytes appended; `None` when the subscription ended.
    pub fn try_next_batch(
        &mut self,
        out: &mut Vec<SharedDelivery>,
        max: usize,
        max_bytes: usize,
    ) -> Option<usize> {
        if max == 0 {
            return Some(0);
        }
        match self.poll_batch(out, max, max_bytes) {
            BatchPoll::Ready { bytes } => Some(bytes),
            BatchPoll::Wait(_) => Some(0),
            BatchPoll::Closed => None,
        }
    }

    /// Like [`Subscription::next_batch`], but a raw-records subscription may also get stored
    /// envelopes verbatim in `raw` (each [`RawDelivery`] counts its records against `max`
    /// and prefetch, its bytes against `max_bytes`). Cancel-safe.
    pub async fn next_frames(
        &mut self,
        out: &mut Vec<SharedDelivery>,
        raw: &mut Vec<RawDelivery>,
        max: usize,
        max_bytes: usize,
    ) -> Option<usize> {
        if max == 0 {
            return Some(0);
        }
        loop {
            let notified = self.group.notify.notified();
            let woken = self.wake.notified();
            tokio::pin!(notified, woken);
            notified.as_mut().enable();
            woken.as_mut().enable();
            match self.poll_frames(out, Some(raw), max, max_bytes) {
                BatchPoll::Ready { bytes } => return Some(bytes),
                BatchPoll::Closed => return None,
                BatchPoll::Wait(Some(until)) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut woken => {}
                        _ = tokio::time::sleep_until(until.into()) => {}
                    }
                }
                BatchPoll::Wait(None) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut woken => {}
                    }
                }
            }
        }
    }

    /// Non-blocking [`Subscription::next_frames`].
    pub fn try_next_frames(
        &mut self,
        out: &mut Vec<SharedDelivery>,
        raw: &mut Vec<RawDelivery>,
        max: usize,
        max_bytes: usize,
    ) -> Option<usize> {
        if max == 0 {
            return Some(0);
        }
        match self.poll_frames(out, Some(raw), max, max_bytes) {
            BatchPoll::Ready { bytes } => Some(bytes),
            BatchPoll::Wait(_) => Some(0),
            BatchPoll::Closed => None,
        }
    }

    fn poll_batch(&self, out: &mut Vec<SharedDelivery>, max: usize, max_bytes: usize) -> BatchPoll {
        self.poll_frames(out, None, max, max_bytes)
    }

    fn poll_frames(
        &self,
        out: &mut Vec<SharedDelivery>,
        raw: Option<&mut Vec<RawDelivery>>,
        max: usize,
        max_bytes: usize,
    ) -> BatchPoll {
        let (poll, fetch) = self
            .group
            .poll_batch(&self.channel, self.id, max, max_bytes, out, raw);
        if !fetch.is_empty() {
            self.group.spawn_fetches(&self.channel, fetch);
        }
        poll
    }

    pub fn ack(&self, delivery_ids: &[u64]) -> Result<()> {
        self.group.ack(&self.channel, delivery_ids)
    }

    /// Acks every delivery id in `ranges` (`AckRanges`).
    pub fn ack_ranges(&self, ranges: &[fluxmq_proto::wire::DeliveryRange]) -> Result<()> {
        self.group.ack_ranges(&self.channel, ranges)
    }

    /// Cumulative ack (`Commit`): settles this subscription's deliveries of `partition`
    /// below `offset`. `InvalidArgument` unless the subscription is cumulative.
    pub fn commit(&self, partition: u32, offset: u64) -> Result<()> {
        self.group.commit(&self.channel, self.id, partition, offset)
    }

    /// `retry = false` dead-letters immediately. Otherwise the message is redelivered after the
    /// channel's backoff, or dead-lettered when attempts are exhausted.
    pub fn nack(&self, delivery_id: u64, reason: &str, retry: bool) -> Result<()> {
        self.group.nack(&self.channel, delivery_id, reason, retry)
    }

    pub fn report_stats(&self, stats: ClientStats) {
        self.group.report_stats(self.id, stats);
    }
}

/// See [`Broker::tail`].
pub struct TailReceiver {
    rx: mpsc::Receiver<Message>,
    channel: Arc<Channel>,
    last_published: u64,
}

impl Drop for TailReceiver {
    fn drop(&mut self) {
        self.rx.close();
        self.channel.tail.prune();
    }
}

impl TailReceiver {
    /// Next sampled message; `None` when the channel is deleted.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    /// Messages published to the channel since the last call (for "Showing N of M").
    pub fn take_published_count(&mut self) -> u64 {
        let now = self.channel.metrics.published.get();
        let n = now.saturating_sub(self.last_published);
        self.last_published = now;
        n
    }
}

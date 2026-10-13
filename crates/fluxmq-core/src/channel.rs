//! A channel: partition logs, its writer thread, consumer groups, DLQ, tail and metrics.
//!
//! Write path: `publish` (async) → bounded mpsc → the channel's dedicated writer thread
//! batches all queued requests, appends them, flushes/fsyncs per [`FsyncPolicy`], publishes
//! visibility, then completes every publisher of the batch (group commit). No file I/O ever
//! runs on the async executor.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use fluxmq_proto::batch::RecordsSection;
use fluxmq_proto::model::{ChannelConfig, DeadLetter, MessageId, PublishMessage, StartFrom};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, warn};

use crate::BrokerEvent;
use crate::config::{BrokerConfig, FsyncPolicy};
use crate::dlq::Dlq;
use crate::error::{CoreError, Result};
use crate::fsync_pool::{FsyncPool, GroupResult, Ticket, chains_for_groups};
use crate::group::{Group, UNINIT, start_position};
use crate::history::AttemptHistory;
use crate::metrics::{ChannelMetrics, record, record_n};
use crate::persist::{self, ChannelFiles, MetaSender, OffsetsFile};
use crate::replica::{
    DLQ_PARTITION_BITS, IsrUpdate, PartitionReplica, ReplicaFile, load_replica_file,
};
use crate::stats::LatencyHistogram;
use crate::storage::record::{FIXED_OVERHEAD, MAX_RECORD_LEN};
use crate::storage::{CommitMark, KeyRoute, LogConfig, PartitionLog, PartitionWriter};
use crate::tail::TailHub;
use crate::util::now_ms;

const WRITE_QUEUE: usize = 1024;
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_BATCH_REQS: usize = 4096;
/// Sticky partitioning: keyless messages of one publish request go to one partition, moving
/// to the next one after about this many bytes.
const STICKY_BYTES: usize = 1024 * 1024;
/// While a batch accumulates, a partition's encoded records are handed to the OS once this
/// many bytes are buffered (everything is written at commit anyway).
const EARLY_FLUSH_BYTES: usize = 128 * 1024;
/// A batch is fsynced in the background (pipelined) when commands are queued behind it or it
/// holds at least this many requests; a lone request is fsynced inline (no thread hand-off).
const PIPELINE_MIN_REQS: usize = 4;

/// Forgets a [`crate::dedup::Begin::Fresh`] entry when its request is dropped before the
/// append was queued.
struct PendingGuard<'a> {
    ch: &'a Channel,
    p: usize,
    pid: u64,
    seq: u64,
    armed: bool,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.ch.dedup[self.p]
                .lock()
                .finish(self.pid, self.seq, None);
        }
    }
}

/// Broker-wide state shared with every channel.
pub struct Shared {
    pub config: BrokerConfig,
    pub events: broadcast::Sender<BrokerEvent>,
    pub fsync_latency: Mutex<LatencyHistogram>,
    /// Group commit: batch submitted → a partition's share of it durable and published (v0.9).
    pub commit_latency: Mutex<LatencyHistogram>,
    /// Long-lived fsync threads shared by every channel writer (v0.6).
    pub fsync_pool: FsyncPool,
    pub meta: MetaSender,
    dlq_seq: AtomicU64,
    pub next_sub_id: AtomicU64,
    dead_letters: Mutex<HashMap<(String, String, String), u64>>,
    pub shutting_down: AtomicBool,
    pub node_id: String,
    /// Replicated mode (set once by `Broker::enable_replication`; channels created later
    /// start replicated).
    pub replicated: AtomicBool,
    pub isr_events: broadcast::Sender<IsrUpdate>,
}

impl Shared {
    pub fn new(config: BrokerConfig, meta: MetaSender, first_dlq_id: u64, node_id: String) -> Self {
        Self {
            node_id,
            replicated: AtomicBool::new(false),
            isr_events: broadcast::channel(4096).0,
            fsync_pool: FsyncPool::new(config.fsync_threads),
            config,
            events: broadcast::channel(1024).0,
            fsync_latency: Mutex::new(crate::stats::new_latency_histogram()),
            commit_latency: Mutex::new(crate::stats::new_latency_histogram()),
            meta,
            dlq_seq: AtomicU64::new(first_dlq_id.max(1)),
            next_sub_id: AtomicU64::new(1),
            dead_letters: Mutex::new(HashMap::new()),
            shutting_down: AtomicBool::new(false),
        }
    }

    pub fn emit(&self, ev: BrokerEvent) {
        let _ = self.events.send(ev);
    }

    pub fn next_dlq_id(&self) -> u64 {
        self.dlq_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Raises the DLQ id sequence to at least `min`.
    pub fn bump_dlq_seq(&self, min: u64) {
        self.dlq_seq.fetch_max(min, Ordering::Relaxed);
    }

    /// Replicated mode: the sequence counts in units of partitions (see `DLQ_PARTITION_BITS`).
    pub fn set_dlq_seq(&self, v: u64) {
        self.dlq_seq.store(v.max(1), Ordering::Relaxed);
    }

    pub fn note_dead_letter(&self, channel: &str, group: &str, reason: &str) {
        *self
            .dead_letters
            .lock()
            .entry((channel.to_string(), group.to_string(), reason.to_string()))
            .or_default() += 1;
    }

    /// Emits accumulated `DeadLettered` events (called once per second).
    pub fn flush_dead_letter_events(&self) {
        let drained: Vec<_> = self.dead_letters.lock().drain().collect();
        for ((channel, group, reason), count) in drained {
            self.emit(BrokerEvent::DeadLettered {
                channel,
                group,
                reason,
                count,
            });
        }
    }
}

/// Storage state recovered from disk (built on a blocking thread).
pub struct Recovered {
    pub config: ChannelConfig,
    pub dir: PathBuf,
    pub logs: Vec<Arc<PartitionLog>>,
    pub writers: Vec<PartitionWriter>,
    pub dlq: BTreeMap<u64, DeadLetter>,
    pub offsets: OffsetsFile,
    /// Per partition `replica.json` (replication epoch + leader-epoch history), if any.
    pub replica_files: Vec<Option<ReplicaFile>>,
}

pub fn log_config(cfg: &BrokerConfig) -> LogConfig {
    LogConfig {
        segment_bytes: cfg.segment_bytes,
        sync: matches!(cfg.fsync, FsyncPolicy::GroupCommit { .. }),
        roll_jitter: cfg.segment_jitter,
        cache_bytes: cfg.partition_cache_bytes,
    }
}

/// Opens (creating if needed) all partitions, the DLQ and the offsets of a channel. Blocking.
pub fn recover(dir: &Path, config: ChannelConfig, cfg: &BrokerConfig) -> io::Result<Recovered> {
    fs::create_dir_all(dir)?;
    let lc = log_config(cfg);
    let mut logs = Vec::with_capacity(config.partitions as usize);
    let mut writers = Vec::with_capacity(config.partitions as usize);
    let mut replica_files = Vec::with_capacity(config.partitions as usize);
    for p in 0..config.partitions {
        let pdir = dir.join(format!("p{p}"));
        let (log, w) = PartitionLog::open(&pdir, p, lc)?;
        logs.push(log);
        writers.push(w);
        replica_files.push(load_replica_file(&pdir)?);
    }
    let dlq = persist::load_dlq(dir)?;
    let offsets = persist::load_offsets(dir).unwrap_or_else(|e| {
        warn!(?dir, error = %e, "unreadable offsets checkpoint; groups restart from their start position");
        OffsetsFile::default()
    });
    Ok(Recovered {
        config,
        dir: dir.to_path_buf(),
        logs,
        writers,
        dlq,
        offsets,
        replica_files,
    })
}

pub enum WriteCmd {
    Append {
        msgs: Vec<(u32, PublishMessage)>,
        received: Instant,
        /// Replicated mode: `(partition, leader epoch)` the publish was admitted under; the
        /// writer re-checks leadership before appending (epoch fencing).
        fence: Option<Vec<(u32, u64)>>,
        done: oneshot::Sender<Result<Vec<MessageId>>>,
    },
    /// One producer records section (validated) for `partition`, stored as one envelope.
    /// Completes with `(base_offset, record_count)` once durable per policy.
    AppendBatch {
        partition: u32,
        records: RecordsSection,
        received: Instant,
        /// Replicated mode: the leader epoch the publish was admitted under.
        fence: Option<u64>,
        done: oneshot::Sender<Result<(u64, u32)>>,
    },
    /// Follower: records fetched from the leader under `epoch`. Completes with the new LEO
    /// once durable per policy.
    AppendRaw {
        partition: u32,
        epoch: u64,
        base_offset: u64,
        bytes: bytes::Bytes,
        done: oneshot::Sender<Result<u64>>,
    },
    /// Follower: a barrier — everything queued before it commits first — then the log is
    /// truncated to `min(offset, LEO)` (or, with `reset`, restarted empty at `offset`).
    /// Completes with the resulting LEO. Evaluating the bound *after* the barrier matters: a
    /// publish admitted while this node still led the partition may be queued uncommitted
    /// when it is demoted; it must be removed, not published after the follower decided how
    /// far to truncate.
    Truncate {
        partition: u32,
        epoch: u64,
        offset: u64,
        reset: bool,
        done: oneshot::Sender<Result<u64>>,
    },
    /// Removed replica: a barrier, then — if the partition is still `Offline` at `epoch` —
    /// every record of the partition is deleted and its log restarts empty at offset 0.
    Purge {
        partition: u32,
        epoch: u64,
        done: oneshot::Sender<Result<()>>,
    },
    /// Replicated mode: removes the channel directory when no partition holds anything on
    /// this node (every partition `Offline`, empty at offset 0, no DLQ entries). Completes
    /// with whether the storage is released. The writer re-creates the directory before the
    /// next append / replicated append / truncation (see `ensure_materialized`).
    Release { done: oneshot::Sender<Result<bool>> },
    /// Test hook: blocks the writer thread (mid-batch, before committing) until the sender is
    /// dropped or signalled.
    Pause(std::sync::mpsc::Receiver<()>),
    /// Commits outstanding work, stops the writer, optionally removes `remove_dir`.
    Close {
        remove_dir: Option<PathBuf>,
        done: Option<oneshot::Sender<()>>,
    },
}

pub struct Channel {
    pub name: String,
    pub config: RwLock<ChannelConfig>,
    pub files: Arc<ChannelFiles>,
    pub logs: Vec<Arc<PartitionLog>>,
    writer_tx: mpsc::Sender<WriteCmd>,
    pub groups: RwLock<BTreeMap<String, Arc<Group>>>,
    pub metrics: ChannelMetrics,
    pub history: AttemptHistory,
    pub dlq: Dlq,
    pub tail: TailHub,
    pub shared: Arc<Shared>,
    pub deleted: AtomicBool,
    rr: AtomicU64,
    /// Replicated mode: roles/HW gate publishes and dispatch (false = v0.1 behaviour).
    pub replicated: AtomicBool,
    /// Replication state per partition (inert unless `replicated`).
    pub repl: Arc<[Arc<PartitionReplica>]>,
    /// v0.10 live subscriptions: generation bumped whenever readable data may have moved
    /// (commit, HW, role change, close). See [`crate::live`].
    pub live: tokio::sync::watch::Sender<u64>,
    /// v0.12 idempotent publishing: recent producer batches per partition (see [`crate::dedup`]).
    dedup: Vec<Mutex<crate::dedup::DedupTable>>,
    /// Serializes a partition's table rebuild after a restart / leader change.
    dedup_rebuild: Vec<tokio::sync::Mutex<()>>,
    /// Retried batches answered from the table instead of being stored again.
    pub duplicates: AtomicU64,
}

impl Channel {
    /// Builds the channel from recovered storage, restores groups and starts the writer.
    pub fn start(shared: Arc<Shared>, rec: Recovered) -> io::Result<Arc<Channel>> {
        let (tx, rx) = mpsc::channel(WRITE_QUEUE);
        let files = ChannelFiles::new(rec.dir.clone());
        let repl: Arc<[Arc<PartitionReplica>]> = rec
            .replica_files
            .into_iter()
            .enumerate()
            .map(|(p, f)| {
                Arc::new(PartitionReplica::new(
                    &rec.config.name,
                    p as u32,
                    &shared.node_id,
                    f,
                ))
            })
            .collect();
        let replicated = shared.replicated.load(Ordering::Acquire);
        if replicated {
            // Until the cluster says every replica reads envelopes (rolling upgrade).
            for log in &rec.logs {
                log.set_batch_writes(false);
            }
        }
        let ch = Arc::new(Channel {
            name: rec.config.name.clone(),
            files: files.clone(),
            logs: rec.logs,
            writer_tx: tx,
            groups: RwLock::new(BTreeMap::new()),
            metrics: ChannelMetrics::default(),
            history: AttemptHistory::new(shared.config.attempt_history_capacity),
            dlq: Dlq::new(files, shared.meta.clone(), rec.dlq, repl.clone()),
            replicated: AtomicBool::new(replicated),
            repl,
            tail: TailHub::new(shared.config.tail_max_rate),
            shared,
            deleted: AtomicBool::new(false),
            rr: AtomicU64::new(0),
            dedup: (0..rec.config.partitions)
                .map(|_| Mutex::new(crate::dedup::DedupTable::default()))
                .collect(),
            dedup_rebuild: (0..rec.config.partitions)
                .map(|_| tokio::sync::Mutex::new(()))
                .collect(),
            duplicates: AtomicU64::new(0),
            config: RwLock::new(rec.config),
            live: tokio::sync::watch::Sender::new(0),
        });
        {
            let mode = ch.config.read().mode;
            let mut groups = ch.groups.write();
            let mut acked = rec.offsets.acked;
            for (name, offs) in rec.offsets.groups {
                let ranges = acked.remove(&name).unwrap_or_default();
                let parts = ch
                    .logs
                    .iter()
                    .enumerate()
                    .map(|(p, log)| {
                        let cursor = match offs.get(p).copied() {
                            // Replicated mode: never initialized on this partition.
                            Some(UNINIT) => UNINIT,
                            o => o.unwrap_or(0).clamp(log.low(), log.high()),
                        };
                        let high = log.high();
                        let r = ranges
                            .get(p)
                            .map(|v| {
                                v.iter()
                                    .map(|&[s, e]| (s, e.min(high)))
                                    .filter(|&(s, e)| s < e)
                                    .collect()
                            })
                            .unwrap_or_default();
                        (cursor, r)
                    })
                    .collect();
                groups.insert(name.clone(), Arc::new(Group::restore(&name, mode, parts)));
            }
        }
        let writer_ch = ch.clone();
        let writers = rec.writers;
        std::thread::Builder::new()
            .name(format!("fluxmq-writer-{}", ch.name))
            .spawn(move || writer_loop(writer_ch, writers, rx))?;
        Ok(ch)
    }

    pub fn partitions(&self) -> u32 {
        self.logs.len() as u32
    }

    pub(crate) fn closed_error(&self) -> CoreError {
        if self.deleted.load(Ordering::Acquire) {
            CoreError::ChannelNotFound(self.name.clone())
        } else {
            CoreError::ShuttingDown
        }
    }

    /// Stable partition choice: `crc32(key) % partitions`, round-robin without a key.
    pub fn partition_for(&self, key: Option<&str>) -> u32 {
        let n = self.partitions().max(1);
        match key {
            Some(k) => crc32fast::hash(k.as_bytes()) % n,
            None => (self.rr.fetch_add(1, Ordering::Relaxed) % u64::from(n)) as u32,
        }
    }

    /// Appends `messages`; `partition` forces every message into that partition (the caller
    /// validated it), otherwise each goes to [`Channel::partition_for`].
    pub async fn publish(
        &self,
        messages: Vec<PublishMessage>,
        partition: Option<u32>,
        received: Instant,
    ) -> Result<Vec<MessageId>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let mut msgs = Vec::with_capacity(messages.len());
        let mut sticky = StickyPartitioner::new(self.partitions(), &self.rr);
        // `Priority` channels: a message's partition is its level, whatever its key.
        let levels = (self.config.read().mode == fluxmq_proto::model::ChannelMode::Priority)
            .then(|| self.partitions());
        for m in messages {
            let size = FIXED_OVERHEAD
                + m.key.as_ref().map_or(0, String::len)
                + m.payload.len()
                + m.headers
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 10)
                    .sum::<usize>();
            if size > MAX_RECORD_LEN {
                return Err(CoreError::InvalidArgument(format!(
                    "message of ~{size} bytes exceeds the {MAX_RECORD_LEN} byte limit"
                )));
            }
            let p = match (partition, levels) {
                (Some(p), Some(l)) => {
                    let level = fluxmq_proto::model::priority_level(&m.headers, l);
                    if level != p {
                        return Err(CoreError::InvalidArgument(format!(
                            "message with priority {level} sent to partition {p} of priority channel '{}' (partition = priority level)",
                            self.name
                        )));
                    }
                    p
                }
                (Some(p), None) => p,
                (None, Some(l)) => fluxmq_proto::model::priority_level(&m.headers, l),
                (None, None) => sticky.next(m.key.as_deref(), size),
            };
            msgs.push((p, m));
        }
        let fence = if self.replicated.load(Ordering::Acquire) {
            let mut fence: Vec<(u32, u64)> = Vec::new();
            for (p, _) in &msgs {
                if !fence.iter().any(|(q, _)| q == p) {
                    fence.push((*p, self.repl[*p as usize].admit_publish()?));
                }
            }
            Some(fence)
        } else {
            None
        };
        let (done, rx) = oneshot::channel();
        self.send_cmd(WriteCmd::Append {
            msgs,
            received,
            fence: fence.clone(),
            done,
        })
        .await?;
        let ids = rx.await.map_err(|_| self.closed_error())??;
        if let Some(fence) = fence {
            // acks=all: wait until the ISR holds every message (HW past the last offset).
            for (p, epoch) in fence {
                let end = ids
                    .iter()
                    .filter(|id| id.partition == p)
                    .map(|id| id.offset + 1)
                    .max()
                    .unwrap_or(0);
                self.repl[p as usize].wait_acked(self, epoch, end).await?;
            }
        }
        Ok(ids)
    }

    /// Appends one producer batch (`PublishBatch`) as one envelope, stored verbatim; returns
    /// `(partition, base_offset, record_count)` once durable (and, in replicated mode,
    /// acknowledged by the ISR) — the same durability as [`publish`](Self::publish).
    ///
    /// The section is validated first ([`crate::storage::validate_section`]). With
    /// `partition: None` the batch is routed whole: to the partition every keyed record maps
    /// to (`key_partition`), or a sticky partition when no record has a key; keyed records
    /// spanning partitions are refused. Batches are never split.
    pub async fn publish_batch(
        self: &Arc<Self>,
        partition: Option<u32>,
        records: RecordsSection,
        received: Instant,
    ) -> Result<(u32, u64, u32)> {
        let n = self.partitions();
        if let Some(p) = partition
            && p >= n
        {
            return Err(CoreError::InvalidArgument(format!(
                "partition {p} out of range (channel '{}' has {n})",
                self.name
            )));
        }
        let levels =
            (self.config.read().mode == fluxmq_proto::model::ChannelMode::Priority).then_some(n);
        let info = crate::storage::validate_section_with(&records, n, levels)
            .map_err(|e| CoreError::InvalidArgument(format!("invalid record batch: {e}")))?;
        let p = match (partition, info.priority, info.route) {
            // `Priority` channels: the partition is the batch's level.
            (Some(p), Some(level), _) if p != level => {
                return Err(CoreError::InvalidArgument(format!(
                    "batch with priority {level} sent to partition {p} of priority channel '{}' (partition = priority level)",
                    self.name
                )));
            }
            (_, Some(level), _) => level,
            (Some(p), None, _) => p,
            (None, None, KeyRoute::Partition(p)) => p,
            (None, None, KeyRoute::Keyless) => StickyPartitioner::new(n, &self.rr).next(None, 0),
            (None, None, KeyRoute::Spans) => {
                return Err(CoreError::InvalidArgument(
                    "keyed batch spans partitions; send partition: Some(p)".into(),
                ));
            }
        };
        let fence = if self.replicated.load(Ordering::Acquire) {
            Some(self.repl[p as usize].admit_publish()?)
        } else {
            None
        };
        let Some((pid, seq)) = info.producer else {
            let (base, count) = self.append_section(p, records, received, fence).await?;
            return Ok((p, base, count));
        };
        // Idempotent publishing (v0.12): a retried batch gets the stored copy's offsets.
        self.dedup_ready(p as usize, fence).await;
        for _round in 0..3 {
            let begin = self.dedup[p as usize]
                .lock()
                .begin(pid, seq, Instant::now());
            match begin {
                crate::dedup::Begin::Done(base, count) => {
                    if let Some(epoch) = fence {
                        self.repl[p as usize]
                            .wait_acked(self, epoch, base + u64::from(count))
                            .await?;
                    }
                    self.duplicates.fetch_add(1, Ordering::Relaxed);
                    return Ok((p, base, count));
                }
                crate::dedup::Begin::Wait(mut rx) => {
                    let outcome = match rx.wait_for(Option::is_some).await {
                        Ok(v) => *v,
                        Err(_) => None,
                    };
                    if let Some(Ok((base, count))) = outcome {
                        self.duplicates.fetch_add(1, Ordering::Relaxed);
                        return Ok((p, base, count));
                    }
                    // The original failed: this retry appends (next round is `Fresh`).
                }
                crate::dedup::Begin::Fresh(tx) => {
                    // Until the append is queued, a dropped request (the client went away)
                    // forgets the entry, so its retry appends.
                    let mut guard = PendingGuard {
                        ch: self,
                        p: p as usize,
                        pid,
                        seq,
                        armed: true,
                    };
                    let (done, rx) = oneshot::channel();
                    self.send_cmd(WriteCmd::AppendBatch {
                        partition: p,
                        records,
                        received,
                        fence,
                        done,
                    })
                    .await?;
                    guard.armed = false;
                    // Once queued the batch may be stored whatever happens to this request:
                    // settle the entry in a task of its own, so a retry after a lost
                    // connection — the case this exists for — finds the stored copy.
                    let ch = self.clone();
                    let settle = tokio::spawn(async move {
                        let r: Result<(u64, u32)> = async {
                            let (base, count) = rx.await.map_err(|_| ch.closed_error())??;
                            if let Some(epoch) = fence {
                                ch.repl[p as usize]
                                    .wait_acked(&ch, epoch, base + u64::from(count))
                                    .await?;
                            }
                            Ok((base, count))
                        }
                        .await;
                        let stored = r.as_ref().ok().copied();
                        ch.dedup[p as usize].lock().finish(pid, seq, stored);
                        tx.send_replace(Some(stored.ok_or(())));
                        r
                    });
                    let (base, count) = settle.await.map_err(|_| self.closed_error())??;
                    return Ok((p, base, count));
                }
            }
        }
        // The same batch failed three times while retries waited on it: store it plainly.
        let (base, count) = self.append_section(p, records, received, fence).await?;
        Ok((p, base, count))
    }

    /// Appends one validated records section to partition `p` and waits until it is durable
    /// (and, replicated, acknowledged by the ISR).
    async fn append_section(
        &self,
        p: u32,
        records: RecordsSection,
        received: Instant,
        fence: Option<u64>,
    ) -> Result<(u64, u32)> {
        let (done, rx) = oneshot::channel();
        self.send_cmd(WriteCmd::AppendBatch {
            partition: p,
            records,
            received,
            fence,
            done,
        })
        .await?;
        let (base, count) = rx.await.map_err(|_| self.closed_error())??;
        if let Some(epoch) = fence {
            self.repl[p as usize]
                .wait_acked(self, epoch, base + u64::from(count))
                .await?;
        }
        Ok((base, count))
    }

    /// Makes partition `p`'s idempotence table current for leadership `epoch`: rebuilt from
    /// the tail of the log after a restart or a leader change.
    async fn dedup_ready(&self, p: usize, epoch: Option<u64>) {
        {
            let t = self.dedup[p].lock();
            if t.ready && t.epoch == epoch {
                return;
            }
        }
        let _rebuilding = self.dedup_rebuild[p].lock().await;
        {
            let t = self.dedup[p].lock();
            if t.ready && t.epoch == epoch {
                return;
            }
        }
        let log = self.logs[p].clone();
        let found =
            tokio::task::spawn_blocking(move || crate::dedup::scan_tail(&log, now_ms())).await;
        let mut t = self.dedup[p].lock();
        t.reset(epoch);
        match found {
            Ok(Ok(batches)) => {
                let now = Instant::now();
                for (pid, seq, base, count) in batches {
                    t.record(pid, seq, base, count, now);
                }
            }
            Ok(Err(e)) => {
                warn!(channel = %self.name, partition = p, error = %e, "idempotence table rebuild failed; retries across the restart are not deduplicated");
            }
            Err(_) => {}
        }
        t.ready = true;
    }

    /// Commands queued for the writer (test hook).
    pub fn writer_queue_len(&self) -> usize {
        self.writer_tx.max_capacity() - self.writer_tx.capacity()
    }

    pub(crate) async fn send_cmd(&self, cmd: WriteCmd) -> Result<()> {
        self.writer_tx
            .send(cmd)
            .await
            .map_err(|_| self.closed_error())
    }

    /// Highest offset (exclusive) consumers may receive on `p`; `None` = this node does not
    /// serve the partition (replicated mode, not its leader).
    #[inline]
    pub fn dispatch_limit(&self, p: usize) -> Option<u64> {
        if self.replicated.load(Ordering::Relaxed) {
            self.repl[p].dispatch_limit()
        } else {
            Some(self.logs[p].high())
        }
    }

    /// Upper bound (exclusive) for browsing reads of `p`: the committed end in single-node
    /// mode; in replicated mode the high watermark, so offsets not yet replicated to the ISR
    /// (which a failover may discard) are never shown.
    #[inline]
    pub fn read_limit(&self, p: usize) -> u64 {
        let high = self.logs[p].high();
        if self.replicated.load(Ordering::Relaxed) {
            self.repl[p].high_watermark().min(high)
        } else {
            high
        }
    }

    /// Restarts every group's dispatch state of partition `p` from its committed offset
    /// (role change in replicated mode).
    pub fn reset_partition_groups(&self, p: usize) {
        let groups: Vec<Arc<Group>> = self.groups.read().values().cloned().collect();
        let high = self.logs[p].high();
        for g in groups {
            g.reset_partition(p, None, high);
        }
    }

    /// Committed offset of every group on partition `p` (sorted by group; uninitialized
    /// partitions omitted) — the leader's replicated consumer state.
    pub fn partition_group_offsets(&self, p: usize) -> Vec<(String, u64)> {
        let groups: Vec<(String, Arc<Group>)> = self
            .groups
            .read()
            .iter()
            .map(|(n, g)| (n.clone(), g.clone()))
            .collect();
        groups
            .into_iter()
            .filter_map(|(n, g)| {
                let c = g.committed_at(p);
                (c != UNINIT).then_some((n, c))
            })
            .collect()
    }

    /// Follower: makes the committed positions on `p` exactly `offsets` (the leader's
    /// snapshot): listed groups are set (created if needed), other groups lose their position
    /// on `p`; a group left with no known position and no subscriber is removed (it was
    /// deleted on the leader).
    pub fn apply_replicated_offsets(&self, p: usize, offsets: &[(String, u64)]) {
        for (g, off) in offsets {
            self.apply_replicated_offset(g, p, *off);
        }
        let others: Vec<(String, Arc<Group>)> = self
            .groups
            .read()
            .iter()
            .filter(|(n, _)| !offsets.iter().any(|(g, _)| g == *n))
            .map(|(n, g)| (n.clone(), g.clone()))
            .collect();
        for (name, g) in others {
            if g.committed_at(p) != UNINIT {
                g.reset_partition(p, Some(UNINIT), u64::MAX);
            }
            if g.all_uninit() && g.subscriber_count() == 0 {
                let mut groups = self.groups.write();
                if groups.get(&name).is_some_and(|x| Arc::ptr_eq(x, &g))
                    && g.all_uninit()
                    && g.subscriber_count() == 0
                {
                    g.close();
                    groups.remove(&name);
                }
            }
        }
    }

    /// Follower: sets `group`'s committed position on `p` (creating the group if needed;
    /// its other partitions stay uninitialized until a subscriber or replication sets them).
    pub fn apply_replicated_offset(&self, group: &str, p: usize, offset: u64) {
        let existing = self.groups.read().get(group).cloned();
        let g = match existing {
            Some(g) => g,
            None => {
                let mut groups = self.groups.write();
                groups
                    .entry(group.to_string())
                    .or_insert_with(|| {
                        let mode = self.config.read().mode;
                        let cursors = (0..self.logs.len()).map(|_| (UNINIT, None)).collect();
                        Arc::new(Group::new(group, mode, cursors))
                    })
                    .clone()
            }
        };
        g.reset_partition(p, Some(offset), u64::MAX);
    }

    /// Switches the channel to replicated mode (every partition starts `Offline`).
    pub fn enable_replication(&self) {
        self.replicated.store(true, Ordering::Release);
        for log in &self.logs {
            log.set_batch_writes(false);
        }
        for r in self.repl.iter() {
            r.wake();
        }
        self.notify_groups();
    }

    /// DLQ id for a dead letter of partition `p`.
    pub fn next_dlq_id(&self, p: u32) -> u64 {
        let seq = self.shared.next_dlq_id();
        if self.replicated.load(Ordering::Relaxed) {
            (seq << DLQ_PARTITION_BITS) | u64::from(p)
        } else {
            seq
        }
    }

    /// Asks the writer to stop (and optionally delete `remove_dir` afterwards). Returns a
    /// receiver completed when the writer has exited.
    pub fn close_writer(&self, remove_dir: Option<PathBuf>) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = WriteCmd::Close {
            remove_dir,
            done: Some(tx),
        };
        match self.writer_tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(cmd)) => {
                let sender = self.writer_tx.clone();
                match tokio::runtime::Handle::try_current() {
                    Ok(h) => {
                        h.spawn(async move {
                            let _ = sender.send(cmd).await;
                        });
                    }
                    Err(_) => {
                        let _ = sender.blocking_send(cmd);
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
        rx
    }

    pub fn notify_groups(&self) {
        for g in self.groups.read().values() {
            g.notify.notify_waiters();
        }
        self.notify_live();
    }

    /// Wakes live-subscription readers (v0.10). One atomic load when there are none.
    #[inline]
    pub fn notify_live(&self) {
        if self.live.receiver_count() > 0 {
            self.live.send_modify(|g| *g = g.wrapping_add(1));
        }
    }

    /// Existing group, or a new one starting at `start`.
    pub fn group(&self, name: &str, start: StartFrom) -> Arc<Group> {
        let existing = self.groups.read().get(name).cloned();
        if let Some(g) = existing {
            // Created by replication: its never-initialized partitions start at `start`.
            g.init_uninit(self, start);
            return g;
        }
        let mut groups = self.groups.write();
        groups
            .entry(name.to_string())
            .or_insert_with(|| {
                let mode = self.config.read().mode;
                let cursors = (0..self.logs.len())
                    .map(|p| start_position(self, p, start))
                    .collect();
                Arc::new(Group::new(name, mode, cursors))
            })
            .clone()
    }

    /// Serialized offsets checkpoint of all groups.
    pub fn offsets_snapshot(&self) -> Vec<u8> {
        let groups = self.groups.read();
        let mut file = OffsetsFile::default();
        for (n, g) in groups.iter() {
            let cp = g.checkpoint();
            if cp.iter().any(|(_, r)| !r.is_empty()) {
                file.acked.insert(
                    n.clone(),
                    cp.iter()
                        .map(|(_, r)| r.iter().map(|&(s, e)| [s, e]).collect())
                        .collect(),
                );
            }
            file.groups
                .insert(n.clone(), cp.into_iter().map(|(c, _)| c).collect());
        }
        serde_json::to_vec(&file).expect("offsets serialize to JSON")
    }

    /// Ends subscriptions and tail receivers (delete / shutdown).
    pub fn close_consumers(&self) {
        for g in self.groups.read().values() {
            g.close();
        }
        self.tail.close();
        self.notify_live();
    }
}

/// Partition choice for one publish request (v0.6 sticky partitioner): keyed messages go to
/// `crc32(key) % partitions` (unchanged); keyless messages of the request all go to one
/// partition, taken round-robin per request from `rr`, moving to the next round-robin
/// partition after about [`STICKY_BYTES`]. Keyless traffic is balanced across partitions over
/// many requests, not per message, and a request costs one fsync instead of one per partition.
pub struct StickyPartitioner<'a> {
    n: u32,
    rr: &'a AtomicU64,
    current: Option<u32>,
    bytes: usize,
}

impl<'a> StickyPartitioner<'a> {
    pub fn new(partitions: u32, rr: &'a AtomicU64) -> Self {
        Self {
            n: partitions.max(1),
            rr,
            current: None,
            bytes: 0,
        }
    }

    /// Partition of the next message (`size` = its approximate encoded size).
    pub fn next(&mut self, key: Option<&str>, size: usize) -> u32 {
        if let Some(k) = key {
            return crc32fast::hash(k.as_bytes()) % self.n;
        }
        let p = match self.current {
            Some(p) if self.bytes < STICKY_BYTES => p,
            _ => {
                self.bytes = 0;
                let p = (self.rr.fetch_add(1, Ordering::Relaxed) % u64::from(self.n)) as u32;
                self.current = Some(p);
                p
            }
        };
        self.bytes += size;
        p
    }
}

enum Done {
    Publish(oneshot::Sender<Result<Vec<MessageId>>>, Vec<MessageId>),
    /// Replicated append: completes with the partition's new LEO.
    Raw(oneshot::Sender<Result<u64>>, u64),
    /// Producer batch: `(base_offset, record_count)`.
    Batch(oneshot::Sender<Result<(u64, u32)>>, (u64, u32)),
}

/// Where a request's records went, as `(partition, end offset)`: it is acked once each of
/// those partitions has published (made durable) up to that end.
enum Parts {
    One(usize, u64),
    Many(Vec<(usize, u64)>),
}

struct Waiter {
    received: Instant,
    n: u64,
    bytes: u64,
    parts: Parts,
    done: Done,
}

impl Waiter {
    fn ok(self) {
        match self.done {
            Done::Publish(tx, ids) => {
                let _ = tx.send(Ok(ids));
            }
            Done::Raw(tx, leo) => {
                let _ = tx.send(Ok(leo));
            }
            Done::Batch(tx, r) => {
                let _ = tx.send(Ok(r));
            }
        }
    }

    fn fail(self, e: CoreError) {
        match self.done {
            Done::Publish(tx, _) => {
                let _ = tx.send(Err(e));
            }
            Done::Raw(tx, _) => {
                let _ = tx.send(Err(e));
            }
            Done::Batch(tx, _) => {
                let _ = tx.send(Err(e));
            }
        }
    }
}

fn io_err_copy(e: &io::Error) -> CoreError {
    CoreError::Io(io::Error::new(e.kind(), e.to_string()))
}

/// A partition's share of one commit, waiting to be published (in commit order).
struct PendingMark {
    seq: u64,
    mark: CommitMark,
    /// Its fsync chain(s) succeeded.
    synced: bool,
    submitted: Instant,
}

/// One submitted commit (v0.9): the [`CommitMark`]s of the partitions it covers, their
/// fsyncs grouped per partition on one [`Ticket`].
struct Commit {
    seq: u64,
    ticket: Ticket,
    /// Group index → partition.
    parts: Vec<usize>,
    /// Groups not yet taken from the ticket; the commit leaves `inflight` at 0 (then none
    /// of its fsyncs is running any more).
    untaken: usize,
    /// Unpipelined (v0.8 semantics): groups taken and synced so far; the marks are only
    /// published once all of them are.
    synced: usize,
}

/// Writer-thread state. Group commit, pipelined per partition (v0.9):
/// * the *current* batch (`dirty_list`) accumulates appends, written to the OS as they
///   come; each request waits in its partition's `acks` queue (or in `multi`) for the
///   partition's durable end to reach the request's end offset;
/// * a commit takes a [`CommitMark`] of each dirty partition that has fewer than `depth`
///   marks in flight (`marks[p]`, a FIFO) and starts their fsyncs on the broker's
///   [`FsyncPool`], grouped per partition. A partition whose earlier batch is still
///   fsyncing keeps accumulating and is submitted as soon as that batch is durable — it
///   never waits for other partitions' fsyncs, and other partitions never wait for it;
/// * a partition's mark is published — records visible, replica durable end advanced,
///   requests acked — as soon as its own fsync chain(s) finished *and* every earlier mark of
///   the partition was published. Durable marks advance strictly in order per partition and
///   never past an fsync;
/// * on any fsync failure every in-flight fsync is first waited for, then everything not
///   yet published — every in-flight mark and the current batch — is rolled back to the
///   last published mark and its requests failed (as in v0.6–v0.8). An I/O error in the
///   current batch first lets the in-flight batches finish (published or failed on their
///   own), so a rollback never truncates data whose fsync is still running.
///
/// With `pipeline` off a submitted batch is waited for right away and published only once
/// every partition of it is durable (v0.8 behaviour).
struct WriterState {
    writers: Vec<PartitionWriter>,
    dirty: Vec<bool>,
    dirty_list: Vec<usize>,
    /// Bytes / requests appended to each dirty partition since its last mark.
    pend_bytes: Vec<usize>,
    pend_reqs: Vec<usize>,
    /// Sums of `pend_bytes` / `pend_reqs` (batch size bound).
    batch_bytes: usize,
    batch_reqs: usize,
    /// Single-partition requests not yet acked, per partition, in offset order.
    acks: Vec<VecDeque<Waiter>>,
    /// Requests spanning several partitions, not yet acked.
    multi: Vec<Waiter>,
    /// The channel directory was removed (replicated mode, nothing held locally). Only the
    /// writer releases and re-creates storage, so a plain field suffices.
    released: bool,
    /// Submitted commits with fsyncs not yet taken, oldest first.
    inflight: VecDeque<Commit>,
    /// Per partition: marks taken but not yet published, in commit order.
    marks: Vec<VecDeque<PendingMark>>,
    next_seq: u64,
    /// Group commit (fsync before ack) vs OS-buffered.
    sync: bool,
    pipeline: bool,
    /// Max marks in flight per partition.
    depth: usize,
    /// Every commit ends in one whole-device flush (Apple `F_FULLFSYNC`, see
    /// `chains_for_groups`), which the device serializes: a second commit started while one
    /// is flushing only queues behind it and adds a flush. So only one commit at a time may
    /// have its barrier running (`Ticket::barrier_running`); partitions still complete
    /// individually.
    serial_barrier: bool,
    /// Moving average of a mark's submit → published time, µs (stagger, see `eligible`).
    mark_ewma_us: u64,
    /// Scratch for completed groups.
    ready: Vec<(u64, usize, GroupResult)>,
}

impl WriterState {
    /// Re-creates released storage (channel directory + `channel.json`, then every
    /// partition's directory with an empty segment at its current — zero — offset) before
    /// anything is written. No-op unless released.
    fn ensure_materialized(&mut self, ch: &Channel) -> Result<()> {
        if !self.released {
            return Ok(());
        }
        // Lock order: the config read guard is released before the ChannelFiles lock.
        let cfg = ch.config.read().clone();
        let json =
            serde_json::to_vec_pretty(&cfg).map_err(|e| CoreError::Corrupt(e.to_string()))?;
        if !ch.files.materialize(&json)? {
            return Err(ch.closed_error());
        }
        for w in &mut self.writers {
            w.recreate().map_err(|e| {
                error!(channel = %ch.name, error = %e, "re-creating released partition storage failed");
                CoreError::Io(e)
            })?;
        }
        self.released = false;
        // Replica metadata (epoch fence) persisted while released was skipped: write it now.
        for r in ch.repl.iter() {
            r.persist_current(ch);
        }
        tracing::info!(channel = %ch.name, "channel storage re-created");
        Ok(())
    }

    /// See [`WriteCmd::Purge`].
    fn purge(&mut self, ch: &Channel, p: u32, epoch: u64) -> Result<()> {
        self.commit(ch);
        ch.repl[p as usize].check_offline(epoch)?;
        if self.released {
            // Nothing on disk; every log is empty at 0.
            return Ok(());
        }
        self.writers[p as usize].truncate(0).map_err(|e| {
            error!(channel = %ch.name, partition = p, error = %e, "purging partition data failed");
            CoreError::Io(e)
        })
    }

    /// See [`WriteCmd::Release`].
    fn release(&mut self, ch: &Channel) -> Result<bool> {
        self.commit(ch);
        if self.released {
            return Ok(true);
        }
        let holds_nothing = (0..self.writers.len()).all(|p| {
            ch.repl[p].is_offline()
                && self.writers[p].next_offset() == 0
                && ch.logs[p].high() == 0
                && ch.logs[p].low() == 0
                && ch.dlq.count_partition(p as u32) == 0
        });
        if !ch.replicated.load(Ordering::Acquire) || !holds_nothing {
            return Ok(false);
        }
        let res = ch.files.release_with(|dir| match fs::remove_dir_all(dir) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => {
                if let Some(parent) = dir.parent() {
                    let _ = crate::util::fsync_dir(parent);
                }
                Ok(())
            }
        });
        if res.is_err() {
            // Partly removed: re-create everything before the next write.
            self.released = true;
        }
        let released = res?;
        if released {
            self.released = true;
            tracing::info!(channel = %ch.name, "no local partition data left: channel storage released");
        }
        Ok(released)
    }

    /// Fails the current batch after an I/O error in it: lets the in-flight batches finish
    /// first (published or failed on their own), then rolls the current batch back.
    fn fail_all(&mut self, ch: &Channel, e: &io::Error) {
        self.drain(ch);
        self.rollback_and_fail(ch, &[], e);
    }

    /// Queues a request until its partitions are durable up to its end.
    fn enqueue(&mut self, w: Waiter) {
        let first = match &w.parts {
            Parts::One(p, _) => *p,
            Parts::Many(ps) => ps[0].0,
        };
        self.pend_bytes[first] += w.bytes as usize;
        self.pend_reqs[first] += 1;
        self.batch_bytes += w.bytes as usize;
        self.batch_reqs += 1;
        match &w.parts {
            Parts::One(p, _) => self.acks[*p].push_back(w),
            Parts::Many(_) => self.multi.push(w),
        }
    }

    /// Rolls back every partition in `extra` or the current batch to its last published mark
    /// and fails every request waiting on one of them. Requires no fsync of those
    /// partitions to be running.
    fn rollback_and_fail(&mut self, ch: &Channel, extra: &[usize], e: &io::Error) {
        for &p in extra {
            self.mark_dirty(p);
        }
        if self.dirty_list.is_empty() {
            return;
        }
        error!(channel = %ch.name, error = %e, "write failed; rolling back uncommitted batches");
        let parts = std::mem::take(&mut self.dirty_list);
        let mut failed = Vec::new();
        for &p in &parts {
            if let Err(re) = self.writers[p].rollback() {
                error!(channel = %ch.name, partition = p, error = %re, "rollback failed");
            }
            self.dirty[p] = false;
            self.batch_bytes -= std::mem::take(&mut self.pend_bytes[p]);
            self.batch_reqs -= std::mem::take(&mut self.pend_reqs[p]);
            failed.extend(self.acks[p].drain(..));
        }
        let mut i = 0;
        while i < self.multi.len() {
            let hit = match &self.multi[i].parts {
                Parts::Many(ps) => ps.iter().any(|(p, _)| parts.contains(p)),
                Parts::One(..) => false,
            };
            if hit {
                failed.push(self.multi.swap_remove(i));
            } else {
                i += 1;
            }
        }
        for w in failed {
            w.fail(io_err_copy(e));
        }
    }

    fn append(
        &mut self,
        ch: &Channel,
        msgs: Vec<(u32, PublishMessage)>,
        received: Instant,
        fence: Option<Vec<(u32, u64)>>,
        done: oneshot::Sender<Result<Vec<MessageId>>>,
    ) {
        if let Some(fence) = &fence {
            for &(p, epoch) in fence {
                if let Err(e) = ch.repl[p as usize].check_leader(epoch) {
                    let _ = done.send(Err(e));
                    return;
                }
            }
            if let Err(e) = self.ensure_materialized(ch) {
                let _ = done.send(Err(e));
                return;
            }
            for &(p, epoch) in fence {
                ch.repl[p as usize].maybe_record_epoch_start(
                    ch,
                    epoch,
                    self.writers[p as usize].next_offset(),
                );
            }
        }
        let now = now_ms();
        let n = msgs.len() as u64;
        let bytes: u64 = msgs.iter().map(|(_, m)| m.payload.len() as u64).sum();
        // Group by partition (keeping each partition's order): one call — usually one
        // envelope — per partition. Ids are returned in request order. The common case (one
        // partition per request: explicit or sticky) moves the messages straight through.
        let single = msgs
            .first()
            .map(|(p, _)| *p)
            .filter(|p| msgs.iter().all(|(q, _)| q == p));
        let parts;
        let ids = if let Some(p) = single {
            let pi = p as usize;
            self.mark_dirty(pi);
            match self.writers[pi].append_messages(now, msgs.into_iter().map(|(_, m)| m).collect())
            {
                Ok(base) => {
                    parts = Parts::One(pi, self.writers[pi].next_offset());
                    (0..n)
                        .map(|k| MessageId {
                            partition: p,
                            offset: base + k,
                        })
                        .collect()
                }
                Err(e) => {
                    let _ = done.send(Err(io_err_copy(&e)));
                    self.fail_all(ch, &e);
                    return;
                }
            }
        } else {
            let mut ids = vec![
                MessageId {
                    partition: 0,
                    offset: 0
                };
                msgs.len()
            ];
            let mut groups: Vec<(u32, Vec<usize>, Vec<PublishMessage>)> = Vec::new();
            for (i, (p, m)) in msgs.into_iter().enumerate() {
                match groups.iter_mut().find(|(q, _, _)| *q == p) {
                    Some((_, idx, ms)) => {
                        idx.push(i);
                        ms.push(m);
                    }
                    None => groups.push((p, vec![i], vec![m])),
                }
            }
            let touched: Vec<usize> = groups.iter().map(|(p, _, _)| *p as usize).collect();
            for (p, idx, ms) in groups {
                let pi = p as usize;
                self.mark_dirty(pi);
                match self.writers[pi].append_messages(now, ms) {
                    Ok(base) => {
                        for (k, i) in idx.into_iter().enumerate() {
                            ids[i] = MessageId {
                                partition: p,
                                offset: base + k as u64,
                            };
                        }
                    }
                    Err(e) => {
                        let _ = done.send(Err(io_err_copy(&e)));
                        self.fail_all(ch, &e);
                        return;
                    }
                }
            }
            parts = Parts::Many(
                touched
                    .into_iter()
                    .map(|p| (p, self.writers[p].next_offset()))
                    .collect(),
            );
            ids
        };
        self.enqueue(Waiter {
            received,
            n,
            bytes,
            parts,
            done: Done::Publish(done, ids),
        });
    }

    fn append_batch(
        &mut self,
        ch: &Channel,
        p: u32,
        records: RecordsSection,
        received: Instant,
        fence: Option<u64>,
        done: oneshot::Sender<Result<(u64, u32)>>,
    ) {
        let pi = p as usize;
        if let Some(epoch) = fence {
            if let Err(e) = ch.repl[pi]
                .check_leader(epoch)
                .and_then(|()| self.ensure_materialized(ch))
            {
                let _ = done.send(Err(e));
                return;
            }
            ch.repl[pi].maybe_record_epoch_start(ch, epoch, self.writers[pi].next_offset());
        }
        self.mark_dirty(pi);
        match self.writers[pi].append_batch(now_ms(), &records) {
            Ok((base, count)) => {
                let bytes = records.body.len() as u64;
                self.enqueue(Waiter {
                    received,
                    n: u64::from(count),
                    bytes,
                    parts: Parts::One(pi, base + u64::from(count)),
                    done: Done::Batch(done, (base, count)),
                });
            }
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                // Rejected before anything was queued.
                let _ = done.send(Err(CoreError::InvalidArgument(e.to_string())));
            }
            Err(e) => {
                let _ = done.send(Err(io_err_copy(&e)));
                self.fail_all(ch, &e);
            }
        }
    }

    fn mark_dirty(&mut self, pi: usize) {
        if !self.dirty[pi] {
            self.dirty[pi] = true;
            self.dirty_list.push(pi);
        }
    }

    fn append_raw(
        &mut self,
        ch: &Channel,
        p: u32,
        epoch: u64,
        base_offset: u64,
        bytes: &bytes::Bytes,
        done: oneshot::Sender<Result<u64>>,
    ) {
        let pi = p as usize;
        if let Err(e) = ch.repl[pi]
            .check_follower(epoch)
            .and_then(|()| self.ensure_materialized(ch))
        {
            let _ = done.send(Err(e));
            return;
        }
        let next = self.writers[pi].next_offset();
        if base_offset != next {
            let _ = done.send(Err(CoreError::InvalidArgument(format!(
                "replicated chunk starts at {base_offset}, log end is {next}"
            ))));
            return;
        }
        match self.writers[pi].append_raw(bytes) {
            Ok(leo) => {
                self.mark_dirty(pi);
                self.pend_bytes[pi] += bytes.len();
                self.batch_bytes += bytes.len();
                self.enqueue(Waiter {
                    received: Instant::now(),
                    n: 0,
                    bytes: 0,
                    parts: Parts::One(pi, leo),
                    done: Done::Raw(done, leo),
                });
            }
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                // Verification failed before anything was buffered.
                let _ = done.send(Err(CoreError::Corrupt(e.to_string())));
            }
            Err(e) => {
                self.mark_dirty(pi);
                let _ = done.send(Err(io_err_copy(&e)));
                self.fail_all(ch, &e);
            }
        }
    }

    /// Barrier: commits pending work, then truncates.
    fn truncate(
        &mut self,
        ch: &Channel,
        p: u32,
        epoch: u64,
        offset: u64,
        reset: bool,
    ) -> Result<u64> {
        self.commit(ch);
        ch.repl[p as usize].check_follower(epoch)?;
        self.ensure_materialized(ch)?;
        let w = &mut self.writers[p as usize];
        let high = w.next_offset();
        if (reset && offset != high) || offset < high {
            w.truncate(offset).map_err(|e| {
                error!(channel = %ch.name, partition = p, error = %e, "truncation failed");
                CoreError::Io(e)
            })?;
            // Entries go whole, so the result may be below `offset` (rounded down to an
            // envelope's base). The high watermark is always an entry boundary (batches are
            // acked whole), so rounding never removes acknowledged records.
            let leo = w.next_offset();
            if !reset && leo < offset {
                let hw = ch.repl[p as usize].high_watermark();
                if offset >= hw && leo < hw {
                    error!(channel = %ch.name, partition = p, offset, leo, hw, "truncation rounded below the high watermark");
                    debug_assert!(false, "truncation rounded below the high watermark");
                }
            }
        }
        Ok(w.next_offset())
    }

    /// Barrier commit: finishes every in-flight batch, then makes the current batch durable,
    /// publishes it and completes its publishers before returning.
    fn commit(&mut self, ch: &Channel) {
        self.drain(ch);
        let all = self.dirty_list.clone();
        self.submit(ch, &all, false);
        self.drain(ch);
    }

    /// Waits for every in-flight batch and publishes or fails it.
    fn drain(&mut self, ch: &Channel) {
        while let Some(oldest) = self.inflight.front() {
            // The oldest commit always makes progress once a group of it is ready: its
            // marks are at the front of their partitions' queues.
            oldest.ticket.wait_ready();
            self.pump(ch);
        }
    }

    /// Dirty partitions that may be submitted now, and otherwise the shortest wait until one
    /// may (`None` = only a finished fsync changes anything). A partition may have `depth`
    /// marks in flight; beyond the first, starts are staggered by (average mark time ÷
    /// depth), so overlapping fsyncs of one partition spread out evenly instead of starting
    /// back to back and then leaving every slot busy for a whole fsync.
    fn eligible(&self, out: &mut Vec<usize>) -> Option<Duration> {
        out.clear();
        if self.serial_barrier && self.inflight.iter().any(|c| c.ticket.barrier_running()) {
            return None;
        }
        let stagger = Duration::from_micros(self.mark_ewma_us / self.depth.max(1) as u64);
        let mut wait: Option<Duration> = None;
        for &p in &self.dirty_list {
            let q = &self.marks[p];
            match q.back() {
                None => out.push(p),
                Some(_) if q.len() >= self.depth => {}
                Some(last) => {
                    let since = last.submitted.elapsed();
                    if since >= stagger {
                        out.push(p);
                    } else {
                        let d = stagger - since;
                        wait = Some(wait.map_or(d, |w| w.min(d)));
                    }
                }
            }
        }
        wait
    }

    /// Takes marks of the dirty partitions `parts` and starts their fsyncs, grouped per
    /// partition: inline on this thread when nothing is in flight, a single chain suffices
    /// and there is nothing to overlap with (`overlap` false, or the pipeline is off) — that
    /// saves two thread hand-offs per commit, which is most of the cost at low rates —
    /// otherwise on the pool. Pipelined, the commit then stays in flight while the writer
    /// goes on; unpipelined it is waited for here.
    fn submit(&mut self, ch: &Channel, parts: &[usize], overlap: bool) {
        if parts.is_empty() {
            return;
        }
        let mut taken: Vec<(usize, CommitMark)> = Vec::with_capacity(parts.len());
        for &p in parts {
            match self.writers[p].prepare_commit(self.sync) {
                Ok(m) => taken.push((p, m)),
                Err(e) => {
                    // Let the in-flight batches finish first, then roll back the whole
                    // current batch (taken marks included — rollback discards everything
                    // unpublished).
                    drop(taken);
                    self.drain(ch);
                    self.rollback_and_fail(ch, &[], &e);
                    return;
                }
            }
        }
        for &(p, _) in &taken {
            self.dirty[p] = false;
            self.batch_bytes -= std::mem::take(&mut self.pend_bytes[p]);
            self.batch_reqs -= std::mem::take(&mut self.pend_reqs[p]);
        }
        self.dirty_list.retain(|p| self.dirty[*p]);
        let seq = self.next_seq;
        self.next_seq += 1;
        let n = taken.len();
        let now = Instant::now();
        let mut groups = Vec::with_capacity(n);
        let mut gparts = Vec::with_capacity(n);
        for (p, mut m) in taken {
            groups.push(std::mem::take(&mut m.ops));
            gparts.push(p);
            self.marks[p].push_back(PendingMark {
                seq,
                mark: m,
                synced: false,
                submitted: now,
            });
        }
        let chains = chains_for_groups(groups);
        // With a serial device barrier (Apple) only one commit flushes at a time anyway, so a
        // single-chain commit is always run inline, as in v0.8: the pool would only add two
        // thread hand-offs.
        let inline = self.inflight.is_empty()
            && chains.len() <= 1
            && (!self.pipeline || !overlap || chains.is_empty() || self.serial_barrier);
        let ticket = if inline {
            FsyncPool::run_inline(chains, n)
        } else {
            ch.shared.fsync_pool.submit_groups(chains, n)
        };
        self.inflight.push_back(Commit {
            seq,
            ticket,
            parts: gparts,
            untaken: n,
            synced: 0,
        });
        self.pump(ch);
        if !self.pipeline {
            self.drain(ch);
        }
    }

    /// Takes every finished group of the in-flight commits: on success marks its partition's
    /// mark synced and publishes, per partition and in commit order, every mark whose fsyncs
    /// (and those of all earlier marks of the partition) succeeded, acking the requests that
    /// are then durable; on failure aborts (see [`abort`](Self::abort)).
    fn pump(&mut self, ch: &Channel) {
        let mut ready = std::mem::take(&mut self.ready);
        for c in &self.inflight {
            c.ticket.take_ready_into(c.seq, &mut ready);
        }
        if ready.is_empty() {
            self.ready = ready;
            return;
        }
        let mut failed: Option<io::Error> = None;
        let mut touched: Vec<usize> = Vec::new();
        {
            let mut h = ch.shared.fsync_latency.lock();
            for (seq, g, res) in ready.drain(..) {
                let c = self
                    .inflight
                    .iter_mut()
                    .find(|c| c.seq == seq)
                    .expect("ready group of an in-flight commit");
                c.untaken -= 1;
                match res {
                    Ok(times) => {
                        for d in times {
                            record(&mut h, d.as_micros() as u64);
                        }
                        if self.pipeline {
                            let p = c.parts[g];
                            if let Some(pm) = self.marks[p].iter_mut().find(|m| m.seq == seq) {
                                pm.synced = true;
                            }
                            if !touched.contains(&p) {
                                touched.push(p);
                            }
                        } else {
                            // v0.8 semantics: the whole commit or nothing.
                            c.synced += 1;
                            if c.synced == c.parts.len() {
                                for &p in &c.parts {
                                    if let Some(pm) =
                                        self.marks[p].iter_mut().find(|m| m.seq == seq)
                                    {
                                        pm.synced = true;
                                    }
                                    touched.push(p);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        failed.get_or_insert(e);
                    }
                }
            }
        }
        self.ready = ready;
        self.inflight.retain(|c| c.untaken > 0);
        if let Some(e) = failed {
            self.abort(ch, &e);
            return;
        }
        let replicated = ch.replicated.load(Ordering::Acquire);
        let mut acked: Vec<Waiter> = Vec::new();
        let mut any = false;
        let done_at = Instant::now();
        for p in touched {
            while self.marks[p].front().is_some_and(|m| m.synced) {
                let pm = self.marks[p].pop_front().expect("checked above");
                any = true;
                let leo = pm.mark.next_offset();
                let entries = self.writers[p].publish_mark(pm.mark);
                let us = done_at.duration_since(pm.submitted).as_micros() as u64;
                record(&mut ch.shared.commit_latency.lock(), us);
                self.mark_ewma_us = if self.mark_ewma_us == 0 {
                    us
                } else {
                    (self.mark_ewma_us * 7 + us) / 8
                };
                if !self.writers[p].fsync_times.is_empty() {
                    let mut h = ch.shared.fsync_latency.lock();
                    for d in self.writers[p].fsync_times.drain(..) {
                        record(&mut h, d.as_micros() as u64);
                    }
                }
                if ch.tail.is_active() && !entries.is_empty() {
                    let msgs: Vec<_> = entries
                        .iter()
                        .flat_map(|e| e.messages(p as u32).iter().cloned().collect::<Vec<_>>())
                        .collect();
                    ch.tail.offer(&msgs);
                }
                if replicated {
                    // The mark's end, not the writer's: later appends are not durable yet.
                    ch.repl[p].on_leo(ch, leo);
                }
                let q = &mut self.acks[p];
                while q
                    .front()
                    .is_some_and(|w| matches!(w.parts, Parts::One(_, end) if end <= leo))
                {
                    acked.push(q.pop_front().expect("checked above"));
                }
            }
        }
        if any && !self.multi.is_empty() {
            let logs = &ch.logs;
            let mut i = 0;
            while i < self.multi.len() {
                let complete = match &self.multi[i].parts {
                    Parts::Many(ps) => ps.iter().all(|&(p, end)| logs[p].high() >= end),
                    Parts::One(..) => false,
                };
                if complete {
                    acked.push(self.multi.swap_remove(i));
                } else {
                    i += 1;
                }
            }
        }
        if !acked.is_empty() {
            let (mut n_total, mut bytes_total) = (0, 0);
            {
                let mut h = ch.metrics.publish_latency.lock();
                for w in acked.iter().filter(|w| w.n > 0) {
                    record_n(
                        &mut h,
                        done_at.duration_since(w.received).as_micros() as u64,
                        w.n,
                    );
                    n_total += w.n;
                    bytes_total += w.bytes;
                }
            }
            ch.metrics.published.add(n_total);
            ch.metrics.published_bytes.add(bytes_total);
            for w in acked {
                w.ok();
            }
        }
        if any {
            ch.notify_groups();
        }
    }

    /// An fsync failed: waits until no fsync of any in-flight commit is running, then rolls
    /// back every partition with an unpublished mark (and the current batch) to its last
    /// published mark and fails every request not acked yet. Nothing is published after the
    /// failure was seen, even marks whose own fsyncs succeeded.
    fn abort(&mut self, ch: &Channel, e: &io::Error) {
        for c in self.inflight.drain(..) {
            let _ = c.ticket.wait();
        }
        self.ready.clear();
        let mut extra = Vec::new();
        for (p, q) in self.marks.iter_mut().enumerate() {
            if !q.is_empty() {
                q.clear();
                extra.push(p);
            }
        }
        self.rollback_and_fail(ch, &extra, e);
    }

    fn batch_full(&self) -> bool {
        self.batch_bytes >= MAX_BATCH_BYTES || self.batch_reqs >= MAX_BATCH_REQS
    }
}

struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// What woke the writer while batches were in flight.
enum Wake {
    Cmd(WriteCmd),
    /// A group finished, or the timeout passed.
    Synced,
    Disconnected,
}

/// Blocks until a command arrives (only with `rx`), a group of an in-flight batch finished,
/// or `timeout` passed.
fn wait_cmd_or_sync(
    rx: Option<&mut mpsc::Receiver<WriteCmd>>,
    inflight: &VecDeque<Commit>,
    timeout: Option<Duration>,
    waker: &Waker,
) -> Wake {
    let mut cx = Context::from_waker(waker);
    let deadline = timeout.map(|t| Instant::now() + t);
    let mut rx = rx;
    loop {
        if inflight
            .iter()
            .any(|c| c.ticket.poll_ready(&mut cx).is_ready())
        {
            return Wake::Synced;
        }
        if let Some(rx) = rx.as_deref_mut() {
            match rx.poll_recv(&mut cx) {
                Poll::Ready(Some(cmd)) => return Wake::Cmd(cmd),
                Poll::Ready(None) => return Wake::Disconnected,
                Poll::Pending => {}
            }
        }
        match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return Wake::Synced;
                }
                std::thread::park_timeout(d - now);
            }
            None => std::thread::park(),
        }
    }
}

fn writer_loop(ch: Arc<Channel>, writers: Vec<PartitionWriter>, mut rx: mpsc::Receiver<WriteCmd>) {
    let policy = ch.shared.config.fsync;
    let sync = matches!(policy, FsyncPolicy::GroupCommit { .. });
    let n = writers.len();
    // Nothing to overlap without fsyncs.
    let pipeline = sync && ch.shared.config.commit_pipeline;
    let mut st = WriterState {
        writers,
        dirty: vec![false; n],
        dirty_list: Vec::new(),
        pend_bytes: vec![0; n],
        pend_reqs: vec![0; n],
        batch_bytes: 0,
        batch_reqs: 0,
        acks: (0..n).map(|_| VecDeque::new()).collect(),
        multi: Vec::new(),
        released: false,
        inflight: VecDeque::new(),
        marks: (0..n).map(|_| VecDeque::new()).collect(),
        next_seq: 0,
        sync,
        pipeline,
        depth: if pipeline {
            ch.shared.config.commit_pipeline_depth.max(1)
        } else {
            1
        },
        mark_ewma_us: 0,
        ready: Vec::new(),
        serial_barrier: sync && cfg!(target_vendor = "apple"),
    };
    let mut parts: Vec<usize> = Vec::new();
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let overlapping = st.depth > 1;
    for w in &mut st.writers {
        w.set_overlapping_syncs(overlapping);
    }
    let mut last_sync = Instant::now();
    let mut close: Option<(Option<PathBuf>, Option<oneshot::Sender<()>>)> = None;

    let handle = |st: &mut WriterState, cmd: WriteCmd, close: &mut Option<_>| match cmd {
        WriteCmd::Append {
            msgs,
            received,
            fence,
            done,
        } => st.append(&ch, msgs, received, fence, done),
        WriteCmd::AppendBatch {
            partition,
            records,
            received,
            fence,
            done,
        } => st.append_batch(&ch, partition, records, received, fence, done),
        WriteCmd::AppendRaw {
            partition,
            epoch,
            base_offset,
            bytes,
            done,
        } => st.append_raw(&ch, partition, epoch, base_offset, &bytes, done),
        WriteCmd::Truncate {
            partition,
            epoch,
            offset,
            reset,
            done,
        } => {
            let _ = done.send(st.truncate(&ch, partition, epoch, offset, reset));
        }
        WriteCmd::Purge {
            partition,
            epoch,
            done,
        } => {
            let _ = done.send(st.purge(&ch, partition, epoch));
        }
        WriteCmd::Release { done } => {
            let _ = done.send(st.release(&ch));
        }
        WriteCmd::Pause(rx) => {
            let _ = rx.recv();
        }
        WriteCmd::Close { remove_dir, done } => *close = Some((remove_dir, done)),
    };

    loop {
        // Publish and ack whatever became durable.
        if !st.inflight.is_empty() {
            st.pump(&ch);
        }
        if close.is_none() && st.dirty_list.is_empty() && st.inflight.is_empty() {
            match rx.blocking_recv() {
                Some(cmd) => handle(&mut st, cmd, &mut close),
                None => break,
            }
        }
        while close.is_none() && !st.batch_full() {
            match rx.try_recv() {
                Ok(cmd) => handle(&mut st, cmd, &mut close),
                Err(_) => break,
            }
        }
        if close.is_some() {
            st.commit(&ch);
            break;
        }
        // When nothing can be submitted yet: how long until a partition may be (stagger),
        // `None` = only a command or a finished fsync changes anything.
        let mut timeout = None;
        if !st.dirty_list.is_empty() {
            let due = match policy {
                FsyncPolicy::OsBuffered => true,
                FsyncPolicy::GroupCommit { interval } => {
                    rx.is_empty() || last_sync.elapsed() >= interval || st.batch_full()
                }
            };
            if !due {
                // More requests are queued: take them into this batch first.
                continue;
            }
            let wait = st.eligible(&mut parts);
            if !parts.is_empty() {
                // Overlap only under concurrent load: requests already queued behind this
                // batch, a batch gathered from several publishers, or commits in flight.
                let overlap =
                    !rx.is_empty() || st.batch_reqs >= PIPELINE_MIN_REQS || !st.inflight.is_empty();
                st.submit(&ch, &parts, overlap);
                last_sync = Instant::now();
                continue;
            }
            timeout = wait;
        }
        if st.inflight.is_empty() {
            continue;
        }
        // Keep accumulating behind the in-flight commits, but hand bytes to the OS so
        // buffers stay small and the write of this batch overlaps the earlier fsyncs. Small
        // buffers wait for the next one (one write(2) per partition per wake-up would cost
        // more than it overlaps).
        for i in 0..st.dirty_list.len() {
            let p = st.dirty_list[i];
            if st.writers[p].buffered() < EARLY_FLUSH_BYTES {
                continue;
            }
            if let Err(e) = st.writers[p].flush() {
                st.fail_all(&ch, &e);
                break;
            }
        }
        if st.inflight.is_empty() {
            continue;
        }
        // Nothing more fits in the batch: wait for an fsync only.
        let rx_opt = (!st.batch_full()).then_some(&mut rx);
        match wait_cmd_or_sync(rx_opt, &st.inflight, timeout, &waker) {
            Wake::Cmd(cmd) => handle(&mut st, cmd, &mut close),
            Wake::Synced => {}
            Wake::Disconnected => close = Some((None, None)),
        }
    }
    // Disconnected with work pending (all senders gone): complete it.
    st.commit(&ch);
    // Stop accepting; fail whatever is still queued.
    rx.close();
    while let Ok(cmd) = rx.try_recv() {
        match cmd {
            WriteCmd::Append { done, .. } => {
                let _ = done.send(Err(ch.closed_error()));
            }
            WriteCmd::AppendRaw { done, .. } => {
                let _ = done.send(Err(ch.closed_error()));
            }
            WriteCmd::AppendBatch { done, .. } => {
                let _ = done.send(Err(ch.closed_error()));
            }
            WriteCmd::Truncate { done, .. } => {
                let _ = done.send(Err(ch.closed_error()));
            }
            WriteCmd::Purge { done, .. } => {
                let _ = done.send(Err(ch.closed_error()));
            }
            WriteCmd::Release { done } => {
                let _ = done.send(Err(ch.closed_error()));
            }
            WriteCmd::Pause(_) => {}
            WriteCmd::Close { done, .. } => {
                if let Some(d) = done {
                    let _ = d.send(());
                }
            }
        }
    }
    for w in &mut st.writers {
        if let Err(e) = w.close() {
            warn!(channel = %ch.name, error = %e, "closing partition writer failed");
        }
    }
    drop(st);
    let (remove_dir, done) = close.unwrap_or((None, None));
    if let Some(dir) = remove_dir
        && let Err(e) = fs::remove_dir_all(&dir)
    {
        warn!(?dir, error = %e, "removing deleted channel data failed");
    }
    if let Some(d) = done {
        let _ = d.send(());
    }
}

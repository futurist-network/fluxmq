//! Per-partition replication state (roadmap stage 9): role, leader epoch, high watermark,
//! in-sync replicas, follower progress, leader-epoch history and the state stream (group
//! offsets + DLQ mutations) that followers replicate alongside the records.
//!
//! The network side (fetchers, listener, role orchestration) lives in `fluxmq-replication`;
//! this module owns every invariant that touches the log:
//!
//! * **Default path.** A broker that never calls [`crate::Broker::enable_replication`] has
//!   `Channel::replicated == false`; every hook below is skipped after one relaxed atomic load
//!   and the broker behaves exactly as in v0.1.
//! * **Roles.** In replicated mode every partition is `Offline` until a role is set. Only a
//!   `Leader` accepts publishes and dispatches to consumers; `Follower`/`Offline` reject
//!   publishes with [`CoreError::NotLeader`] and serve no consumers.
//! * **Epoch fencing.** A role change never goes back in epoch. Every write-path command that
//!   was admitted under a role carries the epoch it was admitted under, and the channel's
//!   writer thread re-checks it before touching the log: a publish admitted by a leader that
//!   was demoted meanwhile, or a replicated chunk fetched under an old follower epoch, is
//!   rejected instead of being appended.
//! * **High watermark.** `hw = min LEO over (ISR ∪ last committed ISR)`, monotonic. Consumers
//!   only see offsets `< hw`. A follower whose LEO is unknown blocks HW progress, so after a
//!   promotion nothing new is exposed until every electable replica has reported in.
//! * **Acks.** `ack_hw` follows `hw` but only advances while `|ISR| >= min_isr`; a publish is
//!   acknowledged once `ack_hw` passes its last offset (on top of local durability), and fails
//!   fast with [`CoreError::NotEnoughReplicas`] when the ISR is too small.
//! * **ISR shrink is two-phase.** Evicting a lagging follower removes it from the ISR (and
//!   reports an [`IsrUpdate`]) immediately, but it keeps counting for the HW until the
//!   controller confirms the smaller ISR via `isr_committed` (it elects only from the committed
//!   ISR, so an acked offset must be on every committed member). Expansion is immediate.
//! * **Leader epochs.** Each partition keeps `epoch → first offset` (persisted, written by the
//!   meta thread). A leader records its epoch lazily, in the writer thread, right before its
//!   first append of the epoch; followers copy the leader's history for the prefix they hold.
//!   A replica that becomes follower asks the leader for the end offset of its own last epoch
//!   and truncates to it (Kafka KIP-101/279 semantics), which removes exactly the divergent
//!   suffix a deposed leader may have.
//! * **Replica moves (v0.4).** A new replica starts at the leader's log start: an empty log,
//!   or one entirely below the leader's retention, is restarted empty at the leader's
//!   `log_start_offset` (`reset_to`; the partition log supports a first segment with any base
//!   offset — creation, recovery, reads, retention, stats). Its first fetch of a leadership
//!   always carries a state *snapshot* (all group committed offsets + the partition's whole
//!   DLQ), so nothing depends on state-stream history it never saw, and the leader admits it
//!   to the ISR only once its state cursor names the current epoch (snapshot applied). A
//!   replica removed from the replica list deletes its data (`remove_local_data`); see the
//!   storage release in `channel.rs`.
//!
//! Lock order: `PartitionReplica::state` is the innermost lock (level 6 in `lib.rs`). It may be
//! taken while holding a group lock or the DLQ state lock, and nothing is acquired under it.
//! `Notify` wake-ups and channel notifications happen after it is released.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, oneshot};
use tracing::{debug, info};

use crate::channel::{Channel, WriteCmd};
use crate::error::{CoreError, Result};
use crate::persist::{MetaOp, StoredDeadLetter};
use crate::storage::RawChunk;

/// How often a leader re-reports an ISR that the controller has not committed yet.
const ISR_RESEND: Duration = Duration::from_secs(2);
/// A member dropped by a committed ISR may not rejoin before this. The committed value can
/// be older than the leader's last expansion (the reconciler applies metadata revisions in
/// order and may lag one write behind); without a hold-off the follower rejoins on its next
/// fetch, is reported, the next stale revision drops it again, and the ISR flaps for as
/// long as the lag lasts (seen for 90 s, ~20 metadata writes/s). A shrink is always safe.
const ISR_REJOIN_HOLDOFF: Duration = Duration::from_secs(1);

const OFFLINE: u8 = 0;
const LEADER: u8 = 1;
const FOLLOWER: u8 = 2;

/// Max DLQ mutations buffered for followers; a follower further behind gets a snapshot.
const STATE_LOG_MAX: usize = 4096;
/// Bits of a replicated-mode DLQ id that carry the partition (ids stay unique cluster-wide:
/// only a partition's leader allocates ids for it).
pub(crate) const DLQ_PARTITION_BITS: u32 = 10;

/// File (per partition directory) holding the highest epoch seen and the epoch history.
pub(crate) const REPLICA_FILE: &str = "replica.json";

/// Persisted replica metadata (`p{n}/replica.json`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ReplicaFile {
    pub epoch: u64,
    /// `(leader epoch, first offset)`, ascending in both.
    pub epochs: Vec<(u64, u64)>,
}

/// Role of this node for a partition, as seen by [`ReplicaHandle::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaRole {
    /// Replication not enabled on this broker (single node).
    Standalone,
    Leader,
    Follower {
        leader: String,
    },
    Offline,
}

/// Parameters of a leadership (see [`ReplicaHandle::become_leader`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderParams {
    pub epoch: u64,
    /// All replicas including this node.
    pub replicas: Vec<String>,
    /// Publishes fail with [`CoreError::NotEnoughReplicas`] while `|ISR| < min_isr`.
    pub min_isr: u32,
    /// A follower not caught up within this long is removed from the ISR.
    pub replica_lag_max: Duration,
    /// A publish whose ack waits on replication longer than this fails with
    /// [`CoreError::AckTimeout`] (retryable; the message may still be committed later).
    pub ack_timeout: Duration,
}

/// Reported (broadcast) whenever a leader changes a partition's ISR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsrUpdate {
    pub channel: String,
    pub partition: u32,
    pub epoch: u64,
    pub isr: Vec<String>,
}

/// Snapshot of a partition's replication state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaStatus {
    pub role: ReplicaRole,
    pub epoch: u64,
    pub log_start_offset: u64,
    pub log_end_offset: u64,
    pub high_watermark: u64,
    /// Leader only: current ISR (sorted, includes this node).
    pub isr: Vec<String>,
    /// Leader only: ISR last confirmed by the controller.
    pub committed_isr: Vec<String>,
    /// Leader only: follower → last reported LEO (`None` = not heard from this epoch).
    pub follower_leo: BTreeMap<String, Option<u64>>,
}

/// Which part of the leader's state stream a follower has applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateCursor {
    /// Leader epoch the state came from (`None` = nothing applied yet).
    pub epoch: Option<u64>,
    pub seq: u64,
    pub offsets_gen: u64,
}

/// A follower's fetch, as seen by the leader.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub replica: String,
    pub epoch: u64,
    /// Where to read from: the follower's log end, which may be ahead of its durable end
    /// (v0.8 followers fetch ahead while earlier chunks are being fsynced).
    pub fetch_offset: u64,
    /// The follower's durable log end (everything below is fsynced on it), `<= fetch_offset`.
    /// Followers before v0.8 fetch only from their durable end: `fetch_offset`. Only this
    /// value counts for the high watermark and ISR admission.
    pub durable_offset: u64,
    pub max_bytes: usize,
    pub state: StateCursor,
}

/// What a leader returns to a fetch.
#[derive(Debug, Clone)]
pub struct FetchData {
    pub epoch: u64,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    pub log_end_offset: u64,
    pub records: Option<RawChunk>,
    pub epochs: Vec<(u64, u64)>,
    /// Opaque state batch for [`ReplicaHandle::apply_state`].
    pub state: Option<Bytes>,
}

/// Replication-protocol level errors (serializable: they travel to followers).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum FetchError {
    #[error("not the leader (epoch {epoch}, leader {leader:?})")]
    NotLeader { epoch: u64, leader: Option<String> },
    /// The requester's epoch is older than the leader's (its role is stale).
    #[error("fenced: requester epoch is older than current epoch {current}")]
    FencedEpoch { current: u64 },
    /// The requester's epoch is newer than ours (we have not learned the new epoch yet).
    #[error("unknown epoch: newer than current epoch {current}")]
    UnknownEpoch { current: u64 },
    #[error("fetch offset out of range (log start {log_start}, log end {log_end})")]
    OffsetOutOfRange { log_start: u64, log_end: u64 },
    #[error("not a replica of this partition")]
    NotReplica,
    #[error("unknown channel or partition")]
    UnknownPartition,
    #[error("{0}")]
    Internal(String),
}

impl From<CoreError> for FetchError {
    fn from(e: CoreError) -> Self {
        match e {
            CoreError::ChannelNotFound(_) => FetchError::UnknownPartition,
            e => FetchError::Internal(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DlqOp {
    Add(Box<StoredDeadLetter>),
    Remove(u64),
}

#[derive(Serialize, Deserialize)]
struct StateBatch {
    epoch: u64,
    seq: u64,
    /// `dlq` is the complete DLQ of the partition (replaces the follower's).
    snapshot: bool,
    dlq: Vec<DlqOp>,
    offsets: Option<(u64, Vec<(String, u64)>)>,
}

struct Progress {
    leo: Option<u64>,
    last_caught_up: Instant,
    /// Durability lag (v0.8 followers fetch ahead of their durable end): the follower's
    /// durable end had reached the leader's written end as of this instant. A follower whose
    /// fsyncs stall keeps fetching for a while, but this stops moving and the follower
    /// leaves the ISR after `replica_lag_max` like one that stopped fetching.
    durable_caught_up: Instant,
    /// `(when, leader's written end then)`: passing that offset durably moves
    /// `durable_caught_up` to `when`.
    durable_mark: Option<(Instant, u64)>,
    last_fetch: Option<Instant>,
    leo_at_last_fetch: u64,
    /// Caught up with the previous fetch but not yet in the ISR: counts for the HW so it can
    /// reach it under continuous load.
    candidate: bool,
    /// Dropped by a committed ISR: no rejoin before this (see [`ISR_REJOIN_HOLDOFF`]).
    rejoin_after: Option<Instant>,
}

struct LeaderState {
    replicas: Vec<String>,
    min_isr: u32,
    lag_max: Duration,
    isr: BTreeSet<String>,
    committed_isr: BTreeSet<String>,
    /// While the ISR differs from the controller-committed one: when to report it again.
    /// An ISR report can be lost (the metadata write gave up, a lagged broadcast); without
    /// a re-send, an expansion would never reach the controller, because the leader's own
    /// ISR already holds the follower and nothing changes locally any more.
    isr_resend_at: Option<Instant>,
    followers: HashMap<String, Progress>,
    state_log: VecDeque<(u64, DlqOp)>,
    state_seq: u64,
    offsets_cache: Vec<(String, u64)>,
    offsets_gen: u64,
}

struct ReplicaState {
    /// Leader id while following.
    leader_id: Option<String>,
    leader: Option<LeaderState>,
    epochs: Vec<(u64, u64)>,
    cursor: StateCursor,
}

/// Replication state of one partition (one per partition of every channel).
pub struct PartitionReplica {
    channel: String,
    partition: u32,
    self_id: String,
    mode: AtomicU8,
    epoch: AtomicU64,
    hw: AtomicU64,
    ack_hw: AtomicU64,
    isr_len: AtomicU32,
    min_isr: AtomicU32,
    ack_timeout_ms: AtomicU64,
    /// Set on promotion; the writer records `(epoch, next offset)` before its first append.
    epoch_start_pending: AtomicBool,
    /// HW / ack HW / ISR / role changes (publish waiters).
    notify: Notify,
    /// LEO advanced or role changed (leader-side fetch long-polls).
    data: Notify,
    state: Mutex<ReplicaState>,
}

/// Outcome of a state-changing step, applied after the replica lock is released.
#[derive(Default)]
struct Effects {
    hw_changed: bool,
    notify: bool,
    isr: Option<Vec<String>>,
}

impl PartitionReplica {
    pub(crate) fn new(
        channel: &str,
        partition: u32,
        self_id: &str,
        file: Option<ReplicaFile>,
    ) -> Self {
        let file = file.unwrap_or_default();
        Self {
            channel: channel.to_string(),
            partition,
            self_id: self_id.to_string(),
            mode: AtomicU8::new(OFFLINE),
            epoch: AtomicU64::new(file.epoch),
            hw: AtomicU64::new(0),
            ack_hw: AtomicU64::new(0),
            isr_len: AtomicU32::new(0),
            min_isr: AtomicU32::new(1),
            ack_timeout_ms: AtomicU64::new(30_000),
            epoch_start_pending: AtomicBool::new(false),
            notify: Notify::new(),
            data: Notify::new(),
            state: Mutex::new(ReplicaState {
                leader_id: None,
                leader: None,
                epochs: file.epochs,
                cursor: StateCursor::default(),
            }),
        }
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.mode.load(Ordering::Acquire) == LEADER
    }

    pub(crate) fn high_watermark(&self) -> u64 {
        self.hw.load(Ordering::Acquire)
    }

    /// Leader or follower: committed data below the HW may be served read-only (v0.10 live
    /// subscriptions). Offline replicas serve nothing.
    pub(crate) fn serves_reads(&self) -> bool {
        matches!(self.mode.load(Ordering::Acquire), LEADER | FOLLOWER)
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Highest offset (exclusive) consumers may see; `None` = do not serve this partition.
    #[inline]
    pub(crate) fn dispatch_limit(&self) -> Option<u64> {
        if self.mode.load(Ordering::Acquire) == LEADER {
            Some(self.hw.load(Ordering::Acquire))
        } else {
            None
        }
    }

    fn not_leader(&self) -> CoreError {
        CoreError::NotLeader {
            channel: self.channel.clone(),
            partition: self.partition,
            leader: self.state.lock().leader_id.clone(),
        }
    }

    /// Admission check for a publish; returns the epoch to fence the write with.
    pub(crate) fn admit_publish(&self) -> Result<u64> {
        if !self.is_leader() {
            return Err(self.not_leader());
        }
        let epoch = self.epoch();
        let (isr, min_isr) = (
            self.isr_len.load(Ordering::Acquire),
            self.min_isr.load(Ordering::Acquire),
        );
        if isr < min_isr {
            return Err(self.not_enough(isr, min_isr));
        }
        Ok(epoch)
    }

    fn not_enough(&self, isr: u32, min_isr: u32) -> CoreError {
        CoreError::NotEnoughReplicas {
            channel: self.channel.clone(),
            partition: self.partition,
            isr,
            min_isr,
        }
    }

    /// Writer-thread fence for a leader append admitted under `epoch`.
    pub(crate) fn check_leader(&self, epoch: u64) -> Result<()> {
        if self.is_leader() && self.epoch() == epoch {
            Ok(())
        } else {
            Err(self.not_leader())
        }
    }

    pub(crate) fn is_offline(&self) -> bool {
        self.mode.load(Ordering::Acquire) == OFFLINE
    }

    /// Writer-thread fence for a removed replica's purge admitted under `epoch`.
    pub(crate) fn check_offline(&self, epoch: u64) -> Result<()> {
        let current = self.epoch();
        if !self.is_offline() || current != epoch {
            return Err(CoreError::StaleEpoch {
                got: epoch,
                current,
            });
        }
        Ok(())
    }

    /// Writer-thread fence for follower-side commands admitted under `epoch`.
    pub(crate) fn check_follower(&self, epoch: u64) -> Result<()> {
        let current = self.epoch();
        if self.mode.load(Ordering::Acquire) != FOLLOWER || current != epoch {
            return Err(CoreError::StaleEpoch {
                got: epoch,
                current,
            });
        }
        Ok(())
    }

    /// Called by the writer thread right before the first append of a leadership.
    pub(crate) fn maybe_record_epoch_start(&self, ch: &Channel, epoch: u64, next_offset: u64) {
        if !self.epoch_start_pending.load(Ordering::Acquire) {
            return;
        }
        let file = {
            let mut st = self.state.lock();
            if !self.is_leader() || self.epoch() != epoch {
                return;
            }
            self.epoch_start_pending.store(false, Ordering::Release);
            if st.epochs.last().is_some_and(|&(e, _)| e >= epoch) {
                return;
            }
            st.epochs.push((epoch, next_offset));
            self.file_locked(&st)
        };
        self.persist(ch, file);
    }

    fn file_locked(&self, st: &ReplicaState) -> ReplicaFile {
        ReplicaFile {
            epoch: self.epoch(),
            epochs: st.epochs.clone(),
        }
    }

    /// Persists the current epoch / history (after released storage was re-created).
    pub(crate) fn persist_current(&self, ch: &Channel) {
        let file = self.file_locked(&self.state.lock());
        self.persist(ch, file);
    }

    fn persist(&self, ch: &Channel, file: ReplicaFile) {
        let bytes = serde_json::to_vec(&file).expect("replica file serializes to JSON");
        let _ = ch.shared.meta.send(MetaOp::File {
            files: ch.files.clone(),
            name: format!("p{}/{REPLICA_FILE}", self.partition),
            bytes,
        });
    }

    /// Called by the writer after a commit made `leo` visible locally.
    pub(crate) fn on_leo(&self, ch: &Channel, leo: u64) {
        if self.is_leader() {
            let fx = {
                let mut st = self.state.lock();
                self.recompute_locked(&mut st, leo)
            };
            self.apply(ch, fx);
        }
        self.data.notify_waiters();
    }

    /// Recomputes HW / ack HW (leader). Caller holds the state lock.
    fn recompute_locked(&self, st: &mut ReplicaState, leo: u64) -> Effects {
        let mut fx = Effects::default();
        let Some(l) = st.leader.as_ref() else {
            return fx;
        };
        let mut min = leo;
        for id in l.isr.iter().chain(l.committed_isr.iter()) {
            if *id == self.self_id {
                continue;
            }
            match l.followers.get(id).and_then(|p| p.leo) {
                Some(f) => min = min.min(f),
                // Unknown LEO (not heard from this epoch, or not a follower we track): the
                // HW cannot move until it reports.
                None => return fx,
            }
        }
        for p in l.followers.values() {
            if p.candidate
                && let Some(f) = p.leo
            {
                min = min.min(f);
            }
        }
        let old = self.hw.load(Ordering::Acquire);
        if min > old {
            self.hw.store(min, Ordering::Release);
            fx.hw_changed = true;
            fx.notify = true;
        }
        let hw = self.hw.load(Ordering::Acquire);
        if l.isr.len() as u32 >= l.min_isr && hw > self.ack_hw.load(Ordering::Acquire) {
            self.ack_hw.store(hw, Ordering::Release);
            fx.notify = true;
        }
        fx
    }

    fn apply(&self, ch: &Channel, fx: Effects) {
        if fx.notify {
            self.notify.notify_waiters();
        }
        if fx.hw_changed {
            ch.notify_groups();
        }
        if let Some(isr) = fx.isr {
            let _ = ch.shared.isr_events.send(IsrUpdate {
                channel: self.channel.clone(),
                partition: self.partition,
                epoch: self.epoch(),
                isr,
            });
        }
    }

    /// Waits until `target` (exclusive end offset) is acknowledged by the ISR under `epoch`.
    /// Bounded by the leadership's `ack_timeout`.
    pub(crate) async fn wait_acked(&self, ch: &Channel, epoch: u64, target: u64) -> Result<()> {
        let ms = self.ack_timeout_ms.load(Ordering::Acquire).max(1);
        match tokio::time::timeout(
            Duration::from_millis(ms),
            self.wait_acked_inner(ch, epoch, target),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(CoreError::AckTimeout {
                channel: self.channel.clone(),
                partition: self.partition,
                waited_ms: ms,
            }),
        }
    }

    async fn wait_acked_inner(&self, ch: &Channel, epoch: u64, target: u64) -> Result<()> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.ack_hw.load(Ordering::Acquire) >= target
                && self.epoch() == epoch
                && self.is_leader()
            {
                return Ok(());
            }
            if ch.shared.shutting_down.load(Ordering::Acquire) {
                return Err(CoreError::ShuttingDown);
            }
            if ch.deleted.load(Ordering::Acquire) {
                return Err(CoreError::ChannelNotFound(ch.name.clone()));
            }
            if !self.is_leader() || self.epoch() != epoch {
                return Err(self.not_leader());
            }
            let (isr, min_isr) = (
                self.isr_len.load(Ordering::Acquire),
                self.min_isr.load(Ordering::Acquire),
            );
            if isr < min_isr {
                return Err(self.not_enough(isr, min_isr));
            }
            notified.await;
        }
    }

    /// Wakes every waiter (shutdown / deletion) so it re-checks its condition.
    pub(crate) fn wake(&self) {
        self.notify.notify_waiters();
        self.data.notify_waiters();
    }

    fn set_params(&self, p: &LeaderParams) {
        let ms = p.ack_timeout.as_millis().clamp(1, u128::from(u64::MAX)) as u64;
        self.ack_timeout_ms.store(ms, Ordering::Release);
    }

    fn set_isr_len(&self, l: &LeaderState) {
        self.isr_len.store(l.isr.len() as u32, Ordering::Release);
        self.min_isr.store(l.min_isr, Ordering::Release);
    }

    /// DLQ hook (called with the DLQ state lock held, see lock order): records a mutation for
    /// followers when this node leads the partition.
    pub(crate) fn push_dlq_add(&self, dl: &fluxmq_proto::model::DeadLetter) {
        if self.is_leader() {
            self.push_op(DlqOp::Add(Box::new(StoredDeadLetter::from(dl))));
        }
    }

    pub(crate) fn push_dlq_remove(&self, id: u64) {
        if self.is_leader() {
            self.push_op(DlqOp::Remove(id));
        }
    }

    fn push_op(&self, op: DlqOp) {
        let mut st = self.state.lock();
        if let Some(l) = st.leader.as_mut() {
            l.state_seq += 1;
            let seq = l.state_seq;
            l.state_log.push_back((seq, op));
            while l.state_log.len() > STATE_LOG_MAX {
                l.state_log.pop_front();
            }
        }
    }

    /// Current state sequence (read under the DLQ lock while taking a DLQ snapshot).
    pub(crate) fn state_seq(&self) -> u64 {
        self.state.lock().leader.as_ref().map_or(0, |l| l.state_seq)
    }
}

/// Handle on one partition's replication state; obtained from
/// [`crate::Broker::replica`]. Cheap to clone. Operations fail with
/// [`CoreError::ChannelNotFound`] once the channel is deleted.
#[derive(Clone)]
pub struct ReplicaHandle {
    ch: Arc<Channel>,
    p: u32,
}

impl ReplicaHandle {
    pub(crate) fn new(ch: Arc<Channel>, p: u32) -> Self {
        Self { ch, p }
    }

    fn r(&self) -> &PartitionReplica {
        &self.ch.repl[self.p as usize]
    }

    fn log(&self) -> &crate::storage::PartitionLog {
        &self.ch.logs[self.p as usize]
    }

    fn alive(&self) -> Result<()> {
        if self.ch.deleted.load(Ordering::Acquire) {
            Err(CoreError::ChannelNotFound(self.ch.name.clone()))
        } else {
            Ok(())
        }
    }

    pub fn channel(&self) -> &str {
        &self.ch.name
    }

    pub fn partition(&self) -> u32 {
        self.p
    }

    /// True once the channel was deleted (the handle is dead; get a new one).
    pub fn is_deleted(&self) -> bool {
        self.ch.deleted.load(Ordering::Acquire)
    }

    pub fn epoch(&self) -> u64 {
        self.r().epoch()
    }

    pub fn log_end_offset(&self) -> u64 {
        self.log().high()
    }

    pub fn log_start_offset(&self) -> u64 {
        self.log().low()
    }

    pub fn high_watermark(&self) -> u64 {
        self.r().hw.load(Ordering::Acquire)
    }

    /// Write format of new appends on this partition: batch envelopes (`true`, the
    /// single-node default) or legacy records (`false`, set for every partition by
    /// `Broker::enable_replication` until the cluster knows every replica reads envelopes;
    /// v0.6 followers can't parse them during a rolling upgrade). `Broker::publish_batch` on
    /// a partition with batch writes off decodes the section and writes legacy records (same
    /// offsets and ack semantics). Followers append whatever the leader sent, verbatim.
    pub fn set_batch_writes(&self, enabled: bool) {
        self.log().set_batch_writes(enabled);
    }

    /// See [`set_batch_writes`](Self::set_batch_writes).
    pub fn batch_writes(&self) -> bool {
        self.log().batch_writes()
    }

    /// Fault injection for tests: the next fsync of this partition's data fails with `EIO`
    /// (see `PartitionLog::fail_next_fsync_for_tests`).
    #[doc(hidden)]
    pub fn fail_next_fsync_for_tests(&self) {
        self.log().fail_next_fsync_for_tests();
    }

    /// Like [`fail_next_fsync_for_tests`](Self::fail_next_fsync_for_tests), but the failing
    /// fsync sleeps `delay` first.
    #[doc(hidden)]
    pub fn fail_next_fsync_for_tests_after(&self, delay: std::time::Duration) {
        self.log().fail_next_fsync_for_tests_after(delay);
    }

    fn check_epoch(&self, epoch: u64, to_offline: bool, same_kind: bool) -> Result<()> {
        let current = self.r().epoch();
        let mode = self.r().mode.load(Ordering::Acquire);
        if epoch < current || (epoch == current && mode != OFFLINE && !to_offline && !same_kind) {
            return Err(CoreError::StaleEpoch {
                got: epoch,
                current,
            });
        }
        Ok(())
    }

    /// Makes this node the partition leader for `params.epoch` (idempotent for the same epoch;
    /// an equal epoch may also update replicas / min_isr). Rejects older epochs, and a
    /// follower → leader change without an epoch bump. Group dispatch for the partition
    /// restarts from the replicated committed offsets; publishes are accepted from now on.
    pub fn become_leader(&self, params: LeaderParams) -> Result<()> {
        self.alive()?;
        let r = self.r();
        let was_leader = r.is_leader();
        let same = was_leader && r.epoch() == params.epoch;
        self.check_epoch(params.epoch, false, was_leader)?;
        r.set_params(&params);
        let mut fx = Effects::default();
        {
            let mut st = r.state.lock();
            let leo = self.log().high();
            let followers: Vec<String> = params
                .replicas
                .iter()
                .filter(|id| **id != r.self_id)
                .cloned()
                .collect();
            if same {
                let l = st
                    .leader
                    .as_mut()
                    .expect("leader state exists while leading");
                l.min_isr = params.min_isr.max(1);
                l.lag_max = params.replica_lag_max;
                l.replicas = params.replicas.clone();
                let now = Instant::now();
                l.followers.retain(|id, _| followers.contains(id));
                for id in &followers {
                    l.followers.entry(id.clone()).or_insert(Progress::new(now));
                }
                let before = l.isr.clone();
                l.isr
                    .retain(|id| *id == r.self_id || followers.contains(id));
                l.committed_isr
                    .retain(|id| *id == r.self_id || followers.contains(id));
                if l.isr != before {
                    fx.isr = Some(l.isr.iter().cloned().collect());
                }
                let l = st.leader.as_ref().expect("leader state");
                r.set_isr_len(l);
                let more = r.recompute_locked(&mut st, leo);
                fx.hw_changed |= more.hw_changed;
                fx.notify = true;
            } else {
                let now = Instant::now();
                // Optimistic ISR (all replicas) — a superset is always safe for the HW, and
                // `min_isr` is not tripped by a fresh election. Until the controller confirms
                // the ISR, every replica counts as committed (it may be electable).
                let all: BTreeSet<String> = params
                    .replicas
                    .iter()
                    .cloned()
                    .chain(std::iter::once(r.self_id.clone()))
                    .collect();
                let l = LeaderState {
                    replicas: params.replicas.clone(),
                    min_isr: params.min_isr.max(1),
                    lag_max: params.replica_lag_max,
                    isr: all.clone(),
                    committed_isr: all,
                    isr_resend_at: None,
                    followers: followers
                        .iter()
                        .map(|id| (id.clone(), Progress::new(now)))
                        .collect(),
                    state_log: VecDeque::new(),
                    state_seq: 0,
                    offsets_cache: Vec::new(),
                    offsets_gen: 0,
                };
                r.set_isr_len(&l);
                st.leader = Some(l);
                st.leader_id = Some(r.self_id.clone());
                // Start from the HW this replica knew as follower (never above its LEO).
                let hw = r.hw.load(Ordering::Acquire).min(leo);
                r.hw.store(hw, Ordering::Release);
                r.ack_hw.store(hw, Ordering::Release);
                r.epoch.store(params.epoch, Ordering::Release);
                r.epoch_start_pending.store(true, Ordering::Release);
                r.mode.store(LEADER, Ordering::Release);
                let more = r.recompute_locked(&mut st, leo);
                fx.hw_changed = true;
                let _ = more;
                fx.notify = true;
                let file = r.file_locked(&st);
                drop(st);
                r.persist(&self.ch, file);
                info!(channel = %self.ch.name, partition = self.p, epoch = params.epoch, leo, hw, "became leader");
            }
        }
        if !same {
            // Resume dispatch from the replicated committed offsets — unless this node already
            // led the partition (an epoch bump without a leader change, e.g. a replica-set
            // change during a move): its log and group state are continuous, so in-flight
            // deliveries stay valid and nothing needs redelivering.
            if !was_leader {
                self.ch.reset_partition_groups(self.p as usize);
            }
            r.data.notify_waiters();
        }
        r.apply(&self.ch, fx);
        Ok(())
    }

    /// Makes this node a follower of `leader` for `epoch` (idempotent). Publishes are
    /// rejected and no consumer is served from now on; in-flight deliveries of the partition
    /// are forgotten (they are redelivered by the leader). The caller must then truncate
    /// (see [`ReplicaHandle::truncation_offset`]) and fetch.
    pub fn become_follower(&self, epoch: u64, leader: &str) -> Result<()> {
        self.alive()?;
        let r = self.r();
        let mode = r.mode.load(Ordering::Acquire);
        let same = mode == FOLLOWER && r.epoch() == epoch;
        self.check_epoch(epoch, false, mode == FOLLOWER)?;
        let file = {
            let mut st = r.state.lock();
            st.leader_id = Some(leader.to_string());
            if same {
                None
            } else {
                st.leader = None;
                st.cursor = StateCursor::default();
                r.epoch.store(epoch, Ordering::Release);
                r.epoch_start_pending.store(false, Ordering::Release);
                r.mode.store(FOLLOWER, Ordering::Release);
                r.isr_len.store(0, Ordering::Release);
                Some(r.file_locked(&st))
            }
        };
        if let Some(file) = file {
            r.persist(&self.ch, file);
            if mode == LEADER {
                self.ch.reset_partition_groups(self.p as usize);
            }
            info!(channel = %self.ch.name, partition = self.p, epoch, leader, "became follower");
            r.wake();
            self.ch.notify_groups();
        }
        Ok(())
    }

    /// No replica here / partition offline: reject publishes and consumers.
    pub fn become_offline(&self, epoch: u64) -> Result<()> {
        self.alive()?;
        let r = self.r();
        let mode = r.mode.load(Ordering::Acquire);
        self.check_epoch(epoch, true, false)?;
        if mode == OFFLINE && r.epoch() == epoch {
            return Ok(());
        }
        let file = {
            let mut st = r.state.lock();
            st.leader = None;
            st.leader_id = None;
            r.epoch.store(epoch, Ordering::Release);
            r.mode.store(OFFLINE, Ordering::Release);
            r.isr_len.store(0, Ordering::Release);
            r.file_locked(&st)
        };
        r.persist(&self.ch, file);
        if mode == LEADER {
            self.ch.reset_partition_groups(self.p as usize);
        }
        r.wake();
        self.ch.notify_groups();
        Ok(())
    }

    /// Leader: the controller durably committed `isr` for `epoch`. Members removed from the
    /// committed ISR stop counting for the HW (see module docs). Stale epochs are ignored.
    pub fn isr_committed(&self, epoch: u64, isr: &[String]) {
        let r = self.r();
        let fx = {
            let mut st = r.state.lock();
            if !r.is_leader() || r.epoch() != epoch {
                return;
            }
            let leo = self.log().high();
            let self_id = r.self_id.clone();
            let Some(l) = st.leader.as_mut() else { return };
            let committed: BTreeSet<String> = isr.iter().cloned().collect();
            l.committed_isr = committed.clone();
            l.committed_isr.insert(self_id.clone());
            // Members the controller dropped (e.g. a dead former leader) leave the ISR too.
            let before = l.isr.clone();
            l.isr.retain(|id| *id == self_id || committed.contains(id));
            let mut report = l.isr != before;
            let holdoff = Instant::now() + ISR_REJOIN_HOLDOFF;
            for id in before.difference(&l.isr) {
                if let Some(p) = l.followers.get_mut(id) {
                    p.rejoin_after = Some(holdoff);
                }
            }
            let current: Vec<String> = l.isr.iter().cloned().collect();
            if current.iter().ne(l.committed_isr.iter()) {
                // Converge: tell the controller the ISR as we see it now.
                report = true;
            }
            r.set_isr_len(st.leader.as_ref().expect("leader state"));
            let mut fx = r.recompute_locked(&mut st, leo);
            fx.notify = true;
            if report {
                fx.isr = Some(current);
            }
            fx
        };
        r.apply(&self.ch, fx);
    }

    /// Leader: periodic ISR maintenance (evicts followers not caught up within
    /// `replica_lag_max`, drops stale candidates).
    pub fn tick(&self, now: Instant) {
        let r = self.r();
        if !r.is_leader() {
            return;
        }
        let fx = {
            let mut st = r.state.lock();
            let leo = self.log().high();
            let self_id = r.self_id.clone();
            let Some(l) = st.leader.as_mut() else { return };
            let mut changed = false;
            let lag_max = l.lag_max;
            for (id, p) in &mut l.followers {
                if p.candidate
                    && (p.last_fetch.is_none_or(|t| now.duration_since(t) > lag_max)
                        || now.duration_since(p.durable_caught_up) > lag_max)
                {
                    p.candidate = false;
                }
                if *id != self_id
                    && l.isr.contains(id)
                    && now.duration_since(p.last_caught_up.min(p.durable_caught_up)) > lag_max
                {
                    l.isr.remove(id);
                    changed = true;
                    info!(channel = %r.channel, partition = r.partition, follower = %id, "follower fell out of the ISR");
                }
            }
            // Converge with the controller: re-report an ISR that it has not committed.
            let mut resend = false;
            if changed || l.isr == l.committed_isr {
                l.isr_resend_at = (l.isr != l.committed_isr).then(|| now + ISR_RESEND);
            } else {
                match l.isr_resend_at {
                    Some(at) if now >= at => {
                        resend = true;
                        l.isr_resend_at = Some(now + ISR_RESEND);
                    }
                    Some(_) => {}
                    None => l.isr_resend_at = Some(now + ISR_RESEND),
                }
            }
            let isr: Vec<String> = l.isr.iter().cloned().collect();
            r.set_isr_len(st.leader.as_ref().expect("leader state"));
            let mut fx = r.recompute_locked(&mut st, leo);
            if changed {
                fx.notify = true;
            }
            if changed || resend {
                if resend {
                    debug!(channel = %r.channel, partition = r.partition, isr = ?isr, "re-reporting an ISR the controller has not committed");
                }
                fx.isr = Some(isr);
            }
            fx
        };
        r.apply(&self.ch, fx);
    }

    fn validate_leader(&self, epoch: u64) -> Result<(), FetchError> {
        if self.is_deleted() {
            return Err(FetchError::UnknownPartition);
        }
        let r = self.r();
        let current = r.epoch();
        if !r.is_leader() {
            return Err(FetchError::NotLeader {
                epoch: current,
                leader: r.state.lock().leader_id.clone(),
            });
        }
        if epoch < current {
            return Err(FetchError::FencedEpoch { current });
        }
        if epoch > current {
            return Err(FetchError::UnknownEpoch { current });
        }
        Ok(())
    }

    /// Leader: whether `replica` is currently in the ISR (`false` when not leading).
    pub fn in_isr(&self, replica: &str) -> bool {
        let r = self.r();
        if !r.is_leader() {
            return false;
        }
        r.state
            .lock()
            .leader
            .as_ref()
            .is_some_and(|l| l.isr.contains(replica))
    }

    /// Whether the channel's local storage is released (its directory removed because no
    /// partition holds anything on this node; see [`remove_local_data`](Self::remove_local_data)).
    pub fn storage_released(&self) -> bool {
        self.ch.files.is_released()
    }

    /// Committed offset of every group on this partition (sorted by group; groups without a
    /// position here are omitted). On a follower: the offsets replicated from the leader.
    pub fn group_offsets(&self) -> Vec<(String, u64)> {
        self.ch.partition_group_offsets(self.p as usize)
    }

    /// Removed replica: deletes everything this node holds for the partition, fenced at
    /// `epoch` (the assignment epoch that no longer lists this node).
    ///
    /// 1. The partition goes `Offline` at `epoch` (rejected with [`CoreError::StaleEpoch`]
    ///    when `epoch` is older than the current one — the node may have been re-added).
    /// 2. The channel writer (after a barrier) deletes every record: the log restarts empty
    ///    at offset 0, as on a node that never held the partition; the epoch history and HW
    ///    are cleared (the epoch itself is kept as a fence).
    /// 3. Group committed positions on the partition and its DLQ entries are dropped (groups
    ///    left without any position and without subscribers disappear).
    /// 4. If no partition of the channel holds anything on this node any more, the channel
    ///    directory is removed (storage *released*); the channel stays registered and its
    ///    directory is re-created automatically before the next write (a later role).
    ///
    /// Refused with [`CoreError::InvalidArgument`] when the node leads / follows the partition
    /// at `epoch` itself. Every step re-checks that the partition is still `Offline` at `epoch`; a role set
    /// meanwhile (re-added replica) wins and the rest is skipped. Returns whether the
    /// channel's storage is released. Idempotent.
    pub async fn remove_local_data(&self, epoch: u64) -> Result<bool> {
        self.alive()?;
        let r = self.r();
        if epoch == r.epoch() && !r.is_offline() {
            // An assignment that lists this node as leader/follower at `epoch` cannot also
            // remove it (replica-set changes bump the epoch).
            return Err(CoreError::InvalidArgument(format!(
                "{}/{}: still a replica at epoch {epoch}; refusing to delete its data",
                self.ch.name, self.p
            )));
        }
        self.become_offline(epoch)?;
        let (done, rx) = oneshot::channel();
        self.ch
            .send_cmd(WriteCmd::Purge {
                partition: self.p,
                epoch,
                done,
            })
            .await?;
        rx.await.map_err(|_| self.ch.closed_error())??;
        let file = {
            let mut st = r.state.lock();
            if r.check_offline(epoch).is_err() {
                None
            } else {
                st.epochs.clear();
                st.cursor = StateCursor::default();
                r.hw.store(0, Ordering::Release);
                r.ack_hw.store(0, Ordering::Release);
                Some(r.file_locked(&st))
            }
        };
        let Some(file) = file else {
            return Err(CoreError::StaleEpoch {
                got: epoch,
                current: r.epoch(),
            });
        };
        r.persist(&self.ch, file);
        self.ch.apply_replicated_offsets(self.p as usize, &[]);
        self.ch.dlq.replace_partition(self.p, Vec::new());
        let (done, rx) = oneshot::channel();
        self.ch.send_cmd(WriteCmd::Release { done }).await?;
        let released = rx.await.map_err(|_| self.ch.closed_error())??;
        info!(channel = %self.ch.name, partition = self.p, epoch, released, "removed replica data");
        Ok(released)
    }

    /// Leader: records a follower's fetch position and durable LEO, updates its ISR
    /// membership and the HW. Returns the leader's written end (the LEO followers can
    /// fetch up to, see [`read_for_fetch`](Self::read_for_fetch)).
    ///
    /// The fetch position (`fetch_offset`) measures how far behind the follower is (ISR
    /// lag); only the durable end (`durable_offset`) bounds the HW. Reports from one follower
    /// must be applied in the order it sent them (a durable end may go down after a
    /// truncation, and a later report always reflects the follower's current state).
    pub fn record_fetch(&self, req: &FetchRequest) -> Result<u64, FetchError> {
        self.validate_leader(req.epoch)?;
        let r = self.r();
        let log = self.log();
        let fx = {
            let mut st = r.state.lock();
            // Read the LEO under the lock: it orders against `on_leo` recomputations. `leo`
            // (durable) bounds the HW; followers may be ahead of it, up to the written end.
            let leo = log.high();
            let written = log.written_high().max(leo);
            let low = log.low();
            let hw = r.hw.load(Ordering::Acquire);
            let Some(l) = st.leader.as_mut() else {
                return Err(FetchError::NotLeader {
                    epoch: r.epoch(),
                    leader: None,
                });
            };
            if req.fetch_offset > written {
                return Err(FetchError::OffsetOutOfRange {
                    log_start: low,
                    log_end: written,
                });
            }
            let Some(p) = l.followers.get_mut(&req.replica) else {
                return Err(FetchError::NotReplica);
            };
            let now = Instant::now();
            if req.fetch_offset >= written {
                p.last_caught_up = now;
            } else if let Some(prev) = p.last_fetch
                && req.fetch_offset >= p.leo_at_last_fetch
            {
                p.last_caught_up = p.last_caught_up.max(prev);
            }
            let caught_up_prev = p.last_fetch.is_some() && req.fetch_offset >= p.leo_at_last_fetch;
            let durable = req.durable_offset.min(req.fetch_offset);
            p.leo = Some(durable);
            p.note_durable(durable, written, now);
            p.last_fetch = Some(now);
            p.leo_at_last_fetch = written;
            let mut isr_change = None;
            // A follower joins the ISR only once it has applied state (group offsets + DLQ
            // snapshot) from this leadership: its cursor names the current epoch. Otherwise a
            // replica whose first fetch is already at the HW (empty or fully retained-away
            // partition, bootstrap at the leader's log start) could be elected before it ever
            // received the group offsets / DLQ.
            let has_state = req.state.epoch == Some(r.epoch());
            let held_off = p.rejoin_after.is_some_and(|t| now < t);
            if !l.isr.contains(&req.replica) {
                if durable >= hw && has_state && !held_off {
                    p.candidate = false;
                    l.isr.insert(req.replica.clone());
                    isr_change = Some(l.isr.iter().cloned().collect::<Vec<_>>());
                    info!(channel = %r.channel, partition = r.partition, follower = %req.replica, "follower joined the ISR");
                } else {
                    // A candidate holds the HW back so it can reach it under load; one
                    // whose durable end stopped moving (stalled fsyncs, while it keeps
                    // fetching ahead) must not hold it for longer than `lag_max`.
                    p.candidate =
                        caught_up_prev && now.duration_since(p.durable_caught_up) <= l.lag_max;
                }
            }
            r.set_isr_len(st.leader.as_ref().expect("leader state"));
            let mut fx = r.recompute_locked(&mut st, leo);
            if isr_change.is_some() {
                fx.notify = true;
                fx.isr = isr_change;
            }
            (fx, written)
        };
        let (fx, written) = fx;
        r.apply(&self.ch, fx);
        Ok(written)
    }

    /// Leader: a follower's durable end moved (v0.8 followers report it between fetches,
    /// see [`FetchRequest::durable_offset`]). Updates the HW; ISR membership changes only
    /// on fetches. Same ordering rule as [`record_fetch`](Self::record_fetch).
    pub fn record_progress(
        &self,
        replica: &str,
        epoch: u64,
        durable_offset: u64,
    ) -> Result<(), FetchError> {
        self.validate_leader(epoch)?;
        let r = self.r();
        let log = self.log();
        let fx = {
            let mut st = r.state.lock();
            let leo = log.high();
            let written = log.written_high().max(leo);
            let low = log.low();
            let Some(l) = st.leader.as_mut() else {
                return Err(FetchError::NotLeader {
                    epoch: r.epoch(),
                    leader: None,
                });
            };
            if durable_offset > written {
                return Err(FetchError::OffsetOutOfRange {
                    log_start: low,
                    log_end: written,
                });
            }
            let Some(p) = l.followers.get_mut(replica) else {
                return Err(FetchError::NotReplica);
            };
            p.leo = Some(durable_offset);
            p.note_durable(durable_offset, written, Instant::now());
            r.recompute_locked(&mut st, leo)
        };
        r.apply(&self.ch, fx);
        Ok(())
    }

    /// Leader long-poll helper: returns once the written end exceeds `offset` (durable or
    /// not), the role/epoch changes, or `max_wait` elapses.
    pub async fn wait_for_data(&self, offset: u64, epoch: u64, max_wait: Duration) {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            let notified = self.r().data.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let written = self.log().written_notify().notified();
            tokio::pin!(written);
            written.as_mut().enable();
            if self.log().written_high().max(self.log().high()) > offset
                || !self.r().is_leader()
                || self.r().epoch() != epoch
                || self.is_deleted()
                || self.ch.shared.shutting_down.load(Ordering::Acquire)
            {
                return;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = &mut written => {}
                _ = tokio::time::sleep_until(deadline) => return,
            }
        }
    }

    /// Leader: builds the fetch response — raw records from `fetch_offset` (positional
    /// reads; **blocking**, call from `spawn_blocking`), HW, epoch history and the state
    /// stream since the follower's cursor.
    /// `max_bytes == 0` returns no records (everything else as usual).
    pub fn read_for_fetch(&self, req: &FetchRequest) -> Result<FetchData, FetchError> {
        self.validate_leader(req.epoch)?;
        let log = self.log();
        let low = log.low();
        if req.fetch_offset < low {
            return Err(FetchError::OffsetOutOfRange {
                log_start: low,
                log_end: log.written_high(),
            });
        }
        // v0.7: followers read the written tail, fsynced or not, so the leader's fsync
        // overlaps theirs. Acks are unchanged: the HW only counts the leader's durable LEO
        // (`high`) and every ISR member's durable LEO (their fetch offsets). A failed fsync
        // after such a read poisons the writer (storage), so the lost tail is never
        // rewritten under this epoch; followers truncate it under the next leader.
        // `max_bytes == 0`: metadata and state only (a throttled follower out of budget).
        let records = if req.max_bytes == 0 {
            Ok(None)
        } else {
            log.read_envelopes_unsynced(req.fetch_offset, req.max_bytes)
                .map(|c| {
                    c.map(|c| RawChunk {
                        base_offset: c.first_base,
                        count: c.next_offset - c.first_base,
                        bytes: c.bytes,
                    })
                })
        };
        let records = records.map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidInput {
                FetchError::OffsetOutOfRange {
                    log_start: log.low(),
                    log_end: log.written_high(),
                }
            } else {
                FetchError::Internal(e.to_string())
            }
        })?;
        let state = self.state_since(req.state);
        let r = self.r();
        let epochs = r.state.lock().epochs.clone();
        Ok(FetchData {
            epoch: r.epoch(),
            high_watermark: r.hw.load(Ordering::Acquire),
            log_start_offset: low,
            log_end_offset: log.written_high(),
            records,
            epochs,
            state,
        })
    }

    /// Leader: state batch for a follower at `cursor` (`None` when it is up to date).
    fn state_since(&self, cursor: StateCursor) -> Option<Bytes> {
        let r = self.r();
        // Offsets first (group locks), then the DLQ ops up to "now": every DLQ add that a
        // committed offset already reflects was pushed before the snapshot was taken.
        let offsets = self.ch.partition_group_offsets(self.p as usize);
        let (epoch, need_snapshot, ops, seq, offsets_out) = {
            let mut st = r.state.lock();
            let epoch = r.epoch();
            let l = st.leader.as_mut()?;
            if offsets != l.offsets_cache {
                l.offsets_cache = offsets;
                l.offsets_gen += 1;
            }
            let fresh = cursor.epoch != Some(epoch);
            let offsets_out = (fresh || cursor.offsets_gen != l.offsets_gen)
                .then(|| (l.offsets_gen, l.offsets_cache.clone()));
            let front = l.state_log.front().map_or(l.state_seq + 1, |(s, _)| *s);
            let need_snapshot = fresh || cursor.seq > l.state_seq || cursor.seq + 1 < front;
            let ops: Vec<DlqOp> = if need_snapshot {
                Vec::new()
            } else {
                l.state_log
                    .iter()
                    .filter(|(s, _)| *s > cursor.seq)
                    .map(|(_, op)| op.clone())
                    .collect()
            };
            (epoch, need_snapshot, ops, l.state_seq, offsets_out)
        };
        let batch = if need_snapshot {
            let (entries, seq) = self.ch.dlq.snapshot_partition(self.p, || r.state_seq());
            StateBatch {
                epoch,
                seq,
                snapshot: true,
                dlq: entries
                    .iter()
                    .map(|d| DlqOp::Add(Box::new(StoredDeadLetter::from(d))))
                    .collect(),
                offsets: offsets_out,
            }
        } else {
            if ops.is_empty() && offsets_out.is_none() {
                return None;
            }
            StateBatch {
                epoch,
                seq,
                snapshot: false,
                dlq: ops,
                offsets: offsets_out,
            }
        };
        Some(Bytes::from(
            postcard::to_stdvec(&batch).expect("postcard encoding into Vec is infallible"),
        ))
    }

    /// Leader: `(largest epoch <= requested, its end offset)` from this leader's history (Kafka
    /// OffsetForLeaderEpoch). The end offset of the current epoch is the LEO; `None` epoch
    /// when the history has nothing at or below `requested`.
    pub fn epoch_end_offset(
        &self,
        leader_epoch: u64,
        requested: u64,
    ) -> Result<(Option<u64>, u64), FetchError> {
        self.validate_leader(leader_epoch)?;
        // LEO before the history: a lazily recorded epoch start is always visible to us if
        // any record of that epoch is included in the LEO we read (it is recorded before the
        // first append of the epoch). The written end: followers may hold the unsynced tail.
        let leo = self.log().written_high().max(self.log().high());
        let st = self.r().state.lock();
        Ok(end_offset_for(&st.epochs, requested, leo, self.log().low()))
    }

    /// Follower: this replica's latest epoch (`None` = no history).
    pub fn last_epoch(&self) -> Option<u64> {
        self.r().state.lock().epochs.last().map(|&(e, _)| e)
    }

    /// Follower: where to truncate given the leader's answer `(epoch, end)` to
    /// [`epoch_end_offset`](Self::epoch_end_offset) for our [`last_epoch`](Self::last_epoch).
    ///
    /// The result is an upper bound for [`truncate`](Self::truncate), which clamps it to the
    /// LEO *after* draining the writer (it is deliberately not clamped to the LEO here: that
    /// value may still grow by appends admitted under the previous leadership).
    pub fn truncation_offset(&self, leader_epoch: Option<u64>, leader_end: u64) -> u64 {
        let st = self.r().state.lock();
        let local_end = match leader_epoch {
            // Our end offset for the epoch the leader matched.
            Some(e) => end_offset_for(&st.epochs, e, u64::MAX, self.log().low()).1,
            None => leader_end,
        };
        leader_end.min(local_end)
    }

    /// Follower: drains the writer (every append queued under an earlier role commits first),
    /// then truncates the log to `min(offset, LEO)` under `epoch` (fenced in the writer
    /// thread) and drops epoch-history entries at or after the result. Never extends the
    /// log. Returns the resulting LEO.
    pub async fn truncate(&self, epoch: u64, offset: u64) -> Result<u64> {
        self.truncate_cmd(epoch, offset, false).await
    }

    /// Follower: restarts the log empty at `offset` (the leader's retention moved past us).
    pub async fn reset_to(&self, epoch: u64, offset: u64) -> Result<u64> {
        self.truncate_cmd(epoch, offset, true).await
    }

    async fn truncate_cmd(&self, epoch: u64, offset: u64, reset: bool) -> Result<u64> {
        self.alive()?;
        self.r().check_follower(epoch)?;
        let (done, rx) = oneshot::channel();
        self.ch
            .send_cmd(WriteCmd::Truncate {
                partition: self.p,
                epoch,
                offset,
                reset,
                done,
            })
            .await?;
        let offset = rx.await.map_err(|_| self.ch.closed_error())??;
        let r = self.r();
        let file = {
            let mut st = r.state.lock();
            let before = st.epochs.len();
            st.epochs.retain(|&(_, s)| s < offset);
            let hw = r.hw.load(Ordering::Acquire).min(offset);
            r.hw.store(hw, Ordering::Release);
            r.ack_hw.store(hw, Ordering::Release);
            (st.epochs.len() != before).then(|| r.file_locked(&st))
        };
        if let Some(f) = file {
            r.persist(&self.ch, f);
        }
        debug!(channel = %self.ch.name, partition = self.p, offset, "truncated replica");
        Ok(offset)
    }

    /// Follower: appends a chunk of raw records fetched from the leader under `epoch`
    /// (verified: CRC + contiguity at our LEO; fenced by epoch in the writer thread), adopts
    /// the leader's epoch history for the prefix we now hold and its HW. Returns the new LEO.
    pub async fn append(
        &self,
        epoch: u64,
        chunk: &RawChunk,
        leader_hw: u64,
        leader_epochs: &[(u64, u64)],
    ) -> Result<u64> {
        self.alive()?;
        self.r().check_follower(epoch)?;
        let leo = if chunk.bytes.is_empty() {
            self.log().high()
        } else {
            let (done, rx) = oneshot::channel();
            self.ch
                .send_cmd(WriteCmd::AppendRaw {
                    partition: self.p,
                    epoch,
                    base_offset: chunk.base_offset,
                    bytes: chunk.bytes.clone(),
                    done,
                })
                .await?;
            rx.await.map_err(|_| self.ch.closed_error())??
        };
        self.update_follower_meta(leo, leader_hw, leader_epochs);
        Ok(leo)
    }

    /// Follower: like [`append`](Self::append), but returns once the writer has the chunk
    /// queued; [`AppendTicket::wait`] completes with the new LEO once the chunk is durable
    /// (the caller then calls [`update_follower_meta`](Self::update_follower_meta)). Chunks
    /// of one partition are appended in the order they were handed over, so a follower can
    /// fetch the next chunk while earlier ones are being fsynced. Empty chunks are refused.
    pub async fn append_async(&self, epoch: u64, chunk: &RawChunk) -> Result<AppendTicket> {
        self.alive()?;
        self.r().check_follower(epoch)?;
        if chunk.bytes.is_empty() {
            return Err(CoreError::InvalidArgument("empty replicated chunk".into()));
        }
        let (done, rx) = oneshot::channel();
        self.ch
            .send_cmd(WriteCmd::AppendRaw {
                partition: self.p,
                epoch,
                base_offset: chunk.base_offset,
                bytes: chunk.bytes.clone(),
                done,
            })
            .await?;
        Ok(AppendTicket {
            rx,
            ch: self.ch.clone(),
        })
    }

    /// Follower: adopts the leader HW (capped by our LEO) and epoch history for `[.., leo)`.
    pub fn update_follower_meta(&self, leo: u64, leader_hw: u64, leader_epochs: &[(u64, u64)]) {
        let r = self.r();
        let mut hw_moved = false;
        let file = {
            let mut st = r.state.lock();
            if r.mode.load(Ordering::Acquire) != FOLLOWER {
                return;
            }
            let hw = leader_hw
                .min(leo)
                .max(r.hw.load(Ordering::Acquire).min(leo));
            if r.hw.swap(hw, Ordering::AcqRel) != hw {
                hw_moved = true;
            }
            r.ack_hw.store(hw, Ordering::Release);
            let learned: Vec<(u64, u64)> = leader_epochs
                .iter()
                .copied()
                .filter(|&(_, s)| s < leo)
                .collect();
            (learned != st.epochs).then(|| {
                st.epochs = learned;
                r.file_locked(&st)
            })
        };
        if let Some(f) = file {
            r.persist(&self.ch, f);
        }
        if hw_moved {
            // Followers serve live subscriptions up to their HW (v0.10).
            self.ch.notify_live();
        }
    }

    /// Follower: state stream position to send with the next fetch.
    pub fn state_cursor(&self) -> StateCursor {
        self.r().state.lock().cursor
    }

    /// Follower: applies a state batch from the leader (group committed offsets, DLQ
    /// mutations or snapshot) fetched under `epoch`.
    pub fn apply_state(&self, epoch: u64, bytes: &[u8]) -> Result<()> {
        self.alive()?;
        let r = self.r();
        r.check_follower(epoch)?;
        let batch: StateBatch = postcard::from_bytes(bytes)
            .map_err(|e| CoreError::Corrupt(format!("replicated state batch: {e}")))?;
        let p = self.p;
        let dls: Vec<(bool, Option<fluxmq_proto::model::DeadLetter>, u64)> = batch
            .dlq
            .into_iter()
            .map(|op| match op {
                DlqOp::Add(d) => {
                    let dl = fluxmq_proto::model::DeadLetter::from(*d);
                    let id = dl.id;
                    (true, Some(dl), id)
                }
                DlqOp::Remove(id) => (false, None, id),
            })
            .collect();
        for (_, dl, _) in &dls {
            if let Some(dl) = dl {
                self.ch
                    .shared
                    .bump_dlq_seq((dl.id >> DLQ_PARTITION_BITS) + 1);
            }
        }
        if batch.snapshot {
            let entries = dls.into_iter().filter_map(|(_, d, _)| d).collect();
            self.ch.dlq.replace_partition(p, entries);
        } else {
            for (add, dl, id) in dls {
                match (add, dl) {
                    (true, Some(dl)) => self.ch.dlq.apply_add(dl),
                    _ => {
                        self.ch.dlq.remove(id);
                    }
                }
            }
        }
        if let Some((_, offsets)) = &batch.offsets {
            self.ch.apply_replicated_offsets(p as usize, offsets);
        }
        let mut st = r.state.lock();
        st.cursor.epoch = Some(batch.epoch);
        st.cursor.seq = batch.seq;
        if let Some((gen_, _)) = batch.offsets {
            st.cursor.offsets_gen = gen_;
        }
        Ok(())
    }

    pub fn status(&self) -> ReplicaStatus {
        let r = self.r();
        let log = self.log();
        let st = r.state.lock();
        let mode = r.mode.load(Ordering::Acquire);
        let role = if !self.ch.replicated.load(Ordering::Acquire) {
            ReplicaRole::Standalone
        } else {
            match mode {
                LEADER => ReplicaRole::Leader,
                FOLLOWER => ReplicaRole::Follower {
                    leader: st.leader_id.clone().unwrap_or_default(),
                },
                _ => ReplicaRole::Offline,
            }
        };
        let (isr, committed_isr, follower_leo) = match &st.leader {
            Some(l) => (
                l.isr.iter().cloned().collect(),
                l.committed_isr.iter().cloned().collect(),
                l.followers
                    .iter()
                    .map(|(id, p)| (id.clone(), p.leo))
                    .collect(),
            ),
            None => Default::default(),
        };
        let high = log.high();
        ReplicaStatus {
            role,
            epoch: r.epoch(),
            log_start_offset: log.low(),
            log_end_offset: high,
            high_watermark: if self.ch.replicated.load(Ordering::Acquire) {
                r.hw.load(Ordering::Acquire)
            } else {
                high
            },
            isr,
            committed_isr,
            follower_leo,
        }
    }
}

/// A replicated chunk queued by [`ReplicaHandle::append_async`].
pub struct AppendTicket {
    rx: oneshot::Receiver<Result<u64>>,
    ch: Arc<Channel>,
}

impl AppendTicket {
    /// The partition's LEO once the chunk is durable (or the append's error).
    pub async fn wait(self) -> Result<u64> {
        let AppendTicket { rx, ch } = self;
        rx.await.map_err(|_| ch.closed_error())?
    }

    /// Non-blocking [`wait`](Self::wait): `None` while the chunk is not durable yet (or its
    /// result was already taken by polling the ticket as a future).
    pub fn try_wait(&mut self) -> Option<Result<u64>> {
        match self.rx.try_recv() {
            Ok(r) => Some(r),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(self.ch.closed_error())),
        }
    }
}

/// Polling the ticket is [`AppendTicket::wait`] without consuming it (for `select!`). Don't
/// poll it again after it completed.
impl std::future::Future for AppendTicket {
    type Output = Result<u64>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = &mut *self;
        std::pin::Pin::new(&mut this.rx)
            .poll(cx)
            .map(|r| r.unwrap_or_else(|_| Err(this.ch.closed_error())))
    }
}

impl Progress {
    /// A report of the follower's durable end (`written`: the leader's written end now).
    fn note_durable(&mut self, durable: u64, written: u64, now: Instant) {
        if durable >= written {
            self.durable_caught_up = now;
            self.durable_mark = None;
            return;
        }
        match self.durable_mark {
            Some((at, mark)) if durable >= mark => {
                self.durable_caught_up = self.durable_caught_up.max(at);
                self.durable_mark = Some((now, written));
            }
            Some(_) => {}
            None => self.durable_mark = Some((now, written)),
        }
    }

    fn new(now: Instant) -> Self {
        Self {
            leo: None,
            last_caught_up: now,
            durable_caught_up: now,
            durable_mark: None,
            last_fetch: None,
            leo_at_last_fetch: 0,
            candidate: false,
            rejoin_after: None,
        }
    }
}

/// `(largest epoch <= requested, end offset)`; the end offset of the last epoch is `leo`.
fn end_offset_for(epochs: &[(u64, u64)], requested: u64, leo: u64, low: u64) -> (Option<u64>, u64) {
    let idx = epochs.partition_point(|&(e, _)| e <= requested);
    if idx == 0 {
        return (None, epochs.first().map_or(low, |&(_, s)| s).min(leo));
    }
    let (e, _) = epochs[idx - 1];
    let end = epochs.get(idx).map_or(leo, |&(_, s)| s).min(leo);
    (Some(e), end)
}

/// Loads `p{n}/replica.json` if present.
pub(crate) fn load_replica_file(dir: &std::path::Path) -> std::io::Result<Option<ReplicaFile>> {
    match std::fs::read(dir.join(REPLICA_FILE)) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::end_offset_for;

    #[test]
    fn epoch_end_offsets() {
        let h = [(1, 0), (3, 50), (4, 80)];
        assert_eq!(end_offset_for(&h, 1, 100, 0), (Some(1), 50));
        assert_eq!(end_offset_for(&h, 2, 100, 0), (Some(1), 50));
        assert_eq!(end_offset_for(&h, 3, 100, 0), (Some(3), 80));
        assert_eq!(end_offset_for(&h, 4, 100, 0), (Some(4), 100));
        assert_eq!(end_offset_for(&h, 9, 100, 0), (Some(4), 100));
        assert_eq!(end_offset_for(&h, 0, 100, 0), (None, 0));
        assert_eq!(end_offset_for(&[], 5, 100, 7), (None, 7));
    }
}

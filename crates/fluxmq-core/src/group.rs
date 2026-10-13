//! Consumer groups: assignment, dispatch, acks, retries, timeouts and dead-lettering.
//!
//! All state of a group lives behind one mutex ([`GroupState`]); every operation is a short,
//! non-blocking critical section (no I/O — disk reads are delegated to `spawn_blocking`
//! fetches that install read-ahead buffers). Waiting subscriptions park on the group's
//! [`Notify`] and on their own per-subscription one. Anything that may make a delivery
//! possible for any subscription calls `notify_waiters` on the group's; an ack, which only
//! frees the acking subscription's prefetch credit (unless it unblocks a deferred key), wakes
//! just that subscription.
//!
//! Per partition the group tracks:
//! * `cursor` — next offset never dispatched before.
//! * `acks` ([`AckState`]) — the committed low watermark plus the finished offsets above it
//!   as ranges. Unfinished ("pending") offsets are `[committed, cursor)` minus those ranges:
//!   in flight, waiting for retry, ready for redelivery, or deferred behind a key. Nothing is
//!   stored per pending message unless it needs to be (retry, ready, deferred).
//! * `preacked` — finished ranges at/after `cursor` restored from a checkpoint (acked out of
//!   order before a restart); dispatch skips them, so a restart redelivers exactly the
//!   unfinished messages.
//! * `retry` — failed messages ordered by due time.
//! * `ready` — messages to (re)deliver immediately: returned by a closed subscription, or
//!   unblocked keyed messages.
//! * keyed mode: `active_keys` (a message of that key is unfinished) and `deferred` (later
//!   messages of an active key, in offset order).
//!
//! In-flight deliveries are kept per **run**: the deliveries handed out by one poll (one
//! `DeliverBatch` / `DeliverRecords` frame) share consecutive delivery ids, one subscription,
//! one delivery instant and one ack deadline, so a run is one map entry and one timer
//! (bucketed deadlines) however many messages it holds. A first attempt carries no attempt
//! record of its own (it is derived from the run); only messages with more than one attempt,
//! DLQ retries and orphaned entries carry a boxed [`Prior`]. Raw runs (`DeliverRecords`) hold
//! the stored envelopes instead of decoded messages and decode only what must be redelivered.
//! Acks (`Ack { ids }`, `AckRanges`, `Commit`) are applied as run/range operations under one
//! lock acquisition; attempt-history updates happen after the lock is released.
//!
//! Lag (documented in `stats::GroupStats::lag`) is exact: messages at/after `cursor` plus
//! pending, plus out-of-band redeliveries (DLQ retries / entries orphaned by a reset).
//!
//! Attempts: every delivery is an attempt (1-based). A failed attempt (nack with retry, ack
//! timeout, or the subscription closing with the delivery outstanding) is retried after
//! `backoff_ms[min(n-1, len-1)]` (immediately for a closed subscription) until
//! `max_attempts`, then dead-lettered.
//!
//! Cumulative subscriptions (`AckMode::Cumulative`, stream channels only) have no ack
//! deadline and settle with `Commit { partition, offset }`; when they end, their unsettled
//! deliveries are redelivered from the committed position like any other.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fluxmq_proto::batch::EnvelopeHeader;
use fluxmq_proto::model::{
    AttemptOutcome, ChannelMode, DeadLetter, DeliveryAttempt, Message, MessageId, RetryPolicy,
    StartFrom,
};
use fluxmq_proto::wire::{ClientStats, Delivery, DeliveryRange, MAX_DELIVER_BATCH};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::warn;

use crate::ackset::{AckState, RangeSet};
use crate::broker::SubscriberInfo;
use crate::channel::Channel;
use crate::error::{CoreError, Result};
use crate::history::{Failed, Spans};
use crate::metrics::{GroupCounters, record, record_n};
use crate::stats::{GroupStats, InstanceStats, LatencyHistogram, new_latency_histogram};
use crate::storage::EnvelopeChunk;
use crate::util::{now_ms, now_us};

/// Read-ahead fetch size per (group, partition): one sequential read of whole stored
/// entries. A new fetch is issued when less than half of the previous one is left, so a
/// lagging group reads ahead while it dispatches (double buffering).
const FETCH_RECORDS: usize = 16 * 1024;
const FETCH_BYTES: usize = 4 * 1024 * 1024;
/// Raw read-ahead depth per (group, partition): chunks of up to `FETCH_BYTES` held ahead of
/// the cursor (one more may be in flight).
const RAW_AHEAD_CHUNKS: usize = 3;
/// A mostly settled raw run is compacted only this long after its delivery (see
/// `GroupState::compact_runs`).
const COMPACT_AFTER: Duration = Duration::from_millis(250);
/// Keyed mode: at most this many messages per partition may wait behind blocked keys; beyond
/// it the partition stops scanning ahead (bounded memory under a stuck hot key).
pub const MAX_DEFERRED: usize = 10_000;
const FETCH_ERROR_BACKOFF: Duration = Duration::from_millis(200);
/// Ack deadlines are rounded up to this granularity, so the runs of one burst share a
/// timer bucket.
const DEADLINE_BUCKET: Duration = Duration::from_millis(10);
/// At most this many `DeliverRecords` ranges per poll (each becomes one frame).
pub const MAX_RAW_FRAMES_PER_POLL: usize = 8;
/// Cached entries smaller than this are coalesced into one `DeliverRecords` frame (one copy);
/// larger ones go out as they are (zero-copy), one frame each.
const RAW_COALESCE_BELOW: usize = 64 * 1024;
/// At most this many finished ranges above the watermark are checkpointed per partition
/// (the lowest ones). Ranges beyond it are only redelivered after a restart.
pub const MAX_CHECKPOINT_RANGES: usize = 1024;
pub const DISCONNECT_REASON: &str = "consumer disconnected";
pub const TIMEOUT_REASON: &str = "ack timeout";
/// Cursor of a partition whose position is unknown (replicated mode: the group was created
/// by replicating another partition's offsets). Never dispatched; persisted as `u64::MAX`;
/// initialized by the next subscriber's start position or by replicated offsets.
pub const UNINIT: u64 = u64::MAX;

/// A delivery handed out by [`Group::poll_batch`]: like the wire [`Delivery`] but sharing
/// the message with the group's in-flight record instead of deep-copying it.
#[derive(Debug, Clone, PartialEq)]
pub struct SharedDelivery {
    pub delivery_id: u64,
    /// 1-based.
    pub attempt: u32,
    pub message: Arc<Message>,
}

impl SharedDelivery {
    /// The wire form (copies the message only if it is still shared).
    pub fn into_wire(self) -> Delivery {
        Delivery {
            delivery_id: self.delivery_id,
            attempt: self.attempt,
            message: Arc::unwrap_or_clone(self.message),
        }
    }
}

impl From<SharedDelivery> for Delivery {
    fn from(d: SharedDelivery) -> Self {
        d.into_wire()
    }
}

/// Stored envelopes delivered verbatim (`ServerFrame::DeliverRecords`): the records with
/// offsets `from_offset .. from_offset + count` of `partition`, all first attempts; the
/// delivery id of offset `o` is `first_delivery_id + (o - from_offset)`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawDelivery {
    pub partition: u32,
    pub first_delivery_id: u64,
    pub from_offset: u64,
    pub count: u32,
    /// Whole envelopes covering the range (possibly records outside it at both ends).
    pub envelopes: Bytes,
}

/// How a subscription settles messages and what frames it accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SubMode {
    /// `AckMode::Cumulative`: no ack deadline, settled with `Commit`.
    pub cumulative: bool,
    /// Receives stored envelopes verbatim when possible (`DeliverRecords`).
    pub raw_records: bool,
}

/// One delivery attempt of a pending message. Turned into a [`DeliveryAttempt`] only when
/// the message is dead-lettered.
struct AttemptRec {
    consumer: Arc<str>,
    delivered_at_ms: u64,
    finished_at_ms: Option<u64>,
    outcome: AttemptOutcome,
}

/// Attempts of a pending message; the common single attempt needs no allocation.
#[derive(Default)]
struct Attempts {
    earlier: Vec<AttemptRec>,
    last: Option<AttemptRec>,
}

impl Attempts {
    fn len(&self) -> usize {
        self.earlier.len() + usize::from(self.last.is_some())
    }

    fn is_empty(&self) -> bool {
        self.last.is_none()
    }

    fn push(&mut self, a: AttemptRec) {
        if let Some(prev) = self.last.replace(a) {
            self.earlier.push(prev);
        }
    }

    fn last_mut(&mut self) -> Option<&mut AttemptRec> {
        self.last.as_mut()
    }

    /// Finishes the current attempt (if any).
    fn finish(&mut self, at_ms: u64, outcome: AttemptOutcome) {
        if let Some(a) = self.last_mut() {
            a.finished_at_ms = Some(at_ms);
            a.outcome = outcome;
        }
    }

    fn into_records(self) -> Vec<DeliveryAttempt> {
        self.earlier
            .into_iter()
            .chain(self.last)
            .enumerate()
            .map(|(i, a)| DeliveryAttempt {
                attempt: i as u32 + 1,
                consumer: a.consumer.to_string(),
                delivered_at_ms: a.delivered_at_ms,
                finished_at_ms: a.finished_at_ms,
                outcome: a.outcome,
            })
            .collect()
    }
}

/// A message waiting for its next delivery attempt.
struct Pending {
    msg: Arc<Message>,
    /// Attempts so far (the next attempt number is `len + 1`).
    attempts: Attempts,
    /// Counted in the partition's pending range (false for DLQ retries and entries orphaned
    /// by an offset reset).
    tracked: bool,
    /// Set when this is a DLQ retry of that entry.
    dlq_id: Option<u64>,
    /// The payload is known to own its buffer (see [`Pending::detach`]).
    detached: bool,
}

impl Pending {
    fn new(msg: Arc<Message>) -> Self {
        Self {
            msg,
            attempts: Attempts::default(),
            tracked: true,
            dlq_id: None,
            detached: false,
        }
    }

    /// Gives the message a payload buffer of its own before it is parked for long (retry
    /// backoff, DLQ). A payload decoded straight from a publish frame is a slice of that
    /// frame; holding one message for minutes must not pin the whole frame. Cold path only.
    fn detach(&mut self) {
        if !self.detached {
            let mut m = (*self.msg).clone();
            m.payload = bytes::Bytes::copy_from_slice(&m.payload);
            self.msg = Arc::new(m);
            self.detached = true;
        }
    }

    fn id(&self) -> MessageId {
        msg_id(&self.msg)
    }

    /// The attempt in flight (the last one), for the attempt history.
    fn failed(&self) -> Failed {
        let last = self.attempts.last.as_ref();
        Failed {
            id: self.id(),
            attempt: self.attempts.len() as u32,
            consumer: last.map_or_else(|| Arc::from(""), |a| a.consumer.clone()),
            delivered_at_ms: last.map_or(0, |a| a.delivered_at_ms),
        }
    }
}

fn msg_id(m: &Message) -> MessageId {
    MessageId {
        partition: m.partition,
        offset: m.offset,
    }
}

/// What an in-flight entry needs beyond its message when it is not a plain first attempt
/// of a tracked partition message (the common case, which stores nothing extra).
struct Prior {
    /// Earlier (finished) attempts; the current one is derived from the run.
    attempts: Attempts,
    tracked: bool,
    dlq_id: Option<u64>,
    detached: bool,
}

struct Entry {
    msg: Arc<Message>,
    prior: Option<Box<Prior>>,
}

impl Entry {
    fn from_pending(p: Pending) -> Self {
        let prior = (!p.attempts.is_empty() || !p.tracked || p.dlq_id.is_some()).then(|| {
            Box::new(Prior {
                attempts: p.attempts,
                tracked: p.tracked,
                dlq_id: p.dlq_id,
                detached: p.detached,
            })
        });
        Self { msg: p.msg, prior }
    }

    fn fresh(msg: Arc<Message>) -> Self {
        Self { msg, prior: None }
    }

    fn tracked(&self) -> bool {
        self.prior.as_ref().is_none_or(|p| p.tracked)
    }

    fn dlq_id(&self) -> Option<u64> {
        self.prior.as_ref().and_then(|p| p.dlq_id)
    }

    /// 1-based number of the attempt in flight.
    fn attempt(&self) -> u32 {
        self.prior.as_ref().map_or(0, |p| p.attempts.len()) as u32 + 1
    }

    /// Back to a [`Pending`] whose last attempt is the one in flight (still `InFlight`).
    fn into_pending(self, consumer: &Arc<str>, delivered_at_ms: u64) -> Pending {
        let (mut attempts, tracked, dlq_id, detached) = match self.prior {
            None => (Attempts::default(), true, None, false),
            Some(p) => (p.attempts, p.tracked, p.dlq_id, p.detached),
        };
        attempts.push(AttemptRec {
            consumer: consumer.clone(),
            delivered_at_ms,
            finished_at_ms: None,
            outcome: AttemptOutcome::InFlight,
        });
        Pending {
            msg: self.msg,
            attempts,
            tracked,
            dlq_id,
            detached,
        }
    }

    /// Marks the entry as no longer counted in its partition's pending range (offset
    /// reset). Returns true if it was.
    fn orphan(&mut self) -> bool {
        match &mut self.prior {
            None => {
                self.prior = Some(Box::new(Prior {
                    attempts: Attempts::default(),
                    tracked: false,
                    dlq_id: None,
                    detached: false,
                }));
                true
            }
            Some(p) => std::mem::replace(&mut p.tracked, false),
        }
    }
}

/// Entries of a run, indexed by `delivery_id - first_id`; `None` = settled.
enum Body {
    Msgs(Vec<Option<Entry>>),
    /// First attempts of `partition`, offsets `from_offset ..` (one per id), delivered as
    /// stored envelopes. `envelopes` covers them all and is decoded only for redelivery.
    Raw {
        partition: u32,
        from_offset: u64,
        envelopes: Bytes,
        /// One bit per entry: still in flight.
        live: Vec<u64>,
    },
}

struct Run {
    sub: u64,
    consumer: Arc<str>,
    delivered_at: Instant,
    delivered_at_ms: u64,
    /// `None` for cumulative subscriptions (no ack timeout).
    deadline: Option<Instant>,
    len: u32,
    live: u32,
    body: Body,
    /// Queued in `GroupState::compact_later`.
    compact_queued: bool,
}

/// Location of one in-flight entry: what settling needs without touching the entry.
struct Slot {
    partition: u32,
    offset: u64,
}

impl Run {
    fn is_live(&self, i: usize) -> bool {
        match &self.body {
            Body::Msgs(v) => v[i].is_some(),
            Body::Raw { live, .. } => live[i / 64] & (1 << (i % 64)) != 0,
        }
    }

    fn slot(&self, i: usize) -> Slot {
        match &self.body {
            Body::Msgs(v) => {
                let m = &v[i].as_ref().expect("live entry").msg;
                Slot {
                    partition: m.partition,
                    offset: m.offset,
                }
            }
            Body::Raw {
                partition,
                from_offset,
                ..
            } => Slot {
                partition: *partition,
                offset: from_offset + i as u64,
            },
        }
    }

    /// Settles entry `i` (must be live) and returns it if the run holds decoded entries.
    /// Raw entries are not decoded here; see [`Run::take_pending`].
    fn settle(&mut self, i: usize) -> Option<Entry> {
        self.live -= 1;
        match &mut self.body {
            Body::Msgs(v) => v[i].take(),
            Body::Raw { live, .. } => {
                live[i / 64] &= !(1 << (i % 64));
                None
            }
        }
    }

    /// Settles entry `i` (must be live) for a redelivery or dead-lettering, decoding raw
    /// entries. `None` only if a raw envelope can't be decoded (corruption).
    fn take_pending(&mut self, i: usize) -> Option<Pending> {
        let slot = self.slot(i);
        let consumer = self.consumer.clone();
        let at = self.delivered_at_ms;
        if let Body::Raw { envelopes, .. } = &self.body {
            let found = crate::storage::decode_chunk(slot.partition, envelopes)
                .filter_map(|r| r.ok())
                .find(|m| m.offset == slot.offset);
            self.settle(i);
            return found.map(|m| Entry::fresh(Arc::new(m)).into_pending(&consumer, at));
        }
        self.settle(i).map(|e| e.into_pending(&consumer, at))
    }

    /// All live entries as pendings (in id order), emptying the run. Raw envelopes are
    /// decoded once.
    fn drain_pending(&mut self) -> Vec<Pending> {
        let consumer = self.consumer.clone();
        let at = self.delivered_at_ms;
        self.live = 0;
        match &mut self.body {
            Body::Msgs(v) => v
                .iter_mut()
                .filter_map(Option::take)
                .map(|e| e.into_pending(&consumer, at))
                .collect(),
            Body::Raw {
                partition,
                from_offset,
                envelopes,
                live,
            } => {
                let (p, from, len) = (*partition, *from_offset, self.len as u64);
                let is_live = |o: u64| {
                    let i = (o - from) as usize;
                    live[i / 64] & (1 << (i % 64)) != 0
                };
                let out = crate::storage::decode_chunk(p, envelopes)
                    .filter_map(|r| r.ok())
                    .filter(|m| m.offset >= from && m.offset < from + len && is_live(m.offset))
                    .map(|m| Entry::fresh(Arc::new(m)).into_pending(&consumer, at))
                    .collect();
                live.fill(0);
                out
            }
        }
    }

    /// Turns a raw body into decoded entries, keeping each entry at its index (cold path:
    /// offset reset). Entries that can't be decoded (corruption) are dropped; the caller
    /// adjusts in-flight accounting by the change of `live`.
    fn is_raw(&self) -> bool {
        matches!(self.body, Body::Raw { .. })
    }

    /// Gives every live decoded entry a payload buffer of its own (see `Pending::detach`).
    fn detach_payloads(&mut self) {
        if let Body::Msgs(v) = &mut self.body {
            for e in v.iter_mut().flatten() {
                let mut m = (*e.msg).clone();
                m.payload = Bytes::copy_from_slice(&m.payload);
                e.msg = Arc::new(m);
            }
        }
    }

    fn materialize(&mut self) {
        let Body::Raw {
            partition,
            from_offset,
            envelopes,
            live,
        } = &self.body
        else {
            return;
        };
        let (from, len) = (*from_offset, self.len as usize);
        let mut v: Vec<Option<Entry>> = (0..len).map(|_| None).collect();
        let mut n = 0u32;
        for m in crate::storage::decode_chunk(*partition, envelopes).filter_map(|r| r.ok()) {
            if m.offset < from || m.offset >= from + len as u64 {
                continue;
            }
            let i = (m.offset - from) as usize;
            if live[i / 64] & (1 << (i % 64)) != 0 {
                v[i] = Some(Entry::fresh(Arc::new(m)));
                n += 1;
            }
        }
        self.live = n;
        self.body = Body::Msgs(v);
    }
}

#[derive(Default)]
struct PartState {
    cursor: u64,
    acks: AckState,
    /// Finished ranges at/after `cursor` restored from a checkpoint.
    preacked: RangeSet,
    retry: BTreeMap<(Instant, u64), Pending>,
    ready: VecDeque<Pending>,
    active_keys: HashSet<String>,
    deferred: HashMap<String, VecDeque<Arc<Message>>>,
    deferred_len: usize,
    /// Decoded read-ahead (offsets ascending).
    readahead: VecDeque<Arc<Message>>,
    /// Raw read-ahead for raw subscriptions: whole envelopes, batches only.
    raw_ahead: VecDeque<EnvelopeChunk>,
    fetching: bool,
    fetch_retry_at: Option<Instant>,
    /// Cursor at which the read-ahead last started above the cursor: refetched once from
    /// there; a second time is an unreadable region (skipped, counted, warned).
    hole_at: Option<u64>,
    generation: u64,
    /// Skip (consume without delivering) records older than this (start/reset by timestamp).
    skip_before_ts: Option<u64>,
}

impl PartState {
    fn new(cursor: u64, skip_before_ts: Option<u64>) -> Self {
        Self {
            cursor,
            acks: AckState::new(cursor),
            skip_before_ts,
            ..Default::default()
        }
    }

    fn committed(&self) -> u64 {
        if self.cursor == UNINIT {
            UNINIT
        } else {
            self.acks.committed().min(self.cursor)
        }
    }

    /// Unfinished offsets below the cursor.
    fn pending(&self) -> u64 {
        if self.cursor == UNINIT {
            0
        } else {
            self.acks.pending(self.cursor)
        }
    }

    /// Moves the cursor forward to `to` without dispatching what lies between (retention
    /// skip, a hole, restored acks): those offsets count as finished.
    fn skip_to(&mut self, to: u64) {
        if to > self.cursor {
            self.acks.finish_range(self.cursor, to);
            self.cursor = to;
            self.preacked.remove_below(to);
        }
    }

    /// End (exclusive) of the read-ahead already held, if any: where a prefetch continues.
    fn ahead_end(&self) -> Option<u64> {
        let a = self.readahead.back().map(|m| m.offset + 1);
        let b = self.raw_ahead.back().map(|c| c.next_offset);
        a.max(b)
    }
}

struct SubState {
    info: SubscriberInfo,
    instance: Arc<str>,
    /// Wakes only this subscription (see the module docs).
    wake: Arc<Notify>,
    prefetch: u32,
    mode: SubMode,
    connected_at_ms: u64,
    in_flight: u64,
    assigned: Vec<u32>,
    rr: usize,
    delivered: u64,
    acked: u64,
    nacked: u64,
    retries: u64,
    processing: LatencyHistogram,
    last_error: Option<String>,
    last_error_at_ms: Option<u64>,
    client_stats: Option<ClientStats>,
}

struct GroupState {
    closed: bool,
    next_delivery_id: u64,
    parts: Vec<PartState>,
    /// In-flight runs by first delivery id.
    runs: BTreeMap<u64, Run>,
    /// (deadline bucket, first id of a run).
    deadlines: BTreeSet<(Instant, u64)>,
    /// Live in-flight entries over all runs.
    in_flight: u64,
    subs: BTreeMap<u64, SubState>,
    oob_ready: VecDeque<Pending>,
    /// Untracked (out-of-band) messages not yet finished.
    oob_live: u64,
    retry_seq: u64,
    processing: LatencyHistogram,
    /// Messages skipped because retention removed them before this group consumed them.
    retention_skipped: u64,
    /// Mostly settled raw runs too young to compact: (delivered at, first id), oldest first.
    compact_later: VecDeque<(Instant, u64)>,
    /// `Priority` channel (v0.12): partitions are levels, taken highest first, not round-robin.
    priority: bool,
}

pub enum Poll {
    Ready(SharedDelivery),
    /// Nothing deliverable now; wait for a notification or until the given instant.
    Wait(Option<Instant>),
    Closed,
}

/// Result of [`Group::poll_batch`].
pub enum BatchPoll {
    /// At least one delivery was appended, `bytes` of estimated wire size in total.
    Ready {
        bytes: usize,
    },
    /// Nothing deliverable now; wait for a notification or until the given instant.
    Wait(Option<Instant>),
    Closed,
}

/// Estimated encoded size of one delivery carrying `m` (payload, key, headers plus a fixed
/// allowance for ids, varints and timestamps). Used to bound delivery batches by bytes; it
/// over- rather than under-estimates.
pub fn delivery_wire_size(m: &Message) -> usize {
    const FIXED: usize = 48;
    let key = m.key.as_ref().map_or(0, |k| k.len() + 5);
    let headers: usize = m.headers.iter().map(|(k, v)| k.len() + v.len() + 10).sum();
    FIXED + m.payload.len() + key + headers
}

/// A message taken for delivery by [`GroupState::next_pending`], with what is needed to put it
/// back untouched ([`GroupState::unpop`]).
struct Taken {
    pending: Pending,
    /// Partition index it came from; `None` = the out-of-band queue.
    from: Option<usize>,
    /// The subscription's round-robin position before the take.
    prev_rr: usize,
}

/// What [`GroupState::next_pending`] found.
enum Next {
    Msg(Taken),
    Raw(RawTake),
}

/// A contiguous fresh range taken for raw delivery.
struct RawTake {
    partition: u32,
    from_offset: u64,
    count: u32,
    envelopes: Bytes,
}

pub struct FetchReq {
    partition: usize,
    from: u64,
    generation: u64,
    /// Keep whole envelopes (raw delivery) instead of decoding them.
    raw: bool,
}

/// What a fetch read: decoded messages or raw envelopes.
enum Fetched {
    Msgs(Vec<Arc<Message>>),
    Raw(EnvelopeChunk),
}

/// The channel's current retry/DLQ policy, captured once per operation.
struct Policy<'a> {
    retry: &'a RetryPolicy,
    dlq_enabled: bool,
}

impl Policy<'_> {
    fn ack_timeout(&self) -> Duration {
        Duration::from_millis(self.retry.ack_timeout_ms.max(1))
    }

    fn backoff(&self, failed_attempts: u32) -> Duration {
        let b = &self.retry.backoff_ms;
        if b.is_empty() {
            return Duration::ZERO;
        }
        let idx = (failed_attempts.max(1) as usize - 1).min(b.len() - 1);
        Duration::from_millis(b[idx])
    }
}

/// Rounds `t` up to the next [`DEADLINE_BUCKET`] boundary relative to `base`.
fn bucket(base: Instant, t: Instant) -> Instant {
    let since = t.saturating_duration_since(base).as_nanos();
    let b = DEADLINE_BUCKET.as_nanos();
    let rounded = since.div_ceil(b) * b;
    base + Duration::from_nanos(rounded as u64)
}

/// Attempt-history updates collected under the group lock and applied after it.
#[derive(Default)]
struct HistoryOps {
    /// Plain first attempts, as offset spans (no per-message record).
    first: Spans,
    /// Redeliveries and DLQ retries.
    detailed: Vec<(MessageId, u32)>,
    /// Channel metrics of the deliveries: (messages, payload bytes, retries).
    metrics: (u64, u64, u64),
}

pub struct Group {
    pub name: Arc<str>,
    keyed: bool,
    queue: bool,
    state: Mutex<GroupState>,
    pub notify: Notify,
    pub counters: GroupCounters,
    /// Reference instant for deadline buckets.
    epoch: Instant,
}

/// Where a new group (or a reset) starts on one partition.
pub fn start_position(ch: &Channel, partition: usize, start: StartFrom) -> (u64, Option<u64>) {
    let log = &ch.logs[partition];
    match start {
        StartFrom::Earliest => (log.low(), None),
        // Replicated: "latest" is the high watermark on a leader (offsets above it are not
        // committed yet), the local log end otherwise.
        StartFrom::Latest => (
            ch.dispatch_limit(partition)
                .unwrap_or_else(|| log.high())
                .min(log.high()),
            None,
        ),
        StartFrom::TimestampMs(t) => (log.offset_for_time_approx(t), Some(t)),
    }
}

impl Group {
    /// `cursors[p]` = first offset to deliver on partition p.
    pub fn new(name: &str, mode: ChannelMode, cursors: Vec<(u64, Option<u64>)>) -> Self {
        let parts = cursors
            .into_iter()
            .map(|(cursor, skip_before_ts)| PartState::new(cursor, skip_before_ts))
            .collect();
        Self::with_parts(name, mode, parts)
    }

    /// A group restored from a checkpoint: per partition the committed offset and the
    /// finished ranges above it (see [`Group::checkpoint`]).
    pub fn restore(name: &str, mode: ChannelMode, parts: Vec<(u64, Vec<(u64, u64)>)>) -> Self {
        let parts = parts
            .into_iter()
            .map(|(cursor, acked)| {
                let mut p = PartState::new(cursor, None);
                if cursor != UNINIT {
                    for (s, e) in acked {
                        p.preacked.insert(s.max(cursor), e);
                    }
                }
                p
            })
            .collect();
        Self::with_parts(name, mode, parts)
    }

    fn with_parts(name: &str, mode: ChannelMode, parts: Vec<PartState>) -> Self {
        Self {
            name: Arc::from(name),
            keyed: mode == ChannelMode::Keyed,
            queue: mode.is_queue(),
            state: Mutex::new(GroupState {
                priority: mode == ChannelMode::Priority,
                closed: false,
                next_delivery_id: 1,
                parts,
                runs: BTreeMap::new(),
                deadlines: BTreeSet::new(),
                in_flight: 0,
                subs: BTreeMap::new(),
                oob_ready: VecDeque::new(),
                oob_live: 0,
                retry_seq: 0,
                processing: new_latency_histogram(),
                retention_skipped: 0,
                compact_later: VecDeque::new(),
            }),
            notify: Notify::new(),
            counters: GroupCounters::default(),
            epoch: Instant::now(),
        }
    }

    /// Joins a subscription; returns its instance id (`{client_name}#{connection_id}`) and its
    /// own wake-up [`Notify`], which its waiters must watch in addition to [`Group::notify`].
    pub fn add_sub(
        &self,
        id: u64,
        info: SubscriberInfo,
        prefetch: u32,
        mode: SubMode,
    ) -> (Arc<str>, Arc<Notify>) {
        let instance: Arc<str> = Arc::from(format!("{}#{}", info.client_name, info.connection_id));
        let wake = Arc::new(Notify::new());
        {
            let mut st = self.state.lock();
            st.subs.insert(
                id,
                SubState {
                    info,
                    instance: instance.clone(),
                    wake: wake.clone(),
                    prefetch: prefetch.max(1),
                    mode,
                    connected_at_ms: now_ms(),
                    in_flight: 0,
                    assigned: Vec::new(),
                    rr: 0,
                    delivered: 0,
                    acked: 0,
                    nacked: 0,
                    retries: 0,
                    processing: new_latency_histogram(),
                    last_error: None,
                    last_error_at_ms: None,
                    client_stats: None,
                },
            );
            st.rebalance(self.queue);
        }
        self.notify.notify_waiters();
        (instance, wake)
    }

    /// Leaves; outstanding deliveries of the subscription return to the group as a failed
    /// attempt and are redelivered immediately (or dead-lettered when exhausted).
    pub fn remove_sub(&self, ch: &Channel, id: u64) {
        let mut finished: Vec<Failed> = Vec::new();
        let now_ms = now_ms();
        {
            let cfg = ch.config.read();
            let pol = Policy {
                retry: &cfg.retry,
                dlq_enabled: cfg.dlq_enabled,
            };
            let mut st = self.state.lock();
            if st.subs.remove(&id).is_none() {
                return;
            }
            // Newest run first, entries newest first, each pushed to the front of `ready`:
            // the oldest ends up first.
            let firsts: Vec<u64> = st
                .runs
                .iter()
                .rev()
                .filter(|(_, r)| r.sub == id)
                .map(|(f, _)| *f)
                .collect();
            for f in firsts {
                let mut run = st.runs.remove(&f).expect("id collected above");
                if let Some(d) = run.deadline {
                    st.deadlines.remove(&(d, f));
                }
                st.in_flight -= u64::from(run.live);
                let reason = DISCONNECT_REASON.to_string();
                for mut p in run.drain_pending().into_iter().rev() {
                    let attempt = p.attempts.len() as u32;
                    finished.push(p.failed());
                    p.attempts.finish(
                        now_ms,
                        AttemptOutcome::Nacked {
                            reason: reason.clone(),
                        },
                    );
                    if attempt >= pol.retry.max_attempts {
                        st.dead_letter(self, ch, &pol, p, &reason);
                    } else {
                        let pi = st.part_index(&p.msg);
                        st.parts[pi].ready.push_front(p);
                    }
                }
            }
            st.rebalance(self.queue);
        }
        let outcome = AttemptOutcome::Nacked {
            reason: DISCONNECT_REASON.to_string(),
        };
        ch.history
            .on_failures(&finished, &self.name, now_ms, &outcome);
        self.notify.notify_waiters();
    }

    /// Ends all subscriptions (`next()` returns `None`).
    pub fn close(&self) {
        self.state.lock().closed = true;
        self.notify.notify_waiters();
    }

    /// Tries to produce the next delivery for `sub`. Never blocks; returns read-ahead fetches
    /// the caller must spawn (outside the lock).
    pub fn poll(&self, ch: &Channel, sub_id: u64) -> (Poll, Vec<FetchReq>) {
        let mut out = Vec::with_capacity(1);
        let (poll, fetch) = self.poll_batch(ch, sub_id, 1, usize::MAX, &mut out, None);
        let poll = match poll {
            BatchPoll::Ready { .. } => Poll::Ready(out.pop().expect("one delivery")),
            BatchPoll::Wait(w) => Poll::Wait(w),
            BatchPoll::Closed => Poll::Closed,
        };
        (poll, fetch)
    }

    /// Batch variant of [`Group::poll`]: appends up to `max` deliveries for `sub` to `out` in
    /// one critical section, bounded by the subscription's remaining prefetch credit and by
    /// `max_bytes` of estimated wire size ([`delivery_wire_size`]; when `out` is empty the first
    /// delivery is taken however large, so a batch is never empty for size reasons; when `out`
    /// already holds a batch being topped up, the bound is strict). Dispatch order and
    /// fairness are exactly those of `max` consecutive `poll` calls. Never blocks.
    ///
    /// With `raw` (a raw-records subscription), contiguous fresh ranges of stored batches are
    /// appended there as [`RawDelivery`] instead (each counting its records against `max` and
    /// the prefetch credit, its envelope bytes against `max_bytes`); redeliveries, legacy
    /// records, keyed and queue channels still go to `out`.
    pub fn poll_batch(
        &self,
        ch: &Channel,
        sub_id: u64,
        max: usize,
        max_bytes: usize,
        out: &mut Vec<SharedDelivery>,
        mut raw: Option<&mut Vec<RawDelivery>>,
    ) -> (BatchPoll, Vec<FetchReq>) {
        let mut hist = HistoryOps::default();
        let mut lat = Vec::new();
        let mut fetch = Vec::new();
        let now = Instant::now();
        let now_ms = now_ms();
        let consumer;
        let (taken_n, bytes, earliest) = {
            let cfg = ch.config.read();
            let pol = Policy {
                retry: &cfg.retry,
                dlq_enabled: cfg.dlq_enabled,
            };
            let mut st = self.state.lock();
            if st.closed {
                return (BatchPoll::Closed, Vec::new());
            }
            let Some(sub) = st.subs.get(&sub_id) else {
                return (BatchPoll::Closed, Vec::new());
            };
            consumer = sub.instance.clone();
            let raw_ok = raw.is_some() && sub.mode.raw_records && !self.keyed && !self.queue;
            let credit = u64::from(sub.prefetch).saturating_sub(sub.in_flight);
            let max = (max as u64).min(credit) as usize;
            let mut earliest: Option<Instant> = None;
            // The first delivery of a batch is taken whatever its size; when topping up a
            // non-empty batch the byte bound is strict.
            let starts_batch = out.is_empty() && raw.as_ref().is_none_or(|r| r.is_empty());
            let mut taken: Vec<Pending> = Vec::new();
            let mut taken_n = 0usize;
            let mut bytes = 0usize;
            while taken_n < max {
                let room = if starts_batch && taken_n == 0 {
                    max_bytes.max(1)
                } else {
                    max_bytes.saturating_sub(bytes)
                };
                let raw_room = (raw_ok
                    && raw
                        .as_ref()
                        .is_some_and(|r| r.len() < MAX_RAW_FRAMES_PER_POLL))
                .then_some((max - taken_n, room));
                let Some(next) = st.next_pending(
                    self.keyed,
                    ch,
                    sub_id,
                    now,
                    &mut earliest,
                    &mut fetch,
                    raw_room,
                ) else {
                    break;
                };
                match next {
                    Next::Msg(t) => {
                        let size = delivery_wire_size(&t.pending.msg);
                        // Decoded deliveries of one poll form one `DeliverBatch`: at most
                        // `MAX_DELIVER_BATCH` of them, even when a raw poll allows more records.
                        let full = raw_ok && out.len() + taken.len() >= MAX_DELIVER_BATCH;
                        if full
                            || (!(starts_batch && taken_n == 0)
                                && bytes.saturating_add(size) > max_bytes)
                        {
                            st.unpop(sub_id, t);
                            break;
                        }
                        bytes += size;
                        taken.push(t.pending);
                        taken_n += 1;
                    }
                    Next::Raw(r) => {
                        // Messages taken so far go out first, in their own run.
                        if !taken.is_empty() {
                            st.deliver_run(
                                self,
                                &pol,
                                sub_id,
                                std::mem::take(&mut taken),
                                now,
                                now_ms,
                                &mut lat,
                                &mut hist,
                                out,
                            );
                        }
                        bytes += r.envelopes.len();
                        taken_n += r.count as usize;
                        let d = st.deliver_raw(self, &pol, sub_id, r, now, now_ms, &mut hist);
                        raw.as_mut().expect("raw_ok").push(d);
                    }
                }
            }
            if !taken.is_empty() {
                st.deliver_run(
                    self, &pol, sub_id, taken, now, now_ms, &mut lat, &mut hist, out,
                );
            }
            (taken_n, bytes, earliest)
        };
        record_delivery_latency(ch, &lat);
        let (n, size, retries) = hist.metrics;
        if n > 0 {
            ch.metrics.delivered.add(n);
            ch.metrics.delivered_bytes.add(size);
            ch.metrics.retries.add(retries);
        }
        ch.history
            .on_deliveries(&hist.first, &hist.detailed, &self.name, &consumer, now_ms);
        if taken_n > 0 {
            (BatchPoll::Ready { bytes }, fetch)
        } else {
            (BatchPoll::Wait(earliest), fetch)
        }
    }

    pub fn spawn_fetches(self: &Arc<Self>, ch: &Arc<Channel>, reqs: Vec<FetchReq>) {
        for r in reqs {
            let g = self.clone();
            let log = ch.logs[r.partition].clone();
            let partition = log.partition;
            tokio::task::spawn_blocking(move || {
                // One sequential read of whole entries; decoded here (off the group lock and
                // off the async runtime) unless the reader wants them raw.
                let res = log.read_envelopes(r.from, FETCH_BYTES).map(|c| match c {
                    None => Fetched::Msgs(Vec::new()),
                    Some(c) if r.raw && c.batches_only => Fetched::Raw(c),
                    Some(c) => Fetched::Msgs(
                        crate::storage::decode_chunk(partition, &c.bytes)
                            .filter_map(|m| m.ok())
                            .filter(|m| m.offset >= r.from)
                            .take(FETCH_RECORDS)
                            .map(Arc::new)
                            .collect(),
                    ),
                });
                // The log below `from` was removed by retention: fall back to the
                // per-record read, which starts at the first retained offset.
                let res = match res {
                    Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => log
                        .read_batch(r.from, FETCH_RECORDS, FETCH_BYTES)
                        .map(Fetched::Msgs),
                    other => other,
                };
                if let Ok(f) = &res {
                    let n = match f {
                        Fetched::Msgs(m) => m.iter().map(|m| m.payload.len() as u64).sum(),
                        Fetched::Raw(c) => c.bytes.len() as u64,
                    };
                    g.counters.catchup_read_bytes.add(n);
                }
                g.install_fetch(r, res);
                g.notify.notify_waiters();
            });
        }
    }

    fn install_fetch(&self, r: FetchReq, res: std::io::Result<Fetched>) {
        let mut st = self.state.lock();
        let part = &mut st.parts[r.partition];
        part.fetching = false;
        if part.generation != r.generation {
            return;
        }
        let cursor = part.cursor;
        // Anything already held ends where this fetch started; drop stale data otherwise.
        let expected = part.ahead_end().unwrap_or(cursor).max(cursor);
        if r.from != expected {
            part.readahead.clear();
            part.raw_ahead.clear();
            if r.from != cursor {
                return;
            }
        }
        // The held read-ahead (decoded or raw) ends at `expected` and this fetch continues it:
        // keep it. Dropping it here skipped stored, undelivered messages (the dispatch saw the
        // next fetch's first offset above its cursor and took it for a hole in the log).
        match res {
            Ok(Fetched::Msgs(msgs)) if !msgs.is_empty() => {
                let pnum = r.partition as u32;
                for c in std::mem::take(&mut part.raw_ahead) {
                    part.readahead.extend(
                        crate::storage::decode_chunk(pnum, &c.bytes)
                            .filter_map(|m| m.ok())
                            .filter(|m| m.offset >= cursor)
                            .map(Arc::new),
                    );
                }
                let from = r.from.max(cursor);
                part.readahead
                    .extend(msgs.into_iter().filter(|m| m.offset >= from));
            }
            Ok(Fetched::Raw(c)) if c.next_offset > cursor => {
                if part.readahead.is_empty() {
                    part.raw_ahead.push_back(c);
                } else {
                    // Decoded read-ahead held: continue it decoded (order and contiguity).
                    let from = r.from.max(cursor);
                    part.readahead.extend(
                        crate::storage::decode_chunk(r.partition as u32, &c.bytes)
                            .filter_map(|m| m.ok())
                            .filter(|m| m.offset >= from)
                            .map(Arc::new),
                    );
                }
            }
            Ok(_) => part.fetch_retry_at = Some(Instant::now() + FETCH_ERROR_BACKOFF),
            Err(e) => {
                warn!(group = %self.name, partition = r.partition, error = %e, "read-ahead fetch failed");
                part.fetch_retry_at = Some(Instant::now() + FETCH_ERROR_BACKOFF);
            }
        }
    }

    /// Acks deliveries by id (any order; unknown ids are reported, the rest still applied).
    pub fn ack(&self, ch: &Channel, delivery_ids: &[u64]) -> Result<()> {
        self.settle(ch, |st, sink| {
            let mut first_err = None;
            let mut hint: Option<u64> = None;
            for &id in delivery_ids {
                if !st.ack_one(id, &mut hint, sink) {
                    first_err.get_or_insert(CoreError::UnknownDelivery(id));
                }
            }
            first_err.map_or(Ok(()), Err)
        })
    }

    /// Acks every delivery id in `ranges` (`AckRanges`). Work is proportional to the runs
    /// and ids actually covered, never to the claimed range sizes.
    pub fn ack_ranges(&self, ch: &Channel, ranges: &[DeliveryRange]) -> Result<()> {
        self.settle(ch, |st, sink| {
            let mut first_err = None;
            for r in ranges {
                if r.count == 0 {
                    continue;
                }
                let lo = r.first;
                let hi = r.first.saturating_add(u64::from(r.count));
                let covered = st.ack_range(lo, hi, sink);
                if covered < hi - lo {
                    first_err.get_or_insert(CoreError::UnknownDelivery(lo));
                }
            }
            first_err.map_or(Ok(()), Err)
        })
    }

    /// Cumulative commit by subscription `sub_id`: its deliveries of `partition` below
    /// `offset` are settled (acked). Lower offsets than already committed are a no-op.
    pub fn commit(&self, ch: &Channel, sub_id: u64, partition: u32, offset: u64) -> Result<()> {
        self.settle(ch, |st, sink| {
            let Some(sub) = st.subs.get(&sub_id) else {
                return Err(CoreError::InvalidArgument("unknown subscription".into()));
            };
            if !sub.mode.cumulative {
                return Err(CoreError::InvalidArgument(
                    "Commit is only valid for cumulative-ack subscriptions".into(),
                ));
            }
            if partition as usize >= st.parts.len() {
                return Err(CoreError::InvalidArgument(format!(
                    "partition {partition} out of range"
                )));
            }
            let firsts: Vec<u64> = st
                .runs
                .iter()
                .filter(|(_, r)| r.sub == sub_id)
                .map(|(f, _)| *f)
                .collect();
            for f in firsts {
                let len = st.runs[&f].len as usize;
                for i in 0..len {
                    let run = &st.runs[&f];
                    if !run.is_live(i) {
                        continue;
                    }
                    let slot = run.slot(i);
                    if slot.partition == partition && slot.offset < offset {
                        st.ack_at(f, i, sink);
                    }
                }
            }
            Ok(())
        })
    }

    /// Runs an ack-type operation under the lock, then applies counters, history and
    /// wake-ups outside it.
    fn settle(
        &self,
        ch: &Channel,
        op: impl FnOnce(&mut GroupState, &mut AckSink) -> Result<()>,
    ) -> Result<()> {
        let mut sink = AckSink {
            keyed: self.keyed,
            now: Some(Instant::now()),
            ..Default::default()
        };
        let result = {
            let mut st = self.state.lock();
            let r = op(&mut st, &mut sink);
            st.compact_runs(&mut sink);
            sink.flush_latency(&mut st);
            r
        };
        let n = sink.acked;
        if n > 0 {
            self.counters.acked.add(n);
            ch.metrics.acked.add(n);
            ch.history.on_acks(
                &sink.acked_first,
                &sink.acked_detailed,
                &self.name,
                now_ms(),
            );
        }
        for id in sink.dlq_removed {
            ch.dlq.remove(id);
        }
        if sink.broad {
            self.notify.notify_waiters();
        } else {
            for w in sink.woken {
                w.notify_waiters();
            }
        }
        result
    }

    pub fn nack(&self, ch: &Channel, delivery_id: u64, reason: &str, retry: bool) -> Result<()> {
        let now_ms = now_ms();
        let reason = if reason.is_empty() { "unknown" } else { reason };
        let outcome = AttemptOutcome::Nacked {
            reason: reason.to_string(),
        };
        let failed = {
            let cfg = ch.config.read();
            let pol = Policy {
                retry: &cfg.retry,
                dlq_enabled: cfg.dlq_enabled,
            };
            let mut st = self.state.lock();
            let (first, i) = st
                .locate(delivery_id)
                .ok_or(CoreError::UnknownDelivery(delivery_id))?;
            let sub_id = st.runs[&first].sub;
            if st.subs.get(&sub_id).is_some_and(|s| s.mode.cumulative) {
                return Err(CoreError::InvalidArgument(
                    "Nack is not available for cumulative-ack subscriptions".into(),
                ));
            }
            let now = Instant::now();
            let delivered_at = st.runs[&first].delivered_at;
            let Some(mut p) = st.take_pending_at(first, i) else {
                return Err(CoreError::UnknownDelivery(delivery_id));
            };
            if let Some(sub) = st.subs.get_mut(&sub_id) {
                sub.nacked += 1;
                sub.last_error = Some(reason.to_string());
                sub.last_error_at_ms = Some(now_ms);
                record(
                    &mut sub.processing,
                    now.duration_since(delivered_at).as_micros() as u64,
                );
            }
            self.counters.nacked.inc();
            ch.metrics.nacked.inc();
            let failed = p.failed();
            p.attempts.finish(now_ms, outcome.clone());
            if retry {
                st.fail_attempt(self, ch, &pol, p, reason, now);
            } else {
                st.dead_letter(self, ch, &pol, p, reason);
            }
            failed
        };
        ch.history
            .on_failures(&[failed], &self.name, now_ms, &outcome);
        self.notify.notify_waiters();
        Ok(())
    }

    /// Expires deliveries past their ack deadline. Returns true if anything changed.
    pub fn reap(&self, ch: &Channel, now: Instant) -> bool {
        let now_ms = now_ms();
        let mut finished: Vec<Failed> = Vec::new();
        {
            // Lock order: channel config before group state (see lib.rs).
            let cfg = ch.config.read();
            let pol = Policy {
                retry: &cfg.retry,
                dlq_enabled: cfg.dlq_enabled,
            };
            let mut st = self.state.lock();
            if !st.compact_later.is_empty() {
                let mut sink = AckSink {
                    now: Some(now),
                    ..Default::default()
                };
                st.compact_runs(&mut sink);
            }
            match st.deadlines.first() {
                Some(&(d, _)) if d <= now => {}
                _ => return false,
            }
            while let Some(&(deadline, first)) = st.deadlines.first() {
                if deadline > now {
                    break;
                }
                st.deadlines.pop_first();
                let Some(mut run) = st.runs.remove(&first) else {
                    continue;
                };
                st.in_flight -= u64::from(run.live);
                if let Some(sub) = st.subs.get_mut(&run.sub) {
                    sub.in_flight -= u64::from(run.live);
                    sub.last_error = Some(TIMEOUT_REASON.to_string());
                    sub.last_error_at_ms = Some(now_ms);
                }
                ch.metrics.timed_out.add(u64::from(run.live));
                for mut p in run.drain_pending() {
                    finished.push(p.failed());
                    p.attempts.finish(now_ms, AttemptOutcome::TimedOut);
                    st.fail_attempt(self, ch, &pol, p, TIMEOUT_REASON, now);
                }
            }
        }
        ch.history
            .on_failures(&finished, &self.name, now_ms, &AttemptOutcome::TimedOut);
        self.notify.notify_waiters();
        true
    }

    /// Re-enqueues a dead letter with a fresh attempt budget.
    pub fn inject_dlq_retry(&self, dl: DeadLetter) {
        {
            let mut st = self.state.lock();
            st.oob_live += 1;
            st.oob_ready.push_back(Pending {
                msg: Arc::new(dl.message),
                attempts: Attempts::default(),
                tracked: false,
                dlq_id: Some(dl.id),
                detached: true,
            });
        }
        self.notify.notify_waiters();
    }

    /// Moves every partition's position. Messages already delivered (in flight or waiting for
    /// a retry) keep going as out-of-band entries; not-yet-delivered ones are dropped.
    pub fn reset(&self, ch: &Channel, to: StartFrom) {
        {
            let mut st = self.state.lock();
            let mut orphaned = 0u64;
            // Raw runs are decoded so their entries can be marked out-of-band.
            let mut lost: Vec<(u64, u32)> = Vec::new();
            for run in st.runs.values_mut() {
                let before = run.live;
                run.materialize();
                if run.live < before {
                    lost.push((run.sub, before - run.live));
                }
            }
            for (sub_id, n) in lost {
                st.in_flight -= u64::from(n);
                if let Some(sub) = st.subs.get_mut(&sub_id) {
                    sub.in_flight = sub.in_flight.saturating_sub(u64::from(n));
                }
            }
            let empty: Vec<u64> = st
                .runs
                .iter()
                .filter(|(_, r)| r.live == 0)
                .map(|(f, _)| *f)
                .collect();
            for f in empty {
                st.remove_run(f);
            }
            for run in st.runs.values_mut() {
                if let Body::Msgs(v) = &mut run.body {
                    for e in v.iter_mut().flatten() {
                        if e.orphan() {
                            orphaned += 1;
                        }
                    }
                }
            }
            for (p, part) in st.parts.iter_mut().enumerate() {
                for r in part.retry.values_mut() {
                    if r.tracked {
                        r.tracked = false;
                        orphaned += 1;
                    }
                }
                // Ready entries that were delivered before stay as out-of-band.
                part.ready.retain(|r| !r.tracked || !r.attempts.is_empty());
                for r in part.ready.iter_mut() {
                    if r.tracked {
                        r.tracked = false;
                        orphaned += 1;
                    }
                }
                let (cursor, skip) = start_position(ch, p, to);
                part.cursor = cursor;
                part.skip_before_ts = skip;
                part.acks.reset(cursor);
                part.preacked.clear();
                part.active_keys.clear();
                part.deferred.clear();
                part.deferred_len = 0;
                part.readahead.clear();
                part.raw_ahead.clear();
                part.fetching = false;
                part.fetch_retry_at = None;
                part.generation += 1;
            }
            st.oob_live += orphaned;
        }
        self.notify.notify_waiters();
    }

    /// Drops partition `p`'s dispatch state (role change in replicated mode, or replicated
    /// offsets on a follower): in-flight deliveries of it are forgotten (their acks fail with
    /// `UnknownDelivery`; the partition leader redelivers), retries/ready/read-ahead are
    /// discarded and the cursor becomes `to` — or, with `None`, the committed offset capped at
    /// `high`.
    pub fn reset_partition(&self, p: usize, to: Option<u64>, high: u64) {
        {
            let mut st = self.state.lock();
            if p >= st.parts.len() {
                return;
            }
            let committed = st.parts[p].committed();
            let mut oob_dropped = 0u64;
            let firsts: Vec<u64> = st.runs.keys().copied().collect();
            for f in firsts {
                let len = st.runs[&f].len as usize;
                for i in 0..len {
                    let run = &st.runs[&f];
                    if !run.is_live(i) || run.slot(i).partition as usize != p {
                        continue;
                    }
                    let sub_id = run.sub;
                    let run = st.runs.get_mut(&f).expect("present");
                    let entry = run.settle(i);
                    let empty = run.live == 0;
                    if entry.as_ref().is_some_and(|e| !e.tracked()) {
                        oob_dropped += 1;
                    }
                    st.in_flight -= 1;
                    if let Some(sub) = st.subs.get_mut(&sub_id) {
                        sub.in_flight = sub.in_flight.saturating_sub(1);
                    }
                    if empty {
                        st.remove_run(f);
                    }
                }
            }
            st.oob_ready.retain(|x| {
                let keep = x.msg.partition as usize != p;
                if !keep {
                    oob_dropped += 1;
                }
                keep
            });
            let part = &mut st.parts[p];
            oob_dropped += part.retry.values().filter(|x| !x.tracked).count() as u64;
            oob_dropped += part.ready.iter().filter(|x| !x.tracked).count() as u64;
            let cursor = match to {
                Some(o) => o,
                None if committed == UNINIT => UNINIT,
                None => committed.min(high),
            };
            let generation = part.generation + 1;
            *part = PartState::new(cursor, None);
            part.generation = generation;
            st.oob_live = st.oob_live.saturating_sub(oob_dropped);
        }
        self.notify.notify_waiters();
    }

    /// Initializes never-initialized partitions at `start` (a subscriber joined).
    pub fn init_uninit(&self, ch: &Channel, start: StartFrom) {
        let changed = {
            let mut st = self.state.lock();
            let mut changed = false;
            for (p, part) in st.parts.iter_mut().enumerate() {
                if part.cursor == UNINIT {
                    let (c, skip) = start_position(ch, p, start);
                    part.cursor = c;
                    part.acks.reset(c);
                    part.skip_before_ts = skip;
                    changed = true;
                }
            }
            changed
        };
        if changed {
            self.notify.notify_waiters();
        }
    }

    /// Number of active subscriptions.
    pub fn subscriber_count(&self) -> usize {
        self.state.lock().subs.len()
    }

    /// True when no partition has a known position (replicated-only group shell).
    pub fn all_uninit(&self) -> bool {
        self.state.lock().parts.iter().all(|p| p.cursor == UNINIT)
    }

    /// Committed offset of partition `p` ([`UNINIT`] if never initialized).
    pub fn committed_at(&self, p: usize) -> u64 {
        let st = self.state.lock();
        st.parts.get(p).map_or(UNINIT, PartState::committed)
    }

    /// Committed offset (lowest unfinished) per partition.
    pub fn committed(&self) -> Vec<u64> {
        let st = self.state.lock();
        st.parts.iter().map(PartState::committed).collect()
    }

    /// What checkpoints persist, per partition: the committed offset and up to
    /// [`MAX_CHECKPOINT_RANGES`] finished ranges above it (acked out of order, plus restored
    /// ranges not dispatched again yet).
    pub fn checkpoint(&self) -> Vec<(u64, Vec<(u64, u64)>)> {
        let st = self.state.lock();
        st.parts
            .iter()
            .map(|p| {
                let committed = p.committed();
                if committed == UNINIT {
                    return (UNINIT, Vec::new());
                }
                let (_, mut ranges) = p.acks.checkpoint(MAX_CHECKPOINT_RANGES);
                ranges.retain(|&(_, e)| e > committed);
                let room = MAX_CHECKPOINT_RANGES.saturating_sub(ranges.len());
                ranges.extend(p.preacked.iter().take(room));
                (committed, ranges)
            })
            .collect()
    }

    pub fn report_stats(&self, sub: u64, stats: ClientStats) {
        let mut st = self.state.lock();
        if let Some(s) = st.subs.get_mut(&sub) {
            if let Some(e) = &stats.last_error
                && s.last_error.as_deref() != Some(e.as_str())
            {
                s.last_error = Some(e.clone());
                s.last_error_at_ms = Some(now_ms());
            }
            s.client_stats = Some(stats);
        }
    }

    pub fn stats(&self, ch: &Channel) -> GroupStats {
        let st = self.state.lock();
        let mut part_lag = Vec::with_capacity(st.parts.len());
        let mut lag = st.oob_live;
        let mut retry_pending = 0u64;
        for (p, part) in st.parts.iter().enumerate() {
            let log = &ch.logs[p];
            let high = log.high();
            let cursor = part.cursor.max(log.low()).min(high);
            let l = (high - cursor) - part.preacked.count_in(cursor, high) + part.pending();
            part_lag.push(l);
            lag += l;
            retry_pending += part.retry.len() as u64;
        }
        let n_subs = st.subs.len() as u64;
        let instances = st
            .subs
            .iter()
            .map(|(&id, s)| InstanceStats {
                id: s.instance.to_string(),
                subscription_id: id,
                client_name: s.info.client_name.clone(),
                remote_addr: s.info.remote_addr.clone(),
                connected_at_ms: s.connected_at_ms,
                prefetch: s.prefetch,
                in_flight: s.in_flight,
                assigned_partitions: s.assigned.clone(),
                lag: if self.queue {
                    lag / n_subs.max(1)
                } else {
                    s.assigned.iter().map(|&p| part_lag[p as usize]).sum()
                },
                delivered_total: s.delivered,
                acked_total: s.acked,
                nacked_total: s.nacked,
                retries_total: s.retries,
                processing_latency_us: s.processing.clone(),
                last_error: s.last_error.clone(),
                last_error_at_ms: s.last_error_at_ms,
                client_stats: s.client_stats.clone(),
            })
            .collect();
        GroupStats {
            name: self.name.to_string(),
            lag,
            in_flight: st.in_flight,
            retry_pending,
            delivered_total: self.counters.delivered.get(),
            retention_skipped_total: st.retention_skipped,
            acked_total: self.counters.acked.get(),
            nacked_total: self.counters.nacked.get(),
            dead_lettered_total: self.counters.dead_lettered.get(),
            catchup_read_bytes_total: self.counters.catchup_read_bytes.get(),
            processing_latency_us: st.processing.clone(),
            instances,
        }
    }
}

/// Accumulates the effects of settling deliveries under the group lock.
#[derive(Default)]
struct AckSink {
    keyed: bool,
    now: Option<Instant>,
    /// Deliveries acked.
    acked: u64,
    /// Acked plain first attempts (history spans).
    acked_first: Spans,
    /// Acked redeliveries / DLQ retries.
    acked_detailed: Vec<(MessageId, u32)>,
    dlq_removed: Vec<u64>,
    woken: Vec<Arc<Notify>>,
    /// A deferred keyed message became ready: wake every subscription.
    broad: bool,
    /// Processing-latency samples not yet recorded: (sub, µs, count).
    lat: Option<(u64, u64, u64)>,
    /// Raw runs that became mostly settled in this batch: compacted (if still mostly
    /// settled) once the whole batch is applied, see [`GroupState::compact_runs`].
    compact: Vec<u64>,
}

impl AckSink {
    fn now(&self) -> Instant {
        self.now.unwrap_or_else(Instant::now)
    }

    fn sample(&mut self, st: &mut GroupState, sub: u64, lat_us: u64) {
        self.sample_n(st, sub, lat_us, 1);
    }

    /// `count` samples of the same latency.
    fn sample_n(&mut self, st: &mut GroupState, sub: u64, lat_us: u64, count: u64) {
        match &mut self.lat {
            Some((s, l, n)) if *s == sub && *l == lat_us => *n += count,
            _ => {
                self.flush_latency(st);
                self.lat = Some((sub, lat_us, count));
            }
        }
    }

    fn flush_latency(&mut self, st: &mut GroupState) {
        if let Some((sub, l, n)) = self.lat.take() {
            if let Some(s) = st.subs.get_mut(&sub) {
                record_n(&mut s.processing, l, n);
            }
            record_n(&mut st.processing, l, n);
        }
    }
}

impl GroupState {
    fn part_index(&self, m: &Message) -> usize {
        (m.partition as usize).min(self.parts.len().saturating_sub(1))
    }

    /// The run and index holding live delivery `id`.
    fn locate(&self, id: u64) -> Option<(u64, usize)> {
        let (&first, run) = self.runs.range(..=id).next_back()?;
        let i = (id - first) as usize;
        (i < run.len as usize && run.is_live(i)).then_some((first, i))
    }

    fn remove_run(&mut self, first: u64) {
        if let Some(run) = self.runs.remove(&first)
            && let Some(d) = run.deadline
        {
            self.deadlines.remove(&(d, first));
        }
    }

    /// Compacts the raw runs `sink.compact` names that are still mostly settled after the
    /// ack batch (stragglers keep their own payload copies, the envelope buffer is freed).
    /// Entries whose envelope can't be decoded are dropped from the in-flight counts, as
    /// before.
    ///
    /// v0.8: only runs delivered at least [`COMPACT_AFTER`] ago are compacted; younger ones
    /// are queued and compacted by a later ack batch or the reaper tick once old enough (if
    /// still mostly settled). With small ack batches (the SDK flushes every 64 acks) the last
    /// eighth of a run is usually acked a few milliseconds later: compacting it at once decoded
    /// and CRC-checked whole envelopes for nothing (~25 % of broker CPU at 100 B 1×1).
    fn compact_runs(&mut self, sink: &mut AckSink) {
        let now = sink.now();
        let mut due = std::mem::take(&mut sink.compact);
        while let Some(&(at, first)) = self.compact_later.front() {
            if now.saturating_duration_since(at) < COMPACT_AFTER {
                break;
            }
            self.compact_later.pop_front();
            due.push(first);
        }
        for first in due {
            let Some(run) = self.runs.get_mut(&first) else {
                continue;
            };
            if !(run.live > 0 && run.is_raw() && run.len >= 64 && run.live * 8 <= run.len) {
                continue;
            }
            if now.saturating_duration_since(run.delivered_at) < COMPACT_AFTER {
                if !run.compact_queued {
                    run.compact_queued = true;
                    self.compact_later.push_back((run.delivered_at, first));
                }
                continue;
            }
            let before = run.live;
            run.materialize();
            run.detach_payloads();
            let lost = u64::from(before - run.live);
            let sub_id = run.sub;
            if lost > 0 {
                self.in_flight -= lost;
                if let Some(sub) = self.subs.get_mut(&sub_id) {
                    sub.in_flight -= lost;
                }
            }
        }
    }

    /// Acks live delivery `first + i`: settles it, finishes its offset, records counters
    /// in `sink`.
    fn ack_at(&mut self, first: u64, i: usize, sink: &mut AckSink) {
        let run = self.runs.get_mut(&first).expect("caller located the run");
        let slot = run.slot(i);
        let sub_id = run.sub;
        let delivered_at = run.delivered_at;
        let entry = run.settle(i);
        // A mostly settled raw run would pin its whole envelope buffer for a few stragglers:
        // keep only the stragglers, with payloads of their own. Decided after the whole ack
        // batch: an `AckRanges` usually settles the rest of the run a moment later, and
        // compacting in the middle decoded (and CRC-checked) every delivered envelope a
        // second time (~10 % of broker CPU at 1 KiB in the v0.7 profile).
        if run.live > 0 && run.is_raw() && run.len >= 64 && run.live * 8 <= run.len {
            sink.compact.push(first);
        }
        let empty = run.live == 0;
        self.in_flight -= 1;
        let lat = sink.now().duration_since(delivered_at).as_micros() as u64;
        if let Some(sub) = self.subs.get_mut(&sub_id) {
            sub.in_flight -= 1;
            sub.acked += 1;
            if !sink.woken.iter().any(|w| Arc::ptr_eq(w, &sub.wake)) {
                sink.woken.push(sub.wake.clone());
            }
        }
        sink.sample(self, sub_id, lat);
        let id = MessageId {
            partition: slot.partition,
            offset: slot.offset,
        };
        sink.acked += 1;
        match entry {
            Some(e) => {
                let attempt = e.attempt();
                if attempt == 1 && e.dlq_id().is_none() {
                    sink.acked_first.push(id.partition, id.offset);
                } else {
                    sink.acked_detailed.push((id, attempt));
                }
                if let Some(d) = e.dlq_id() {
                    sink.dlq_removed.push(d);
                }
                sink.broad |= self.complete(sink.keyed, &e.msg, e.tracked());
            }
            None => {
                // Raw entry: a tracked first attempt of a stream partition.
                sink.acked_first.push(id.partition, id.offset);
                self.finish_offset(slot.partition, slot.offset);
            }
        }
        if empty {
            self.remove_run(first);
        }
    }

    /// Acks one id; `hint` caches the run of the previous id (acks come in runs).
    fn ack_one(&mut self, id: u64, hint: &mut Option<u64>, sink: &mut AckSink) -> bool {
        let first = match *hint {
            Some(f) if id >= f && self.runs.get(&f).is_some_and(|r| id - f < u64::from(r.len)) => f,
            _ => match self.runs.range(..=id).next_back() {
                Some((&f, r)) if id - f < u64::from(r.len) => f,
                _ => return false,
            },
        };
        let i = (id - first) as usize;
        if !self.runs[&first].is_live(i) {
            return false;
        }
        *hint = Some(first);
        self.ack_at(first, i, sink);
        true
    }

    /// Acks every live id in `[lo, hi)`; returns how many ids were live.
    fn ack_range(&mut self, lo: u64, hi: u64, sink: &mut AckSink) -> u64 {
        let mut firsts: Vec<u64> = Vec::new();
        if let Some((&f, r)) = self.runs.range(..lo).next_back()
            && f + u64::from(r.len) > lo
        {
            firsts.push(f);
        }
        firsts.extend(self.runs.range(lo..hi).map(|(&f, _)| f));
        let mut covered = 0;
        for f in firsts {
            let len = u64::from(self.runs[&f].len);
            let a = lo.max(f) - f;
            let b = hi.min(f + len) - f;
            if self.runs[&f].is_raw() {
                covered += self.ack_raw_range(f, a as usize, b as usize, sink);
                continue;
            }
            for i in a..b {
                if !self.runs.contains_key(&f) {
                    break;
                }
                if self.runs[&f].is_live(i as usize) {
                    self.ack_at(f, i as usize, sink);
                    covered += 1;
                }
            }
        }
        covered
    }

    /// [`GroupState::ack_at`] for every live entry `a .. b` of raw run `first`, as range
    /// operations: live bits cleared a word at a time, offsets finished and history spans
    /// recorded per contiguous stretch, counters and the latency sample applied once. Same
    /// effects as acking the entries one by one. Returns how many were live.
    fn ack_raw_range(&mut self, first: u64, a: usize, b: usize, sink: &mut AckSink) -> u64 {
        let run = self.runs.get_mut(&first).expect("caller located the run");
        let (sub_id, delivered_at) = (run.sub, run.delivered_at);
        let Body::Raw {
            partition,
            from_offset,
            live,
            ..
        } = &mut run.body
        else {
            unreachable!("raw run");
        };
        let (partition, from) = (*partition, *from_offset);
        // Contiguous stretches of live entries, as offsets.
        let mut stretches: Vec<(u64, u64)> = Vec::new();
        let mut n = 0u64;
        let mut i = a;
        while i < b {
            let w = i / 64;
            let bit = i % 64;
            let span = (64 - bit).min(b - i);
            let mask = if span == 64 {
                u64::MAX
            } else {
                ((1u64 << span) - 1) << bit
            };
            let hit = live[w] & mask;
            if hit != 0 {
                live[w] &= !mask;
                n += u64::from(hit.count_ones());
                // Walk the set bits of `hit` as runs of ones.
                let mut h = hit;
                while h != 0 {
                    let s = h.trailing_zeros() as usize;
                    let ones = (!(h >> s)).trailing_zeros() as usize;
                    let lo = from + (w * 64 + s) as u64;
                    let hi = lo + ones as u64;
                    match stretches.last_mut() {
                        Some((_, e)) if *e == lo => *e = hi,
                        _ => stretches.push((lo, hi)),
                    }
                    h &= if s + ones >= 64 {
                        0
                    } else {
                        !((1u64 << (s + ones)) - 1)
                    };
                }
            }
            i += span;
        }
        if n == 0 {
            return 0;
        }
        run.live -= n as u32;
        if run.live > 0 && run.len >= 64 && run.live * 8 <= run.len {
            sink.compact.push(first);
        }
        let empty = run.live == 0;
        self.in_flight -= n;
        let lat = sink.now().duration_since(delivered_at).as_micros() as u64;
        if let Some(sub) = self.subs.get_mut(&sub_id) {
            sub.in_flight -= n;
            sub.acked += n;
            if !sink.woken.iter().any(|w| Arc::ptr_eq(w, &sub.wake)) {
                sink.woken.push(sub.wake.clone());
            }
        }
        sink.sample_n(self, sub_id, lat, n);
        sink.acked += n;
        let pi = (partition as usize).min(self.parts.len().saturating_sub(1));
        for (lo, hi) in stretches {
            sink.acked_first.push_range(partition, lo, hi);
            let part = &mut self.parts[pi];
            if part.cursor != UNINIT {
                part.acks.finish_range(lo, hi.min(part.cursor));
            }
        }
        if empty {
            self.remove_run(first);
        }
        n
    }

    /// Settles live entry `first + i` for a failure path: removes it from in-flight
    /// accounting and returns it as a [`Pending`] whose last attempt is the one in flight.
    fn take_pending_at(&mut self, first: u64, i: usize) -> Option<Pending> {
        let run = self.runs.get_mut(&first)?;
        let sub_id = run.sub;
        let p = run.take_pending(i);
        let empty = run.live == 0;
        self.in_flight -= 1;
        if let Some(sub) = self.subs.get_mut(&sub_id) {
            sub.in_flight -= 1;
        }
        if empty {
            self.remove_run(first);
        }
        p
    }

    /// Deterministic assignment: instances sorted by (instance id, subscription id);
    /// partition p → instance p mod n. Queue groups: every instance may take any partition.
    fn rebalance(&mut self, queue: bool) {
        let p_count = self.parts.len() as u32;
        let mut ids: Vec<(Arc<str>, u64)> = self
            .subs
            .iter()
            .map(|(&id, s)| (s.instance.clone(), id))
            .collect();
        ids.sort();
        for s in self.subs.values_mut() {
            s.assigned.clear();
            s.rr = 0;
        }
        if ids.is_empty() {
            return;
        }
        for p in 0..p_count {
            if queue {
                for (_, id) in &ids {
                    self.subs
                        .get_mut(id)
                        .expect("id from subs")
                        .assigned
                        .push(p);
                }
            } else {
                let (_, id) = &ids[p as usize % ids.len()];
                self.subs
                    .get_mut(id)
                    .expect("id from subs")
                    .assigned
                    .push(p);
            }
        }
        if self.priority {
            // Highest level first, for every consumer (strict priority).
            for s in self.subs.values_mut() {
                s.assigned.reverse();
            }
        } else if queue {
            // Spread starting partitions so competing consumers do not all start at p0.
            for (i, (_, id)) in ids.iter().enumerate() {
                self.subs.get_mut(id).expect("id from subs").rr = i % p_count.max(1) as usize;
            }
        }
    }

    /// Next message (or raw range, with `raw_room = Some((max records, max bytes))`) for
    /// `sub_id` (which must exist): out-of-band redeliveries first, then its assigned
    /// partitions round-robin starting at its `rr` position (`Priority` channels: highest
    /// level first, every time). Advances `rr` past the
    /// partition it took from. Prefetch credit is the caller's business.
    #[allow(clippy::too_many_arguments)]
    fn next_pending(
        &mut self,
        keyed: bool,
        ch: &Channel,
        sub_id: u64,
        now: Instant,
        earliest: &mut Option<Instant>,
        fetch: &mut Vec<FetchReq>,
        raw_room: Option<(usize, usize)>,
    ) -> Option<Next> {
        let sub = self.subs.get_mut(&sub_id).expect("caller checked the sub");
        let prev_rr = sub.rr;
        if let Some(pending) = self.oob_ready.pop_front() {
            sub.rr = (prev_rr + 1) % sub.assigned.len().max(1);
            return Some(Next::Msg(Taken {
                pending,
                from: None,
                prev_rr,
            }));
        }
        let assigned = std::mem::take(&mut sub.assigned);
        let n = assigned.len();
        let mut found = None;
        for i in 0..n {
            let p = if self.priority {
                assigned[i]
            } else {
                assigned[(prev_rr + i) % n]
            } as usize;
            if let Some(room) = raw_room
                && let Some(r) = self.take_raw(ch, p, now, earliest, fetch, room)
            {
                found = Some((i, p, Err(r)));
                break;
            }
            if let Some(pending) =
                self.take_next(keyed, ch, p, now, earliest, fetch, raw_room.is_some())
            {
                found = Some((i, p, Ok(pending)));
                break;
            }
        }
        let sub = self.subs.get_mut(&sub_id).expect("caller checked the sub");
        sub.assigned = assigned;
        let (i, p, got) = found?;
        sub.rr = (prev_rr + i + 1) % n.max(1);
        Some(match got {
            Ok(pending) => Next::Msg(Taken {
                pending,
                from: Some(p),
                prev_rr,
            }),
            Err(r) => Next::Raw(r),
        })
    }

    /// Puts back a message taken by `next_pending` and not delivered, so that it is the next
    /// one taken. A partition message goes to the front of `ready` (its pending/key bookkeeping
    /// stays as `take_next` left it, which is what `ready` entries carry).
    fn unpop(&mut self, sub_id: u64, taken: Taken) {
        match taken.from {
            None => self.oob_ready.push_front(taken.pending),
            Some(p) => self.parts[p].ready.push_front(taken.pending),
        }
        if let Some(sub) = self.subs.get_mut(&sub_id) {
            sub.rr = taken.prev_rr;
        }
    }

    /// Queues a read-ahead fetch of partition `p` from `from` unless one is in flight or
    /// backing off. Returns false while backing off (and updates `earliest`).
    fn request_fetch(
        part: &mut PartState,
        p: usize,
        from: u64,
        raw: bool,
        now: Instant,
        earliest: &mut Option<Instant>,
        fetch: &mut Vec<FetchReq>,
    ) {
        if part.fetching {
            return;
        }
        if let Some(at) = part.fetch_retry_at {
            if at > now {
                *earliest = Some(earliest.map_or(at, |e| e.min(at)));
                return;
            }
            part.fetch_retry_at = None;
        }
        part.fetching = true;
        fetch.push(FetchReq {
            partition: p,
            from,
            generation: part.generation,
            raw,
        });
    }

    /// Raw fast path for partition `p`: a contiguous range of fresh first attempts from the
    /// cursor, as whole stored envelopes. `None` whenever anything needs per-message
    /// handling (due retries, ready messages, timestamp skip, legacy records), in which case
    /// [`GroupState::take_next`] serves the partition.
    fn take_raw(
        &mut self,
        ch: &Channel,
        p: usize,
        now: Instant,
        earliest: &mut Option<Instant>,
        fetch: &mut Vec<FetchReq>,
        (max_records, max_bytes): (usize, usize),
    ) -> Option<RawTake> {
        let limit = ch.dispatch_limit(p)?;
        let part = &mut self.parts[p];
        if part.cursor == UNINIT || part.skip_before_ts.is_some() || !part.ready.is_empty() {
            return None;
        }
        if part
            .retry
            .first_key_value()
            .is_some_and(|((due, _), _)| *due <= now)
        {
            return None;
        }
        let log = &ch.logs[p];
        let high = limit.min(log.high());
        if part.cursor < log.low() {
            return None; // take_next accounts for the retention skip
        }
        // Restored acks at the cursor: skip them (they were finished before a restart).
        if let Some((_, e)) = part.preacked.range_containing(part.cursor) {
            part.skip_to(e.min(high));
        }
        let cursor = part.cursor;
        if cursor >= high || max_records == 0 {
            return None;
        }
        // Raw read-ahead first, then the recent cache.
        while part
            .raw_ahead
            .front()
            .is_some_and(|c| c.next_offset <= cursor)
        {
            part.raw_ahead.pop_front();
        }
        let chunk = match part.raw_ahead.front() {
            Some(c) if c.first_base <= cursor => Some(c.clone()),
            Some(_) => {
                // A hole before the read-ahead: let take_next handle it.
                return None;
            }
            // One cached entry is zero-copy; several are concatenated (a copy), worth it
            // only for small entries (many tiny publishes).
            None => match log.cached_envelopes(cursor, 1) {
                Some(c) if c.bytes.len() < RAW_COALESCE_BELOW && max_bytes > c.bytes.len() => {
                    log.cached_envelopes(cursor, max_bytes.min(RAW_COALESCE_BELOW * 4))
                }
                other => other,
            },
        };
        let Some(chunk) = chunk else {
            if part.readahead.is_empty() {
                Self::request_fetch(part, p, cursor, true, now, earliest, fetch);
            }
            return None;
        };
        if !chunk.batches_only {
            return None;
        }
        // Read ahead while this chunk is consumed: up to `RAW_AHEAD_CHUNKS` chunks held. A
        // lagging group reads from disk, where (under heavy writes and fsyncs) a fetch's
        // latency, not its bandwidth, bounded catch-up with one chunk in reserve.
        if part.raw_ahead.len() < RAW_AHEAD_CHUNKS
            && let Some(back) = part.raw_ahead.back()
            && back.next_offset < high
        {
            let from = back.next_offset;
            Self::request_fetch(part, p, from, true, now, earliest, fetch);
        }
        // Stop before the next restored range, the dispatch limit and the credit.
        let mut end = chunk.next_offset.min(high).min(cursor + max_records as u64);
        if let Some((s, _)) = part.preacked.first() {
            end = end.min(s.max(cursor));
        }
        if end <= cursor {
            return None;
        }
        // Slice the envelopes covering [cursor, end), within max_bytes (at least one).
        let bytes = &chunk.bytes;
        let mut pos = 0usize;
        let mut start: Option<usize> = None;
        let mut stop = 0usize;
        let mut covered_end = cursor;
        while pos < bytes.len() {
            let h = EnvelopeHeader::parse(&bytes[pos..])?;
            let next = h.next_offset();
            let total = h.total_len();
            if next <= cursor {
                pos += total;
                continue;
            }
            if h.base_offset >= end || (start.is_some() && h.base_offset != covered_end) {
                // Past the range, or a gap between entries: stop at the covered end.
                break;
            }
            if start.is_none() && h.base_offset > cursor {
                return None; // the chunk doesn't hold the cursor
            }
            let s = *start.get_or_insert(pos);
            if pos + total - s > max_bytes && pos > s {
                break;
            }
            stop = pos + total;
            covered_end = next.min(end);
            pos += total;
        }
        let s = start?;
        let count = (covered_end - cursor) as u32;
        if count == 0 {
            return None;
        }
        part.cursor = covered_end;
        Some(RawTake {
            partition: p as u32,
            from_offset: cursor,
            count,
            envelopes: bytes.slice(s..stop),
        })
    }

    /// Next deliverable message on partition `p`: due retries, then ready, then new messages.
    #[allow(clippy::too_many_arguments)]
    fn take_next(
        &mut self,
        keyed: bool,
        ch: &Channel,
        p: usize,
        now: Instant,
        earliest: &mut Option<Instant>,
        fetch: &mut Vec<FetchReq>,
        raw_sub: bool,
    ) -> Option<Pending> {
        // Replicated mode: only a partition's leader serves it, and only below the HW.
        let mut limit = ch.dispatch_limit(p)?;
        let part = &mut self.parts[p];
        if part.cursor == UNINIT {
            return None;
        }
        if let Some((&(due, _), _)) = part.retry.first_key_value() {
            if due <= now {
                return part.retry.pop_first().map(|(_, v)| v);
            }
            *earliest = Some(earliest.map_or(due, |e| e.min(due)));
        }
        if let Some(r) = part.ready.pop_front() {
            return Some(r);
        }
        let log = &ch.logs[p];
        loop {
            let high = limit.min(log.high());
            let low = log.low();
            if part.cursor < low {
                // Retention removed messages this group never consumed: skip them, but
                // never silently (metric + warning with the range).
                let skipped = low - part.cursor;
                warn!(
                    channel = %ch.name,
                    partition = p,
                    from = part.cursor,
                    to = low,
                    skipped,
                    "retention removed messages a consumer group had not consumed; skipping them"
                );
                part.skip_to(low);
                self.retention_skipped += skipped;
            }
            if let Some((_, e)) = part.preacked.range_containing(part.cursor) {
                part.skip_to(e.min(high));
                continue;
            }
            if part.cursor >= high {
                return None;
            }
            if keyed && part.deferred_len >= MAX_DEFERRED {
                return None;
            }
            let cursor = part.cursor;
            let mut msg = None;
            while let Some(front) = part.readahead.front() {
                if front.offset < cursor {
                    part.readahead.pop_front();
                } else if front.offset > cursor {
                    // The read-ahead starts above the cursor. The cursor is in the retained
                    // log (checked above), so never skip on that alone: drop the read-ahead
                    // and read again exactly from the cursor. Only a hole seen again at the
                    // same cursor (an unreadable region) is skipped, counted and warned.
                    let to = front.offset;
                    if part.hole_at != Some(cursor) {
                        part.hole_at = Some(cursor);
                        part.readahead.clear();
                        part.raw_ahead.clear();
                        Self::request_fetch(part, p, cursor, raw_sub, now, earliest, fetch);
                        return None;
                    }
                    let skipped = to - cursor;
                    warn!(
                        channel = %ch.name,
                        partition = p,
                        from = cursor,
                        to,
                        skipped,
                        "unreadable region in the log; skipping messages a consumer group had not consumed"
                    );
                    part.skip_to(to);
                    self.retention_skipped += skipped;
                    break;
                } else {
                    msg = part.readahead.pop_front();
                    break;
                }
            }
            if part.cursor != cursor {
                limit = ch.dispatch_limit(p)?;
                continue;
            }
            // Raw read-ahead this path can't serve as is (the raw fast path declined the
            // partition: timestamp skip, a hole, ...): decode it into the message read-ahead
            // (rare; never loses the fetched data, never stalls).
            if msg.is_none() && !part.raw_ahead.is_empty() {
                let pnum = p as u32;
                for c in std::mem::take(&mut part.raw_ahead) {
                    part.readahead.extend(
                        crate::storage::decode_chunk(pnum, &c.bytes)
                            .filter_map(|m| m.ok())
                            .filter(|m| m.offset >= cursor)
                            .map(Arc::new),
                    );
                }
                if !part.readahead.is_empty() {
                    continue;
                }
            }
            let msg = match msg.or_else(|| log.cached(cursor)) {
                Some(m) => m,
                None => {
                    let from = part.ahead_end().unwrap_or(cursor).max(cursor);
                    if from == cursor {
                        Self::request_fetch(part, p, cursor, raw_sub, now, earliest, fetch);
                    }
                    return None;
                }
            };
            // Keep reading ahead while dispatching from the read-ahead (double buffering).
            if !part.readahead.is_empty()
                && (part.readahead.len() as u64) < FETCH_RECORDS as u64 / 2
                && let Some(end) = part.ahead_end()
                && end < high
            {
                Self::request_fetch(part, p, end, raw_sub, now, earliest, fetch);
            }
            part.cursor += 1;
            if let Some(t) = part.skip_before_ts {
                if msg.timestamp_ms < t {
                    part.acks.finish(msg.offset);
                    continue;
                }
                part.skip_before_ts = None;
            }
            if keyed && let Some(k) = &msg.key {
                if part.active_keys.contains(k) {
                    part.deferred.entry(k.clone()).or_default().push_back(msg);
                    part.deferred_len += 1;
                    continue;
                }
                part.active_keys.insert(k.clone());
            }
            return Some(Pending::new(msg));
        }
    }

    /// Records the delivery of `pendings` to `sub_id` as one run (consecutive ids, one
    /// deadline) and appends them to `out`. First-attempt publish timestamps go to `lat` and
    /// history updates to `hist`, for the caller to apply outside the group lock.
    #[allow(clippy::too_many_arguments)]
    fn deliver_run(
        &mut self,
        g: &Group,
        pol: &Policy<'_>,
        sub_id: u64,
        pendings: Vec<Pending>,
        now: Instant,
        now_ms: u64,
        lat: &mut Vec<u64>,
        hist: &mut HistoryOps,
        out: &mut Vec<SharedDelivery>,
    ) {
        let n = pendings.len() as u32;
        let first = self.next_delivery_id;
        self.next_delivery_id += u64::from(n);
        let sub = self
            .subs
            .get_mut(&sub_id)
            .expect("caller holds a valid subscription");
        let mut entries = Vec::with_capacity(pendings.len());
        let mut size = 0u64;
        let mut retries = 0u64;
        for (i, p) in pendings.into_iter().enumerate() {
            let attempt_no = p.attempts.len() as u32 + 1;
            if attempt_no == 1 && p.dlq_id.is_none() {
                hist.first.push(p.msg.partition, p.msg.offset);
            } else {
                hist.detailed.push((p.id(), attempt_no));
            }
            size += p.msg.payload.len() as u64;
            if attempt_no > 1 {
                retries += 1;
            } else if p.dlq_id.is_none() {
                lat.push(p.msg.timestamp_ms);
            }
            out.push(SharedDelivery {
                delivery_id: first + i as u64,
                attempt: attempt_no,
                message: p.msg.clone(),
            });
            entries.push(Some(Entry::from_pending(p)));
        }
        sub.in_flight += u64::from(n);
        sub.delivered += u64::from(n);
        sub.retries += retries;
        let deadline = (!sub.mode.cumulative).then(|| bucket(g.epoch, now + pol.ack_timeout()));
        let consumer = sub.instance.clone();
        g.counters.delivered.add(u64::from(n));
        hist.metrics.0 += u64::from(n);
        hist.metrics.1 += size;
        hist.metrics.2 += retries;
        self.in_flight += u64::from(n);
        if let Some(d) = deadline {
            self.deadlines.insert((d, first));
        }
        self.runs.insert(
            first,
            Run {
                sub: sub_id,
                consumer,
                delivered_at: now,
                delivered_at_ms: now_ms,
                deadline,
                len: n,
                live: n,
                body: Body::Msgs(entries),
                compact_queued: false,
            },
        );
    }

    /// Records a raw delivery (first attempts, consecutive ids) as one run.
    #[allow(clippy::too_many_arguments)]
    fn deliver_raw(
        &mut self,
        g: &Group,
        pol: &Policy<'_>,
        sub_id: u64,
        r: RawTake,
        now: Instant,
        now_ms: u64,
        hist: &mut HistoryOps,
    ) -> RawDelivery {
        let n = r.count;
        let first = self.next_delivery_id;
        self.next_delivery_id += u64::from(n);
        let sub = self
            .subs
            .get_mut(&sub_id)
            .expect("caller holds a valid subscription");
        sub.in_flight += u64::from(n);
        sub.delivered += u64::from(n);
        let deadline = (!sub.mode.cumulative).then(|| bucket(g.epoch, now + pol.ack_timeout()));
        let consumer = sub.instance.clone();
        g.counters.delivered.add(u64::from(n));
        hist.first
            .push_range(r.partition, r.from_offset, r.from_offset + u64::from(n));
        self.in_flight += u64::from(n);
        if let Some(d) = deadline {
            self.deadlines.insert((d, first));
        }
        let words = (n as usize).div_ceil(64);
        let mut live = vec![u64::MAX; words];
        if !n.is_multiple_of(64) {
            live[words - 1] = (1u64 << (n % 64)) - 1;
        }
        hist.metrics.0 += u64::from(n);
        hist.metrics.1 += r.envelopes.len() as u64;
        self.runs.insert(
            first,
            Run {
                sub: sub_id,
                consumer,
                delivered_at: now,
                delivered_at_ms: now_ms,
                deadline,
                len: n,
                live: n,
                compact_queued: false,
                body: Body::Raw {
                    partition: r.partition,
                    from_offset: r.from_offset,
                    envelopes: r.envelopes.clone(),
                    live,
                },
            },
        );
        RawDelivery {
            partition: r.partition,
            first_delivery_id: first,
            from_offset: r.from_offset,
            count: n,
            envelopes: r.envelopes,
        }
    }

    /// Marks a tracked offset of partition `partition` finished (below the cursor only).
    fn finish_offset(&mut self, partition: u32, offset: u64) {
        let pi = (partition as usize).min(self.parts.len().saturating_sub(1));
        let part = &mut self.parts[pi];
        if part.cursor != UNINIT && offset < part.cursor {
            part.acks.finish(offset);
        }
    }

    /// Marks a message finished for this group (acked or dead-lettered). Returns true when a
    /// deferred message of the same key became ready.
    fn complete(&mut self, keyed: bool, msg: &Message, tracked: bool) -> bool {
        if !tracked {
            self.oob_live = self.oob_live.saturating_sub(1);
            return false;
        }
        self.finish_offset(msg.partition, msg.offset);
        let pi = self.part_index(msg);
        let part = &mut self.parts[pi];
        if keyed && let Some(k) = &msg.key {
            let next = part.deferred.get_mut(k).and_then(|q| q.pop_front());
            match next {
                Some(next) => {
                    part.deferred_len -= 1;
                    if part.deferred.get(k).is_some_and(|q| q.is_empty()) {
                        part.deferred.remove(k);
                    }
                    part.ready.push_back(Pending::new(next));
                    return true;
                }
                None => {
                    part.active_keys.remove(k);
                }
            }
        }
        false
    }

    fn fail_attempt(
        &mut self,
        g: &Group,
        ch: &Channel,
        pol: &Policy<'_>,
        mut p: Pending,
        reason: &str,
        now: Instant,
    ) {
        let n = p.attempts.len() as u32;
        if n >= pol.retry.max_attempts {
            self.dead_letter(g, ch, pol, p, reason);
            return;
        }
        p.detach();
        let due = now + pol.backoff(n);
        self.retry_seq += 1;
        let seq = self.retry_seq;
        let pi = self.part_index(&p.msg);
        self.parts[pi].retry.insert((due, seq), p);
    }

    fn dead_letter(
        &mut self,
        g: &Group,
        ch: &Channel,
        pol: &Policy<'_>,
        mut p: Pending,
        reason: &str,
    ) {
        self.complete(g.keyed, &p.msg, p.tracked);
        g.counters.dead_lettered.inc();
        ch.metrics.dead_lettered.inc();
        if let Some(old) = p.dlq_id {
            ch.dlq.remove(old);
        }
        if pol.dlq_enabled {
            p.detach();
            let id = ch.next_dlq_id(p.msg.partition);
            ch.dlq.add(DeadLetter {
                id,
                channel: ch.name.clone(),
                group: g.name.to_string(),
                message: Arc::unwrap_or_clone(p.msg),
                attempts: p.attempts.into_records(),
                reason: reason.to_string(),
                dead_at_ms: now_ms(),
            });
        }
        ch.shared.note_dead_letter(&ch.name, &g.name, reason);
    }
}

/// Records first-attempt delivery latencies (publish timestamps in `ts_ms`) under one
/// acquisition of the channel's histogram lock.
fn record_delivery_latency(ch: &Channel, ts_ms: &[u64]) {
    if ts_ms.is_empty() {
        return;
    }
    let now = now_us();
    let mut h = ch.metrics.delivery_latency.lock();
    for &t in ts_ms {
        record(&mut h, now.saturating_sub(t.saturating_mul(1000)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(consumer: &str, at: u64) -> AttemptRec {
        AttemptRec {
            consumer: Arc::from(consumer),
            delivered_at_ms: at,
            finished_at_ms: None,
            outcome: AttemptOutcome::InFlight,
        }
    }

    #[test]
    fn attempts_number_and_finish_in_order() {
        let mut a = Attempts::default();
        assert!(a.is_empty());
        a.push(rec("c#1", 10));
        a.finish(
            11,
            AttemptOutcome::Nacked {
                reason: "boom".into(),
            },
        );
        a.push(rec("c#2", 20));
        a.push(rec("c#1", 30));
        a.finish(31, AttemptOutcome::TimedOut);
        assert_eq!(a.len(), 3);
        let r = a.into_records();
        assert_eq!(
            r.iter().map(|x| x.attempt).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(r[0].consumer, "c#1");
        assert_eq!(r[0].finished_at_ms, Some(11));
        assert_eq!(
            r[0].outcome,
            AttemptOutcome::Nacked {
                reason: "boom".into()
            }
        );
        assert_eq!(r[1].consumer, "c#2");
        assert_eq!(r[1].outcome, AttemptOutcome::InFlight);
        assert_eq!(r[2].outcome, AttemptOutcome::TimedOut);
        assert_eq!(r[2].delivered_at_ms, 30);
    }

    #[test]
    fn detach_copies_payload_once() {
        let frame = bytes::Bytes::from(vec![1u8; 1000]);
        let msg = Arc::new(Message {
            partition: 0,
            offset: 5,
            key: Some("k".into()),
            headers: Default::default(),
            timestamp_ms: 1,
            payload: frame.slice(10..20),
        });
        let mut p = Pending::new(msg.clone());
        p.detach();
        assert_eq!(*p.msg, *msg);
        assert_ne!(p.msg.payload.as_ptr(), msg.payload.as_ptr());
        let once = p.msg.clone();
        p.detach();
        assert!(Arc::ptr_eq(&once, &p.msg), "second detach is a no-op");
    }

    #[test]
    fn deadline_buckets_round_up() {
        let base = Instant::now();
        let t = base + Duration::from_millis(23);
        let b = bucket(base, t);
        assert!(b >= t);
        assert_eq!(b - base, Duration::from_millis(30));
        assert_eq!(
            bucket(base, base + Duration::from_millis(30)) - base,
            Duration::from_millis(30)
        );
    }
}

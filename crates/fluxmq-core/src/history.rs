//! Bounded, in-memory delivery-attempt history (the retry timeline in the message inspector).
//!
//! Keyed by [`MessageId`] → attempts of every group. Capacity is per channel; the map is
//! sharded by message id to keep lock hold times and contention low on the dispatch path.
//! When a shard is full, its oldest message (by first delivery) is evicted.
//!
//! Memory: this holds up to `attempt_history_capacity` messages per channel, so entries are
//! kept compact — one small `Vec` per message (group and consumer names are shared `Arc<str>`,
//! not per-attempt `String`s/maps). The proto [`DeliveryAttempt`] is only built on [`get`].
//!
//! v0.8: first attempts — nearly every delivery — store nothing per message. A delivery batch
//! records its first attempts as offset [`Span`]s (one entry per contiguous range of a
//! partition: group, consumer, delivered-at), an ack batch its acked first attempts the same
//! way, and [`get`] derives those attempts from the spans covering the id. Per-message entries
//! exist only for attempts that failed (nack, ack timeout, disconnect: [`on_failures`]
//! records the whole attempt), later attempts and DLQ retries. The spans are bounded like the
//! per-message map (at most `capacity` messages covered, oldest dropped first). What [`get`]
//! returns is unchanged.
//!
//! [`get`]: AttemptHistory::get
//! [`on_failures`]: AttemptHistory::on_failures

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use fluxmq_proto::model::{AttemptOutcome, DeliveryAttempt, MessageId};
use parking_lot::Mutex;

const SHARDS: usize = 16;

#[derive(Debug, Clone)]
enum Outcome {
    InFlight,
    Acked,
    Nacked(Box<str>),
    TimedOut,
}

impl From<AttemptOutcome> for Outcome {
    fn from(o: AttemptOutcome) -> Self {
        match o {
            AttemptOutcome::InFlight => Outcome::InFlight,
            AttemptOutcome::Acked => Outcome::Acked,
            AttemptOutcome::Nacked { reason } => Outcome::Nacked(reason.into_boxed_str()),
            AttemptOutcome::TimedOut => Outcome::TimedOut,
        }
    }
}

impl From<&Outcome> for AttemptOutcome {
    fn from(o: &Outcome) -> Self {
        match o {
            Outcome::InFlight => AttemptOutcome::InFlight,
            Outcome::Acked => AttemptOutcome::Acked,
            Outcome::Nacked(r) => AttemptOutcome::Nacked {
                reason: r.to_string(),
            },
            Outcome::TimedOut => AttemptOutcome::TimedOut,
        }
    }
}

/// One attempt of one group (attempts of a message are stored in delivery order).
#[derive(Debug, Clone)]
struct Attempt {
    group: Arc<str>,
    consumer: Arc<str>,
    attempt: u32,
    delivered_at_ms: u64,
    finished_at_ms: Option<u64>,
    outcome: Outcome,
}

fn shard_index(id: MessageId) -> usize {
    let h = id.offset.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ u64::from(id.partition);
    ((h >> 32) as usize) % SHARDS
}

/// Multiplicative hash for [`MessageId`] keys (two integers; no need for SipHash's DoS
/// resistance: ids are broker-assigned).
#[derive(Default, Clone, Copy)]
struct IdHasher(u64);

impl std::hash::Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u64(u64::from(b));
        }
    }
    fn write_u32(&mut self, i: u32) {
        self.write_u64(u64::from(i));
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x51_7C_C1_B7_27_22_0A_95);
    }
}

type IdMap<V> = HashMap<MessageId, V, std::hash::BuildHasherDefault<IdHasher>>;

#[derive(Default)]
struct Shard {
    map: IdMap<Vec<Attempt>>,
    order: VecDeque<MessageId>,
}

/// Offsets `from .. to` of `partition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub partition: u32,
    pub from: u64,
    pub to: u64,
}

impl Span {
    fn contains(&self, id: MessageId) -> bool {
        self.partition == id.partition && id.offset >= self.from && id.offset < self.to
    }

    fn len(&self) -> usize {
        (self.to - self.from) as usize
    }
}

/// Builds [`Span`]s from ids pushed in delivery order (offsets ascending per partition,
/// partitions interleaved round-robin): an id extends a recent span of its partition when
/// contiguous, so a batch costs one entry per partition, not one per message.
#[derive(Debug, Default)]
pub struct Spans(Vec<Span>);

impl Spans {
    /// How far back [`Spans::push_range`] looks for a span to extend (one per partition a
    /// consumer takes from round-robin).
    const LOOKBACK: usize = 16;

    pub fn push(&mut self, partition: u32, offset: u64) {
        self.push_range(partition, offset, offset + 1);
    }

    pub fn push_range(&mut self, partition: u32, from: u64, to: u64) {
        if from >= to {
            return;
        }
        for s in self.0.iter_mut().rev().take(Self::LOOKBACK) {
            if s.partition == partition && s.to == from {
                s.to = to;
                return;
            }
        }
        self.0.push(Span {
            partition,
            from,
            to,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[Span] {
        &self.0
    }
}

/// First attempts of one delivery batch: `span` delivered to `consumer` of `group`.
struct DeliveredSpan {
    group: Arc<str>,
    consumer: Arc<str>,
    span: Span,
    at_ms: u64,
}

/// First attempts of `span` acked by `group`.
struct AckedSpan {
    group: Arc<str>,
    span: Span,
    at_ms: u64,
}

/// Span records, each ring bounded by the messages its spans cover.
#[derive(Default)]
struct FirstAttempts {
    delivered: VecDeque<DeliveredSpan>,
    delivered_n: usize,
    acked: VecDeque<AckedSpan>,
    acked_n: usize,
}

impl FirstAttempts {
    fn trim(&mut self, cap: usize) {
        while self.delivered_n > cap {
            let Some(d) = self.delivered.pop_front() else {
                break;
            };
            self.delivered_n -= d.span.len();
        }
        while self.acked_n > cap {
            let Some(a) = self.acked.pop_front() else {
                break;
            };
            self.acked_n -= a.span.len();
        }
    }
}

/// A failed attempt with what [`AttemptHistory::on_failures`] needs to record it whole.
pub struct Failed {
    pub id: MessageId,
    pub attempt: u32,
    pub consumer: Arc<str>,
    pub delivered_at_ms: u64,
}

pub struct AttemptHistory {
    shards: Vec<Mutex<Shard>>,
    cap_per_shard: usize,
    capacity: usize,
    first: Mutex<FirstAttempts>,
}

impl AttemptHistory {
    pub fn new(capacity: usize) -> Self {
        let cap_per_shard = if capacity == 0 {
            0
        } else {
            capacity.div_ceil(SHARDS)
        };
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            cap_per_shard,
            capacity,
            first: Mutex::new(FirstAttempts::default()),
        }
    }

    fn shard(&self, id: MessageId) -> &Mutex<Shard> {
        &self.shards[shard_index(id)]
    }

    /// Adds `a` to the attempts of `id` (evicting the shard's oldest message when full).
    fn insert(&self, s: &mut Shard, id: MessageId, a: Attempt) {
        if !s.map.contains_key(&id) {
            while s.map.len() >= self.cap_per_shard {
                match s.order.pop_front() {
                    Some(old) => {
                        s.map.remove(&old);
                    }
                    None => break,
                }
            }
            s.order.push_back(id);
        }
        s.map
            .entry(id)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(a);
    }

    /// Records a new (in-flight) delivery of `id` to `consumer` in `group`.
    #[cfg(test)]
    pub fn on_delivery(
        &self,
        id: MessageId,
        group: &Arc<str>,
        consumer: &Arc<str>,
        attempt: u32,
        delivered_at_ms: u64,
    ) {
        if attempt == 1 {
            let mut spans = Spans::default();
            spans.push(id.partition, id.offset);
            self.on_deliveries(&spans, &[], group, consumer, delivered_at_ms);
        } else {
            self.on_deliveries(
                &Spans::default(),
                &[(id, attempt)],
                group,
                consumer,
                delivered_at_ms,
            );
        }
    }

    /// Several messages delivered at once to one consumer: first attempts as `first` spans
    /// (one record per span), every other attempt (redeliveries, DLQ retries) in `detailed`
    /// (one record per message; each shard locked once).
    pub fn on_deliveries(
        &self,
        first: &Spans,
        detailed: &[(MessageId, u32)],
        group: &Arc<str>,
        consumer: &Arc<str>,
        delivered_at_ms: u64,
    ) {
        if self.cap_per_shard == 0 {
            return;
        }
        if !first.is_empty() {
            let mut f = self.first.lock();
            for s in first.as_slice() {
                f.delivered_n += s.len();
                f.delivered.push_back(DeliveredSpan {
                    group: group.clone(),
                    consumer: consumer.clone(),
                    span: *s,
                    at_ms: delivered_at_ms,
                });
            }
            f.trim(self.capacity);
        }
        if detailed.is_empty() {
            return;
        }
        self.for_each_shard(
            detailed,
            |d| d.0,
            |s, &(id, attempt)| {
                self.insert(
                    s,
                    id,
                    Attempt {
                        group: group.clone(),
                        consumer: consumer.clone(),
                        attempt,
                        delivered_at_ms,
                        finished_at_ms: None,
                        outcome: Outcome::InFlight,
                    },
                );
            },
        );
    }

    /// Acks: first attempts as `first` spans, other attempts in `detailed`.
    pub fn on_acks(
        &self,
        first: &Spans,
        detailed: &[(MessageId, u32)],
        group: &Arc<str>,
        finished_at_ms: u64,
    ) {
        if self.cap_per_shard == 0 {
            return;
        }
        if !first.is_empty() {
            let mut f = self.first.lock();
            for s in first.as_slice() {
                f.acked_n += s.len();
                f.acked.push_back(AckedSpan {
                    group: group.clone(),
                    span: *s,
                    at_ms: finished_at_ms,
                });
            }
            f.trim(self.capacity);
        }
        if detailed.is_empty() {
            return;
        }
        self.for_each_shard(
            detailed,
            |d| d.0,
            |s, &(id, attempt)| {
                if let Some(a) = in_flight_attempt(s, id, group, attempt) {
                    a.finished_at_ms = Some(finished_at_ms);
                    a.outcome = Outcome::Acked;
                }
            },
        );
    }

    /// Failed attempts (nack, ack timeout, disconnect), all with `outcome`. An attempt
    /// recorded in flight is finished; one known only from its delivery span (a first
    /// attempt) or evicted is recorded whole.
    pub fn on_failures(
        &self,
        failed: &[Failed],
        group: &Arc<str>,
        finished_at_ms: u64,
        outcome: &AttemptOutcome,
    ) {
        if self.cap_per_shard == 0 || failed.is_empty() {
            return;
        }
        let outcome: Outcome = outcome.clone().into();
        self.for_each_shard(
            failed,
            |f| f.id,
            |s, f| {
                if let Some(a) = in_flight_attempt(s, f.id, group, f.attempt) {
                    a.finished_at_ms = Some(finished_at_ms);
                    a.outcome = outcome.clone();
                    return;
                }
                self.insert(
                    s,
                    f.id,
                    Attempt {
                        group: group.clone(),
                        consumer: f.consumer.clone(),
                        attempt: f.attempt,
                        delivered_at_ms: f.delivered_at_ms,
                        finished_at_ms: Some(finished_at_ms),
                        outcome: outcome.clone(),
                    },
                );
            },
        );
    }

    /// Calls `f` for every item with the shard of its id locked, locking each shard once
    /// (items of a shard keep their relative order).
    fn for_each_shard<T>(
        &self,
        items: &[T],
        key: impl Fn(&T) -> MessageId,
        mut f: impl FnMut(&mut Shard, &T),
    ) {
        if let [one] = items {
            f(&mut self.shard(key(one)).lock(), one);
            return;
        }
        let mut order: Vec<(u8, u32)> = items
            .iter()
            .enumerate()
            .map(|(i, t)| (shard_index(key(t)) as u8, i as u32))
            .collect();
        order.sort_unstable();
        let mut i = 0;
        while i < order.len() {
            let shard = order[i].0;
            let mut s = self.shards[shard as usize].lock();
            while i < order.len() && order[i].0 == shard {
                f(&mut s, &items[order[i].1 as usize]);
                i += 1;
            }
        }
    }

    /// Attempts of `id` per group, each list in delivery order.
    pub fn get(&self, id: MessageId) -> BTreeMap<String, Vec<DeliveryAttempt>> {
        let mut out: BTreeMap<String, Vec<DeliveryAttempt>> = BTreeMap::new();
        // Per-message records (failed attempts recorded whole, later attempts, DLQ retries).
        let recorded: Vec<Attempt> = {
            let s = self.shard(id).lock();
            s.map.get(&id).cloned().unwrap_or_default()
        };
        {
            let f = self.first.lock();
            for d in f.delivered.iter().filter(|d| d.span.contains(id)) {
                // A failed first attempt is recorded whole; don't list it twice.
                let dup = recorded.iter().any(|a| {
                    a.attempt == 1
                        && a.group == d.group
                        && a.delivered_at_ms == d.at_ms
                        && a.consumer == d.consumer
                });
                if dup {
                    continue;
                }
                let ack = f
                    .acked
                    .iter()
                    .find(|a| a.group == d.group && a.at_ms >= d.at_ms && a.span.contains(id));
                out.entry(d.group.to_string())
                    .or_default()
                    .push(DeliveryAttempt {
                        attempt: 1,
                        consumer: d.consumer.to_string(),
                        delivered_at_ms: d.at_ms,
                        finished_at_ms: ack.map(|a| a.at_ms),
                        outcome: if ack.is_some() {
                            AttemptOutcome::Acked
                        } else {
                            AttemptOutcome::InFlight
                        },
                    });
            }
        }
        for a in &recorded {
            out.entry(a.group.to_string())
                .or_default()
                .push(DeliveryAttempt {
                    attempt: a.attempt,
                    consumer: a.consumer.to_string(),
                    delivered_at_ms: a.delivered_at_ms,
                    finished_at_ms: a.finished_at_ms,
                    outcome: (&a.outcome).into(),
                });
        }
        for v in out.values_mut() {
            // Delivery order (stable: equal instants keep span-then-record order).
            v.sort_by_key(|a| (a.delivered_at_ms, a.attempt));
        }
        out
    }
}

/// The latest still-in-flight attempt number `attempt` of `id` for `group`.
fn in_flight_attempt<'a>(
    s: &'a mut Shard,
    id: MessageId,
    group: &Arc<str>,
    attempt: u32,
) -> Option<&'a mut Attempt> {
    s.map.get_mut(&id).and_then(|v| {
        v.iter_mut().rev().find(|a| {
            a.attempt == attempt && a.group == *group && matches!(a.outcome, Outcome::InFlight)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mid(offset: u64) -> MessageId {
        MessageId {
            partition: 0,
            offset,
        }
    }

    fn failed(id: MessageId, attempt: u32, c: &Arc<str>, at: u64) -> Failed {
        Failed {
            id,
            attempt,
            consumer: c.clone(),
            delivered_at_ms: at,
        }
    }

    #[test]
    fn records_and_evicts() {
        let h = AttemptHistory::new(SHARDS); // one per shard
        let g: Arc<str> = Arc::from("g");
        let g2: Arc<str> = Arc::from("g2");
        let c: Arc<str> = Arc::from("c#1");
        let id = mid(1);
        h.on_delivery(id, &g, &c, 1, 1);
        h.on_delivery(id, &g2, &c, 1, 2);
        h.on_failures(
            &[failed(id, 1, &c, 1)],
            &g,
            5,
            &AttemptOutcome::Nacked { reason: "x".into() },
        );
        h.on_delivery(id, &g, &c, 2, 6);
        let mut acked = Spans::default();
        acked.push(0, 1);
        h.on_acks(&acked, &[], &g2, 7);
        let got = h.get(id);
        assert_eq!(got["g"].len(), 2);
        assert_eq!(got["g"][0].finished_at_ms, Some(5));
        assert_eq!(
            got["g"][0].outcome,
            AttemptOutcome::Nacked { reason: "x".into() }
        );
        assert_eq!(got["g"][0].delivered_at_ms, 1);
        assert_eq!(got["g"][1].attempt, 2);
        assert_eq!(got["g"][1].outcome, AttemptOutcome::InFlight);
        assert_eq!(got["g"][0].consumer, "c#1");
        assert_eq!(got["g2"].len(), 1);
        assert_eq!(got["g2"][0].outcome, AttemptOutcome::Acked);
        assert_eq!(got["g2"][0].finished_at_ms, Some(7));
        // The second attempt acked (detailed path), then a timeout of a third.
        h.on_acks(&Spans::default(), &[(id, 2)], &g, 8);
        assert_eq!(h.get(id)["g"][1].outcome, AttemptOutcome::Acked);
        // Fill far beyond capacity: total entries stay bounded.
        for o in 0..1000 {
            h.on_delivery(mid(1000 + o), &g, &c, 2, 1);
            h.on_delivery(mid(5000 + 2 * o), &g, &c, 1, 1);
        }
        let total: usize = h.shards.iter().map(|s| s.lock().map.len()).sum();
        assert!(total <= SHARDS);
        let f = h.first.lock();
        assert!(f.delivered_n <= SHARDS && f.delivered.len() <= SHARDS);
        drop(f);
        assert!(AttemptHistory::new(0).get(id).is_empty());
    }

    /// The span-based records answer `get` exactly like per-message records did.
    #[test]
    fn spans_answer_like_per_message_records() {
        let h = AttemptHistory::new(100_000);
        let g: Arc<str> = Arc::from("g");
        let c: Arc<str> = Arc::from("c#1");
        // One batch over 3 partitions, round-robin.
        let mut d = Spans::default();
        for o in 10..20u64 {
            for p in 0..3u32 {
                d.push(p, o);
            }
        }
        assert_eq!(d.as_slice().len(), 3);
        h.on_deliveries(&d, &[], &g, &c, 100);
        // Acks of partition 1 only, then a timeout of offset 15 on partition 2.
        let mut a = Spans::default();
        a.push_range(1, 10, 20);
        h.on_acks(&a, &[], &g, 150);
        let id = |p, o| MessageId {
            partition: p,
            offset: o,
        };
        h.on_failures(
            &[failed(id(2, 15), 1, &c, 100)],
            &g,
            160,
            &AttemptOutcome::TimedOut,
        );
        let acked = h.get(id(1, 12));
        assert_eq!(acked["g"].len(), 1);
        assert_eq!(acked["g"][0].outcome, AttemptOutcome::Acked);
        assert_eq!(acked["g"][0].delivered_at_ms, 100);
        assert_eq!(acked["g"][0].finished_at_ms, Some(150));
        let open = h.get(id(0, 19));
        assert_eq!(open["g"][0].outcome, AttemptOutcome::InFlight);
        assert_eq!(open["g"][0].finished_at_ms, None);
        let timed = h.get(id(2, 15));
        assert_eq!(timed["g"].len(), 1, "a failed first attempt is listed once");
        assert_eq!(timed["g"][0].outcome, AttemptOutcome::TimedOut);
        assert_eq!(timed["g"][0].consumer, "c#1");
        assert!(h.get(id(0, 20)).is_empty());
        assert!(h.get(id(3, 12)).is_empty());
        // A DLQ retry of an exhausted message: attempts restart at 1 (detailed record).
        h.on_deliveries(&Spans::default(), &[(id(2, 15), 1)], &g, &c, 200);
        h.on_acks(&Spans::default(), &[(id(2, 15), 1)], &g, 210);
        let t = h.get(id(2, 15));
        assert_eq!(t["g"].len(), 2);
        assert_eq!(t["g"][0].outcome, AttemptOutcome::TimedOut);
        assert_eq!(t["g"][1].outcome, AttemptOutcome::Acked);
        assert_eq!(t["g"][1].delivered_at_ms, 200);
    }
}

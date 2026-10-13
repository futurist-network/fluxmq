//! Range-based acknowledgement state of one (group, partition) (v0.7).
//!
//! Instead of a per-message set of unfinished offsets, a partition keeps:
//! * `committed` — the low watermark: every offset below it is finished (acked or
//!   dead-lettered, or skipped);
//! * `done` — a compact set of finished offsets above the watermark, as disjoint half-open
//!   ranges. Acks arrive mostly in order, so `done` is almost always empty or tiny and the
//!   common ack is a single increment of `committed`.
//!
//! Unfinished ("pending") offsets are `[committed, cursor) \ done`, where `cursor` is the
//! group's next never-dispatched offset (owned by the caller). Both structures are O(number of
//! gaps) in memory regardless of how many messages are in flight.
//!
//! The checkpoint persists `committed` plus `done` (bounded, see [`AckState::checkpoint`]), so a
//! restart redelivers exactly the messages that were not finished.

use std::collections::BTreeMap;

/// Disjoint, non-adjacent half-open ranges `[start, end)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeSet {
    /// start → end.
    ranges: BTreeMap<u64, u64>,
    /// Number of offsets covered.
    len: u64,
}

impl RangeSet {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    /// Offsets covered.
    pub fn len(&self) -> u64 {
        self.len
    }

    #[cfg(test)]
    /// Number of disjoint ranges.
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    #[cfg(test)]
    pub fn contains(&self, o: u64) -> bool {
        self.range_containing(o).is_some()
    }

    /// The range holding `o`, if any.
    pub fn range_containing(&self, o: u64) -> Option<(u64, u64)> {
        let (&s, &e) = self.ranges.range(..=o).next_back()?;
        (o < e).then_some((s, e))
    }

    pub fn first(&self) -> Option<(u64, u64)> {
        self.ranges.first_key_value().map(|(&s, &e)| (s, e))
    }

    pub fn pop_first(&mut self) -> Option<(u64, u64)> {
        let (s, e) = self.ranges.pop_first()?;
        self.len -= e - s;
        Some((s, e))
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.ranges.iter().map(|(&s, &e)| (s, e))
    }

    /// Adds `[lo, hi)`; returns how many offsets were newly added.
    pub fn insert(&mut self, lo: u64, hi: u64) -> u64 {
        if lo >= hi {
            return 0;
        }
        let mut start = lo;
        let mut end = hi;
        let mut covered = 0u64;
        // A range starting at or before `lo` that touches it.
        if let Some((&s, &e)) = self.ranges.range(..=lo).next_back()
            && e >= lo
        {
            if e >= hi {
                return 0;
            }
            start = s;
            covered += e - lo.max(s);
            end = end.max(e);
            self.ranges.remove(&s);
        }
        // Ranges starting inside (lo, hi] are swallowed.
        while let Some((&s, &e)) = self.ranges.range(lo..=end).next() {
            covered += e.min(hi).saturating_sub(s.max(lo));
            end = end.max(e);
            self.ranges.remove(&s);
        }
        self.ranges.insert(start, end);
        let added = (hi - lo) - covered;
        self.len += added;
        added
    }

    /// Removes everything below `o`.
    pub fn remove_below(&mut self, o: u64) {
        while let Some((&s, &e)) = self.ranges.first_key_value() {
            if s >= o {
                break;
            }
            self.ranges.remove(&s);
            if e > o {
                self.ranges.insert(o, e);
                self.len -= o - s;
                break;
            }
            self.len -= e - s;
        }
    }

    /// Offsets covered in `[lo, hi)`.
    pub fn count_in(&self, lo: u64, hi: u64) -> u64 {
        if lo >= hi {
            return 0;
        }
        let mut n = 0;
        if let Some((&s, &e)) = self.ranges.range(..lo).next_back()
            && e > lo
        {
            n += e.min(hi) - lo;
            let _ = s;
        }
        for (&s, &e) in self.ranges.range(lo..hi) {
            n += e.min(hi) - s;
        }
        n
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
        self.len = 0;
    }
}

/// Committed low watermark + finished offsets above it, for one (group, partition).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AckState {
    committed: u64,
    done: RangeSet,
}

impl AckState {
    pub fn new(committed: u64) -> Self {
        Self {
            committed,
            done: RangeSet::new(),
        }
    }

    /// Every offset below this is finished.
    pub fn committed(&self) -> u64 {
        self.committed
    }

    #[cfg(test)]
    pub fn is_done(&self, o: u64) -> bool {
        o < self.committed || self.done.contains(o)
    }

    /// Restarts at `committed` with nothing finished above it.
    pub fn reset(&mut self, committed: u64) {
        self.committed = committed;
        self.done.clear();
    }

    /// Marks `o` finished. Returns false when it already was.
    pub fn finish(&mut self, o: u64) -> bool {
        self.finish_range(o, o + 1) > 0
    }

    /// Marks `[lo, hi)` finished; returns how many offsets were newly finished.
    pub fn finish_range(&mut self, lo: u64, hi: u64) -> u64 {
        let lo = lo.max(self.committed);
        if lo >= hi {
            return 0;
        }
        if lo == self.committed {
            // In-order fast path: move the watermark, then absorb a touching done range.
            let already = self.done.count_in(lo, hi);
            self.done.remove_below(hi);
            self.committed = hi;
            if let Some((s, e)) = self.done.first()
                && s == self.committed
            {
                self.done.pop_first();
                self.committed = e;
            }
            return (hi - lo) - already;
        }
        self.done.insert(lo, hi)
    }

    /// Unfinished offsets below `cursor` (`cursor >= committed`, `done` below `cursor`).
    pub fn pending(&self, cursor: u64) -> u64 {
        cursor.saturating_sub(self.committed) - self.done.count_in(self.committed, cursor)
    }

    /// Checkpoint form: `committed` plus at most `max_ranges` done ranges (the lowest ones;
    /// dropping higher ranges only means those messages are redelivered after a restart).
    pub fn checkpoint(&self, max_ranges: usize) -> (u64, Vec<(u64, u64)>) {
        (self.committed, self.done.iter().take(max_ranges).collect())
    }
}

/// Tiny deterministic PRNG for property tests (no external dependency).
#[cfg(test)]
pub(crate) struct XorShift(pub u64);

#[cfg(test)]
impl XorShift {
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn rangeset_merges() {
        let mut r = RangeSet::new();
        assert_eq!(r.insert(5, 10), 5);
        assert_eq!(r.insert(12, 15), 3);
        assert_eq!(r.insert(10, 12), 2);
        assert_eq!(r.range_count(), 1);
        assert_eq!(r.first(), Some((5, 15)));
        assert_eq!(r.insert(0, 20), 10);
        assert_eq!(r.len(), 20);
        assert_eq!(r.insert(3, 4), 0);
        r.remove_below(7);
        assert_eq!(r.first(), Some((7, 20)));
        assert_eq!(r.len(), 13);
        assert_eq!(r.count_in(0, 8), 1);
        assert_eq!(r.count_in(19, 100), 1);
    }

    #[test]
    fn rangeset_matches_naive_model() {
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        for _ in 0..200 {
            let mut r = RangeSet::new();
            let mut m = BTreeSet::new();
            for _ in 0..200 {
                match rng.below(10) {
                    0..=6 => {
                        let lo = rng.below(300);
                        let hi = lo + rng.below(12);
                        let before = m.len();
                        m.extend(lo..hi);
                        assert_eq!(r.insert(lo, hi), (m.len() - before) as u64);
                    }
                    7 => {
                        let o = rng.below(300);
                        m.retain(|&x| x >= o);
                        r.remove_below(o);
                    }
                    _ => {
                        let lo = rng.below(300);
                        let hi = lo + rng.below(50);
                        assert_eq!(r.count_in(lo, hi), m.range(lo..hi).count() as u64);
                    }
                }
                assert_eq!(r.len(), m.len() as u64);
                for o in 0..320 {
                    assert_eq!(r.contains(o), m.contains(&o), "offset {o}");
                }
                // Disjoint and non-adjacent.
                let v: Vec<_> = r.iter().collect();
                for w in v.windows(2) {
                    assert!(w[0].1 < w[1].0, "{v:?}");
                }
            }
        }
    }

    /// Random finish sequences vs a naive set of unfinished offsets.
    #[test]
    fn ackstate_matches_naive_model() {
        let mut rng = XorShift(42);
        for _ in 0..300 {
            let base = rng.below(1000);
            let cursor = base + rng.below(400);
            let mut a = AckState::new(base);
            let mut pending: BTreeSet<u64> = (base..cursor).collect();
            for _ in 0..300 {
                let lo = base + rng.below(cursor - base + 5);
                let span = if rng.below(4) == 0 { 30 } else { 1 };
                let hi = lo + 1 + rng.below(span);
                let hi = hi.min(cursor);
                let expect = pending.range(lo..hi.max(lo)).count() as u64;
                let got = a.finish_range(lo, hi);
                assert_eq!(got, expect);
                for o in lo..hi {
                    pending.remove(&o);
                }
                let committed = pending.first().copied().unwrap_or(cursor);
                assert!(a.committed() <= committed);
                // Committed is exactly the lowest unfinished offset once something below
                // the cursor is pending; when nothing is pending everything up to the last
                // finished offset is committed.
                if pending.is_empty() {
                    assert!(a.pending(cursor) == 0);
                } else {
                    assert_eq!(a.committed(), committed);
                }
                assert_eq!(a.pending(cursor), pending.len() as u64);
                for o in base..cursor {
                    assert_eq!(a.is_done(o), !pending.contains(&o));
                }
            }
            // Checkpoint round trip: restoring committed + ranges gives the same pending set.
            let (c, ranges) = a.checkpoint(usize::MAX);
            let mut b = AckState::new(c);
            for (s, e) in ranges {
                b.finish_range(s, e);
            }
            assert_eq!(b, a);
        }
    }
}

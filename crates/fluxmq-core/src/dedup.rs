//! Idempotent publishing (v0.12): a producer's retried batch is answered with the offsets of
//! the copy already stored instead of being stored again.
//!
//! The SDK marks the first record of every `PublishBatch` with
//! `flux-pseq: <producer id>.<batch sequence>` (one id per client, one sequence per batch,
//! unchanged across retries). Per partition, the leader keeps the last
//! [`RECENT_PER_PRODUCER`] batches of each producer: a batch whose (producer, sequence) is
//! already stored gets the original `(base_offset, count)`; one still being written waits for
//! that write and shares its result (a failed write lets the retry append).
//!
//! The table lives in memory. After a restart or a leader change it is rebuilt from the tail
//! of the partition's log (the batches of the last [`REBUILD_WINDOW_MS`], at most
//! [`REBUILD_MAX_BYTES`] read), so a retry that crosses a failover is recognized too, as long
//! as its batch is within that tail. Batches without the header (other clients, older SDKs)
//! are not affected; legacy (non-batch) records are not indexed.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::watch;

/// Remembered batches per producer and partition (in-flight pipelining depth is far lower).
pub const RECENT_PER_PRODUCER: usize = 64;
/// Producers remembered per partition; the least recently seen are forgotten beyond it.
pub const MAX_PRODUCERS: usize = 20_000;
/// A producer silent for this long is forgotten.
pub const PRODUCER_IDLE: Duration = Duration::from_secs(15 * 60);
/// Rebuild after a restart / leader change: batches appended within this window (twice the
/// SDK's default publish timeout).
pub const REBUILD_WINDOW_MS: u64 = 60_000;
/// Rebuild reads at most this much of the log's tail.
pub const REBUILD_MAX_BYTES: usize = 256 << 20;

/// Outcome shared with retries waiting on an in-flight original.
pub type Outcome = Option<Result<(u64, u32), ()>>;

enum Slot {
    Done { base: u64, count: u32 },
    Pending(watch::Receiver<Outcome>),
}

struct Producer {
    recent: VecDeque<(u64, Slot)>,
    last: Instant,
}

/// What a publish with a producer mark should do.
pub enum Begin {
    /// Not seen: append, then [`DedupTable::finish`] and send the outcome on the sender.
    Fresh(watch::Sender<Outcome>),
    /// Already stored at these offsets.
    Done(u64, u32),
    /// The original is being written: wait for its outcome.
    Wait(watch::Receiver<Outcome>),
}

/// One partition's table.
#[derive(Default)]
pub struct DedupTable {
    /// Leadership epoch the table was built for (`None` outside replicated mode).
    pub(crate) epoch: Option<u64>,
    /// False until rebuilt from the log for `epoch`.
    pub(crate) ready: bool,
    producers: HashMap<u64, Producer>,
}

impl DedupTable {
    /// Starts over for a new leadership epoch (rebuild pending).
    pub fn reset(&mut self, epoch: Option<u64>) {
        self.epoch = epoch;
        self.ready = false;
        self.producers.clear();
    }

    pub fn len(&self) -> usize {
        self.producers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.producers.is_empty()
    }

    fn producer(&mut self, pid: u64, now: Instant) -> &mut Producer {
        if !self.producers.contains_key(&pid) && self.producers.len() >= MAX_PRODUCERS {
            self.evict(now);
        }
        let p = self.producers.entry(pid).or_insert_with(|| Producer {
            recent: VecDeque::new(),
            last: now,
        });
        p.last = now;
        p
    }

    /// Forgets idle producers, or the least recently seen tenth when none is idle.
    fn evict(&mut self, now: Instant) {
        let before = self.producers.len();
        self.producers
            .retain(|_, p| now.duration_since(p.last) < PRODUCER_IDLE);
        if self.producers.len() == before {
            let mut ages: Vec<(Instant, u64)> =
                self.producers.iter().map(|(id, p)| (p.last, *id)).collect();
            ages.sort_unstable();
            for (_, id) in ages.into_iter().take(before / 10 + 1) {
                self.producers.remove(&id);
            }
        }
    }

    fn push(p: &mut Producer, seq: u64, slot: Slot) {
        if p.recent.len() >= RECENT_PER_PRODUCER {
            p.recent.pop_front();
        }
        p.recent.push_back((seq, slot));
    }

    pub fn begin(&mut self, pid: u64, seq: u64, now: Instant) -> Begin {
        let p = self.producer(pid, now);
        if let Some((_, slot)) = p.recent.iter().find(|(s, _)| *s == seq) {
            return match slot {
                Slot::Done { base, count } => Begin::Done(*base, *count),
                Slot::Pending(rx) => Begin::Wait(rx.clone()),
            };
        }
        let (tx, rx) = watch::channel(None);
        Self::push(p, seq, Slot::Pending(rx));
        Begin::Fresh(tx)
    }

    /// Settles a [`Begin::Fresh`] batch: stored at `Some((base, count))`, or failed (`None`:
    /// forgotten, so a retry appends).
    pub fn finish(&mut self, pid: u64, seq: u64, stored: Option<(u64, u32)>) {
        let Some(p) = self.producers.get_mut(&pid) else {
            return;
        };
        let Some(i) = p.recent.iter().position(|(s, _)| *s == seq) else {
            return;
        };
        match stored {
            Some((base, count)) => p.recent[i].1 = Slot::Done { base, count },
            None => {
                p.recent.remove(i);
            }
        }
    }

    /// A batch found in the log while rebuilding.
    pub fn record(&mut self, pid: u64, seq: u64, base: u64, count: u32, now: Instant) {
        let p = self.producer(pid, now);
        if !p.recent.iter().any(|(s, _)| *s == seq) {
            Self::push(p, seq, Slot::Done { base, count });
        }
    }
}

/// Producer marks of the batches in the tail of `log`: `(producer, sequence, base, count)`,
/// oldest first. Reads the batches appended in the last [`REBUILD_WINDOW_MS`] (or, when that is
/// more than [`REBUILD_MAX_BYTES`], the last few seconds). Blocking.
pub fn scan_tail(
    log: &crate::storage::PartitionLog,
    now_ms: u64,
) -> std::io::Result<Vec<(u64, u64, u64, u32)>> {
    use fluxmq_proto::batch::{self, Compression, ENVELOPE_HEADER_LEN, EnvelopeHeader, codec};
    let high = log.high();
    let mut from = log
        .offset_for_time_approx(now_ms.saturating_sub(REBUILD_WINDOW_MS))
        .max(log.low());
    let mut out = Vec::new();
    let mut read = 0usize;
    let mut narrowed = false;
    while from < high {
        let chunk = match log.read_envelopes(from, 4 << 20) {
            Ok(Some(c)) => c,
            Ok(None) => break,
            // Retention removed the segment under us: continue from what is left.
            Err(e) if e.kind() == std::io::ErrorKind::InvalidInput && from < log.low() => {
                from = log.low();
                continue;
            }
            Err(e) => return Err(e),
        };
        read += chunk.bytes.len();
        if read > REBUILD_MAX_BYTES {
            if narrowed {
                break;
            }
            // Too busy a partition for the whole window: keep only its last seconds.
            narrowed = true;
            out.clear();
            read = 0;
            from = log
                .offset_for_time_approx(now_ms.saturating_sub(5_000))
                .max(chunk.next_offset.min(high));
            continue;
        }
        let bytes = &chunk.bytes[..];
        let mut pos = 0usize;
        while pos + 4 <= bytes.len() {
            let first = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4 bytes"));
            if !batch::is_batch(first) {
                pos += crate::storage::record::LEN_PREFIX + first as usize;
                continue;
            }
            let Some(h) = EnvelopeHeader::parse(&bytes[pos..]) else {
                break;
            };
            let end = pos + h.total_len();
            if end > bytes.len() {
                break;
            }
            let body = &bytes[pos + ENVELOPE_HEADER_LEN..end];
            let section = match h.compression {
                Compression::None => std::borrow::Cow::Borrowed(body),
                c => match codec::decompress(c, body, batch::MAX_BATCH_LEN) {
                    Ok(v) => std::borrow::Cow::Owned(v),
                    Err(_) => {
                        pos = end;
                        continue;
                    }
                },
            };
            if let Some((pid, seq)) = batch::section_pseq(&section, h.record_count) {
                out.push((pid, seq, h.base_offset, h.record_count));
            }
            pos = end;
        }
        from = chunk.next_offset;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_of_stored_batch_gets_original_offsets() {
        let mut t = DedupTable::default();
        let now = Instant::now();
        let Begin::Fresh(tx) = t.begin(7, 1, now) else {
            panic!("fresh")
        };
        // A retry while the original is in flight waits for it.
        let Begin::Wait(rx) = t.begin(7, 1, now) else {
            panic!("wait")
        };
        t.finish(7, 1, Some((100, 5)));
        tx.send_replace(Some(Ok((100, 5))));
        assert_eq!(*rx.borrow(), Some(Ok((100, 5))));
        assert!(matches!(t.begin(7, 1, now), Begin::Done(100, 5)));
        // Another sequence or producer is new.
        assert!(matches!(t.begin(7, 2, now), Begin::Fresh(_)));
        assert!(matches!(t.begin(8, 1, now), Begin::Fresh(_)));
    }

    #[test]
    fn failed_original_lets_the_retry_append() {
        let mut t = DedupTable::default();
        let now = Instant::now();
        assert!(matches!(t.begin(1, 9, now), Begin::Fresh(_)));
        t.finish(1, 9, None);
        assert!(matches!(t.begin(1, 9, now), Begin::Fresh(_)));
    }

    #[test]
    fn window_and_rebuild_records() {
        let mut t = DedupTable::default();
        let now = Instant::now();
        for s in 0..(RECENT_PER_PRODUCER as u64 + 10) {
            t.record(3, s, s * 10, 1, now);
        }
        assert!(matches!(t.begin(3, 0, now), Begin::Fresh(_)), "aged out");
        assert!(matches!(t.begin(3, 70, now), Begin::Done(700, 1)));
        t.reset(Some(2));
        assert!(t.is_empty());
        assert!(!t.ready);
    }

    #[test]
    fn producer_cap_evicts_the_oldest() {
        let mut t = DedupTable::default();
        let t0 = Instant::now();
        for pid in 0..MAX_PRODUCERS as u64 {
            t.record(pid, 1, 0, 1, t0);
        }
        t.record(u64::MAX, 1, 0, 1, t0 + Duration::from_secs(1));
        assert!(t.len() <= MAX_PRODUCERS);
        assert!(matches!(t.begin(u64::MAX, 1, t0), Begin::Done(0, 1)));
    }
}

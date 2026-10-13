//! Per-partition append-only segmented log.
//!
//! Layout: `<dir>/{base:020}.log` + `<dir>/{base:020}.idx`, one pair per segment.
//!
//! A segment is a sequence of entries ([`super::entry`]): batch envelopes (v0.7, everything
//! this broker writes unless a message is too large for an envelope or batch writes are
//! off for a rolling upgrade) and legacy single records (v0.1–v0.6, still readable; one
//! segment may mix both).
//!
//! Concurrency model:
//! * [`PartitionWriter`] is owned by exactly one writer thread. It appends to the active
//!   (last) segment, rolls segments and publishes visibility after each commit.
//! * [`PartitionLog`] is shared (`Arc`) by readers, the dispatcher, retention and stats.
//!   Readers bound every scan by [`PartitionLog::high`] and the per-segment visible length, so
//!   they never see uncommitted bytes.
//! * Retention removes whole segments from the front, never the active segment. Removed
//!   segments stay readable by readers that still hold their `Arc` (POSIX unlink semantics).
//!
//! Recovery (on open): the last segment is scanned entry by entry, verifying each CRC
//! (CRC-32C for envelopes, CRC-32 for legacy records) and offset contiguity; the first torn
//! or invalid entry and everything after it is truncated (a torn batch goes as a unit: it
//! was never acknowledged), and the index is rebuilt. Sealed segments trust their index file
//! after validation (rebuilt from the log otherwise) and only the tail after the last index
//! entry is scanned to learn their end offset and max timestamp.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, IoSlice, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fluxmq_proto::batch::{
    self, Compression, ENVELOPE_HEADER_LEN, EnvelopeHeader, MAX_BATCH_LEN, RecordsSection, codec,
};
use fluxmq_proto::model::{Message, PublishMessage};
use parking_lot::{Mutex, RwLock};
use tracing::{debug, error, warn};

use super::entry::{self, ChunkRecords};
use super::index::{INDEX_INTERVAL_BYTES, IndexEntry, SparseIndex, parse_index};
use super::record::{self, Record};
use super::segment::{
    RawEntries, Segment, SegmentPin, SegmentScan, index_file_name, log_file_name,
    parse_log_file_name, to_io,
};
use crate::fsync_pool::SyncOp;
use crate::util::fsync_dir;

/// Default size of the per-partition recent-entry cache (`BrokerConfig::partition_cache_bytes`).
pub const DEFAULT_CACHE_BYTES: usize = 16 * 1024 * 1024;
/// Largest segment we allow (index positions are u32).
const MAX_SEGMENT_BYTES: u64 = 1 << 31;
/// `publish` (messages, not a producer batch) cuts envelopes at about this many section
/// bytes. Bigger envelopes save little more and make reads at an arbitrary offset coarser.
const ENVELOPE_TARGET_BYTES: usize = 1024 * 1024;
/// Read-ahead of the recovery scan.
const RECOVERY_CHUNK: usize = 1024 * 1024;
/// iovecs per `writev` (POSIX guarantees at least 16, Linux and macOS allow 1024).
const MAX_IOV: usize = 1024;

/// Segment roll size jitter, in per-mille of `segment_bytes` (±10%).
const ROLL_JITTER_PERMILLE: u64 = 100;

#[derive(Debug, Clone, Copy)]
pub struct LogConfig {
    /// Nominal segment roll size (each segment rolls within ±10% of it with `roll_jitter`).
    pub segment_bytes: u64,
    /// fsync on roll / truncation (false under `FsyncPolicy::OsBuffered`).
    pub sync: bool,
    /// Deterministic ±10% roll-size jitter per (partition directory, segment base offset), so
    /// the partitions of a channel written at the same rate don't roll — and later get
    /// deleted by retention — at the same moment.
    pub roll_jitter: bool,
    /// Bytes of recently published entries kept in memory per partition.
    pub cache_bytes: usize,
}

impl LogConfig {
    /// Roll size of the segment starting at `base` in the partition identified by `seed`.
    pub fn roll_size(&self, seed: u64, base: u64) -> u64 {
        let nominal = self.segment_bytes.clamp(1, MAX_SEGMENT_BYTES);
        if !self.roll_jitter {
            return nominal;
        }
        let r = splitmix64(seed ^ base.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        // Uniform in [-JITTER, +JITTER] per-mille.
        let permille = r % (2 * ROLL_JITTER_PERMILLE + 1);
        let scaled =
            u128::from(nominal) * u128::from(1000 - ROLL_JITTER_PERMILLE + permille) / 1000;
        (scaled as u64).clamp(1, MAX_SEGMENT_BYTES)
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Stable per-partition jitter seed: the partition directory (includes the channel name).
fn jitter_seed(dir: &Path, partition: u32) -> u64 {
    // Channel directory + partition directory names: independent of where the data dir lives.
    let mut h = crc32fast::Hasher::new();
    for part in [dir.parent().and_then(Path::file_name), dir.file_name()]
        .into_iter()
        .flatten()
    {
        h.update(part.as_encoded_bytes());
        h.update(b"/");
    }
    (u64::from(h.finalize()) << 32) | u64::from(partition)
}

/// One published entry kept in the recent cache: its stored bytes (an envelope or a legacy
/// record) and its messages, materialized at most once (eagerly when the publish path
/// already has them, else on first per-message access).
pub struct CachedEntry {
    pub base: u64,
    pub count: u32,
    pub is_batch: bool,
    pub bytes: Bytes,
    msgs: OnceLock<Arc<[Arc<Message>]>>,
}

impl std::fmt::Debug for CachedEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedEntry")
            .field("base", &self.base)
            .field("count", &self.count)
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl CachedEntry {
    fn new(base: u64, count: u32, is_batch: bool, bytes: Bytes) -> Self {
        Self {
            base,
            count,
            is_batch,
            bytes,
            msgs: OnceLock::new(),
        }
    }

    fn with_messages(mut self, msgs: Arc<[Arc<Message>]>) -> Self {
        self.msgs = OnceLock::from(msgs);
        self
    }

    pub fn next_offset(&self) -> u64 {
        self.base + u64::from(self.count)
    }

    /// Approximate memory held (bytes plus per-message overhead once materialized).
    fn cost(&self) -> usize {
        self.bytes.len() + 64 * self.count as usize
    }

    /// The entry's messages (decoded on first use; payloads share the entry's bytes).
    pub fn messages(&self, partition: u32) -> Arc<[Arc<Message>]> {
        self.msgs
            .get_or_init(|| {
                let mut out = Vec::with_capacity(self.count as usize);
                for m in entry::decode_chunk(partition, &self.bytes) {
                    match m {
                        Ok(m) => out.push(Arc::new(m)),
                        Err(e) => {
                            error!(partition, base = self.base, error = %e, "cached entry does not decode");
                            return Arc::from(Vec::new());
                        }
                    }
                }
                Arc::from(out)
            })
            .clone()
    }
}

/// Recently published entries (contiguous offsets) so caught-up consumers never touch the
/// disk. Bounded by bytes only.
struct RecentCache {
    entries: VecDeque<Arc<CachedEntry>>,
    bytes: usize,
    max_bytes: usize,
}

impl RecentCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    fn push(&mut self, e: Arc<CachedEntry>) {
        if self.max_bytes == 0 {
            return;
        }
        if self
            .entries
            .back()
            .is_some_and(|l| l.next_offset() != e.base)
        {
            self.clear();
        }
        self.bytes += e.cost();
        self.entries.push_back(e);
        while self.bytes > self.max_bytes && self.entries.len() > 1 {
            if let Some(old) = self.entries.pop_front() {
                self.bytes -= old.cost();
            }
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    /// Index of the entry containing `offset`.
    fn find(&self, offset: u64) -> Option<usize> {
        let i = self.entries.partition_point(|e| e.base <= offset);
        let i = i.checked_sub(1)?;
        (offset < self.entries[i].next_offset()).then_some(i)
    }
}

/// Encoded entries read for replication (`count` consecutive offsets from `base_offset`;
/// `base_offset` is the first entry's base, which may be below the requested offset when
/// that offset lies inside an envelope).
#[derive(Debug, Clone)]
pub struct RawChunk {
    pub base_offset: u64,
    pub count: u64,
    pub bytes: bytes::Bytes,
}

/// Whole stored entries, back to back, for raw delivery ([`PartitionLog::read_envelopes`],
/// [`PartitionLog::cached_envelopes`]). Decode with [`entry::decode_chunk`].
#[derive(Debug, Clone)]
pub struct EnvelopeChunk {
    /// Base offset of the first entry (<= the requested offset).
    pub first_base: u64,
    /// Offset after the last record.
    pub next_offset: u64,
    /// Whole entries, contiguous offsets.
    pub bytes: Bytes,
    /// False if any legacy (v0.6) record is inside.
    pub batches_only: bool,
}

impl From<RawEntries> for EnvelopeChunk {
    fn from(r: RawEntries) -> Self {
        EnvelopeChunk {
            first_base: r.first_base,
            next_offset: r.next_offset,
            bytes: r.bytes,
            batches_only: r.batches_only,
        }
    }
}

/// Shared, read-mostly view of one partition.
pub struct PartitionLog {
    pub partition: u32,
    pub dir: PathBuf,
    segments: RwLock<Vec<Arc<Segment>>>,
    /// Next offset to be made visible == one past the last committed record.
    high: AtomicU64,
    cache: Mutex<RecentCache>,
    pub published_total: AtomicU64,
    /// New appends are written as batch envelopes (default). Off: legacy records, for
    /// replicas that may still run v0.6 during a rolling upgrade.
    batch_writes: AtomicBool,
    /// Offset after the last entry handed to the OS (maybe not durable yet), `>= high`.
    written_high: AtomicU64,
    /// Woken whenever `written_high` moves (replica long-polls).
    written_notify: tokio::sync::Notify,
    /// Highest offset (exclusive) served by an unsynced read. A rollback that discards
    /// offsets below it poisons the writer: a follower may hold them, so they must never be
    /// reassigned to other data before a restart (recovery and leader election reconcile).
    unsynced_served: AtomicU64,
    /// Fault injection: the next commit's fsync of this partition fails with `EIO` after
    /// this many milliseconds (`u64::MAX` = off).
    fail_next_fsync: AtomicU64,
    /// Fault injection: every commit's fsync of this partition first sleeps this many
    /// milliseconds (0 = off).
    delay_fsync: AtomicU64,
    /// Fault injection: the next commit's fsync of this partition first sleeps this many
    /// milliseconds (`u64::MAX` = off).
    delay_next_fsync: AtomicU64,
}

impl std::fmt::Debug for PartitionLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionLog")
            .field("partition", &self.partition)
            .field("high", &self.high())
            .finish()
    }
}

impl PartitionLog {
    /// Opens (creating if needed) the partition directory, recovering existing segments.
    pub fn open(
        dir: &Path,
        partition: u32,
        cfg: LogConfig,
    ) -> io::Result<(Arc<PartitionLog>, PartitionWriter)> {
        fs::create_dir_all(dir)?;
        let mut bases = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(base) = parse_log_file_name(&name) {
                bases.push(base);
            } else if name.ends_with(".idx") {
                let log = dir.join(name.replace(".idx", ".log"));
                if !log.exists() {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
        bases.sort_unstable();
        let mut segments = Vec::with_capacity(bases.len().max(1));
        let n = bases.len();
        for (i, base) in bases.into_iter().enumerate() {
            segments.push(Arc::new(recover_segment(dir, base, i + 1 == n, cfg.sync)?));
        }
        if segments.is_empty() {
            segments.push(Arc::new(create_segment(dir, 0, cfg.sync)?));
        }
        let active = segments
            .last()
            .cloned()
            .expect("at least one segment exists");
        let high = active
            .next_offset()
            .max(segments.iter().map(|s| s.next_offset()).max().unwrap_or(0));
        let last_ts = segments.iter().rev().find_map(|s| s.max_ts()).unwrap_or(0);
        let log = Arc::new(PartitionLog {
            partition,
            dir: dir.to_path_buf(),
            segments: RwLock::new(segments),
            high: AtomicU64::new(high),
            cache: Mutex::new(RecentCache::new(cfg.cache_bytes)),
            published_total: AtomicU64::new(0),
            batch_writes: AtomicBool::new(true),
            written_high: AtomicU64::new(high),
            written_notify: tokio::sync::Notify::new(),
            unsynced_served: AtomicU64::new(0),
            fail_next_fsync: AtomicU64::new(u64::MAX),
            delay_fsync: AtomicU64::new(0),
            delay_next_fsync: AtomicU64::new(u64::MAX),
        });
        let writer = PartitionWriter::new(log.clone(), active, cfg, last_ts)?;
        Ok((log, writer))
    }

    /// One past the last committed offset.
    pub fn high(&self) -> u64 {
        self.high.load(Ordering::Acquire)
    }

    /// Offset after the last entry whose `write(2)` finished, durable or not (`>= high`).
    /// A replicated leader serves followers up to here ([`read_envelopes_unsynced`]) so their
    /// fsync overlaps its own; acks still wait for every ISR member's durable LEO. Always an
    /// entry boundary.
    ///
    /// [`read_envelopes_unsynced`]: Self::read_envelopes_unsynced
    pub fn written_high(&self) -> u64 {
        self.written_high.load(Ordering::Acquire)
    }

    /// Notified (`notify_waiters`) whenever [`written_high`](Self::written_high) moves.
    /// Register with `notified()` (and `enable()`) before re-checking `written_high`.
    pub fn written_notify(&self) -> &tokio::sync::Notify {
        &self.written_notify
    }

    fn set_written_high(&self, v: u64) {
        let old = self.written_high.swap(v, Ordering::AcqRel);
        if v > old {
            self.written_notify.notify_waiters();
        }
    }

    /// Like [`read_envelopes`](Self::read_envelopes) but bounded by
    /// [`written_high`](Self::written_high) instead of `high`: the entries may not be durable
    /// yet. For replicating to followers only (the leader's fsync then overlaps theirs); a
    /// failed fsync after such a read poisons the writer (see `unsynced_served`). Never
    /// served from the cache (it holds published entries only). Blocking.
    pub fn read_envelopes_unsynced(
        &self,
        from: u64,
        max_bytes: usize,
    ) -> io::Result<Option<EnvelopeChunk>> {
        let wh = self.written_high();
        if from >= wh {
            return Ok(None);
        }
        let segs = self.snapshot();
        let Some(seg) = segs
            .iter()
            .rev()
            .find(|s| s.base_offset <= from)
            .filter(|s| from < s.written_next())
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("offset {from} is not in the retained log"),
            ));
        };
        let r = seg.read_entries_in(from, max_bytes, true)?;
        if r.next_offset > self.high() {
            self.unsynced_served
                .fetch_max(r.next_offset, Ordering::AcqRel);
        }
        Ok(Some(r.into()))
    }

    /// Fault injection for tests: the next commit that fsyncs this partition's data fails
    /// with `EIO` (as if the device reported a write error), exercising rollback/poisoning.
    #[doc(hidden)]
    pub fn fail_next_fsync_for_tests(&self) {
        self.fail_next_fsync_for_tests_after(Duration::ZERO);
    }

    /// Like [`fail_next_fsync_for_tests`](Self::fail_next_fsync_for_tests), but the failing
    /// fsync first sleeps `delay` (so followers can fetch the unsynced tail meanwhile).
    #[doc(hidden)]
    pub fn fail_next_fsync_for_tests_after(&self, delay: Duration) {
        let ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX - 1);
        self.fail_next_fsync.store(ms, Ordering::Release);
    }

    /// Fault injection for tests: from now on every commit's fsync of this partition takes
    /// at least `delay` longer (a slow device for this partition); `Duration::ZERO` stops it.
    #[doc(hidden)]
    pub fn delay_fsyncs_for_tests(&self, delay: Duration) {
        let ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX - 1);
        self.delay_fsync.store(ms, Ordering::Release);
    }

    /// Fault injection for tests: the next commit's fsync of this partition takes at least
    /// `delay` longer (one-shot), so later commits' fsyncs finish first.
    #[doc(hidden)]
    pub fn delay_next_fsync_for_tests(&self, delay: Duration) {
        let ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX - 1);
        self.delay_next_fsync.store(ms, Ordering::Release);
    }

    /// First retained offset.
    pub fn low(&self) -> u64 {
        let segs = self.segments.read();
        segs.first().map_or(self.high(), |s| s.base_offset)
    }

    /// Whether new appends are written as batch envelopes (see [`set_batch_writes`]).
    ///
    /// [`set_batch_writes`]: Self::set_batch_writes
    pub fn batch_writes(&self) -> bool {
        self.batch_writes.load(Ordering::Acquire)
    }

    /// Switches the write format of new appends: envelopes (true, default) or legacy
    /// records readable by v0.6 replicas (false). Offsets and ack semantics are the same;
    /// the writer reads the flag per append.
    pub fn set_batch_writes(&self, enabled: bool) {
        self.batch_writes.store(enabled, Ordering::Release);
    }

    pub fn storage_bytes(&self) -> u64 {
        self.segments.read().iter().map(|s| s.visible_len()).sum()
    }

    pub fn segment_count(&self) -> u32 {
        self.segments.read().len() as u32
    }

    pub fn oldest_timestamp(&self) -> Option<u64> {
        self.segments.read().iter().find_map(|s| s.min_ts())
    }

    fn snapshot(&self) -> Vec<Arc<Segment>> {
        self.segments.read().clone()
    }

    /// Message from the in-memory recent cache, if present. The first access to an entry
    /// that was not published by `publish` (a producer batch) decodes it once.
    pub fn cached(&self, offset: u64) -> Option<Arc<Message>> {
        if offset >= self.high() {
            return None;
        }
        let e = {
            let c = self.cache.lock();
            c.entries[c.find(offset)?].clone()
        };
        e.messages(self.partition)
            .get((offset - e.base) as usize)
            .cloned()
    }

    /// Messages `[from, end)` from the recent cache (v0.10 live reads): at most `max_records`
    /// and about `max_bytes` of payload, stopping at the first offset that is not cached (empty
    /// when `from` is not). One cache lock for the whole range; payloads share the entries'
    /// bytes.
    pub fn cached_range(
        &self,
        from: u64,
        end: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Vec<Arc<Message>> {
        let end = end.min(self.high());
        if from >= end || max_records == 0 {
            return Vec::new();
        }
        let entries: Vec<Arc<CachedEntry>> = {
            let c = self.cache.lock();
            let Some(i) = c.find(from) else {
                return Vec::new();
            };
            let mut n = 0usize;
            let mut out = Vec::new();
            for e in c.entries.range(i..) {
                if e.base >= end || n >= max_records {
                    break;
                }
                n += e.count as usize;
                out.push(e.clone());
            }
            out
        };
        let mut out = Vec::new();
        let mut bytes = 0usize;
        'entries: for e in entries {
            for m in e.messages(self.partition).iter() {
                if m.offset < from {
                    continue;
                }
                if m.offset >= end || out.len() >= max_records || bytes >= max_bytes {
                    break 'entries;
                }
                bytes += m.payload.len();
                out.push(m.clone());
            }
        }
        out
    }

    /// Whole cached entries starting with the one containing `from`: about `max_bytes`, at
    /// least one entry. `None` when `from` is not cached. Zero-copy for a single entry;
    /// several entries are concatenated (one copy).
    pub fn cached_envelopes(&self, from: u64, max_bytes: usize) -> Option<EnvelopeChunk> {
        self.cached_entries(from, max_bytes)
            .map(EnvelopeChunk::from)
    }

    fn cached_entries(&self, from: u64, max_bytes: usize) -> Option<RawEntries> {
        if from >= self.high() {
            return None;
        }
        let picked: Vec<Arc<CachedEntry>> = {
            let c = self.cache.lock();
            let i = c.find(from)?;
            let mut len = 0usize;
            let mut out = Vec::new();
            for e in c.entries.range(i..) {
                if !out.is_empty() && len + e.bytes.len() > max_bytes {
                    break;
                }
                len += e.bytes.len();
                out.push(e.clone());
            }
            out
        };
        let first = picked.first()?;
        let last = picked.last()?;
        let bytes = if picked.len() == 1 {
            first.bytes.clone()
        } else {
            let mut v = Vec::with_capacity(picked.iter().map(|e| e.bytes.len()).sum());
            for e in &picked {
                v.extend_from_slice(&e.bytes);
            }
            Bytes::from(v)
        };
        Some(RawEntries {
            bytes,
            first_base: first.base,
            next_offset: last.next_offset(),
            records: picked.iter().map(|e| u64::from(e.count)).sum(),
            batches_only: picked.iter().all(|e| e.is_batch),
        })
    }

    /// Whole stored entries starting with the one containing `from`, for raw delivery: about
    /// `max_bytes` (at least one entry), never crossing a segment boundary, no per-record
    /// work. Served from the recent cache when it holds `from`, else one positional read
    /// (big sequential reads go through the page cache's readahead). `Ok(None)` when
    /// `from >= high`; `InvalidInput` when `from` is below the retained range. Blocking.
    pub fn read_envelopes(&self, from: u64, max_bytes: usize) -> io::Result<Option<EnvelopeChunk>> {
        Ok(self.read_entries(from, max_bytes)?.map(EnvelopeChunk::from))
    }

    fn read_entries(&self, from: u64, max_bytes: usize) -> io::Result<Option<RawEntries>> {
        let high = self.high();
        if from >= high {
            return Ok(None);
        }
        let segs = self.snapshot();
        let Some(seg) = segs
            .iter()
            .rev()
            .find(|s| s.base_offset <= from)
            .filter(|s| from < s.next_offset())
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("offset {from} is not in the retained log"),
            ));
        };
        // The cache may span segments; never return a range crossing one.
        if let Some(c) = self.cached_entries(from, max_bytes)
            && c.first_base >= seg.base_offset
            && c.next_offset <= seg.next_offset()
        {
            return Ok(Some(c));
        }
        seg.read_entries(from, max_bytes).map(Some)
    }

    /// Approximate (lower bound) first offset with timestamp >= `ts`, from in-memory metadata
    /// only (no I/O). Callers needing exactness skip records older than `ts`.
    pub fn offset_for_time_approx(&self, ts: u64) -> u64 {
        let segs = self.segments.read();
        for s in segs.iter() {
            match s.max_ts() {
                Some(max) if max >= ts => return s.offset_for_time_approx(ts).max(s.base_offset),
                _ => continue,
            }
        }
        self.high()
    }

    /// Scans committed records in offset order starting at `start` (clamped to the retained
    /// range) until `f` breaks or the end is reached. Returns the number of records visited.
    pub fn scan(&self, start: u64, f: impl FnMut(Record) -> ControlFlow<()>) -> io::Result<u64> {
        self.scan_until(start, self.high(), f)
    }

    /// [`scan`](Self::scan) bounded by `end` (exclusive) as well as the committed end.
    pub fn scan_until(
        &self,
        start: u64,
        end: u64,
        mut f: impl FnMut(Record) -> ControlFlow<()>,
    ) -> io::Result<u64> {
        let high = self.high().min(end);
        let mut visited = 0u64;
        for seg in self.snapshot() {
            if seg.next_offset() <= start {
                continue;
            }
            let mut scan = seg.scan_from(seg.position_for_offset(start));
            while let Some(e) = scan.next_entry(false)? {
                if e.meta.base_offset >= high {
                    return Ok(visited);
                }
                if e.meta.next_offset() <= start {
                    continue;
                }
                for rec in ChunkRecords::new(e.bytes) {
                    let rec = rec.map_err(to_io)?;
                    if rec.offset >= high {
                        return Ok(visited);
                    }
                    if rec.offset < start {
                        continue;
                    }
                    visited += 1;
                    if f(rec).is_break() {
                        return Ok(visited);
                    }
                }
            }
        }
        Ok(visited)
    }

    /// Like [`scan`](Self::scan) but starts at the first record with timestamp >= `ts`.
    pub fn scan_from_time(
        &self,
        ts: u64,
        f: impl FnMut(Record) -> ControlFlow<()>,
    ) -> io::Result<u64> {
        self.scan_from_time_until(ts, self.high(), f)
    }

    /// [`scan_from_time`](Self::scan_from_time) bounded by `end` (exclusive).
    pub fn scan_from_time_until(
        &self,
        ts: u64,
        end: u64,
        mut f: impl FnMut(Record) -> ControlFlow<()>,
    ) -> io::Result<u64> {
        let high = self.high().min(end);
        let mut visited = 0u64;
        for seg in self.snapshot() {
            match seg.max_ts() {
                Some(max) if max >= ts => {}
                _ => continue,
            }
            let mut scan = seg.scan_from(seg.position_for_time(ts));
            while let Some(e) = scan.next_entry(false)? {
                if e.meta.base_offset >= high {
                    return Ok(visited);
                }
                for rec in ChunkRecords::new(e.bytes) {
                    let rec = rec.map_err(to_io)?;
                    if rec.offset >= high {
                        return Ok(visited);
                    }
                    if rec.timestamp_ms < ts {
                        continue;
                    }
                    visited += 1;
                    if f(rec).is_break() {
                        return Ok(visited);
                    }
                }
            }
        }
        Ok(visited)
    }

    /// Exact first offset whose timestamp is >= `ts` (or `high` if none).
    pub fn offset_for_time(&self, ts: u64) -> io::Result<u64> {
        let mut found = None;
        self.scan_from_time(ts, |r| {
            found = Some(r.offset);
            ControlFlow::Break(())
        })?;
        Ok(found.unwrap_or_else(|| self.high()))
    }

    pub fn get(&self, offset: u64) -> io::Result<Option<Message>> {
        if let Some(m) = self.cached(offset) {
            return Ok(Some((*m).clone()));
        }
        let mut out = None;
        self.scan(offset, |r| {
            if r.offset == offset {
                out = Some(r.into_message(self.partition));
            }
            ControlFlow::Break(())
        })?;
        Ok(out)
    }

    /// Reads up to `max_records` / about `max_bytes` committed messages from `start`.
    pub fn read_batch(
        &self,
        start: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> io::Result<Vec<Arc<Message>>> {
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut next = start;
        // Serve from the cache while possible.
        while out.len() < max_records && bytes < max_bytes {
            match self.cached(next) {
                Some(m) => {
                    bytes += m.payload.len();
                    out.push(m);
                    next += 1;
                }
                None => break,
            }
        }
        if out.len() >= max_records || bytes >= max_bytes || next >= self.high() {
            return Ok(out);
        }
        self.scan(next, |r| {
            bytes += r.payload.len();
            out.push(Arc::new(r.into_message(self.partition)));
            if out.len() >= max_records || bytes >= max_bytes {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?;
        Ok(out)
    }

    /// Raw entries for replication: whole entries starting with the one containing `from`
    /// (about `max_bytes`, at least one entry, never crossing a segment boundary). The
    /// chunk's `base_offset` is the first entry's base (below `from` when `from` lies inside
    /// an envelope) and `count` the records it holds. `Ok(None)` when `from >= high`; an
    /// `InvalidInput` error when `from` is below the retained range. Positional reads only.
    /// (Replication uses [`read_envelopes_unsynced`](Self::read_envelopes_unsynced).)
    #[cfg(test)]
    pub fn read_raw(&self, from: u64, max_bytes: usize) -> io::Result<Option<RawChunk>> {
        Ok(self.read_entries(from, max_bytes)?.map(|r| RawChunk {
            base_offset: r.first_base,
            count: r.records,
            bytes: r.bytes,
        }))
    }

    /// Removes segments from the front while `pred` holds; never removes the active segment
    /// nor a segment pinned by a backup cut (removal stops there and resumes once the pin is
    /// released). Files are unlinked after the in-memory list is updated. Returns
    /// `(segments, bytes)`.
    #[cfg(test)]
    pub fn remove_oldest_while(
        &self,
        pred: impl FnMut(&Segment) -> bool,
    ) -> io::Result<(u32, u64)> {
        self.remove_oldest(usize::MAX, pred, None)
    }

    /// How many segments [`remove_oldest_while`](Self::remove_oldest_while) would remove now
    /// (same rules; nothing is removed).
    pub fn count_removable(&self, mut pred: impl FnMut(&Segment) -> bool) -> usize {
        let segs = self.segments.read();
        let mut n = 0;
        while n + 1 < segs.len() && !segs[n].is_pinned() && pred(&segs[n]) {
            n += 1;
        }
        n
    }

    /// Like [`remove_oldest_while`](Self::remove_oldest_while) but removes at most `max`
    /// segments. With `shrink = Some((step, pause))`, a removed segment nobody else references
    /// any more (no reader, backup cut or writer handle) is first truncated from the end in
    /// `step`-byte steps with `pause` between them, then unlinked: freeing a large file's
    /// extents in one go can make concurrent fsyncs on the same filesystem wait behind the
    /// journal commit (observed on ext4 as a 0.5–1 s ack stall). Segments still referenced
    /// are only unlinked, so their readers keep the full file (POSIX unlink semantics).
    pub fn remove_oldest(
        &self,
        max: usize,
        mut pred: impl FnMut(&Segment) -> bool,
        shrink: Option<(u64, Duration)>,
    ) -> io::Result<(u32, u64)> {
        let removed: Vec<Arc<Segment>> = {
            let mut segs = self.segments.write();
            let mut n = 0;
            while n < max && n + 1 < segs.len() && !segs[n].is_pinned() && pred(&segs[n]) {
                n += 1;
            }
            segs.drain(..n).collect()
        };
        let mut bytes = 0;
        for s in removed.iter() {
            bytes += s.visible_len();
        }
        let count = removed.len() as u32;
        for s in removed {
            let (log_path, index_path, base) =
                (s.log_path.clone(), s.index_path.clone(), s.base_offset);
            if let Some((step, pause)) = shrink {
                // Only our reference left: nobody can read it any more (it left the list).
                if let Ok(seg) = Arc::try_unwrap(s) {
                    drop(seg);
                    shrink_in_steps(&log_path, step.max(1 << 20), pause);
                }
            }
            if let Err(e) = fs::remove_file(&log_path)
                && e.kind() != io::ErrorKind::NotFound
            {
                return Err(e);
            }
            let _ = fs::remove_file(&index_path);
            debug!(
                partition = self.partition,
                base, "retention removed segment"
            );
        }
        Ok((count, bytes))
    }
}

/// One segment's share of a backup cut: bytes `[0, len)` hold exactly the records
/// `[base_offset, end_offset)`. Holds a [`SegmentPin`] until dropped.
#[derive(Debug)]
pub struct SegmentCut {
    pub base_offset: u64,
    pub end_offset: u64,
    pub len: u64,
    /// Index entries covering `[0, len)` (16-byte entries, see `index.rs`).
    pub index: Vec<u8>,
    pin: SegmentPin,
}

impl SegmentCut {
    /// Positional read within `[0, len)`; returns 0 at the end. Blocking.
    pub fn read_at(&self, buf: &mut [u8], pos: u64) -> io::Result<usize> {
        if pos >= self.len {
            return Ok(0);
        }
        let n = buf.len().min((self.len - pos) as usize);
        let mut done = 0;
        while done < n {
            let r = self
                .pin
                .segment()
                .read_at(&mut buf[done..n], pos + done as u64)?;
            if r == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "segment {} shorter than its captured length",
                        self.base_offset
                    ),
                ));
            }
            done += r;
        }
        Ok(n)
    }

    pub fn messages(&self) -> u64 {
        self.end_offset - self.base_offset
    }
}

impl PartitionLog {
    /// Backup cut: the segments holding the committed records `[low, end)` (`end` clamped to
    /// [`high`](Self::high)), each pinned against retention. Byte lengths are computed from
    /// the entries themselves (a short index-guided scan per segment), so a concurrent
    /// publish or roll can't make the cut include records at or past `end`. An envelope that
    /// straddles `end` is left out whole (the cut then ends at its base). The segment list
    /// is only read-locked while the segments are cloned and pinned; scans run after.
    /// Returns `(cut segments, end)`. Blocking (small positional reads).
    pub fn backup_cut(&self, end: u64) -> io::Result<(Vec<SegmentCut>, u64)> {
        let end = end.min(self.high());
        let pinned: Vec<SegmentPin> = {
            let segs = self.segments.read();
            segs.iter()
                .filter(|s| s.base_offset < end)
                .map(|s| SegmentPin::new(s.clone()))
                .collect()
        };
        let mut out: Vec<SegmentCut> = Vec::with_capacity(pinned.len());
        for pin in pinned {
            let seg = pin.segment();
            let rel = u32::try_from(end - seg.base_offset).unwrap_or(u32::MAX);
            // Start at the last index entry at or before `end`. Every record below `end` is
            // visible (end <= high), so the entry's record — and the scan — stay in range.
            let (rel_e, pos_e) = seg.index.lookup_offset(rel);
            let mut len = u64::from(pos_e);
            let mut end_offset = seg.base_offset + u64::from(rel_e);
            if len > seg.visible_len() {
                return Err(io::Error::other(format!(
                    "segment {} changed during the backup cut; retry",
                    seg.base_offset
                )));
            }
            let mut scan = seg.scan_from(len);
            while let Some(e) = scan.next_entry(false)? {
                if e.meta.next_offset() > end {
                    len = e.pos;
                    break;
                }
                end_offset = e.meta.next_offset();
                len = scan.position();
            }
            out.push(SegmentCut {
                base_offset: seg.base_offset,
                end_offset,
                len,
                index: seg.index.bytes_below(len),
                pin,
            });
        }
        // Keep the chain contiguous: a segment whose records end before the next segment's
        // base (a sealed segment with an unreadable tail, or an envelope straddling `end`)
        // ends the cut.
        let mut cut_end = out.first().map_or(end, |s| s.base_offset);
        let mut keep = 0;
        for s in &out {
            if s.base_offset != cut_end {
                break;
            }
            cut_end = s.end_offset;
            keep += 1;
        }
        out.truncate(keep);
        Ok((out, cut_end))
    }
}

/// Truncates `path` from the end in `step`-byte steps (best effort; errors are ignored, the
/// caller unlinks the file anyway).
fn shrink_in_steps(path: &Path, step: u64, pause: Duration) {
    let Ok(f) = OpenOptions::new().write(true).open(path) else {
        return;
    };
    let Ok(mut len) = f.metadata().map(|m| m.len()) else {
        return;
    };
    while len > step {
        len -= step;
        if f.set_len(len).is_err() {
            return;
        }
        if !pause.is_zero() {
            std::thread::sleep(pause);
        }
    }
}

/// Opens an existing segment file for fsync only (a description of its own; see
/// [`PartitionWriter::prepare_commit`]).
fn open_sync(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

fn open_rw(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn create_segment(dir: &Path, base: u64, sync: bool) -> io::Result<Segment> {
    let log_path = dir.join(log_file_name(base));
    let idx_path = dir.join(index_file_name(base));
    File::create(&log_path)?;
    File::create(&idx_path)?;
    if sync {
        fsync_dir(dir)?;
    }
    let reader = File::open(&log_path)?;
    Ok(Segment::new(dir, base, reader, SparseIndex::default()))
}

/// Recovers one segment (see module docs).
fn recover_segment(dir: &Path, base: u64, is_last: bool, sync: bool) -> io::Result<Segment> {
    let log_path = dir.join(log_file_name(base));
    let idx_path = dir.join(index_file_name(base));
    let reader = File::open(&log_path)?;
    let file_len = reader.metadata()?.len();

    // Try the persisted index for sealed segments.
    let mut entries: Option<Vec<IndexEntry>> = None;
    if !is_last && let Ok(bytes) = fs::read(&idx_path) {
        entries = parse_index(&bytes, file_len).filter(|e| {
            // Verify the last entry actually points at the entry it claims.
            e.last().is_none_or(|last| {
                let mut s = SegmentScan::raw(&reader, u64::from(last.position), file_len);
                matches!(s.next_entry(true), Ok(Some(e)) if e.meta.base_offset == base + u64::from(last.rel_offset))
            })
        });
    }
    let rebuild = entries.is_none();
    let mut entries = entries.unwrap_or_default();
    let (mut pos, mut expected, mut min_ts, mut max_ts) = match entries.last() {
        Some(last) => (
            u64::from(last.position),
            base + u64::from(last.rel_offset),
            entries[0].timestamp_ms,
            last.timestamp_ms,
        ),
        None => (0, base, u64::MAX, 0),
    };
    let mut last_index_pos: Option<u64> = entries.last().map(|e| u64::from(e.position));
    let mut scan = SegmentScan::raw(&reader, pos, file_len);
    scan.chunk = RECOVERY_CHUNK;
    loop {
        match scan.next_entry(true) {
            Ok(Some(e)) => {
                let (off, ts, at) = (e.meta.base_offset, e.meta.timestamp_ms, e.pos);
                if off != expected {
                    warn!(
                        ?log_path,
                        at, off, expected, "unexpected offset in segment; truncating scan"
                    );
                    break;
                }
                if rebuild && last_index_pos.is_none_or(|p| at - p >= INDEX_INTERVAL_BYTES) {
                    entries.push(IndexEntry {
                        rel_offset: (off - base) as u32,
                        position: at as u32,
                        timestamp_ms: ts,
                    });
                    last_index_pos = Some(at);
                }
                min_ts = min_ts.min(ts);
                max_ts = max_ts.max(ts);
                expected = e.meta.next_offset();
                pos = scan.position();
            }
            Ok(None) => break,
            Err(e) => {
                warn!(?log_path, at = pos, error = %e, "invalid entry in segment");
                break;
            }
        }
    }
    let mut index_dirty = rebuild;
    if pos < file_len {
        if is_last {
            warn!(
                ?log_path,
                valid = pos,
                file_len,
                "truncating torn tail of active segment"
            );
            let f = OpenOptions::new().write(true).open(&log_path)?;
            f.set_len(pos)?;
            if sync {
                f.sync_all()?;
            }
            entries.retain(|e| u64::from(e.position) < pos);
            index_dirty = true;
        } else {
            warn!(
                ?log_path,
                valid = pos,
                file_len,
                "sealed segment has an invalid region; it is not readable"
            );
        }
    }
    let index = SparseIndex::new(entries);
    if index_dirty {
        let tmp = idx_path.with_extension("idx.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&index.to_bytes())?;
            if sync {
                f.sync_data()?;
            }
        }
        fs::rename(&tmp, &idx_path)?;
    }
    let seg = Segment::new(dir, base, reader, index);
    seg.publish(pos, expected, min_ts, max_ts);
    Ok(seg)
}

/// `write(2)`s all of `bufs` with as few `writev` calls as possible.
fn write_all_bufs(mut f: &File, bufs: &[Bytes]) -> io::Result<()> {
    let (mut i, mut off) = (0usize, 0usize);
    loop {
        while i < bufs.len() && off == bufs[i].len() {
            i += 1;
            off = 0;
        }
        if i == bufs.len() {
            return Ok(());
        }
        let n = if bufs.len() - i == 1 {
            f.write(&bufs[i][off..])?
        } else {
            let slices: Vec<IoSlice<'_>> = std::iter::once(IoSlice::new(&bufs[i][off..]))
                .chain(
                    bufs[i + 1..]
                        .iter()
                        .take(MAX_IOV - 1)
                        .map(|b| IoSlice::new(b)),
                )
                .collect();
            f.write_vectored(&slices)?
        };
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        let mut n = n;
        while n > 0 {
            let rem = bufs[i].len() - off;
            if n >= rem {
                n -= rem;
                i += 1;
                off = 0;
            } else {
                off += n;
                n = 0;
            }
        }
    }
}

/// State restored on writer rollback after an I/O error.
#[derive(Clone, Copy)]
struct Checkpoint {
    /// Base offset of the active segment at checkpoint time.
    active_base: u64,
    pos: u64,
    next_offset: u64,
    last_ts: u64,
    bytes_since_index: u64,
    seg_min_ts: u64,
    seg_max_ts: u64,
}

/// Everything appended up to one point, taken by [`PartitionWriter::prepare_commit`]: the
/// durability work still needed for it and the state to publish once that work succeeded.
/// Several marks may be outstanding (pipelined group commit); they must be published in the
/// order they were taken, and a failure is handled by [`PartitionWriter::rollback`], which
/// discards everything after the last published mark.
pub struct CommitMark {
    /// Must all succeed before [`PartitionWriter::publish_mark`].
    pub ops: Vec<SyncOp>,
    state: Checkpoint,
    active: Arc<Segment>,
    sealed: Vec<(Arc<Segment>, u64, u64, u64, u64)>,
    entries: Vec<Arc<CachedEntry>>,
}

impl CommitMark {
    /// One past the last record this mark covers.
    pub fn next_offset(&self) -> u64 {
        self.state.next_offset
    }
}

/// Exclusive append handle of one partition (owned by the channel writer thread).
pub struct PartitionWriter {
    log: Arc<PartitionLog>,
    cfg: LogConfig,
    /// Shared with in-flight fsync jobs (no `dup` per sync).
    file: Arc<File>,
    index_file: Arc<File>,
    active: Arc<Segment>,
    /// Roll size of the active segment (jittered `segment_bytes`).
    roll_at: u64,
    jitter_seed: u64,
    /// Durability work for the next commit besides the active file's data: the directory
    /// entry of a segment created by a roll and the sealed segment's index.
    deferred: Vec<SyncOp>,
    /// Bytes written to the active segment file.
    pos: u64,
    next_offset: u64,
    last_ts: u64,
    bytes_since_index: u64,
    seg_min_ts: u64,
    seg_max_ts: u64,
    /// Encoded entries not yet written (written with one `writev`).
    out: Vec<Bytes>,
    out_len: usize,
    index_buf: Vec<u8>,
    /// Segments sealed since the last publish: (segment, len, next_offset, min_ts, max_ts).
    sealed_unpublished: Vec<(Arc<Segment>, u64, u64, u64, u64)>,
    /// Entries appended since the last mark, for the recent cache.
    pending: Vec<Arc<CachedEntry>>,
    needs_sync: bool,
    checkpoint: Checkpoint,
    /// fsync durations since the last drain (for the broker fsync histogram).
    pub fsync_times: Vec<Duration>,
    /// Set when a rollback failed: files and memory may disagree, so every further write is
    /// refused until restart (recovery repairs the files).
    poisoned: Option<String>,
    /// Commit fsyncs of this partition may overlap (pipeline depth > 1): each one gets its
    /// own file description (see [`prepare_commit`](Self::prepare_commit)).
    overlapping_syncs: bool,
}

impl PartitionWriter {
    fn new(
        log: Arc<PartitionLog>,
        active: Arc<Segment>,
        mut cfg: LogConfig,
        last_ts: u64,
    ) -> io::Result<Self> {
        cfg.segment_bytes = cfg.segment_bytes.clamp(1, MAX_SEGMENT_BYTES);
        let file = Arc::new(open_rw(&active.log_path)?);
        let index_file = Arc::new(open_rw(&active.index_path)?);
        let jitter_seed = jitter_seed(&log.dir, log.partition);
        let roll_at = cfg.roll_size(jitter_seed, active.base_offset);
        let pos = active.visible_len();
        let bytes_since_index = active
            .index
            .last()
            .map_or(INDEX_INTERVAL_BYTES, |e| pos - u64::from(e.position));
        let seg_min_ts = active.min_ts().unwrap_or(u64::MAX);
        let seg_max_ts = active.max_ts().unwrap_or(0);
        let next_offset = log.high();
        let checkpoint = Checkpoint {
            active_base: active.base_offset,
            pos,
            next_offset,
            last_ts,
            bytes_since_index,
            seg_min_ts,
            seg_max_ts,
        };
        Ok(Self {
            log,
            cfg,
            file,
            index_file,
            active,
            roll_at,
            jitter_seed,
            deferred: Vec::new(),
            pos,
            next_offset,
            last_ts,
            bytes_since_index,
            seg_min_ts,
            seg_max_ts,
            out: Vec::new(),
            out_len: 0,
            index_buf: Vec::new(),
            sealed_unpublished: Vec::new(),
            pending: Vec::new(),
            needs_sync: false,
            checkpoint,
            fsync_times: Vec::new(),
            poisoned: None,
            overlapping_syncs: false,
        })
    }

    /// Queues one complete entry (rolling the segment first if it would overflow) and
    /// indexes it. `meta.base_offset` must be the writer's next offset.
    fn push_entry(
        &mut self,
        bytes: Bytes,
        base: u64,
        count: u32,
        ts: u64,
        cache: Option<CachedEntry>,
    ) -> io::Result<()> {
        debug_assert_eq!(base, self.next_offset);
        let size = bytes.len() as u64;
        let seg_pos = self.pos + self.out_len as u64;
        if seg_pos > 0 && seg_pos + size > self.roll_at {
            self.roll()?;
        }
        let seg_pos = self.pos + self.out_len as u64;
        if seg_pos == 0 || self.bytes_since_index >= INDEX_INTERVAL_BYTES {
            let e = IndexEntry {
                rel_offset: u32::try_from(base - self.active.base_offset)
                    .map_err(|_| io::Error::other("segment holds more than u32::MAX records"))?,
                position: seg_pos as u32,
                timestamp_ms: ts,
            };
            self.active.index.push(e);
            e.encode(&mut self.index_buf);
            self.bytes_since_index = 0;
        }
        self.out.push(bytes);
        self.out_len += size as usize;
        self.bytes_since_index += size;
        self.next_offset = base + u64::from(count);
        self.last_ts = self.last_ts.max(ts);
        self.seg_min_ts = self.seg_min_ts.min(ts);
        self.seg_max_ts = self.seg_max_ts.max(ts);
        if let Some(c) = cache {
            self.pending.push(Arc::new(c));
        }
        Ok(())
    }

    /// Appends one message as a legacy (v0.6 format) record. Used for messages too large
    /// for an envelope and while batch writes are off (rolling upgrade).
    pub fn append_legacy(&mut self, now_ms: u64, msg: PublishMessage) -> io::Result<u64> {
        self.check_poisoned()?;
        let ts = now_ms.max(self.last_ts);
        let offset = self.next_offset;
        let mut buf = Vec::with_capacity(record::encoded_len(
            msg.key.as_deref(),
            &msg.headers,
            msg.payload.len(),
        ));
        record::encode_into(
            &mut buf,
            offset,
            ts,
            msg.key.as_deref(),
            &msg.headers,
            &msg.payload,
        );
        let m: Arc<[Arc<Message>]> = Arc::from(vec![Arc::new(Message {
            partition: self.log.partition,
            offset,
            key: msg.key,
            headers: msg.headers,
            timestamp_ms: ts,
            payload: msg.payload,
        })]);
        let bytes = Bytes::from(buf);
        let cache = CachedEntry::new(offset, 1, false, bytes.clone()).with_messages(m);
        self.push_entry(bytes, offset, 1, ts, Some(cache))?;
        Ok(offset)
    }

    /// Appends `msgs` with consecutive offsets; returns the first one. They are encoded into
    /// batch envelopes of about [`ENVELOPE_TARGET_BYTES`] (one per call for usual request
    /// sizes) sharing one timestamp; a message too large for an envelope becomes a legacy
    /// record, and so does everything while batch writes are off. The messages become
    /// visible on [`publish_mark`](Self::publish_mark); their materialized form goes into
    /// the recent cache (payloads share the envelope's bytes).
    pub fn append_messages(&mut self, now_ms: u64, msgs: Vec<PublishMessage>) -> io::Result<u64> {
        self.check_poisoned()?;
        let first = self.next_offset;
        if !self.log.batch_writes() {
            for m in msgs {
                self.append_legacy(now_ms, m)?;
            }
            return Ok(first);
        }
        let mut cur: Vec<PublishMessage> = Vec::new();
        let mut cur_bytes = 0usize;
        let mut limit = self.envelope_limit();
        for m in msgs {
            let hint = batch::record_size_hint(m.key.as_deref(), &m.headers, m.payload.len());
            if ENVELOPE_HEADER_LEN + hint > MAX_BATCH_LEN {
                self.flush_envelope(now_ms, std::mem::take(&mut cur), cur_bytes)?;
                cur_bytes = 0;
                self.append_legacy(now_ms, m)?;
                limit = self.envelope_limit();
                continue;
            }
            if !cur.is_empty() && cur_bytes + hint > limit {
                self.flush_envelope(now_ms, std::mem::take(&mut cur), cur_bytes)?;
                cur_bytes = 0;
                limit = self.envelope_limit();
            }
            cur_bytes += hint;
            cur.push(m);
        }
        self.flush_envelope(now_ms, cur, cur_bytes)?;
        Ok(first)
    }

    /// Section bytes the next envelope built from messages may hold: about
    /// [`ENVELOPE_TARGET_BYTES`], and no more than what still fits in the active segment, so
    /// envelopes end at roll points and segments stay near their nominal size (retention
    /// granularity). A full segment gives a full-size budget: the envelope rolls first.
    fn envelope_limit(&self) -> usize {
        let seg_pos = self.pos + self.out_len as u64;
        let room = self.roll_at.saturating_sub(seg_pos) as usize;
        let room = room.saturating_sub(ENVELOPE_HEADER_LEN);
        if seg_pos == 0 || room == 0 {
            (self.roll_at as usize)
                .saturating_sub(ENVELOPE_HEADER_LEN)
                .clamp(1, ENVELOPE_TARGET_BYTES)
        } else {
            room.min(ENVELOPE_TARGET_BYTES)
        }
    }

    /// Encodes `msgs` (non-empty unless nothing to do) as one envelope and queues it.
    fn flush_envelope(
        &mut self,
        now_ms: u64,
        msgs: Vec<PublishMessage>,
        size_hint: usize,
    ) -> io::Result<()> {
        if msgs.is_empty() {
            return Ok(());
        }
        let ts = now_ms.max(self.last_ts);
        let base = self.next_offset;
        let count = msgs.len() as u32;
        let mut buf = Vec::with_capacity(ENVELOPE_HEADER_LEN + size_hint);
        buf.resize(ENVELOPE_HEADER_LEN, 0);
        let mut payload_at = Vec::with_capacity(msgs.len());
        for (i, m) in msgs.iter().enumerate() {
            batch::encode_record(&mut buf, i as u32, m.key.as_deref(), &m.headers, &m.payload);
            payload_at.push(buf.len() - m.payload.len());
        }
        seal_envelope(&mut buf, base, ts, count, Compression::None);
        let bytes = Bytes::from(buf);
        let partition = self.log.partition;
        let materialized: Arc<[Arc<Message>]> = msgs
            .into_iter()
            .zip(payload_at)
            .enumerate()
            .map(|(i, (m, at))| {
                Arc::new(Message {
                    partition,
                    offset: base + i as u64,
                    key: m.key,
                    headers: m.headers,
                    timestamp_ms: ts,
                    payload: bytes.slice(at..at + m.payload.len()),
                })
            })
            .collect();
        let cache = CachedEntry::new(base, count, true, bytes.clone()).with_messages(materialized);
        self.push_entry(bytes, base, count, ts, Some(cache))
    }

    /// Appends a producer's records section (validated by the caller with
    /// [`entry::validate_section`]) as one envelope, verbatim: the broker only adds the
    /// header (base offset, timestamp, CRC-32C). Returns `(base_offset, record_count)`. While
    /// batch writes are off the records are written as legacy records instead (same offsets).
    pub fn append_batch(&mut self, now_ms: u64, sec: &RecordsSection) -> io::Result<(u64, u32)> {
        self.check_poisoned()?;
        if !self.log.batch_writes() {
            let msgs = entry::section_messages(sec)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let n = msgs.len() as u32;
            let base = self.append_messages(now_ms, msgs)?;
            return Ok((base, n));
        }
        if sec.record_count == 0 || ENVELOPE_HEADER_LEN + sec.body.len() > MAX_BATCH_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty or oversized records section",
            ));
        }
        let ts = now_ms.max(self.last_ts);
        let base = self.next_offset;
        let mut buf = Vec::with_capacity(ENVELOPE_HEADER_LEN + sec.body.len());
        buf.resize(ENVELOPE_HEADER_LEN, 0);
        buf.extend_from_slice(&sec.body);
        seal_envelope(&mut buf, base, ts, sec.record_count, sec.compression);
        let bytes = Bytes::from(buf);
        let cache = CachedEntry::new(base, sec.record_count, true, bytes.clone());
        self.push_entry(bytes, base, sec.record_count, ts, Some(cache))?;
        Ok((base, sec.record_count))
    }

    /// Appends one message (as a one-record envelope, or a legacy record when batch writes
    /// are off / it is too large). Returns the stored message.
    #[cfg(test)]
    pub fn append(&mut self, now_ms: u64, msg: PublishMessage) -> io::Result<Arc<Message>> {
        self.append_messages(now_ms, vec![msg])?;
        let e = self.pending.last().expect("just appended");
        Ok(e.messages(self.log.partition)[0].clone())
    }

    /// Next offset this writer will assign (includes appended-but-unpublished records).
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Appends entries received from a partition leader verbatim: legacy records and batch
    /// envelopes, mixed. The whole chunk is verified first (every entry's CRC — CRC-32C for
    /// envelopes, without decoding or decompressing records — and contiguous offsets starting
    /// at the writer's next offset); nothing is appended if any check fails (`InvalidData`).
    /// Entries keep the leader's offsets and timestamps. They become visible on publish (but
    /// are not put in the recent cache: followers serve no consumers). Returns the new next
    /// offset.
    pub fn append_raw(&mut self, chunk: &Bytes) -> io::Result<u64> {
        self.check_poisoned()?;
        let metas = entry::split_entries(chunk, self.next_offset).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("replicated chunk: {e}"))
        })?;
        for (at, m) in metas {
            self.push_entry(
                chunk.slice(at..at + m.len),
                m.base_offset,
                m.count,
                m.timestamp_ms,
                None,
            )?;
        }
        Ok(self.next_offset)
    }

    /// Discards every record at or after `offset` (follower truncation after a leader change)
    /// and continues appending there. Entries are removed whole: an `offset` inside an
    /// envelope is rounded down to the envelope's base, and the caller reads the resulting
    /// end with [`next_offset`](Self::next_offset). When `offset` is outside `[low, high]`
    /// the log is emptied and restarts at `offset` (e.g. the leader's retention moved past
    /// this replica). Must only be called with nothing appended since the last publish.
    /// Visibility is lowered before any file is touched; readers racing with a truncation may
    /// get an I/O error, never data past the new end.
    pub fn truncate(&mut self, offset: u64) -> io::Result<()> {
        self.check_poisoned()?;
        if !self.pending.is_empty() || !self.out.is_empty() || self.next_offset != self.log.high() {
            return Err(io::Error::other("truncate with unpublished appends"));
        }
        let (low, high) = (self.log.low(), self.log.high());
        if offset == high {
            return Ok(());
        }
        self.log.cache.lock().clear();
        if offset < low || offset > high {
            return self.reset_to(offset);
        }
        let target = {
            let segs = self.log.segments.read();
            let keep = segs
                .iter()
                .rposition(|s| s.base_offset <= offset)
                .expect("offset >= low, so some segment starts at or before it");
            segs[keep].clone()
        };
        // The entry boundary at or below `offset`.
        let (pos, offset) = if offset == target.base_offset {
            (0, offset)
        } else if offset >= target.next_offset() {
            (target.visible_len(), offset)
        } else {
            let (_, e) = target.seek_entry(offset, 0)?;
            (e.pos, e.meta.base_offset)
        };
        if offset == high {
            return Ok(());
        }
        self.log.high.store(offset, Ordering::Release);
        self.log.written_high.store(offset, Ordering::Release);
        let removed: Vec<Arc<Segment>> = {
            let mut segs = self.log.segments.write();
            let keep = segs
                .iter()
                .rposition(|s| s.base_offset <= offset)
                .expect("offset >= low, so some segment starts at or before it");
            segs.drain(keep + 1..).collect()
        };
        for s in &removed {
            let _ = fs::remove_file(&s.log_path);
            let _ = fs::remove_file(&s.index_path);
        }
        let kept = target.index.truncate_from_position(pos);
        target.reset_visibility(pos, offset);
        // Timestamps are monotonic per partition: min = first entry, max = last entry.
        let (min_ts, max_ts) = if pos == 0 {
            (u64::MAX, 0)
        } else {
            let first = target.index.first().map_or(0, |e| e.timestamp_ms);
            let mut last = first;
            let from = target.index.last().map_or(0, |e| u64::from(e.position));
            let mut scan = target.scan_from(from);
            while let Some(e) = scan.next_entry(false)? {
                last = e.meta.timestamp_ms;
            }
            (first, last)
        };
        target.reset_ts(min_ts, max_ts);
        let f = OpenOptions::new().write(true).open(&target.log_path)?;
        f.set_len(pos)?;
        let idx = OpenOptions::new().write(true).open(&target.index_path)?;
        let idx_len = idx.metadata()?.len();
        idx.set_len(idx_len.min((kept * super::index::ENTRY_SIZE) as u64))?;
        if self.cfg.sync {
            f.sync_all()?;
            idx.sync_all()?;
            if !removed.is_empty() {
                fsync_dir(&self.log.dir)?;
            }
        }
        self.bytes_since_index = target
            .index
            .last()
            .map_or(INDEX_INTERVAL_BYTES, |e| pos - u64::from(e.position));
        self.set_active(target)?;
        self.pos = pos;
        self.next_offset = offset;
        self.last_ts = self
            .log
            .segments
            .read()
            .iter()
            .rev()
            .find_map(|s| s.max_ts())
            .unwrap_or(0);
        self.seg_min_ts = min_ts;
        self.seg_max_ts = max_ts;
        self.after_reset();
        Ok(())
    }

    /// Re-creates the partition directory and an empty active segment at the current next
    /// offset, discarding the previous segment files (used when released storage — whose
    /// directory was removed — is written to again). Requires an empty log.
    pub fn recreate(&mut self) -> io::Result<()> {
        self.check_poisoned()?;
        if self.log.low() != self.log.high() || !self.pending.is_empty() || !self.out.is_empty() {
            return Err(io::Error::other("recreate of a non-empty partition log"));
        }
        fs::create_dir_all(&self.log.dir)?;
        self.reset_to(self.next_offset)
    }

    /// Replaces the whole log with one empty segment starting at `offset`.
    fn reset_to(&mut self, offset: u64) -> io::Result<()> {
        let old: Vec<Arc<Segment>> = std::mem::take(&mut *self.log.segments.write());
        self.log
            .high
            .store(offset.min(self.log.high()), Ordering::Release);
        self.log
            .written_high
            .store(offset.min(self.log.high()), Ordering::Release);
        for s in &old {
            let _ = fs::remove_file(&s.log_path);
            let _ = fs::remove_file(&s.index_path);
        }
        let seg = Arc::new(create_segment(&self.log.dir, offset, self.cfg.sync)?);
        self.set_active(seg.clone())?;
        self.log.segments.write().push(seg);
        self.log.high.store(offset, Ordering::Release);
        self.log.set_written_high(offset);
        self.pos = 0;
        self.next_offset = offset;
        self.bytes_since_index = INDEX_INTERVAL_BYTES;
        self.seg_min_ts = u64::MAX;
        self.seg_max_ts = 0;
        self.after_reset();
        Ok(())
    }

    /// Makes `seg` the active segment: opens its files for appending and computes its roll
    /// size.
    fn set_active(&mut self, seg: Arc<Segment>) -> io::Result<()> {
        self.file = Arc::new(open_rw(&seg.log_path)?);
        self.index_file = Arc::new(open_rw(&seg.index_path)?);
        self.roll_at = self.cfg.roll_size(self.jitter_seed, seg.base_offset);
        self.active = seg;
        Ok(())
    }

    fn after_reset(&mut self) {
        self.needs_sync = false;
        self.deferred.clear();
        self.sealed_unpublished.clear();
        self.checkpoint = Checkpoint {
            active_base: self.active.base_offset,
            pos: self.pos,
            next_offset: self.next_offset,
            last_ts: self.last_ts,
            bytes_since_index: self.bytes_since_index,
            seg_min_ts: self.seg_min_ts,
            seg_max_ts: self.seg_max_ts,
        };
    }

    fn check_poisoned(&self) -> io::Result<()> {
        match &self.poisoned {
            Some(why) => Err(io::Error::other(format!(
                "partition {} writer disabled after a failed rollback ({why}); restart the broker",
                self.log.partition
            ))),
            None => Ok(()),
        }
    }

    fn write_out(&mut self) -> io::Result<()> {
        self.check_poisoned()?;
        if !self.out.is_empty() {
            write_all_bufs(&self.file, &self.out)?;
            self.pos += self.out_len as u64;
            self.out.clear();
            self.out_len = 0;
            self.needs_sync = true;
            self.active.set_written(self.pos, self.next_offset);
            self.log.set_written_high(self.next_offset);
        }
        if !self.index_buf.is_empty() {
            (&*self.index_file).write_all(&self.index_buf)?;
            self.index_buf.clear();
        }
        Ok(())
    }

    fn timed_sync(&mut self, f: &File) -> io::Result<()> {
        let t = Instant::now();
        f.sync_data()?;
        self.fsync_times.push(t.elapsed());
        Ok(())
    }

    /// Seals the active segment and starts a new one. Only the sealed segment's data is
    /// synced here, before the new file exists, so a durable later segment never follows a
    /// torn earlier one (no offset gap after a crash). The new directory entry and the sealed
    /// index are made durable by the next commit ([`prepare_commit`](Self::prepare_commit)),
    /// before anything in the new segment is acknowledged — off the append path, concurrently
    /// with the commit's other fsyncs.
    fn roll(&mut self) -> io::Result<()> {
        self.write_out()?;
        if self.cfg.sync {
            // Unconditional: data written before a pipelined commit took its mark may still
            // be in flight, and must be durable before the next segment exists. Through a
            // description of its own: a commit's fsync of this file may be running (see
            // `prepare_commit` on `errseq_t`).
            let f = open_sync(&self.active.log_path)?;
            self.timed_sync(&f)?;
            self.deferred.push(SyncOp::File {
                file: self.index_file.clone(),
                timed: false,
            });
        }
        self.sealed_unpublished.push((
            self.active.clone(),
            self.pos,
            self.next_offset,
            self.seg_min_ts,
            self.seg_max_ts,
        ));
        let seg = Arc::new(create_segment(&self.log.dir, self.next_offset, false)?);
        if self.cfg.sync {
            self.deferred.push(SyncOp::Dir(self.log.dir.clone()));
        }
        self.set_active(seg.clone())?;
        self.log.segments.write().push(seg);
        self.pos = 0;
        self.bytes_since_index = INDEX_INTERVAL_BYTES;
        self.seg_min_ts = u64::MAX;
        self.seg_max_ts = 0;
        self.needs_sync = false;
        Ok(())
    }

    /// Whether several commit marks of this partition may be in flight at once (their fsyncs
    /// overlap); see [`prepare_commit`](Self::prepare_commit).
    pub fn set_overlapping_syncs(&mut self, on: bool) {
        self.overlapping_syncs = on;
    }

    /// Encoded bytes not yet written to the OS.
    pub fn buffered(&self) -> usize {
        self.out_len
    }

    /// Writes buffered entries to the OS (write(2)).
    pub fn flush(&mut self) -> io::Result<()> {
        self.write_out()
    }

    /// Flushes and makes everything written so far durable on the calling thread (the
    /// deferred roll work, then the active segment's data if anything was written since the
    /// last sync).
    pub fn sync(&mut self) -> io::Result<()> {
        self.write_out()?;
        for op in std::mem::take(&mut self.deferred) {
            crate::fsync_pool::run_chain(std::slice::from_ref(&op))?;
        }
        if self.needs_sync {
            let f = self.file.clone();
            self.timed_sync(&f)?;
            self.needs_sync = false;
        }
        Ok(())
    }

    fn current_state(&self) -> Checkpoint {
        Checkpoint {
            active_base: self.active.base_offset,
            pos: self.pos,
            next_offset: self.next_offset,
            last_ts: self.last_ts,
            bytes_since_index: self.bytes_since_index,
            seg_min_ts: self.seg_min_ts,
            seg_max_ts: self.seg_max_ts,
        }
    }

    fn take_mark(&mut self, ops: Vec<SyncOp>) -> CommitMark {
        CommitMark {
            ops,
            state: self.current_state(),
            active: self.active.clone(),
            sealed: std::mem::take(&mut self.sealed_unpublished),
            entries: std::mem::take(&mut self.pending),
        }
    }

    /// Writes buffered entries to the OS and takes a [`CommitMark`] covering everything
    /// appended so far. With `sync`, the mark's `ops` hold the fsyncs it still needs (the
    /// active file if written since the last mark, plus deferred roll work); the caller runs
    /// them (inline or on the fsync pool) and calls [`publish_mark`](Self::publish_mark) on
    /// success or [`rollback`](Self::rollback) on failure. The writer can keep appending
    /// meanwhile.
    pub fn prepare_commit(&mut self, sync: bool) -> io::Result<CommitMark> {
        self.write_out()?;
        let mut ops = std::mem::take(&mut self.deferred);
        if sync && self.needs_sync {
            let fail = self.log.fail_next_fsync.swap(u64::MAX, Ordering::AcqRel);
            if fail != u64::MAX {
                // First in the chain (Apple turns the last file op into the barrier).
                ops.insert(0, SyncOp::Fail(Duration::from_millis(fail)));
            }
            let delay = self.log.delay_fsync.load(Ordering::Acquire);
            if delay > 0 {
                ops.insert(0, SyncOp::Delay(Duration::from_millis(delay)));
            }
            let once = self.log.delay_next_fsync.swap(u64::MAX, Ordering::AcqRel);
            if once != u64::MAX {
                ops.insert(0, SyncOp::Delay(Duration::from_millis(once)));
            }
            // Linux reports a writeback error once per open file description (`errseq_t`):
            // of two overlapping fsyncs through one description, the *later* one may consume
            // the error of a page the earlier one covers, which then returns success. So when
            // a commit's fsync may overlap another one of this file (`overlapping_syncs`:
            // more than one mark in flight per partition), it gets a description of its own,
            // which reports every error recorded after it was opened (or not yet seen by
            // anyone then). Otherwise this file's commit fsyncs never overlap (the next mark
            // is only submitted after the previous one finished, and `roll` syncs through a
            // description of its own), so the writer's description is reused.
            // See docs/design/v0.9-commit-path.md §3.
            let file = if self.overlapping_syncs {
                Arc::new(open_sync(&self.active.log_path)?)
            } else {
                self.file.clone()
            };
            ops.push(SyncOp::File { file, timed: true });
        }
        if !sync {
            ops.clear();
        }
        self.needs_sync = false;
        Ok(self.take_mark(ops))
    }

    /// Makes everything written so far visible to readers and the dispatcher. Must be called
    /// only after the data is durable per policy. Returns the newly visible entries.
    #[cfg(test)]
    pub fn publish(&mut self) -> Vec<Arc<CachedEntry>> {
        debug_assert!(self.out.is_empty(), "publish before flush");
        let mark = self.take_mark(Vec::new());
        self.publish_mark(mark)
    }

    /// Publishes `mark` (taken by [`prepare_commit`](Self::prepare_commit), whose ops all
    /// succeeded): its records become visible and it becomes the rollback point. Marks must
    /// be published in the order they were taken. Returns the newly visible entries (none
    /// for replicated appends).
    pub fn publish_mark(&mut self, mark: CommitMark) -> Vec<Arc<CachedEntry>> {
        let CommitMark {
            state,
            active,
            sealed,
            entries,
            ..
        } = mark;
        for (seg, len, next, min, max) in sealed {
            seg.publish(len, next, min, max);
        }
        active.publish(
            state.pos,
            state.next_offset,
            state.seg_min_ts,
            state.seg_max_ts,
        );
        let mut n = 0u64;
        {
            let mut cache = self.log.cache.lock();
            for e in &entries {
                n += u64::from(e.count);
                cache.push(e.clone());
            }
        }
        self.log.published_total.fetch_add(n, Ordering::Relaxed);
        self.log.high.store(state.next_offset, Ordering::Release);
        self.checkpoint = state;
        entries
    }

    /// Discards everything since the last publish after an I/O error — every unpublished
    /// [`CommitMark`] included — restoring files and in-memory state to the last published
    /// point.
    pub fn rollback(&mut self) -> io::Result<()> {
        let served = self.log.unsynced_served.load(Ordering::Acquire);
        let res = self.try_rollback();
        if let Err(e) = &res {
            self.poisoned = Some(e.to_string());
        } else if served > self.checkpoint.next_offset {
            // A follower may hold the discarded offsets: never reassign them (to different
            // data) in this process.
            self.poisoned = Some(format!(
                "offsets {}..{served} were replicated before their fsync failed",
                self.checkpoint.next_offset
            ));
        }
        res
    }

    /// In-memory state is restored first (infallible); the files are then shrunk with
    /// `set_len`, which needs no free space (rollback must work on a full disk).
    fn try_rollback(&mut self) -> io::Result<()> {
        let cp = self.checkpoint;
        self.out.clear();
        self.out_len = 0;
        self.index_buf.clear();
        self.pending.clear();
        self.sealed_unpublished.clear();
        self.deferred.clear();
        // Segments are only appended by this writer and always with a base offset greater
        // than the previous active segment's; retention only removes from the front. So the
        // segments created since the checkpoint are exactly those with a larger base.
        let (extra, active) = {
            let mut segs = self.log.segments.write();
            let keep = segs
                .iter()
                .position(|s| s.base_offset > cp.active_base)
                .unwrap_or(segs.len());
            let extra: Vec<Arc<Segment>> = segs.drain(keep..).collect();
            (extra, segs.last().cloned())
        };
        let active = active.ok_or_else(|| io::Error::other("no active segment"))?;
        // Unsynced readers stop seeing the discarded entries before the files shrink.
        self.log
            .written_high
            .store(cp.next_offset, Ordering::Release);
        let kept_entries = active.index.truncate_from_position(cp.pos);
        active.reset_visibility(cp.pos, cp.next_offset);
        self.pos = cp.pos;
        self.next_offset = cp.next_offset;
        self.last_ts = cp.last_ts;
        self.bytes_since_index = cp.bytes_since_index;
        self.seg_min_ts = cp.seg_min_ts;
        self.seg_max_ts = cp.seg_max_ts;
        self.needs_sync = false;
        for s in extra {
            let _ = fs::remove_file(&s.log_path);
            let _ = fs::remove_file(&s.index_path);
        }
        // Files: the index file holds at least the kept entries (they were written at the
        // checkpoint's commit), so shrinking it yields exactly the in-memory index.
        OpenOptions::new()
            .write(true)
            .open(&active.log_path)?
            .set_len(cp.pos)?;
        let idx = OpenOptions::new().write(true).open(&active.index_path)?;
        let idx_len = idx.metadata()?.len();
        idx.set_len(idx_len.min((kept_entries * super::index::ENTRY_SIZE) as u64))?;
        self.set_active(active)?;
        Ok(())
    }

    /// Flushes and syncs the index of the active segment (shutdown).
    pub fn close(&mut self) -> io::Result<()> {
        self.sync()?;
        self.index_file.sync_data()?;
        Ok(())
    }
}

/// Fills the 32-byte header reserved at the start of `buf` (header + records section) and
/// computes the CRC-32C over bytes `[8, len)`.
fn seal_envelope(buf: &mut [u8], base: u64, ts: u64, count: u32, compression: Compression) {
    let mut h = EnvelopeHeader {
        len: (buf.len() - 4) as u32,
        crc32c: 0,
        base_offset: base,
        base_timestamp_ms: ts,
        record_count: count,
        compression,
    };
    let mut hdr = [0u8; ENVELOPE_HEADER_LEN];
    h.write(&mut hdr);
    h.crc32c = codec::crc32c_append(codec::crc32c(&hdr[8..]), &buf[ENVELOPE_HEADER_LEN..]);
    h.write(&mut hdr);
    buf[..ENVELOPE_HEADER_LEN].copy_from_slice(&hdr);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;

    use super::*;

    fn cfg(segment_bytes: u64) -> LogConfig {
        LogConfig {
            segment_bytes,
            sync: true,
            roll_jitter: false,
            cache_bytes: DEFAULT_CACHE_BYTES,
        }
    }

    fn msg(i: u64, key: Option<&str>) -> PublishMessage {
        let mut headers = BTreeMap::new();
        if i.is_multiple_of(3) {
            headers.insert("h".to_string(), format!("v{i}"));
        }
        PublishMessage {
            key: key.map(str::to_string),
            headers,
            payload: Bytes::from(format!("payload-{i:06}")),
        }
    }

    fn append_n(w: &mut PartitionWriter, from: u64, n: u64, ts: u64) {
        for i in from..from + n {
            w.append(ts + i, msg(i, Some(&format!("k{}", i % 5))))
                .unwrap();
        }
        w.sync().unwrap();
        w.publish();
    }

    fn collect(log: &PartitionLog, start: u64) -> Vec<Record> {
        let mut out = Vec::new();
        log.scan(start, |r| {
            out.push(r);
            ControlFlow::Continue(())
        })
        .unwrap();
        out
    }

    #[test]
    fn append_read_roll() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1024)).unwrap();
        assert_eq!((log.low(), log.high()), (0, 0));
        w.append(1, msg(0, None)).unwrap();
        w.sync().unwrap();
        // Not visible until published.
        assert_eq!(log.high(), 0);
        assert!(collect(&log, 0).is_empty());
        w.publish();
        assert_eq!(log.high(), 1);
        append_n(&mut w, 1, 499, 1000);
        assert_eq!(log.high(), 500);
        assert!(
            log.segment_count() > 5,
            "rolled into {} segments",
            log.segment_count()
        );
        let all = collect(&log, 0);
        assert_eq!(all.len(), 500);
        for (i, r) in all.iter().enumerate() {
            assert_eq!(r.offset, i as u64);
            assert_eq!(&r.payload[..], format!("payload-{i:06}").as_bytes());
        }
        // Seek into the middle (index lookup) and the cache.
        let mid = collect(&log, 333);
        assert_eq!(mid.first().unwrap().offset, 333);
        assert_eq!(mid.len(), 167);
        assert_eq!(log.get(250).unwrap().unwrap().offset, 250);
        assert!(log.get(500).unwrap().is_none());
        // Time seek: timestamps are 1000 + i (monotonic).
        assert_eq!(log.offset_for_time(1000 + 400).unwrap(), 400);
        assert!(log.offset_for_time_approx(1400) <= 400);
        assert_eq!(log.offset_for_time(1_000_000).unwrap(), 500);
        let batch = log.read_batch(10, 20, usize::MAX).unwrap();
        assert_eq!(batch.len(), 20);
        assert_eq!(batch[0].offset, 10);
    }

    #[test]
    fn timestamps_are_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
        w.append(100, msg(0, None)).unwrap();
        w.append(50, msg(1, None)).unwrap();
        w.sync().unwrap();
        w.publish();
        let r = collect(&log, 0);
        assert_eq!(r[1].timestamp_ms, 100);
    }

    #[test]
    fn recovery_truncates_torn_tail_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (_log, mut w) = PartitionLog::open(dir.path(), 3, cfg(2048)).unwrap();
            append_n(&mut w, 0, 100, 1000);
            w.close().unwrap();
        }
        // Tear the active segment: append half a record of garbage.
        let mut logs: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "log"))
            .collect();
        logs.sort();
        let last = logs.last().unwrap().clone();
        let good_len = fs::metadata(&last).unwrap().len();
        {
            let mut buf = Vec::new();
            record::encode_into(
                &mut buf,
                100,
                5000,
                Some("x"),
                &BTreeMap::new(),
                b"torn-record-payload",
            );
            let mut f = OpenOptions::new().append(true).open(&last).unwrap();
            f.write_all(&buf[..buf.len() / 2]).unwrap();
        }
        // Also delete one sealed index to force a rebuild.
        fs::remove_file(logs[0].with_extension("idx")).unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 3, cfg(2048)).unwrap();
        assert_eq!(fs::metadata(&last).unwrap().len(), good_len);
        assert_eq!(log.high(), 100);
        assert_eq!(collect(&log, 0).len(), 100);
        assert_eq!(collect(&log, 57)[0].offset, 57);
        // Appends continue at the right offset with monotonic timestamps.
        append_n(&mut w, 100, 10, 0);
        let all = collect(&log, 0);
        assert_eq!(all.len(), 110);
        assert!(
            all.windows(2)
                .all(|w| w[0].offset + 1 == w[1].offset && w[0].timestamp_ms <= w[1].timestamp_ms)
        );
    }

    #[test]
    fn recovery_with_corrupt_crc_in_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (_log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
            append_n(&mut w, 0, 10, 1000);
        }
        let path = dir.path().join(log_file_name(0));
        let mut bytes = fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n - 3] ^= 0x55; // corrupt the last record's payload
        fs::write(&path, &bytes).unwrap();
        let (log, _w) = PartitionLog::open(dir.path(), 0, cfg(1 << 20)).unwrap();
        assert_eq!(log.high(), 9);
        assert_eq!(collect(&log, 0).len(), 9);
    }

    #[test]
    fn retention_removes_whole_old_segments_only() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1024)).unwrap();
        append_n(&mut w, 0, 200, 1000);
        let segs = log.segment_count();
        assert!(segs > 3);
        // Remove everything older than ts 1100 (whole segments only).
        let (removed, bytes) = log
            .remove_oldest_while(|s| s.max_ts().is_some_and(|t| t < 1100))
            .unwrap();
        assert!(removed > 0 && bytes > 0);
        assert!(log.low() <= 100 && log.low() > 0);
        assert_eq!(collect(&log, 0).first().unwrap().offset, log.low());
        // Even an always-true predicate keeps the active segment.
        log.remove_oldest_while(|_| true).unwrap();
        assert_eq!(log.segment_count(), 1);
        assert_eq!(log.high(), 200);
        // Reopen after retention.
        drop(w);
        let (log2, _w) = PartitionLog::open(dir.path(), 0, cfg(1024)).unwrap();
        assert_eq!(log2.high(), 200);
        assert_eq!(log2.low(), log.low());
    }

    #[test]
    fn rollback_discards_unpublished() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(512)).unwrap();
        append_n(&mut w, 0, 5, 1000);
        for i in 5..50 {
            w.append(2000, msg(i, None)).unwrap();
        }
        w.flush().unwrap();
        w.rollback().unwrap();
        assert_eq!(log.high(), 5);
        append_n(&mut w, 5, 5, 3000);
        let all = collect(&log, 0);
        assert_eq!(all.len(), 10);
        assert!(all.iter().enumerate().all(|(i, r)| r.offset == i as u64));
        drop(w);
        let (log2, _w) = PartitionLog::open(dir.path(), 0, cfg(512)).unwrap();
        assert_eq!(log2.high(), 10);
    }

    /// Copies the whole log of `src` into `dst` through raw chunks of ~`max` bytes.
    fn replicate(src: &PartitionLog, w: &mut PartitionWriter, max: usize) {
        while let Some(c) = src.read_raw(w.next_offset(), max).unwrap() {
            assert_eq!(c.base_offset, w.next_offset());
            let end = w.append_raw(&c.bytes).unwrap();
            assert_eq!(end, c.base_offset + c.count);
            w.sync().unwrap();
            w.publish();
        }
    }

    #[test]
    fn raw_replication_roundtrip() {
        let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (src, mut ws) = PartitionLog::open(d1.path(), 0, cfg(1024)).unwrap();
        append_n(&mut ws, 0, 300, 1000);
        // Different segment size on the follower: boundaries differ, content must not.
        let (dst, mut wd) = PartitionLog::open(d2.path(), 0, cfg(700)).unwrap();
        replicate(&src, &mut wd, 333);
        assert_eq!(dst.high(), 300);
        assert_eq!(collect(&dst, 0), collect(&src, 0));
        assert_eq!(collect(&dst, 123)[0].offset, 123);
        assert_eq!(dst.offset_for_time(1000 + 200).unwrap(), 200);
        // Tiny max_bytes still makes progress (one whole record).
        let c = src.read_raw(5, 1).unwrap().unwrap();
        assert_eq!(c.count, 1);
        // Non-contiguous and corrupt chunks are rejected without appending anything.
        let c = src.read_raw(10, 4096).unwrap().unwrap();
        assert!(wd.append_raw(&c.bytes).is_err());
        let mut bad = src.read_raw(0, 4096).unwrap().unwrap().bytes.to_vec();
        let n = bad.len();
        bad[n - 1] ^= 1;
        let (_d3, mut w3) = {
            let d3 = tempfile::tempdir().unwrap();
            let (_l, w) = PartitionLog::open(d3.path(), 0, cfg(1 << 20)).unwrap();
            (d3, w)
        };
        assert!(w3.append_raw(&Bytes::from(bad)).is_err());
        assert_eq!(w3.next_offset(), 0);
        // Reopen the follower copy: recovery accepts the replicated format.
        drop(wd);
        let (dst2, _w) = PartitionLog::open(d2.path(), 0, cfg(700)).unwrap();
        assert_eq!(collect(&dst2, 0), collect(&src, 0));
        assert!(src.read_raw(300, 10).unwrap().is_none());
    }

    #[test]
    fn truncate_then_continue_and_reopen() {
        for at in [0u64, 1, 57, 100, 199, 200] {
            let dir = tempfile::tempdir().unwrap();
            let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1024)).unwrap();
            append_n(&mut w, 0, 200, 1000);
            // Make sure the truncation point sits on/inside several segment layouts.
            w.truncate(at).unwrap();
            assert_eq!(log.high(), at, "at {at}");
            assert_eq!(collect(&log, 0).len() as u64, at);
            assert!(log.get(at).unwrap().is_none());
            if at > 0 {
                assert_eq!(log.get(at - 1).unwrap().unwrap().offset, at - 1);
            }
            // Appends continue at `at` with fresh content.
            for i in at..at + 30 {
                w.append(5000 + i, msg(i + 1000, None)).unwrap();
            }
            w.sync().unwrap();
            w.publish();
            let all = collect(&log, 0);
            assert_eq!(all.len() as u64, at + 30);
            assert!(all.iter().enumerate().all(|(i, r)| r.offset == i as u64));
            assert_eq!(
                &all[at as usize].payload[..],
                format!("payload-{:06}", at + 1000).as_bytes()
            );
            drop(w);
            let (log2, _w) = PartitionLog::open(dir.path(), 0, cfg(1024)).unwrap();
            assert_eq!(collect(&log2, 0), all, "reopen after truncate at {at}");
        }
    }

    #[test]
    fn truncate_outside_range_resets() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(1024)).unwrap();
        append_n(&mut w, 0, 50, 1000);
        w.truncate(500).unwrap();
        assert_eq!((log.low(), log.high()), (500, 500));
        assert!(collect(&log, 0).is_empty());
        w.append(1, msg(1, None)).unwrap();
        w.sync().unwrap();
        w.publish();
        assert_eq!(collect(&log, 0)[0].offset, 500);
        drop(w);
        let (log2, _w) = PartitionLog::open(dir.path(), 0, cfg(1024)).unwrap();
        assert_eq!((log2.low(), log2.high()), (500, 501));
    }

    fn run_ops(m: &CommitMark) {
        for op in &m.ops {
            crate::fsync_pool::run_chain(std::slice::from_ref(op)).unwrap();
        }
    }

    fn log_files(dir: &Path) -> Vec<PathBuf> {
        let mut logs: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "log"))
            .collect();
        logs.sort();
        logs
    }

    #[test]
    fn pipelined_marks_publish_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(700)).unwrap();
        for i in 0..20 {
            w.append(1000 + i, msg(i, None)).unwrap();
        }
        let a = w.prepare_commit(true).unwrap();
        assert!(!a.ops.is_empty());
        // Batch B is appended (rolling segments) while A's fsync is "in flight".
        for i in 20..60 {
            w.append(1000 + i, msg(i, None)).unwrap();
        }
        let b = w.prepare_commit(true).unwrap();
        assert!(
            b.ops.iter().any(|o| matches!(o, SyncOp::Dir(_))),
            "a roll defers the directory fsync to the commit"
        );
        assert_eq!(log.high(), 0, "nothing visible before its fsync");
        run_ops(&a);
        assert_eq!(w.publish_mark(a).len(), 20);
        assert_eq!(log.high(), 20);
        assert_eq!(collect(&log, 0).len(), 20, "B stays invisible");
        run_ops(&b);
        assert_eq!(w.publish_mark(b).len(), 40);
        assert_eq!(log.high(), 60);
        let all = collect(&log, 0);
        assert!(all.iter().enumerate().all(|(i, r)| r.offset == i as u64));
        drop(w);
        let (log2, _w) = PartitionLog::open(dir.path(), 0, cfg(700)).unwrap();
        assert_eq!(collect(&log2, 0), all);
    }

    #[test]
    fn rollback_discards_every_unpublished_mark() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(700)).unwrap();
        append_n(&mut w, 0, 10, 1000);
        let segs_before = log.segment_count();
        // Two outstanding marks (A in flight, B taken), then A's fsync "fails".
        for i in 10..30 {
            w.append(2000 + i, msg(i, None)).unwrap();
        }
        let a = w.prepare_commit(true).unwrap();
        for i in 30..70 {
            w.append(2000 + i, msg(i, None)).unwrap();
        }
        let b = w.prepare_commit(true).unwrap();
        // The current (third) batch has unflushed appends too.
        w.append(5000, msg(70, None)).unwrap();
        drop((a, b));
        w.rollback().unwrap();
        assert_eq!(log.high(), 10);
        assert_eq!(w.next_offset(), 10);
        assert_eq!(log.segment_count(), segs_before);
        // A published A, then B fails: rollback keeps A.
        for i in 10..30 {
            w.append(3000 + i, msg(i + 100, None)).unwrap();
        }
        let a = w.prepare_commit(true).unwrap();
        for i in 30..50 {
            w.append(3000 + i, msg(i + 100, None)).unwrap();
        }
        let b = w.prepare_commit(true).unwrap();
        run_ops(&a);
        w.publish_mark(a);
        drop(b);
        w.rollback().unwrap();
        assert_eq!(log.high(), 30);
        append_n(&mut w, 30, 5, 9000);
        let all = collect(&log, 0);
        assert_eq!(all.len(), 35);
        assert!(all.iter().enumerate().all(|(i, r)| r.offset == i as u64));
        assert_eq!(&all[10].payload[..], b"payload-000110");
        drop(w);
        let (log2, _w) = PartitionLog::open(dir.path(), 0, cfg(700)).unwrap();
        assert_eq!(collect(&log2, 0), all);
    }

    /// Crash while a pipelined batch is unsynced: only a prefix of it may survive, never a
    /// gap, and everything published before stays.
    #[test]
    fn crash_recovery_with_pipelined_commit() {
        for tear in ["mid-record", "new-segment-lost", "new-segment-empty"] {
            let dir = tempfile::tempdir().unwrap();
            let published;
            {
                let (_log, mut w) = PartitionLog::open(dir.path(), 0, cfg(700)).unwrap();
                append_n(&mut w, 0, 15, 1000);
                for i in 15..25 {
                    w.append(2000 + i, msg(i, None)).unwrap();
                }
                let a = w.prepare_commit(true).unwrap();
                run_ops(&a);
                w.publish_mark(a);
                published = 25;
                // Batch B: written (and rolled), its fsync never completes.
                for i in 25..60 {
                    w.append(3000 + i, msg(i, None)).unwrap();
                }
                let _b = w.prepare_commit(true).unwrap();
                // Crash: the writer is dropped without close.
            }
            let logs = log_files(dir.path());
            let last = logs.last().unwrap().clone();
            match tear {
                "mid-record" => {
                    let len = fs::metadata(&last).unwrap().len();
                    OpenOptions::new()
                        .write(true)
                        .open(&last)
                        .unwrap()
                        .set_len(len - 7)
                        .unwrap();
                }
                "new-segment-lost" => {
                    // The roll's directory entry never became durable.
                    fs::remove_file(&last).unwrap();
                    let _ = fs::remove_file(last.with_extension("idx"));
                }
                _ => fs::write(&last, b"").unwrap(),
            }
            let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(700)).unwrap();
            let high = log.high();
            assert!((published..60).contains(&high), "{tear}: high {high}");
            let all = collect(&log, 0);
            assert_eq!(all.len() as u64, high, "{tear}");
            assert!(all.iter().enumerate().all(|(i, r)| r.offset == i as u64));
            append_n(&mut w, high, 3, 9000);
            assert_eq!(collect(&log, 0).len() as u64, high + 3);
        }
    }

    #[test]
    fn mixed_segment_sizes_recover_and_replicate() {
        let dir = tempfile::tempdir().unwrap();
        let mut n = 0u64;
        // Old data with large segments, then smaller, then larger again.
        for (seg, count) in [(64 * 1024, 300u64), (1024, 200), (2048, 150), (1 << 20, 50)] {
            let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(seg)).unwrap();
            assert_eq!(log.high(), n);
            append_n(&mut w, n, count, 1000);
            n += count;
            w.close().unwrap();
        }
        let (log, _w) = PartitionLog::open(dir.path(), 0, cfg(4096)).unwrap();
        assert_eq!(log.high(), n);
        let all = collect(&log, 0);
        assert_eq!(all.len() as u64, n);
        assert!(all.iter().enumerate().all(|(i, r)| r.offset == i as u64));
        for at in [0, 299, 300, 450, 499, 500, 649, 650, n - 1] {
            assert_eq!(collect(&log, at)[0].offset, at);
        }
        assert_eq!(log.offset_for_time(1000 + 520).unwrap(), 520);
        // Replication across segment boundaries of different sizes.
        let d2 = tempfile::tempdir().unwrap();
        let (dst, mut wd) = PartitionLog::open(d2.path(), 0, cfg(3000)).unwrap();
        replicate(&log, &mut wd, 5000);
        assert_eq!(collect(&dst, 0), all);
        // Size retention over mixed sizes keeps whole segments.
        let (removed, _) = log.remove_oldest_while(|s| s.base_offset < 400).unwrap();
        assert!(removed > 0);
        // Whole segments only: the first kept segment starts at or after 400 and still
        // holds everything from its base on.
        let low = log.low();
        assert!((400..n).contains(&low), "low {low}");
        assert_eq!(collect(&log, 0).len() as u64, n - low);
    }

    #[test]
    fn roll_jitter_is_deterministic_and_bounded() {
        let lc = LogConfig {
            segment_bytes: 64 << 20,
            sync: false,
            roll_jitter: true,
            cache_bytes: DEFAULT_CACHE_BYTES,
        };
        let (lo, hi) = ((64u64 << 20) * 9 / 10, (64u64 << 20) * 11 / 10);
        let mut distinct = std::collections::BTreeSet::new();
        for p in 0..8u32 {
            let seed = jitter_seed(Path::new("/data/channels/orders/p0"), p);
            for base in [0u64, 12345, 99_999_999] {
                let r = lc.roll_size(seed, base);
                assert!((lo..=hi).contains(&r), "{r}");
                assert_eq!(r, lc.roll_size(seed, base), "deterministic");
                distinct.insert(r);
            }
        }
        assert!(
            distinct.len() > 16,
            "jitter spreads roll sizes: {distinct:?}"
        );
        // The data directory location does not matter, the channel/partition does.
        assert_eq!(
            jitter_seed(Path::new("/a/orders/p3"), 3),
            jitter_seed(Path::new("/b/c/orders/p3"), 3)
        );
        assert_ne!(
            jitter_seed(Path::new("/a/orders/p3"), 3),
            jitter_seed(Path::new("/a/events/p3"), 3)
        );
        let off = LogConfig {
            roll_jitter: false,
            ..lc
        };
        assert_eq!(off.roll_size(1, 2), 64 << 20);
        // Partitions written identically roll at different points.
        let mut layouts = std::collections::BTreeSet::new();
        for p in 0..4u32 {
            let dir = tempfile::tempdir().unwrap();
            let pdir = dir.path().join(format!("p{p}"));
            let lc = LogConfig {
                segment_bytes: 4096,
                sync: false,
                roll_jitter: true,
                cache_bytes: DEFAULT_CACHE_BYTES,
            };
            let (log, mut w) = PartitionLog::open(&pdir, p, lc).unwrap();
            append_n(&mut w, 0, 400, 1000);
            let bases: Vec<u64> = log.segments.read().iter().map(|s| s.base_offset).collect();
            layouts.insert(bases);
        }
        assert!(
            layouts.len() > 1,
            "jittered partitions roll at different offsets"
        );
    }

    #[test]
    fn paced_removal_and_stepwise_shrink() {
        let dir = tempfile::tempdir().unwrap();
        let (log, mut w) = PartitionLog::open(dir.path(), 0, cfg(4 << 20)).unwrap();
        // ~3 segments of ~4 MiB each plus the active one.
        let big = Bytes::from(vec![7u8; 64 * 1024]);
        for i in 0..230u64 {
            w.append(
                1000 + i,
                PublishMessage {
                    key: None,
                    headers: BTreeMap::new(),
                    payload: big.clone(),
                },
            )
            .unwrap();
        }
        w.sync().unwrap();
        w.publish();
        let n = log.segment_count();
        assert!(n >= 4, "{n} segments");
        assert_eq!(log.count_removable(|_| true), n as usize - 1);
        // At most one segment per call.
        let (removed, bytes) = log.remove_oldest(1, |_| true, None).unwrap();
        assert_eq!((removed, log.segment_count()), (1, n - 1));
        assert!(bytes > 3 << 20);
        // A segment a reader still holds is only unlinked (the reader keeps the full file).
        let held = log.segments.read()[0].clone();
        let held_len = held.visible_len();
        log.remove_oldest(1, |_| true, Some((1 << 20, Duration::ZERO)))
            .unwrap();
        assert!(!held.log_path.exists());
        let mut buf = vec![0u8; 16];
        assert_eq!(held.read_at(&mut buf, held_len - 16).unwrap(), 16);
        // An unreferenced one is shrunk in steps, then unlinked.
        let next = log.segments.read()[0].log_path.clone();
        log.remove_oldest(1, |_| true, Some((1 << 20, Duration::from_millis(1))))
            .unwrap();
        assert!(!next.exists());
        assert_eq!(log.high(), 230);
        assert_eq!(collect(&log, 0).first().unwrap().offset, log.low());
    }

    #[test]
    #[ignore]
    fn writer_cpu_microbench() {
        fn thread_cpu_ns() -> u64 {
            let mut ts = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
            ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
        }
        for size in [100usize, 1024] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = cfg(64 << 20);
            c.sync = false;
            let (_log, mut w) = PartitionLog::open(dir.path(), 0, c).unwrap();
            let payload = Bytes::from(vec![b'x'; size]);
            let reqs = 3000usize;
            let per = 200usize;
            let mut body = Vec::new();
            for i in 0..per {
                batch::encode_record(&mut body, i as u32, None, &BTreeMap::new(), &payload);
            }
            let sec = RecordsSection {
                compression: Compression::None,
                record_count: per as u32,
                body: Bytes::from(body),
            };
            let t0 = thread_cpu_ns();
            let w0 = std::time::Instant::now();
            for _ in 0..reqs {
                let msgs: Vec<PublishMessage> = (0..per)
                    .map(|_| PublishMessage {
                        key: None,
                        headers: Default::default(),
                        payload: payload.clone(),
                    })
                    .collect();
                if std::env::var("MICRO_BATCH").is_ok() {
                    drop(msgs);
                    w.append_batch(1, &sec).unwrap();
                } else {
                    w.append_messages(1, msgs).unwrap();
                }
                let m = w.prepare_commit(false).unwrap();
                let _ = w.publish_mark(m);
            }
            let ns = thread_cpu_ns() - t0;
            println!(
                "MICRO size={size}: {:.0} ns CPU/msg, wall {:?}",
                ns as f64 / (reqs * per) as f64,
                w0.elapsed()
            );
        }
    }
}

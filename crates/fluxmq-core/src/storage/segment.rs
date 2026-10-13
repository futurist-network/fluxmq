//! A single log segment: `{base_offset:020}.log` + `{base_offset:020}.idx`.
//!
//! Invariants:
//! * Records in a segment have consecutive offsets starting at `base_offset` (a corrupt
//!   region in a sealed segment ends its readable range; see recovery in `log.rs`).
//! * `visible_len` / `next_offset` only grow, and only cover bytes that the writer has fully
//!   written (and fsynced, under group commit). Readers never look past them, so they never
//!   observe a partially written record.
//! * Readers use positional reads on a dedicated read-only handle, so they never contend with
//!   the writer's file cursor.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;

use super::entry::{self, Entry};
use super::index::SparseIndex;
use super::record::DecodeError;

const READ_CHUNK: usize = 64 * 1024;

pub fn log_file_name(base: u64) -> String {
    format!("{base:020}.log")
}

pub fn index_file_name(base: u64) -> String {
    format!("{base:020}.idx")
}

/// Parses `{base}.log` file names.
pub fn parse_log_file_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".log")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

#[derive(Debug)]
pub struct Segment {
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    reader: File,
    visible_len: AtomicU64,
    next_offset: AtomicU64,
    /// Written (`write(2)` finished) but maybe not yet durable: `>= visible`. Only unsynced
    /// reads (replication of a leader, see `PartitionLog::written_high`) look past visible.
    written_len: AtomicU64,
    written_next: AtomicU64,
    min_ts: AtomicU64,
    max_ts: AtomicU64,
    pub index: SparseIndex,
    /// Backup cuts referencing this segment (see [`SegmentPin`]); retention never removes a
    /// pinned segment.
    pins: AtomicU32,
}

/// Keeps a segment out of retention while a backup cut references it. Pins are taken under
/// the partition's segment-list read lock (retention removes under the write lock), so a
/// segment is either pinned before retention looks at it or already gone from the cut.
#[derive(Debug)]
pub struct SegmentPin(std::sync::Arc<Segment>);

impl SegmentPin {
    pub fn new(seg: std::sync::Arc<Segment>) -> Self {
        seg.pins.fetch_add(1, Ordering::AcqRel);
        SegmentPin(seg)
    }

    pub fn segment(&self) -> &Segment {
        &self.0
    }
}

impl Drop for SegmentPin {
    fn drop(&mut self) {
        self.0.pins.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Segment {
    pub fn new(dir: &Path, base_offset: u64, reader: File, index: SparseIndex) -> Self {
        Self {
            base_offset,
            log_path: dir.join(log_file_name(base_offset)),
            index_path: dir.join(index_file_name(base_offset)),
            reader,
            visible_len: AtomicU64::new(0),
            next_offset: AtomicU64::new(base_offset),
            written_len: AtomicU64::new(0),
            written_next: AtomicU64::new(base_offset),
            min_ts: AtomicU64::new(u64::MAX),
            max_ts: AtomicU64::new(0),
            index,
            pins: AtomicU32::new(0),
        }
    }

    /// Whether a backup cut currently references this segment.
    pub fn is_pinned(&self) -> bool {
        self.pins.load(Ordering::Acquire) > 0
    }

    /// Positional read of the segment file (backups). The caller bounds `pos` by a length it
    /// captured; works after the file was unlinked (the handle stays open).
    pub fn read_at(&self, buf: &mut [u8], pos: u64) -> io::Result<usize> {
        self.reader.read_at(buf, pos)
    }

    pub fn visible_len(&self) -> u64 {
        self.visible_len.load(Ordering::Acquire)
    }

    /// Offset after the last visible record.
    pub fn next_offset(&self) -> u64 {
        self.next_offset.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.next_offset() == self.base_offset
    }

    pub fn min_ts(&self) -> Option<u64> {
        let v = self.min_ts.load(Ordering::Acquire);
        (v != u64::MAX).then_some(v)
    }

    pub fn max_ts(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            Some(self.max_ts.load(Ordering::Acquire))
        }
    }

    /// Publishes newly written records. Order matters: length/timestamps first, then the
    /// offset (readers bound by offset then always find the bytes).
    pub fn publish(&self, len: u64, next_offset: u64, min_ts: u64, max_ts: u64) {
        self.written_len.fetch_max(len, Ordering::AcqRel);
        self.written_next.fetch_max(next_offset, Ordering::AcqRel);
        self.visible_len.store(len, Ordering::Release);
        self.min_ts.fetch_min(min_ts, Ordering::AcqRel);
        self.max_ts.fetch_max(max_ts, Ordering::AcqRel);
        self.next_offset.store(next_offset, Ordering::Release);
    }

    /// Writer rollback: resets visibility to an earlier state.
    pub fn reset_visibility(&self, len: u64, next_offset: u64) {
        self.next_offset.store(next_offset, Ordering::Release);
        self.written_next.store(next_offset, Ordering::Release);
        self.visible_len.store(len, Ordering::Release);
        self.written_len.store(len, Ordering::Release);
    }

    /// Records bytes handed to the OS (not yet durable). Length first, then the offset.
    pub fn set_written(&self, len: u64, next_offset: u64) {
        self.written_len.store(len, Ordering::Release);
        self.written_next.store(next_offset, Ordering::Release);
    }

    /// Offset after the last written (maybe not durable) entry.
    pub fn written_next(&self) -> u64 {
        self.written_next.load(Ordering::Acquire)
    }

    fn end(&self, unsynced: bool) -> u64 {
        if unsynced {
            self.written_len.load(Ordering::Acquire)
        } else {
            self.visible_len()
        }
    }

    fn next_for(&self, unsynced: bool) -> u64 {
        if unsynced {
            self.written_next()
        } else {
            self.next_offset()
        }
    }

    /// Truncation: overrides the timestamp range (`min_ts == u64::MAX` = empty).
    pub fn reset_ts(&self, min_ts: u64, max_ts: u64) {
        self.min_ts.store(min_ts, Ordering::Release);
        self.max_ts.store(max_ts, Ordering::Release);
    }

    /// The visible entry containing `offset` (index lookup, then a forward scan over entry
    /// headers; no record decode). Returns the scan positioned at that entry and the entry.
    /// `InvalidData` if the offset isn't in this segment's visible range.
    pub fn seek_entry(&self, offset: u64, chunk: usize) -> io::Result<(SegmentScan<'_>, Entry)> {
        self.seek_entry_in(offset, chunk, false)
    }

    fn seek_entry_in(
        &self,
        offset: u64,
        chunk: usize,
        unsynced: bool,
    ) -> io::Result<(SegmentScan<'_>, Entry)> {
        let mut scan = SegmentScan::raw(
            &self.reader,
            self.position_for_offset(offset),
            self.end(unsynced),
        );
        scan.chunk = chunk.max(READ_CHUNK);
        loop {
            match scan.peek_entry(false)? {
                Some(e) if e.meta.contains(offset) => return Ok((scan, e)),
                Some(e) if e.meta.next_offset() <= offset => scan.advance(&e),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("offset {offset} not found in segment {}", self.base_offset),
                    ));
                }
            }
        }
    }

    /// Whole entries starting with the one containing `from` (which must be in this
    /// segment's visible range): about `max_bytes` but at least one entry. Usually one
    /// positional read (the seek's read-ahead covers `max_bytes`); the bytes are a zero-copy
    /// slice of the read buffer when they fit in it. Returns `(bytes, first entry base, next
    /// offset after the last entry, all entries are batches)`.
    pub fn read_entries(&self, from: u64, max_bytes: usize) -> io::Result<RawEntries> {
        self.read_entries_in(from, max_bytes, false)
    }

    /// [`read_entries`](Self::read_entries) bounded by the written (maybe not yet durable)
    /// length instead of the visible one.
    pub fn read_entries_in(
        &self,
        from: u64,
        max_bytes: usize,
        unsynced: bool,
    ) -> io::Result<RawEntries> {
        if from >= self.next_for(unsynced) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("offset {from} not in segment {}", self.base_offset),
            ));
        }
        let (mut scan, first) = self.seek_entry_in(from, max_bytes, unsynced)?;
        let start = first.pos;
        let mut next = first.meta.next_offset();
        let mut batches_only = first.meta.is_batch();
        let mut records = u64::from(first.meta.count);
        let mut len = first.meta.len;
        scan.advance(&first);
        while len < max_bytes {
            match scan.peek_entry(false)? {
                Some(e) if len + e.meta.len <= max_bytes => {
                    len += e.meta.len;
                    next = e.meta.next_offset();
                    records += u64::from(e.meta.count);
                    batches_only &= e.meta.is_batch();
                    scan.advance(&e);
                }
                _ => break,
            }
        }
        let bytes = match scan.slice(start, start + len as u64) {
            Some(b) => b,
            None => Bytes::from(read_exact_at_uninit(&self.reader, len, start)?),
        };
        Ok(RawEntries {
            bytes,
            first_base: first.meta.base_offset,
            next_offset: next,
            records,
            batches_only,
        })
    }

    /// Positional scan starting at byte `pos`, bounded by the visible length.
    pub fn scan_from(&self, pos: u64) -> SegmentScan<'_> {
        SegmentScan::raw(&self.reader, pos, self.visible_len())
    }

    /// Byte position to start scanning from to find `offset`.
    pub fn position_for_offset(&self, offset: u64) -> u64 {
        if offset <= self.base_offset {
            return 0;
        }
        let rel = u32::try_from(offset - self.base_offset).unwrap_or(u32::MAX);
        u64::from(self.index.lookup_offset(rel).1)
    }

    /// Byte position to start scanning from to find the first record with timestamp >= `ts`.
    pub fn position_for_time(&self, ts: u64) -> u64 {
        u64::from(self.index.lookup_time(ts).1)
    }

    /// Approximate (lower bound) offset of the first record with timestamp >= `ts`, using only
    /// the in-memory index.
    pub fn offset_for_time_approx(&self, ts: u64) -> u64 {
        self.base_offset + u64::from(self.index.lookup_time(ts).0)
    }
}

/// Whole entries read by [`Segment::read_entries`].
#[derive(Debug, Clone)]
pub struct RawEntries {
    pub bytes: Bytes,
    pub first_base: u64,
    pub next_offset: u64,
    pub records: u64,
    pub batches_only: bool,
}

/// Sequential reader over a segment's entries using positional reads with a read-ahead
/// buffer. Entries are zero-copy slices of the buffer.
pub struct SegmentScan<'a> {
    file: &'a File,
    pos: u64,
    end: u64,
    buf: Bytes,
    buf_start: u64,
    /// Read-ahead size.
    pub chunk: usize,
}

impl<'a> SegmentScan<'a> {
    /// Scan over an arbitrary file range (recovery, before visibility is known).
    pub fn raw(file: &'a File, pos: u64, end: u64) -> Self {
        SegmentScan {
            file,
            pos,
            end,
            buf: Bytes::new(),
            buf_start: pos,
            chunk: READ_CHUNK,
        }
    }

    /// Ensures `buf` holds at least `need` bytes starting at `self.pos`. Returns false if the
    /// range ends first.
    fn fill(&mut self, need: usize) -> io::Result<bool> {
        if self.pos >= self.buf_start {
            let have_from = (self.pos - self.buf_start) as usize;
            if self.buf.len() >= have_from + need {
                return Ok(true);
            }
        }
        if self.pos + need as u64 > self.end {
            return Ok(false);
        }
        let want = need.max(self.chunk).min((self.end - self.pos) as usize);
        self.buf = Bytes::from(read_exact_at_uninit(self.file, want, self.pos)?);
        self.buf_start = self.pos;
        Ok(true)
    }

    /// Position of the next entry.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// The bytes `[from, to)` as a zero-copy slice if the read buffer holds them.
    pub fn slice(&self, from: u64, to: u64) -> Option<Bytes> {
        (from >= self.buf_start && to <= self.buf_start + self.buf.len() as u64).then(|| {
            self.buf
                .slice((from - self.buf_start) as usize..(to - self.buf_start) as usize)
        })
    }

    /// The next entry without moving past it; `Ok(None)` at the end of the range. With
    /// `verify`, its CRC is checked.
    pub fn peek_entry(&mut self, verify: bool) -> io::Result<Option<Entry>> {
        if !self.fill(4)? {
            if self.pos < self.end {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial entry header at the end of the range",
                ));
            }
            return Ok(None);
        }
        let at = (self.pos - self.buf_start) as usize;
        let total = entry::peek_entry_len(&self.buf[at..]).map_err(to_io)?;
        if !self.fill(total)? {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "entry crosses the end of the range",
            ));
        }
        let at = (self.pos - self.buf_start) as usize;
        let bytes = self.buf.slice(at..at + total);
        let meta = entry::parse_meta(&bytes, verify).map_err(to_io)?;
        Ok(Some(Entry {
            pos: self.pos,
            meta,
            bytes,
        }))
    }

    /// Moves past `e` (returned by the last [`peek_entry`](Self::peek_entry)).
    pub fn advance(&mut self, e: &Entry) {
        self.pos = e.pos + e.meta.len as u64;
    }

    /// Next entry and moves past it.
    pub fn next_entry(&mut self, verify: bool) -> io::Result<Option<Entry>> {
        let e = self.peek_entry(verify)?;
        if let Some(e) = &e {
            self.advance(e);
        }
        Ok(e)
    }
}

pub(crate) fn to_io(e: DecodeError) -> io::Error {
    match e {
        DecodeError::Incomplete { needed } => io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("incomplete record, need {needed}"),
        ),
        DecodeError::Corrupt(m) => io::Error::new(io::ErrorKind::InvalidData, m),
    }
}

/// `len` bytes at `pos`, read straight into a fresh allocation: unlike `vec![0; len]` +
/// `read_exact_at`, the buffer is not zeroed first (catch-up reads are 4 MiB each, and the
/// memset cost as much as the copy from the page cache).
fn read_exact_at_uninit(file: &File, len: usize, pos: u64) -> io::Result<Vec<u8>> {
    use std::os::fd::AsRawFd;
    let mut buf = Vec::<u8>::with_capacity(len);
    let mut done = 0usize;
    while done < len {
        let off = libc::off_t::try_from(pos + done as u64)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset out of range"))?;
        // SAFETY: the destination is the unused capacity of `buf` (`len - done` bytes from
        // `done`); the kernel writes at most that many bytes and nothing reads them before
        // `set_len` covers exactly the bytes it reported.
        let n = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buf.as_mut_ptr().add(done).cast(),
                len - done,
                off,
            )
        };
        match n {
            n if n > 0 => {
                done += n as usize;
                // SAFETY: bytes `..done` were initialized by `pread` (above or earlier rounds).
                unsafe { buf.set_len(done) };
            }
            0 => return Err(io::ErrorKind::UnexpectedEof.into()),
            _ => {
                let e = io::Error::last_os_error();
                if e.kind() != io::ErrorKind::Interrupted {
                    return Err(e);
                }
            }
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names() {
        assert_eq!(log_file_name(42), "00000000000000000042.log");
        assert_eq!(parse_log_file_name("00000000000000000042.log"), Some(42));
        assert_eq!(parse_log_file_name("42.log"), None);
        assert_eq!(parse_log_file_name("00000000000000000042.idx"), None);
    }

    #[test]
    fn uninit_read_returns_exact_bytes_and_fails_past_eof() {
        use std::io::Write;
        let mut f = tempfile::tempfile().unwrap();
        let data: Vec<u8> = (0..300_000u32).map(|i| (i * 7 % 251) as u8).collect();
        f.write_all(&data).unwrap();
        assert_eq!(read_exact_at_uninit(&f, data.len(), 0).unwrap(), data);
        assert_eq!(
            read_exact_at_uninit(&f, 1000, 123_457).unwrap(),
            &data[123_457..124_457]
        );
        assert!(read_exact_at_uninit(&f, 0, 5).unwrap().is_empty());
        let e = read_exact_at_uninit(&f, 10, data.len() as u64 - 5).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
    }
}

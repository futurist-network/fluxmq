//! Backup support: a consistent per-channel **cut** for online backups and an offline
//! **restore writer** that lays a backup down into an empty data directory.
//!
//! ## Cut ([`crate::Broker::backup_cut`])
//!
//! Per partition the cut records the segments holding the committed records `[low, end)`:
//! `end` is the committed end (`high`) in single-node mode and the high watermark in
//! replicated mode, where only partitions this node leads are included. For every segment
//! the cut captures the byte length that holds exactly the records below `end` plus the
//! in-memory index entries covering it, and **pins** the segment so retention skips it until
//! the cut is dropped. Nothing is copied while a lock is held: the segment list is
//! read-locked only to clone and pin the segment handles; the bytes are read afterwards with
//! positional reads on the segment's own read-only handle ([`SegmentCut::read_at`]), bounded
//! by the captured length. Sealed segments are immutable and the active segment is only read
//! up to its captured length, so the writer is never blocked or observed mid-record.
//!
//! Group committed offsets and the DLQ export are taken **after** all partition cuts, so a
//! restore never has an offset pointing past data it lacks for a consumer that processed
//! it… but may redeliver messages consumed between the cut and the offset snapshot (offsets
//! above a partition's restored end are clamped on open) — at-least-once.
//!
//! ## Restore ([`RestoreWriter`])
//!
//! Writes the exact on-disk layout [`crate::Broker::open`] recovers:
//! `channels/<name>/channel.json`, `p<N>/<base:020>.log` + `.idx`, `offsets.json`,
//! `dlq.log`. A partition without segments gets one empty segment at its cut end so offsets
//! continue where the backup ended. Every laid-down segment can be verified record by record
//! ([`verify_segment`]: CRC + contiguous offsets) before the broker ever opens it.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use fluxmq_proto::model::{ChannelConfig, DeadLetter};

use crate::persist::{self, CONFIG_FILE, DLQ_FILE, DlqRecord, OFFSETS_FILE, OffsetsFile};
use crate::storage::segment::{SegmentScan, index_file_name, log_file_name};
use crate::util::{atomic_write, fsync_dir};

pub use crate::storage::log::SegmentCut;

/// Committed offset value meaning "never initialized on this partition" (replicated mode).
pub const UNINIT_OFFSET: u64 = crate::group::UNINIT;

/// Backup cut of one partition.
#[derive(Debug)]
pub struct PartitionCut {
    pub partition: u32,
    /// First offset held by the cut (== `end` when it holds no records).
    pub low: u64,
    /// One past the last record in the cut.
    pub end: u64,
    /// Contiguous segments covering `[low, end)`, oldest first (pinned until dropped).
    pub segments: Vec<SegmentCut>,
}

impl PartitionCut {
    pub fn messages(&self) -> u64 {
        self.end - self.low
    }

    pub fn bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.len).sum()
    }
}

/// Backup cut of one channel (see module docs).
#[derive(Debug)]
pub struct ChannelCut {
    pub config: ChannelConfig,
    pub taken_at_ms: u64,
    /// Included partitions (all in single-node mode; locally led ones in replicated mode).
    pub partitions: Vec<PartitionCut>,
    /// Group → committed offset per partition (index = partition), captured after the
    /// segment cut. Entries of partitions not in [`ChannelCut::partitions`] are
    /// [`UNINIT_OFFSET`].
    pub group_offsets: BTreeMap<String, Vec<u64>>,
    /// Dead letters of the included partitions, captured after the segment cut.
    pub dlq: Vec<DeadLetter>,
}

/// Offline writer of a restored data directory (see module docs). Blocking file I/O.
#[derive(Debug)]
pub struct RestoreWriter {
    data_dir: PathBuf,
    sync: bool,
}

fn channel_dir(data_dir: &Path, channel: &str) -> PathBuf {
    data_dir.join("channels").join(channel)
}

impl RestoreWriter {
    /// Prepares `data_dir`, which must not exist or be an empty directory.
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        if data_dir.exists() {
            if !data_dir.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{} is not a directory", data_dir.display()),
                ));
            }
            if fs::read_dir(data_dir)?.next().is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "data directory {} is not empty; restore only into an empty directory",
                        data_dir.display()
                    ),
                ));
            }
        }
        fs::create_dir_all(data_dir.join("channels"))?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            sync: true,
        })
    }

    /// Skips fsyncs (tests).
    pub fn without_sync(mut self) -> Self {
        self.sync = false;
        self
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Creates the channel directory with its partitions and `channel.json`.
    pub fn create_channel(&self, config: &ChannelConfig) -> io::Result<()> {
        fluxmq_proto::model::validate_channel_name(&config.name)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let dir = channel_dir(&self.data_dir, &config.name);
        if dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("channel '{}' restored twice", config.name),
            ));
        }
        for p in 0..config.partitions {
            fs::create_dir_all(dir.join(format!("p{p}")))?;
        }
        let json = serde_json::to_vec_pretty(config).map_err(io::Error::other)?;
        atomic_write(&dir.join(CONFIG_FILE), &json)
    }

    fn partition_dir(&self, channel: &str, partition: u32) -> io::Result<PathBuf> {
        let dir = channel_dir(&self.data_dir, channel).join(format!("p{partition}"));
        if !dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("partition {partition} of channel '{channel}' was not created"),
            ));
        }
        Ok(dir)
    }

    /// Creates `<base>.log` of a partition for the caller to stream the segment bytes into.
    /// Call [`RestoreWriter::finish_segment`] afterwards.
    pub fn create_segment(&self, channel: &str, partition: u32, base: u64) -> io::Result<File> {
        let path = self
            .partition_dir(channel, partition)?
            .join(log_file_name(base));
        OpenOptions::new().write(true).create_new(true).open(path)
    }

    /// Verifies the log (see [`verify_segment`]: records `[base, end)` exactly), writes the
    /// segment's index (`None`: no index file — recovery rebuilds it from the log) and fsyncs.
    pub fn finish_segment(
        &self,
        channel: &str,
        partition: u32,
        base: u64,
        end: u64,
        index: Option<&[u8]>,
    ) -> io::Result<()> {
        let dir = self.partition_dir(channel, partition)?;
        let log = dir.join(log_file_name(base));
        let n = verify_segment(&log, base)?;
        if base + n != end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{}: holds offsets {base}..{} but the backup recorded {base}..{end}",
                    log.display(),
                    base + n
                ),
            ));
        }
        if let Some(index) = index {
            let mut f = File::create(dir.join(index_file_name(base)))?;
            f.write_all(index)?;
            if self.sync {
                f.sync_data()?;
            }
        }
        if self.sync {
            File::open(&log)?.sync_data()?;
        }
        Ok(())
    }

    /// A partition with no segment in the backup: one empty segment starting at `at` (the
    /// cut end), so offsets continue there.
    pub fn empty_partition(&self, channel: &str, partition: u32, at: u64) -> io::Result<()> {
        let dir = self.partition_dir(channel, partition)?;
        File::create(dir.join(log_file_name(at)))?;
        File::create(dir.join(index_file_name(at)))?;
        Ok(())
    }

    /// Writes the consumer groups' committed offsets (`group → offset per partition`).
    pub fn write_offsets(
        &self,
        channel: &str,
        groups: &BTreeMap<String, Vec<u64>>,
    ) -> io::Result<()> {
        let file = OffsetsFile {
            groups: groups.clone(),
            acked: BTreeMap::new(),
        };
        let json = serde_json::to_vec(&file).map_err(io::Error::other)?;
        atomic_write(
            &channel_dir(&self.data_dir, channel).join(OFFSETS_FILE),
            &json,
        )
    }

    /// Writes the channel's DLQ.
    pub fn write_dlq(&self, channel: &str, entries: &[DeadLetter]) -> io::Result<()> {
        let mut out = Vec::new();
        for dl in entries {
            out.extend_from_slice(&persist::encode_dlq_record(&DlqRecord::Add(Box::new(
                dl.clone(),
            ))));
        }
        atomic_write(&channel_dir(&self.data_dir, channel).join(DLQ_FILE), &out)
    }

    /// Fsyncs the directory tree (call once everything is written).
    pub fn finish(self) -> io::Result<()> {
        if !self.sync {
            return Ok(());
        }
        let channels = self.data_dir.join("channels");
        for entry in fs::read_dir(&channels)? {
            let dir = entry?.path();
            for p in fs::read_dir(&dir)? {
                let p = p?.path();
                if p.is_dir() {
                    fsync_dir(&p)?;
                }
            }
            fsync_dir(&dir)?;
        }
        fsync_dir(&channels)?;
        fsync_dir(&self.data_dir)
    }
}

/// Scans a segment file entry by entry (length, CRC — CRC-32C for batch envelopes, CRC-32
/// for legacy records — and contiguous offsets from `base`) and returns the record count. Any torn or corrupt byte fails the check.
pub fn verify_segment(path: &Path, base: u64) -> io::Result<u64> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut scan = SegmentScan::raw(&file, 0, len);
    let mut expected = base;
    loop {
        match scan.next_entry(true)?.map(|e| e.meta) {
            Some(m) if m.base_offset == expected => expected = m.next_offset(),
            Some(m) => {
                let off = m.base_offset;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{}: offset {off} where {expected} was expected",
                        path.display()
                    ),
                ));
            }
            None => break,
        }
    }
    if scan.position() != len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: trailing bytes after the last record", path.display()),
        ));
    }
    Ok(expected - base)
}

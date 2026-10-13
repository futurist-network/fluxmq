//! Metadata persistence off the async executor: one broker-wide "meta writer" thread appends
//! DLQ records and writes offset checkpoints, batching fsyncs.
//!
//! Files per channel (in `data_dir/channels/<name>/`):
//! * `dlq.log` — append-only `u32 len | u32 crc32 | postcard(DlqRecord)` records. Removals are
//!   tombstones; the file is compacted on open (and online once tombstones dominate).
//! * `offsets.json` — `{ "groups": { "<group>": [committed offset per partition] },
//!   "acked": { "<group>": [[[start, end], ...] per partition] } }` (`acked`: v0.7, finished
//!   ranges above the committed offset, optional), written atomically (tmp + fsync + rename +
//!   fsync dir).
//!
//! Ordering: every op for a channel goes through the single meta channel, so the file sees ops
//! in the order they were sent. Channel deletion takes [`ChannelFiles`]' lock to mark the
//! channel deleted before moving its directory; the writer checks the flag under the same lock,
//! so a late write can never land in a re-created channel of the same name.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

use fluxmq_proto::model::{AttemptOutcome, DeadLetter, DeliveryAttempt, Message};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::util::atomic_write;

pub const DLQ_FILE: &str = "dlq.log";
pub const OFFSETS_FILE: &str = "offsets.json";
pub const CONFIG_FILE: &str = "channel.json";

/// Per-channel file handles shared with the meta writer.
pub struct ChannelFiles {
    pub dir: PathBuf,
    state: Mutex<FilesState>,
}

#[derive(Default)]
struct FilesState {
    deleted: bool,
    /// Replicated mode: the channel holds no partition data on this node and its directory
    /// was removed (see `Channel` storage release). Writes are skipped until the channel
    /// writer re-creates the directory ([`ChannelFiles::materialize`]).
    released: bool,
    dlq: Option<File>,
}

impl ChannelFiles {
    pub fn new(dir: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            dir,
            state: Mutex::new(FilesState::default()),
        })
    }

    /// Runs `f` (the directory move) while holding the lock and, if it succeeds, marks the
    /// channel deleted so no further writes happen.
    /// Returns `false` (without running `f`) when the directory does not exist because the
    /// channel's storage was released.
    pub fn delete_with(&self, f: impl FnOnce(&Path) -> io::Result<()>) -> io::Result<bool> {
        let mut st = self.state.lock();
        let moved = if st.released {
            false
        } else {
            f(&self.dir)?;
            true
        };
        st.deleted = true;
        st.dlq = None;
        Ok(moved)
    }

    /// Replicated mode: runs `f` (removal of the channel directory) under the lock and marks
    /// the storage released, so no metadata write recreates the directory. `Ok(false)` when
    /// the channel was deleted meanwhile (nothing done). On an error from `f` the storage
    /// still counts as released.
    pub fn release_with(&self, f: impl FnOnce(&Path) -> io::Result<()>) -> io::Result<bool> {
        let mut st = self.state.lock();
        if st.deleted {
            return Ok(false);
        }
        if !st.released {
            st.dlq = None;
            // Marked released even if `f` fails part-way: whatever is left is re-created
            // (config first) before the next write, never written into blindly.
            st.released = true;
            f(&self.dir)?;
        }
        Ok(true)
    }

    /// Re-creates a released channel directory with its `channel.json` (before any partition
    /// data is written, so a crash never leaves partition data without a config). No-op when
    /// not released. `Ok(false)` when the channel was deleted.
    pub fn materialize(&self, config_json: &[u8]) -> io::Result<bool> {
        let mut st = self.state.lock();
        if st.deleted {
            return Ok(false);
        }
        if st.released {
            fs::create_dir_all(&self.dir)?;
            atomic_write(&self.dir.join(CONFIG_FILE), config_json)?;
            if let Some(parent) = self.dir.parent() {
                crate::util::fsync_dir(parent)?;
            }
            st.released = false;
        }
        Ok(true)
    }

    pub fn is_released(&self) -> bool {
        self.state.lock().released
    }

    /// Writes `channel.json` (skipped while released or deleted: the writer writes the current
    /// config when it re-creates the directory).
    pub fn write_config(&self, bytes: &[u8]) -> io::Result<()> {
        self.write_file(CONFIG_FILE, bytes)
    }

    fn append_dlq(&self, bytes: &[u8]) -> io::Result<bool> {
        let mut st = self.state.lock();
        if st.deleted || st.released {
            return Ok(false);
        }
        if st.dlq.is_none() {
            st.dlq = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.dir.join(DLQ_FILE))?,
            );
        }
        let f = st.dlq.as_mut().expect("opened above");
        f.write_all(bytes)?;
        Ok(true)
    }

    fn sync_dlq(&self) -> io::Result<()> {
        let st = self.state.lock();
        if let Some(f) = &st.dlq {
            f.sync_data()?;
        }
        Ok(())
    }

    fn rewrite_dlq(&self, bytes: &[u8]) -> io::Result<()> {
        let mut st = self.state.lock();
        if st.deleted || st.released {
            return Ok(());
        }
        st.dlq = None;
        atomic_write(&self.dir.join(DLQ_FILE), bytes)
    }

    fn write_file(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        let st = self.state.lock();
        if st.deleted || st.released {
            return Ok(());
        }
        atomic_write(&self.dir.join(name), bytes)
    }
}

pub enum MetaOp {
    DlqAppend {
        files: Arc<ChannelFiles>,
        bytes: Vec<u8>,
    },
    /// Replaces the whole DLQ file (compaction).
    DlqRewrite {
        files: Arc<ChannelFiles>,
        bytes: Vec<u8>,
    },
    Offsets {
        files: Arc<ChannelFiles>,
        bytes: Vec<u8>,
    },
    /// Atomically replaces `dir/<name>` (`name` may contain a partition subdirectory).
    /// Within a batch only the last write per (channel, name) is performed.
    File {
        files: Arc<ChannelFiles>,
        name: String,
        bytes: Vec<u8>,
    },
    /// Acknowledged once every op sent before it is durable.
    Flush(mpsc::SyncSender<()>),
}

pub type MetaSender = mpsc::Sender<MetaOp>;

/// Starts the meta writer thread; it exits when all senders are dropped.
pub fn spawn_meta_writer() -> io::Result<(MetaSender, std::thread::JoinHandle<()>)> {
    let (tx, rx) = mpsc::channel::<MetaOp>();
    let handle = std::thread::Builder::new()
        .name("fluxmq-meta".into())
        .spawn(move || meta_loop(rx))?;
    Ok((tx, handle))
}

fn meta_loop(rx: mpsc::Receiver<MetaOp>) {
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        while batch.len() < 4096 {
            match rx.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }
        let mut dirty: Vec<Arc<ChannelFiles>> = Vec::new();
        let mut flushes = Vec::new();
        // Offsets: only the last checkpoint per channel in a batch matters.
        let mut offsets: Vec<(Arc<ChannelFiles>, Vec<u8>)> = Vec::new();
        let mut other: Vec<(Arc<ChannelFiles>, String, Vec<u8>)> = Vec::new();
        for op in batch {
            match op {
                MetaOp::DlqAppend { files, bytes } => match files.append_dlq(&bytes) {
                    Ok(true) => {
                        if !dirty.iter().any(|d| Arc::ptr_eq(d, &files)) {
                            dirty.push(files);
                        }
                    }
                    Ok(false) => {}
                    Err(e) => error!(dir = ?files.dir, error = %e, "DLQ append failed"),
                },
                MetaOp::DlqRewrite { files, bytes } => {
                    // Earlier appends in this batch are already in the file being replaced.
                    if let Err(e) = files.rewrite_dlq(&bytes) {
                        error!(dir = ?files.dir, error = %e, "DLQ compaction failed");
                    }
                    dirty.retain(|d| !Arc::ptr_eq(d, &files));
                }
                MetaOp::Offsets { files, bytes } => {
                    if let Some(slot) = offsets.iter_mut().find(|(f, _)| Arc::ptr_eq(f, &files)) {
                        slot.1 = bytes;
                    } else {
                        offsets.push((files, bytes));
                    }
                }
                MetaOp::File { files, name, bytes } => {
                    if let Some(slot) = other
                        .iter_mut()
                        .find(|(f, n, _)| Arc::ptr_eq(f, &files) && *n == name)
                    {
                        slot.2 = bytes;
                    } else {
                        other.push((files, name, bytes));
                    }
                }
                MetaOp::Flush(ack) => flushes.push(ack),
            }
        }
        for (f, name, bytes) in other {
            if let Err(e) = f.write_file(&name, &bytes) {
                error!(dir = ?f.dir, name, error = %e, "metadata file write failed");
            }
        }
        for f in &dirty {
            if let Err(e) = f.sync_dlq() {
                error!(dir = ?f.dir, error = %e, "DLQ fsync failed");
            }
        }
        for (f, bytes) in offsets {
            if let Err(e) = f.write_file(OFFSETS_FILE, &bytes) {
                warn!(dir = ?f.dir, error = %e, "offset checkpoint failed");
            }
        }
        for ack in flushes {
            let _ = ack.send(());
        }
    }
}

/// Sends a flush marker and waits until the meta writer processed everything before it.
/// Blocking: call from a blocking context.
pub fn flush_blocking(tx: &MetaSender) {
    let (ack_tx, ack_rx) = mpsc::sync_channel(1);
    if tx.send(MetaOp::Flush(ack_tx)).is_ok() {
        let _ = ack_rx.recv();
    }
}

// ---------------------------------------------------------------------------------------
// DLQ file format
// ---------------------------------------------------------------------------------------

/// Logical DLQ file operation.
#[derive(Debug, Clone)]
pub enum DlqRecord {
    Add(Box<DeadLetter>),
    Remove(u64),
}

// The proto `AttemptOutcome` is an internally tagged serde enum, which postcard (not
// self-describing) cannot deserialize. The file therefore uses these mirror types.

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StoredOutcome {
    InFlight,
    Acked,
    Nacked(String),
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAttempt {
    attempt: u32,
    consumer: String,
    delivered_at_ms: u64,
    finished_at_ms: Option<u64>,
    outcome: StoredOutcome,
}

/// Postcard-friendly mirror of [`DeadLetter`] (also used by the replication state stream).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredDeadLetter {
    id: u64,
    channel: String,
    group: String,
    message: Message,
    attempts: Vec<StoredAttempt>,
    reason: String,
    dead_at_ms: u64,
}

#[derive(Serialize, Deserialize)]
enum StoredRecord {
    Add(Box<StoredDeadLetter>),
    Remove(u64),
}

impl From<&DeadLetter> for StoredDeadLetter {
    fn from(d: &DeadLetter) -> Self {
        StoredDeadLetter {
            id: d.id,
            channel: d.channel.clone(),
            group: d.group.clone(),
            message: d.message.clone(),
            attempts: d
                .attempts
                .iter()
                .map(|a| StoredAttempt {
                    attempt: a.attempt,
                    consumer: a.consumer.clone(),
                    delivered_at_ms: a.delivered_at_ms,
                    finished_at_ms: a.finished_at_ms,
                    outcome: match &a.outcome {
                        AttemptOutcome::InFlight => StoredOutcome::InFlight,
                        AttemptOutcome::Acked => StoredOutcome::Acked,
                        AttemptOutcome::Nacked { reason } => StoredOutcome::Nacked(reason.clone()),
                        AttemptOutcome::TimedOut => StoredOutcome::TimedOut,
                    },
                })
                .collect(),
            reason: d.reason.clone(),
            dead_at_ms: d.dead_at_ms,
        }
    }
}

impl From<StoredDeadLetter> for DeadLetter {
    fn from(d: StoredDeadLetter) -> Self {
        DeadLetter {
            id: d.id,
            channel: d.channel,
            group: d.group,
            message: d.message,
            attempts: d
                .attempts
                .into_iter()
                .map(|a| DeliveryAttempt {
                    attempt: a.attempt,
                    consumer: a.consumer,
                    delivered_at_ms: a.delivered_at_ms,
                    finished_at_ms: a.finished_at_ms,
                    outcome: match a.outcome {
                        StoredOutcome::InFlight => AttemptOutcome::InFlight,
                        StoredOutcome::Acked => AttemptOutcome::Acked,
                        StoredOutcome::Nacked(reason) => AttemptOutcome::Nacked { reason },
                        StoredOutcome::TimedOut => AttemptOutcome::TimedOut,
                    },
                })
                .collect(),
            reason: d.reason,
            dead_at_ms: d.dead_at_ms,
        }
    }
}

pub fn encode_dlq_record(rec: &DlqRecord) -> Vec<u8> {
    let stored = match rec {
        DlqRecord::Add(dl) => StoredRecord::Add(Box::new(StoredDeadLetter::from(dl.as_ref()))),
        DlqRecord::Remove(id) => StoredRecord::Remove(*id),
    };
    let body = postcard::to_stdvec(&stored).expect("postcard encoding into Vec is infallible");
    let mut out = Vec::with_capacity(body.len() + 8);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Loads the DLQ of a channel, compacting the file if it holds tombstones or a torn tail.
pub fn load_dlq(dir: &Path) -> io::Result<BTreeMap<u64, DeadLetter>> {
    let path = dir.join(DLQ_FILE);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(e),
    };
    let mut entries = BTreeMap::new();
    let mut pos = 0usize;
    let mut needs_compaction = false;
    while pos < bytes.len() {
        if bytes.len() - pos < 8 {
            needs_compaction = true;
            break;
        }
        let len =
            u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4-byte slice")) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().expect("4-byte slice"));
        let body_end = pos + 8 + len;
        if body_end > bytes.len() || crc32fast::hash(&bytes[pos + 8..body_end]) != crc {
            warn!(
                ?path,
                at = pos,
                "DLQ file has a torn/corrupt tail; truncating"
            );
            needs_compaction = true;
            break;
        }
        // CRC-valid but undecodable means a format bug, not a torn write: refuse to guess
        // (truncating would silently destroy dead letters).
        match postcard::from_bytes::<StoredRecord>(&bytes[pos + 8..body_end]) {
            Ok(StoredRecord::Add(dl)) => {
                entries.insert(dl.id, DeadLetter::from(*dl));
            }
            Ok(StoredRecord::Remove(id)) => {
                entries.remove(&id);
                needs_compaction = true;
            }
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{}: undecodable DLQ record at byte {pos}: {e}",
                        path.display()
                    ),
                ));
            }
        }
        pos = body_end;
    }
    if needs_compaction {
        let mut out = Vec::new();
        for dl in entries.values() {
            out.extend_from_slice(&encode_dlq_record(&DlqRecord::Add(Box::new(dl.clone()))));
        }
        atomic_write(&path, &out)?;
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------------------
// Offsets
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OffsetsFile {
    /// Group → committed offset (lowest unacked) per partition.
    pub groups: BTreeMap<String, Vec<u64>>,
    /// v0.7: group → per partition, offsets finished (acked or dead-lettered) above the
    /// committed offset, as half-open `[start, end)` ranges (bounded per partition). Lets a
    /// restart redeliver exactly the unfinished messages. Absent in older files (everything
    /// from the committed offset is redelivered, as before); older brokers ignore it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub acked: BTreeMap<String, Vec<Vec<[u64; 2]>>>,
}

pub fn load_offsets(dir: &Path) -> io::Result<OffsetsFile> {
    match fs::read(dir.join(OFFSETS_FILE)) {
        Ok(b) => {
            serde_json::from_slice(&b).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(OffsetsFile::default()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn dl(id: u64) -> DeadLetter {
        DeadLetter {
            id,
            channel: "c".into(),
            group: "g".into(),
            message: Message {
                partition: 0,
                offset: id,
                key: Some("k".into()),
                headers: Default::default(),
                timestamp_ms: 1,
                payload: Bytes::from_static(b"x"),
            },
            attempts: vec![
                DeliveryAttempt {
                    attempt: 1,
                    consumer: "c#1".into(),
                    delivered_at_ms: 1,
                    finished_at_ms: Some(2),
                    outcome: AttemptOutcome::TimedOut,
                },
                DeliveryAttempt {
                    attempt: 2,
                    consumer: "c#1".into(),
                    delivered_at_ms: 3,
                    finished_at_ms: Some(4),
                    outcome: AttemptOutcome::Nacked { reason: "x".into() },
                },
            ],
            reason: "boom".into(),
            dead_at_ms: 2,
        }
    }

    #[test]
    fn dlq_file_roundtrip_with_tombstones_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let files = ChannelFiles::new(dir.path().to_path_buf());
        let (tx, handle) = spawn_meta_writer().unwrap();
        for id in 1..=5 {
            tx.send(MetaOp::DlqAppend {
                files: files.clone(),
                bytes: encode_dlq_record(&DlqRecord::Add(Box::new(dl(id)))),
            })
            .unwrap();
        }
        tx.send(MetaOp::DlqAppend {
            files: files.clone(),
            bytes: encode_dlq_record(&DlqRecord::Remove(2)),
        })
        .unwrap();
        flush_blocking(&tx);
        drop(tx);
        handle.join().unwrap();
        // Torn tail.
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.path().join(DLQ_FILE))
            .unwrap();
        f.write_all(&encode_dlq_record(&DlqRecord::Add(Box::new(dl(9))))[..10])
            .unwrap();
        let loaded = load_dlq(dir.path()).unwrap();
        assert_eq!(loaded.keys().copied().collect::<Vec<_>>(), vec![1, 3, 4, 5]);
        // Compacted: reload yields the same and the file has no tombstones.
        let again = load_dlq(dir.path()).unwrap();
        assert_eq!(again, loaded);
    }

    #[test]
    fn deleted_channel_files_are_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let files = ChannelFiles::new(dir.path().join("ch"));
        fs::create_dir_all(&files.dir).unwrap();
        files.delete_with(|d| fs::remove_dir_all(d)).unwrap();
        assert!(!files.append_dlq(b"x").unwrap());
        files.write_file(OFFSETS_FILE, b"{}").unwrap();
        assert!(!dir.path().join("ch").exists());
    }
}

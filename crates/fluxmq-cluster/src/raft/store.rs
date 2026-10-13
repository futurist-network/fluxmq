//! Persistent Raft storage under `data_dir/raft/`:
//!
//! * `log.bin` — append-only records `u32 len LE | u32 crc32 LE | JSON(Entry)`; a torn tail is
//!   truncated on open. Truncation (conflicting suffix) and purge (after a snapshot) rewrite
//!   the file atomically — both are rare and the metadata log is small.
//! * `vote.json` — the vote, atomically replaced and fsync'd before `save_vote` returns.
//! * `purged.json` — last purged log id (fsync'd before the log is rewritten).
//! * `committed.json` — last known committed log id, best effort (no fsync): lets a restarted
//!   node re-apply committed entries immediately instead of waiting for a leader.
//! * `snapshot.json` — the latest state-machine snapshot (atomic + fsync). The state machine
//!   itself lives in memory and is rebuilt from the snapshot plus the log on start.
//!
//! All file I/O runs on the blocking pool; openraft serializes calls into the log store, so
//! writes hit the disk in order.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fluxmq_meta::{MetaEvent, Versioned};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    AnyError, EntryPayload, ErrorSubject, ErrorVerb, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, StorageIOError,
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use super::{
    Entry, LogId, MetaCommand, MetaReply, NodeId, SnapshotMeta, StorageError, StoredMembership,
    TypeConfig, Vote,
};

const LOG_FILE: &str = "log.bin";
const VOTE_FILE: &str = "vote.json";
const PURGED_FILE: &str = "purged.json";
const COMMITTED_FILE: &str = "committed.json";
const SNAPSHOT_FILE: &str = "snapshot.json";
const RECORD_HEADER: usize = 8;

fn io_err(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    e: impl std::fmt::Display,
) -> StorageError {
    StorageIOError::new(subject, verb, AnyError::error(e.to_string())).into()
}

fn join_err(e: tokio::task::JoinError) -> StorageError {
    io_err(ErrorSubject::Store, ErrorVerb::Write, e)
}

/// tmp + fsync + rename + dir fsync.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8], durable: bool) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        if durable {
            f.sync_all()?;
        }
    }
    std::fs::rename(&tmp, path)?;
    if durable && let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> std::io::Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(std::io::Error::other),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn encode_record(out: &mut Vec<u8>, entry: &Entry) -> std::io::Result<()> {
    let body = serde_json::to_vec(entry).map_err(std::io::Error::other)?;
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(())
}

/// Parses records; returns the entries and the length of the valid prefix.
fn decode_records(bytes: &[u8]) -> (Vec<Entry>, usize) {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + RECORD_HEADER <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().expect("4 bytes"));
        let end = pos + RECORD_HEADER + len;
        if end > bytes.len() || crc32fast::hash(&bytes[pos + RECORD_HEADER..end]) != crc {
            break;
        }
        match serde_json::from_slice::<Entry>(&bytes[pos + RECORD_HEADER..end]) {
            Ok(e) => out.push(e),
            Err(_) => break,
        }
        pos = end;
    }
    (out, pos)
}

// ---------------------------------------------------------------------------------------------
// Log store
// ---------------------------------------------------------------------------------------------

struct LogInner {
    dir: PathBuf,
    entries: BTreeMap<u64, Entry>,
    last_purged: Option<LogId>,
    vote: Option<Vote>,
    committed: Option<LogId>,
    file: std::fs::File,
}

impl LogInner {
    fn open_append(dir: &Path) -> std::io::Result<std::fs::File> {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))
    }

    /// Rewrites `log.bin` from memory (atomic) and reopens the append handle.
    fn rewrite(&mut self) -> std::io::Result<()> {
        let mut buf = Vec::new();
        for e in self.entries.values() {
            encode_record(&mut buf, e)?;
        }
        atomic_write(&self.dir.join(LOG_FILE), &buf, true)?;
        self.file = Self::open_append(&self.dir)?;
        Ok(())
    }
}

/// File-backed Raft log + vote. Cheap to clone (the clone is also the log reader).
#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<LogInner>>,
}

impl LogStore {
    /// Blocking: call from `spawn_blocking` or at startup.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let vote: Option<Vote> = read_json(&dir.join(VOTE_FILE))?;
        let last_purged: Option<LogId> = read_json(&dir.join(PURGED_FILE))?;
        // Best effort file: a torn write just means "unknown".
        let committed: Option<LogId> = read_json(&dir.join(COMMITTED_FILE)).unwrap_or(None);

        let log_path = dir.join(LOG_FILE);
        let bytes = match std::fs::read(&log_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        let (decoded, valid) = decode_records(&bytes);
        if valid < bytes.len() {
            tracing::warn!(
                path = %log_path.display(),
                dropped = bytes.len() - valid,
                "raft log has a torn tail; truncating"
            );
            let f = std::fs::OpenOptions::new().write(true).open(&log_path)?;
            f.set_len(valid as u64)?;
            f.sync_all()?;
        }
        let purged_idx = last_purged.map(|l| l.index);
        let entries = decoded
            .into_iter()
            .filter(|e| purged_idx.is_none_or(|p| e.log_id.index > p))
            .map(|e| (e.log_id.index, e))
            .collect();
        let file = LogInner::open_append(dir)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(LogInner {
                dir: dir.to_path_buf(),
                entries,
                last_purged,
                vote,
                committed,
                file,
            })),
        })
    }

    /// Runs `f` on the blocking pool with the locked log.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut LogInner) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&mut inner.lock()))
            .await
            .map_err(join_err)?
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let inner = self.inner.lock();
        Ok(inner.entries.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let inner = self.inner.lock();
        let last = inner
            .entries
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        let vote = *vote;
        self.blocking(move |inner| {
            let bytes = serde_json::to_vec(&vote)
                .map_err(|e| io_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
            atomic_write(&inner.dir.join(VOTE_FILE), &bytes, true)
                .map_err(|e| io_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
            inner.vote = Some(vote);
            Ok(())
        })
        .await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        Ok(self.inner.lock().vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        self.blocking(move |inner| {
            if let Ok(bytes) = serde_json::to_vec(&committed) {
                // Best effort, no fsync: losing it only delays re-applying after a restart.
                let _ = atomic_write(&inner.dir.join(COMMITTED_FILE), &bytes, false);
            }
            inner.committed = committed;
            Ok(())
        })
        .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(self.inner.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let res = self
            .blocking(move |inner| {
                let mut buf = Vec::new();
                for e in &entries {
                    encode_record(&mut buf, e)
                        .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
                }
                inner
                    .file
                    .write_all(&buf)
                    .and_then(|()| inner.file.sync_data())
                    .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
                for e in entries {
                    inner.entries.insert(e.log_id.index, e);
                }
                Ok(())
            })
            .await;
        match &res {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => callback.log_io_completed(Err(std::io::Error::other(e.to_string()))),
        }
        res
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        self.blocking(move |inner| {
            inner.entries.split_off(&log_id.index);
            inner
                .rewrite()
                .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Delete, e))
        })
        .await
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        self.blocking(move |inner| {
            let bytes = serde_json::to_vec(&log_id)
                .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
            atomic_write(&inner.dir.join(PURGED_FILE), &bytes, true)
                .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
            inner.last_purged = Some(log_id);
            let keep = inner.entries.split_off(&(log_id.index + 1));
            inner.entries = keep;
            inner
                .rewrite()
                .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Delete, e))
        })
        .await
    }
}

// ---------------------------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------------------------

/// The replicated KV map.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SmData {
    /// Store-wide revision: +1 per applied data write (not per log entry).
    pub revision: u64,
    /// Revision of the last write under `cluster/` — the cluster metadata epoch.
    pub cluster_epoch: u64,
    pub entries: BTreeMap<String, Versioned>,
}

impl SmData {
    /// Applies one command; returns the reply and the event (if anything changed).
    pub fn apply(&mut self, cmd: MetaCommand) -> (MetaReply, Option<MetaEvent>) {
        let (key, value, pre) = match cmd {
            MetaCommand::Put {
                key,
                value,
                precondition,
            } => (key, Some(value), precondition),
            MetaCommand::Delete { key, precondition } => (key, None, precondition),
        };
        if let Some(pre) = pre {
            let current = self.entries.get(&pre.key).map_or(0, |v| v.mod_revision);
            if current != pre.expected_mod_revision {
                return (
                    MetaReply::Conflict {
                        expected: pre.expected_mod_revision,
                        current,
                    },
                    None,
                );
            }
        }
        self.revision += 1;
        let revision = self.revision;
        match &value {
            Some(v) => {
                self.entries.insert(
                    key.clone(),
                    Versioned {
                        value: v.clone(),
                        mod_revision: revision,
                    },
                );
            }
            None => {
                self.entries.remove(&key);
            }
        }
        if key.starts_with("cluster/") {
            self.cluster_epoch = revision;
        }
        (
            MetaReply::Applied { revision },
            Some(MetaEvent {
                revision,
                key,
                value,
            }),
        )
    }

    /// Events turning `self` into `next` (used when a snapshot replaces the state).
    fn diff(&self, next: &SmData) -> Vec<MetaEvent> {
        let mut out = Vec::new();
        for (k, v) in &next.entries {
            if self.entries.get(k).map(|o| o.mod_revision) != Some(v.mod_revision) {
                out.push(MetaEvent {
                    revision: next.revision,
                    key: k.clone(),
                    value: Some(v.value.clone()),
                });
            }
        }
        for k in self.entries.keys() {
            if !next.entries.contains_key(k) {
                out.push(MetaEvent {
                    revision: next.revision,
                    key: k.clone(),
                    value: None,
                });
            }
        }
        out
    }
}

pub(crate) struct SmState {
    pub last_applied: Option<LogId>,
    pub last_membership: StoredMembership,
    pub data: SmData,
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta,
    /// JSON of [`SmData`].
    data: String,
}

/// State shared between the openraft-owned state machine and the [`crate::meta::RaftMetaStore`]
/// read side.
pub struct SmShared {
    pub(crate) state: RwLock<SmState>,
    pub(crate) events: broadcast::Sender<MetaEvent>,
    /// Revision of the last applied write.
    pub(crate) applied: watch::Sender<u64>,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
    snapshot_seq: AtomicU64,
    dir: PathBuf,
}

impl SmShared {
    /// Loads the last snapshot (blocking).
    pub fn open(dir: &Path) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;
        let snap: Option<StoredSnapshot> = read_json(&dir.join(SNAPSHOT_FILE))?;
        let (state, current) = match snap {
            Some(s) => {
                let data: SmData = serde_json::from_str(&s.data).map_err(std::io::Error::other)?;
                (
                    SmState {
                        last_applied: s.meta.last_log_id,
                        last_membership: s.meta.last_membership.clone(),
                        data,
                    },
                    Some(s),
                )
            }
            None => (
                SmState {
                    last_applied: None,
                    last_membership: StoredMembership::default(),
                    data: SmData::default(),
                },
                None,
            ),
        };
        let (events, _) = broadcast::channel(4096);
        let (applied, _) = watch::channel(state.data.revision);
        Ok(Arc::new(Self {
            state: RwLock::new(state),
            events,
            applied,
            current_snapshot: Mutex::new(current),
            snapshot_seq: AtomicU64::new(0),
            dir: dir.to_path_buf(),
        }))
    }

    pub fn revision(&self) -> u64 {
        self.state.read().data.revision
    }

    pub fn cluster_epoch(&self) -> u64 {
        self.state.read().data.cluster_epoch
    }
}

/// The openraft state machine handle.
#[derive(Clone)]
pub struct StateMachine {
    pub shared: Arc<SmShared>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let (meta_base, data) = {
            let st = self.shared.state.read();
            (
                (st.last_applied, st.last_membership.clone()),
                serde_json::to_string(&st.data)
                    .map_err(|e| io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?,
            )
        };
        let seq = self.shared.snapshot_seq.fetch_add(1, Ordering::Relaxed);
        let snapshot_id = format!(
            "{}-{}-{seq}",
            meta_base.0.map_or(0, |l| l.leader_id.term),
            meta_base.0.map_or(0, |l| l.index)
        );
        let meta = SnapshotMeta {
            last_log_id: meta_base.0,
            last_membership: meta_base.1,
            snapshot_id,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        let shared = self.shared.clone();
        let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, StorageError> {
            let file = serde_json::to_vec(&stored)
                .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
            atomic_write(&shared.dir.join(SNAPSHOT_FILE), &file, true)
                .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
            let bytes = stored.data.clone().into_bytes();
            *shared.current_snapshot.lock() = Some(stored);
            Ok(bytes)
        })
        .await
        .map_err(join_err)??;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = StateMachine;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let st = self.shared.state.read();
        Ok((st.last_applied, st.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetaReply>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut replies = Vec::new();
        let mut events = Vec::new();
        let revision = {
            let mut st = self.shared.state.write();
            for entry in entries {
                st.last_applied = Some(entry.log_id);
                match entry.payload {
                    EntryPayload::Blank => replies.push(MetaReply::Applied {
                        revision: st.data.revision,
                    }),
                    EntryPayload::Normal(cmd) => {
                        let (reply, event) = st.data.apply(cmd);
                        events.extend(event);
                        replies.push(reply);
                    }
                    EntryPayload::Membership(m) => {
                        st.last_membership = StoredMembership::new(Some(entry.log_id), m);
                        replies.push(MetaReply::Applied {
                            revision: st.data.revision,
                        });
                    }
                }
            }
            st.data.revision
        };
        // Published in log order: apply() calls are sequential.
        for e in events {
            let _ = self.shared.events.send(e);
        }
        self.shared.applied.send_replace(revision);
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        mut snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let mut raw = String::new();
        snapshot
            .read_to_string(&mut raw)
            .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let data: SmData = serde_json::from_str(&raw)
            .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: raw,
        };
        let shared = self.shared.clone();
        let stored =
            tokio::task::spawn_blocking(move || -> Result<StoredSnapshot, StorageError> {
                let file = serde_json::to_vec(&stored)
                    .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
                atomic_write(&shared.dir.join(SNAPSHOT_FILE), &file, true)
                    .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
                Ok(stored)
            })
            .await
            .map_err(join_err)??;
        let (events, revision) = {
            let mut st = self.shared.state.write();
            let events = st.data.diff(&data);
            st.last_applied = meta.last_log_id;
            st.last_membership = meta.last_membership.clone();
            st.data = data;
            (events, st.data.revision)
        };
        *self.shared.current_snapshot.lock() = Some(stored);
        tracing::info!(
            revision,
            changed = events.len(),
            "installed metadata snapshot"
        );
        for e in events {
            let _ = self.shared.events.send(e);
        }
        self.shared.applied.send_replace(revision);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        let cur = self.shared.current_snapshot.lock();
        Ok(cur.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone().into_bytes())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxmq_meta::Precondition;
    use openraft::{CommittedLeaderId, EntryPayload};

    fn entry(term: u64, index: u64, cmd: Option<MetaCommand>) -> Entry {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
            payload: cmd.map_or(EntryPayload::Blank, EntryPayload::Normal),
        }
    }

    fn put(key: &str, v: i64) -> MetaCommand {
        MetaCommand::Put {
            key: key.into(),
            value: serde_json::json!(v),
            precondition: None,
        }
    }

    #[test]
    fn sm_preconditions_and_epoch() {
        let mut d = SmData::default();
        assert_eq!(
            d.apply(put("routes/a", 1)).0,
            MetaReply::Applied { revision: 1 }
        );
        assert_eq!(d.cluster_epoch, 0);
        assert_eq!(
            d.apply(put("cluster/x", 1)).0,
            MetaReply::Applied { revision: 2 }
        );
        assert_eq!(d.cluster_epoch, 2);
        let (r, ev) = d.apply(MetaCommand::Put {
            key: "routes/a".into(),
            value: serde_json::json!(2),
            precondition: Some(Precondition {
                key: "routes/a".into(),
                expected_mod_revision: 0,
            }),
        });
        assert_eq!(
            r,
            MetaReply::Conflict {
                expected: 0,
                current: 1
            }
        );
        assert!(ev.is_none());
        assert_eq!(d.revision, 2);
        let (r, ev) = d.apply(MetaCommand::Delete {
            key: "nope".into(),
            precondition: None,
        });
        assert_eq!(r, MetaReply::Applied { revision: 3 });
        assert_eq!(ev.unwrap().value, None);
    }

    #[tokio::test]
    async fn log_store_persists_truncates_and_purges() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::open(dir.path()).unwrap();
        {
            let mut inner = store.inner.lock();
            let mut buf = Vec::new();
            for i in 1..=5 {
                encode_record(&mut buf, &entry(1, i, Some(put("k", i as i64)))).unwrap();
                inner
                    .entries
                    .insert(i, entry(1, i, Some(put("k", i as i64))));
            }
            inner.file.write_all(&buf).unwrap();
        }
        store
            .truncate(LogId::new(CommittedLeaderId::new(1, 1), 4))
            .await
            .unwrap();
        store
            .purge(LogId::new(CommittedLeaderId::new(1, 1), 1))
            .await
            .unwrap();
        store.save_vote(&Vote::new(3, 7)).await.unwrap();
        drop(store);
        // Torn tail is ignored.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join(LOG_FILE))
                .unwrap();
            f.write_all(&[9, 0, 0, 0, 1, 2]).unwrap();
        }
        let mut store = LogStore::open(dir.path()).unwrap();
        let st = store.get_log_state().await.unwrap();
        assert_eq!(st.last_purged_log_id.unwrap().index, 1);
        assert_eq!(st.last_log_id.unwrap().index, 3);
        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(
            got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(3, 7)));
    }
}

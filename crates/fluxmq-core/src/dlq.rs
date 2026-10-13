//! Per-channel dead-letter queue: in-memory index + persistence through the meta writer.
//!
//! Entry lifecycle:
//! * `add` — a message exhausted its attempts (or was nacked with `retry = false`).
//! * `begin_retry` — an operator re-enqueued it. The entry stays durable but is hidden from
//!   list/summary/size; it is removed when the redelivery is acked, or replaced by a new entry
//!   if it fails again. After a restart, entries that were being retried simply reappear in
//!   the DLQ (nothing is lost; at-least-once).
//! * `remove` — discarded by an operator, or its retry succeeded.
//!
//! Ids come from a broker-wide counter, so they are unique across channels and increase with
//! time (newest first = id descending).

use std::collections::BTreeMap;
use std::sync::Arc;

use fluxmq_proto::model::DeadLetter;
use parking_lot::Mutex;

use crate::persist::{ChannelFiles, DlqRecord, MetaOp, MetaSender, encode_dlq_record};
use crate::replica::PartitionReplica;

/// Compact the file online once there are this many tombstones (and more than live entries).
const COMPACT_TOMBSTONES: u64 = 4096;

struct Entry {
    dl: DeadLetter,
    retrying: bool,
}

#[derive(Default)]
struct State {
    entries: BTreeMap<u64, Entry>,
    visible: u64,
    tombstones: u64,
}

pub struct Dlq {
    state: Mutex<State>,
    files: Arc<ChannelFiles>,
    meta: MetaSender,
    /// Per-partition replication state: mutations of partitions this node leads are recorded
    /// for followers (under the DLQ lock, so followers see them in file order).
    repl: Arc<[Arc<PartitionReplica>]>,
}

impl Dlq {
    pub fn new(
        files: Arc<ChannelFiles>,
        meta: MetaSender,
        loaded: BTreeMap<u64, DeadLetter>,
        repl: Arc<[Arc<PartitionReplica>]>,
    ) -> Self {
        let visible = loaded.len() as u64;
        let entries = loaded
            .into_iter()
            .map(|(id, dl)| {
                (
                    id,
                    Entry {
                        dl,
                        retrying: false,
                    },
                )
            })
            .collect();
        Self {
            state: Mutex::new(State {
                entries,
                visible,
                tombstones: 0,
            }),
            files,
            meta,
            repl,
        }
    }

    fn replica(&self, dl: &DeadLetter) -> Option<&PartitionReplica> {
        self.repl.get(dl.message.partition as usize).map(|r| &**r)
    }

    fn persist(&self, rec: &DlqRecord) {
        // The meta writer only stops after every broker handle is gone; a send error means
        // shutdown, where losing the record is the documented in-memory-only window.
        let _ = self.meta.send(MetaOp::DlqAppend {
            files: self.files.clone(),
            bytes: encode_dlq_record(rec),
        });
    }

    fn maybe_compact(&self, st: &mut State) {
        if st.tombstones >= COMPACT_TOMBSTONES && st.tombstones > st.entries.len() as u64 {
            let mut bytes = Vec::new();
            for e in st.entries.values() {
                bytes
                    .extend_from_slice(&encode_dlq_record(&DlqRecord::Add(Box::new(e.dl.clone()))));
            }
            let _ = self.meta.send(MetaOp::DlqRewrite {
                files: self.files.clone(),
                bytes,
            });
            st.tombstones = 0;
        }
    }

    pub fn add(&self, dl: DeadLetter) {
        let mut st = self.state.lock();
        // Sent under the lock so file order matches in-memory order.
        self.persist(&DlqRecord::Add(Box::new(dl.clone())));
        if let Some(r) = self.replica(&dl) {
            r.push_dlq_add(&dl);
        }
        st.visible += 1;
        st.entries.insert(
            dl.id,
            Entry {
                dl,
                retrying: false,
            },
        );
    }

    pub fn remove(&self, id: u64) -> Option<DeadLetter> {
        let mut st = self.state.lock();
        let e = st.entries.remove(&id)?;
        if !e.retrying {
            st.visible -= 1;
        }
        self.persist(&DlqRecord::Remove(id));
        if let Some(r) = self.replica(&e.dl) {
            r.push_dlq_remove(id);
        }
        st.tombstones += 1;
        self.maybe_compact(&mut st);
        Some(e.dl)
    }

    /// Follower: inserts (or replaces) a replicated entry.
    pub fn apply_add(&self, dl: DeadLetter) {
        let mut st = self.state.lock();
        self.persist(&DlqRecord::Add(Box::new(dl.clone())));
        let prev = st.entries.insert(
            dl.id,
            Entry {
                dl,
                retrying: false,
            },
        );
        match prev {
            Some(p) if !p.retrying => {}
            _ => st.visible += 1,
        }
    }

    /// Follower: makes the entries of `partition` exactly `entries` (state snapshot).
    pub fn replace_partition(&self, partition: u32, entries: Vec<DeadLetter>) {
        let keep: std::collections::BTreeSet<u64> = entries.iter().map(|d| d.id).collect();
        let stale: Vec<u64> = {
            let st = self.state.lock();
            st.entries
                .values()
                .filter(|e| e.dl.message.partition == partition && !keep.contains(&e.dl.id))
                .map(|e| e.dl.id)
                .collect()
        };
        for id in stale {
            self.remove(id);
        }
        for dl in entries {
            let same = self
                .state
                .lock()
                .entries
                .get(&dl.id)
                .is_some_and(|e| !e.retrying && e.dl == dl);
            if !same {
                self.apply_add(dl);
            }
        }
    }

    /// Leader: all entries of `partition` plus `seq()` read while the DLQ lock is held (so the
    /// snapshot is exactly the state after mutation `seq`).
    pub fn snapshot_partition(
        &self,
        partition: u32,
        seq: impl FnOnce() -> u64,
    ) -> (Vec<DeadLetter>, u64) {
        let st = self.state.lock();
        let entries = st
            .entries
            .values()
            .filter(|e| e.dl.message.partition == partition)
            .map(|e| e.dl.clone())
            .collect();
        (entries, seq())
    }

    /// Every entry (including ones being retried, which are still durable) whose source
    /// partition satisfies `include` — the backup export, oldest first.
    pub fn export(&self, mut include: impl FnMut(u32) -> bool) -> Vec<DeadLetter> {
        let st = self.state.lock();
        st.entries
            .values()
            .filter(|e| include(e.dl.message.partition))
            .map(|e| e.dl.clone())
            .collect()
    }

    /// Source partition of entry `id`.
    pub fn partition_of(&self, id: u64) -> Option<u32> {
        self.state
            .lock()
            .entries
            .get(&id)
            .map(|e| e.dl.message.partition)
    }

    /// Largest entry id (0 when empty).
    pub fn max_id(&self) -> u64 {
        self.state
            .lock()
            .entries
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
    }

    /// Marks a visible entry as being retried; returns a copy of it.
    pub fn begin_retry(&self, id: u64) -> Option<DeadLetter> {
        let mut st = self.state.lock();
        let e = st.entries.get_mut(&id)?;
        if e.retrying {
            return None;
        }
        e.retrying = true;
        let dl = e.dl.clone();
        st.visible -= 1;
        Some(dl)
    }

    /// Entries of `partition` (including ones being retried).
    pub fn count_partition(&self, partition: u32) -> usize {
        self.state
            .lock()
            .entries
            .values()
            .filter(|e| e.dl.message.partition == partition)
            .count()
    }

    /// Number of visible entries.
    pub fn len(&self) -> u64 {
        self.state.lock().visible
    }

    /// Visits visible entries newest first until `f` returns false.
    pub fn for_each_newest_first(&self, mut f: impl FnMut(&DeadLetter) -> bool) {
        let st = self.state.lock();
        for e in st.entries.values().rev() {
            if !e.retrying && !f(&e.dl) {
                break;
            }
        }
    }

    /// Ids of visible entries matching `pred`.
    pub fn select(&self, mut pred: impl FnMut(&DeadLetter) -> bool) -> Vec<u64> {
        let st = self.state.lock();
        st.entries
            .values()
            .filter(|e| !e.retrying && pred(&e.dl))
            .map(|e| e.dl.id)
            .collect()
    }

    pub fn is_visible(&self, id: u64) -> bool {
        self.state
            .lock()
            .entries
            .get(&id)
            .is_some_and(|e| !e.retrying)
    }
}

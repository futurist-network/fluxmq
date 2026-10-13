//! Audit log of mutating admin calls: an in-memory ring (served by `GET /api/audit`) plus an
//! optional NDJSON file (`data_dir/audit.log`) appended by a dedicated thread so request
//! handlers never block on disk I/O.

use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

use fluxmq_proto::api::AuditEntry;
use parking_lot::Mutex;

/// Default number of entries kept in memory.
pub const DEFAULT_AUDIT_CAPACITY: usize = 10_000;

/// Cheap to clone; all clones share one log.
#[derive(Clone)]
pub struct AuditLog {
    inner: Arc<Inner>,
}

struct Inner {
    ring: Mutex<VecDeque<AuditEntry>>,
    capacity: usize,
    /// Serialized NDJSON lines for the file writer thread. `None` = memory only.
    file_tx: Option<mpsc::Sender<String>>,
}

impl AuditLog {
    /// Memory-only log (tests, or when no data dir is available).
    pub fn in_memory(capacity: usize) -> Self {
        Self::build(VecDeque::new(), capacity, None)
    }

    /// Opens (creating) `path`, loads its most recent `capacity` entries into memory and
    /// starts the append thread. **Blocking** — call from `spawn_blocking` or before the
    /// runtime starts serving.
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let ring = load_tail(&path, capacity)?;
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let (tx, rx) = mpsc::channel::<String>();
        std::thread::Builder::new()
            .name("fluxmq-audit".into())
            .spawn(move || writer_loop(file, rx, path))?;
        Ok(Self::build(ring, capacity, Some(tx)))
    }

    fn build(
        ring: VecDeque<AuditEntry>,
        capacity: usize,
        file_tx: Option<mpsc::Sender<String>>,
    ) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner: Arc::new(Inner {
                ring: Mutex::new(ring),
                capacity,
                file_tx,
            }),
        }
    }

    /// Records one mutating action.
    pub fn record(&self, actor: &str, action: &str, target: &str, detail: serde_json::Value) {
        let entry = AuditEntry {
            ts: fluxmq_proto::now_ms(),
            actor: actor.to_string(),
            action: action.to_string(),
            target: target.to_string(),
            detail,
        };
        tracing::info!(target: "fluxmq::audit", actor, action, target, "audit");
        if let Some(tx) = &self.inner.file_tx {
            match serde_json::to_string(&entry) {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        tracing::warn!("audit file writer is gone; entry kept in memory only");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "failed to serialize audit entry"),
            }
        }
        let mut ring = self.inner.ring.lock();
        if ring.len() >= self.inner.capacity {
            ring.pop_front();
        }
        ring.push_back(entry);
    }

    /// Up to `limit` entries, newest first.
    pub fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        self.inner
            .ring
            .lock()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

fn load_tail(path: &PathBuf, capacity: usize) -> io::Result<VecDeque<AuditEntry>> {
    let mut ring = VecDeque::new();
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(ring),
        Err(e) => return Err(e),
    };
    for line in BufReader::new(file).lines() {
        let line = line?;
        // A torn last line (crash mid-write) or garbage is skipped, not fatal.
        if let Ok(entry) = serde_json::from_str::<AuditEntry>(&line) {
            if ring.len() >= capacity.max(1) {
                ring.pop_front();
            }
            ring.push_back(entry);
        }
    }
    Ok(ring)
}

fn writer_loop(file: std::fs::File, rx: mpsc::Receiver<String>, path: PathBuf) {
    let mut out = BufWriter::new(file);
    while let Ok(first) = rx.recv() {
        let mut result = writeln!(out, "{first}");
        // Drain whatever else is queued, then flush once.
        while result.is_ok() {
            match rx.try_recv() {
                Ok(line) => result = writeln!(out, "{line}"),
                Err(_) => break,
            }
        }
        if let Err(e) = result.and_then(|_| out.flush()) {
            tracing::warn!(error = %e, path = %path.display(), "failed to append audit log");
        }
    }
    let _ = out.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_is_bounded_and_newest_first() {
        let log = AuditLog::in_memory(3);
        for i in 0..5 {
            log.record(
                "admin",
                "channel.create",
                &format!("c{i}"),
                serde_json::json!({}),
            );
        }
        let recent = log.recent(10);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].target, "c4");
        assert_eq!(recent[2].target, "c2");
        assert_eq!(log.recent(1).len(), 1);
    }

    #[test]
    fn persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        {
            let log = AuditLog::open(&path, 100).unwrap();
            log.record(
                "admin",
                "channel.delete",
                "orders",
                serde_json::json!({"x": 1}),
            );
            log.record("admin", "dlq.retry", "dlq", serde_json::json!({}));
        }
        // The writer thread flushes asynchronously; wait for both lines.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            if text.lines().count() == 2 || std::time::Instant::now() > deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let log = AuditLog::open(&path, 100).unwrap();
        let recent = log.recent(10);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].action, "dlq.retry");
        assert_eq!(recent[1].detail, serde_json::json!({"x": 1}));
    }
}

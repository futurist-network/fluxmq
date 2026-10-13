//! Single-node [`MetaStore`]: the whole map in memory, persisted to one JSON file with an
//! atomic rewrite (tmp + fsync + rename + dir fsync) on every write. Metadata changes are rare
//! (token/route/channel edits), so simplicity beats an append log here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{
    MetaError, MetaEvent, MetaFuture, MetaResult, MetaStore, Precondition, Versioned, validate,
};

#[derive(Default, Serialize, Deserialize)]
struct FileImage {
    revision: u64,
    entries: BTreeMap<String, Versioned>,
}

struct State {
    image: FileImage,
}

/// Cheap to clone.
#[derive(Clone)]
pub struct LocalMetaStore {
    inner: Arc<Inner>,
}

struct Inner {
    path: Option<PathBuf>,
    // Writers serialize on this mutex, including the file write, so revisions hit disk in order.
    state: Mutex<State>,
    events: broadcast::Sender<MetaEvent>,
}

impl LocalMetaStore {
    /// Opens (or creates) `path`. Blocking file I/O — call at startup or via `spawn_blocking`.
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Self> {
        let path = path.as_ref().to_path_buf();
        let image = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => FileImage::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self::with(Some(path), image))
    }

    /// Memory-only store (tests, `AppState::new` defaults).
    pub fn in_memory() -> Self {
        Self::with(None, FileImage::default())
    }

    fn with(path: Option<PathBuf>, image: FileImage) -> Self {
        let (events, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(Inner {
                path,
                state: Mutex::new(State { image }),
                events,
            }),
        }
    }

    fn apply(
        &self,
        key: &str,
        value: Option<serde_json::Value>,
        pre: Option<Precondition>,
    ) -> MetaResult<u64> {
        validate(key, value.as_ref())?;
        let mut st = self.inner.state.lock();
        if let Some(pre) = pre {
            let current = st.image.entries.get(&pre.key).map_or(0, |v| v.mod_revision);
            if current != pre.expected_mod_revision {
                return Err(MetaError::Conflict {
                    expected: pre.expected_mod_revision,
                    current,
                });
            }
        }
        let revision = st.image.revision + 1;
        let previous = match &value {
            Some(v) => st.image.entries.insert(
                key.to_string(),
                Versioned {
                    value: v.clone(),
                    mod_revision: revision,
                },
            ),
            None => st.image.entries.remove(key),
        };
        st.image.revision = revision;
        if let Some(path) = &self.inner.path
            && let Err(e) = persist(path, &st.image)
        {
            // Roll back so memory never runs ahead of disk.
            st.image.revision = revision - 1;
            match previous {
                Some(prev) => st.image.entries.insert(key.to_string(), prev),
                None => st.image.entries.remove(key),
            };
            return Err(e);
        }
        drop(st);
        let _ = self.inner.events.send(MetaEvent {
            revision,
            key: key.to_string(),
            value,
        });
        Ok(revision)
    }
}

fn persist(path: &Path, image: &FileImage) -> MetaResult<()> {
    use std::io::Write;
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(image)?;
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent()
        && let Ok(d) = std::fs::File::open(dir)
    {
        let _ = d.sync_all();
    }
    Ok(())
}

impl MetaStore for LocalMetaStore {
    fn get(&self, key: &str) -> Option<Versioned> {
        self.inner.state.lock().image.entries.get(key).cloned()
    }

    fn list(&self, prefix: &str) -> Vec<(String, Versioned)> {
        let st = self.inner.state.lock();
        st.image
            .entries
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn revision(&self) -> u64 {
        self.inner.state.lock().image.revision
    }

    fn put(
        &self,
        key: &str,
        value: serde_json::Value,
        precondition: Option<Precondition>,
    ) -> MetaFuture<'_, u64> {
        // Small synchronous fsync'd write; acceptable for rare metadata changes.
        let r = self.apply(key, Some(value), precondition);
        Box::pin(async move { r })
    }

    fn delete(&self, key: &str, precondition: Option<Precondition>) -> MetaFuture<'_, u64> {
        let r = self.apply(key, None, precondition);
        Box::pin(async move { r })
    }

    fn watch(&self) -> broadcast::Receiver<MetaEvent> {
        self.inner.events.subscribe()
    }

    fn wait_for(&self, _revision: u64) -> MetaFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaStoreExt;

    #[tokio::test]
    async fn crud_persist_watch_and_preconditions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.json");
        let store = LocalMetaStore::open(&path).unwrap();
        let mut rx = store.watch();

        let r1 = store
            .put("routes/a", serde_json::json!({"x": 1}), None)
            .await
            .unwrap();
        let r2 = store
            .put("routes/b", serde_json::json!({"x": 2}), None)
            .await
            .unwrap();
        store
            .put("auth/tokens/t", serde_json::json!("t"), None)
            .await
            .unwrap();
        assert_eq!((r1, r2), (1, 2));
        assert_eq!(rx.recv().await.unwrap().key, "routes/a");
        assert_eq!(store.list("routes/").len(), 2);

        // Precondition: create-only and compare-and-set.
        let err = store
            .put(
                "routes/a",
                serde_json::json!(0),
                Some(Precondition {
                    key: "routes/a".into(),
                    expected_mod_revision: 0,
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict { current: 1, .. }));
        store
            .put(
                "routes/a",
                serde_json::json!({"x": 3}),
                Some(Precondition {
                    key: "routes/a".into(),
                    expected_mod_revision: 1,
                }),
            )
            .await
            .unwrap();

        store.delete("routes/b", None).await.unwrap();
        drop(store);

        let reopened = LocalMetaStore::open(&path).unwrap();
        assert_eq!(reopened.revision(), 5);
        let a: serde_json::Value = reopened.get_as("routes/a").unwrap().unwrap();
        assert_eq!(a["x"], 3);
        assert!(reopened.get("routes/b").is_none());
        assert!(matches!(
            reopened.put("/bad", serde_json::json!(1), None).await,
            Err(MetaError::InvalidKey(_))
        ));
    }
}

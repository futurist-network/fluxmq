//! Offline restore of a backup into an empty data directory (always a **single node**; a
//! cluster is re-seeded from it). The broker must not be running on that directory.
//!
//! Order: manifest → encryption key check → metadata (`meta.json`, without `cluster/*` and
//! `backup/*` keys) → per channel: config, segments (streamed, decrypted, CRC- and
//! record-verified), group offsets, DLQ → directory fsync. Any failure aborts; the partially
//! written directory must then be removed before retrying.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fluxmq_core::backup::RestoreWriter;
use fluxmq_meta::{LocalMetaStore, MetaStore};
use serde::Serialize;
use tracing::warn;

use crate::crypto::Key;
use crate::manifest::{ChannelState, Manifest, MetaSnapshot};
use crate::objects;
use crate::service::{load_manifest, resolve_id};
use crate::store::Target;
use crate::{BackupError, Result, join_err};

#[derive(Debug, Clone, Default, Serialize)]
pub struct RestoreSummary {
    pub id: String,
    pub channels: u32,
    pub partitions: u32,
    pub segments: u32,
    pub messages: u64,
    pub bytes: u64,
    pub meta_keys: u32,
    pub dlq_entries: u64,
    pub groups: u32,
}

/// Restore options.
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    /// fsync restored files (disable only in tests).
    pub fsync: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self { fsync: true }
    }
}

/// Meta keys never restored: cluster placement (restore is a single node) and the backup
/// configuration (never part of a snapshot anyway).
pub fn restorable_meta_key(key: &str) -> bool {
    !key.starts_with("cluster/") && !key.starts_with("backup/")
}

/// Checks the key against the manifest's encryption fingerprint.
pub fn check_key(manifest: &Manifest, key: Option<&Key>) -> Result<()> {
    match (&manifest.encryption, key) {
        (None, _) => Ok(()),
        (Some(_), None) => Err(BackupError::Crypto(format!(
            "backup '{}' is encrypted: pass --encryption-key (or FLUXMQ_BACKUP_ENCRYPTION_KEY)",
            manifest.info.id
        ))),
        (Some(fp), Some(k)) if fp == k.fingerprint() => Ok(()),
        (Some(fp), Some(k)) => Err(BackupError::Crypto(format!(
            "wrong encryption key for backup '{}' (key fingerprint {}, backup needs {fp})",
            manifest.info.id,
            k.fingerprint()
        ))),
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> Result<T> {
    Ok(tokio::task::spawn_blocking(f).await.map_err(join_err)??)
}

/// Restores backup `id` (`latest` = newest) from `target` into `data_dir` (must be missing
/// or empty). `log` receives one human-readable line per step.
pub async fn restore(
    target: &Target,
    id: &str,
    data_dir: &Path,
    key: Option<&Key>,
    opts: &RestoreOptions,
    log: &(dyn Fn(&str) + Send + Sync),
) -> Result<RestoreSummary> {
    let id = resolve_id(target, id).await?;
    let manifest = load_manifest(target, &id).await?;
    check_key(&manifest, key)?;
    let dir = data_dir.to_path_buf();
    let fsync = opts.fsync;
    let writer = Arc::new(
        blocking(move || {
            let w = RestoreWriter::new(&dir)?;
            Ok(if fsync { w } else { w.without_sync() })
        })
        .await?,
    );
    let mut sum = RestoreSummary {
        id: id.clone(),
        ..RestoreSummary::default()
    };
    log(&format!(
        "restoring backup {id} (taken {} on node {}, {} channel(s))",
        manifest.info.started_at_ms,
        manifest.node_id,
        manifest.channels.len()
    ));

    // Metadata.
    let meta_bytes = objects::get_bytes(target, &manifest.meta, key).await?;
    let snap: MetaSnapshot = serde_json::from_slice(&meta_bytes)?;
    let restorable: Vec<(String, serde_json::Value)> = snap
        .entries
        .into_iter()
        .filter(|(k, _)| restorable_meta_key(k))
        .collect();
    if !restorable.is_empty() {
        let path: PathBuf = data_dir.join("meta.json");
        let store = tokio::task::spawn_blocking(move || LocalMetaStore::open(path))
            .await
            .map_err(join_err)??;
        for (k, v) in restorable {
            store.put(&k, v, None).await?;
            sum.meta_keys += 1;
        }
    }
    log(&format!("metadata: {} key(s)", sum.meta_keys));

    for ch in &manifest.channels {
        let name = ch.config.name.clone();
        let cfg = ch.config.clone();
        let w = writer.clone();
        blocking(move || w.create_channel(&cfg)).await?;
        sum.channels += 1;
        for p in &ch.partitions {
            sum.partitions += 1;
            if p.segments.is_empty() {
                let (w, n, part, end) = (writer.clone(), name.clone(), p.partition, p.end);
                blocking(move || w.empty_partition(&n, part, end)).await?;
                continue;
            }
            for seg in &p.segments {
                let (w, n, part, base) =
                    (writer.clone(), name.clone(), p.partition, seg.base_offset);
                let file = blocking(move || w.create_segment(&n, part, base)).await?;
                let mut file = tokio::fs::File::from_std(file);
                objects::download_to_file(target, &seg.log, key, &mut file).await?;
                drop(file);
                let index = match objects::get_bytes(target, &seg.index, key).await {
                    Ok(b) => Some(b),
                    Err(e) => {
                        warn!(object = %seg.index.key, error = %e, "segment index unusable; it will be rebuilt");
                        None
                    }
                };
                let (w, n, part, base, end) = (
                    writer.clone(),
                    name.clone(),
                    p.partition,
                    seg.base_offset,
                    seg.end_offset,
                );
                blocking(move || w.finish_segment(&n, part, base, end, index.as_deref())).await?;
                sum.segments += 1;
                sum.bytes += seg.log.len;
            }
            sum.messages += p.end - p.low;
        }
        let state_bytes = objects::get_bytes(target, &ch.state, key).await?;
        let state: ChannelState = serde_json::from_slice(&state_bytes)?;
        sum.groups += state.group_offsets.len() as u32;
        sum.dlq_entries += state.dlq.len() as u64;
        let (w, n) = (writer.clone(), name.clone());
        blocking(move || {
            w.write_offsets(&n, &state.group_offsets)?;
            w.write_dlq(&n, &state.dlq)
        })
        .await?;
        log(&format!(
            "channel {name}: {} partition(s) restored",
            ch.partitions.len()
        ));
    }
    let w =
        Arc::try_unwrap(writer).map_err(|_| BackupError::Other("writer still shared".into()))?;
    blocking(move || w.finish()).await?;
    log(&format!(
        "done: {} channel(s), {} segment(s), {} message(s), {} byte(s)",
        sum.channels, sum.segments, sum.messages, sum.bytes
    ));
    Ok(sum)
}

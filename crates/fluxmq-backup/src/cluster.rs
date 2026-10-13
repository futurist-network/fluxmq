//! Cluster backups: the controller coordinates, every node uploads the partitions it leads.
//!
//! 1. The controller (the only node whose scheduler / `run` executes in a cluster) snapshots
//!    the expected layout (channel → partition count) from the cluster metadata.
//! 2. It cuts and uploads its own led partitions ([`crate::service::cut_and_upload`]) and asks
//!    every other node to do the same through `POST /internal/v1/backup/partial`
//!    ([`PartialRequest`] → [`Partial`]). Each node reads the backup configuration from the
//!    replicated meta store and refuses when its copy doesn't match the controller's target /
//!    key fingerprint (not replicated yet).
//! 3. [`merge`] unions the partials: every partition of every expected channel must be covered
//!    (a partition whose leadership moved during the backup may be reported twice — the cut
//!    with the higher end wins; one reported by nobody fails the run), group offsets are taken
//!    per partition from the node that cut it, DLQ entries are concatenated.
//! 4. The controller writes the state objects, the metadata snapshot and the manifest
//!    ([`crate::service::finish`]). Restore is unchanged (always a single node).
//!
//! The HTTP transport lives in the API layer ([`ClusterHook`] is implemented there), so this
//! crate stays independent of the cluster runtime.

use std::collections::BTreeMap;

use fluxmq_core::backup::UNINIT_OFFSET;
use fluxmq_proto::model::{ChannelConfig, DeadLetter};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::manifest::PartitionEntry;
use crate::{BackupError, Result};

/// Internal endpoint a node answers with its [`Partial`].
pub const INTERNAL_PARTIAL_PATH: &str = "/internal/v1/backup/partial";

/// Controller → node: cut and upload your led partitions for backup `id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialRequest {
    pub id: String,
    pub include_messages: bool,
    /// `Target::describe` of the controller's configuration (consistency check).
    pub target: String,
    /// Encryption key fingerprint of the controller's configuration.
    pub fingerprint: Option<String>,
}

/// One channel as cut by one node (only the partitions it led).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialChannel {
    pub config: ChannelConfig,
    pub partitions: Vec<PartitionEntry>,
    /// Group → committed offset per partition (index = partition; partitions not cut by this
    /// node are `UNINIT_OFFSET`).
    pub group_offsets: BTreeMap<String, Vec<u64>>,
    pub dlq: Vec<DeadLetter>,
}

/// What one node uploaded (segments are already on the target).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partial {
    pub node_id: String,
    pub channels: Vec<PartialChannel>,
    pub messages: u64,
    /// Logical bytes of the segments (incl. reused).
    pub segment_bytes: u64,
    pub bytes_uploaded: u64,
    pub segments_uploaded: u32,
    pub segments_reused: u32,
}

/// Cluster access for the controller-side run (implemented by the API layer).
pub trait ClusterHook: Send + Sync + 'static {
    /// True when backups are coordinated here (the controller).
    fn is_controller(&self) -> bool;
    /// `Some(reason)` when this cluster can't run backups (a node older than 0.3.0 doesn't
    /// answer the internal backup API).
    fn unsupported(&self) -> Option<String>;
    /// Channel → partition count of every channel right now.
    fn expected(&self) -> BTreeMap<String, u32>;
    /// Asks every **other** node for its partial (concurrently). Any failure fails the run.
    fn remote_partials(&self, req: PartialRequest) -> BoxFuture<'static, Result<Vec<Partial>>>;
}

/// A merged channel, ready for [`crate::service::finish`].
#[derive(Debug, Clone)]
pub struct MergedChannel {
    pub config: ChannelConfig,
    pub partitions: Vec<PartitionEntry>,
    pub group_offsets: BTreeMap<String, Vec<u64>>,
    pub dlq: Vec<DeadLetter>,
}

/// Unions partials (see module docs). `expected = None` (single node): no coverage check.
/// Output is ordered by channel name, partitions by number.
pub fn merge(
    partials: Vec<Partial>,
    expected: Option<&BTreeMap<String, u32>>,
) -> Result<Vec<MergedChannel>> {
    struct Acc {
        config: ChannelConfig,
        /// partition → (entry, index of the partial that cut it)
        parts: BTreeMap<u32, (PartitionEntry, usize)>,
        /// (partial index, group offsets, cut partitions)
        offsets: Vec<(usize, BTreeMap<String, Vec<u64>>)>,
        dlq: Vec<(usize, DeadLetter)>,
    }
    let mut acc: BTreeMap<String, Acc> = BTreeMap::new();
    for (pi, partial) in partials.into_iter().enumerate() {
        for ch in partial.channels {
            let a = acc.entry(ch.config.name.clone()).or_insert_with(|| Acc {
                config: ch.config.clone(),
                parts: BTreeMap::new(),
                offsets: Vec::new(),
                dlq: Vec::new(),
            });
            for pe in ch.partitions {
                match a.parts.get(&pe.partition) {
                    Some((cur, _)) if cur.end >= pe.end => {}
                    _ => {
                        a.parts.insert(pe.partition, (pe, pi));
                    }
                }
            }
            a.offsets.push((pi, ch.group_offsets));
            a.dlq.extend(ch.dlq.into_iter().map(|d| (pi, d)));
        }
    }
    let mut out = Vec::with_capacity(acc.len());
    for (name, a) in acc {
        let want = expected.map(|e| e.get(&name).copied());
        let count = match want {
            // Not expected (created or deleted during the run): keep only if complete.
            Some(None) => {
                if (0..a.config.partitions).all(|p| a.parts.contains_key(&p)) {
                    a.config.partitions
                } else {
                    tracing::warn!(channel = %name, "channel changed during the backup; skipped");
                    continue;
                }
            }
            Some(Some(n)) => n,
            None => a.config.partitions,
        };
        if expected.is_some()
            && let Some(p) = (0..count).find(|p| !a.parts.contains_key(p))
        {
            return Err(BackupError::Other(format!(
                "partition {name}/{p} was not backed up: no node led it during the backup \
                 (leadership moving or a node unreachable); retry"
            )));
        }
        let owner: BTreeMap<u32, usize> = a.parts.iter().map(|(p, (_, i))| (*p, *i)).collect();
        let width = a.config.partitions.max(count) as usize;
        let mut group_offsets: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for (pi, groups) in a.offsets {
            for (g, offs) in groups {
                let merged = group_offsets
                    .entry(g)
                    .or_insert_with(|| vec![UNINIT_OFFSET; width]);
                for (p, off) in offs.into_iter().enumerate() {
                    if p < merged.len() && owner.get(&(p as u32)) == Some(&pi) {
                        merged[p] = off;
                    }
                }
            }
        }
        let dlq = a
            .dlq
            .into_iter()
            .filter(|(pi, d)| owner.get(&d.message.partition) == Some(pi))
            .map(|(_, d)| d)
            .collect();
        out.push(MergedChannel {
            config: a.config,
            partitions: a.parts.into_values().map(|(e, _)| e).collect(),
            group_offsets,
            dlq,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxmq_proto::model::ChannelMode;

    fn pe(p: u32, end: u64) -> PartitionEntry {
        PartitionEntry {
            partition: p,
            low: 0,
            end,
            segments: vec![],
        }
    }

    fn ch(parts: Vec<PartitionEntry>, offs: Vec<u64>) -> PartialChannel {
        let mut config = ChannelConfig::new("c", ChannelMode::Stream);
        config.partitions = 3;
        PartialChannel {
            config,
            partitions: parts,
            group_offsets: BTreeMap::from([("g".to_string(), offs)]),
            dlq: vec![],
        }
    }

    fn partial(node: &str, c: PartialChannel) -> Partial {
        Partial {
            node_id: node.into(),
            channels: vec![c],
            messages: 0,
            segment_bytes: 0,
            bytes_uploaded: 0,
            segments_uploaded: 0,
            segments_reused: 0,
        }
    }

    #[test]
    fn merges_partitions_and_offsets_by_owner() {
        const U: u64 = UNINIT_OFFSET;
        let a = partial("a", ch(vec![pe(0, 10), pe(2, 5)], vec![7, U, 3]));
        // b also reports partition 2 (leadership moved) with a higher end: it wins.
        let b = partial("b", ch(vec![pe(1, 4), pe(2, 6)], vec![U, 2, 4]));
        let expected = BTreeMap::from([("c".to_string(), 3)]);
        let m = merge(vec![a, b], Some(&expected)).unwrap();
        assert_eq!(m.len(), 1);
        let ends: Vec<u64> = m[0].partitions.iter().map(|p| p.end).collect();
        assert_eq!(ends, [10, 4, 6]);
        assert_eq!(m[0].group_offsets["g"], [7, 2, 4]);
    }

    #[test]
    fn missing_partition_fails_and_single_node_is_unchecked() {
        let a = partial("a", ch(vec![pe(0, 10)], vec![1, 0, 0]));
        let expected = BTreeMap::from([("c".to_string(), 3)]);
        let err = merge(vec![a.clone()], Some(&expected)).unwrap_err();
        assert!(err.to_string().contains("partition c/1"), "{err}");
        assert_eq!(merge(vec![a], None).unwrap()[0].partitions.len(), 1);
    }
}

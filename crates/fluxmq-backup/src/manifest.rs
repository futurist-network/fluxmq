//! Backup manifest (`manifests/<backup_id>.json`) and object naming.
//!
//! Object layout under the target prefix:
//!
//! * `manifests/<id>.json` — this manifest, plain JSON (never encrypted, so backups can be
//!   listed without the key; it holds channel configs and offsets ranges, no payloads).
//! * `segments/<channel>/<p>/<base:020>-<crc:08x>.log` (+ `.idx`) — segment bytes `[0, len)`;
//!   `crc` is the CRC-32 of the plaintext log bytes, so identical content maps to the same
//!   name and is uploaded once (incremental). Encrypted objects carry the key fingerprint:
//!   `<base:020>-<crc:08x>.k<fingerprint>.log` (content under another key is another object).
//! * `state/<id>/meta.json` — meta-store snapshot (minus `backup/*`).
//! * `state/<id>/channels/<channel>.json` — group offsets + DLQ of the channel.
//!
//! Every object referenced here records its plaintext length and CRC; restore verifies both.

use fluxmq_proto::backup::{BackupChannelSummary, BackupInfo, BackupManifest};
use fluxmq_proto::model::ChannelConfig;
use serde::{Deserialize, Serialize};

pub const FORMAT: u32 = 1;
pub const MANIFESTS_DIR: &str = "manifests";
pub const SEGMENTS_DIR: &str = "segments";
pub const STATE_DIR: &str = "state";

/// A stored object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    /// Key relative to the target prefix.
    pub key: String,
    /// Plaintext length.
    pub len: u64,
    /// CRC-32 of the plaintext.
    pub crc: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub base_offset: u64,
    /// One past the last record in the object.
    pub end_offset: u64,
    pub log: ObjectRef,
    pub index: ObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionEntry {
    pub partition: u32,
    /// First offset held (== `end` when no records are held).
    pub low: u64,
    /// One past the last offset held: the partition's consistent cut point.
    pub end: u64,
    /// Contiguous, oldest first. Empty for metadata-only backups.
    pub segments: Vec<SegmentEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelEntry {
    pub config: ChannelConfig,
    pub partitions: Vec<PartitionEntry>,
    /// Group offsets + DLQ ([`ChannelState`]).
    pub state: ObjectRef,
    /// Consumer groups in the state object (summary; 0 in manifests before v0.3.0 GA).
    #[serde(default)]
    pub groups: u32,
    /// Dead letters in the state object (summary).
    #[serde(default)]
    pub dlq_entries: u64,
}

/// `state/<id>/channels/<name>.json`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelState {
    /// Group → committed offset per partition (`u64::MAX` = never initialized).
    pub group_offsets: std::collections::BTreeMap<String, Vec<u64>>,
    pub dlq: Vec<fluxmq_proto::model::DeadLetter>,
}

/// `state/<id>/meta.json`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaSnapshot {
    pub revision: u64,
    pub entries: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub info: BackupInfo,
    pub node_id: String,
    pub cluster_name: String,
    pub fluxmq_version: String,
    pub include_messages: bool,
    /// Fingerprint of the encryption key (`None` = not encrypted).
    pub encryption: Option<String>,
    pub meta: ObjectRef,
    pub channels: Vec<ChannelEntry>,
    /// Nodes that uploaded partition data (cluster: the leaders; single node: itself).
    #[serde(default)]
    pub nodes: Vec<String>,
}

impl Manifest {
    /// The API view (`GET /api/backups/{id}`): no object keys / CRCs.
    pub fn summary(&self) -> BackupManifest {
        let mut channels: Vec<BackupChannelSummary> = self
            .channels
            .iter()
            .map(|c| BackupChannelSummary {
                name: c.config.name.clone(),
                mode: c.config.mode,
                partitions: c.partitions.len() as u32,
                messages: if self.include_messages {
                    c.partitions.iter().map(|p| p.end - p.low).sum()
                } else {
                    0
                },
                bytes: c
                    .partitions
                    .iter()
                    .flat_map(|p| &p.segments)
                    .map(|s| s.log.len)
                    .sum(),
                segments: c.partitions.iter().map(|p| p.segments.len() as u32).sum(),
                groups: c.groups,
                dlq_entries: c.dlq_entries,
            })
            .collect();
        channels.sort_by(|a, b| a.name.cmp(&b.name));
        let nodes = if self.nodes.is_empty() {
            vec![self.node_id.clone()]
        } else {
            self.nodes.clone()
        };
        BackupManifest {
            info: self.info.clone(),
            node_id: self.node_id.clone(),
            cluster_name: self.cluster_name.clone(),
            fluxmq_version: self.fluxmq_version.clone(),
            include_messages: self.include_messages,
            encryption: self.encryption.clone(),
            nodes,
            channels,
        }
    }

    /// Every object key this manifest references (for garbage collection).
    pub fn referenced(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.meta.key.as_str()).chain(self.channels.iter().flat_map(|c| {
            std::iter::once(c.state.key.as_str()).chain(c.partitions.iter().flat_map(|p| {
                p.segments
                    .iter()
                    .flat_map(|s| [s.log.key.as_str(), s.index.key.as_str()])
            }))
        }))
    }
}

pub fn manifest_key(id: &str) -> String {
    format!("{MANIFESTS_DIR}/{id}.json")
}

/// Parses `manifests/<id>.json` back to `<id>`.
pub fn id_from_manifest_key(key: &str) -> Option<&str> {
    let name = key.rsplit('/').next()?;
    name.strip_suffix(".json")
}

/// `(log key, index key)` of a segment object.
pub fn segment_keys(
    channel: &str,
    partition: u32,
    base: u64,
    crc: u32,
    fingerprint: Option<&str>,
) -> (String, String) {
    let stem = match fingerprint {
        None => format!("{SEGMENTS_DIR}/{channel}/{partition}/{base:020}-{crc:08x}"),
        Some(fp) => format!("{SEGMENTS_DIR}/{channel}/{partition}/{base:020}-{crc:08x}.k{fp}"),
    };
    (format!("{stem}.log"), format!("{stem}.idx"))
}

pub fn meta_key(id: &str) -> String {
    format!("{STATE_DIR}/{id}/meta.json")
}

pub fn channel_state_key(id: &str, channel: &str) -> String {
    format!("{STATE_DIR}/{id}/channels/{channel}.json")
}

/// New sortable backup id: `<yyyymmddThhmmssZ>-<mmm><3 hex>` — the suffix starts with the
/// milliseconds so ids of one second still sort by time, then 3 random hex digits.
pub fn new_backup_id(now_ms: u64) -> String {
    let t =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now_ms as i64).unwrap_or_default();
    let mut r = [0u8; 2];
    let _ = getrandom::fill(&mut r);
    format!(
        "{}-{:03}{:03x}",
        t.format("%Y%m%dT%H%M%SZ"),
        now_ms % 1000,
        u16::from_le_bytes(r) & 0xfff
    )
}

/// Ids are sortable by time; this validates the shape (path-safe).
pub fn valid_backup_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_keys() {
        let id = new_backup_id(1_700_000_000_000);
        assert!(id.starts_with("20231114T221320Z-"), "{id}");
        assert!(valid_backup_id(&id));
        assert!(!valid_backup_id("../x"));
        assert_eq!(id_from_manifest_key(&manifest_key(&id)), Some(id.as_str()));
        let (l, i) = segment_keys("orders", 2, 42, 0xdead_beef, None);
        assert_eq!(l, "segments/orders/2/00000000000000000042-deadbeef.log");
        assert_eq!(i, "segments/orders/2/00000000000000000042-deadbeef.idx");
        let (l, _) = segment_keys("o", 0, 0, 1, Some("ab"));
        assert_eq!(l, "segments/o/0/00000000000000000000-00000001.kab.log");
    }
}

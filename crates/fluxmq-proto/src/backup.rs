//! Backups contract (v0.3): consistent, incremental backups to S3-compatible object storage (AWS
//! S3, MinIO, Cloudflare R2, Backblaze B2, …) or a filesystem path, and restore into an empty
//! data directory.
//!
//! A backup = a JSON **manifest** + content objects:
//! * metadata snapshot (meta store dump: channels, routes, tokens (hashes only), notifiers,
//!   webhooks, cluster assignments),
//! * per channel: config, consumer group committed offsets, DLQ dump,
//! * per partition: **sealed segments** (immutable → uploaded once, reused by later backups =
//!   incremental) and the active segment up to a consistent committed offset.
//!
//! Object layout under `prefix`: `manifests/<backup_id>.json`, `segments/<channel>/<p>/<base_offset>-<crc>.log`
//! (+ `.idx`), `state/<backup_id>/…`. Retention deletes manifests beyond `keep_last` /
//! `keep_days` and garbage-collects segments no remaining manifest references.
//!
//! Consistency: per partition, data up to the offset recorded in the manifest; group offsets
//! and DLQ captured after the segment cut (restore may redeliver a little — at-least-once).
//! Cluster: the backup runs on the **controller** (scheduler and `POST /api/backups/run` on any
//! node are executed there); it asks every node (`/internal/v1/backup/partial`) to cut and
//! upload the partitions it currently leads, checks that every partition of every channel was
//! covered exactly once (else the run fails — e.g. a leader was moving), then writes the state
//! objects and the manifest. Partition cuts of different leaders are taken at slightly
//! different moments (each partition is consistent on its own). Restore always produces a
//! **single node** (seed for a new cluster).
//!
//! When a cluster can't run a backup (a node too old to answer the internal backup API) the
//! API answers HTTP 400 with `ApiError.error = "unsupported_in_cluster"`
//! (`crate::api::ERROR_UNSUPPORTED_IN_CLUSTER`).
//!
//! Optional client-side encryption (`encryption_key`, AES-256-GCM, base64 32-byte key): object
//! bodies are encrypted before upload; without the key a backup can't be restored.
//! Secrets are redacted on read (`notify::REDACTED`, uniform keep/remove rule from `notify`).
//!
//! REST details: `GET /api/backups/config` returns `null` when nothing is configured;
//! `GET /api/backups/{id}` (`latest` = newest) returns a [`BackupManifest`] — a summary of the
//! stored manifest (object keys and CRCs stay internal to the backup format);
//! `POST /api/backups/test-target` with a [`BackupTarget`] body checks connectivity/permissions
//! (write + read + delete of a probe object) and returns `notify::TestResult`.
//! Restore with encryption: `fluxmq restore … --encryption-key <base64>` or env
//! `FLUXMQ_BACKUP_ENCRYPTION_KEY`.

use serde::{Deserialize, Serialize};

use crate::model::ChannelMode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackupTarget {
    S3 {
        /// Custom endpoint for MinIO/R2/B2 (`https://<account>.r2.cloudflarestorage.com`);
        /// `None` = AWS.
        endpoint: Option<String>,
        region: String,
        bucket: String,
        #[serde(default)]
        prefix: String,
        access_key_id: String,
        secret_access_key: String,
        /// Path-style addressing (MinIO).
        #[serde(default)]
        path_style: bool,
    },
    Filesystem {
        path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupConfig {
    pub enabled: bool,
    pub target: BackupTarget,
    /// Scheduled run every N seconds (≥ 300). Default 3600.
    pub interval_secs: u32,
    /// Keep at most this many backups (≥ 1). Default 24.
    pub keep_last: u32,
    /// Also delete backups older than this (0 = no age limit). Default 7.
    pub keep_days: u32,
    /// false = metadata/offsets/DLQ only (tiny, fast).
    pub include_messages: bool,
    pub encryption_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupInfo {
    /// Sortable: `<yyyymmddThhmmssZ>-<short random>`.
    pub id: String,
    pub status: BackupStatus,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    /// "scheduled" | "manual" | "cli".
    pub trigger: String,
    pub channels: u32,
    pub partitions: u32,
    pub messages: u64,
    pub bytes_uploaded: u64,
    pub segments_uploaded: u32,
    /// Segments reused from earlier backups (incremental savings).
    pub segments_reused: u32,
    /// While running: bytes planned for this backup (known after the cut); when finished: the
    /// logical size of the backup (incl. reused segments).
    pub total_bytes: u64,
    /// While running: bytes processed so far (uploaded or confirmed reusable). Progress =
    /// `progress_bytes / total_bytes`.
    pub progress_bytes: u64,
    pub error: Option<String>,
}

/// `GET /api/backups/status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupOverview {
    pub configured: bool,
    pub enabled: bool,
    pub running: Option<BackupInfo>,
    pub last_success: Option<BackupInfo>,
    pub last_failure: Option<BackupInfo>,
    pub next_run_ms: Option<u64>,
}

/// `GET /api/backups/{id}`: what a backup holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub info: BackupInfo,
    /// Node that wrote the manifest (the controller in a cluster).
    pub node_id: String,
    pub cluster_name: String,
    pub fluxmq_version: String,
    /// false = metadata, offsets and DLQ only.
    pub include_messages: bool,
    /// Fingerprint of the encryption key (`None` = not encrypted); restore needs the key
    /// with this fingerprint.
    pub encryption: Option<String>,
    /// Nodes that uploaded partition data (cluster: the leaders at backup time; single node:
    /// the node itself).
    pub nodes: Vec<String>,
    /// Per channel, ordered by name.
    pub channels: Vec<BackupChannelSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupChannelSummary {
    pub name: String,
    pub mode: ChannelMode,
    pub partitions: u32,
    /// Messages held up to the cut (0 for metadata-only backups).
    pub messages: u64,
    /// Logical bytes of the channel's segments in this backup (incl. reused ones).
    pub bytes: u64,
    pub segments: u32,
    /// Consumer groups whose committed offsets are included.
    pub groups: u32,
    pub dlq_entries: u64,
}

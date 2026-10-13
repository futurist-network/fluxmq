//! FluxMQ backups (v0.3): consistent, incremental backups of a broker to S3-compatible object
//! storage (AWS S3, MinIO, Cloudflare R2, Backblaze B2, …) or a filesystem path, and restore
//! into an empty data directory. Contract: `fluxmq_proto::backup`; design:
//! `docs/design/v0.3.md` §3; operations: `docs/ARCHITECTURE.md` (Backups).
//!
//! * [`store`] — target → `object_store` handle (+ prefix), connectivity probe.
//! * [`crypto`] — optional AES-256-GCM chunked object encryption (format documented there).
//! * [`manifest`] — the JSON manifest and object naming.
//! * [`objects`] — streaming, bounded-memory upload/download with CRC and encryption.
//! * [`config`] — validation, secret redaction and the keep/remove merge rule.
//! * [`service`] — [`BackupService`]: scheduler, manual runs, progress, history, retention
//!   and garbage collection.
//! * [`cluster`] — cluster backups: the controller merges the partitions every leader
//!   uploaded ([`cluster::ClusterHook`] is implemented by the API layer).
//! * [`restore`] — offline restore into an empty data directory.

pub mod cluster;
pub mod config;
pub mod crypto;
pub mod manifest;
pub mod objects;
pub mod restore;
pub mod service;
pub mod store;

pub use service::{BackupEvent, BackupService, ServiceOptions};
pub use store::Target;

/// Meta-store key of the backup configuration.
pub const CONFIG_KEY: &str = "backup/config";

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("invalid: {0}")]
    Invalid(String),
    #[error("backups are not configured")]
    NotConfigured,
    #[error("{0}")]
    Conflict(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Unsupported(String),
    /// The cluster can't run backups (API: `unsupported_in_cluster`).
    #[error("{0}")]
    UnsupportedInCluster(String),
    /// A cluster backup was started on a node that isn't the controller.
    #[error("{0}")]
    NotController(String),
    #[error("encryption: {0}")]
    Crypto(String),
    #[error("storage: {0}")]
    Store(#[from] object_store::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("broker: {0}")]
    Core(#[from] fluxmq_core::CoreError),
    #[error("metadata: {0}")]
    Meta(#[from] fluxmq_meta::MetaError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T, E = BackupError> = std::result::Result<T, E>;

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

pub(crate) fn join_err(e: tokio::task::JoinError) -> BackupError {
    BackupError::Other(format!("background task failed: {e}"))
}

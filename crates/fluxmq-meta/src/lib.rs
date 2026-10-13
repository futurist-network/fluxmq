//! Metadata store: a small, strongly consistent, revisioned key → JSON document map.
//!
//! Everything that must be identical on every node lives here — RBAC tokens (`auth/…`),
//! routes (`routes/…`), and in a cluster also channel configs and partition assignments
//! (`cluster/…`). Subsystems program against [`MetaStore`]; the server picks the
//! implementation:
//!
//! * [`LocalMetaStore`] — single node: one JSON file, atomic rewrite on every change.
//! * a Raft-backed store from `fluxmq-cluster` — writes go through the Raft leader and are
//!   applied on every node in log order.
//!
//! Contract:
//! * Writes are linearizable and return the new store-wide `revision` (monotonic, +1 per write).
//! * Reads are served from the local replica (may lag a committed write by a few ms on
//!   followers — callers needing read-your-writes use the revision returned by the write
//!   with [`MetaStore::wait_for`]).
//! * [`MetaStore::watch`] delivers every applied change in revision order. A lagging receiver
//!   gets `RecvError::Lagged` and must reload via [`MetaStore::list`].
//! * Values are JSON objects/arrays/scalars up to 1 MiB; keys are `/`-separated, ≤ 256 bytes.

mod local;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

pub use local::LocalMetaStore;

pub const MAX_KEY_LEN: usize = 256;
pub const MAX_VALUE_LEN: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error("value too large ({0} bytes)")]
    TooLarge(usize),
    #[error("revision conflict: expected {expected}, current {current}")]
    Conflict { expected: u64, current: u64 },
    /// Cluster: no Raft leader right now (election in progress) — retry.
    #[error("metadata store unavailable: {0}")]
    Unavailable(String),
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("storage: {0}")]
    Io(#[from] std::io::Error),
}

pub type MetaResult<T> = Result<T, MetaError>;
pub type MetaFuture<'a, T> = Pin<Box<dyn Future<Output = MetaResult<T>> + Send + 'a>>;

/// One applied change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetaEvent {
    pub revision: u64,
    pub key: String,
    /// `None` = deleted.
    pub value: Option<serde_json::Value>,
}

/// A stored document with the revision that last modified it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Versioned {
    pub value: serde_json::Value,
    pub mod_revision: u64,
}

/// A conditional write: applied only if the key's current `mod_revision` equals `expected`
/// (`0` = key must not exist). Enables safe read-modify-write without a global lock.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Precondition {
    pub key: String,
    pub expected_mod_revision: u64,
}

pub trait MetaStore: Send + Sync + 'static {
    fn get(&self, key: &str) -> Option<Versioned>;

    /// All entries whose key starts with `prefix`, sorted by key.
    fn list(&self, prefix: &str) -> Vec<(String, Versioned)>;

    /// Current store-wide revision (last applied write).
    fn revision(&self) -> u64;

    /// Upsert; returns the new revision.
    fn put(
        &self,
        key: &str,
        value: serde_json::Value,
        precondition: Option<Precondition>,
    ) -> MetaFuture<'_, u64>;

    /// Delete (no-op if absent, still returns a revision); returns the new revision.
    fn delete(&self, key: &str, precondition: Option<Precondition>) -> MetaFuture<'_, u64>;

    fn watch(&self) -> broadcast::Receiver<MetaEvent>;

    /// Resolves once the local replica has applied `revision` (immediately for the local store).
    fn wait_for(&self, revision: u64) -> MetaFuture<'_, ()>;
}

/// Typed helpers over any [`MetaStore`].
pub trait MetaStoreExt: MetaStore {
    fn get_as<T: DeserializeOwned>(&self, key: &str) -> MetaResult<Option<T>> {
        self.get(key)
            .map(|v| serde_json::from_value(v.value))
            .transpose()
            .map_err(Into::into)
    }

    fn list_as<T: DeserializeOwned>(&self, prefix: &str) -> MetaResult<Vec<(String, T)>> {
        self.list(prefix)
            .into_iter()
            .map(|(k, v)| Ok((k, serde_json::from_value(v.value)?)))
            .collect()
    }

    fn put_as<'a, T: Serialize>(&'a self, key: &'a str, value: &T) -> MetaFuture<'a, u64> {
        match serde_json::to_value(value) {
            Ok(v) => self.put(key, v, None),
            Err(e) => Box::pin(async move { Err(e.into()) }),
        }
    }
}

impl<S: MetaStore + ?Sized> MetaStoreExt for S {}

/// Shared handle used across subsystems.
pub type SharedMetaStore = Arc<dyn MetaStore>;

pub fn validate(key: &str, value: Option<&serde_json::Value>) -> MetaResult<()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN || key.starts_with('/') || key.contains("//") {
        return Err(MetaError::InvalidKey(key.to_string()));
    }
    if let Some(v) = value {
        let len = serde_json::to_vec(v)?.len();
        if len > MAX_VALUE_LEN {
            return Err(MetaError::TooLarge(len));
        }
    }
    Ok(())
}

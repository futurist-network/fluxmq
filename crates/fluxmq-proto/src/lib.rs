//! Shared contract between every FluxMQ component.
//!
//! * [`model`] — domain types (channels, messages, delivery attempts, dead letters).
//! * [`wire`] — the binary client ⇄ broker protocol (length-prefixed postcard frames).
//! * [`batch`] — record batch format (v0.7): produced, stored, replicated and delivered as is.
//! * [`telemetry`] — the observability contract consumed by the UI over WebSocket (JSON).
//! * [`api`] — REST request/response bodies.
//! * [`live`] — live subscription gateway status (v0.11).
//! * [`scheduled`] — delayed messages (v0.12).
//!
//! This crate is deliberately dependency-light and contains no I/O so that the broker,
//! the SDK, the gateway and tests all agree on one definition. `docs/CONTRACT.md` is the
//! human-readable mirror of it — keep both in sync.

pub mod api;
pub mod auth;
pub mod backup;
pub mod batch;
pub mod cluster;
pub mod live;
pub mod model;
pub mod notify;
pub mod routing;
pub mod scheduled;
pub mod telemetry;
pub mod tls;
pub mod webhooks;
pub mod wire;

pub use model::*;

/// Version string reported by the broker and the SDK.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default TCP port for the client protocol.
pub const DEFAULT_CLIENT_PORT: u16 = 7420;
/// Default HTTP port for REST, WebSocket telemetry and the bundled UI.
pub const DEFAULT_HTTP_PORT: u16 = 7421;
/// Default port for Raft (metadata consensus) between nodes.
pub const DEFAULT_RAFT_PORT: u16 = 7422;
/// Default port for partition data replication between nodes.
pub const DEFAULT_REPLICATION_PORT: u16 = 7423;

/// Consumer groups with this prefix are reserved for the system (e.g. `$router`).
pub const SYSTEM_GROUP_PREFIX: char = '$';
/// The built-in router's consumer group.
pub const ROUTER_GROUP: &str = "$router";

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

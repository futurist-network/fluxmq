//! Observability plane.
//!
//! One collector task samples [`fluxmq_core::Broker::stats`] on a fixed tick, diffs consecutive
//! snapshots into rates and windowed percentiles, maintains per-topic state documents
//! (the payloads defined in [`fluxmq_proto::telemetry`]), evaluates alert rules, records
//! history (ring buffers + rollups) and fans out JSON merge-patch deltas to sessions.
//!
//! Computation is done once per tick regardless of how many browsers are connected.
//! Sessions are latest-wins: a slow session coalesces pending deltas per topic.
//!
//! Module map:
//! * `collector` — the tick loop and cadences; drives everything below.
//! * [`rates`] — snapshot window, rates, windowed percentiles.
//! * [`documents`] — builders for the topic payloads.
//! * [`health`] / [`alerts`] — health rules and the alert engine (hysteresis, texts).
//! * [`events`] — the events feed.
//! * [`tail`] — refcounted live-tail pumps.
//! * [`history`] — metric ring buffers, rollups, persistence.
//! * [`node`] — sysinfo sampling.
//! * [`patch`] — RFC 7386 merge-patch diff/apply.
//! * `hub` / [`session`] — topic store, refcounts, per-WebSocket sessions.
//! * [`snapshot`] — node-stats wire format (`/internal/v1/node-stats`) and the cluster merge.
//! * [`cluster_source`] — peer polling + cluster-wide view (clustered nodes only).
//! * [`prometheus`] — `GET /metrics` text exposition (node-local).

use std::time::Duration;

pub mod alerts;
pub mod cluster_source;
mod collector;
pub mod documents;
pub mod events;
pub mod health;
pub mod history;
mod hub;
pub mod node;
pub mod patch;
pub mod prometheus;
pub mod rates;
pub mod session;
pub mod snapshot;
mod source;
pub mod tail;

#[cfg(test)]
mod cluster_tests;
#[cfg(test)]
mod tests;

pub use hub::TelemetryHub;
pub use session::Session;

/// What [`TelemetryHub::notifications`] delivers.
#[derive(Debug, Clone, PartialEq)]
pub enum TelemetrySignal {
    /// An alert passed the open hysteresis.
    AlertOpened(fluxmq_proto::telemetry::Alert),
    /// An open alert passed the resolve hysteresis (last known text/severity).
    AlertResolved(fluxmq_proto::telemetry::Alert),
    /// A new entry of the events feed (also emitted for alert transitions, kinds
    /// `alert_opened` / `alert_resolved`). Coalesced dead-letter updates are not re-emitted.
    Event(fluxmq_proto::telemetry::Event),
}

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Base collector tick (rates are computed over a sliding window of ticks).
    pub tick: Duration,
    /// Window for rates/percentiles shown as "current".
    pub rate_window: Duration,
    /// Where rollups are persisted across restarts (`None` = memory only).
    pub history_dir: Option<std::path::PathBuf>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
            rate_window: Duration::from_secs(2),
            history_dir: None,
        }
    }
}

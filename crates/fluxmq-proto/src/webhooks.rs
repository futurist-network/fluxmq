//! HTTP push subscriptions (v0.3): the broker delivers a channel's messages to an HTTP endpoint,
//! so consumers need no SDK.
//!
//! Each subscription is a consumer group `$webhook:<name>` on its channel (system group: lag,
//! retries, DLQ and retry timeline appear in the console like any group). Workers run where the
//! channel's partitions are led (single node: locally). Stored in meta `webhooks/<id>`.
//!
//! Request: `POST url` (+ configured headers), `Content-Type: application/json`, body =
//! [`WebhookEnvelope`] (or the raw payload when `format = raw`, metadata then in `X-FluxMQ-*`
//! headers). When `secret` is set: `X-FluxMQ-Signature: t=<unix_ts>,v1=<hex hmac-sha256(secret,
//! "<t>.<body>")>` (same scheme as notification webhooks). Always: `X-FluxMQ-Delivery-Id`
//! (stable across retries of the same message: `{channel}/{partition}/{offset}`),
//! `X-FluxMQ-Attempt`.
//!
//! Outcome mapping: 2xx → ack. 408, 425, 429, 5xx, timeouts, connection errors → nack with retry
//! (channel retry policy; `Retry-After` honoured up to the backoff cap); other 4xx → nack without
//! retry (straight to DLQ) unless `retry_on_4xx`. A target blocked by the SSRF policy at
//! delivery time (DNS now resolves to a private address, or the policy changed) is never
//! retried: straight to DLQ with reason `blocked by policy`. DLQ reasons are short classes —
//! `HTTP <status>`, `timeout`, `connection refused`, `connection failed`, `TLS error`,
//! `DNS error`, `blocked by policy` (`crate::api::short_error`) — so the DLQ inbox groups
//! failures meaningfully; the full cause is in `last_error_detail` / `error_detail`.
//!
//! 3xx is treated like a non-retryable 4xx (redirects are not followed; reason `HTTP 3xx`).
//!
//! Ordering: `max_in_flight = 1` preserves per-partition order; higher values trade order for
//! throughput. Batching (`batch_max > 1`) sends a JSON array of envelopes in one request.
//!
//! Validation: `name` 1–64 chars `[a-z0-9._-]` (group `$webhook:<name>`); `timeout_ms`
//! 100–60000 (must stay below the channel's `ack_timeout_ms`, else rejected); `max_in_flight`
//! 1–64; `batch_max` 1–500; `batch_linger_ms` 0–10000; `format = raw` ⇒ `batch_max = 1`.
//! `name`, `channel` and `start` are immutable after creation. Secrets (`secret`, every header
//! value) follow the uniform rule in `notify` module docs (REDACTED keeps, `""` removes).
//! Test before saving: `POST /api/webhooks/test` with a [`CreateWebhookRequest`] body. Test an
//! edited subscription: `POST /api/webhooks/{id}/test` with an **optional**
//! [`UpdateWebhookRequest`] body merged over the stored one (`REDACTED` = stored secret /
//! header value), validated like an update and not persisted; no body = the stored one.
//! Stats are cluster-wide (summed over nodes when clustered).
//!
//! Circuit breaker (v0.11, `pause_after_failures`, 0 = off): after that many consecutive
//! requests failed with a retryable outcome (timeout, connection error, 5xx, 408/425/429) a
//! node's worker stops taking messages from the channel, so they wait there instead of burning
//! retry attempts and filling the DLQ while the endpoint is down. It then probes the endpoint
//! with a synthetic test delivery (`delivery_id` = `probe/<ts>`, `flux-test` header) every
//! 2 s, doubling up to 60 s; the first 2xx resumes delivery. A non-retryable answer (other 4xx)
//! means the endpoint is up and resets the count. Recent attempts:
//! `GET /api/webhooks/{id}/attempts`; delivery rate history: `GET /api/webhooks/{id}/history`
//! (both node-local memory, merged over the cluster, reset on restart).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::StartFrom;
use crate::routing::Condition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadFormat {
    /// [`WebhookEnvelope`] JSON.
    #[default]
    Envelope,
    /// Body = message payload bytes; metadata in `X-FluxMQ-Channel`, `-Partition`, `-Offset`,
    /// `-Key`, `-Timestamp` and `X-FluxMQ-Header-<name>` headers. Not combinable with batching.
    Raw,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookSubscription {
    pub id: String,
    /// Unique; the consumer group is `$webhook:<name>`.
    pub name: String,
    pub channel: String,
    pub url: String,
    pub enabled: bool,
    pub headers: BTreeMap<String, String>,
    /// Read back as `notify::REDACTED`.
    pub secret: Option<String>,
    /// Only matching messages are delivered (non-matching are acked and skipped).
    pub filter: Vec<Condition>,
    pub format: PayloadFormat,
    pub timeout_ms: u32,
    pub max_in_flight: u32,
    pub batch_max: u32,
    pub batch_linger_ms: u32,
    pub retry_on_4xx: bool,
    /// Circuit breaker: pause after this many consecutive retryable failures (0 = off).
    /// Additive (v0.11): stored subscriptions without it have the breaker off.
    #[serde(default)]
    pub pause_after_failures: u32,
    /// Where a new group starts (ignored afterwards).
    pub start: StartFrom,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateWebhookRequest {
    pub name: String,
    pub channel: String,
    pub url: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub secret: Option<String>,
    #[serde(default)]
    pub filter: Vec<Condition>,
    #[serde(default)]
    pub format: PayloadFormat,
    /// Defaults: timeout 10000, in-flight 4, batch 1, linger 0, retry_on_4xx false, start latest.
    pub timeout_ms: Option<u32>,
    pub max_in_flight: Option<u32>,
    pub batch_max: Option<u32>,
    pub batch_linger_ms: Option<u32>,
    pub retry_on_4xx: Option<bool>,
    /// Default 0 (off).
    #[serde(default)]
    pub pause_after_failures: Option<u32>,
    pub start: Option<StartFrom>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct UpdateWebhookRequest {
    pub url: Option<String>,
    pub enabled: Option<bool>,
    pub headers: Option<BTreeMap<String, String>>,
    /// `notify::REDACTED` keeps, `""` removes.
    pub secret: Option<String>,
    pub filter: Option<Vec<Condition>>,
    pub format: Option<PayloadFormat>,
    pub timeout_ms: Option<u32>,
    pub max_in_flight: Option<u32>,
    pub batch_max: Option<u32>,
    pub batch_linger_ms: Option<u32>,
    pub retry_on_4xx: Option<bool>,
    #[serde(default)]
    pub pause_after_failures: Option<u32>,
}

/// JSON body per message (`format = envelope`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookEnvelope {
    /// `{channel}/{partition}/{offset}` — stable across retries (dedupe on it).
    pub delivery_id: String,
    pub subscription: String,
    pub channel: String,
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub timestamp_ms: u64,
    pub attempt: u32,
    pub payload: crate::api::JsonPayload,
}

/// `GET /api/webhooks/stats`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookStats {
    pub id: String,
    pub delivered: u64,
    pub failed: u64,
    pub skipped_by_filter: u64,
    pub in_flight: u64,
    /// HTTP status of the last attempt (`None` before the first one, or when the last attempt
    /// got no response — then `last_error` says why).
    pub last_status: Option<u16>,
    /// Short error of the last failed delivery (`HTTP 503`, `timeout`, `connection refused`,
    /// `TLS error`, `DNS error`, `blocked by policy`, …).
    pub last_error: Option<String>,
    /// Full cause / response snippet behind `last_error`. Additive (v0.3).
    #[serde(default)]
    pub last_error_detail: Option<String>,
    pub last_delivery_ms: Option<u64>,
    pub latency_p50_ms: f64,
    pub latency_p99_ms: f64,
    /// Set while the circuit breaker holds delivery on at least one node. Additive (v0.11).
    #[serde(default)]
    pub breaker: Option<WebhookBreaker>,
}

/// An open circuit breaker (merged over nodes: earliest `since_ms` / `next_probe_ms`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookBreaker {
    pub since_ms: u64,
    /// Consecutive failed requests that opened it, plus failed probes since.
    pub failures: u32,
    pub next_probe_ms: u64,
    /// Nodes where it is open (cluster: each node delivers the partitions it leads).
    pub nodes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    /// 2xx: acked.
    Delivered,
    /// Retryable failure: redelivered after the channel's backoff (or dead-lettered when
    /// attempts are exhausted).
    Retry,
    /// Non-retryable: dead-lettered.
    Dead,
    /// Circuit breaker probe (synthetic delivery) succeeded / failed.
    ProbeOk,
    ProbeFailed,
}

/// One request of a webhook (`GET /api/webhooks/{id}/attempts`, newest first, last 100 per
/// node).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookAttempt {
    pub at_ms: u64,
    pub node: String,
    /// Delivery id of the first message (`{channel}/{partition}/{offset}`; `probe/<ts>` for
    /// probes).
    pub delivery_id: String,
    /// Messages in the request (batching).
    pub messages: u32,
    /// Attempt number of the first message (1-based; 0 for probes).
    pub attempt: u32,
    pub outcome: AttemptOutcome,
    pub status: Option<u16>,
    pub latency_ms: f64,
    /// Short error (same classes as [`WebhookStats::last_error`]).
    pub error: Option<String>,
    pub error_detail: Option<String>,
}

/// `GET /api/webhooks/{id}/history?range=1h|24h`: requests per bucket, summed over nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookHistory {
    pub bucket_ms: u64,
    pub points: Vec<WebhookHistoryPoint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookHistoryPoint {
    /// Bucket start.
    pub ts_ms: u64,
    /// Messages acked / failed (retry or dead) in the bucket.
    pub delivered: u64,
    pub failed: u64,
    /// Response latency of requests with a response.
    pub latency_avg_ms: f64,
    pub latency_max_ms: f64,
}

/// `POST /api/webhooks/{id}/test` — sends one synthetic envelope (`delivery_id` = `test/...`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookTestResult {
    pub ok: bool,
    pub status: Option<u16>,
    pub latency_ms: u64,
    pub response_snippet: Option<String>,
    /// Short error (same classes as [`WebhookStats::last_error`]).
    pub error: Option<String>,
    /// Full cause chain behind `error`. Additive (v0.3).
    #[serde(default)]
    pub error_detail: Option<String>,
}

fn yes() -> bool {
    true
}

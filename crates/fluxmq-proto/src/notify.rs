//! Notifications contract (v0.3): deliver alerts and broker events to Telegram, Slack, Discord,
//! email, PagerDuty and generic webhooks.
//!
//! Model: **notifiers** (where to send) + **rules** (what to send where). Both live in the meta
//! store (`notify/notifiers/<id>`, `notify/rules/<id>`) and are therefore identical on every
//! node; in a cluster only the **controller** dispatches (no duplicates), on a single node the
//! node itself. Inputs are the telemetry plane's alert transitions (opened / resolved / still
//! open) and broker events (`EventKind`).
//!
//! Delivery: per (rule, notifier) a grouping window (`group_wait_secs`) batches everything that
//! fired together into one message ("3 issues need attention …"); open alerts are re-notified
//! every `repeat_interval_secs`; `send_resolved` sends a recovery message. Failed sends are
//! retried with backoff (3 attempts) and recorded in the history (`GET /api/notify/history`,
//! persisted since v0.5 in the replicated system channel [`SYSTEM_NOTIFICATIONS_CHANNEL`]
//! (controller appends; 7 days, newest 10 000 readable; any node serves it).
//!
//! Secrets (`bot_token`, Slack/Discord webhook URLs, SMTP password, routing keys, HMAC secrets,
//! and **every custom header value**) are never returned by the API: reads show [`REDACTED`].
//! Uniform update rule for every secret field (here, in webhooks and backups): [`REDACTED`] =
//! keep the stored value, `""` = remove it (optional fields), anything else = replace. The
//! generic webhook `url` is not a secret and is returned as is.
//!
//! `min_severity` / `alert_kinds` filter alerts only; events are selected by `event_kinds` (and
//! `channels`). Endpoints for testing before saving: `POST /api/notify/notifiers/test` with a
//! [`CreateNotifierRequest`] body (no persistence) and `POST /api/notify/notifiers/{id}/test`
//! with an **optional** [`UpdateNotifierRequest`] body: the draft is merged over the stored
//! notifier (secret rule as for updates — [`REDACTED`] = the stored secret) and tested without
//! being persisted; no body = test the stored notifier as is.
//!
//! Errors are short and human (`error`: `connection refused`, `timeout`, `HTTP 503`,
//! `TLS error`, `DNS error`, … — see [`crate::api::short_error`]) with the full cause chain /
//! response snippet in the additive `error_detail` field (secrets are never included).
//!
//! History in a cluster: only the controller dispatches, so `GET /api/notify/history` on any
//! other node is answered by the controller (proxied over the internal API); when the
//! controller can't be reached the node answers with its own (usually empty) history.
//!
//! **Pull feed** for external systems (no inbound endpoint needed): every alert transition and
//! broker event is also appended to the system channel [`SYSTEM_EVENTS_CHANNEL`] (1 partition,
//! retention 7 days, created on start, visible read-only in the console). Read it with
//! `GET /api/events/feed?after=<cursor>&limit=<1..1000, default 100>&wait_ms=<0..30000>`
//! (long-poll: returns as soon as something newer exists or after `wait_ms`), or consume it via
//! the SDK / an HTTP push webhook like any channel. Cursor = the item's offset as a decimal
//! string; omit `after` to start from the oldest retained item, `after=latest` to only get new
//! ones. `tail=N` (1–1000, mutually exclusive with `after`) returns the newest N retained items
//! (oldest first) — use it to seed a "latest events" view, then continue with
//! `after=<next_cursor>`; with `wait_ms` and an empty feed it long-polls like `latest`.
//! Feed responses always carry the real `cursor` of every item. Permission: Read (or a token
//! whose consume scope matches the channel).
//!
//! The JSON stored in the channel is the same [`FeedItem`] with `cursor: ""` (the offset is not
//! known before the append): SDK / webhook consumers of `$system.events` use the **message
//! offset** as the cursor.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::telemetry::{AlertKind, EventKind, Health};

/// Placeholder returned instead of secrets; echo it back to keep the stored value.
pub const REDACTED: &str = "********";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotifierKind {
    /// Bot API `sendMessage` (HTML parse mode). `thread_id` = forum topic.
    Telegram {
        bot_token: String,
        chat_id: String,
        thread_id: Option<i64>,
    },
    /// Incoming webhook (Block Kit message).
    Slack { webhook_url: String },
    /// Discord webhook (embeds).
    Discord { webhook_url: String },
    /// Email via SMTP. `tls`: "starttls" | "tls" | "none".
    Email {
        smtp_host: String,
        smtp_port: u16,
        username: Option<String>,
        password: Option<String>,
        tls: String,
        from: String,
        to: Vec<String>,
    },
    /// PagerDuty Events API v2 (trigger/resolve with `dedup_key` = alert id).
    #[serde(rename = "pagerduty")]
    PagerDuty { routing_key: String },
    /// Generic JSON POST of [`NotificationPayload`]. When `secret` is set the request carries
    /// `X-FluxMQ-Signature: t=<unix_ts>,v1=<hex hmac-sha256(secret, "<t>.<body>")>`.
    Webhook {
        url: String,
        secret: Option<String>,
        headers: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notifier {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub kind: NotifierKind,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateNotifierRequest {
    pub name: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub kind: NotifierKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UpdateNotifierRequest {
    pub name: Option<String>,
    pub enabled: Option<bool>,
    /// Secret fields may be [`REDACTED`] to keep the stored value.
    pub kind: Option<NotifierKind>,
}

/// What a rule matches. Empty lists = any.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleMatch {
    /// Alerts with severity ≥ this (`warning` or `critical`).
    pub min_severity: Health,
    #[serde(default)]
    pub alert_kinds: Vec<AlertKind>,
    /// Broker events to forward (none by default — events are chatty).
    #[serde(default)]
    pub event_kinds: Vec<EventKind>,
    /// Channel globs (`orders.*`). Alerts/events without a channel match only when empty.
    #[serde(default)]
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationRule {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub notifier_ids: Vec<String>,
    #[serde(rename = "match")]
    pub matcher: RuleMatch,
    /// Batch everything firing within this window into one message. Default 30.
    pub group_wait_secs: u32,
    /// Re-notify still-open alerts. Default 14400 (4 h); 0 = never.
    pub repeat_interval_secs: u32,
    pub send_resolved: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRuleRequest {
    pub name: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub notifier_ids: Vec<String>,
    #[serde(rename = "match")]
    pub matcher: RuleMatch,
    pub group_wait_secs: Option<u32>,
    pub repeat_interval_secs: Option<u32>,
    pub send_resolved: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UpdateRuleRequest {
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub notifier_ids: Option<Vec<String>>,
    #[serde(rename = "match")]
    pub matcher: Option<RuleMatch>,
    pub group_wait_secs: Option<u32>,
    pub repeat_interval_secs: Option<u32>,
    pub send_resolved: Option<bool>,
}

/// Body of generic webhook notifications (and the shape rendered by chat notifiers).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationPayload {
    /// Unique per send (idempotency key for receivers).
    pub id: String,
    pub cluster: String,
    pub sent_at_ms: u64,
    pub rule: String,
    /// "firing" | "resolved" | "event" | "test".
    pub status: String,
    pub alerts: Vec<crate::telemetry::Alert>,
    pub events: Vec<crate::telemetry::Event>,
    /// Absolute console URL when `--public-url` is configured.
    pub console_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Sent,
    Failed,
    /// Matched but suppressed (repeat interval, disabled notifier).
    Suppressed,
}

/// `GET /api/notify/history` (newest first).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationRecord {
    pub id: String,
    pub ts: u64,
    pub rule_id: Option<String>,
    pub notifier_id: String,
    pub status: DeliveryStatus,
    pub summary: String,
    pub attempts: u32,
    /// Short error (`connection refused`, `timeout`, `HTTP 503`, `TLS error`, `DNS error`, …).
    pub error: Option<String>,
    /// Full cause chain / response snippet behind `error` (secrets stripped). Additive (v0.3).
    #[serde(default)]
    pub error_detail: Option<String>,
}

/// `POST /api/notify/notifiers/{id}/test` → sends a test message synchronously. Also the
/// result of `POST /api/backups/test-target`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestResult {
    pub ok: bool,
    pub latency_ms: u64,
    /// Short error (see [`NotificationRecord::error`]).
    pub error: Option<String>,
    /// Full cause chain behind `error`. Additive (v0.3).
    #[serde(default)]
    pub error_detail: Option<String>,
}

/// System channel carrying alert transitions and broker events (the pull feed).
pub const SYSTEM_EVENTS_CHANNEL: &str = "$system.events";

/// v0.5: notification delivery history (`NotificationRecord` JSON payloads).
pub const SYSTEM_NOTIFICATIONS_CHANNEL: &str = "$system.notifications";

/// One item of the pull feed (also the JSON payload stored in [`SYSTEM_EVENTS_CHANNEL`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedItem {
    /// Offset in the system channel, decimal string; pass as `after` to continue. Always set
    /// in feed responses; `""` in the stored channel payload (use the message offset there).
    pub cursor: String,
    pub ts: u64,
    /// "alert_opened" | "alert_resolved" | "event".
    pub kind: String,
    pub alert: Option<crate::telemetry::Alert>,
    pub event: Option<crate::telemetry::Event>,
    /// Node that produced it (the controller in a cluster).
    pub node: String,
}

/// `GET /api/events/feed`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedPage {
    pub items: Vec<FeedItem>,
    /// Cursor to pass as `after` next time (= last item's cursor, or the request's `after` when
    /// empty; for `latest` / `tail` on an empty page: the cursor of the newest retained item,
    /// `None` when the feed has never had an item).
    pub next_cursor: Option<String>,
    /// True when older items were already removed by retention (a gap since `after`).
    pub gap: bool,
}

fn yes() -> bool {
    true
}

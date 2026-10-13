//! Routing contract (roadmap stage 8): content-based routes evaluated by the built-in router,
//! plus the filter DSL shared by routes and message search.
//!
//! Semantics: every enabled route of a source channel is evaluated for each message the
//! router consumes (system consumer group `$router` on the source channel, so routing never
//! slows the publish path and its lag is visible like any group). For each matching route the
//! message is re-published to every target channel with headers `flux-routed-from`
//! (`{channel}/{partition}/{offset}`), `flux-route-id` and `flux-route-path` (comma list of
//! visited channels; a target already on the path is skipped — loop protection, max 8 hops).
//! Failed forwards are retried and finally dead-lettered in the source channel's DLQ with
//! group `$router`. Groups whose name starts with `$` are reserved for the system.
//!
//! Routes apply only to messages published **after** the route exists: a new source channel
//! worker starts at the end of the log (an existing `$router` group keeps its committed
//! offset). History is never re-routed automatically — the backtest (`POST /api/routes/test`)
//! only *previews* what a route would have matched; use replay to re-process old messages.
//! When the last route of a source channel is deleted, its worker stops and the `$router`
//! group is deleted.
//!
//! Filter DSL (message search `filter=` and the routing editor's text mode):
//! ```text
//! expr   := cond ( "AND" cond )*
//! cond   := field op value | field "exists" | field "not exists"
//! field  := "key" | "partition" | "header." name | payload-path   (a.b.0.c — JSON path)
//!           (`key` and `partition` always mean the message key / partition: a payload field
//!            literally named `key` or `partition` must be written `payload.key`; any other
//!            `payload.`-prefixed path is the same as the bare path)
//! op     := "=" | "!=" | ">" | ">=" | "<" | "<=" | "in" | "not in" | "prefix" | "contains"
//! value  := number | "string" | true | false | null | "[" value ("," value)* "]"
//! ```
//! Example: `type = "order.created" AND country = "MD" AND amount >= 100`.
//! Bare words in value position are treated as strings (`country = MD`).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "name")]
pub enum FieldRef {
    Key,
    Partition,
    Header(String),
    /// Dot path into the JSON payload (`order.items.0.sku`). Non-JSON payloads never match.
    Payload(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Prefix,
    Contains,
    Exists,
    NotExists,
}

/// One predicate. `value` is required for all ops except `exists`/`not_exists`.
/// Comparison: numbers compare numerically, strings lexically; `eq` between a number and a
/// numeric string matches (`order_id = 8842` matches `"8842"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    pub field: FieldRef,
    pub op: Op,
    /// Absent = no value; an explicit JSON `null` is the value `null` (`note = null`).
    #[serde(
        default,
        deserialize_with = "explicit_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<serde_json::Value>,
}

/// Keeps an explicit `null` as `Some(Value::Null)` (a missing field stays `None` via
/// `default`), so `{"op":"eq","value":null}` round-trips.
fn explicit_null<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(d).map(Some)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Route {
    pub id: String,
    pub name: String,
    pub source: String,
    pub enabled: bool,
    /// All must match (AND). Empty = match everything.
    pub conditions: Vec<Condition>,
    pub targets: Vec<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// `POST /api/routes` (admin).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateRouteRequest {
    pub name: String,
    pub source: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub conditions: Vec<Condition>,
    pub targets: Vec<String>,
}

/// `PATCH /api/routes/{id}` (admin).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct UpdateRouteRequest {
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub conditions: Option<Vec<Condition>>,
    pub targets: Option<Vec<String>>,
}

/// `POST /api/routes/test` — backtest conditions against stored messages ("Matched last 24h").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteTestRequest {
    pub source: String,
    pub conditions: Vec<Condition>,
    /// Look-back window; default 24 h.
    pub window_ms: Option<u64>,
    /// Matching messages to return as samples; default 20, max 100.
    pub sample_limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteTestResponse {
    pub scanned: u64,
    pub matched: u64,
    /// True when the scan cap (200k messages) was hit before covering the window.
    pub truncated: bool,
    pub samples: Vec<crate::api::MessageView>,
}

/// `GET /api/routes/stats` — per route counters since broker start (node-local in a cluster:
/// each node counts what its router workers processed).
///
/// `evaluated`/`matched` count messages; `forwarded`/`failed` count **per target** — a message
/// matching a route with 3 targets adds 1 to `matched` and 3 to `forwarded`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteStats {
    pub id: String,
    pub evaluated: u64,
    pub matched: u64,
    pub forwarded: u64,
    pub failed: u64,
    pub last_matched_ms: Option<u64>,
}

/// `POST /api/filter/parse` (viewer) — DSL → conditions (and back via `rendered`) for the UI
/// editor. A syntax error is a 400 whose `ApiError.position` is the byte offset of the error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParseFilterRequest {
    pub filter: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParseFilterResponse {
    pub conditions: Vec<Condition>,
    /// Canonical DSL text.
    pub rendered: String,
}

/// `POST /api/filter/render` (viewer) — structured conditions → canonical DSL text (the UI's
/// builder → text direction). Invalid conditions (e.g. a missing value) are a 400.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenderFilterRequest {
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenderFilterResponse {
    pub rendered: String,
}

fn default_true() -> bool {
    true
}

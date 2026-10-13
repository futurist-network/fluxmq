//! Notifications (v0.3): notifier/rule CRUD, test sends, delivery history, and the
//! `$system.events` pull feed. Secrets are redacted by the service on every read; audit
//! entries carry redacted copies only. Permissions live in the central table (`auth.rs`).

use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use fluxmq_notify::{FeedAfter, NotifyError, validate};
use fluxmq_proto::notify::{
    CreateNotifierRequest, CreateRuleRequest, FeedPage, NotificationRecord, NotificationRule,
    Notifier, TestResult, UpdateNotifierRequest, UpdateRuleRequest,
};
use serde::Deserialize;
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Json, OptionalJson, Path, Query};
use crate::state::AppState;

/// NotifyError → HTTP: 404, 400 invalid, 409 in use / concurrent change, 503 unavailable.
pub fn notify_err(e: NotifyError) -> ApiErr {
    match e {
        NotifyError::NotFound(what) => ApiErr::not_found(format!("{what} not found")),
        NotifyError::Invalid(m) => ApiErr::bad_request(m),
        NotifyError::InUse(rules) => ApiErr::conflict(format!(
            "notifier is used by rule(s): {rules}; remove it from them first"
        )),
        NotifyError::Unavailable => ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "notifications are not available on this server",
        ),
        NotifyError::Meta(fluxmq_meta::MetaError::Conflict { .. }) => {
            ApiErr::conflict("modified concurrently; reload and retry")
        }
        NotifyError::Meta(fluxmq_meta::MetaError::Unavailable(m)) => ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal",
            format!("metadata store unavailable: {m}"),
        ),
        NotifyError::Core(e) => ApiErr::from(e),
        other => {
            tracing::error!(error = %other, "notifications error");
            ApiErr::internal(other.to_string())
        }
    }
}

// ---------------------------------------------------------------------------------------
// Notifiers
// ---------------------------------------------------------------------------------------

pub async fn list_notifiers(State(s): State<AppState>) -> axum::Json<Vec<Notifier>> {
    axum::Json(s.notify.notifiers())
}

pub async fn get_notifier(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<Notifier>> {
    s.notify
        .notifier(&id)
        .map(axum::Json)
        .ok_or_else(|| ApiErr::not_found(format!("notifier '{id}' not found")))
}

pub async fn create_notifier(
    State(s): State<AppState>,
    Json(req): Json<CreateNotifierRequest>,
) -> ApiResult<(StatusCode, axum::Json<Notifier>)> {
    let n = s.notify.create_notifier(req).await.map_err(notify_err)?;
    // `n` is already redacted.
    s.audit(
        "notifier.create",
        &n.id,
        serde_json::to_value(&n).unwrap_or_default(),
    );
    Ok((StatusCode::CREATED, axum::Json(n)))
}

pub async fn update_notifier(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateNotifierRequest>,
) -> ApiResult<axum::Json<Notifier>> {
    let detail = json!({
        "name": req.name,
        "enabled": req.enabled,
        "kind": req.kind.as_ref().map(validate::redact_kind),
    });
    let n = s
        .notify
        .update_notifier(&id, req)
        .await
        .map_err(notify_err)?;
    s.audit("notifier.update", &id, detail);
    Ok(axum::Json(n))
}

pub async fn delete_notifier(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    s.notify.delete_notifier(&id).await.map_err(notify_err)?;
    s.audit("notifier.delete", &id, json!({}));
    Ok(StatusCode::NO_CONTENT)
}

/// Sends a test message through a stored notifier (synchronous, one attempt); an optional
/// draft ([`UpdateNotifierRequest`]) is merged over the stored notifier, not persisted.
pub async fn test_notifier(
    State(s): State<AppState>,
    Path(id): Path<String>,
    OptionalJson(draft): OptionalJson<UpdateNotifierRequest>,
) -> ApiResult<axum::Json<TestResult>> {
    let edited = draft.is_some();
    let r = s.notify.test(&id, draft).await.map_err(notify_err)?;
    s.audit(
        "notifier.test",
        &id,
        json!({ "ok": r.ok, "error": r.error, "draft": edited }),
    );
    Ok(axum::Json(r))
}

/// Test-before-save: same body as create, nothing persisted.
pub async fn test_unsaved(
    State(s): State<AppState>,
    Json(req): Json<CreateNotifierRequest>,
) -> ApiResult<axum::Json<TestResult>> {
    let detail = json!({ "name": req.name, "kind": validate::redact_kind(&req.kind) });
    let r = s.notify.test_unsaved(req).await.map_err(notify_err)?;
    let mut detail = detail;
    detail["ok"] = json!(r.ok);
    s.audit("notifier.test", "(unsaved)", detail);
    Ok(axum::Json(r))
}

// ---------------------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------------------

pub async fn list_rules(State(s): State<AppState>) -> axum::Json<Vec<NotificationRule>> {
    axum::Json(s.notify.rules())
}

pub async fn get_rule(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<NotificationRule>> {
    s.notify
        .rule(&id)
        .map(axum::Json)
        .ok_or_else(|| ApiErr::not_found(format!("rule '{id}' not found")))
}

pub async fn create_rule(
    State(s): State<AppState>,
    Json(req): Json<CreateRuleRequest>,
) -> ApiResult<(StatusCode, axum::Json<NotificationRule>)> {
    let r = s.notify.create_rule(req).await.map_err(notify_err)?;
    s.audit(
        "notify_rule.create",
        &r.id,
        serde_json::to_value(&r).unwrap_or_default(),
    );
    Ok((StatusCode::CREATED, axum::Json(r)))
}

pub async fn update_rule(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateRuleRequest>,
) -> ApiResult<axum::Json<NotificationRule>> {
    let detail = serde_json::to_value(&req).unwrap_or_default();
    let r = s.notify.update_rule(&id, req).await.map_err(notify_err)?;
    s.audit("notify_rule.update", &id, detail);
    Ok(axum::Json(r))
}

pub async fn delete_rule(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    s.notify.delete_rule(&id).await.map_err(notify_err)?;
    s.audit("notify_rule.delete", &id, json!({}));
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------------------
// History + feed
// ---------------------------------------------------------------------------------------

/// Internal path of a node-local history read (cluster secret): the partition leader of
/// `$system.notifications` answers history requests proxied by other nodes.
pub const INTERNAL_HISTORY_PATH: &str = "/internal/v1/notify/history";

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct HistoryQuery {
    pub limit: Option<usize>,
}

/// `GET /api/notify/history?limit=` (default 1000, at most 10 000; newest first).
///
/// v0.5: the history is the replicated system channel `$system.notifications` (the
/// controller appends), so it survives a controller change and a full restart. Like the
/// events feed, the request is answered by that channel's partition leader (proxied over the
/// internal API); with no reachable leader the node answers from its own copy merged with its
/// in-memory ring of recent records.
pub async fn history(
    State(s): State<AppState>,
    Query(q): Query<HistoryQuery>,
) -> axum::Json<Vec<NotificationRecord>> {
    let limit = q
        .limit
        .unwrap_or(fluxmq_notify::HISTORY_CAPACITY)
        .clamp(1, fluxmq_notify::history::HISTORY_MAX_RECORDS);
    if s.cluster.is_clustered()
        && let fluxmq_cluster::PartitionRoute::Remote(leader) = s
            .cluster
            .route_partition(fluxmq_proto::notify::SYSTEM_NOTIFICATIONS_CHANNEL, 0)
    {
        let q = HistoryQuery { limit: Some(limit) };
        match s
            .cluster
            .internal_http()
            .get::<_, Vec<NotificationRecord>>(&leader.http_addr, INTERNAL_HISTORY_PATH, &q)
            .await
        {
            Ok(v) => return axum::Json(v),
            Err(e) => {
                tracing::debug!(leader = %leader.id, error = %e, "notification history proxy failed; answering locally")
            }
        }
    }
    axum::Json(s.notify.stored_history(limit).await)
}

/// Node-local history read (proxy target; never proxies again).
async fn internal_history(
    State(s): State<AppState>,
    Query(q): Query<HistoryQuery>,
) -> axum::Json<Vec<NotificationRecord>> {
    let limit = q.limit.unwrap_or(fluxmq_notify::HISTORY_CAPACITY);
    axum::Json(s.notify.stored_history(limit).await)
}

/// Controller side of a record produced on another node (a test send handled there).
async fn internal_history_append(
    State(s): State<AppState>,
    Json(records): Json<Vec<NotificationRecord>>,
) -> ApiResult<axum::Json<serde_json::Value>> {
    s.notify.append_history(records).await.map_err(notify_err)?;
    Ok(axum::Json(json!({})))
}

/// Node-to-node routes, merged into `cluster::internal`.
pub fn internal() -> axum::Router<AppState> {
    axum::Router::new()
        .route(INTERNAL_HISTORY_PATH, axum::routing::get(internal_history))
        .route(
            fluxmq_notify::history::INTERNAL_HISTORY_APPEND_PATH,
            axum::routing::post(internal_history_append),
        )
        .route(INTERNAL_FEED_PATH, axum::routing::get(internal_feed))
}

/// Internal path of a node-local feed read (cluster secret): the partition leader of
/// `$system.events` answers feed requests proxied by nodes that don't hold the replica.
pub const INTERNAL_FEED_PATH: &str = "/internal/v1/events/feed";

/// Extra time the proxy waits beyond the long-poll itself (network, the leader's read).
const FEED_PROXY_SLACK: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct FeedQuery {
    pub after: Option<String>,
    pub tail: Option<u32>,
    pub limit: Option<u32>,
    pub wait_ms: Option<u64>,
}

/// `GET /api/events/feed?after=<cursor|latest>|tail=<N>&limit=&wait_ms=` (long-poll ≤ 30 s).
///
/// Cluster: `$system.events` has one partition replicated to RF nodes, so a node may hold no
/// copy of it. Like message reads, the request is answered by the partition leader (proxied
/// over the internal API with the long-poll intact); cursors are message offsets, identical
/// on every replica. With no leader right now, the node answers from its own copy (empty
/// when it has none) rather than failing a poll loop.
pub async fn feed(
    State(s): State<AppState>,
    Query(q): Query<FeedQuery>,
) -> ApiResult<axum::Json<FeedPage>> {
    let (after, limit, wait) = parse_feed_query(&q)?;
    if s.cluster.is_clustered()
        && let fluxmq_cluster::PartitionRoute::Remote(leader) = s
            .cluster
            .route_partition(fluxmq_proto::notify::SYSTEM_EVENTS_CHANNEL, 0)
    {
        return proxy_feed(&s, &leader, q, wait).await;
    }
    local_feed(&s, after, limit, wait).await
}

fn parse_feed_query(q: &FeedQuery) -> ApiResult<(FeedAfter, u32, Duration)> {
    let after = FeedAfter::from_query(q.after.as_deref(), q.tail).map_err(notify_err)?;
    let limit = q.limit.unwrap_or(fluxmq_notify::feed::DEFAULT_LIMIT);
    if !(1..=fluxmq_notify::feed::MAX_LIMIT).contains(&limit) {
        return Err(ApiErr::bad_request("limit must be 1-1000"));
    }
    let wait_ms = q.wait_ms.unwrap_or(0);
    if wait_ms > fluxmq_notify::feed::MAX_WAIT.as_millis() as u64 {
        return Err(ApiErr::bad_request("wait_ms must be 0-30000"));
    }
    Ok((after, limit, Duration::from_millis(wait_ms)))
}

/// The feed from this node's replica of `$system.events`.
async fn local_feed(
    s: &AppState,
    after: FeedAfter,
    limit: u32,
    wait: Duration,
) -> ApiResult<axum::Json<FeedPage>> {
    // Graceful shutdown ends a pending long-poll right away (empty page).
    let shutdown = s.shutdown.clone();
    let page = tokio::select! {
        r = s.notify.feed(after, limit, wait) => r.map_err(notify_err)?,
        () = shutdown.cancelled() => s
            .notify
            .feed(after, limit, Duration::ZERO)
            .await
            .map_err(notify_err)?,
    };
    Ok(axum::Json(page))
}

/// Forwards a validated feed request to the partition leader. Shutdown cuts the long-poll
/// short by re-asking without waiting, so the returned cursor stays the leader's.
async fn proxy_feed(
    s: &AppState,
    leader: &fluxmq_proto::cluster::NodeInfo,
    q: FeedQuery,
    wait: Duration,
) -> ApiResult<axum::Json<FeedPage>> {
    let http = s.cluster.internal_http();
    let call = |q: FeedQuery, wait: Duration| {
        let http = http.clone().with_timeout(wait + FEED_PROXY_SLACK);
        let addr = leader.http_addr.clone();
        async move { http.get::<_, FeedPage>(&addr, INTERNAL_FEED_PATH, &q).await }
    };
    let shutdown = s.shutdown.clone();
    let result = tokio::select! {
        r = call(q.clone(), wait) => r,
        () = shutdown.cancelled() => {
            let q = FeedQuery { wait_ms: Some(0), ..q };
            call(q, Duration::ZERO).await
        }
    };
    match result {
        Ok(page) => Ok(axum::Json(page)),
        Err(fluxmq_cluster::TransportError::Status(400, body)) => Err(ApiErr::bad_request(body)),
        Err(e) => Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            format!(
                "events feed: partition leader {} unreachable: {e}; retry",
                leader.id
            ),
        )),
    }
}

/// Node-local feed read (proxy target). Never proxies again, so a leadership change between
/// the caller's routing decision and this call can't loop: the old leader still holds the
/// replica and answers from it.
async fn internal_feed(
    State(s): State<AppState>,
    Query(q): Query<FeedQuery>,
) -> ApiResult<axum::Json<FeedPage>> {
    let (after, limit, wait) = parse_feed_query(&q)?;
    local_feed(&s, after, limit, wait).await
}

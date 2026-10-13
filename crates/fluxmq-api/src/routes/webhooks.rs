//! HTTP push webhooks (v0.3): subscription CRUD, test delivery (stored or unsaved), and
//! cluster-wide stats. Secrets (`secret`, header values) are redacted by the service; audit
//! details never contain them. Permissions live in the central table (`auth.rs`).

use axum::extract::State;
use axum::http::StatusCode;
use fluxmq_proto::webhooks::{
    CreateWebhookRequest, UpdateWebhookRequest, WebhookAttempt, WebhookHistory, WebhookStats,
    WebhookSubscription, WebhookTestResult,
};
use fluxmq_webhooks::{
    INTERNAL_ATTEMPTS_PATH, INTERNAL_HISTORY_PATH, INTERNAL_STATS_PATH, NodeHistory,
    NodeWebhookStats, WebhookError, group_name,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Json, OptionalJson, Path, Query};
use crate::state::AppState;

/// WebhookError → HTTP: 404 not found, 400 invalid, 409 duplicate name / revision conflict.
pub fn webhook_err(e: WebhookError) -> ApiErr {
    match e {
        WebhookError::NotFound => ApiErr::not_found("webhook not found"),
        WebhookError::Invalid(m) => ApiErr::bad_request(m),
        WebhookError::ChannelNotFound(c) => ApiErr::not_found(format!("channel '{c}' not found")),
        WebhookError::Conflict(m) => ApiErr::conflict(m),
        WebhookError::Meta(fluxmq_meta::MetaError::Conflict { .. }) => {
            ApiErr::conflict("webhook was modified concurrently; reload and retry")
        }
        WebhookError::Meta(fluxmq_meta::MetaError::Unavailable(m)) => ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal",
            format!("metadata store unavailable: {m}"),
        ),
        WebhookError::Meta(e) => {
            tracing::error!(error = %e, "webhook metadata error");
            ApiErr::internal(e.to_string())
        }
    }
}

/// Audit detail of a request: secrets replaced by the redaction marker.
fn redacted_detail<T: serde::Serialize>(v: &T) -> serde_json::Value {
    let mut d = serde_json::to_value(v).unwrap_or_default();
    if let Some(o) = d.as_object_mut() {
        if o.get("secret").is_some_and(|s| !s.is_null()) {
            o.insert("secret".into(), json!(fluxmq_proto::notify::REDACTED));
        }
        if let Some(h) = o.get_mut("headers").and_then(|h| h.as_object_mut()) {
            for v in h.values_mut() {
                *v = json!(fluxmq_proto::notify::REDACTED);
            }
        }
    }
    d
}

pub async fn list(State(s): State<AppState>) -> axum::Json<Vec<WebhookSubscription>> {
    axum::Json(s.webhooks.list())
}

pub async fn get(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<WebhookSubscription>> {
    s.webhooks
        .get(&id)
        .map(axum::Json)
        .ok_or_else(|| ApiErr::not_found(format!("webhook '{id}' not found")))
}

pub async fn create(
    State(s): State<AppState>,
    Json(req): Json<CreateWebhookRequest>,
) -> ApiResult<(StatusCode, axum::Json<WebhookSubscription>)> {
    let sub = s.webhooks.create(req).await.map_err(webhook_err)?;
    s.audit("webhook.create", &sub.id, redacted_detail(&sub));
    Ok((StatusCode::CREATED, axum::Json(sub)))
}

pub async fn update(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateWebhookRequest>,
) -> ApiResult<axum::Json<WebhookSubscription>> {
    let detail = redacted_detail(&req);
    let sub = s.webhooks.update(&id, req).await.map_err(webhook_err)?;
    s.audit("webhook.update", &id, detail);
    Ok(axum::Json(sub))
}

pub async fn delete(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult<StatusCode> {
    let sub = s.webhooks.delete(&id).await.map_err(webhook_err)?;
    s.audit(
        "webhook.delete",
        &id,
        json!({ "name": sub.name, "channel": sub.channel }),
    );
    tokio::spawn(release_group(s.clone(), sub));
    Ok(StatusCode::NO_CONTENT)
}

/// The subscription is gone: once its worker (on every node) has let go of `$webhook:<name>`
/// (in-flight requests finish first, bounded by the request timeout), delete the group so it
/// doesn't linger with growing lag. Gives up if a subscription with that name reappears.
async fn release_group(s: AppState, sub: WebhookSubscription) {
    const EVERY: std::time::Duration = std::time::Duration::from_millis(250);
    let group = group_name(&sub.name);
    let attempts = (u64::from(sub.timeout_ms) + 10_000) / EVERY.as_millis() as u64;
    for attempt in 1..=attempts {
        if s.webhooks.uses_group(&sub.channel, &group) || s.broker.channel(&sub.channel).is_none() {
            return;
        }
        match super::channels::delete_group_everywhere(&s, &sub.channel, &group).await {
            Ok(()) => {
                s.audit(
                    "group.delete",
                    &format!("{}/{group}", sub.channel),
                    json!({ "reason": "webhook deleted" }),
                );
                return;
            }
            Err(e) if e.status == StatusCode::NOT_FOUND => return,
            Err(e)
                if attempt < attempts
                    && (e.status == StatusCode::CONFLICT
                        || e.status == StatusCode::SERVICE_UNAVAILABLE) =>
            {
                tokio::time::sleep(EVERY).await;
            }
            Err(e) => {
                tracing::warn!(channel = %sub.channel, %group, error = %e.body.message, "could not delete the webhook group");
                return;
            }
        }
    }
}

/// `POST /api/webhooks/{id}/test`: one synthetic envelope to the stored subscription, or to
/// the stored subscription with an optional draft ([`UpdateWebhookRequest`]) merged over it
/// (not persisted).
pub async fn test(
    State(s): State<AppState>,
    Path(id): Path<String>,
    OptionalJson(draft): OptionalJson<UpdateWebhookRequest>,
) -> ApiResult<axum::Json<WebhookTestResult>> {
    let edited = draft.is_some();
    let r = s.webhooks.test(&id, draft).await.map_err(webhook_err)?;
    s.audit(
        "webhook.test",
        &id,
        json!({ "ok": r.ok, "status": r.status, "draft": edited }),
    );
    Ok(axum::Json(r))
}

/// `POST /api/webhooks/test`: test before saving (validated like a create, not persisted).
pub async fn test_unsaved(
    State(s): State<AppState>,
    Json(req): Json<CreateWebhookRequest>,
) -> ApiResult<axum::Json<WebhookTestResult>> {
    let name = req.name.clone();
    let r = s.webhooks.test_request(req).await.map_err(webhook_err)?;
    s.audit(
        "webhook.test",
        &name,
        json!({ "ok": r.ok, "status": r.status, "unsaved": true }),
    );
    Ok(axum::Json(r))
}

/// `GET /api/webhooks/stats`: summed over all nodes when clustered (a node that can't be
/// reached is skipped — stats are best-effort), node-local otherwise.
pub async fn stats(State(s): State<AppState>) -> axum::Json<Vec<WebhookStats>> {
    if !s.cluster.is_clustered() {
        return axum::Json(s.webhooks.stats());
    }
    let mut all = s.webhooks.local_stats();
    for remote in from_peers::<_, Vec<NodeWebhookStats>>(&s, INTERNAL_STATS_PATH, &()).await {
        all.extend(remote);
    }
    axum::Json(s.webhooks.merge_stats(all))
}

/// Collects one node-local value from every peer (best-effort: unreachable nodes are skipped).
pub(crate) async fn from_peers<Q: Serialize + Sync, R: serde::de::DeserializeOwned>(
    s: &AppState,
    path: &str,
    query: &Q,
) -> Vec<R> {
    let http = s.cluster.internal_http();
    let calls = s.cluster.peers().into_iter().map(|n| {
        let http = http.clone();
        async move {
            http.get::<_, R>(&n.http_addr, path, query)
                .await
                .map_err(
                    |e| tracing::debug!(node = %n.id, path, error = %e, "cluster fan-out failed"),
                )
                .ok()
        }
    });
    futures::future::join_all(calls)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// `GET /api/webhooks/{id}/attempts`: the last requests (newest first, up to 100), over all
/// nodes when clustered. Node-local memory: empty after a restart.
pub async fn attempts(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<Vec<WebhookAttempt>>> {
    let local = s
        .webhooks
        .local_attempts(&id)
        .ok_or_else(|| ApiErr::not_found(format!("webhook '{id}' not found")))?;
    let mut all = vec![local];
    if s.cluster.is_clustered() {
        let path = INTERNAL_ATTEMPTS_PATH.replace("{id}", &id);
        all.extend(from_peers::<_, Vec<WebhookAttempt>>(&s, &path, &()).await);
    }
    Ok(axum::Json(fluxmq_webhooks::stats::merge_attempts(all)))
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HistoryQuery {
    #[serde(default = "default_range")]
    pub range: String,
}

fn default_range() -> String {
    "1h".into()
}

/// `GET /api/webhooks/{id}/history?range=15m|1h|24h`: delivered / failed messages and response
/// latency per bucket, summed over all nodes when clustered.
pub async fn history(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<axum::Json<WebhookHistory>> {
    s.webhooks
        .get(&id)
        .ok_or_else(|| ApiErr::not_found(format!("webhook '{id}' not found")))?;
    let local = s.webhooks.local_history(&id, &q.range).ok_or_else(|| {
        ApiErr::bad_request(format!("unknown range '{}' (15m, 1h, 24h)", q.range))
    })?;
    let mut all = vec![local];
    if s.cluster.is_clustered() {
        let path = INTERNAL_HISTORY_PATH.replace("{id}", &id);
        all.extend(from_peers::<_, NodeHistory>(&s, &path, &q).await);
    }
    fluxmq_webhooks::stats::merge_history(&q.range, fluxmq_proto::now_ms(), all)
        .map(axum::Json)
        .ok_or_else(|| ApiErr::bad_request("unknown range"))
}

async fn internal_attempts(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> axum::Json<Vec<WebhookAttempt>> {
    axum::Json(s.webhooks.local_attempts(&id).unwrap_or_default())
}

async fn internal_history(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<axum::Json<NodeHistory>> {
    s.webhooks
        .local_history(&id, &q.range)
        .map(axum::Json)
        .ok_or_else(|| ApiErr::not_found("unknown webhook or range"))
}

/// `GET /internal/v1/webhooks/stats` (cluster secret): this node's mergeable stats.
async fn internal_stats(State(s): State<AppState>) -> axum::Json<Vec<NodeWebhookStats>> {
    axum::Json(s.webhooks.local_stats())
}

/// Node-to-node routes, merged into `cluster::internal`.
pub fn internal() -> axum::Router<AppState> {
    axum::Router::new()
        .route(INTERNAL_STATS_PATH, axum::routing::get(internal_stats))
        .route(
            INTERNAL_ATTEMPTS_PATH,
            axum::routing::get(internal_attempts),
        )
        .route(INTERNAL_HISTORY_PATH, axum::routing::get(internal_history))
}

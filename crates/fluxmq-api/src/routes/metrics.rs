//! `GET /metrics` (Prometheus text exposition, node-local, RBAC `Read`) and
//! `GET /internal/v1/node-stats` (compact node snapshot pulled by cluster peers, cluster-secret
//! auth).

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use fluxmq_telemetry::prometheus::{CONTENT_TYPE, Writer};

use crate::auth::constant_time_eq;
use crate::error::ApiErr;
use crate::state::AppState;

/// `GET /metrics`.
pub async fn metrics(State(s): State<AppState>) -> Response {
    let mut body = s.telemetry.prometheus();
    body.push_str(&router_metrics(&s));
    body.push_str(&super::tls::metrics(&s));
    body.push_str(&s.live.prometheus(s.broker.node_id()));
    ([(header::CONTENT_TYPE, CONTENT_TYPE)], body).into_response()
}

/// Content-based routing totals (no per-route labels: bounded cardinality).
fn router_metrics(s: &AppState) -> String {
    let stats = s.router.stats();
    let mut w = Writer::default();
    let node = s.broker.node_id();
    let l = [("node", node)];
    w.family("fluxmq_router_routes", "gauge", "Configured routes.");
    w.sample("fluxmq_router_routes", &l, s.router.routes().len() as f64);
    let fams: [(&str, &str, u64); 4] = [
        (
            "fluxmq_router_evaluated_total",
            "Messages evaluated against route filters.",
            stats.iter().map(|r| r.evaluated).sum(),
        ),
        (
            "fluxmq_router_matched_total",
            "Messages that matched a route filter.",
            stats.iter().map(|r| r.matched).sum(),
        ),
        (
            "fluxmq_router_forwarded_total",
            "Messages forwarded to a route's target channel.",
            stats.iter().map(|r| r.forwarded).sum(),
        ),
        (
            "fluxmq_router_failed_total",
            "Route forwards that failed.",
            stats.iter().map(|r| r.failed).sum(),
        ),
    ];
    for (name, help, v) in fams {
        w.family(name, "counter", help);
        w.sample(name, &l, v as f64);
    }
    w.finish()
}

/// Internal (node-to-node) routes owned by telemetry. Mounted outside the RBAC layer.
pub fn internal() -> Router<AppState> {
    Router::new().route(
        fluxmq_telemetry::cluster_source::NODE_STATS_PATH,
        get(node_stats),
    )
}

/// `GET /internal/v1/node-stats` — `application/x-fluxmq-node-stats` (postcard).
///
/// 404 on a single node. When clustered, the `x-fluxmq-cluster-secret` header must match
/// `--cluster-secret` (checked here until the cluster engineer's `/internal/*` middleware
/// covers this route; then the check is redundant but harmless).
pub async fn node_stats(State(s): State<AppState>, headers: HeaderMap) -> Response {
    if !s.telemetry.is_clustered() {
        return ApiErr::not_found("not clustered").into_response();
    }
    if let Some(secret) = s.cluster.cluster_secret() {
        let given = headers
            .get(fluxmq_cluster::CLUSTER_SECRET_HEADER)
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        if !constant_time_eq(given, secret.as_bytes()) {
            return ApiErr::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid cluster secret",
            )
            .into_response();
        }
    }
    match s.telemetry.node_stats() {
        Some(bytes) => (
            [(
                header::CONTENT_TYPE,
                fluxmq_telemetry::snapshot::CONTENT_TYPE,
            )],
            bytes.as_ref().clone(),
        )
            .into_response(),
        None => ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "telemetry not sampled yet",
        )
        .into_response(),
    }
}

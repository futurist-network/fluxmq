//! REST handlers, one module per resource.

use axum::Router;
use axum::routing::{get, post};

use crate::error::ApiErr;
use crate::state::AppState;

pub mod audit;
pub mod auth;
pub mod backups;
pub mod channels;
pub mod cluster;
pub mod dlq;
pub mod history;
pub mod live_status;
pub mod messages;
pub mod metrics;
pub mod notify;
pub mod routing;
pub mod scheduled;
pub mod search;
pub mod tls;
pub mod webhooks;

/// Every authenticated `/api/*` route (health is mounted separately, unauthenticated).
pub fn api() -> Router<AppState> {
    Router::new()
        .route("/api/channels", get(channels::list).post(channels::create))
        .route(
            "/api/channels/{name}",
            get(channels::get)
                .patch(channels::update)
                .delete(channels::delete),
        )
        .route("/api/channels/{name}/publish", post(channels::publish))
        .route("/api/channels/{name}/messages", get(messages::query))
        .route(
            "/api/channels/{name}/messages/{partition}/{offset}",
            get(messages::detail),
        )
        .route("/api/channels/{name}/replay", post(channels::replay))
        .route(
            "/api/channels/{name}/groups/{group}/reset",
            post(channels::reset_group),
        )
        .route(
            "/api/channels/{name}/groups/{group}",
            axum::routing::delete(channels::delete_group),
        )
        .route("/api/dlq/summary", get(dlq::summary))
        .route("/api/dlq", get(dlq::list))
        .route("/api/dlq/retry", post(dlq::retry))
        .route("/api/dlq/discard", post(dlq::discard))
        .route("/api/dlq/export", get(dlq::export))
        .route("/api/history", get(history::history))
        .route("/api/alerts", get(history::alerts))
        .route("/api/tls", get(tls::status))
        .route("/api/search", get(search::search))
        .route("/api/audit", get(audit::list))
        .route("/api/auth/me", get(auth::me))
        .route("/api/auth/tokens", get(auth::list).post(auth::create))
        .route("/api/auth/tokens/{id}", axum::routing::delete(auth::revoke))
        .route(
            "/api/auth/cert-bindings",
            get(auth::list_cert_bindings).post(auth::create_cert_binding),
        )
        .route(
            "/api/auth/cert-bindings/{cn}",
            axum::routing::delete(auth::delete_cert_binding),
        )
        // Content-based routing (stage 8).
        .route("/api/routes", get(routing::list).post(routing::create))
        .route("/api/routes/test", post(routing::test))
        .route("/api/routes/stats", get(routing::stats))
        .route(
            "/api/routes/{id}",
            get(routing::get)
                .patch(routing::update)
                .delete(routing::delete),
        )
        .route("/api/filter/parse", post(routing::parse_filter))
        .route("/api/filter/render", post(routing::render_filter))
        // HTTP push webhooks (v0.3).
        .route("/api/webhooks", get(webhooks::list).post(webhooks::create))
        .route("/api/webhooks/test", post(webhooks::test_unsaved))
        .route("/api/webhooks/stats", get(webhooks::stats))
        .route(
            "/api/webhooks/{id}",
            get(webhooks::get)
                .patch(webhooks::update)
                .delete(webhooks::delete),
        )
        .route("/api/webhooks/{id}/test", post(webhooks::test))
        .route("/api/webhooks/{id}/attempts", get(webhooks::attempts))
        .route("/api/live/status", get(live_status::status))
        .route("/api/scheduled", get(scheduled::list))
        .route(
            "/api/scheduled/{partition}/{offset}",
            axum::routing::delete(scheduled::cancel),
        )
        .route(
            "/api/scheduled/{partition}/{offset}/release",
            axum::routing::post(scheduled::release),
        )
        .route("/api/webhooks/{id}/history", get(webhooks::history))
        // Backups (v0.3).
        .route(
            "/api/backups/config",
            get(backups::get_config).put(backups::put_config),
        )
        .route("/api/backups/status", get(backups::status))
        .route("/api/backups/run", post(backups::run))
        .route("/api/backups/test-target", post(backups::test_target))
        .route("/api/backups", get(backups::list))
        .route(
            "/api/backups/{id}",
            get(backups::get).delete(backups::delete),
        )
        // Notifications + system events feed (v0.3).
        .route(
            "/api/notify/notifiers",
            get(notify::list_notifiers).post(notify::create_notifier),
        )
        .route("/api/notify/notifiers/test", post(notify::test_unsaved))
        .route(
            "/api/notify/notifiers/{id}",
            get(notify::get_notifier)
                .patch(notify::update_notifier)
                .delete(notify::delete_notifier),
        )
        .route(
            "/api/notify/notifiers/{id}/test",
            post(notify::test_notifier),
        )
        .route(
            "/api/notify/rules",
            get(notify::list_rules).post(notify::create_rule),
        )
        .route(
            "/api/notify/rules/{id}",
            get(notify::get_rule)
                .patch(notify::update_rule)
                .delete(notify::delete_rule),
        )
        .route("/api/notify/history", get(notify::history))
        .route("/api/events/feed", get(notify::feed))
        // Cluster (stage 9).
        .route("/api/cluster/metadata", get(cluster::metadata))
        .route("/api/cluster/status", get(cluster::status))
        .route("/api/cluster/partitions/reassign", post(cluster::reassign))
        .route("/api/cluster/nodes/{id}/drain", post(cluster::drain))
        // Node decommission / forget (v0.5).
        .route(
            "/api/cluster/nodes/{id}/decommission",
            get(cluster::decommission_status).post(cluster::decommission),
        )
        .route(
            "/api/cluster/nodes/{id}",
            axum::routing::delete(cluster::forget_node),
        )
        .route("/api/cluster/decommissions", get(cluster::decommissions))
        // Automatic rebalancing (v0.4).
        .route(
            "/api/cluster/rebalance",
            get(cluster::rebalance_status).post(cluster::rebalance),
        )
        .route(
            "/api/cluster/rebalance/config",
            axum::routing::put(cluster::rebalance_config),
        )
        .route(
            "/api/cluster/rebalance/pause",
            post(cluster::rebalance_pause),
        )
        .route(
            "/api/cluster/rebalance/resume",
            post(cluster::rebalance_resume),
        )
        .route(
            "/api/cluster/rebalance/cancel",
            post(cluster::rebalance_cancel),
        )
        // Prometheus exposition (node-local).
        .route("/metrics", get(metrics::metrics))
        // Unknown API paths are JSON 404s, never the SPA's index.html.
        .route("/api", axum::routing::any(api_not_found))
        .route("/api/{*rest}", axum::routing::any(api_not_found))
}

async fn api_not_found(uri: axum::http::Uri) -> ApiErr {
    ApiErr::not_found(format!("no route for {}", uri.path()))
}

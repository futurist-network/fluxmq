//! FluxMQ HTTP surface: REST API (`/api/*`), WebSocket telemetry gateway (`/ws`), live
//! subscriptions (`/api/channels/{name}/live` WS, `/api/channels/{name}/events` SSE; v0.10) and
//! the bundled web UI (everything else). See `docs/ARCHITECTURE.md` for the route table.
//!
//! ```ignore
//! let state = AppState::new(broker, telemetry).with_admin_token(token).with_ui(UiSource::Dir(dir));
//! let shutdown = state.shutdown.clone();
//! axum::serve(listener, fluxmq_api::router(state))
//!     .with_graceful_shutdown(async move { shutdown.cancelled().await })
//!     .await?;
//! ```

pub mod audit;
pub mod auth;
pub mod error;
pub mod extract;
pub mod live;
pub mod payload;
#[cfg(feature = "profiling")]
pub mod profile;
pub mod routes;
pub mod state;
pub mod static_ui;
pub mod ws;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub use audit::AuditLog;
pub use error::{ApiErr, ApiResult};
pub use live::{LiveConfig, LiveService};
pub use state::{AppState, WsLimits};
pub use static_ui::UiSource;

/// Builds the complete HTTP application.
pub fn router(state: AppState) -> Router {
    let protected = routes::api().route("/ws", get(ws::upgrade));
    #[cfg(feature = "profiling")]
    let protected = protected.route("/api/debug/pprof/profile", get(profile::profile));
    let protected = protected.route_layer(axum::middleware::from_fn_with_state(
        state.clone(),
        auth::require_auth,
    ));

    let app = Router::new()
        .route("/api/health", get(routes::audit::health))
        // Live subscriptions (v0.10) authenticate and authorize themselves: the token may
        // arrive in a WS subprotocol or (SSE) the query string, and they need
        // `Subscribe(channel)`, which `subscriber` tokens hold without any REST right.
        .route("/api/channels/{name}/live", get(live::handler::ws))
        .route("/api/channels/{name}/events", get(live::handler::sse))
        // Node-to-node `/internal/v1/*` (incl. telemetry's node-stats): cluster-secret auth.
        .merge(routes::cluster::internal(state.clone()))
        .merge(protected)
        .layer(DefaultBodyLimit::max(state.max_body_bytes));
    let app = static_ui::attach(app, &state.ui);

    // Spans carry the path only: the query string may hold a secret (`/ws?token=...`).
    let app = app.layer(TraceLayer::new_for_http().make_span_with(
        |req: &axum::http::Request<axum::body::Body>| {
            tracing::debug_span!(
                "request",
                method = %req.method(),
                path = %req.uri().path(),
                version = ?req.version(),
            )
        },
    ));
    // Permissive CORS only in open (dev) mode: lets the Vite dev server on another port talk
    // to the broker. Evaluated per request, so creating the first token (which enforces auth)
    // turns it off without a restart; with auth enforced the UI must be served same-origin.
    let auth_for_cors = state.auth.clone();
    let app = app.layer(CorsLayer::permissive().allow_origin(
        tower_http::cors::AllowOrigin::predicate(move |_, _| {
            auth_for_cors.mode() == fluxmq_proto::auth::AuthMode::Open
        }),
    ));
    app.with_state(state)
}

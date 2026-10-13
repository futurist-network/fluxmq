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
    // Open (dev) mode: every request is an admin, so browsers on other sites must not reach
    // the API. Cross-origin requests are refused unless they come from a loopback origin (the
    // Vite dev server on another port), which also gets permissive CORS. Evaluated per
    // request, so creating the first token (which enforces auth) turns both off without a
    // restart; with auth enforced the UI must be served same-origin.
    let auth_for_cors = state.auth.clone();
    let app = app.layer(CorsLayer::permissive().allow_origin(
        tower_http::cors::AllowOrigin::predicate(move |origin, _| {
            auth_for_cors.mode() == fluxmq_proto::auth::AuthMode::Open
                && origin.to_str().is_ok_and(is_loopback_origin)
        }),
    ));
    let app = app.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        open_mode_origin_guard,
    ));
    app.with_state(state)
}

/// In open mode, 403 for browser requests from a foreign origin: not same-origin (`Origin`
/// host:port = `Host`) and not loopback. Requests without `Origin` (curl, SDKs) pass.
async fn open_mode_origin_guard(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::header::{HOST, ORIGIN};
    use axum::response::IntoResponse;
    if state.auth.mode() == fluxmq_proto::auth::AuthMode::Open
        && let Some(origin) = req.headers().get(ORIGIN)
    {
        let origin = origin.to_str().unwrap_or("");
        let host = req.headers().get(HOST).and_then(|v| v.to_str().ok());
        let same_origin = host.is_some_and(|h| origin_authority(origin).eq_ignore_ascii_case(h));
        if !same_origin && !is_loopback_origin(origin) {
            return ApiErr::new(
                axum::http::StatusCode::FORBIDDEN,
                "forbidden",
                "cross-origin request refused while auth is open",
            )
            .into_response();
        }
    }
    next.run(req).await
}

/// `host[:port]` of an `http(s)://` origin, `""` otherwise.
fn origin_authority(origin: &str) -> &str {
    origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
        .unwrap_or("")
}

/// `http(s)://localhost`, `127.x.x.x` or `[::1]`, any port.
fn is_loopback_origin(origin: &str) -> bool {
    let authority = origin_authority(origin);
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => authority.split(':').next().unwrap_or(""),
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_origins() {
        for o in [
            "http://localhost:5173",
            "http://LOCALHOST",
            "https://127.0.0.1:8443",
            "http://127.1.2.3",
            "http://[::1]:5173",
        ] {
            assert!(is_loopback_origin(o), "{o}");
        }
        for o in [
            "https://evil.example",
            "http://localhost.evil.example",
            "http://10.0.0.1:7421",
            "null",
            "file://",
            "",
        ] {
            assert!(!is_loopback_origin(o), "{o}");
        }
    }
}

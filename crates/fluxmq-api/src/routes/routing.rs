//! Content-based routing (stage 8): route CRUD, backtest, stats and the filter DSL parser
//! used by the UI editor. Permissions live in the central table (`auth.rs`).

use axum::extract::State;
use axum::http::StatusCode;
use fluxmq_proto::routing::{
    CreateRouteRequest, ParseFilterRequest, ParseFilterResponse, RenderFilterRequest,
    RenderFilterResponse, Route, RouteStats, RouteTestRequest, RouteTestResponse,
    UpdateRouteRequest,
};
use fluxmq_router::{RouterError, filter};
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Json, Path};
use crate::state::AppState;

/// RouterError → HTTP: 404 not found, 400 invalid, 409 meta revision conflict.
pub fn router_err(e: RouterError) -> ApiErr {
    match e {
        RouterError::NotFound => ApiErr::not_found("route not found"),
        RouterError::Invalid(m) => ApiErr::bad_request(m),
        RouterError::ChannelNotFound(c) => ApiErr::not_found(format!("channel '{c}' not found")),
        RouterError::Meta(fluxmq_meta::MetaError::Conflict { .. }) => {
            ApiErr::conflict("route was modified concurrently; reload and retry")
        }
        RouterError::Meta(fluxmq_meta::MetaError::Unavailable(m)) => ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal",
            format!("metadata store unavailable: {m}"),
        ),
        RouterError::Meta(e) => {
            tracing::error!(error = %e, "route metadata error");
            ApiErr::internal(e.to_string())
        }
        RouterError::Core(e) => ApiErr::from(e),
    }
}

pub async fn list(State(s): State<AppState>) -> axum::Json<Vec<Route>> {
    axum::Json(s.router.routes())
}

pub async fn get(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<Route>> {
    s.router
        .route(&id)
        .map(axum::Json)
        .ok_or_else(|| ApiErr::not_found(format!("route '{id}' not found")))
}

pub async fn create(
    State(s): State<AppState>,
    Json(req): Json<CreateRouteRequest>,
) -> ApiResult<(StatusCode, axum::Json<Route>)> {
    let route = s.router.create(req).await.map_err(router_err)?;
    s.audit(
        "route.create",
        &route.id,
        serde_json::to_value(&route).unwrap_or_default(),
    );
    Ok((StatusCode::CREATED, axum::Json(route)))
}

pub async fn update(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateRouteRequest>,
) -> ApiResult<axum::Json<Route>> {
    let detail = serde_json::to_value(&req).unwrap_or_default();
    let route = s.router.update(&id, req).await.map_err(router_err)?;
    s.audit("route.update", &id, detail);
    Ok(axum::Json(route))
}

pub async fn delete(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult<StatusCode> {
    let source = s.router.route(&id).map(|r| r.source);
    s.router.delete(&id).await.map_err(router_err)?;
    s.audit("route.delete", &id, json!({}));
    if let Some(source) = source
        && !s.router.uses_source(&source)
    {
        tokio::spawn(release_router_group(s.clone(), source));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// The last route of `source` is gone: once the router workers (on every node) have let go
/// of the `$router` group, delete it so it doesn't linger with growing lag. Retries while
/// workers are still stopping or leadership moves; gives up if a new route claims the source.
async fn release_router_group(s: AppState, source: String) {
    const ATTEMPTS: u32 = 40;
    const EVERY: std::time::Duration = std::time::Duration::from_millis(250);
    let group = fluxmq_proto::ROUTER_GROUP;
    for attempt in 1..=ATTEMPTS {
        if s.router.uses_source(&source) || s.broker.channel(&source).is_none() {
            return;
        }
        match super::channels::delete_group_everywhere(&s, &source, group).await {
            Ok(()) => {
                s.audit(
                    "group.delete",
                    &format!("{source}/{group}"),
                    json!({ "reason": "last route deleted" }),
                );
                return;
            }
            Err(e) if e.status == StatusCode::NOT_FOUND => return,
            Err(e)
                if attempt < ATTEMPTS
                    && (e.status == StatusCode::CONFLICT
                        || e.status == StatusCode::SERVICE_UNAVAILABLE) =>
            {
                tokio::time::sleep(EVERY).await;
            }
            Err(e) => {
                tracing::warn!(%source, error = %e.body.message, "could not delete the $router group");
                return;
            }
        }
    }
}

/// Backtest: scans stored messages (blocking) on the blocking pool.
pub async fn test(
    State(s): State<AppState>,
    Json(req): Json<RouteTestRequest>,
) -> ApiResult<axum::Json<RouteTestResponse>> {
    let router = s.router.clone();
    let r = tokio::task::spawn_blocking(move || router.test(&req))
        .await?
        .map_err(router_err)?;
    Ok(axum::Json(r))
}

pub async fn stats(State(s): State<AppState>) -> axum::Json<Vec<RouteStats>> {
    axum::Json(s.router.stats())
}

/// DSL → structured conditions + canonical text; 400 with the byte position on a syntax error.
pub async fn parse_filter(
    Json(req): Json<ParseFilterRequest>,
) -> ApiResult<axum::Json<ParseFilterResponse>> {
    let conditions = filter::parse(&req.filter).map_err(|e| ApiErr::filter_syntax(&e))?;
    let rendered = filter::render(&conditions);
    Ok(axum::Json(ParseFilterResponse {
        conditions,
        rendered,
    }))
}

/// Structured conditions → canonical DSL text (the editor's builder → text direction).
/// Conditions are validated like a route's (400 names the offending condition).
pub async fn render_filter(
    Json(req): Json<RenderFilterRequest>,
) -> ApiResult<axum::Json<RenderFilterResponse>> {
    let conditions = fluxmq_router::validate_conditions(req.conditions).map_err(router_err)?;
    Ok(axum::Json(RenderFilterResponse {
        rendered: filter::render(&conditions),
    }))
}

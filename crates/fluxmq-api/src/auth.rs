//! RBAC middleware for the HTTP surface: authentication, the central permission table, and
//! the request-scoped [`Principal`].
//!
//! Every route behind [`require_auth`] is mapped `(method, matched route pattern) → rule` by
//! [`PERMISSIONS`] — the single place that decides who may call what. Rules:
//!
//! * unknown route: `GET`/`HEAD` → `Read`, anything else → `Admin` (deny by default);
//! * `/internal/*` is never authorized here (cluster-secret auth, mounted outside this layer);
//! * `/api/health` is mounted outside this layer and is always open.
//!
//! The authenticated [`Principal`] is inserted into request extensions and made available to
//! the handler's task via [`current_principal`] so `AppState::audit` records the real actor.
//! 401 = missing/invalid/expired token, 403 = authenticated but not allowed
//! (`ApiError { error: "forbidden" }`).

use axum::extract::{MatchedPath, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use fluxmq_auth::{Action, AuthError, Principal};

use crate::error::ApiErr;
use crate::state::AppState;

tokio::task_local! {
    static PRINCIPAL: Principal;
}

/// The principal of the HTTP request being handled on this task, if any.
pub fn current_principal() -> Option<Principal> {
    PRINCIPAL.try_with(Clone::clone).ok()
}

/// Runs `fut` with `principal` as the [`current_principal`] (tests, background jobs acting on
/// behalf of a user).
pub async fn with_principal<F: std::future::Future>(principal: Principal, fut: F) -> F::Output {
    PRINCIPAL.scope(principal, fut).await
}

/// Role-level requirement of a route (data-plane scopes don't apply to REST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    Read,
    Operate,
    Admin,
}

impl Need {
    pub fn action(self) -> Action<'static> {
        match self {
            Need::Read => Action::Read,
            Need::Operate => Action::Operate,
            Need::Admin => Action::Admin,
        }
    }
}

/// What the middleware does with a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// No authentication (`/api/health`).
    Open,
    /// Authorized elsewhere (`/internal/*`: cluster secret) — passed through untouched.
    External,
    Require(Need),
}

/// The permission table: `(method, axum route pattern, requirement)`. `GET` entries also
/// cover `HEAD`. Keep in sync with `routes::api()` and the table in `docs/ARCHITECTURE.md`.
pub const PERMISSIONS: &[(&str, &str, Need)] = &[
    // Channels.
    ("GET", "/api/channels", Need::Read),
    ("POST", "/api/channels", Need::Admin),
    ("GET", "/api/channels/{name}", Need::Read),
    ("PATCH", "/api/channels/{name}", Need::Admin),
    ("DELETE", "/api/channels/{name}", Need::Admin),
    ("POST", "/api/channels/{name}/publish", Need::Operate),
    ("GET", "/api/channels/{name}/messages", Need::Read),
    (
        "GET",
        "/api/channels/{name}/messages/{partition}/{offset}",
        Need::Read,
    ),
    ("POST", "/api/channels/{name}/replay", Need::Operate),
    (
        "POST",
        "/api/channels/{name}/groups/{group}/reset",
        Need::Operate,
    ),
    ("DELETE", "/api/channels/{name}/groups/{group}", Need::Admin),
    // DLQ (export dumps payloads in bulk: operator).
    ("GET", "/api/dlq/summary", Need::Read),
    ("GET", "/api/dlq", Need::Read),
    ("POST", "/api/dlq/retry", Need::Operate),
    ("POST", "/api/dlq/discard", Need::Operate),
    ("GET", "/api/dlq/export", Need::Operate),
    // Observability.
    ("GET", "/api/history", Need::Read),
    ("GET", "/api/alerts", Need::Read),
    // TLS status (v0.4): certificate subjects / expiry / failures, no secrets.
    ("GET", "/api/tls", Need::Read),
    ("GET", "/api/search", Need::Read),
    ("GET", "/api/audit", Need::Read),
    ("GET", "/ws", Need::Read),
    ("GET", "/metrics", Need::Read),
    // RBAC.
    ("GET", "/api/auth/me", Need::Read),
    ("GET", "/api/auth/tokens", Need::Admin),
    ("POST", "/api/auth/tokens", Need::Admin),
    ("DELETE", "/api/auth/tokens/{id}", Need::Admin),
    // Certificate bindings (v0.5): mTLS CN → principal. Listing them is admin too.
    ("GET", "/api/auth/cert-bindings", Need::Admin),
    ("POST", "/api/auth/cert-bindings", Need::Admin),
    ("DELETE", "/api/auth/cert-bindings/{cn}", Need::Admin),
    // Routing (stage 8). Backtest and filter parsing are read-only.
    ("GET", "/api/routes", Need::Read),
    ("POST", "/api/routes", Need::Admin),
    ("GET", "/api/routes/stats", Need::Read),
    ("POST", "/api/routes/test", Need::Read),
    ("GET", "/api/routes/{id}", Need::Read),
    ("PATCH", "/api/routes/{id}", Need::Admin),
    ("DELETE", "/api/routes/{id}", Need::Admin),
    ("POST", "/api/filter/parse", Need::Read),
    ("POST", "/api/filter/render", Need::Read),
    // HTTP push webhooks (v0.3). Test deliveries make outbound requests: admin.
    ("GET", "/api/webhooks", Need::Read),
    ("POST", "/api/webhooks", Need::Admin),
    ("POST", "/api/webhooks/test", Need::Admin),
    ("GET", "/api/webhooks/stats", Need::Read),
    ("GET", "/api/webhooks/{id}", Need::Read),
    ("PATCH", "/api/webhooks/{id}", Need::Admin),
    ("DELETE", "/api/webhooks/{id}", Need::Admin),
    ("POST", "/api/webhooks/{id}/test", Need::Admin),
    ("GET", "/api/webhooks/{id}/attempts", Need::Read),
    ("GET", "/api/webhooks/{id}/history", Need::Read),
    // Live subscription gateway status (v0.11): limits and counts, nothing secret.
    ("GET", "/api/live/status", Need::Read),
    // Delayed messages (v0.12): list for viewers; cancel / release now like other
    // message operations (operator).
    ("GET", "/api/scheduled", Need::Read),
    (
        "DELETE",
        "/api/scheduled/{partition}/{offset}",
        Need::Operate,
    ),
    (
        "POST",
        "/api/scheduled/{partition}/{offset}/release",
        Need::Operate,
    ),
    // Backups (v0.3). Reads redact secrets; anything that writes or probes a target: admin.
    ("GET", "/api/backups/config", Need::Read),
    ("PUT", "/api/backups/config", Need::Admin),
    ("GET", "/api/backups/status", Need::Read),
    ("POST", "/api/backups/run", Need::Admin),
    ("POST", "/api/backups/test-target", Need::Admin),
    ("GET", "/api/backups", Need::Read),
    ("GET", "/api/backups/{id}", Need::Read),
    ("DELETE", "/api/backups/{id}", Need::Admin),
    // Notifications (v0.3): reads redact secrets; mutations and test sends: admin. The
    // events feed is readable by any token (Read covers consume-scoped client tokens).
    ("GET", "/api/notify/notifiers", Need::Read),
    ("POST", "/api/notify/notifiers", Need::Admin),
    ("POST", "/api/notify/notifiers/test", Need::Admin),
    ("GET", "/api/notify/notifiers/{id}", Need::Read),
    ("PATCH", "/api/notify/notifiers/{id}", Need::Admin),
    ("DELETE", "/api/notify/notifiers/{id}", Need::Admin),
    ("POST", "/api/notify/notifiers/{id}/test", Need::Admin),
    ("GET", "/api/notify/rules", Need::Read),
    ("POST", "/api/notify/rules", Need::Admin),
    ("GET", "/api/notify/rules/{id}", Need::Read),
    ("PATCH", "/api/notify/rules/{id}", Need::Admin),
    ("DELETE", "/api/notify/rules/{id}", Need::Admin),
    ("GET", "/api/notify/history", Need::Read),
    ("GET", "/api/events/feed", Need::Read),
    // CPU profile (`--features profiling` builds only).
    ("GET", "/api/debug/pprof/profile", Need::Admin),
    // Cluster (stage 9). Other `/api/cluster/*` routes fall back to GET=Read / else Admin.
    ("GET", "/api/cluster/metadata", Need::Read),
    ("GET", "/api/cluster/status", Need::Read),
    ("POST", "/api/cluster/partitions/reassign", Need::Admin),
    ("POST", "/api/cluster/nodes/{id}/drain", Need::Admin),
    // Node decommission / forget (v0.5): progress for viewers, mutations admin.
    ("POST", "/api/cluster/nodes/{id}/decommission", Need::Admin),
    ("GET", "/api/cluster/nodes/{id}/decommission", Need::Read),
    ("DELETE", "/api/cluster/nodes/{id}", Need::Admin),
    ("GET", "/api/cluster/decommissions", Need::Read),
    // Automatic rebalancing (v0.4): status for viewers, everything else admin.
    ("GET", "/api/cluster/rebalance", Need::Read),
    ("POST", "/api/cluster/rebalance", Need::Admin),
    ("PUT", "/api/cluster/rebalance/config", Need::Admin),
    ("POST", "/api/cluster/rebalance/pause", Need::Admin),
    ("POST", "/api/cluster/rebalance/resume", Need::Admin),
    ("POST", "/api/cluster/rebalance/cancel", Need::Admin),
];

/// Resolves the rule for a request. `matched` is axum's matched route pattern (`None` when
/// no route matched); `path` the raw request path.
pub fn rule_for(method: &Method, matched: Option<&str>, path: &str) -> Rule {
    if path == "/api/health" {
        return Rule::Open;
    }
    if path.starts_with("/internal/") || matched.is_some_and(|m| m.starts_with("/internal/")) {
        return Rule::External;
    }
    let is_read = *method == Method::GET || *method == Method::HEAD;
    let method_name = if *method == Method::HEAD {
        "GET"
    } else {
        method.as_str()
    };
    if let Some(pattern) = matched
        && let Some((_, _, need)) = PERMISSIONS
            .iter()
            .find(|(m, p, _)| *m == method_name && *p == pattern)
    {
        return Rule::Require(*need);
    }
    // Deny by default: unknown mutations need admin; unknown reads need Read.
    Rule::Require(if is_read { Need::Read } else { Need::Admin })
}

/// Authenticates the bearer token, authorizes the route via [`PERMISSIONS`], and runs the
/// handler with the [`Principal`] in request extensions and [`current_principal`].
///
/// Accepts `Authorization: Bearer <token>`; WebSocket upgrades (`/ws`) may instead pass
/// `?token=<token>` because browsers cannot set headers on `new WebSocket(...)`.
pub async fn require_auth(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path().to_owned();
    let matched = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned());
    let need = match rule_for(req.method(), matched.as_deref(), &path) {
        Rule::Open | Rule::External => return next.run(req).await,
        Rule::Require(need) => need,
    };

    let token = bearer(&req).or_else(|| {
        (path == "/ws")
            .then(|| query_token(req.uri().query()))
            .flatten()
    });
    let principal = match state.auth.authenticate(token.as_deref()) {
        Ok(p) => p,
        Err(e) => return ApiErr::from(e).into_response(),
    };
    if let Err(e) = state.auth.authorize(&principal, need.action()) {
        tracing::debug!(
            actor = %principal.actor(),
            method = %req.method(),
            route = matched.as_deref().unwrap_or(&path),
            "forbidden"
        );
        return ApiErr::from(e).into_response();
    }
    req.extensions_mut().insert(principal.clone());
    PRINCIPAL.scope(principal, next.run(req)).await
}

/// Former single-token middleware; kept as an alias of [`require_auth`].
pub async fn require_token(state: State<AppState>, req: Request, next: Next) -> Response {
    require_auth(state, req, next).await
}

impl From<AuthError> for ApiErr {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::Unauthenticated => ApiErr::unauthorized("missing, invalid or expired token"),
            AuthError::Forbidden(msg) => ApiErr::new(StatusCode::FORBIDDEN, "forbidden", msg),
            AuthError::Invalid(msg) => ApiErr::bad_request(msg),
            AuthError::NotFound => ApiErr::not_found("token not found"),
            AuthError::BindingNotFound => ApiErr::not_found("certificate binding not found"),
            AuthError::LastAdmin(msg) => ApiErr::new(
                StatusCode::CONFLICT,
                fluxmq_proto::auth::ERROR_LAST_ADMIN,
                msg,
            ),
            AuthError::Conflict(msg) => ApiErr::conflict(msg),
            AuthError::Meta(fluxmq_meta::MetaError::Unavailable(msg)) => {
                ApiErr::new(StatusCode::SERVICE_UNAVAILABLE, "internal", msg)
            }
            AuthError::Meta(e) => {
                tracing::error!(error = %e, "auth metadata error");
                ApiErr::internal("metadata store error")
            }
        }
    }
}

fn bearer(req: &Request) -> Option<String> {
    let v = req.headers().get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = v.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim().to_string())
        .filter(|t| !t.is_empty())
}

fn query_token(query: Option<&str>) -> Option<String> {
    let query = query?;
    url_pairs(query).find(|(k, _)| k == "token").map(|(_, v)| v)
}

/// Minimal `application/x-www-form-urlencoded` pair iterator (percent and `+` decoding).
fn url_pairs(query: &str) -> impl Iterator<Item = (String, String)> + '_ {
    query.split('&').filter(|p| !p.is_empty()).map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(k), percent_decode(v))
    })
}

pub(crate) fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok());
                match hex {
                    Some(b) => {
                        out.push(b);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Compares in time independent of where the inputs differ (length is not secret).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    std::hint::black_box(diff) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ct_eq() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn auth_error_mapping() {
        let e = ApiErr::from(AuthError::LastAdmin("x".into()));
        assert_eq!(e.status, StatusCode::CONFLICT);
        assert_eq!(e.body.error, fluxmq_proto::auth::ERROR_LAST_ADMIN);
        let e = ApiErr::from(AuthError::Conflict("dup".into()));
        assert_eq!(e.status, StatusCode::CONFLICT);
        assert_eq!(e.body.error, "conflict");
        let e = ApiErr::from(AuthError::BindingNotFound);
        assert_eq!(e.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn query_token_parsing() {
        assert_eq!(query_token(Some("a=1&token=abc")), Some("abc".into()));
        assert_eq!(query_token(Some("token=a%2Bb%3D")), Some("a+b=".into()));
        assert_eq!(query_token(Some("token=a+b")), Some("a b".into()));
        assert_eq!(query_token(Some("x=1")), None);
        assert_eq!(query_token(None), None);
        assert_eq!(query_token(Some("token=%zz%")), Some("%zz%".into()));
    }

    #[test]
    fn permission_table() {
        let r = |m: Method, p: &str| rule_for(&m, Some(p), p);
        assert_eq!(r(Method::GET, "/api/channels"), Rule::Require(Need::Read));
        assert_eq!(r(Method::HEAD, "/api/channels"), Rule::Require(Need::Read));
        assert_eq!(r(Method::POST, "/api/channels"), Rule::Require(Need::Admin));
        assert_eq!(
            r(Method::POST, "/api/dlq/retry"),
            Rule::Require(Need::Operate)
        );
        assert_eq!(
            r(Method::GET, "/api/dlq/export"),
            Rule::Require(Need::Operate)
        );
        assert_eq!(
            r(Method::GET, "/api/auth/tokens"),
            Rule::Require(Need::Admin)
        );
        for (m, p) in [
            (Method::GET, "/api/auth/cert-bindings"),
            (Method::POST, "/api/auth/cert-bindings"),
            (Method::DELETE, "/api/auth/cert-bindings/{cn}"),
        ] {
            assert_eq!(r(m, p), Rule::Require(Need::Admin), "{p}");
        }
        assert_eq!(r(Method::GET, "/api/health"), Rule::Open);
        assert_eq!(r(Method::POST, "/internal/v1/fetch"), Rule::External);
        assert_eq!(
            r(Method::GET, "/api/cluster/rebalance"),
            Rule::Require(Need::Read)
        );
        for (m, p) in [
            (Method::POST, "/api/cluster/rebalance"),
            (Method::PUT, "/api/cluster/rebalance/config"),
            (Method::POST, "/api/cluster/rebalance/pause"),
            (Method::POST, "/api/cluster/rebalance/resume"),
            (Method::POST, "/api/cluster/rebalance/cancel"),
            (Method::POST, "/api/cluster/nodes/{id}/decommission"),
            (Method::DELETE, "/api/cluster/nodes/{id}"),
        ] {
            assert_eq!(r(m, p), Rule::Require(Need::Admin), "{p}");
        }
        assert_eq!(
            r(Method::GET, "/api/cluster/nodes/{id}/decommission"),
            Rule::Require(Need::Read)
        );
        assert_eq!(
            r(Method::GET, "/api/cluster/decommissions"),
            Rule::Require(Need::Read)
        );
        // Unknown routes: reads need Read, mutations need admin.
        assert_eq!(
            r(Method::GET, "/api/cluster/whatever"),
            Rule::Require(Need::Read)
        );
        assert_eq!(
            r(Method::PUT, "/api/channels/{name}"),
            Rule::Require(Need::Admin)
        );
        assert_eq!(
            rule_for(&Method::DELETE, Some("/api/{*rest}"), "/api/x"),
            Rule::Require(Need::Admin)
        );
        assert_eq!(
            rule_for(&Method::POST, None, "/api/x"),
            Rule::Require(Need::Admin)
        );
        // Every table entry is unique.
        for (i, a) in PERMISSIONS.iter().enumerate() {
            for b in &PERMISSIONS[i + 1..] {
                assert!(!(a.0 == b.0 && a.1 == b.1), "duplicate {a:?}");
            }
        }
    }
}

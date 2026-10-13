//! RBAC over HTTP: roles, scopes, 401 vs 403, audit actor, token endpoints, open mode, CORS.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use fluxmq_api::{AppState, AuditLog, router};
use fluxmq_auth::AuthService;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

const BOOT: &str = "boot-s3cret";

struct H {
    app: Router,
    audit: AuditLog,
    _dir: tempfile::TempDir,
}

async fn harness(bootstrap: Option<&str>) -> H {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    let broker = Broker::open(cfg).await.unwrap();
    let telemetry = TelemetryHub::start(broker.clone(), TelemetryConfig::default());
    let audit = AuditLog::in_memory(1000);
    let state = AppState::new(broker, telemetry)
        .with_auth(AuthService::in_memory(bootstrap.map(str::to_string)))
        .with_audit(audit.clone());
    H {
        app: router(state),
        audit,
        _dir: dir,
    }
}

impl H {
    async fn raw(&self, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Value) {
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let v = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
        };
        (status, headers, v)
    }

    async fn call(
        &self,
        method: &str,
        uri: &str,
        body: Option<Value>,
        token: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        let req = match body {
            Some(b) => req
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap();
        let (s, _, v) = self.raw(req).await;
        (s, v)
    }

    async fn token(&self, by: &str, body: Value) -> (String, String) {
        let (s, v) = self
            .call("POST", "/api/auth/tokens", Some(body), Some(by))
            .await;
        assert_eq!(s, StatusCode::CREATED, "{v}");
        (
            v["token"].as_str().unwrap().to_string(),
            v["info"]["id"].as_str().unwrap().to_string(),
        )
    }
}

fn msg() -> Value {
    json!({"messages": [{"payload": {"encoding": "utf8", "data": "hi"}}]})
}

#[tokio::test]
async fn roles_over_rest() {
    let h = harness(Some(BOOT)).await;
    let (viewer, _) = h
        .token(BOOT, json!({"name": "dash", "role": "viewer"}))
        .await;
    let (operator, _) = h
        .token(BOOT, json!({"name": "ops", "role": "operator"}))
        .await;
    let (admin, admin_id) = h
        .token(BOOT, json!({"name": "root", "role": "admin"}))
        .await;

    // Admin creates the channel.
    let (s, _) = h
        .call(
            "POST",
            "/api/channels",
            Some(json!({"name": "jobs", "mode": "queue"})),
            Some(&admin),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);

    // Viewer: all GETs, no DLQ retry, no publish, no channel create.
    for path in [
        "/api/channels",
        "/api/channels/jobs",
        "/api/dlq",
        "/api/dlq/summary",
        "/api/alerts",
        "/api/audit",
        "/api/auth/me",
    ] {
        let (s, v) = h.call("GET", path, None, Some(&viewer)).await;
        assert_eq!(s, StatusCode::OK, "viewer GET {path}: {v}");
    }
    let (s, v) = h
        .call(
            "POST",
            "/api/dlq/retry",
            Some(json!({"all": true})),
            Some(&viewer),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(v["error"], "forbidden");
    for (m, p, b) in [
        ("GET", "/api/dlq/export", None),
        ("POST", "/api/channels/jobs/publish", Some(msg())),
        (
            "POST",
            "/api/channels",
            Some(json!({"name": "x", "mode": "queue"})),
        ),
        ("DELETE", "/api/channels/jobs", None),
        ("GET", "/api/auth/tokens", None),
    ] {
        assert_eq!(
            h.call(m, p, b, Some(&viewer)).await.0,
            StatusCode::FORBIDDEN,
            "viewer {m} {p}"
        );
    }

    // Operator: DLQ retry, publish, replay — but not channel admin or tokens.
    let (s, v) = h
        .call(
            "POST",
            "/api/dlq/retry",
            Some(json!({"all": true})),
            Some(&operator),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(
        h.call(
            "POST",
            "/api/channels/jobs/publish",
            Some(msg()),
            Some(&operator)
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        h.call("GET", "/api/dlq/export", None, Some(&operator))
            .await
            .0,
        StatusCode::OK
    );
    for (m, p, b) in [
        (
            "POST",
            "/api/channels",
            Some(json!({"name": "x", "mode": "queue"})),
        ),
        ("PATCH", "/api/channels/jobs", Some(json!({}))),
        ("DELETE", "/api/channels/jobs", None),
        (
            "POST",
            "/api/auth/tokens",
            Some(json!({"name": "x", "role": "viewer"})),
        ),
        ("DELETE", &format!("/api/auth/tokens/{admin_id}"), None),
        // Unknown mutation: deny by default (admin only).
        ("POST", "/api/nope", Some(json!({}))),
    ] {
        assert_eq!(
            h.call(m, p, b, Some(&operator)).await.0,
            StatusCode::FORBIDDEN,
            "operator {m} {p}"
        );
    }
    // Unknown read: Read → falls through to the JSON 404.
    assert_eq!(
        h.call("GET", "/api/nope", None, Some(&viewer)).await.0,
        StatusCode::NOT_FOUND
    );

    // Admin: everything.
    assert_eq!(
        h.call("GET", "/api/auth/tokens", None, Some(&admin))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        h.call("DELETE", "/api/channels/jobs", None, Some(&admin))
            .await
            .0,
        StatusCode::NO_CONTENT
    );

    // 401 vs 403.
    let (s, v) = h.call("GET", "/api/channels", None, None).await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::UNAUTHORIZED, Some("unauthorized"))
    );
    let forged = format!("{viewer}x");
    assert_eq!(
        h.call("GET", "/api/channels", None, Some(&forged)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        h.call("GET", "/api/health", None, None).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn token_endpoints_and_audit() {
    let h = harness(Some(BOOT)).await;
    let (s, v) = h.call("GET", "/api/auth/me", None, Some(BOOT)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["name"], "admin");
    assert_eq!(v["role"], "admin");
    assert_eq!(v["mode"], "enforced");

    let (s, created) = h
        .call(
            "POST",
            "/api/auth/tokens",
            Some(json!({
                "name": "ci",
                "role": "admin",
                "scopes": {"publish": ["orders.*"]},
                "ttl_secs": 3600
            })),
            Some(BOOT),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let token = created["token"].as_str().unwrap().to_string();
    let id = created["info"]["id"].as_str().unwrap().to_string();
    assert!(token.starts_with(&format!("fmq_{id}_")));
    let secret = token.rsplit('_').next().unwrap().to_string();
    assert!(created["info"]["expires_at_ms"].is_u64());

    let (s, me) = h.call("GET", "/api/auth/me", None, Some(&token)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(me["name"], "ci");
    assert_eq!(me["token_id"], id.as_str());
    assert_eq!(me["scopes"]["publish"], json!(["orders.*"]));

    // Actions by the token are attributed to `name#id`.
    let (s, _) = h
        .call(
            "POST",
            "/api/channels",
            Some(json!({"name": "orders", "mode": "stream"})),
            Some(&token),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let entries = h.audit.recent(100);
    let create = entries
        .iter()
        .find(|e| e.action == "channel.create")
        .unwrap();
    assert_eq!(create.actor, format!("ci#{id}"));
    let tok = entries.iter().find(|e| e.action == "token.create").unwrap();
    assert_eq!(tok.actor, "admin");
    assert_eq!(tok.target, id);

    // Listing: no secret anywhere; last_used tracked.
    let (s, list) = h.call("GET", "/api/auth/tokens", None, Some(BOOT)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert!(list[0]["last_used_ms"].is_u64());
    assert!(!list.to_string().contains(&secret));

    // Validation errors are 400s.
    let (s, v) = h
        .call(
            "POST",
            "/api/auth/tokens",
            Some(json!({"name": "admin", "role": "viewer"})),
            Some(BOOT),
        )
        .await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("bad_request"))
    );
    let (s, _) = h
        .call(
            "POST",
            "/api/auth/tokens",
            Some(json!({"name": "x", "role": "root"})),
            Some(BOOT),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    // Revoke → token stops working; second revoke 404.
    let uri = format!("/api/auth/tokens/{id}");
    assert_eq!(
        h.call("DELETE", &uri, None, Some(BOOT)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        h.call("GET", "/api/auth/me", None, Some(&token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        h.call("DELETE", &uri, None, Some(BOOT)).await.0,
        StatusCode::NOT_FOUND
    );
    let entries = h.audit.recent(100);
    let rev = entries.iter().find(|e| e.action == "token.revoke").unwrap();
    assert_eq!(
        (rev.actor.as_str(), rev.target.as_str()),
        ("admin", id.as_str())
    );

    // The secret never appears in the audit log.
    let all = serde_json::to_string(&h.audit.recent(1000)).unwrap();
    assert!(!all.contains(&secret));
    assert!(!all.contains(BOOT));
}

#[tokio::test]
async fn open_mode_first_token_enforces() {
    let h = harness(None).await;
    let (s, me) = h.call("GET", "/api/auth/me", None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        (me["mode"].as_str(), me["name"].as_str()),
        (Some("open"), Some("anonymous"))
    );

    // CORS is permissive in open mode.
    let cors_req = || {
        Request::builder()
            .uri("/api/channels")
            .header(header::ORIGIN, "http://localhost:5173")
            .body(Body::empty())
            .unwrap()
    };
    let (_, headers, _) = h.raw(cors_req()).await;
    assert!(headers.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));

    // The first token must be admin; reserved names are refused.
    for body in [
        json!({"name": "first", "role": "viewer"}),
        json!({"name": "Admin", "role": "admin"}),
    ] {
        let (s, v) = h.call("POST", "/api/auth/tokens", Some(body), None).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    }
    let (s, created) = h
        .call(
            "POST",
            "/api/auth/tokens",
            Some(json!({"name": "first", "role": "admin"})),
            Some("ignored-in-open-mode"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["adopt"], true, "{created}");
    let token = created["token"].as_str().unwrap().to_string();
    let (s, second) = h
        .call(
            "POST",
            "/api/auth/tokens",
            Some(json!({"name": "dash", "role": "viewer"})),
            Some(&token),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{second}");
    assert_eq!(second["adopt"], false, "{second}");
    let entries = h.audit.recent(10);
    let entries: Vec<_> = entries
        .into_iter()
        .filter(|e| e.target != second["info"]["id"])
        .collect();
    assert_eq!(entries[0].actor, "anonymous");

    // Enforced now: anonymous access is gone, the new token works, CORS is off.
    assert_eq!(
        h.call("GET", "/api/channels", None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (s, me) = h.call("GET", "/api/auth/me", None, Some(&token)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(me["mode"], "enforced");
    let (_, headers, _) = h.raw(cors_req()).await;
    assert!(!headers.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));

    // The last token cannot be revoked (would silently reopen the broker).
    let viewer_id = second["info"]["id"].as_str().unwrap();
    let (s, _) = h
        .call(
            "DELETE",
            &format!("/api/auth/tokens/{viewer_id}"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let id = me["token_id"].as_str().unwrap();
    let (s, v) = h
        .call(
            "DELETE",
            &format!("/api/auth/tokens/{id}"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
}

#[tokio::test]
async fn ws_requires_read_and_accepts_query_token() {
    let h = harness(Some(BOOT)).await;
    let (viewer, _) = h
        .token(BOOT, json!({"name": "dash", "role": "viewer"}))
        .await;
    let ws_req = |uri: &str| {
        Request::builder()
            .uri(uri)
            .header(header::CONNECTION, "upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(h.raw(ws_req("/ws")).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        h.raw(ws_req("/ws?token=wrong")).await.0,
        StatusCode::UNAUTHORIZED
    );
    // Authorized: passes the middleware (oneshot can't complete a real upgrade, so anything
    // but 401/403 means auth accepted it).
    let s = h.raw(ws_req(&format!("/ws?token={viewer}"))).await.0;
    assert!(
        s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
        "{s}"
    );
    // `?token=` is only honoured on /ws.
    assert_eq!(
        h.call("GET", &format!("/api/channels?token={viewer}"), None, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}

//! Notifications API: CRUD, redaction round-trip, RBAC (viewer 403), test sends, history,
//! the events feed, and the system-channel write protections.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request as AxRequest, State};
use axum::http::{Request, StatusCode, header};
use fluxmq_api::{AppState, AuditLog, router};
use fluxmq_auth::AuthService;
use fluxmq_cluster::Cluster;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_notify::{Endpoints, NotifyConfig, NotifyService};
use fluxmq_proto::notify::REDACTED;
use fluxmq_router::RouterService;
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};
use http_body_util::BodyExt;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tower::ServiceExt;

const BOOT: &str = "boot-s3cret";
const TG_TOKEN: &str = "123456:AAtopSecretToken";

type Seen = Arc<Mutex<Vec<(String, Value)>>>;

/// Records `(path, json body)` of every request; answers 200 `{"ok":true}`.
async fn mock() -> (String, Seen) {
    async fn h(State(seen): State<Seen>, req: AxRequest) -> axum::Json<Value> {
        let path = req.uri().path().to_string();
        let body = axum::body::to_bytes(req.into_body(), 1 << 20)
            .await
            .unwrap_or_default();
        let v = serde_json::from_slice(&body).unwrap_or(Value::Null);
        seen.lock().push((path, v));
        axum::Json(json!({"ok": true}))
    }
    let seen: Seen = Arc::default();
    let app = Router::new().fallback(h).with_state(seen.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    (format!("http://{addr}"), seen)
}

struct H {
    app: Router,
    audit: AuditLog,
    base: String,
    seen: Seen,
    viewer: String,
    notify: NotifyService,
    _dir: tempfile::TempDir,
}

impl Drop for H {
    fn drop(&mut self) {
        self.notify.shutdown();
    }
}

async fn harness() -> H {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    let broker = Broker::open(cfg).await.unwrap();
    let telemetry = TelemetryHub::start(broker.clone(), TelemetryConfig::default());
    let cluster = Cluster::standalone(broker.clone());
    let (base, seen) = mock().await;
    let config = NotifyConfig {
        endpoints: Endpoints {
            telegram_api: base.clone(),
            pagerduty_events: format!("{base}/v2/enqueue"),
        },
        ..NotifyConfig::default()
    };
    let notify = NotifyService::start(
        broker.clone(),
        cluster.meta(),
        &telemetry,
        cluster.clone(),
        config,
    )
    .await
    .unwrap();
    let router_svc = RouterService::start(broker.clone(), cluster.meta())
        .await
        .unwrap();
    let audit = AuditLog::in_memory(1000);
    let state = AppState::new(broker, telemetry)
        .with_auth(AuthService::in_memory(Some(BOOT.into())))
        .with_audit(audit.clone())
        .with_cluster(cluster)
        .with_router(router_svc)
        .with_notify(notify.clone());
    let mut h = H {
        app: router(state),
        audit,
        base,
        seen,
        viewer: String::new(),
        notify,
        _dir: dir,
    };
    let (s, v) = h
        .call(
            "POST",
            "/api/auth/tokens",
            Some(json!({"name": "dash", "role": "viewer"})),
            Some(BOOT),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    h.viewer = v["token"].as_str().unwrap().to_string();
    h
}

impl H {
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
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let v = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, v)
    }

    async fn admin(&self, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.call(method, uri, body, Some(BOOT)).await
    }
}

fn telegram() -> Value {
    json!({
        "name": "On-call",
        "kind": {"type": "telegram", "bot_token": TG_TOKEN, "chat_id": "-1001234567890", "thread_id": null}
    })
}

#[tokio::test]
async fn crud_redaction_and_test_send() {
    let h = harness().await;
    let (s, n) = h
        .admin("POST", "/api/notify/notifiers", Some(telegram()))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{n}");
    assert_eq!(n["kind"]["bot_token"], REDACTED);
    assert_eq!(n["enabled"], true);
    let id = n["id"].as_str().unwrap().to_string();

    let (s, got) = h
        .call(
            "GET",
            &format!("/api/notify/notifiers/{id}"),
            None,
            Some(&h.viewer),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(got["kind"]["bot_token"], REDACTED);
    let (_, list) = h.admin("GET", "/api/notify/notifiers", None).await;
    assert_eq!(list[0]["kind"]["bot_token"], REDACTED);

    // Echo the redacted kind back with a new chat id: the token is kept.
    let mut kind = got["kind"].clone();
    kind["chat_id"] = json!("@ops_room");
    let (s, upd) = h
        .admin(
            "PATCH",
            &format!("/api/notify/notifiers/{id}"),
            Some(json!({"kind": kind, "name": "On-call 2"})),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{upd}");
    assert_eq!(upd["name"], "On-call 2");
    assert_eq!(upd["kind"]["chat_id"], "@ops_room");
    assert_eq!(upd["kind"]["bot_token"], REDACTED);

    // Test send proves the stored token survived (it is in the Bot API path).
    let (s, r) = h
        .admin("POST", &format!("/api/notify/notifiers/{id}/test"), None)
        .await;
    assert_eq!(s, StatusCode::OK, "{r}");
    assert_eq!(r["ok"], true, "{r}");
    {
        let seen = h.seen.lock();
        assert_eq!(seen[0].0, format!("/bot{TG_TOKEN}/sendMessage"));
        assert_eq!(seen[0].1["chat_id"], "@ops_room");
    }

    // Validation errors are 400.
    let (s, _) = h
        .admin(
            "POST",
            "/api/notify/notifiers",
            Some(json!({"name": "x", "kind": {"type": "slack", "webhook_url": "not a url"}})),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, _) = h
        .admin(
            "POST",
            "/api/notify/notifiers",
            Some(json!({"name": "x", "kind": {"type": "pagerduty", "routing_key": REDACTED}})),
        )
        .await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "REDACTED on create has nothing to keep"
    );

    // Rules.
    let (s, rule) = h
        .admin(
            "POST",
            "/api/notify/rules",
            Some(json!({
                "name": "Critical",
                "notifier_ids": [id],
                "match": {"min_severity": "critical", "channels": ["orders.*"]}
            })),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{rule}");
    assert_eq!(rule["group_wait_secs"], 30);
    assert_eq!(rule["repeat_interval_secs"], 14400);
    assert_eq!(rule["send_resolved"], true);
    let rid = rule["id"].as_str().unwrap().to_string();
    let (s, _) = h
        .admin(
            "POST",
            "/api/notify/rules",
            Some(json!({"name": "bad", "notifier_ids": ["nope"], "match": {"min_severity": "warning"}})),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, upd) = h
        .admin(
            "PATCH",
            &format!("/api/notify/rules/{rid}"),
            Some(json!({"group_wait_secs": 5, "enabled": false})),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(upd["group_wait_secs"], 5);
    assert_eq!(upd["enabled"], false);

    // A notifier in use can't be deleted.
    let (s, e) = h
        .admin("DELETE", &format!("/api/notify/notifiers/{id}"), None)
        .await;
    assert_eq!(s, StatusCode::CONFLICT, "{e}");
    let (s, _) = h
        .admin("DELETE", &format!("/api/notify/rules/{rid}"), None)
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, _) = h
        .admin("DELETE", &format!("/api/notify/notifiers/{id}"), None)
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, _) = h
        .admin("GET", &format!("/api/notify/notifiers/{id}"), None)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // History has the test send; nothing secret in the audit log.
    let (_, hist) = h.admin("GET", "/api/notify/history?limit=10", None).await;
    assert_eq!(hist[0]["summary"], "test message");
    assert_eq!(hist[0]["status"], "sent");
    let audit = serde_json::to_string(&h.audit.recent(1000)).unwrap();
    assert!(audit.contains("notifier.create"));
    assert!(audit.contains("notifier.update"));
    assert!(audit.contains("notifier.test"));
    assert!(audit.contains("notify_rule.delete"));
    assert!(!audit.contains("AAtopSecretToken"), "{audit}");
}

#[tokio::test]
async fn viewer_reads_but_cannot_mutate() {
    let h = harness().await;
    let (_, n) = h
        .admin("POST", "/api/notify/notifiers", Some(telegram()))
        .await;
    let id = n["id"].as_str().unwrap();
    let v = Some(h.viewer.as_str());
    for path in [
        "/api/notify/notifiers".to_string(),
        format!("/api/notify/notifiers/{id}"),
        "/api/notify/rules".to_string(),
        "/api/notify/history".to_string(),
        "/api/events/feed".to_string(),
    ] {
        let (s, body) = h.call("GET", &path, None, v).await;
        assert_eq!(s, StatusCode::OK, "viewer GET {path}: {body}");
    }
    let rule = json!({"name": "r", "notifier_ids": [id], "match": {"min_severity": "warning"}});
    for (m, p, b) in [
        (
            "POST",
            "/api/notify/notifiers".to_string(),
            Some(telegram()),
        ),
        (
            "POST",
            "/api/notify/notifiers/test".to_string(),
            Some(telegram()),
        ),
        (
            "PATCH",
            format!("/api/notify/notifiers/{id}"),
            Some(json!({"enabled": false})),
        ),
        ("DELETE", format!("/api/notify/notifiers/{id}"), None),
        ("POST", format!("/api/notify/notifiers/{id}/test"), None),
        ("POST", "/api/notify/rules".to_string(), Some(rule)),
        ("PATCH", "/api/notify/rules/x".to_string(), Some(json!({}))),
        ("DELETE", "/api/notify/rules/x".to_string(), None),
    ] {
        let (s, body) = h.call(m, &p, b, v).await;
        assert_eq!(s, StatusCode::FORBIDDEN, "viewer {m} {p}: {body}");
    }
    assert_eq!(
        h.call("GET", "/api/notify/notifiers", None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert!(h.seen.lock().is_empty(), "no test was sent");
}

#[tokio::test]
async fn test_before_save_persists_nothing() {
    let h = harness().await;
    let body = json!({
        "name": "hook",
        "kind": {"type": "webhook", "url": format!("{}/in", h.base), "secret": "k", "headers": {"X-Token": "abc"}}
    });
    let (s, r) = h
        .admin("POST", "/api/notify/notifiers/test", Some(body))
        .await;
    assert_eq!(s, StatusCode::OK, "{r}");
    assert_eq!(r["ok"], true);
    assert_eq!(h.seen.lock()[0].0, "/in");
    assert_eq!(h.seen.lock()[0].1["status"], "test");
    let (_, list) = h.admin("GET", "/api/notify/notifiers", None).await;
    assert_eq!(list, json!([]));
    // A failing target reports ok=false with the error (not an HTTP error).
    let body = json!({
        "name": "dead",
        "kind": {"type": "webhook", "url": "http://127.0.0.1:9/x", "secret": null, "headers": {}}
    });
    let (s, r) = h
        .admin("POST", "/api/notify/notifiers/test", Some(body))
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(r["ok"], false);
    assert!(r["error"].as_str().unwrap().contains("connection"), "{r}");
    let audit = serde_json::to_string(&h.audit.recent(100)).unwrap();
    assert!(
        !audit.contains("\"abc\""),
        "header values are redacted in audit: {audit}"
    );
}

#[tokio::test]
async fn webhook_header_values_are_redacted() {
    let h = harness().await;
    let body = json!({
        "name": "hook",
        "kind": {"type": "webhook", "url": format!("{}/in", h.base), "secret": "k",
                 "headers": {"Authorization": "Bearer abc", "X-Env": "prod"}}
    });
    let (s, n) = h.admin("POST", "/api/notify/notifiers", Some(body)).await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(
        n["kind"]["url"],
        format!("{}/in", h.base),
        "url is not a secret"
    );
    assert_eq!(n["kind"]["secret"], REDACTED);
    assert_eq!(n["kind"]["headers"]["Authorization"], REDACTED);
    assert_eq!(n["kind"]["headers"]["X-Env"], REDACTED);
    let id = n["id"].as_str().unwrap();
    // Keep Authorization, drop X-Env, remove the secret.
    let kind = json!({"type": "webhook", "url": format!("{}/in", h.base), "secret": "",
                      "headers": {"Authorization": REDACTED, "X-Env": ""}});
    let (s, upd) = h
        .admin(
            "PATCH",
            &format!("/api/notify/notifiers/{id}"),
            Some(json!({"kind": kind})),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{upd}");
    assert_eq!(upd["kind"]["secret"], Value::Null);
    assert_eq!(upd["kind"]["headers"], json!({"Authorization": REDACTED}));
}

#[tokio::test]
async fn events_feed_over_http() {
    let h = harness().await;
    let (s, page) = h.admin("GET", "/api/events/feed?after=latest", None).await;
    assert_eq!(s, StatusCode::OK, "{page}");
    let cursor = page["next_cursor"].clone();
    let app = h.app.clone();
    let uri = match cursor.as_str() {
        Some(c) => format!("/api/events/feed?after={c}&wait_ms=15000"),
        None => "/api/events/feed?wait_ms=15000".to_string(),
    };
    let started = Instant::now();
    let poll = tokio::spawn(async move {
        let req = Request::builder()
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {BOOT}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (s, _) = h
        .admin(
            "POST",
            "/api/channels",
            Some(json!({"name": "orders", "mode": "stream", "partitions": 1})),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let page = poll.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(page["items"][0]["kind"], "event");
    assert_eq!(page["items"][0]["event"]["subject"], "orders");
    assert_eq!(page["items"][0]["event"]["kind"], "channel_created");
    assert_eq!(page["gap"], false);
    assert_eq!(page["next_cursor"], page["items"][0]["cursor"]);

    let (s, tail) = h.admin("GET", "/api/events/feed?tail=1", None).await;
    assert_eq!(s, StatusCode::OK, "{tail}");
    assert_eq!(tail["items"].as_array().unwrap().len(), 1);
    assert_eq!(tail["items"][0]["cursor"], page["items"][0]["cursor"]);
    assert_eq!(tail["next_cursor"], page["items"][0]["cursor"]);

    for bad in [
        "/api/events/feed?after=abc",
        "/api/events/feed?after=1&tail=5",
        "/api/events/feed?tail=0",
        "/api/events/feed?tail=1001",
        "/api/events/feed?limit=0",
        "/api/events/feed?limit=1001",
        "/api/events/feed?wait_ms=30001",
    ] {
        assert_eq!(
            h.admin("GET", bad, None).await.0,
            StatusCode::BAD_REQUEST,
            "{bad}"
        );
    }
}

#[tokio::test]
async fn system_channels_are_read_only() {
    let h = harness().await;
    let sys = "%24system.events";
    // Visible like any channel.
    let (s, ch) = h.admin("GET", &format!("/api/channels/{sys}"), None).await;
    assert_eq!(s, StatusCode::OK, "{ch}");
    assert_eq!(ch["partitions"], 1);
    // Creating `$…` is reserved.
    let (s, e) = h
        .admin(
            "POST",
            "/api/channels",
            Some(json!({"name": "$mine", "mode": "stream"})),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{e}");
    for (m, p, b) in [
        ("DELETE", format!("/api/channels/{sys}"), None),
        (
            "PATCH",
            format!("/api/channels/{sys}"),
            Some(json!({"dlq_enabled": true})),
        ),
        (
            "POST",
            format!("/api/channels/{sys}/publish"),
            Some(json!({"messages": [{"payload": {"encoding": "utf8", "data": "x"}}]})),
        ),
        (
            "POST",
            format!("/api/channels/{sys}/replay"),
            Some(json!({"ids": [{"partition": 0, "offset": 0}]})),
        ),
    ] {
        let (s, e) = h.admin(m, &p, b).await;
        assert_eq!(s, StatusCode::FORBIDDEN, "{m} {p}: {e}");
        assert!(e["message"].as_str().unwrap().contains("read-only"), "{e}");
    }
    // Replay *into* a system channel is a write too.
    h.admin(
        "POST",
        "/api/channels",
        Some(json!({"name": "orders", "mode": "stream", "partitions": 1})),
    )
    .await;
    let (s, _) = h
        .admin(
            "POST",
            "/api/channels/orders/replay",
            Some(
                json!({"ids": [{"partition": 0, "offset": 0}], "target_channel": "$system.events"}),
            ),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    // Routing: a system channel may be a source, never a target.
    let (s, e) = h
        .admin(
            "POST",
            "/api/routes",
            Some(json!({"name": "bad", "source": "orders", "targets": ["$system.events"], "conditions": []})),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{e}");
    assert!(
        e["message"].as_str().unwrap().contains("system channel"),
        "{e}"
    );
    let (s, e) = h
        .admin(
            "POST",
            "/api/routes",
            Some(json!({"name": "ok", "source": "$system.events", "targets": ["orders"], "conditions": []})),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{e}");
    // Channel still there.
    assert_eq!(
        h.admin("GET", &format!("/api/channels/{sys}"), None)
            .await
            .0,
        StatusCode::OK
    );
}

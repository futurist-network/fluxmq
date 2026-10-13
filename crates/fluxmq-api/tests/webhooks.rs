//! Webhooks REST API: CRUD, redaction, permissions, test deliveries, stats, group release.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use fluxmq_api::{AppState, router};
use fluxmq_auth::AuthService;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_meta::{LocalMetaStore, SharedMetaStore};
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};
use fluxmq_webhooks::{WebhookConfig, WebhookService};
use http_body_util::BodyExt;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tower::ServiceExt;

const BOOT: &str = "boot-s3cret";
const REDACTED: &str = fluxmq_proto::notify::REDACTED;

struct H {
    app: Router,
    broker: Broker,
    _dir: tempfile::TempDir,
}

async fn harness() -> H {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    let broker = Broker::open(cfg).await.unwrap();
    let telemetry = TelemetryHub::start(broker.clone(), TelemetryConfig::default());
    let meta: SharedMetaStore = Arc::new(LocalMetaStore::in_memory());
    let webhooks = WebhookService::start(broker.clone(), meta, WebhookConfig::default())
        .await
        .unwrap();
    let state = AppState::new(broker.clone(), telemetry)
        .with_auth(AuthService::in_memory(Some(BOOT.to_string())))
        .with_webhooks(webhooks);
    H {
        app: router(state),
        broker,
        _dir: dir,
    }
}

impl H {
    async fn call(
        &self,
        method: &str,
        uri: &str,
        body: Option<Value>,
        token: &str,
    ) -> (StatusCode, Value) {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"));
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
        self.call(method, uri, body, BOOT).await
    }
}

/// Requests seen by the receiver: (headers, body).
type Seen = Arc<Mutex<Vec<(BTreeMap<String, String>, String)>>>;

/// Minimal receiver: records bodies + headers, answers with the configured status.
async fn receiver(status: u16) -> (String, Seen) {
    let seen: Seen = Arc::default();
    let s2 = seen.clone();
    let app = axum::Router::new().route(
        "/hook",
        axum::routing::post(move |headers: axum::http::HeaderMap, body: String| {
            let seen = s2.clone();
            async move {
                let h = headers
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect();
                seen.lock().push((h, body));
                (StatusCode::from_u16(status).unwrap(), "received")
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (format!("http://{addr}/hook"), seen)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_crud_redaction_permissions_and_release() {
    let h = harness().await;
    let (url, seen) = receiver(200).await;
    let (s, _) = h
        .admin(
            "POST",
            "/api/channels",
            Some(json!({"name": "orders", "mode": "stream", "partitions": 2})),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, v) = h
        .admin(
            "POST",
            "/api/auth/tokens",
            Some(json!({"name": "dash", "role": "viewer"})),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    let viewer = v["token"].as_str().unwrap().to_string();

    let body = json!({
        "name": "orders-hook",
        "channel": "orders",
        "url": url,
        "secret": "whsec_1",
        "headers": {"Authorization": "Bearer top-secret"},
        "timeout_ms": 2000,
        "start": "earliest",
    });

    // Viewer: reads only.
    let (s, _) = h
        .call("POST", "/api/webhooks", Some(body.clone()), &viewer)
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _) = h
        .call("POST", "/api/webhooks/test", Some(body.clone()), &viewer)
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // Create: redacted response.
    let (s, sub) = h.admin("POST", "/api/webhooks", Some(body.clone())).await;
    assert_eq!(s, StatusCode::CREATED, "{sub}");
    let id = sub["id"].as_str().unwrap().to_string();
    assert_eq!(sub["secret"], REDACTED);
    assert_eq!(sub["headers"]["Authorization"], REDACTED);
    assert_eq!(sub["max_in_flight"], 4);
    assert_eq!(sub["format"], "envelope");

    let (s, list) = h.call("GET", "/api/webhooks", None, &viewer).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["secret"], REDACTED);
    let (s, one) = h
        .call("GET", &format!("/api/webhooks/{id}"), None, &viewer)
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(one["headers"]["Authorization"], REDACTED);
    for (m, p) in [
        ("PATCH", format!("/api/webhooks/{id}")),
        ("DELETE", format!("/api/webhooks/{id}")),
        ("POST", format!("/api/webhooks/{id}/test")),
    ] {
        let (s, _) = h.call(m, &p, Some(json!({})), &viewer).await;
        assert_eq!(s, StatusCode::FORBIDDEN, "viewer {m} {p}");
    }

    // Duplicate name → 409; invalid → 400; unknown channel → 404.
    let (s, _) = h.admin("POST", "/api/webhooks", Some(body.clone())).await;
    assert_eq!(s, StatusCode::CONFLICT);
    let mut bad = body.clone();
    bad["name"] = json!("other");
    bad["batch_max"] = json!(1000);
    assert_eq!(
        h.admin("POST", "/api/webhooks", Some(bad)).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut bad = body.clone();
    bad["name"] = json!("other");
    bad["channel"] = json!("nope");
    assert_eq!(
        h.admin("POST", "/api/webhooks", Some(bad)).await.0,
        StatusCode::NOT_FOUND
    );

    // Delivery with the stored (unredacted) secret and header.
    let (s, _) = h
        .admin(
            "POST",
            "/api/channels/orders/publish",
            Some(json!({"messages": [{"payload": {"encoding": "utf8", "data": "{\"a\":1}"}}]})),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.lock().is_empty() {
        assert!(Instant::now() < deadline, "no delivery");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    {
        let (headers, body) = seen.lock()[0].clone();
        assert_eq!(headers["authorization"], "Bearer top-secret");
        let now = fluxmq_proto::now_ms() / 1000;
        assert!(
            fluxmq_webhooks::verify(
                "whsec_1",
                &headers["x-fluxmq-signature"],
                body.as_bytes(),
                60,
                now
            )
            .is_ok()
        );
    }

    // PATCH with REDACTED keeps secrets; stats; test endpoint.
    let (s, upd) = h
        .admin(
            "PATCH",
            &format!("/api/webhooks/{id}"),
            Some(
                json!({"secret": REDACTED, "headers": {"Authorization": REDACTED}, "batch_max": 5}),
            ),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{upd}");
    assert_eq!(upd["batch_max"], 5);
    assert_eq!(upd["secret"], REDACTED);
    let (s, stats) = h.call("GET", "/api/webhooks/stats", None, &viewer).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(stats[0]["id"], id.as_str());
    assert_eq!(stats[0]["delivered"], 1);
    assert!(stats[0]["breaker"].is_null());

    // Recent attempts and request history (readable by viewers).
    let (s, log) = h
        .call(
            "GET",
            &format!("/api/webhooks/{id}/attempts"),
            None,
            &viewer,
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{log}");
    assert_eq!(log[0]["outcome"], "delivered");
    assert_eq!(log[0]["status"], 200);
    assert!(
        log[0]["delivery_id"]
            .as_str()
            .unwrap()
            .starts_with("orders/")
    );
    let (s, hist) = h
        .call(
            "GET",
            &format!("/api/webhooks/{id}/history?range=15m"),
            None,
            &viewer,
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{hist}");
    assert_eq!(hist["bucket_ms"], 10_000);
    let points = hist["points"].as_array().unwrap();
    assert_eq!(points.len(), 90);
    assert_eq!(
        points
            .iter()
            .map(|p| p["delivered"].as_u64().unwrap())
            .sum::<u64>(),
        1
    );
    let (s, _) = h
        .call(
            "GET",
            &format!("/api/webhooks/{id}/history?range=7d"),
            None,
            &viewer,
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, _) = h
        .call("GET", "/api/webhooks/nope/attempts", None, &viewer)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = h
        .call("GET", "/api/webhooks/nope/history", None, &viewer)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, t) = h
        .admin("POST", &format!("/api/webhooks/{id}/test"), None)
        .await;
    assert_eq!(s, StatusCode::OK, "{t}");
    assert_eq!(t["ok"], true);
    assert_eq!(t["status"], 200);
    let last = seen.lock().last().unwrap().clone();
    assert_eq!(
        last.0["authorization"], "Bearer top-secret",
        "kept header value"
    );
    assert!(last.0["x-fluxmq-delivery-id"].starts_with("test/"));
    let mut unsaved = body.clone();
    unsaved["name"] = json!("draft");
    let (s, t) = h.admin("POST", "/api/webhooks/test", Some(unsaved)).await;
    assert_eq!(s, StatusCode::OK, "{t}");
    assert_eq!(t["ok"], true);
    assert_eq!(
        h.admin("GET", "/api/webhooks", None)
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Audit never contains secrets.
    let (_, audit) = h.admin("GET", "/api/audit?limit=100", None).await;
    let text = audit.to_string();
    assert!(
        text.contains("webhook.create") && text.contains("webhook.update"),
        "{text}"
    );
    assert!(
        !text.contains("whsec_1") && !text.contains("top-secret"),
        "{text}"
    );

    // The group can't be deleted while the webhook exists.
    let (s, _) = h
        .admin(
            "DELETE",
            "/api/channels/orders/groups/%24webhook%3Aorders-hook",
            None,
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT);

    // Delete → worker stops → group released.
    let (s, _) = h
        .admin("DELETE", &format!("/api/webhooks/{id}"), None)
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let deadline = Instant::now() + Duration::from_secs(10);
    while h
        .broker
        .has_group("orders", "$webhook:orders-hook")
        .unwrap()
    {
        assert!(Instant::now() < deadline, "group not released");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        h.admin("GET", &format!("/api/webhooks/{id}"), None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        h.admin("DELETE", &format!("/api/webhooks/{id}"), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failing_test_delivery_reports_status() {
    let h = harness().await;
    let (url, _) = receiver(503).await;
    h.admin(
        "POST",
        "/api/channels",
        Some(json!({"name": "c", "mode": "stream", "partitions": 1})),
    )
    .await;
    let (s, t) = h
        .admin(
            "POST",
            "/api/webhooks/test",
            Some(json!({"name": "x", "channel": "c", "url": url, "timeout_ms": 1000})),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{t}");
    assert_eq!(t["ok"], false);
    assert_eq!(t["status"], 503);
    assert_eq!(t["error"], "HTTP 503");
    assert_eq!(t["response_snippet"], "received");
}

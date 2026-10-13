//! Handler tests: the full router driven with `tower::ServiceExt::oneshot` against a real
//! broker in a temp dir.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use fluxmq_api::{AppState, router};
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

struct Harness {
    app: Router,
    broker: Broker,
    _dir: tempfile::TempDir,
}

async fn harness(token: Option<&str>) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    let broker = Broker::open(cfg).await.unwrap();
    let telemetry = TelemetryHub::start(broker.clone(), TelemetryConfig::default());
    let state =
        AppState::new(broker.clone(), telemetry).with_admin_token(token.map(str::to_string));
    Harness {
        app: router(state),
        broker,
        _dir: dir,
    }
}

impl Harness {
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
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
        };
        (status, value)
    }
}

#[tokio::test]
async fn reset_offsets_existing_unknown_and_bad_body() {
    let h = harness(None).await;
    let (s, _) = h
        .call(
            "POST",
            "/api/channels",
            Some(json!({"name": "jobs", "mode": "queue"})),
            None,
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    // A group exists once something created it (normally a consumer subscribing).
    h.broker
        .reset_offsets("jobs", "workers", fluxmq_proto::model::StartFrom::Latest)
        .unwrap();
    let uri = "/api/channels/jobs/groups/workers/reset";
    let (s, _) = h
        .call("POST", uri, Some(json!({"to": {"timestamp_ms": 1}})), None)
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, v) = h
        .call(
            "POST",
            "/api/channels/jobs/groups/typo/reset",
            Some(json!({"to": "earliest"})),
            None,
        )
        .await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("not_found"))
    );
    assert!(!h.broker.has_group("jobs", "typo").unwrap());
    let (s, _) = h
        .call("POST", uri, Some(json!({"group": "workers"})), None)
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn channel_lifecycle_and_errors() {
    let h = harness(None).await;
    let (s, v) = h
        .call(
            "POST",
            "/api/channels",
            Some(json!({"name": "orders", "mode": "queue"})),
            None,
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(v["partitions"], 1, "queue default partitions");

    let (s, v) = h
        .call(
            "POST",
            "/api/channels",
            Some(json!({"name": "orders", "mode": "queue"})),
            None,
        )
        .await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::CONFLICT, Some("conflict"))
    );

    let (s, v) = h
        .call(
            "POST",
            "/api/channels",
            Some(json!({"name": "x", "mode": "queue", "partitions": 0})),
            None,
        )
        .await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("bad_request"))
    );

    let (s, _) = h.call("GET", "/api/channels/orders", None, None).await;
    assert_eq!(s, StatusCode::OK);
    let (s, v) = h.call("GET", "/api/channels/missing", None, None).await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("not_found"))
    );

    let (s, v) = h
        .call(
            "POST",
            "/api/channels/orders/publish",
            Some(json!({"messages": [{"payload": {"encoding": "utf8", "data": "hello"}}]})),
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["ids"].as_array().unwrap().len(), 1);

    let (s, v) = h
        .call(
            "GET",
            "/api/channels/orders/messages?limit=5000",
            None,
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        v["messages"][0]["payload"],
        json!({"encoding": "utf8", "data": "hello"})
    );

    // `newest_first` is part of `MessageQuery` and must pass the query extractor.
    let (s, v) = h
        .call(
            "GET",
            "/api/channels/orders/messages?since_ms=0&newest_first=true&limit=10",
            None,
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);

    let (s, v) = h
        .call(
            "GET",
            "/api/channels/orders/messages/0/notanumber",
            None,
            None,
        )
        .await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("bad_request"))
    );

    let (s, _) = h.call("DELETE", "/api/channels/orders", None, None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);

    let (_, audit) = h.call("GET", "/api/audit?limit=10", None, None).await;
    let actions: Vec<_> = audit
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        actions,
        ["channel.delete", "channel.publish", "channel.create"]
    );
}

#[tokio::test]
async fn auth_rules() {
    let h = harness(Some("t0k")).await;
    assert_eq!(
        h.call("GET", "/api/health", None, None).await.0,
        StatusCode::OK
    );
    let (s, v) = h.call("GET", "/api/channels", None, None).await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::UNAUTHORIZED, Some("unauthorized"))
    );
    assert_eq!(
        h.call("GET", "/api/channels", None, Some("bad")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        h.call("GET", "/api/channels", None, Some("t0k")).await.0,
        StatusCode::OK
    );
    // Unknown API routes are still behind auth.
    assert_eq!(
        h.call("GET", "/api/nope", None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        h.call("GET", "/api/nope", None, Some("t0k")).await.0,
        StatusCode::NOT_FOUND
    );
    // WS without a token is rejected before the upgrade.
    assert_eq!(
        h.call("GET", "/ws", None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn dlq_and_history_validation() {
    let h = harness(None).await;
    let (s, _) = h
        .call("POST", "/api/dlq/retry", Some(json!({})), None)
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, v) = h
        .call(
            "POST",
            "/api/dlq/discard",
            Some(json!({"reason": "x"})),
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["affected"], 0);
    let (s, v) = h.call("GET", "/api/dlq", None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["total"], 0);
    let (s, _) = h.call("GET", "/api/dlq/summary", None, None).await;
    assert_eq!(s, StatusCode::OK);

    assert_eq!(
        h.call("GET", "/api/history?metric=lag&range=1h", None, None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        h.call("GET", "/api/history?metric=lag&range=3h", None, None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        h.call("GET", "/api/history?metric=nope&range=1h", None, None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        h.call("GET", "/api/alerts", None, None).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn ui_placeholder_when_not_built() {
    let h = harness(None).await;
    let (s, v) = h.call("GET", "/", None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(v.as_str().unwrap().contains("pnpm build"));
}

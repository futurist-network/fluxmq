//! `GET /metrics` (Prometheus) and `GET /internal/v1/node-stats` over HTTP.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use fluxmq_api::{AppState, router};
use fluxmq_auth::AuthService;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_proto::model::{ChannelConfig, ChannelMode};
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn get(app: &axum::Router, uri: &str, token: Option<&str>) -> (StatusCode, String, String) {
    let mut req = Request::get(uri);
    if let Some(t) = token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, ct, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn metrics_exposition_and_internal_node_stats() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    let broker = Broker::open(cfg).await.unwrap();
    broker
        .create_channel(ChannelConfig::new("orders", ChannelMode::Queue))
        .unwrap();
    let telemetry = TelemetryHub::start(
        broker.clone(),
        TelemetryConfig {
            tick: Duration::from_millis(20),
            ..TelemetryConfig::default()
        },
    );
    let state = AppState::new(broker, telemetry)
        .with_auth(AuthService::in_memory(Some("boot-s3cret".into())));
    // No wait for a telemetry tick: `/metrics` renders from a fresh broker snapshot until
    // the first tick has run, so the exposition is complete either way.
    let app = router(state);

    // RBAC: `/metrics` needs a token once auth is enforced.
    let (status, _, _) = get(&app, "/metrics", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, ct, body) = get(&app, "/metrics", Some("boot-s3cret")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/plain; version=0.0.4"), "{ct}");
    assert!(body.contains("# TYPE fluxmq_channel_published_messages_total counter\n"));
    assert!(body.contains("fluxmq_channel_published_messages_total{channel=\"orders\"} 0\n"));
    assert!(body.contains("# TYPE fluxmq_node_fsync_latency_seconds histogram\n"));
    assert!(body.contains("# TYPE fluxmq_router_evaluated_total counter\n"));
    assert!(
        !body.contains("fluxmq_cluster_"),
        "single node: no cluster gauges"
    );
    for line in body.lines().filter(|l| !l.starts_with('#')) {
        let (_, v) = line.rsplit_once(' ').expect("sample line");
        assert!(v == "+Inf" || v.parse::<f64>().is_ok(), "{line}");
    }

    // Single node: the internal endpoint does not exist (and bypasses RBAC).
    let (status, _, _) = get(&app, "/internal/v1/node-stats", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

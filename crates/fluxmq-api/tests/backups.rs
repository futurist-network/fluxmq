//! Backups REST: permissions (viewer 403), secret redaction / keep-on-echo, null config,
//! run → history → detail → delete, target probe, audit.

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
const KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

struct H {
    app: Router,
    audit: AuditLog,
    broker: Broker,
    dir: tempfile::TempDir,
}

async fn harness() -> H {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path().join("data"));
    cfg.fsync = FsyncPolicy::OsBuffered;
    let broker = Broker::open(cfg).await.unwrap();
    let telemetry = TelemetryHub::start(broker.clone(), TelemetryConfig::default());
    let audit = AuditLog::in_memory(1000);
    let state = AppState::new(broker.clone(), telemetry)
        .with_auth(AuthService::in_memory(Some(BOOT.into())))
        .with_audit(audit.clone());
    H {
        app: router(state),
        audit,
        broker,
        dir,
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
            serde_json::from_slice(&bytes)
                .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()))
        };
        (status, v)
    }

    async fn viewer(&self) -> String {
        let (s, v) = self
            .call(
                "POST",
                "/api/auth/tokens",
                Some(json!({"name": "v", "role": "viewer"})),
                BOOT,
            )
            .await;
        assert_eq!(s, StatusCode::CREATED);
        v["token"].as_str().unwrap().to_string()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backups_rest_permissions_redaction_and_lifecycle() {
    let h = harness().await;
    let viewer = h.viewer().await;
    // Unset config is null.
    let (s, v) = h.call("GET", "/api/backups/config", None, &viewer).await;
    assert_eq!((s, v), (StatusCode::OK, Value::Null));
    let (s, v) = h.call("GET", "/api/backups", None, &viewer).await;
    assert_eq!((s, v), (StatusCode::OK, json!([])));
    let (s, _) = h.call("POST", "/api/backups/run", None, BOOT).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "not configured");

    let target_dir = h.dir.path().join("target").display().to_string();
    let cfg = json!({
        "enabled": false,
        "target": {"type": "filesystem", "path": target_dir},
        "interval_secs": 3600, "keep_last": 3, "keep_days": 0,
        "include_messages": true, "encryption_key": KEY,
    });
    // Viewer can't mutate, run, probe or delete.
    for (m, u, b) in [
        ("PUT", "/api/backups/config", Some(cfg.clone())),
        ("POST", "/api/backups/run", None),
        (
            "POST",
            "/api/backups/test-target",
            Some(cfg["target"].clone()),
        ),
        ("DELETE", "/api/backups/x", None),
    ] {
        let (s, v) = h.call(m, u, b, &viewer).await;
        assert_eq!(s, StatusCode::FORBIDDEN, "{m} {u}: {v}");
    }
    let (s, v) = h
        .call("PUT", "/api/backups/config", Some(cfg.clone()), BOOT)
        .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["encryption_key"], "********");
    // Reads redact; echoing the redacted value keeps the key.
    let (_, got) = h.call("GET", "/api/backups/config", None, &viewer).await;
    assert!(!got.to_string().contains(KEY));
    let mut echo = got.clone();
    echo["keep_last"] = json!(4);
    let (s, _) = h.call("PUT", "/api/backups/config", Some(echo), BOOT).await;
    assert_eq!(s, StatusCode::OK);
    let (_, got) = h.call("GET", "/api/backups/config", None, BOOT).await;
    assert_eq!(got["keep_last"], 4);
    assert_eq!(got["encryption_key"], "********", "kept on echo");
    // Validation.
    let mut bad = cfg.clone();
    bad["interval_secs"] = json!(10);
    let (s, _) = h.call("PUT", "/api/backups/config", Some(bad), BOOT).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // S3 secrets are redacted too.
    let s3 = json!({"type": "s3", "endpoint": "http://127.0.0.1:1", "region": "us-east-1",
        "bucket": "b", "prefix": "", "access_key_id": "AK", "secret_access_key": "TOPSECRET",
        "path_style": true});
    let (s, v) = h
        .call("POST", "/api/backups/test-target", Some(s3), BOOT)
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["ok"], false, "nothing listens on port 1");
    assert!(!v.to_string().contains("TOPSECRET"));
    let (s, v) = h
        .call(
            "POST",
            "/api/backups/test-target",
            Some(cfg["target"].clone()),
            BOOT,
        )
        .await;
    assert_eq!((s, v["ok"].clone()), (StatusCode::OK, json!(true)), "{v}");

    // Run → status → history → detail → delete.
    h.broker
        .create_channel(fluxmq_proto::model::ChannelConfig::new(
            "orders",
            fluxmq_proto::model::ChannelMode::Stream,
        ))
        .unwrap();
    let (s, v) = h.call("POST", "/api/backups/run", None, BOOT).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let id = v["id"].as_str().unwrap().to_string();
    let mut done = Value::Null;
    for _ in 0..200 {
        let (_, st) = h.call("GET", "/api/backups/status", None, &viewer).await;
        if st["running"].is_null() && !st["last_success"].is_null() {
            done = st;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(done["last_success"]["id"], id.as_str(), "{done}");
    assert_eq!(done["configured"], true);
    let (_, list) = h.call("GET", "/api/backups", None, &viewer).await;
    assert_eq!(list[0]["id"], id.as_str());
    assert_eq!(list[0]["status"], "succeeded");
    let (s, m) = h
        .call("GET", &format!("/api/backups/{id}"), None, &viewer)
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(m["info"]["id"], id.as_str());
    assert!(m["encryption"].is_string());
    assert!(!m.to_string().contains(KEY));
    // The proto `BackupManifest` summary: no object keys / CRCs.
    let parsed: fluxmq_proto::backup::BackupManifest = serde_json::from_value(m.clone()).unwrap();
    assert_eq!(parsed.channels.len(), 1);
    assert_eq!(parsed.channels[0].name, "orders");
    assert_eq!(parsed.channels[0].partitions, 8);
    assert_eq!(parsed.nodes.len(), 1);
    assert!(m.get("meta").is_none() && !m.to_string().contains("segments/"));
    let (s, _) = h.call("GET", "/api/backups/nope", None, &viewer).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = h
        .call("DELETE", &format!("/api/backups/{id}"), None, BOOT)
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (_, list) = h.call("GET", "/api/backups", None, &viewer).await;
    assert_eq!(list, json!([]));
    // Audit: config, run, delete — never secrets.
    let actions: Vec<String> = h
        .audit
        .recent(100)
        .iter()
        .map(|e| e.action.clone())
        .collect();
    for a in [
        "backup.config",
        "backup.run",
        "backup.delete",
        "backup.test_target",
    ] {
        assert!(actions.iter().any(|x| x == a), "{a} in {actions:?}");
    }
    let dump = serde_json::to_string(&h.audit.recent(100)).unwrap();
    assert!(!dump.contains(KEY) && !dump.contains("TOPSECRET"));
    h.broker.shutdown().await;
}

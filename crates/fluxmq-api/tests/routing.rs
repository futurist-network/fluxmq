//! Routing API + filtered message search, driven through the full router with `oneshot`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use fluxmq_api::{AppState, router};
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_meta::{LocalMetaStore, SharedMetaStore};
use fluxmq_router::RouterService;
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

struct Harness {
    app: Router,
    broker: Broker,
    state: AppState,
    _dir: tempfile::TempDir,
}

async fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::new(dir.path());
    cfg.fsync = FsyncPolicy::OsBuffered;
    let broker = Broker::open(cfg).await.unwrap();
    let telemetry = TelemetryHub::start(broker.clone(), TelemetryConfig::default());
    let meta: SharedMetaStore = Arc::new(LocalMetaStore::in_memory());
    let rs = RouterService::start(broker.clone(), meta).await.unwrap();
    let state = AppState::new(broker.clone(), telemetry).with_router(rs);
    Harness {
        app: router(state.clone()),
        broker,
        state,
        _dir: dir,
    }
}

impl Harness {
    async fn call(&self, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        let req = Request::builder().method(method).uri(uri);
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
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, value)
    }

    async fn channel(&self, name: &str, partitions: u32) {
        let (s, _) = self
            .call(
                "POST",
                "/api/channels",
                Some(json!({"name": name, "mode": "stream", "partitions": partitions})),
            )
            .await;
        assert_eq!(s, StatusCode::CREATED);
    }
}

fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_crud_stats_and_forwarding() {
    let h = harness().await;
    h.channel("orders", 2).await;
    h.channel("orders.md", 1).await;

    let (s, parsed) = h
        .call(
            "POST",
            "/api/filter/parse",
            Some(json!({"filter": "country == 'MD' and amount >= 100"})),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(parsed["rendered"], "country = \"MD\" AND amount >= 100");

    let (s, route) = h
        .call(
            "POST",
            "/api/routes",
            Some(
                json!({"name": "md", "source": "orders", "targets": ["orders.md"],
                        "conditions": parsed["conditions"]}),
            ),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{route}");
    let id = route["id"].as_str().unwrap().to_string();
    assert_eq!(route["enabled"], true);
    assert_eq!(
        route["conditions"][0]["field"],
        json!({"kind":"payload","name":"country"})
    );

    let (s, list) = h.call("GET", "/api/routes", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    let (s, one) = h.call("GET", &format!("/api/routes/{id}"), None).await;
    assert_eq!((s, &one["name"]), (StatusCode::OK, &json!("md")));

    let (s, _) = h
        .call(
            "POST",
            "/api/channels/orders/publish",
            Some(json!({"messages": [
                {"key": "a", "payload": {"encoding":"utf8","data":"{\"country\":\"MD\",\"amount\":150}"}},
                {"key": "b", "payload": {"encoding":"utf8","data":"{\"country\":\"MD\",\"amount\":5}"}}
            ]})),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, stats) = h.call("GET", "/api/routes/stats", None).await;
        if stats[0]["forwarded"] == 1 {
            assert_eq!(stats[0]["id"], id.as_str());
            assert_eq!(stats[0]["matched"], 1);
            assert_eq!(stats[0]["evaluated"], 2);
            break;
        }
        assert!(Instant::now() < deadline, "stats never updated: {stats}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (_, page) = h
        .call("GET", "/api/channels/orders.md/messages", None)
        .await;
    assert_eq!(page["messages"][0]["key"], "a");
    assert_eq!(page["messages"][0]["headers"]["flux-route-id"], id.as_str());

    // The `$router` group is reserved while a route uses the source.
    assert!(h.broker.has_group("orders", "$router").unwrap());
    let (s, e) = h
        .call("DELETE", "/api/channels/orders/groups/$router", None)
        .await;
    assert_eq!(s, StatusCode::CONFLICT, "{e}");

    let (s, upd) = h
        .call(
            "PATCH",
            &format!("/api/routes/{id}"),
            Some(json!({"enabled": false, "name": "md2"})),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        (upd["enabled"].clone(), upd["name"].clone()),
        (json!(false), json!("md2"))
    );

    // Disabled = paused: the group (and its committed offset) stays.
    assert!(h.broker.has_group("orders", "$router").unwrap());

    let (s, _) = h.call("DELETE", &format!("/api/routes/{id}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    // Last route of the source gone: the `$router` group is released.
    let deadline = Instant::now() + Duration::from_secs(10);
    while h.broker.has_group("orders", "$router").unwrap() {
        assert!(Instant::now() < deadline, "$router group not deleted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (s, e) = h.call("GET", &format!("/api/routes/{id}"), None).await;
    assert_eq!(
        (s, &e["error"]),
        (StatusCode::NOT_FOUND, &json!("not_found"))
    );
    let (s, _) = h.call("DELETE", &format!("/api/routes/{id}"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let actions: Vec<String> = h
        .state
        .audit
        .recent(100)
        .into_iter()
        .map(|e| e.action)
        .collect();
    for a in [
        "route.create",
        "route.update",
        "route.delete",
        "group.delete",
    ] {
        assert!(
            actions.iter().any(|x| x == a),
            "{a} not audited: {actions:?}"
        );
    }
    h.state.router.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_errors_map_to_status_codes() {
    let h = harness().await;
    h.channel("a", 1).await;
    let cases = [
        (
            json!({"name":"x","source":"nope","targets":["a"],"conditions":[]}),
            StatusCode::NOT_FOUND,
        ),
        (
            json!({"name":"x","source":"a","targets":["nope"],"conditions":[]}),
            StatusCode::NOT_FOUND,
        ),
        (
            json!({"name":"x","source":"a","targets":["a"],"conditions":[]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name":"","source":"a","targets":["a"],"conditions":[]}),
            StatusCode::BAD_REQUEST,
        ),
        (json!({"name":"x","source":"a"}), StatusCode::BAD_REQUEST),
    ];
    for (body, status) in cases {
        let (s, v) = h.call("POST", "/api/routes", Some(body.clone())).await;
        assert_eq!(s, status, "{body} → {v}");
    }
    let (s, _) = h
        .call(
            "PATCH",
            "/api/routes/missing",
            Some(json!({"enabled": true})),
        )
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, v) = h
        .call(
            "POST",
            "/api/filter/parse",
            Some(json!({"filter": "a = 1 OR b = 2"})),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(v["message"].as_str().unwrap().contains("at 6"), "{v}");
    assert_eq!(v["position"], 6, "{v}");

    // Builder → text: conditions → canonical DSL; invalid conditions are a 400 (no position).
    let conditions =
        fluxmq_router::filter::parse("type = \"OrderCreated\" AND country = MD").unwrap();
    let (s, v) = h
        .call(
            "POST",
            "/api/filter/render",
            Some(json!({ "conditions": conditions })),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(
        v["rendered"],
        "type = \"OrderCreated\" AND country = \"MD\""
    );
    let (s, v) = h
        .call(
            "POST",
            "/api/filter/render",
            Some(json!({"conditions": [{"field": {"kind": "key"}, "op": "eq"}]})),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    assert!(v.get("position").is_none(), "{v}");
    // An explicit `null` value survives parse → render.
    let (_, parsed) = h
        .call(
            "POST",
            "/api/filter/parse",
            Some(json!({"filter": "note = null"})),
        )
        .await;
    assert_eq!(parsed["conditions"][0]["value"], Value::Null, "{parsed}");
    let (s, v) = h
        .call(
            "POST",
            "/api/filter/render",
            Some(json!({"conditions": parsed["conditions"]})),
        )
        .await;
    assert_eq!(
        (s, &v["rendered"]),
        (StatusCode::OK, &json!("note = null")),
        "{v}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_endpoint() {
    let h = harness().await;
    h.channel("ev", 2).await;
    let msgs: Vec<Value> = (0..300)
        .map(|i| json!({"payload": {"encoding":"utf8","data": format!("{{\"n\":{i}}}")}}))
        .collect();
    let (s, _) = h
        .call(
            "POST",
            "/api/channels/ev/publish",
            Some(json!({"messages": msgs})),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let conditions = fluxmq_router::filter::parse("n >= 250").unwrap();
    let (s, r) = h
        .call(
            "POST",
            "/api/routes/test",
            Some(json!({"source": "ev", "conditions": conditions, "sample_limit": 3})),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{r}");
    assert_eq!(r["scanned"], 300);
    assert_eq!(r["matched"], 50);
    assert_eq!(r["truncated"], false);
    assert_eq!(r["samples"].as_array().unwrap().len(), 3);
    assert_eq!(r["samples"][0]["payload"]["encoding"], "utf8");
    let (s, _) = h
        .call(
            "POST",
            "/api/routes/test",
            Some(json!({"source": "zz", "conditions": []})),
        )
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn message_search_with_filter() {
    let h = harness().await;
    h.channel("orders", 4).await;
    for chunk in 0..6 {
        let msgs: Vec<Value> = (0..500)
            .map(|j| {
                let i = chunk * 500 + j;
                json!({"key": format!("u{}", i % 7), "payload": {"encoding":"utf8",
                    "data": format!("{{\"order_id\":\"{}\",\"country\":\"{}\"}}", 8000 + i,
                        if i % 3 == 0 { "MD" } else { "RO" })}})
            })
            .collect();
        let (s, _) = h
            .call(
                "POST",
                "/api/channels/orders/publish",
                Some(json!({"messages": msgs})),
            )
            .await;
        assert_eq!(s, StatusCode::OK);
    }
    let get =
        |f: &str, extra: &str| format!("/api/channels/orders/messages?filter={}{extra}", enc(f));
    let (s, page) = h.call("GET", &get("order_id = 8842", ""), None).await;
    assert_eq!(s, StatusCode::OK, "{page}");
    assert_eq!(page["messages"].as_array().unwrap().len(), 1);
    assert_eq!(page["has_more"], false);
    assert_eq!(page["scanned"], 3000);

    let (_, page) = h.call("GET", &get("country = MD", "&limit=20"), None).await;
    let msgs = page["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 20);
    assert_eq!(page["has_more"], true);
    let ts: Vec<u64> = msgs
        .iter()
        .map(|m| m["timestamp_ms"].as_u64().unwrap())
        .collect();
    assert!(ts.windows(2).all(|w| w[0] <= w[1]), "ascending");

    let (_, page) = h
        .call("GET", &get("country = MD", "&key=u3&limit=1000"), None)
        .await;
    let msgs = page["messages"].as_array().unwrap();
    // i ≡ 3 (mod 7) and i ≡ 0 (mod 3) ⇔ i ≡ 3 (mod 21): 143 of 3000.
    assert_eq!(msgs.len(), 143);
    assert!(msgs.iter().all(|m| m["key"] == "u3"));

    // Syntax error → 400 with position; blank filter = plain read.
    let (s, e) = h.call("GET", &get("country ~ MD", ""), None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(e["message"].as_str().unwrap().contains("at 8"), "{e}");
    assert_eq!(e["position"], 8, "{e}");
    let (s, page) = h.call("GET", &get("  ", "&limit=5"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(page["messages"].as_array().unwrap().len(), 5);
    let (s, _) = h
        .call("GET", "/api/channels/nope/messages?filter=a%3D1", None)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let _ = &h.broker;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_consumer_group() {
    let h = harness().await;
    h.channel("jobs", 2).await;
    let info = fluxmq_core::SubscriberInfo {
        client_name: "t".into(),
        remote_addr: "test".into(),
        connection_id: 1,
    };
    let sub = h
        .broker
        .subscribe(
            "jobs",
            "workers",
            info,
            8,
            fluxmq_proto::StartFrom::Earliest,
        )
        .unwrap();
    // Active consumers: 409, nothing deleted.
    let (s, e) = h
        .call("DELETE", "/api/channels/jobs/groups/workers", None)
        .await;
    assert_eq!(s, StatusCode::CONFLICT, "{e}");
    assert!(e["message"].as_str().unwrap().contains("active"), "{e}");
    drop(sub);
    // Idle: deleted (the subscription is released asynchronously).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (s, e) = h
            .call("DELETE", "/api/channels/jobs/groups/workers", None)
            .await;
        if s == StatusCode::NO_CONTENT {
            break;
        }
        assert_eq!(s, StatusCode::CONFLICT, "{e}");
        assert!(Instant::now() < deadline, "group never became idle");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!h.broker.has_group("jobs", "workers").unwrap());
    let (s, _) = h
        .call("DELETE", "/api/channels/jobs/groups/workers", None)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = h
        .call("DELETE", "/api/channels/nope/groups/workers", None)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    // An unused reserved group may be deleted.
    let (s, _) = h
        .call("DELETE", "/api/channels/jobs/groups/$router", None)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let audited = h.state.audit.recent(10);
    assert!(
        audited
            .iter()
            .any(|e| e.action == "group.delete" && e.target == "jobs/workers")
    );
}

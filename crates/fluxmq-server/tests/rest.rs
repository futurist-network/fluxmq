//! REST API end-to-end tests over real HTTP.

mod common;

use common::TestServer;
use reqwest::Method;
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread")]
async fn health_and_ui_placeholder() {
    let srv = TestServer::start().await;
    let health = srv.get_json("/api/health").await;
    assert_eq!(health["status"], "ok");
    assert_eq!(health["version"], fluxmq_proto::VERSION);
    assert!(health["node_id"].as_str().is_some_and(|s| !s.is_empty()));
    // Optional build features; the console shows the CPU profile button only with `profiling`.
    let features = if cfg!(feature = "profiling") {
        json!(["profiling"])
    } else {
        json!([])
    };
    assert_eq!(health["features"], features);

    let root = srv.http.get(srv.url("/")).send().await.unwrap();
    assert_eq!(root.status(), 200);
    assert!(root.text().await.unwrap().contains("pnpm build"));

    let unknown = srv.http.get(srv.url("/api/nope")).send().await.unwrap();
    assert_eq!(unknown.status(), 404);
    assert_eq!(unknown.json::<Value>().await.unwrap()["error"], "not_found");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_built_ui_with_spa_fallback() {
    let srv = TestServer::start_with(|c| {
        let dir = c.data_dir.parent().unwrap().join("dist");
        std::fs::create_dir_all(dir.join("assets")).unwrap();
        std::fs::write(dir.join("index.html"), "<html>FluxMQ UI</html>").unwrap();
        std::fs::write(dir.join("assets/app.js"), "console.log(1)").unwrap();
        c.ui = fluxmq_api::UiSource::Dir(dir);
    })
    .await;
    let get = |p: &str| srv.http.get(srv.url(p)).send();
    let r = get("/").await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("FluxMQ UI"));
    assert_eq!(
        get("/assets/app.js").await.unwrap().text().await.unwrap(),
        "console.log(1)"
    );
    let spa = get("/channels/orders/consumers").await.unwrap();
    assert_eq!(spa.status(), 200);
    assert!(spa.text().await.unwrap().contains("FluxMQ UI"));
    assert_eq!(get("/api/missing").await.unwrap().status(), 404);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn channel_crud() {
    let srv = TestServer::start().await;
    let created = srv
        .post_json(
            "/api/channels",
            json!({ "name": "orders", "mode": "keyed", "partitions": 4, "ordering_key": "order_id" }),
        )
        .await;
    assert_eq!(created.status(), 201);
    let cfg: Value = created.json().await.unwrap();
    assert_eq!(cfg["name"], "orders");
    assert_eq!(cfg["mode"], "keyed");
    assert_eq!(cfg["partitions"], 4);
    assert_eq!(cfg["ordering_key"], "order_id");

    let dup = srv
        .post_json(
            "/api/channels",
            json!({ "name": "orders", "mode": "stream" }),
        )
        .await;
    assert_eq!(dup.status(), 409);
    assert_eq!(dup.json::<Value>().await.unwrap()["error"], "conflict");

    let bad = srv
        .post_json(
            "/api/channels",
            json!({ "name": "bad name!", "mode": "stream" }),
        )
        .await;
    assert_eq!(bad.status(), 400);
    let malformed = srv.post_json("/api/channels", json!({ "name": "x" })).await;
    assert_eq!(malformed.status(), 400);
    assert_eq!(
        malformed.json::<Value>().await.unwrap()["error"],
        "bad_request"
    );

    // User channels plus the server's read-only system channels: the `$system.events` feed
    // and (v0.5, created in the background) the `$system.notifications` history.
    let list = srv.get_json("/api/channels").await;
    let names: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .filter(|n| *n != "$system.notifications")
        .collect();
    assert_eq!(names, ["$system.events", "orders"]);
    assert_eq!(srv.get_json("/api/channels/orders").await["partitions"], 4);
    let missing = srv
        .req(Method::GET, "/api/channels/nope")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    let patched = srv
        .req(Method::PATCH, "/api/channels/orders")
        .json(&json!({ "dlq_enabled": false, "retry": { "max_attempts": 3, "backoff_ms": [100], "ack_timeout_ms": 5000 } }))
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), 200);
    let patched: Value = patched.json().await.unwrap();
    assert_eq!(patched["dlq_enabled"], false);
    assert_eq!(patched["retry"]["max_attempts"], 3);

    let deleted = srv
        .req(Method::DELETE, "/api/channels/orders")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    assert_eq!(
        srv.req(Method::DELETE, "/api/channels/orders")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );

    let audit = srv.get_json("/api/audit").await;
    let actions: Vec<&str> = audit
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(
        actions,
        vec!["channel.delete", "channel.update", "channel.create"]
    );
    assert_eq!(audit[0]["actor"], "anonymous");
    assert_eq!(audit[0]["target"], "orders");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_query_detail_replay() {
    let srv = TestServer::start().await;
    srv.post_json(
        "/api/channels",
        json!({ "name": "events", "mode": "stream", "partitions": 1 }),
    )
    .await;

    let resp = srv
        .post_json(
            "/api/channels/events/publish",
            json!({ "messages": [
                { "key": "k1", "headers": { "flux-type": "Created" }, "payload": { "encoding": "utf8", "data": "{\"n\":1}" } },
                { "key": "k2", "payload": { "encoding": "base64", "data": "AP8=" } },
                { "key": "k1", "payload": { "encoding": "utf8", "data": "third" } }
            ] }),
        )
        .await;
    assert_eq!(resp.status(), 200);
    let ids = resp.json::<Value>().await.unwrap()["ids"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(ids.len(), 3);

    let bad = srv
        .post_json(
            "/api/channels/events/publish",
            json!({ "messages": [{ "payload": { "encoding": "base64", "data": "!!" } }] }),
        )
        .await;
    assert_eq!(bad.status(), 400);
    let empty = srv
        .post_json("/api/channels/events/publish", json!({ "messages": [] }))
        .await;
    assert_eq!(empty.status(), 400);
    let nochan = srv
        .post_json(
            "/api/channels/nope/publish",
            json!({ "messages": [{ "payload": { "encoding": "utf8", "data": "x" } }] }),
        )
        .await;
    assert_eq!(nochan.status(), 404);

    let page = srv.get_json("/api/channels/events/messages").await;
    let msgs = page["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(page["has_more"], false);
    assert_eq!(
        msgs[0]["payload"],
        json!({ "encoding": "utf8", "data": "{\"n\":1}" })
    );
    assert_eq!(msgs[0]["headers"]["flux-type"], "Created");
    assert_eq!(
        msgs[1]["payload"],
        json!({ "encoding": "base64", "data": "AP8=" })
    );
    assert_eq!(msgs[1]["size"], 2);

    let by_key = srv.get_json("/api/channels/events/messages?key=k1").await;
    assert_eq!(by_key["messages"].as_array().unwrap().len(), 2);
    let from_offset = srv
        .get_json("/api/channels/events/messages?partition=0&offset=1&limit=1")
        .await;
    assert_eq!(from_offset["messages"].as_array().unwrap().len(), 1);
    assert_eq!(from_offset["messages"][0]["offset"], 1);
    let bad_query = srv
        .req(Method::GET, "/api/channels/events/messages?limit=abc")
        .send()
        .await
        .unwrap();
    assert_eq!(bad_query.status(), 400);

    let (p, o) = (
        ids[2]["partition"].as_u64().unwrap(),
        ids[2]["offset"].as_u64().unwrap(),
    );
    let detail = srv
        .get_json(&format!("/api/channels/events/messages/{p}/{o}"))
        .await;
    assert_eq!(detail["message"]["payload"]["data"], "third");
    assert!(detail["deliveries"].is_object());
    let gone = srv
        .req(Method::GET, "/api/channels/events/messages/0/999")
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404);

    let replay = srv
        .post_json("/api/channels/events/replay", json!({ "ids": [ids[0]] }))
        .await;
    assert_eq!(replay.status(), 200);
    assert_eq!(
        replay.json::<Value>().await.unwrap()["ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let after = srv.get_json("/api/channels/events/messages").await;
    let msgs = after["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 4);
    assert!(msgs[3]["headers"]["flux-replayed-from"].as_str().is_some());

    let reset = srv
        .post_json(
            "/api/channels/events/groups/g1/reset",
            json!({ "to": "earliest" }),
        )
        .await;
    // No consumer ever joined "g1": resetting it must not silently create a phantom group.
    assert_eq!(reset.status(), 404, "reset unknown group");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dlq_validation_export_and_search() {
    let srv = TestServer::start().await;
    srv.post_json(
        "/api/channels",
        json!({ "name": "orders", "mode": "stream" }),
    )
    .await;

    for body in [
        json!({}),
        json!({ "all": true, "channel": "orders" }),
        json!({ "ids": [["orders", 1]], "reason": "x" }),
    ] {
        let r = srv.post_json("/api/dlq/retry", body.clone()).await;
        assert_eq!(r.status(), 400, "{body}");
    }
    let r = srv
        .post_json("/api/dlq/discard", json!({ "all": true }))
        .await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.json::<Value>().await.unwrap()["affected"], 0);

    let export = srv
        .req(Method::GET, "/api/dlq/export")
        .send()
        .await
        .unwrap();
    assert_eq!(export.status(), 200);
    assert_eq!(export.headers()["content-type"], "application/x-ndjson");
    assert_eq!(export.text().await.unwrap(), "");

    let results = srv.get_json("/api/search?q=ORD").await;
    assert_eq!(
        results[0],
        json!({ "kind": "channel", "label": "orders", "link": "/channels/orders" })
    );
    assert!(
        srv.get_json("/api/search?q=zzz")
            .await
            .as_array()
            .unwrap()
            .is_empty()
    );
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn history_and_alerts() {
    let srv = TestServer::start().await;
    let ok = srv
        .req(Method::GET, "/api/history?metric=ingress_rate&range=5m")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    let series: Value = ok.json().await.unwrap();
    assert_eq!(series["metric"], "ingress_rate");
    assert_eq!(series["range"], "5m");

    for q in [
        "metric=bogus&range=5m",
        "metric=ingress_rate&range=2h",
        "range=5m",
    ] {
        let r = srv
            .req(Method::GET, &format!("/api/history?{q}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{q}");
    }
    let r = srv
        .req(Method::GET, "/api/history?metric=channel.nope.lag&range=1h")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert!(srv.get_json("/api/alerts").await.is_object());
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_token_guards_api_but_not_health() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("s3cret".into())).await;
    let anon = |p: &str| srv.http.get(srv.url(p)).send();
    assert_eq!(anon("/api/health").await.unwrap().status(), 200);
    let r = anon("/api/channels").await.unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(r.json::<Value>().await.unwrap()["error"], "unauthorized");
    let wrong = srv
        .http
        .get(srv.url("/api/channels"))
        .bearer_auth("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 401);
    // ?token= is only for the WebSocket.
    assert_eq!(
        anon("/api/channels?token=s3cret").await.unwrap().status(),
        401
    );
    assert_eq!(
        srv.req(Method::GET, "/api/channels")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let created = srv
        .post_json(
            "/api/channels",
            json!({ "name": "secure", "mode": "queue" }),
        )
        .await;
    assert_eq!(created.status(), 201);
    assert_eq!(srv.get_json("/api/audit").await[0]["actor"], "admin");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn audit_log_is_appended_to_data_dir() {
    let srv = TestServer::start().await;
    srv.post_json(
        "/api/channels",
        json!({ "name": "durable", "mode": "stream" }),
    )
    .await;
    let data_dir = srv.dir.path().join("data");
    let audit_file = data_dir.join("audit.log");
    common::eventually(|| async {
        std::fs::read_to_string(&audit_file)
            .ok()
            .filter(|s| s.contains("durable"))
    })
    .await;
    srv.stop().await;
}

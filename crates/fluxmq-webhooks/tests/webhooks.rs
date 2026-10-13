//! Webhook delivery end-to-end: a real broker in a temp dir, a local mock HTTP receiver.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes as AxBytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy};
use fluxmq_meta::{LocalMetaStore, SharedMetaStore};
use fluxmq_proto::PublishMessage;
use fluxmq_proto::model::{ChannelConfig, ChannelMode, RetentionPolicy, RetryPolicy, StartFrom};
use fluxmq_proto::webhooks::{
    CreateWebhookRequest, PayloadFormat, UpdateWebhookRequest, WebhookEnvelope,
};
use fluxmq_webhooks::{
    PrivatePolicy, WebhookConfig, WebhookError, WebhookService, group_name, signature,
};
use parking_lot::Mutex;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------------------------
// Mock receiver

#[derive(Clone, Debug)]
struct Req {
    at: Instant,
    headers: HeaderMap,
    body: Bytes,
}

impl Req {
    fn h(&self, name: &str) -> String {
        self.headers
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default()
    }
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
    fn envelope(&self) -> WebhookEnvelope {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// Decides the response: (status, extra headers, delay before answering).
type Script = dyn Fn(&Req, usize) -> (u16, Vec<(&'static str, String)>, Duration) + Send + Sync;

#[derive(Clone)]
struct Mock {
    reqs: Arc<Mutex<Vec<Req>>>,
    script: Arc<Mutex<Arc<Script>>>,
}

async fn handler(State(m): State<Mock>, headers: HeaderMap, body: AxBytes) -> Response {
    let req = Req {
        at: Instant::now(),
        headers,
        body: Bytes::from(body.to_vec()),
    };
    let n = {
        let mut r = m.reqs.lock();
        r.push(req.clone());
        r.len()
    };
    let script = m.script.lock().clone();
    let (status, extra, delay) = script(&req, n);
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    let mut resp = (StatusCode::from_u16(status).unwrap(), "ok").into_response();
    for (k, v) in extra {
        resp.headers_mut().insert(k, v.parse().unwrap());
    }
    resp
}

impl Mock {
    async fn start() -> (Mock, String) {
        let m = Mock {
            reqs: Arc::new(Mutex::new(Vec::new())),
            script: Arc::new(Mutex::new(Arc::new(|_: &Req, _| {
                (200, vec![], Duration::ZERO)
            }))),
        };
        let app = axum::Router::new()
            .route("/hook", axum::routing::post(handler))
            .with_state(m.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (m, format!("http://{addr}/hook"))
    }

    fn script(
        &self,
        f: impl Fn(&Req, usize) -> (u16, Vec<(&'static str, String)>, Duration) + Send + Sync + 'static,
    ) {
        *self.script.lock() = Arc::new(f);
    }

    fn reqs(&self) -> Vec<Req> {
        self.reqs.lock().clone()
    }

    fn count(&self) -> usize {
        self.reqs.lock().len()
    }
}

fn ok() -> (u16, Vec<(&'static str, String)>, Duration) {
    (200, vec![], Duration::ZERO)
}

// ---------------------------------------------------------------------------------------------
// Broker helpers

async fn open_broker(dir: &Path) -> Broker {
    let mut c = BrokerConfig::new(dir);
    c.fsync = FsyncPolicy::OsBuffered;
    c.offset_checkpoint_interval = Duration::from_millis(20);
    Broker::open(c).await.unwrap()
}

fn channel(b: &Broker, name: &str, partitions: u32, backoff: Vec<u64>, max_attempts: u32) {
    let mut c = ChannelConfig::new(name, ChannelMode::Stream);
    c.partitions = partitions;
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: None,
    };
    c.retry = RetryPolicy {
        max_attempts,
        backoff_ms: backoff,
        ack_timeout_ms: 5_000,
    };
    b.create_channel(c).unwrap();
}

async fn publish(b: &Broker, ch: &str, msgs: &[(Option<&str>, Value)]) {
    let pms = msgs
        .iter()
        .map(|(k, v)| PublishMessage {
            key: k.map(str::to_string),
            headers: BTreeMap::from([("source".to_string(), "test".to_string())]),
            payload: Bytes::from(v.to_string()),
        })
        .collect();
    b.publish(ch, pms).await.unwrap();
}

fn lag(b: &Broker, ch: &str, group: &str) -> Option<u64> {
    b.stats()
        .channels
        .into_iter()
        .find(|c| c.config.name == ch)?
        .groups
        .into_iter()
        .find(|g| g.name == group)
        .map(|g| g.lag)
}

fn dlq_reasons(b: &Broker, ch: &str) -> Vec<String> {
    b.dlq_list(Some(ch), None, None, 100)
        .0
        .into_iter()
        .map(|d| d.reason)
        .collect()
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn req(name: &str, ch: &str, url: &str) -> CreateWebhookRequest {
    CreateWebhookRequest {
        name: name.into(),
        channel: ch.into(),
        url: url.into(),
        enabled: true,
        headers: BTreeMap::new(),
        secret: None,
        filter: vec![],
        format: PayloadFormat::Envelope,
        timeout_ms: Some(1_000),
        max_in_flight: None,
        batch_max: None,
        batch_linger_ms: None,
        retry_on_4xx: None,
        pause_after_failures: None,
        start: Some(StartFrom::Earliest),
    }
}

struct Env {
    broker: Broker,
    meta: SharedMetaStore,
    svc: WebhookService,
    dir: tempfile::TempDir,
}

async fn env_with(policy: PrivatePolicy) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let broker = open_broker(&dir.path().join("data")).await;
    let meta: SharedMetaStore =
        Arc::new(LocalMetaStore::open(dir.path().join("meta.json")).unwrap());
    let svc = WebhookService::start(
        broker.clone(),
        meta.clone(),
        WebhookConfig { private: policy },
    )
    .await
    .unwrap();
    Env {
        broker,
        meta,
        svc,
        dir,
    }
}

async fn env() -> Env {
    env_with(PrivatePolicy::Fixed(true)).await
}

// ---------------------------------------------------------------------------------------------
// Tests

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_envelopes_and_acks_2xx() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "orders", 2, vec![30], 3);
    let mut r = req("orders-hook", "orders", &url);
    r.headers
        .insert("Authorization".into(), "Bearer abc".into());
    let sub = e.svc.create(r).await.unwrap();
    assert_eq!(sub.headers["Authorization"], fluxmq_proto::notify::REDACTED);
    publish(
        &e.broker,
        "orders",
        &[
            (Some("k1"), json!({"n": 1})),
            (None, json!({"n": 2})),
            (None, json!({"n": 3})),
        ],
    )
    .await;
    eventually("3 deliveries", || mock.count() == 3).await;
    let group = group_name("orders-hook");
    eventually("lag 0", || lag(&e.broker, "orders", &group) == Some(0)).await;
    let reqs = mock.reqs();
    let r0 = reqs.iter().find(|r| r.envelope().key.is_some()).unwrap();
    let env = r0.envelope();
    assert_eq!(env.subscription, "orders-hook");
    assert_eq!(env.channel, "orders");
    assert_eq!(env.attempt, 1);
    assert_eq!(env.headers["source"], "test");
    assert_eq!(
        env.delivery_id,
        format!("orders/{}/{}", env.partition, env.offset)
    );
    assert_eq!(r0.h("x-fluxmq-delivery-id"), env.delivery_id);
    assert_eq!(r0.h("x-fluxmq-attempt"), "1");
    assert_eq!(r0.h("authorization"), "Bearer abc");
    assert_eq!(r0.h("content-type"), "application/json");
    assert!(r0.h("user-agent").starts_with("FluxMQ-Webhooks/"));
    assert_eq!(
        env.payload,
        fluxmq_proto::api::JsonPayload::Utf8(json!({"n": 1}).to_string())
    );
    let st = e.svc.stats();
    eventually("stats", || e.svc.stats()[0].delivered == 3).await;
    assert_eq!(st[0].id, sub.id);
    let st = &e.svc.stats()[0];
    assert_eq!(st.last_status, Some(200));
    assert_eq!(st.in_flight, 0);
    assert!(st.latency_p99_ms > 0.0);
    assert!(dlq_reasons(&e.broker, "orders").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_5xx_until_success() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 5);
    mock.script(|_, n| {
        if n < 3 {
            (500, vec![], Duration::ZERO)
        } else {
            ok()
        }
    });
    e.svc.create(req("h", "c", &url)).await.unwrap();
    publish(&e.broker, "c", &[(None, json!({"x": 1}))]).await;
    eventually("3 attempts", || mock.count() == 3).await;
    eventually("lag 0", || lag(&e.broker, "c", "$webhook:h") == Some(0)).await;
    let reqs = mock.reqs();
    let attempts: Vec<String> = reqs.iter().map(|r| r.h("x-fluxmq-attempt")).collect();
    assert_eq!(attempts, ["1", "2", "3"]);
    let ids: Vec<String> = reqs.iter().map(|r| r.h("x-fluxmq-delivery-id")).collect();
    assert!(ids.iter().all(|i| i == "c/0/0"), "{ids:?}");
    assert_eq!(reqs[2].envelope().attempt, 3);
    assert!(dlq_reasons(&e.broker, "c").is_empty());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(mock.count(), 3);
    let st = &e.svc.stats()[0];
    assert_eq!((st.delivered, st.failed), (1, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_errors_dead_letter_immediately() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 5);
    mock.script(|_, _| (400, vec![], Duration::ZERO));
    e.svc.create(req("h", "c", &url)).await.unwrap();
    publish(&e.broker, "c", &[(None, json!(1))]).await;
    eventually("dead letter", || !dlq_reasons(&e.broker, "c").is_empty()).await;
    assert_eq!(dlq_reasons(&e.broker, "c"), ["HTTP 400"]);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(mock.count(), 1, "no retry for 400");
    let st = &e.svc.stats()[0];
    assert_eq!(st.last_status, Some(400));
    assert!(st.last_error.as_deref().unwrap().starts_with("HTTP 400"));

    // retry_on_4xx: retried, dead-lettered after max attempts with the same reason format.
    let id = e.svc.list()[0].id.clone();
    e.svc
        .update(
            &id,
            UpdateWebhookRequest {
                retry_on_4xx: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    mock.script(|_, _| (404, vec![], Duration::ZERO));
    publish(&e.broker, "c", &[(None, json!(2))]).await;
    eventually("second dead letter", || {
        dlq_reasons(&e.broker, "c").len() == 2
    })
    .await;
    assert_eq!(mock.count(), 1 + 5);
    assert!(dlq_reasons(&e.broker, "c").contains(&"HTTP 404".to_string()));

    // 3xx: not followed, not retried.
    e.svc
        .update(
            &id,
            UpdateWebhookRequest {
                retry_on_4xx: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    mock.script(|_, _| {
        (
            302,
            vec![("location", "http://example.com/".into())],
            Duration::ZERO,
        )
    });
    publish(&e.broker, "c", &[(None, json!(3))]).await;
    eventually("third dead letter", || {
        dlq_reasons(&e.broker, "c").len() == 3
    })
    .await;
    assert!(dlq_reasons(&e.broker, "c").contains(&"HTTP 302".to_string()));
    assert_eq!(mock.count(), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn honours_retry_after() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    // Backoff cap 3 s; first retry backoff 30 ms ⇒ the worker holds ~970 ms for Retry-After: 1.
    channel(&e.broker, "c", 1, vec![30, 3_000], 5);
    mock.script(|_, n| {
        if n == 1 {
            (429, vec![("retry-after", "1".into())], Duration::ZERO)
        } else {
            ok()
        }
    });
    e.svc.create(req("h", "c", &url)).await.unwrap();
    publish(&e.broker, "c", &[(None, json!(1))]).await;
    eventually("2 attempts", || mock.count() == 2).await;
    let r = mock.reqs();
    let gap = r[1].at - r[0].at;
    assert!(gap >= Duration::from_millis(950), "{gap:?}");
    assert!(gap < Duration::from_millis(2_500), "{gap:?}");
    assert_eq!(r[1].h("x-fluxmq-attempt"), "2");

    // Capped at the channel's max backoff: Retry-After 3600 waits ~3 s, not an hour.
    mock.reqs.lock().clear();
    mock.script(|_, n| {
        if n == 1 {
            (503, vec![("retry-after", "3600".into())], Duration::ZERO)
        } else {
            ok()
        }
    });
    publish(&e.broker, "c", &[(None, json!(2))]).await;
    let t0 = Instant::now();
    eventually("1 attempt", || mock.count() == 1).await;
    let deadline = Duration::from_secs(8);
    while mock.count() < 2 {
        assert!(t0.elapsed() < deadline, "capped retry did not happen");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let r = mock.reqs();
    let gap = r[1].at - r[0].at;
    assert!(
        gap >= Duration::from_millis(2_900) && gap < Duration::from_millis(4_500),
        "{gap:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeouts_are_retried() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 5);
    mock.script(|_, n| {
        if n == 1 {
            (200, vec![], Duration::from_millis(800))
        } else {
            ok()
        }
    });
    let mut r = req("h", "c", &url);
    r.timeout_ms = Some(200);
    e.svc.create(r).await.unwrap();
    publish(&e.broker, "c", &[(None, json!(1))]).await;
    eventually("lag 0", || lag(&e.broker, "c", "$webhook:h") == Some(0)).await;
    assert_eq!(mock.count(), 2);
    assert!(dlq_reasons(&e.broker, "c").is_empty());
    let st = &e.svc.stats()[0];
    assert_eq!((st.delivered, st.failed), (1, 1));
    assert_eq!(st.last_status, Some(200));

    // Connection refused → retried, then dead-lettered with that reason.
    let id = e.svc.list()[0].id.clone();
    let dead = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    e.svc
        .update(
            &id,
            UpdateWebhookRequest {
                url: Some(format!("http://{dead}/hook")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    publish(&e.broker, "c", &[(None, json!(2))]).await;
    eventually("dead letter", || !dlq_reasons(&e.broker, "c").is_empty()).await;
    assert_eq!(dlq_reasons(&e.broker, "c"), ["connection refused"]);
    let st = e.svc.stats().remove(0);
    assert_eq!(st.last_error.as_deref(), Some("connection refused"));
    assert!(
        st.last_error_detail
            .is_some_and(|d| d.len() > "connection refused".len())
    );
}

/// The SSRF policy is re-checked at delivery time: a target that became blocked is not
/// retried — it goes straight to the DLQ with reason `blocked by policy`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_by_policy_dead_letters_without_retries() {
    let allow = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let a = allow.clone();
    let e = env_with(PrivatePolicy::Dynamic(Arc::new(move || {
        a.load(std::sync::atomic::Ordering::Relaxed)
    })))
    .await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 5);
    e.svc.create(req("h", "c", &url)).await.unwrap();
    allow.store(false, std::sync::atomic::Ordering::Relaxed);
    publish(&e.broker, "c", &[(None, json!(1))]).await;
    eventually("dead letter", || !dlq_reasons(&e.broker, "c").is_empty()).await;
    assert_eq!(dlq_reasons(&e.broker, "c"), ["blocked by policy"]);
    assert!(mock.reqs().is_empty(), "nothing reached the blocked target");
    let st = e.svc.stats().remove(0);
    assert_eq!(st.failed, 1, "one attempt only: {st:?}");
    assert_eq!(st.last_error.as_deref(), Some("blocked by policy"));
    assert!(st.last_error_detail.unwrap().contains("not allowed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signature_verifies() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    let mut r = req("h", "c", &url);
    r.secret = Some("whsec_test".into());
    let sub = e.svc.create(r).await.unwrap();
    assert_eq!(sub.secret.as_deref(), Some(fluxmq_proto::notify::REDACTED));
    publish(&e.broker, "c", &[(None, json!({"a": 1}))]).await;
    eventually("delivery", || mock.count() == 1).await;
    let r = &mock.reqs()[0];
    let sig = r.h("x-fluxmq-signature");
    assert!(sig.starts_with("t="), "{sig}");
    let now = fluxmq_proto::now_ms() / 1000;
    assert_eq!(
        signature::verify("whsec_test", &sig, &r.body, 300, now),
        Ok(())
    );
    assert!(signature::verify("wrong", &sig, &r.body, 300, now).is_err());
    let mut tampered = r.body.to_vec();
    tampered.push(b' ');
    assert!(signature::verify("whsec_test", &sig, &tampered, 300, now).is_err());

    // Secret removed with "" → no signature header.
    e.svc
        .update(
            &sub.id,
            UpdateWebhookRequest {
                secret: Some(String::new()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    publish(&e.broker, "c", &[(None, json!({"a": 2}))]).await;
    eventually("delivery 2", || mock.count() == 2).await;
    assert_eq!(mock.reqs()[1].h("x-fluxmq-signature"), "");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_into_json_arrays() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    let mut r = req("h", "c", &url);
    r.batch_max = Some(10);
    r.batch_linger_ms = Some(300);
    r.max_in_flight = Some(1);
    e.svc.create(r).await.unwrap();
    for i in 0..5 {
        publish(&e.broker, "c", &[(None, json!({"i": i}))]).await;
    }
    eventually("lag 0", || lag(&e.broker, "c", "$webhook:h") == Some(0)).await;
    let reqs = mock.reqs();
    assert_eq!(reqs.len(), 1, "one batched request");
    let arr: Vec<WebhookEnvelope> = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(arr.len(), 5);
    let offsets: Vec<u64> = arr.iter().map(|e| e.offset).collect();
    assert_eq!(offsets, [0, 1, 2, 3, 4]);
    assert_eq!(reqs[0].h("x-fluxmq-batch-size"), "5");
    assert_eq!(reqs[0].h("x-fluxmq-delivery-id"), "c/0/0");
    assert_eq!(e.svc.stats()[0].delivered, 5);

    // A full batch is sent without waiting for the linger.
    mock.reqs.lock().clear();
    let msgs: Vec<(Option<&str>, Value)> = (0..25).map(|i| (None, json!(i))).collect();
    publish(&e.broker, "c", &msgs).await;
    eventually("lag 0 again", || {
        lag(&e.broker, "c", "$webhook:h") == Some(0)
    })
    .await;
    let sizes: Vec<usize> = mock
        .reqs()
        .iter()
        .map(|r| r.json().as_array().unwrap().len())
        .collect();
    assert_eq!(sizes.iter().sum::<usize>(), 25);
    assert!(sizes.iter().all(|s| *s <= 10), "{sizes:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_format_sends_payload_and_metadata_headers() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    let mut r = req("h", "c", &url);
    r.format = PayloadFormat::Raw;
    r.batch_max = Some(5);
    assert!(matches!(
        e.svc.create(r.clone()).await,
        Err(WebhookError::Invalid(_))
    ));
    r.batch_max = None;
    e.svc.create(r).await.unwrap();
    publish(&e.broker, "c", &[(Some("user-7"), json!({"raw": true}))]).await;
    eventually("delivery", || mock.count() == 1).await;
    let r = &mock.reqs()[0];
    assert_eq!(&r.body[..], json!({"raw": true}).to_string().as_bytes());
    assert_eq!(r.h("x-fluxmq-channel"), "c");
    assert_eq!(r.h("x-fluxmq-partition"), "0");
    assert_eq!(r.h("x-fluxmq-offset"), "0");
    assert_eq!(r.h("x-fluxmq-key"), "user-7");
    assert!(r.h("x-fluxmq-timestamp").parse::<u64>().unwrap() > 0);
    assert_eq!(r.h("x-fluxmq-header-source"), "test");
    assert_eq!(r.h("x-fluxmq-delivery-id"), "c/0/0");
    assert_eq!(r.h("x-fluxmq-attempt"), "1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_skips_non_matching() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    let mut r = req("h", "c", &url);
    r.filter = fluxmq_router::filter::parse("type == 'a'").unwrap();
    e.svc.create(r).await.unwrap();
    publish(
        &e.broker,
        "c",
        &[
            (None, json!({"type": "a", "n": 1})),
            (None, json!({"type": "b", "n": 2})),
            (None, json!({"type": "a", "n": 3})),
        ],
    )
    .await;
    eventually("lag 0", || lag(&e.broker, "c", "$webhook:h") == Some(0)).await;
    eventually("2 deliveries", || mock.count() == 2).await;
    let st = &e.svc.stats()[0];
    assert_eq!((st.delivered, st.skipped_by_filter), (2, 1));
    for r in mock.reqs() {
        assert_ne!(r.envelope().offset, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_one_keeps_partition_order_under_retries() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 2, vec![50, 100], 6);
    // Every message's first two attempts fail for keys k0 offset 1 (and one of k1).
    mock.script(|r, _| {
        let env: WebhookEnvelope = serde_json::from_slice(&r.body).unwrap();
        if env.offset % 3 == 1 && env.attempt < 3 {
            (503, vec![], Duration::ZERO)
        } else {
            ok()
        }
    });
    let mut r = req("h", "c", &url);
    r.max_in_flight = Some(1);
    e.svc.create(r).await.unwrap();
    let mut msgs = Vec::new();
    for i in 0..8 {
        msgs.push((Some("k0"), json!({"k": 0, "i": i})));
        msgs.push((Some("k1"), json!({"k": 1, "i": i})));
    }
    publish(&e.broker, "c", &msgs).await;
    eventually("lag 0", || lag(&e.broker, "c", "$webhook:h") == Some(0)).await;
    // Successful deliveries per partition must arrive in offset order, and no later offset
    // may be attempted before an earlier one succeeded.
    let mut last_ok: BTreeMap<u32, i64> = BTreeMap::new();
    let mut seen: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for r in mock.reqs() {
        let env = r.envelope();
        let prev = *last_ok.get(&env.partition).unwrap_or(&-1);
        assert!(
            env.offset as i64 == prev + 1,
            "partition {} offset {} attempted after {prev} succeeded",
            env.partition,
            env.offset
        );
        let failed = env.offset % 3 == 1 && env.attempt < 3;
        if !failed {
            last_ok.insert(env.partition, env.offset as i64);
            seen.entry(env.partition).or_default().push(env.offset);
        }
    }
    assert_eq!(seen.values().map(Vec::len).sum::<usize>(), 16);
    assert!(dlq_reasons(&e.broker, "c").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disable_pauses_enable_resumes_delete_releases_group() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    let sub = e.svc.create(req("h", "c", &url)).await.unwrap();
    publish(&e.broker, "c", &[(None, json!(1))]).await;
    eventually("delivery", || mock.count() == 1).await;
    e.svc
        .update(
            &sub.id,
            UpdateWebhookRequest {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    eventually("worker stopped", || e.svc.active().is_empty()).await;
    publish(&e.broker, "c", &[(None, json!(2))]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(mock.count(), 1, "disabled: nothing delivered");
    assert!(e.broker.has_group("c", "$webhook:h").unwrap(), "group kept");
    assert_eq!(lag(&e.broker, "c", "$webhook:h"), Some(1));
    assert!(e.svc.uses_group("c", "$webhook:h"));

    e.svc
        .update(
            &sub.id,
            UpdateWebhookRequest {
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    eventually("resumed", || mock.count() == 2).await;
    assert_eq!(mock.reqs()[1].envelope().offset, 1);

    e.svc.delete(&sub.id).await.unwrap();
    assert!(!e.svc.uses_group("c", "$webhook:h"));
    assert!(e.svc.get(&sub.id).is_none());
    // The API releases the group once the worker let go (here: directly).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match e.broker.delete_group("c", "$webhook:h") {
            Ok(()) => break,
            Err(err) => {
                assert!(Instant::now() < deadline, "group not released: {err}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    assert!(!e.broker.has_group("c", "$webhook:h").unwrap());
    assert!(e.svc.active().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_reload_from_meta() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    // Another node (or a restore) writes the definition straight into the meta store.
    let mut sub = fluxmq_webhooks::validate::from_create(req("remote", "c", &url)).unwrap();
    sub.id = "remote0001".into();
    e.meta
        .put(
            "webhooks/remote0001",
            serde_json::to_value(&sub).unwrap(),
            None,
        )
        .await
        .unwrap();
    eventually("worker from meta", || e.svc.active() == ["remote0001"]).await;
    assert_eq!(e.svc.get("remote0001").unwrap().name, "remote");
    publish(&e.broker, "c", &[(None, json!(1))]).await;
    eventually("delivery", || mock.count() == 1).await;

    // Definition change in meta → worker restarts with it.
    sub.headers.insert("X-Rev".into(), "2".into());
    e.meta
        .put(
            "webhooks/remote0001",
            serde_json::to_value(&sub).unwrap(),
            None,
        )
        .await
        .unwrap();
    eventually("reloaded", || {
        e.svc
            .get("remote0001")
            .is_some_and(|s| s.headers.contains_key("X-Rev"))
    })
    .await;
    publish(&e.broker, "c", &[(None, json!(2))]).await;
    eventually("delivery 2", || mock.count() == 2).await;
    assert_eq!(mock.reqs()[1].h("x-rev"), "2");

    e.meta.delete("webhooks/remote0001", None).await.unwrap();
    eventually("removed", || {
        e.svc.active().is_empty() && e.svc.list().is_empty()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validation_and_ssrf_guard() {
    let e = env_with(PrivatePolicy::Fixed(false)).await;
    channel(&e.broker, "c", 1, vec![30], 3);
    let bad = |url: &str| req("h", "c", url);
    for url in [
        "http://127.0.0.1:9/hook",
        "http://localhost:9/hook",
        "http://[::1]:9/hook",
        "http://10.0.0.1/hook",
        "http://169.254.169.254/latest/meta-data",
    ] {
        match e.svc.create(bad(url)).await {
            Err(WebhookError::Invalid(m)) => assert!(m.contains("private"), "{url}: {m}"),
            other => panic!("{url}: {other:?}", other = other.map(|s| s.id)),
        }
    }
    // Test endpoint is guarded too.
    assert!(
        e.svc
            .test_request(bad("http://127.0.0.1:9/"))
            .await
            .is_err()
    );

    let check = |r: CreateWebhookRequest| {
        let svc = e.svc.clone();
        async move { svc.create(r).await.map(|s| s.id) }
    };
    let mut r = bad("ftp://example.com/");
    assert!(matches!(
        check(r.clone()).await,
        Err(WebhookError::Invalid(_))
    ));
    r.url = "https://example.com/hook".into();
    r.channel = "missing".into();
    assert!(matches!(
        check(r.clone()).await,
        Err(WebhookError::ChannelNotFound(_))
    ));
    r.channel = "c".into();
    r.timeout_ms = Some(50);
    assert!(matches!(
        check(r.clone()).await,
        Err(WebhookError::Invalid(_))
    ));
    r.timeout_ms = Some(5_000); // == channel ack timeout
    assert!(matches!(
        check(r.clone()).await,
        Err(WebhookError::Invalid(_))
    ));
    r.timeout_ms = Some(1_000);
    r.max_in_flight = Some(65);
    assert!(matches!(
        check(r.clone()).await,
        Err(WebhookError::Invalid(_))
    ));
    r.max_in_flight = Some(1);
    r.batch_max = Some(501);
    assert!(matches!(
        check(r.clone()).await,
        Err(WebhookError::Invalid(_))
    ));
    r.batch_max = Some(1);
    r.name = "Bad Name".into();
    assert!(matches!(
        check(r.clone()).await,
        Err(WebhookError::Invalid(_))
    ));

    // Private allowed explicitly; `$`-prefixed channels may be subscribed.
    let open = env_with(PrivatePolicy::Fixed(true)).await;
    channel(&open.broker, "$system.events", 1, vec![30], 3);
    let ok = open
        .svc
        .create(req("sys", "$system.events", "http://127.0.0.1:9/hook"))
        .await
        .unwrap();
    assert!(open.svc.uses_group("$system.events", "$webhook:sys"));
    // Names are unique.
    assert!(matches!(
        open.svc
            .create(req("sys", "$system.events", "http://127.0.0.1:9/x"))
            .await,
        Err(WebhookError::Conflict(_))
    ));
    open.svc.delete(&ok.id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_delivery_reports_outcome() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    let mut r = req("h", "c", &url);
    r.secret = Some("k".into());
    let sub = e.svc.create(r.clone()).await.unwrap();
    let res = e.svc.test(&sub.id, None).await.unwrap();
    assert!(res.ok, "{res:?}");
    assert_eq!(res.status, Some(200));
    assert_eq!(res.response_snippet.as_deref(), Some("ok"));
    let req0 = &mock.reqs()[0];
    assert!(req0.h("x-fluxmq-delivery-id").starts_with("test/"));
    let env = req0.envelope();
    assert!(env.delivery_id.starts_with("test/"));
    let now = fluxmq_proto::now_ms() / 1000;
    assert!(signature::verify("k", &req0.h("x-fluxmq-signature"), &req0.body, 60, now).is_ok());

    // Draft merged over the stored subscription: REDACTED keeps the stored secret, the edited
    // header is sent, nothing is persisted.
    let draft = fluxmq_proto::webhooks::UpdateWebhookRequest {
        secret: Some(fluxmq_proto::notify::REDACTED.into()),
        headers: Some(std::collections::BTreeMap::from([(
            "x-draft".to_string(),
            "1".to_string(),
        )])),
        ..Default::default()
    };
    let res = e.svc.test(&sub.id, Some(draft)).await.unwrap();
    assert!(res.ok, "{res:?}");
    let req1 = &mock.reqs()[1];
    assert_eq!(req1.h("x-draft"), "1");
    assert!(signature::verify("k", &req1.h("x-fluxmq-signature"), &req1.body, 60, now).is_ok());
    assert!(
        e.svc.get(&sub.id).unwrap().headers.is_empty(),
        "draft not persisted"
    );

    mock.script(|_, _| (500, vec![], Duration::ZERO));
    r.name = "unsaved".into();
    let res = e.svc.test_request(r).await.unwrap();
    assert!(!res.ok);
    assert_eq!(res.status, Some(500));
    assert_eq!(res.error.as_deref(), Some("HTTP 500"));
    assert_eq!(e.svc.list().len(), 1, "test_request persists nothing");
    assert!(matches!(
        e.svc.test("nope", None).await,
        Err(WebhookError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumes_from_committed_offsets_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (mock, url) = Mock::start().await;
    let data = dir.path().join("data");
    let meta_path = dir.path().join("meta.json");
    {
        let broker = open_broker(&data).await;
        let meta: SharedMetaStore = Arc::new(LocalMetaStore::open(&meta_path).unwrap());
        let svc = WebhookService::start(broker.clone(), meta, WebhookConfig::default())
            .await
            .unwrap();
        channel(&broker, "c", 2, vec![30], 3);
        svc.create(req("h", "c", &url)).await.unwrap();
        let msgs: Vec<(Option<&str>, Value)> = (0..6).map(|i| (None, json!(i))).collect();
        publish(&broker, "c", &msgs).await;
        eventually("lag 0", || lag(&broker, "c", "$webhook:h") == Some(0)).await;
        tokio::time::sleep(Duration::from_millis(100)).await; // offset checkpoint
        svc.shutdown(Duration::from_secs(2)).await;
        broker.shutdown().await;
    }
    assert_eq!(mock.count(), 6);
    let broker = open_broker(&data).await;
    let meta: SharedMetaStore = Arc::new(LocalMetaStore::open(&meta_path).unwrap());
    let svc = WebhookService::start(broker.clone(), meta, WebhookConfig::default())
        .await
        .unwrap();
    assert_eq!(svc.list().len(), 1);
    eventually("worker resumed", || svc.active().len() == 1).await;
    let msgs: Vec<(Option<&str>, Value)> = (6..9).map(|i| (None, json!(i))).collect();
    publish(&broker, "c", &msgs).await;
    eventually("lag 0 after restart", || {
        lag(&broker, "c", "$webhook:h") == Some(0)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let payloads: Vec<String> = mock
        .reqs()
        .iter()
        .skip(6)
        .map(|r| match r.envelope().payload {
            fluxmq_proto::api::JsonPayload::Utf8(s) => s,
            other => panic!("{other:?}"),
        })
        .collect();
    let mut sorted = payloads.clone();
    sorted.sort();
    assert_eq!(sorted, ["6", "7", "8"], "only new messages after restart");
    svc.shutdown(Duration::from_secs(2)).await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_finishes_in_flight() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![30], 3);
    mock.script(|_, _| (200, vec![], Duration::from_millis(300)));
    e.svc.create(req("h", "c", &url)).await.unwrap();
    publish(&e.broker, "c", &[(None, json!(1))]).await;
    eventually("request started", || mock.count() == 1).await;
    e.svc.shutdown(Duration::from_secs(3)).await;
    // The in-flight request completed and was acked before the worker let go.
    assert_eq!(lag(&e.broker, "c", "$webhook:h"), Some(0));
    assert_eq!(e.svc.stats()[0].delivered, 1);
    let _ = &e.dir;
}

/// Throughput against a local receiver that answers 200 immediately. Run with
/// `cargo test -p fluxmq-webhooks --release --test webhooks bench -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_deliveries_per_second() {
    const N: usize = 20_000;
    let e = env().await;
    let (mock, url) = Mock::start().await;
    for (i, (in_flight, batch)) in [(1u32, 1u32), (16, 1), (64, 1), (4, 100)]
        .into_iter()
        .enumerate()
    {
        let ch = format!("bench{i}");
        channel(&e.broker, &ch, 8, vec![100], 3);
        let msgs: Vec<(Option<&str>, Value)> = (0..N)
            .map(|n| (None, json!({"n": n, "pad": "x".repeat(200)})))
            .collect();
        for chunk in msgs.chunks(1000) {
            publish(&e.broker, &ch, chunk).await;
        }
        mock.reqs.lock().clear();
        let mut r = req(&format!("b{i}"), &ch, &url);
        r.max_in_flight = Some(in_flight);
        r.batch_max = Some(batch);
        r.batch_linger_ms = Some(if batch > 1 { 5 } else { 0 });
        let t0 = Instant::now();
        e.svc.create(r).await.unwrap();
        let group = format!("$webhook:b{i}");
        let deadline = Instant::now() + Duration::from_secs(300);
        while lag(&e.broker, &ch, &group) != Some(0) {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let secs = t0.elapsed().as_secs_f64();
        let st = e
            .svc
            .stats()
            .into_iter()
            .find(|s| s.delivered > 0 && s.id == e.svc.list()[i].id)
            .unwrap();
        println!(
            "in_flight={in_flight:>2} batch={batch:>3}: {N} msgs in {secs:.2}s = {:>8.0} msg/s, {} requests, p50 {:.2} ms, p99 {:.2} ms",
            N as f64 / secs,
            mock.count(),
            st.latency_p50_ms,
            st.latency_p99_ms
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn circuit_breaker_pauses_without_burning_attempts() {
    use fluxmq_proto::webhooks::AttemptOutcome;
    use std::sync::atomic::{AtomicBool, Ordering};

    let e = env().await;
    let (mock, url) = Mock::start().await;
    // 3 attempts: without the breaker the first message would be dead-lettered while down.
    channel(&e.broker, "c", 1, vec![30], 3);
    let down = Arc::new(AtomicBool::new(true));
    let d = down.clone();
    mock.script(move |r, _| {
        let probe = r.h("x-fluxmq-delivery-id").starts_with("probe/");
        if d.load(Ordering::SeqCst) {
            (503, vec![], Duration::ZERO)
        } else if probe {
            // A receiver rejecting the synthetic body is still up: the probe succeeds.
            (422, vec![], Duration::ZERO)
        } else {
            ok()
        }
    });
    let mut r = req("h", "c", &url);
    r.max_in_flight = Some(1);
    r.pause_after_failures = Some(2);
    let sub = e.svc.create(r).await.unwrap();
    assert_eq!(sub.pause_after_failures, 2);
    publish(
        &e.broker,
        "c",
        &(0..5).map(|i| (None, json!(i))).collect::<Vec<_>>(),
    )
    .await;

    eventually("breaker open", || e.svc.stats()[0].breaker.is_some()).await;
    let real = |m: &Mock| {
        m.reqs()
            .iter()
            .filter(|r| !r.h("x-fluxmq-delivery-id").starts_with("probe/"))
            .count()
    };
    assert_eq!(real(&mock), 2, "two failed attempts opened it");
    let b = e.svc.stats()[0].breaker.clone().unwrap();
    assert_eq!((b.failures, b.nodes), (2, 1));
    // While open: probes only, no message burns an attempt, nothing is dead-lettered.
    eventually("a failed probe", || mock.count() > 2).await;
    let probe = mock.reqs()[2].clone();
    assert!(probe.envelope().delivery_id.starts_with("probe/"));
    assert_eq!(probe.envelope().headers["flux-test"], "true");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(real(&mock), 2);
    assert!(dlq_reasons(&e.broker, "c").is_empty());
    assert_eq!(lag(&e.broker, "c", "$webhook:h"), Some(5));

    // The endpoint is back: the next probe closes the breaker and every message is delivered.
    down.store(false, Ordering::SeqCst);
    eventually("delivered", || lag(&e.broker, "c", "$webhook:h") == Some(0)).await;
    assert!(e.svc.stats()[0].breaker.is_none());
    assert!(dlq_reasons(&e.broker, "c").is_empty());
    let attempts: Vec<String> = mock
        .reqs()
        .iter()
        .filter(|r| r.h("x-fluxmq-delivery-id") == "c/0/0")
        .map(|r| r.h("x-fluxmq-attempt"))
        .collect();
    assert_eq!(attempts, ["1", "2", "3"]);

    // The attempts log and the history saw all of it.
    let log = e.svc.local_attempts(&sub.id).unwrap();
    let has = |o: AttemptOutcome| log.iter().any(|a| a.outcome == o);
    assert!(has(AttemptOutcome::Retry) && has(AttemptOutcome::ProbeFailed));
    assert!(has(AttemptOutcome::ProbeOk) && has(AttemptOutcome::Delivered));
    assert!(
        log.windows(2).all(|w| w[0].at_ms >= w[1].at_ms),
        "newest first"
    );
    let retry = log
        .iter()
        .find(|a| a.outcome == AttemptOutcome::Retry)
        .unwrap();
    assert_eq!(
        (retry.status, retry.error.as_deref()),
        (Some(503), Some("HTTP 503"))
    );
    assert_eq!(retry.delivery_id, "c/0/0");
    assert_eq!(retry.node, e.broker.node_id());
    let h = e.svc.local_history(&sub.id, "15m").unwrap();
    let (ok_n, failed): (u64, u64) = h
        .slots
        .iter()
        .fold((0, 0), |(a, b), s| (a + s.delivered, b + s.failed));
    assert_eq!((ok_n, failed), (5, 2), "probes are not counted as messages");
    assert!(e.svc.local_history(&sub.id, "7d").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn breaker_off_by_default_dead_letters_as_before() {
    let e = env().await;
    let (mock, url) = Mock::start().await;
    channel(&e.broker, "c", 1, vec![10], 2);
    mock.script(|_, _| (503, vec![], Duration::ZERO));
    let sub = e.svc.create(req("h", "c", &url)).await.unwrap();
    assert_eq!(sub.pause_after_failures, 0);
    publish(
        &e.broker,
        "c",
        &(0..3).map(|i| (None, json!(i))).collect::<Vec<_>>(),
    )
    .await;
    eventually("all dead-lettered", || {
        dlq_reasons(&e.broker, "c").len() == 3
    })
    .await;
    assert!(e.svc.stats()[0].breaker.is_none());
    assert_eq!(mock.count(), 6);
}

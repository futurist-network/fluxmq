//! Idempotent publishing (v0.12): a `PublishBatch` whose first record carries
//! `flux-pseq: <producer>.<sequence>` is stored once however often it is retried — retried
//! after an answer, while the original is still being written, and after a broker restart.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use common::{RawConn, TestServer};
use fluxmq_proto::batch::{Compression, RecordsSection, encode_record};
use fluxmq_proto::model::headers::PRODUCER_SEQ;
use fluxmq_proto::wire::{ClientRequest, ServerResponse};
use fluxmq_server::{Server, ServerConfig};
use serde_json::json;
use tokio_util::sync::CancellationToken;

fn section(n: usize, mark: Option<&str>, tag: &str) -> RecordsSection {
    let mut body = Vec::new();
    for i in 0..n {
        let mut h = BTreeMap::new();
        if i == 0
            && let Some(m) = mark
        {
            h.insert(PRODUCER_SEQ.to_string(), m.to_string());
        }
        encode_record(
            &mut body,
            i as u32,
            None,
            &h,
            format!("{tag}-{i}").as_bytes(),
        );
    }
    RecordsSection {
        compression: Compression::None,
        record_count: n as u32,
        body: Bytes::from(body),
    }
}

async fn publish(c: &mut RawConn, ch: &str, sec: RecordsSection) -> (u32, u64, u32) {
    match c
        .request(ClientRequest::PublishBatch {
            channel: ch.into(),
            partition: Some(0),
            records: sec,
        })
        .await
    {
        Ok(ServerResponse::PublishBatchOk {
            partition,
            base_offset,
            count,
        }) => (partition, base_offset, count),
        other => panic!("publish: {other:?}"),
    }
}

async fn stored(http: &reqwest::Client, base: &str, ch: &str) -> usize {
    let page: serde_json::Value = http
        .get(format!(
            "{base}/api/channels/{ch}/messages?partition=0&offset=0&limit=1000"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    page["messages"].as_array().unwrap().len()
}

#[tokio::test(flavor = "multi_thread")]
async fn retried_batch_is_stored_once() {
    let srv = TestServer::start().await;
    let r = srv
        .post_json(
            "/api/channels",
            json!({"name": "idem", "mode": "stream", "partitions": 1}),
        )
        .await;
    assert!(r.status().is_success());
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    let first = publish(&mut c, "idem", section(3, Some("00000000000000aa.1"), "a")).await;
    // The same batch again (a retry after a lost answer): same offsets, nothing new stored.
    let again = publish(&mut c, "idem", section(3, Some("00000000000000aa.1"), "a")).await;
    assert_eq!(first, again);
    // Another sequence, another producer, and an unmarked batch are stored.
    let next = publish(&mut c, "idem", section(2, Some("00000000000000aa.2"), "b")).await;
    assert_eq!(next.1, first.1 + 3);
    let other = publish(&mut c, "idem", section(1, Some("00000000000000bb.1"), "c")).await;
    assert_eq!(other.1, next.1 + 2);
    let plain1 = publish(&mut c, "idem", section(1, None, "d")).await;
    let plain2 = publish(&mut c, "idem", section(1, None, "d")).await;
    assert_ne!(
        plain1.1, plain2.1,
        "unmarked batches are never deduplicated"
    );
    let base = format!("http://{}", srv.http_addr);
    assert_eq!(stored(&srv.http, &base, "idem").await, 3 + 2 + 1 + 2);
    // From the collector's periodic snapshot.
    let line = common::eventually(|| async {
        let metrics = srv
            .req(reqwest::Method::GET, "/metrics")
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        metrics
            .lines()
            .find(|l| {
                l.starts_with("fluxmq_channel_publish_duplicates_total{") && l.contains("idem")
            })
            .map(str::to_string)
    })
    .await;
    assert!(line.ends_with(" 1"), "{line}");
    srv.stop().await;
}

/// A retry arriving while the original is still being written waits for it and shares its
/// result (both answered with the same offsets, one copy stored).
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_retry_waits_for_the_original() {
    // Group commit, so the delayed fsync below holds the original's answer back.
    let srv = TestServer::start_with(|c| c.fsync = fluxmq_server::FsyncMode::GroupCommit).await;
    let r = srv
        .post_json(
            "/api/channels",
            json!({"name": "race", "mode": "stream", "partitions": 1}),
        )
        .await;
    assert!(r.status().is_success());
    let _ = srv
        .broker
        .debug_delay_next_fsync("race", 0, Duration::from_millis(400));
    let addr = srv.client_addr;
    let send = || async move {
        let mut c = RawConn::connect_hello(addr, None).await;
        publish(&mut c, "race", section(4, Some("00000000000000cc.9"), "r")).await
    };
    let t0 = std::time::Instant::now();
    let (a, b) = tokio::join!(send(), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        send().await
    });
    assert_eq!(a, b);
    // The retry arrived while the original was still waiting for its fsync.
    assert!(
        t0.elapsed() >= Duration::from_millis(350),
        "{:?}",
        t0.elapsed()
    );
    let base = format!("http://{}", srv.http_addr);
    assert_eq!(stored(&srv.http, &base, "race").await, 4);
    srv.stop().await;
}

/// The table is rebuilt from the log's tail after a restart: a retry that crosses the restart
/// is still recognized.
#[tokio::test(flavor = "multi_thread")]
async fn retry_after_restart_is_recognized() {
    let dir = tempfile::tempdir().unwrap();
    let start = |dir: std::path::PathBuf| async move {
        let mut config = ServerConfig::ephemeral(dir);
        config.fsync = fluxmq_server::FsyncMode::Os;
        let server = Server::bind(config).await.unwrap();
        let (addr, http) = (server.client_addr(), server.http_addr());
        let token = CancellationToken::new();
        let h = tokio::spawn(server.run(token.clone().cancelled_owned()));
        (addr, http, token, h)
    };
    let (addr, http, token, h) = start(dir.path().join("data")).await;
    let client = reqwest::Client::new();
    let r = client
        .post(format!("http://{http}/api/channels"))
        .json(&json!({"name": "rs", "mode": "stream", "partitions": 1}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let mut c = RawConn::connect_hello(addr, None).await;
    let first = publish(&mut c, "rs", section(5, Some("00000000000000dd.3"), "x")).await;
    drop(c);
    token.cancel();
    h.await.unwrap().unwrap();

    let (addr, http, token, h) = start(dir.path().join("data")).await;
    let mut c = RawConn::connect_hello(addr, None).await;
    let again = publish(&mut c, "rs", section(5, Some("00000000000000dd.3"), "x")).await;
    assert_eq!(first, again);
    assert_eq!(stored(&client, &format!("http://{http}"), "rs").await, 5);
    drop(c);
    token.cancel();
    h.await.unwrap().unwrap();
}

/// The case idempotence exists for: the client sends a batch and loses its connection before
/// the answer; the batch is stored anyway, and the retry on a new connection gets its offsets.
#[tokio::test(flavor = "multi_thread")]
async fn retry_after_a_dropped_connection_finds_the_stored_batch() {
    let srv = TestServer::start_with(|c| c.fsync = fluxmq_server::FsyncMode::GroupCommit).await;
    let r = srv
        .post_json(
            "/api/channels",
            json!({"name": "drop", "mode": "stream", "partitions": 1}),
        )
        .await;
    assert!(r.status().is_success());
    let _ = srv
        .broker
        .debug_delay_next_fsync("drop", 0, Duration::from_millis(300));
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    c.send(
        77,
        ClientRequest::PublishBatch {
            channel: "drop".into(),
            partition: Some(0),
            records: section(3, Some("00000000000000ff.1"), "lost-answer"),
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(c); // the answer is lost
    let mut c2 = RawConn::connect_hello(srv.client_addr, None).await;
    let retry = tokio::time::timeout(
        Duration::from_secs(5),
        publish(
            &mut c2,
            "drop",
            section(3, Some("00000000000000ff.1"), "lost-answer"),
        ),
    )
    .await
    .expect("the retry is answered (no endless wait)");
    assert_eq!(retry.2, 3);
    let base = format!("http://{}", srv.http_addr);
    assert_eq!(stored(&srv.http, &base, "drop").await, 3, "stored twice");
    srv.stop().await;
}

//! Priority queues (v0.12): partitions are levels, every publish path places a message by its
//! `flux-priority` level, and consumers always take the highest level first.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use common::{RawConn, TestServer, WAIT};
use fluxmq_client::{BatchingOptions, Client, ClientOptions, SubscribeOptions, message};
use fluxmq_proto::batch::{Compression, RecordsSection, encode_record};
use fluxmq_proto::model::StartFrom;
use fluxmq_proto::model::headers::PRIORITY;
use fluxmq_proto::wire::{ClientRequest, ErrorCode, ServerResponse};
use serde_json::{Value, json};

async fn create(srv: &TestServer, name: &str, levels: u32) -> reqwest::StatusCode {
    srv.post_json(
        "/api/channels",
        json!({ "name": name, "mode": "priority", "partitions": levels }),
    )
    .await
    .status()
}

async fn client(srv: &TestServer) -> Client {
    Client::connect(ClientOptions::new(srv.client_addr.to_string(), "prio"))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn highest_level_is_delivered_first() {
    let srv = TestServer::start().await;
    assert!(create(&srv, "jobs", 3).await.is_success());
    let c = client(&srv).await;
    // Interleaved levels, low first; the batch call groups them by level.
    let msgs: Vec<_> = (0..90)
        .map(|i| {
            message(format!("{}-{i}", i % 3))
                .key(format!("k{i}")) // keys never override the level
                .priority((i % 3) as u8)
                .build()
        })
        .collect();
    let ids = c.producer("jobs").send_batch(msgs).await.unwrap();
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(
            id.partition,
            (i % 3) as u32,
            "message {i} stored at level {}",
            id.partition
        );
    }
    let mut cons = c
        .subscribe(
            "jobs",
            "workers",
            SubscribeOptions::default()
                .prefetch(1)
                .start(StartFrom::Earliest),
        )
        .await
        .unwrap();
    let mut levels = Vec::new();
    for _ in 0..90 {
        let d = tokio::time::timeout(WAIT, cons.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let p = String::from_utf8(d.payload().to_vec()).unwrap();
        levels.push(p.split('-').next().unwrap().parse::<u32>().unwrap());
        d.ack().await.unwrap();
    }
    let mut sorted = levels.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(levels, sorted, "not highest-first: {levels:?}");

    // A new urgent message jumps ahead of a waiting backlog of low ones.
    let p = c.producer("jobs");
    for i in 0..20 {
        p.send(message(format!("0-late{i}"))).await.unwrap();
    }
    // One low one is in flight (prefetch 1); the urgent one arrives meanwhile.
    let first = tokio::time::timeout(WAIT, cons.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    p.send(message("2-urgent").priority(9)).await.unwrap(); // clamped to the top level
    first.ack().await.unwrap();
    let next = tokio::time::timeout(WAIT, cons.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&next.payload()[..], b"2-urgent");
    next.ack().await.unwrap();
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn levels_bounds_and_misplaced_batches_are_refused() {
    let srv = TestServer::start().await;
    assert_eq!(create(&srv, "one", 1).await, 400);
    assert_eq!(create(&srv, "eleven", 11).await, 400);
    assert!(create(&srv, "pq", 3).await.is_success());
    let section = |levels: &[u8]| {
        let mut body = Vec::new();
        for (i, l) in levels.iter().enumerate() {
            let h = BTreeMap::from([(PRIORITY.to_string(), l.to_string())]);
            encode_record(&mut body, i as u32, None, &h, b"x");
        }
        RecordsSection {
            compression: Compression::None,
            record_count: levels.len() as u32,
            body: Bytes::from(body),
        }
    };
    let mut raw = RawConn::connect_hello(srv.client_addr, None).await;
    let wrong = raw
        .request(ClientRequest::PublishBatch {
            channel: "pq".into(),
            partition: Some(0),
            records: section(&[2, 2]),
        })
        .await;
    assert!(
        matches!(&wrong, Err(e) if e.code == ErrorCode::BadRequest),
        "{wrong:?}"
    );
    let mixed = raw
        .request(ClientRequest::PublishBatch {
            channel: "pq".into(),
            partition: None,
            records: section(&[0, 1]),
        })
        .await;
    assert!(
        matches!(&mixed, Err(e) if e.code == ErrorCode::BadRequest),
        "{mixed:?}"
    );
    // Without a partition the broker places a one-level batch at its level.
    let placed = raw
        .request(ClientRequest::PublishBatch {
            channel: "pq".into(),
            partition: None,
            records: section(&[1, 1]),
        })
        .await;
    assert!(
        matches!(
            placed,
            Ok(ServerResponse::PublishBatchOk { partition: 1, .. })
        ),
        "{placed:?}"
    );
    // REST: by header, missing = 0.
    let r = srv
        .post_json(
            "/api/channels/pq/publish",
            json!({ "messages": [
                { "payload": { "encoding": "utf8", "data": "a" }, "headers": { "flux-priority": "2" } },
                { "payload": { "encoding": "utf8", "data": "b" } },
                { "key": "k", "payload": { "encoding": "utf8", "data": "c" }, "headers": { "flux-priority": "1" } },
            ]}),
        )
        .await;
    let ids: Value = r.json().await.unwrap();
    let parts: Vec<u64> = ids["ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["partition"].as_u64().unwrap())
        .collect();
    assert_eq!(parts, vec![2, 0, 1]);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn batching_producer_places_by_level() {
    let srv = TestServer::start().await;
    assert!(create(&srv, "bp", 4).await.is_success());
    let c = client(&srv).await;
    let bp = c.batching_producer("bp", BatchingOptions::default());
    let mut futs = Vec::new();
    for i in 0..40u8 {
        futs.push(
            bp.send(message(format!("m{i}")).priority(i % 4))
                .await
                .unwrap(),
        );
    }
    for (i, f) in futs.into_iter().enumerate() {
        let id = tokio::time::timeout(Duration::from_secs(10), f)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id.partition, (i % 4) as u32);
    }
    srv.stop().await;
}

//! Protocol v4 end to end: the real SDK against the real broker. `PublishBatch` with every
//! compression, raw `DeliverRecords` consumption with individual acks (`AckRanges`),
//! nack/redelivery/DLQ on batch-stored data, cumulative commits across a broker restart,
//! v2/v3 consumers reading batch-stored data, keyed batches and TLS.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use common::{RawConn, TestServer, WAIT};
use fluxmq_client::fluxmq_proto::batch::{RecordsSection, encode_record};
use fluxmq_client::fluxmq_proto::wire::{
    ClientRequest, DeliveryRange, ServerFrame, ServerResponse, SubscribeOptions as WireSubOpts,
};
use fluxmq_client::{
    AckMode, BatchingOptions, ChannelConfig, ChannelMode, Client, ClientOptions, Compression,
    Consumer, StartFrom, SubscribeOptions, TlsOptions, message,
};
use fluxmq_proto::model::{RetryPolicy, StartFrom as WireStart};

fn stream(name: &str, partitions: u32) -> ChannelConfig {
    let mut c = ChannelConfig::new(name, ChannelMode::Stream);
    c.partitions = partitions;
    c
}

async fn next(c: &mut Consumer) -> fluxmq_client::Delivery {
    tokio::time::timeout(WAIT, c.next())
        .await
        .expect("delivery in time")
        .expect("open")
        .expect("ok")
}

async fn eventually_lag_zero(srv: &TestServer, ch: &str, g: &str) {
    for _ in 0..200 {
        if srv.group_lag(ch, g) == Some(0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("lag of {ch}/{g} stays {:?}", srv.group_lag(ch, g));
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_batch_every_compression_raw_consume() {
    let srv = TestServer::start().await;
    for (i, comp) in [Compression::None, Compression::Lz4, Compression::Zstd]
        .into_iter()
        .enumerate()
    {
        let client = Client::connect(
            ClientOptions::new(srv.client_addr.to_string(), "v4").compression(comp),
        )
        .await
        .unwrap();
        let ch = format!("v4.c{i}");
        client.create_channel(stream(&ch, 2)).await.unwrap();
        let p = client.producer(&ch);
        let mut sent = BTreeSet::new();
        for b in 0..5 {
            let msgs: Vec<_> = (0..100)
                .map(|j| {
                    let s = format!("{}-{b}-{j}-{}", comp.as_str(), "x".repeat(j % 50));
                    sent.insert(s.clone());
                    message(s)
                })
                .collect();
            let ids = p.send_batch(msgs).await.unwrap();
            assert_eq!(ids.len(), 100);
        }
        let mut c = client
            .subscribe(
                &ch,
                "g",
                SubscribeOptions::default()
                    .start(StartFrom::Earliest)
                    .prefetch(300),
            )
            .await
            .unwrap();
        let mut got = BTreeSet::new();
        while got.len() < sent.len() {
            let d = next(&mut c).await;
            assert_eq!(d.attempt(), 1);
            assert!(got.insert(String::from_utf8(d.payload().to_vec()).unwrap()));
            d.ack().await.unwrap();
        }
        assert_eq!(got, sent);
        c.commit().await.unwrap();
        eventually_lag_zero(&srv, &ch, "g").await;
    }
}

fn section(payloads: &[String], keys: Option<&[&str]>) -> RecordsSection {
    let mut body = Vec::new();
    for (i, p) in payloads.iter().enumerate() {
        encode_record(
            &mut body,
            i as u32,
            keys.map(|k| k[i % k.len()]),
            &BTreeMap::new(),
            p.as_bytes(),
        );
    }
    RecordsSection {
        compression: Compression::None,
        record_count: payloads.len() as u32,
        body: body.into(),
    }
}

/// Raw protocol: `PublishBatch`, then `DeliverRecords` (v4 raw), `DeliverBatch` (v3) and
/// `Deliver` (v2) consumers of the same batch-stored data, each acking everything.
#[tokio::test(flavor = "multi_thread")]
async fn raw_frames_and_old_consumers_on_batch_data() {
    let srv = TestServer::start().await;
    let mut p = RawConn::connect_hello(srv.client_addr, None).await;
    p.request(ClientRequest::CreateChannel(stream("v4.raw", 1)))
        .await
        .unwrap();
    let payloads: Vec<String> = (0..30).map(|i| format!("m{i}")).collect();
    for chunk in payloads.chunks(10) {
        let r = p
            .request(ClientRequest::PublishBatch {
                channel: "v4.raw".into(),
                partition: Some(0),
                records: section(chunk, None),
            })
            .await
            .unwrap();
        assert!(matches!(
            r,
            ServerResponse::PublishBatchOk {
                count: 10,
                partition: 0,
                ..
            }
        ));
    }

    // v4 raw consumer.
    let mut c = RawConn::connect_hello(srv.client_addr, None).await;
    let ServerResponse::Subscribed { subscription_id } = c
        .request(ClientRequest::SubscribeWith {
            channel: "v4.raw".into(),
            group: "raw".into(),
            prefetch: 100,
            start: WireStart::Earliest,
            options: WireSubOpts {
                raw_records: true,
                ..Default::default()
            },
        })
        .await
        .unwrap()
    else {
        panic!("subscribed")
    };
    let mut seen = Vec::new();
    while seen.len() < 30 {
        match c.recv().await.expect("open") {
            ServerFrame::DeliverRecords {
                subscription_id: s,
                partition,
                first_delivery_id,
                from_offset,
                count,
                envelopes,
            } => {
                assert_eq!(s, subscription_id);
                let recs: Vec<_> = fluxmq_core::decode_chunk(partition, &envelopes)
                    .map(|m| m.unwrap())
                    .filter(|m| {
                        m.offset >= from_offset && m.offset < from_offset + u64::from(count)
                    })
                    .collect();
                assert_eq!(recs.len() as u32, count);
                seen.extend(
                    recs.iter()
                        .map(|m| String::from_utf8(m.payload.to_vec()).unwrap()),
                );
                c.send(
                    0,
                    ClientRequest::AckRanges {
                        subscription_id,
                        ranges: vec![DeliveryRange {
                            first: first_delivery_id,
                            count,
                        }],
                    },
                )
                .await;
            }
            other => panic!("expected DeliverRecords, got {other:?}"),
        }
    }
    assert_eq!(seen, payloads);
    eventually_lag_zero(&srv, "v4.raw", "raw").await;

    // v3 and v2 consumers: conversion from batches.
    for (version, group) in [(3u16, "v3"), (2u16, "v2")] {
        let mut c = RawConn::connect(srv.client_addr).await;
        c.hello(version, None).await.unwrap();
        let ServerResponse::Subscribed { subscription_id } = c
            .request(ClientRequest::Subscribe {
                channel: "v4.raw".into(),
                group: group.into(),
                prefetch: 100,
                start: WireStart::Earliest,
            })
            .await
            .unwrap()
        else {
            panic!("subscribed")
        };
        let mut got = Vec::new();
        while got.len() < 30 {
            let (s, d) = c.next_delivery().await;
            assert_eq!(s, subscription_id);
            got.push(String::from_utf8(d.message.payload.to_vec()).unwrap());
            c.send(
                0,
                ClientRequest::Ack {
                    subscription_id,
                    delivery_ids: vec![d.delivery_id],
                },
            )
            .await;
        }
        assert_eq!(got, payloads, "v{version}");
        eventually_lag_zero(&srv, "v4.raw", group).await;
        // v4-only requests are refused on an old connection.
        let r = c
            .request(ClientRequest::Commit {
                subscription_id,
                partition: 0,
                offset: 1,
            })
            .await;
        assert!(r.is_err());
    }
}

/// Nack with retry redelivers (attempt 2) and exhausting attempts dead-letters, on
/// batch-stored data consumed through raw frames; a message never acked is redelivered
/// after the ack timeout.
#[tokio::test(flavor = "multi_thread")]
async fn nack_redelivery_dlq_and_timeout_on_batch_data() {
    let srv = TestServer::start().await;
    let client = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "v4"))
        .await
        .unwrap();
    let mut cfg = stream("v4.nack", 1);
    cfg.retry = RetryPolicy {
        max_attempts: 2,
        backoff_ms: vec![10],
        ack_timeout_ms: 500,
    };
    client.create_channel(cfg).await.unwrap();
    let ids = client
        .producer("v4.nack")
        .send_batch((0..10).map(|i| message(format!("n{i}"))))
        .await
        .unwrap();
    assert_eq!(ids.len(), 10);
    let mut c = client
        .subscribe(
            "v4.nack",
            "g",
            SubscribeOptions::default().start(StartFrom::Earliest),
        )
        .await
        .unwrap();
    let mut attempts: BTreeMap<String, u32> = BTreeMap::new();
    let mut acked = 0;
    let mut timed_out_seen = false;
    while acked < 9 || !timed_out_seen {
        let d = next(&mut c).await;
        let p = String::from_utf8(d.payload().to_vec()).unwrap();
        attempts.insert(p.clone(), d.attempt());
        match (p.as_str(), d.attempt()) {
            ("n3", _) => d.nack("boom", true).await.unwrap(), // attempt 1 retried, 2 → DLQ
            ("n7", 1) => drop(d),                             // never acked: ack timeout
            ("n7", 2) => {
                timed_out_seen = true;
                d.ack().await.unwrap();
                acked += 1;
            }
            _ => {
                d.ack().await.unwrap();
                acked += 1;
            }
        }
    }
    assert_eq!(attempts["n3"], 2);
    assert_eq!(attempts["n7"], 2);
    c.commit().await.unwrap();
    // n3 is in the DLQ with both attempts.
    let mut dlq = Vec::new();
    for _ in 0..100 {
        dlq = srv.broker.dlq_list(Some("v4.nack"), None, None, 10).0;
        if !dlq.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(dlq.len(), 1);
    assert_eq!(&dlq[0].message.payload[..], b"n3");
    assert_eq!(dlq[0].attempts.len(), 2);
    eventually_lag_zero(&srv, "v4.nack", "g").await;
}

/// Cumulative commits survive a broker restart: delivery resumes exactly after the
/// committed position.
#[tokio::test(flavor = "multi_thread")]
async fn cumulative_commit_and_restart_exactness() {
    let data = tempfile::tempdir().unwrap();
    let data_dir = data.path().join("data");
    let dd = data_dir.clone();
    let srv = TestServer::start_with(move |c| c.data_dir = dd).await;
    {
        let client = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "cum"))
            .await
            .unwrap();
        client.create_channel(stream("v4.cum", 1)).await.unwrap();
        client
            .producer("v4.cum")
            .send_batch((0..100).map(|i| message(format!("c{i}"))))
            .await
            .unwrap();
        // Cumulative on a queue channel is refused.
        client
            .create_channel(ChannelConfig::new("v4.q", ChannelMode::Queue))
            .await
            .unwrap();
        assert!(
            client
                .subscribe("v4.q", "g", SubscribeOptions::default().cumulative())
                .await
                .is_err()
        );
        let mut c = client
            .subscribe(
                "v4.cum",
                "g",
                SubscribeOptions::default()
                    .start(StartFrom::Earliest)
                    .ack_mode(AckMode::Cumulative)
                    .prefetch(10),
            )
            .await
            .unwrap();
        for i in 0..60 {
            let d = next(&mut c).await;
            assert_eq!(d.payload(), format!("c{i}").as_bytes());
            d.ack().await.unwrap(); // in cumulative mode: marks it processed locally
        }
        c.commit().await.unwrap();
        client.close();
    }
    for _ in 0..100 {
        if srv.group_lag("v4.cum", "g") == Some(40) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(srv.group_lag("v4.cum", "g"), Some(40));
    srv.stop().await;

    let dd = data_dir.clone();
    let srv = TestServer::start_with(move |c| c.data_dir = dd).await;
    let client = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "cum2"))
        .await
        .unwrap();
    let mut c = client
        .subscribe(
            "v4.cum",
            "g",
            SubscribeOptions::default().cumulative().prefetch(100),
        )
        .await
        .unwrap();
    for i in 60..100 {
        let d = next(&mut c).await;
        assert_eq!(d.payload(), format!("c{i}").as_bytes(), "resumes exactly");
        d.ack().await.unwrap();
    }
    c.commit().await.unwrap();
    eventually_lag_zero(&srv, "v4.cum", "g").await;
    drop(data);
}

/// Keyed channel fed by the batching producer: per-key order holds and nothing is lost.
#[tokio::test(flavor = "multi_thread")]
async fn keyed_batches_keep_per_key_order() {
    let srv = TestServer::start().await;
    let client = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "keyed"))
        .await
        .unwrap();
    let mut cfg = ChannelConfig::new("v4.keyed", ChannelMode::Keyed);
    cfg.partitions = 4;
    client.create_channel(cfg).await.unwrap();
    let p = client.batching_producer("v4.keyed", BatchingOptions::default());
    let mut futs = Vec::new();
    for i in 0..400 {
        futs.push(
            p.send(message(format!("{}:{i}", i % 7)).key(format!("k{}", i % 7)))
                .await
                .unwrap(),
        );
    }
    for f in futs {
        f.await.unwrap();
    }
    let mut c = client
        .subscribe(
            "v4.keyed",
            "g",
            SubscribeOptions::default()
                .start(StartFrom::Earliest)
                .prefetch(50),
        )
        .await
        .unwrap();
    let mut last: BTreeMap<String, i64> = BTreeMap::new();
    for _ in 0..400 {
        let d = next(&mut c).await;
        let s = String::from_utf8(d.payload().to_vec()).unwrap();
        let (k, n) = s.split_once(':').unwrap();
        let n: i64 = n.parse().unwrap();
        let prev = last.insert(k.to_string(), n).unwrap_or(-1);
        assert!(n > prev, "key {k}: {n} after {prev}");
        d.ack().await.unwrap();
    }
    c.commit().await.unwrap();
    eventually_lag_zero(&srv, "v4.keyed", "g").await;
}

/// The same publish-batch + raw-consume path over TLS.
#[tokio::test(flavor = "multi_thread")]
async fn v4_over_tls() {
    let certs = tempfile::tempdir().unwrap();
    fluxmq_tls::dev::generate(&fluxmq_tls::dev::GenDevOptions {
        out: certs.path().to_path_buf(),
        nodes: vec![],
        hosts: vec!["localhost".into(), "127.0.0.1".into()],
        force: false,
        ..Default::default()
    })
    .unwrap();
    let cp = certs.path().to_path_buf();
    let srv = TestServer::start_with(move |c| {
        c.tls.cert = Some(cp.join("server.pem"));
        c.tls.key = Some(cp.join("server-key.pem"));
    })
    .await;
    let client = Client::connect(
        ClientOptions::new(format!("tls://{}", srv.client_addr), "tls")
            .tls(TlsOptions::new().ca_file(certs.path().join("ca.pem")))
            .compression(Compression::Lz4),
    )
    .await
    .unwrap();
    client.create_channel(stream("v4.tls", 2)).await.unwrap();
    client
        .producer("v4.tls")
        .send_batch((0..200).map(|i| message(format!("t{i}"))))
        .await
        .unwrap();
    let mut c = client
        .subscribe(
            "v4.tls",
            "g",
            SubscribeOptions::default()
                .start(StartFrom::Earliest)
                .prefetch(64),
        )
        .await
        .unwrap();
    let mut got = BTreeSet::new();
    while got.len() < 200 {
        let d = next(&mut c).await;
        got.insert(String::from_utf8(d.payload().to_vec()).unwrap());
        d.ack().await.unwrap();
    }
    c.commit().await.unwrap();
    eventually_lag_zero(&srv, "v4.tls", "g").await;
}

/// v0.8 hot path: large raw frames go out through the zero-copy vectored writer (envelopes
/// handed to the socket, partial writes resumed) and are consumed intact with CRC checks on,
/// a v3 (`DeliverBatch`) consumer gets large payloads as separate slices too, and
/// `Delivery::ack_all` settles everything (nothing redelivered, lag 0).
#[tokio::test(flavor = "multi_thread")]
async fn large_frames_zero_copy_and_ack_all() {
    let srv = TestServer::start().await;
    let client = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "hp"))
        .await
        .unwrap();
    let ch = "v8.big";
    client.create_channel(stream(ch, 3)).await.unwrap();
    let p = client.producer(ch);
    let mut sent = BTreeSet::new();
    for b in 0..40 {
        let msgs: Vec<_> = (0..250)
            .map(|j| {
                // 1 KiB .. 5 KiB payloads (some above the payload slice threshold).
                let s = format!("{b}-{j}-{}", "y".repeat(1024 + (j * 17) % 4096));
                sent.insert(s.clone());
                message(s)
            })
            .collect();
        p.send_batch(msgs).await.unwrap();
    }
    for (group, raw) in [("raw", true), ("decoded", false)] {
        let mut c = client
            .subscribe(
                ch,
                group,
                SubscribeOptions::default()
                    .start(StartFrom::Earliest)
                    .prefetch(4000)
                    .raw_records(raw)
                    .verify_crc(true),
            )
            .await
            .unwrap();
        let mut got = BTreeSet::new();
        let mut held = Vec::new();
        while got.len() < sent.len() {
            let d = next(&mut c).await;
            assert_eq!(d.attempt(), 1, "{group}: nothing redelivered");
            assert!(got.insert(String::from_utf8(d.payload().to_vec()).unwrap()));
            held.push(d);
            if held.len() == 97 {
                fluxmq_client::Delivery::ack_all(std::mem::take(&mut held))
                    .await
                    .unwrap();
            }
        }
        fluxmq_client::Delivery::ack_all(held).await.unwrap();
        assert_eq!(got, sent, "{group}");
        c.commit().await.unwrap();
        eventually_lag_zero(&srv, ch, group).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), c.next())
                .await
                .is_err(),
            "{group}: no redelivery after ack_all"
        );
    }
}

/// v0.8: a stored envelope larger than `MAX_DELIVER_BATCH` records goes out once, as one
/// `DeliverRecords` frame covering all its records (v0.7 capped raw polls at 256 records and
/// sent the whole envelope again for every 256-record part). With less prefetch credit than
/// the envelope holds, parts are still exact and nothing is lost.
#[tokio::test(flavor = "multi_thread")]
async fn large_envelope_goes_out_once() {
    let srv = TestServer::start().await;
    let mut p = RawConn::connect_hello(srv.client_addr, None).await;
    p.request(ClientRequest::CreateChannel(stream("v8.env", 1)))
        .await
        .unwrap();
    let payloads: Vec<String> = (0..1000).map(|i| format!("e{i}")).collect();
    p.request(ClientRequest::PublishBatch {
        channel: "v8.env".into(),
        partition: Some(0),
        records: section(&payloads, None),
    })
    .await
    .unwrap();
    for (group, prefetch, want_frames) in [("big", 5000u32, Some(1usize)), ("small", 300, None)] {
        let mut c = RawConn::connect_hello(srv.client_addr, None).await;
        let ServerResponse::Subscribed { subscription_id } = c
            .request(ClientRequest::SubscribeWith {
                channel: "v8.env".into(),
                group: group.into(),
                prefetch,
                start: WireStart::Earliest,
                options: WireSubOpts {
                    raw_records: true,
                    ..Default::default()
                },
            })
            .await
            .unwrap()
        else {
            panic!("subscribed")
        };
        let mut seen = Vec::new();
        let mut frames = 0;
        while seen.len() < payloads.len() {
            let ServerFrame::DeliverRecords {
                partition,
                first_delivery_id,
                from_offset,
                count,
                envelopes,
                ..
            } = c.recv().await.expect("open")
            else {
                panic!("expected DeliverRecords")
            };
            frames += 1;
            assert!(count <= prefetch);
            seen.extend(
                fluxmq_core::decode_chunk(partition, &envelopes)
                    .map(|m| m.unwrap())
                    .filter(|m| {
                        m.offset >= from_offset && m.offset < from_offset + u64::from(count)
                    })
                    .map(|m| String::from_utf8(m.payload.to_vec()).unwrap()),
            );
            c.send(
                0,
                ClientRequest::AckRanges {
                    subscription_id,
                    ranges: vec![DeliveryRange {
                        first: first_delivery_id,
                        count,
                    }],
                },
            )
            .await;
        }
        assert_eq!(seen, payloads, "{group}");
        if let Some(n) = want_frames {
            assert_eq!(frames, n, "{group}: the envelope went out once");
        }
        eventually_lag_zero(&srv, "v8.env", group).await;
    }
}

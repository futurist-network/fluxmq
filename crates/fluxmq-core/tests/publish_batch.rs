//! `Broker::publish_batch` (v0.7): routing, validation, durability across restart, and
//! interplay with message reads and `publish`.

mod common;

use std::collections::BTreeMap;

use bytes::Bytes;
use common::*;
use fluxmq_core::CoreError;
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::batch::{self, Compression, RecordsSection, codec};
use fluxmq_proto::model::{ChannelMode, StartFrom};

fn section(keys: &[Option<&str>], c: Compression) -> RecordsSection {
    let mut body = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        let mut h = BTreeMap::new();
        h.insert("i".to_string(), i.to_string());
        batch::encode_record(&mut body, i as u32, *k, &h, format!("m{i}").as_bytes());
    }
    let (compression, body) = match codec::compress(c, &body) {
        Some(z) => (c, z),
        None => (Compression::None, body),
    };
    RecordsSection {
        compression,
        record_count: keys.len() as u32,
        body: Bytes::from(body),
    }
}

#[tokio::test]
async fn publish_batch_routes_validates_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path()).await;
    b.create_channel(channel("s", ChannelMode::Stream, 4))
        .unwrap();
    // Explicit partition.
    let (p, base, n) = b
        .publish_batch("s", Some(2), section(&[None; 100], Compression::Lz4))
        .await
        .unwrap();
    assert_eq!((p, base, n), (2, 0, 100));
    // Keyless, no partition: sticky (one partition for the whole batch).
    let (p2, base2, _) = b
        .publish_batch("s", None, section(&[None; 10], Compression::None))
        .await
        .unwrap();
    assert!(p2 < 4);
    assert_eq!(base2, if p2 == 2 { 100 } else { 0 });
    // Keyed to one partition: routed by key_partition (same as publish).
    let keyed = section(&[Some("a"), None, Some("a")], Compression::Zstd);
    let (p3, _, _) = b.publish_batch("s", None, keyed).await.unwrap();
    assert_eq!(p3, batch::key_partition("a", 4));
    let ids = b
        .publish("s", vec![msg(Some("a"), "via publish")])
        .await
        .unwrap();
    assert_eq!(ids[0].partition, p3);
    // Keys spanning partitions are refused (batches are never split).
    let keys: Vec<String> = (0..32).map(|i| format!("k{i}")).collect();
    let spans: Vec<Option<&str>> = keys.iter().map(|k| Some(k.as_str())).collect();
    let err = b
        .publish_batch("s", None, section(&spans, Compression::None))
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::InvalidArgument(ref m) if m.contains("spans partitions")));
    // Malformed sections are refused.
    let mut bad = section(&[None; 3], Compression::None);
    bad.record_count = 4;
    assert!(matches!(
        b.publish_batch("s", Some(0), bad).await,
        Err(CoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        b.publish_batch("s", Some(9), section(&[None], Compression::None))
            .await,
        Err(CoreError::InvalidArgument(_))
    ));
    // Consumers (per-message API) see the batch records.
    let mut sub = b
        .subscribe("s", "g", info("c", 1), 1000, StartFrom::Earliest)
        .unwrap();
    let mut seen = 0;
    while seen < 114 {
        let d = next(&mut sub).await;
        sub.ack(&[d.delivery_id]).unwrap();
        seen += 1;
    }
    b.shutdown().await;
    // Restart: everything durable, readable via the browse API.
    let b = open(dir.path()).await;
    let (msgs, _, _) = b
        .read_messages(
            "s",
            &MessageQuery {
                partition: Some(2),
                offset: Some(0),
                limit: Some(1000),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(msgs.len() >= 100);
    assert_eq!(&msgs[42].payload[..], b"m42");
    assert_eq!(msgs[42].headers.get("i").map(String::as_str), Some("42"));
    b.shutdown().await;
}

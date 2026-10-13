//! Size retention must never delete messages a group has not consumed while the group is
//! within the retention budget (v0.5 bench drain miss: 250 acknowledged messages lost).

mod common;

use std::collections::BTreeSet;

use bytes::Bytes;
use common::*;
use fluxmq_core::Broker;
use fluxmq_proto::model::{ChannelMode, PublishMessage, RetentionPolicy, StartFrom};

#[tokio::test]
async fn size_retention_keeps_the_budget_for_a_group_within_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.segment_bytes = 64 * 1024;
    let b = Broker::open(cfg).await.unwrap();
    let mut ch = channel("s", ChannelMode::Stream, 1);
    // Budget per partition == segment size: the bench's 128 MiB / 128 MiB.
    ch.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: Some(64 * 1024),
    };
    b.create_channel(ch).unwrap();
    // A consumer with credit for 10 deliveries (the rest of what it is behind by is
    // undispatched, like a saturated consumer a few ms behind the head).
    let mut sub = b
        .subscribe("s", "g", info("c", 1), 10, StartFrom::Earliest)
        .unwrap();
    let payload = Bytes::from(vec![7u8; 1000]);
    let n = 70u64; // ~70 KiB: one closed segment + a few messages in the active one
    for _ in 0..n {
        b.publish(
            "s",
            vec![PublishMessage {
                key: None,
                headers: Default::default(),
                payload: payload.clone(),
            }],
        )
        .await
        .unwrap();
    }
    let mut got = BTreeSet::new();
    let mut held = Vec::new();
    for _ in 0..10 {
        let d = next(&mut sub).await;
        got.insert(d.message.offset);
        held.push(d.delivery_id);
    }
    // The group is ~60 KiB behind the head, less than the 64 KiB budget.
    b.enforce_retention().await;
    sub.ack(&held).unwrap();
    while let Some(d) = next_within(&mut sub, 500).await {
        got.insert(d.message.offset);
        sub.ack(&[d.delivery_id]).unwrap();
    }
    let missing: Vec<u64> = (0..n).filter(|o| !got.contains(o)).collect();
    assert!(
        missing.is_empty(),
        "{} of {n} published messages never delivered: {:?}",
        missing.len(),
        missing
    );
}

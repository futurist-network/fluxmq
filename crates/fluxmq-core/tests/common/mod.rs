#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy, SubscriberInfo, Subscription};
use fluxmq_proto::model::{
    ChannelConfig, ChannelMode, MessageId, PublishMessage, RetentionPolicy, RetryPolicy,
};
use fluxmq_proto::wire::Delivery;

pub fn config(dir: &Path) -> BrokerConfig {
    let mut c = BrokerConfig::new(dir);
    c.node_id = Some("test-node".into());
    c.offset_checkpoint_interval = Duration::from_millis(50);
    c.fsync = FsyncPolicy::GroupCommit {
        interval: Duration::from_millis(2),
    };
    c
}

pub async fn open(dir: &Path) -> Broker {
    Broker::open(config(dir)).await.expect("open broker")
}

pub fn channel(name: &str, mode: ChannelMode, partitions: u32) -> ChannelConfig {
    let mut c = ChannelConfig::new(name, mode);
    c.partitions = partitions;
    c.retry = RetryPolicy {
        max_attempts: 3,
        backoff_ms: vec![20],
        ack_timeout_ms: 5_000,
    };
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: None,
    };
    c
}

pub fn info(name: &str, conn: u64) -> SubscriberInfo {
    SubscriberInfo {
        client_name: name.into(),
        remote_addr: "127.0.0.1:1".into(),
        connection_id: conn,
    }
}

pub fn msg(key: Option<&str>, payload: &str) -> PublishMessage {
    PublishMessage {
        key: key.map(str::to_string),
        headers: Default::default(),
        payload: Bytes::from(payload.to_string()),
    }
}

pub async fn publish_keyed(b: &Broker, ch: &str, n: usize, keys: usize) -> Vec<MessageId> {
    let msgs = (0..n)
        .map(|i| msg(Some(&format!("k{}", i % keys)), &format!("m{i}")))
        .collect();
    b.publish(ch, msgs).await.expect("publish")
}

pub async fn next(sub: &mut Subscription) -> Delivery {
    tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("delivery within 5s")
        .expect("subscription open")
}

pub async fn next_within(sub: &mut Subscription, ms: u64) -> Option<Delivery> {
    tokio::time::timeout(Duration::from_millis(ms), sub.next())
        .await
        .ok()
        .flatten()
}

pub fn payload(d: &Delivery) -> String {
    String::from_utf8_lossy(&d.message.payload).into_owned()
}

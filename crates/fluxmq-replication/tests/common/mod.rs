#![allow(dead_code)]

use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fluxmq_core::{Broker, BrokerConfig, FsyncPolicy, SubscriberInfo};
use fluxmq_proto::model::{
    ChannelConfig, ChannelMode, MessageId, PublishMessage, RetentionPolicy, RetryPolicy,
};
use fluxmq_replication::{PartitionRole, ReplicaEndpoint, ReplicationConfig, ReplicationManager};

pub const CH: &str = "orders";
pub const SECRET: &str = "s3cret";
pub const LAG_MAX: Duration = Duration::from_millis(400);

pub struct Node {
    pub id: String,
    pub dir: tempfile::TempDir,
    pub broker: Broker,
    pub mgr: ReplicationManager,
}

pub fn broker_config(dir: &Path, id: &str, fsync: FsyncPolicy) -> BrokerConfig {
    let mut c = BrokerConfig::new(dir);
    c.node_id = Some(id.to_string());
    c.offset_checkpoint_interval = Duration::from_millis(50);
    c.fsync = fsync;
    c
}

pub fn repl_config(id: &str) -> ReplicationConfig {
    let mut c = ReplicationConfig::new(id, "127.0.0.1:0".parse().unwrap());
    c.replica_lag_max = LAG_MAX;
    let wait_ms = std::env::var("FLUX_TEST_FETCH_WAIT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    c.fetch_max_wait = Duration::from_millis(wait_ms);
    c.fetch_max_bytes = 64 * 1024;
    c.secret = Some(SECRET.into());
    c
}

pub fn channel(partitions: u32) -> ChannelConfig {
    let mut c = ChannelConfig::new(CH, ChannelMode::Stream);
    c.partitions = partitions;
    c.retry = RetryPolicy {
        max_attempts: 2,
        backoff_ms: vec![10],
        ack_timeout_ms: 5_000,
    };
    c.retention = RetentionPolicy {
        max_age_secs: None,
        max_bytes: None,
    };
    c
}

impl Node {
    pub async fn start(id: &str, fsync: FsyncPolicy) -> Node {
        let dir = tempfile::tempdir().unwrap();
        Self::start_in(id, dir, fsync).await
    }

    pub async fn start_in(id: &str, dir: tempfile::TempDir, fsync: FsyncPolicy) -> Node {
        Self::start_with(id, dir, fsync, |_| {}).await
    }

    pub async fn start_with(
        id: &str,
        dir: tempfile::TempDir,
        fsync: FsyncPolicy,
        tweak: impl FnOnce(&mut ReplicationConfig),
    ) -> Node {
        let broker = Broker::open(broker_config(dir.path(), id, fsync))
            .await
            .unwrap();
        let mut cfg = repl_config(id);
        tweak(&mut cfg);
        let mgr = ReplicationManager::start(broker.clone(), cfg)
            .await
            .unwrap();
        Node {
            id: id.to_string(),
            dir,
            broker,
            mgr,
        }
    }

    /// Stops replication and the broker; returns the data dir for a restart.
    pub async fn stop(self) -> tempfile::TempDir {
        self.mgr.shutdown().await;
        self.broker.shutdown().await;
        self.dir
    }

    pub fn leo(&self, p: u32) -> u64 {
        self.broker.replica(CH, p).unwrap().log_end_offset()
    }

    pub fn hw(&self, p: u32) -> u64 {
        self.broker.replica(CH, p).unwrap().high_watermark()
    }

    pub fn status(&self, p: u32) -> fluxmq_core::ReplicaStatus {
        self.broker.replica(CH, p).unwrap().status()
    }

    pub fn lead(&self, p: u32, epoch: u64, replicas: &[&Node], min_isr: u32) {
        self.mgr
            .set_role(
                CH,
                p,
                PartitionRole::Leader {
                    epoch,
                    replicas: replicas.iter().map(|n| n.id.clone()).collect(),
                    min_isr,
                },
            )
            .unwrap();
    }

    pub fn follow(&self, p: u32, epoch: u64, leader: &Node) {
        self.mgr
            .set_role(
                CH,
                p,
                PartitionRole::Follower {
                    epoch,
                    leader: leader.id.clone(),
                },
            )
            .unwrap();
    }

    pub fn offline(&self, p: u32) {
        self.mgr.set_role(CH, p, PartitionRole::None).unwrap();
    }

    /// The stored log of partition `p`: `(offset, key, payload, timestamp)`.
    pub fn log(&self, p: u32) -> Vec<(u64, Option<String>, Bytes, u64)> {
        let h = self.broker.replica(CH, p).unwrap();
        (h.log_start_offset()..h.log_end_offset())
            .map(|o| {
                let m = self
                    .broker
                    .debug_get_uncapped(
                        CH,
                        MessageId {
                            partition: p,
                            offset: o,
                        },
                    )
                    .unwrap()
                    .unwrap_or_else(|| panic!("{}: missing offset {o}", self.id));
                (m.offset, m.key, m.payload, m.timestamp_ms)
            })
            .collect()
    }
}

/// Starts `n` nodes ("n1".."nN"), wires their endpoints and creates the channel everywhere.
pub async fn cluster(n: usize, partitions: u32, fsync: FsyncPolicy) -> Vec<Node> {
    let mut nodes = Vec::new();
    for i in 1..=n {
        nodes.push(Node::start(&format!("n{i}"), fsync).await);
    }
    wire(&nodes);
    for node in &nodes {
        node.broker.create_channel(channel(partitions)).unwrap();
    }
    nodes
}

pub fn wire(nodes: &[Node]) {
    let eps: Vec<ReplicaEndpoint> = nodes
        .iter()
        .map(|n| ReplicaEndpoint {
            node_id: n.id.clone(),
            addr: n.mgr.local_addr(),
        })
        .collect();
    for n in nodes {
        n.mgr.set_endpoints(eps.clone());
    }
}

/// Simulated controller: confirms every ISR change of `node` immediately.
pub fn auto_commit(node: &Node) -> tokio::task::JoinHandle<()> {
    let mgr = node.mgr.clone();
    let mut rx = mgr.isr_changes();
    tokio::spawn(async move {
        while let Ok(c) = rx.recv().await {
            mgr.isr_committed(&c.channel, c.partition, c.epoch, c.isr);
        }
    })
}

pub fn msg(i: usize) -> PublishMessage {
    PublishMessage {
        key: Some(format!("k{}", i % 7)),
        headers: Default::default(),
        payload: Bytes::from(format!("m{i:06}")),
    }
}

pub fn msgs(range: std::ops::Range<usize>) -> Vec<PublishMessage> {
    range.map(msg).collect()
}

pub fn info(name: &str) -> SubscriberInfo {
    SubscriberInfo {
        client_name: name.into(),
        remote_addr: "127.0.0.1:1".into(),
        connection_id: 1,
    }
}

pub async fn eventually<F, Fut>(what: &str, timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if f().await {
            return;
        }
        assert!(start.elapsed() < timeout, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_sync(what: &str, mut f: impl FnMut() -> bool) {
    eventually(what, Duration::from_secs(10), || {
        let r = f();
        async move { r }
    })
    .await;
}

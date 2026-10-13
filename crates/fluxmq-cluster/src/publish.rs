//! Cluster-aware publish (wire protocol and HTTP), see [`crate::Cluster::publish`].
//!
//! * [`publish_partition`] — `Publish { partition: Some(p) }`: served only by `p`'s leader;
//!   any other node answers `NotLeader` (message = leader id, empty during elections).
//! * [`publish_routed`] — `Publish { partition: None }` on a cluster node: picks the partition
//!   per message with the same function as core (`crc32(key) % P`; without a key, sticky: one
//!   partition per request, round-robin across requests), publishes local partitions directly
//!   and forwards the rest to their leaders over
//!   the internal HTTP API (`/internal/v1/publish`, cluster secret). Ids come back in input
//!   order. A failure of any group fails the request (some groups may have been stored —
//!   a client retry can duplicate, which is within at-least-once).
//! * [`publish_batch_partition`] / [`publish_batch_routed`] — v0.7 `PublishBatch`, the same
//!   rules for a whole records section: it is routed as one unit (key hash when it has keys,
//!   all of which must map to one partition; sticky otherwise) and forwarded as raw bytes
//!   over `/internal/v1/publish_batch`; never split, never re-encoded.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use fluxmq_core::CoreError;
use tokio::time::Instant;

use fluxmq_proto::batch::RecordsSection;
use fluxmq_proto::wire::{ErrorCode, WireError};
use fluxmq_proto::{MessageId, PublishMessage};

use crate::rpc::{
    FORWARD_CONTENT_TYPE, FORWARD_PUBLISH_BATCH_PATH, FORWARD_PUBLISH_PATH, ForwardPublish,
    ForwardPublishBatch,
};
use crate::{Cluster, PartitionRoute};

/// Round-robin cursor for keyless messages forwarded by this node.
static ROUND_ROBIN: AtomicU64 = AtomicU64::new(0);

/// Wire error for a cluster operation failure.
pub fn cluster_err(e: &crate::ClusterError) -> WireError {
    use crate::ClusterError;
    match e {
        ClusterError::Core(c) => WireError::from(c),
        ClusterError::Invalid(m) => WireError::new(ErrorCode::BadRequest, m.clone()),
        other => WireError::new(
            ErrorCode::Overloaded,
            format!("cluster unavailable: {other}"),
        ),
    }
}

fn not_leader(leader: &str) -> WireError {
    WireError::new(ErrorCode::NotLeader, leader)
}

/// How long a node named leader by the metadata waits for its role to become active (lease,
/// reconciler applying the role) before answering `NotLeader` — covers a freshly created
/// channel and a leadership that just moved here, so naive clients don't see spurious errors.
const ROLE_SETTLE: Duration = Duration::from_millis(1500);

/// Local append to a partition this node leads (or is about to lead). Waits up to
/// [`ROLE_SETTLE`] while the partition is temporarily unavailable or core reports
/// `NotLeader` — nothing has been appended in either case, so waiting never duplicates.
/// A partition led elsewhere is answered right away (`Remote`).
async fn publish_local(
    cluster: &Cluster,
    channel: &str,
    partition: u32,
    messages: Vec<PublishMessage>,
) -> Result<Result<Vec<MessageId>, WireError>, fluxmq_proto::cluster::NodeInfo> {
    // Common case: we lead the partition already, so the messages are moved, not cloned. If
    // leadership is lost between this check and the append (nothing appended), the client
    // gets `NotLeader` and re-routes.
    if matches!(
        cluster.route_partition(channel, partition),
        PartitionRoute::Local
    ) && cluster.broker().leads_partition(channel, partition)
    {
        return Ok(cluster
            .broker()
            .publish_to_partition(channel, partition, messages)
            .await
            .map_err(|e| WireError::from(&e)));
    }
    let deadline = Instant::now() + ROLE_SETTLE;
    loop {
        let settling = match cluster.route_partition(channel, partition) {
            PartitionRoute::Local => match cluster
                .broker()
                .publish_to_partition(channel, partition, messages.clone())
                .await
            {
                Err(CoreError::NotLeader { .. }) => true,
                r => return Ok(r.map_err(|e| WireError::from(&e))),
            },
            PartitionRoute::Remote(n) => return Err(n),
            PartitionRoute::Unavailable => true,
        };
        if settling && Instant::now() >= deadline {
            return Ok(Err(not_leader("")));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Partition-targeted publish (leader check, then local append).
pub(crate) async fn publish_partition(
    cluster: &Cluster,
    channel: &str,
    partition: u32,
    messages: Vec<PublishMessage>,
) -> Result<Vec<MessageId>, WireError> {
    match publish_local(cluster, channel, partition, messages).await {
        Ok(r) => r,
        Err(n) => Err(not_leader(&n.id)),
    }
}

async fn forward(
    cluster: &Cluster,
    http_addr: &str,
    req: ForwardPublish,
) -> Result<Vec<MessageId>, WireError> {
    let body = postcard::to_allocvec(&req)
        .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))?;
    let resp = cluster
        .internal_http()
        .post_bytes(http_addr, FORWARD_PUBLISH_PATH, FORWARD_CONTENT_TYPE, body)
        .await
        .map_err(|e| {
            // Leader unreachable / outcome unknown: `NotLeader` with an empty leader makes
            // clients refresh metadata and retry.
            tracing::debug!(error = %e, http_addr, "forwarded publish failed");
            not_leader("")
        })?;
    postcard::from_bytes::<Result<Vec<MessageId>, WireError>>(&resp)
        .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))?
}

/// Partition-less publish on a cluster node.
pub(crate) async fn publish_routed(
    cluster: &Cluster,
    channel: &str,
    messages: Vec<PublishMessage>,
) -> Result<Vec<MessageId>, WireError> {
    // Created through another node a moment ago: wait for the local reconciler.
    cluster.await_local_channel(channel).await;
    let Some(cfg) = cluster.broker().channel(channel) else {
        // Let the broker produce the canonical error (ChannelNotFound).
        return cluster
            .broker()
            .publish(channel, messages)
            .await
            .map_err(|e| WireError::from(&e));
    };
    let parts = cfg.partitions.max(1);
    // Same partitioner as core (sticky for keyless messages, v0.6).
    let mut sticky = fluxmq_core::StickyPartitioner::new(parts, &ROUND_ROBIN);
    // `Priority` channels: partition = the message's level (v0.12).
    let levels = (cfg.mode == fluxmq_proto::model::ChannelMode::Priority).then_some(parts);
    let total = messages.len();
    let mut groups: BTreeMap<u32, (Vec<usize>, Vec<PublishMessage>)> = BTreeMap::new();
    for (i, m) in messages.into_iter().enumerate() {
        let size = m.payload.len() + m.key.as_ref().map_or(0, String::len) + 32;
        let p = match levels {
            Some(l) => fluxmq_proto::model::priority_level(&m.headers, l),
            None => sticky.next(m.key.as_deref(), size),
        };
        let g = groups.entry(p).or_default();
        g.0.push(i);
        g.1.push(m);
    }
    let calls = groups.into_iter().map(|(p, (idx, msgs))| async move {
        let ids = match cluster.route_partition(channel, p) {
            PartitionRoute::Remote(n) => {
                let req = ForwardPublish {
                    channel: channel.to_string(),
                    partition: p,
                    messages: msgs,
                };
                forward(cluster, &n.http_addr, req).await
            }
            // Local, or settling (fresh channel / leadership moving): wait briefly here.
            PartitionRoute::Local | PartitionRoute::Unavailable => {
                match publish_local(cluster, channel, p, msgs.clone()).await {
                    Ok(r) => r,
                    Err(n) => {
                        let req = ForwardPublish {
                            channel: channel.to_string(),
                            partition: p,
                            messages: msgs,
                        };
                        forward(cluster, &n.http_addr, req).await
                    }
                }
            }
        }?;
        Ok::<_, WireError>((idx, ids))
    });
    let mut out: Vec<Option<MessageId>> = vec![None; total];
    for r in futures::future::join_all(calls).await {
        let (idx, ids) = r?;
        if ids.len() != idx.len() {
            return Err(WireError::new(
                ErrorCode::Internal,
                "partition leader returned a wrong number of ids",
            ));
        }
        for (i, id) in idx.into_iter().zip(ids) {
            out[i] = Some(id);
        }
    }
    Ok(out.into_iter().flatten().collect())
}

// ---------------------------------------------------------------------------------------------
// PublishBatch (v0.7): a producer-encoded records section routed as one unit, never decoded
// per message on the way (forwarding copies the bytes once into the internal request).
// ---------------------------------------------------------------------------------------------

/// Round-robin cursor for keyless partition-less batches forwarded by this node.
static BATCH_ROUND_ROBIN: AtomicU64 = AtomicU64::new(0);

/// Refusal for a keyed partition-less batch whose keys hash to several partitions (wire
/// contract: batches are never split).
pub(crate) const SPANS_PARTITIONS: &str = "keyed batch spans partitions; send partition: Some(p)";

/// Partition a partition-less batch must go to: `Some(p)` when it holds keys (all keyed
/// records must map to `p` by the channel key hash; keyless records follow), `None` when it
/// holds no key (the caller picks one, sticky per request). Walks the records section
/// without copying payloads; a compressed section is decompressed for the walk only (slow
/// path: SDKs send `partition: Some` once they know the partition count).
pub(crate) fn batch_key_partition(
    records: &RecordsSection,
    partitions: u32,
) -> Result<Option<u32>, WireError> {
    use fluxmq_proto::batch::{self, Compression, MAX_BATCH_LEN, Records, codec};
    let bad = |e: batch::BatchError| WireError::new(ErrorCode::BadRequest, e.to_string());
    let owned;
    let section: &[u8] = match records.compression {
        Compression::None => &records.body,
        c => {
            owned = codec::decompress(c, &records.body, MAX_BATCH_LEN).map_err(bad)?;
            &owned
        }
    };
    let mut keyed: Option<u32> = None;
    for r in Records::new(section, records.record_count) {
        let Some(key) = r.map_err(bad)?.key else {
            continue;
        };
        let p = batch::key_partition(key, partitions);
        match keyed {
            None => keyed = Some(p),
            Some(q) if q != p => {
                return Err(WireError::new(ErrorCode::BadRequest, SPANS_PARTITIONS));
            }
            Some(_) => {}
        }
    }
    Ok(keyed)
}

/// The one priority level of a batch for a `Priority` channel with `levels` levels (= its
/// partition); mixed levels → `BadRequest`.
fn batch_priority(records: &RecordsSection, levels: u32) -> Result<u32, WireError> {
    use fluxmq_proto::batch::{self, Compression, MAX_BATCH_LEN, codec};
    let bad = |e: String| WireError::new(ErrorCode::BadRequest, e);
    let owned;
    let section: &[u8] = match records.compression {
        Compression::None => &records.body,
        c => {
            owned = codec::decompress(c, &records.body, MAX_BATCH_LEN)
                .map_err(|e| bad(e.to_string()))?;
            &owned
        }
    };
    batch::section_priority(section, records.record_count, levels).map_err(bad)
}

/// Appends a batch to a partition this node leads (core: `Broker::publish_batch`).
async fn broker_publish_batch(
    cluster: &Cluster,
    channel: &str,
    partition: u32,
    records: RecordsSection,
) -> Result<(u32, u64, u32), CoreError> {
    cluster
        .broker()
        .publish_batch(channel, Some(partition), records)
        .await
}

/// [`publish_local`] for a batch (`records` clones are cheap: the body is `Bytes`).
async fn publish_batch_local(
    cluster: &Cluster,
    channel: &str,
    partition: u32,
    records: RecordsSection,
) -> Result<Result<(u64, u32), WireError>, fluxmq_proto::cluster::NodeInfo> {
    let to_wire = |r: Result<(u32, u64, u32), CoreError>| {
        r.map(|(_, base, count)| (base, count))
            .map_err(|e| WireError::from(&e))
    };
    if matches!(
        cluster.route_partition(channel, partition),
        PartitionRoute::Local
    ) && cluster.broker().leads_partition(channel, partition)
    {
        return Ok(to_wire(
            broker_publish_batch(cluster, channel, partition, records).await,
        ));
    }
    let deadline = Instant::now() + ROLE_SETTLE;
    loop {
        let settling = match cluster.route_partition(channel, partition) {
            PartitionRoute::Local => {
                match broker_publish_batch(cluster, channel, partition, records.clone()).await {
                    Err(CoreError::NotLeader { .. }) => true,
                    r => return Ok(to_wire(r)),
                }
            }
            PartitionRoute::Remote(n) => return Err(n),
            PartitionRoute::Unavailable => true,
        };
        if settling && Instant::now() >= deadline {
            return Ok(Err(not_leader("")));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// `PublishBatch { partition: Some(p) }`: served only by `p`'s leader (`NotLeader` with the
/// leader id otherwise, like `Publish`).
pub(crate) async fn publish_batch_partition(
    cluster: &Cluster,
    channel: &str,
    partition: u32,
    records: RecordsSection,
) -> Result<(u64, u32), WireError> {
    match publish_batch_local(cluster, channel, partition, records).await {
        Ok(r) => r,
        Err(n) => Err(not_leader(&n.id)),
    }
}

async fn forward_batch(
    cluster: &Cluster,
    http_addr: &str,
    req: &ForwardPublishBatch,
) -> Result<(u64, u32), WireError> {
    let body = postcard::to_allocvec(req)
        .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))?;
    let resp = cluster
        .internal_http()
        .post_bytes(
            http_addr,
            FORWARD_PUBLISH_BATCH_PATH,
            FORWARD_CONTENT_TYPE,
            body,
        )
        .await
        .map_err(|e| {
            tracing::debug!(error = %e, http_addr, "forwarded batch publish failed");
            not_leader("")
        })?;
    postcard::from_bytes::<Result<(u64, u32), WireError>>(&resp)
        .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))?
}

/// `PublishBatch { partition: None }` on a cluster node: one partition for the whole batch
/// (key hash, or sticky round-robin when keyless), appended locally or forwarded raw to
/// its leader. Returns `(partition, base_offset, count)`.
pub(crate) async fn publish_batch_routed(
    cluster: &Cluster,
    channel: &str,
    records: RecordsSection,
) -> Result<(u32, u64, u32), WireError> {
    cluster.await_local_channel(channel).await;
    let Some(cfg) = cluster.broker().channel(channel) else {
        return Err(WireError::from(&CoreError::ChannelNotFound(
            channel.to_string(),
        )));
    };
    let parts = cfg.partitions.max(1);
    let p = if cfg.mode == fluxmq_proto::model::ChannelMode::Priority {
        batch_priority(&records, parts)?
    } else {
        match batch_key_partition(&records, parts)? {
            Some(p) => p,
            None => fluxmq_core::StickyPartitioner::new(parts, &BATCH_ROUND_ROBIN)
                .next(None, records.body.len()),
        }
    };
    let remote = match cluster.route_partition(channel, p) {
        PartitionRoute::Remote(n) => n,
        PartitionRoute::Local | PartitionRoute::Unavailable => {
            match publish_batch_local(cluster, channel, p, records.clone()).await {
                Ok(r) => return r.map(|(base, count)| (p, base, count)),
                Err(n) => n,
            }
        }
    };
    let req = ForwardPublishBatch {
        channel: channel.to_string(),
        partition: p,
        records,
    };
    forward_batch(cluster, &remote.http_addr, &req)
        .await
        .map(|(base, count)| (p, base, count))
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use fluxmq_proto::batch::{Compression, codec, encode_record};

    fn section(keys: &[Option<&str>], c: Compression) -> RecordsSection {
        let mut b = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            encode_record(&mut b, i as u32, *k, &BTreeMap::new(), &[b'p'; 300]);
        }
        let (compression, body) = match codec::compress(c, &b) {
            Some(z) => (c, z),
            None => (Compression::None, b),
        };
        RecordsSection {
            compression,
            record_count: keys.len() as u32,
            body: body.into(),
        }
    }

    #[test]
    fn keyless_keyed_and_spanning_batches() {
        for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
            assert_eq!(
                batch_key_partition(&section(&[None, None], c), 8).unwrap(),
                None
            );
            let p = fluxmq_proto::batch::key_partition("k1", 8);
            assert_eq!(
                batch_key_partition(&section(&[None, Some("k1"), None, Some("k1")], c), 8).unwrap(),
                Some(p)
            );
            // Find a key that lands elsewhere.
            let other = (0..100)
                .map(|i| format!("x{i}"))
                .find(|k| fluxmq_proto::batch::key_partition(k, 8) != p)
                .unwrap();
            let e = batch_key_partition(&section(&[Some("k1"), Some(&other)], c), 8).unwrap_err();
            assert_eq!(e.code, ErrorCode::BadRequest);
            assert_eq!(e.message, SPANS_PARTITIONS);
        }
        // A malformed section is a BadRequest too.
        let mut bad = section(&[Some("k")], Compression::None);
        bad.record_count = 2;
        assert_eq!(
            batch_key_partition(&bad, 4).unwrap_err().code,
            ErrorCode::BadRequest
        );
    }
}

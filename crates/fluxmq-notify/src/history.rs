//! Persistent delivery history (v0.5): every [`NotificationRecord`] is appended, as JSON, to
//! the replicated system channel [`SYSTEM_NOTIFICATIONS_CHANNEL`] (1 partition, retention 7
//! days; reads expose at most the newest [`HISTORY_MAX_RECORDS`]), so the history survives a
//! controller change and a full restart.
//!
//! * **Only the controller appends** (single node: the node itself). Records produced on
//!   another node (a test send handled there) are forwarded to the controller over
//!   [`INTERNAL_HISTORY_APPEND_PATH`]. The dispatcher's records go through a background writer
//!   (batched, retried); test sends are persisted before the API call returns.
//! * Reads (`GET /api/notify/history`) come from a node's copy of the channel — the HTTP layer
//!   proxies them to the partition leader like the events feed — merged with that node's
//!   in-memory ring of recent records (not yet persisted ones, or when the channel can't be
//!   written), deduplicated by record id, newest first.
//!
//! The stored JSON is exactly the API's `NotificationRecord`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use fluxmq_core::{Broker, CoreError};
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::model::{ChannelConfig, ChannelMode, RetentionPolicy, headers};
use fluxmq_proto::notify::NotificationRecord;
use fluxmq_proto::{Message, PublishMessage};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

use crate::{Inner, NotifyError};

pub use fluxmq_proto::notify::SYSTEM_NOTIFICATIONS_CHANNEL;

pub const HISTORY_RETENTION_SECS: u64 = 7 * 24 * 3600;
/// Reads expose at most this many records (the newest).
pub const HISTORY_MAX_RECORDS: usize = 10_000;
/// Storage backstop besides the 7-day age limit (records are ~0.3–2 KiB, so 10 000 records fit
/// comfortably; segment-granular).
pub const HISTORY_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Internal (cluster-secret) route on the controller that appends records produced elsewhere.
pub const INTERNAL_HISTORY_APPEND_PATH: &str = "/internal/v1/notify/history/append";

const ENSURE_EVERY: Duration = Duration::from_secs(2);
/// Reads per page from the log (the broker's read cap).
const PAGE: u32 = 1000;

pub fn channel_config() -> ChannelConfig {
    let mut c = ChannelConfig::new(SYSTEM_NOTIFICATIONS_CHANNEL, ChannelMode::Stream);
    c.partitions = 1;
    c.retention = RetentionPolicy {
        max_age_secs: Some(HISTORY_RETENTION_SECS),
        max_bytes: Some(HISTORY_MAX_BYTES),
    };
    c.dlq_enabled = false;
    c
}

/// Creates the channel when missing and this node is the controller. `Ok(true)` = exists.
pub(crate) async fn ensure_channel(inner: &Inner) -> Result<bool, String> {
    crate::feed::ensure_system_channel(inner, channel_config()).await
}

/// Keeps the channel present and persists queued records in order (batched).
pub(crate) async fn run_writer(
    inner: Arc<Inner>,
    mut rx: mpsc::UnboundedReceiver<NotificationRecord>,
    mut stop: watch::Receiver<bool>,
) {
    let mut next = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep_until(next) => {
                let exists = ensure_channel(&inner).await.unwrap_or_else(|e| {
                    debug!(error = %e, "notification history channel not created yet");
                    false
                });
                next = crate::feed::next_ensure(exists, ENSURE_EVERY);
            }
            r = rx.recv() => {
                let Some(first) = r else { break };
                let mut batch = vec![first];
                while batch.len() < 256 {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }
                if let Err(e) = persist(&inner, batch.clone()).await {
                    warn!(error = %e, dropped = batch.len(), "notification history records not persisted (kept in memory only)");
                }
            }
        }
    }
}

/// Persists records: appended here on the controller, forwarded to it elsewhere. Retried.
pub(crate) async fn persist(inner: &Inner, records: Vec<NotificationRecord>) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 0..3u32 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
        }
        let r = if inner.leadership.is_controller() {
            append(inner, &records).await
        } else {
            forward(inner, &records).await
        };
        match r {
            Ok(()) => return Ok(()),
            Err(e) => {
                debug!(error = %e, attempt, "persisting notification history");
                last = e;
            }
        }
    }
    Err(last)
}

/// Appends to the channel (controller only; routed to the partition leader when clustered).
pub(crate) async fn append(inner: &Inner, records: &[NotificationRecord]) -> Result<(), String> {
    if records.is_empty() {
        return Ok(());
    }
    if !ensure_channel(inner).await? {
        return Err("the notification history channel does not exist yet".into());
    }
    let messages: Vec<PublishMessage> = records
        .iter()
        .filter_map(|r| {
            let payload = serde_json::to_vec(r).ok()?;
            let mut h = BTreeMap::new();
            h.insert(headers::CONTENT_TYPE.to_string(), "application/json".into());
            Some(PublishMessage {
                key: None,
                headers: h,
                payload: payload.into(),
            })
        })
        .collect();
    match (&inner.cluster, &inner.broker) {
        (Some(c), _) => c
            .publish(SYSTEM_NOTIFICATIONS_CHANNEL, None, messages)
            .await
            .map(drop)
            .map_err(|e| e.message),
        (None, Some(b)) => b
            .publish(SYSTEM_NOTIFICATIONS_CHANNEL, messages)
            .await
            .map(drop)
            .map_err(|e| e.to_string()),
        (None, None) => Err("no broker".into()),
    }
}

/// Hands records to the controller's [`INTERNAL_HISTORY_APPEND_PATH`].
async fn forward(inner: &Inner, records: &[NotificationRecord]) -> Result<(), String> {
    let cluster = inner.cluster.as_ref().ok_or("not clustered")?;
    // A channel filter matching nothing: only the topology.
    let md = cluster.metadata(&["\0".to_string()]);
    let controller = md.controller.ok_or("no controller elected")?;
    let node = md
        .nodes
        .iter()
        .find(|n| n.id == controller)
        .ok_or_else(|| format!("controller {controller} not in the registry"))?;
    cluster
        .internal_http()
        .post::<_, serde_json::Value>(&node.http_addr, INTERNAL_HISTORY_APPEND_PATH, records)
        .await
        .map(drop)
        .map_err(|e| format!("forwarding to controller {controller}: {e}"))
}

/// The newest `limit` records of this node's copy of the channel, newest first (empty when
/// the node holds no copy).
pub(crate) async fn read_local(
    broker: Broker,
    limit: usize,
) -> Result<Vec<NotificationRecord>, NotifyError> {
    let limit = limit.clamp(1, HISTORY_MAX_RECORDS) as u64;
    tokio::task::spawn_blocking(move || -> Result<Vec<NotificationRecord>, CoreError> {
        if broker.channel(SYSTEM_NOTIFICATIONS_CHANNEL).is_none() {
            return Ok(Vec::new());
        }
        let newest = MessageQuery {
            partition: Some(0),
            limit: Some(1),
            newest_first: Some(true),
            ..Default::default()
        };
        let (last, _, _) = broker.read_messages(SYSTEM_NOTIFICATIONS_CHANNEL, &newest)?;
        let Some(end) = last.iter().map(|m| m.offset + 1).max() else {
            return Ok(Vec::new());
        };
        let mut from = end.saturating_sub(limit);
        let mut out: Vec<NotificationRecord> = Vec::new();
        while from < end {
            let q = MessageQuery {
                partition: Some(0),
                offset: Some(from),
                limit: Some(PAGE),
                ..Default::default()
            };
            let (mut msgs, _, _) = broker.read_messages(SYSTEM_NOTIFICATIONS_CHANNEL, &q)?;
            msgs.sort_by_key(|m| m.offset);
            let Some(next) = msgs.last().map(|m| m.offset + 1) else {
                break;
            };
            out.extend(msgs.iter().filter(|m| m.offset < end).filter_map(to_record));
            from = next;
        }
        out.reverse();
        Ok(out)
    })
    .await
    .map_err(|e| NotifyError::Internal(e.to_string()))?
    .map_err(NotifyError::Core)
}

fn to_record(m: &Message) -> Option<NotificationRecord> {
    serde_json::from_slice(&m.payload).ok()
}

/// Stored records merged with the in-memory ring: deduplicated by id, newest first.
pub(crate) fn merge(
    stored: Vec<NotificationRecord>,
    ring: Vec<NotificationRecord>,
    limit: usize,
) -> Vec<NotificationRecord> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut all: Vec<NotificationRecord> = stored
        .into_iter()
        .chain(ring)
        .filter(|r| seen.insert(r.id.clone()))
        .collect();
    // Stable: equal timestamps keep the stored (append) order.
    all.sort_by_key(|r| std::cmp::Reverse(r.ts));
    all.truncate(limit);
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxmq_proto::notify::DeliveryStatus;

    fn rec(id: &str, ts: u64) -> NotificationRecord {
        NotificationRecord {
            id: id.into(),
            ts,
            rule_id: None,
            notifier_id: "n".into(),
            status: DeliveryStatus::Sent,
            summary: "s".into(),
            attempts: 1,
            error: None,
            error_detail: None,
        }
    }

    #[test]
    fn merge_dedups_and_orders_newest_first() {
        let stored = vec![rec("c", 30), rec("b", 20), rec("a", 10)];
        let ring = vec![rec("d", 40), rec("c", 30)];
        let m = merge(stored, ring, 10);
        let ids: Vec<&str> = m.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["d", "c", "b", "a"]);
        assert_eq!(merge(vec![rec("a", 1), rec("b", 2)], vec![], 1)[0].id, "b");
    }

    #[test]
    fn channel_is_a_bounded_single_partition_stream() {
        let c = channel_config();
        assert_eq!(c.partitions, 1);
        assert!(fluxmq_proto::model::is_system_channel(&c.name));
        assert_eq!(c.retention.max_age_secs, Some(HISTORY_RETENTION_SECS));
        assert!(!c.dlq_enabled);
    }
}

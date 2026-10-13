//! The `$system.events` pull feed: a 1-partition system channel (retention 7 days) holding
//! one JSON [`FeedItem`] per alert transition / broker event, appended by the controller
//! only, plus the long-poll reader behind `GET /api/events/feed`.
//!
//! Stored items carry `cursor: ""` (the offset isn't known before the append); the reader
//! fills in the message offset. SDK consumers of the raw channel use the message offset.

use std::sync::Arc;
use std::time::Duration;

use fluxmq_core::{Broker, CoreError};
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::model::{ChannelConfig, ChannelMode, RetentionPolicy, headers};
use fluxmq_proto::notify::{FeedItem, FeedPage, SYSTEM_EVENTS_CHANNEL};
use fluxmq_proto::{Message, PublishMessage};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

use crate::{Inner, NotifyError};

pub const FEED_RETENTION_SECS: u64 = 7 * 24 * 3600;
pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;
pub const MAX_WAIT: Duration = Duration::from_secs(30);
/// Re-read interval while long-polling (catches appends replicated from another node).
const POLL_EVERY: Duration = Duration::from_millis(250);
const ENSURE_EVERY: Duration = Duration::from_secs(2);
/// Re-check cadence while the channel doesn't exist yet (e.g. right after an election), so a
/// new controller creates it promptly; cheap when this node isn't the controller.
pub(crate) const ENSURE_RETRY: Duration = Duration::from_millis(250);

/// Next ensure check: soon while the channel is missing, then every `every`.
pub(crate) fn next_ensure(exists: bool, every: Duration) -> tokio::time::Instant {
    tokio::time::Instant::now() + if exists { every } else { ENSURE_RETRY }
}
/// Upper bound of one system channel creation attempt.
const CREATE_TIMEOUT: Duration = Duration::from_secs(15);

/// Where a feed read starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedAfter {
    /// From the oldest retained item.
    Start,
    /// Only items appended after now.
    Latest,
    /// Items after this offset.
    Offset(u64),
    /// The newest N retained items (1–1000), oldest first.
    Tail(u32),
}

impl FeedAfter {
    /// Parses the `after` / `tail` query parameters (mutually exclusive).
    pub fn from_query(after: Option<&str>, tail: Option<u32>) -> Result<Self, NotifyError> {
        match (after.map(str::trim).filter(|a| !a.is_empty()), tail) {
            (Some(_), Some(_)) => Err(NotifyError::Invalid(
                "tail and after are mutually exclusive".into(),
            )),
            (None, Some(n)) if (1..=MAX_LIMIT).contains(&n) => Ok(FeedAfter::Tail(n)),
            (None, Some(_)) => Err(NotifyError::Invalid(format!("tail must be 1-{MAX_LIMIT}"))),
            (after, None) => Self::parse(after),
        }
    }

    /// `None`/empty → oldest, `latest`, or a decimal offset.
    pub fn parse(s: Option<&str>) -> Result<Self, NotifyError> {
        match s.map(str::trim) {
            None | Some("") => Ok(FeedAfter::Start),
            Some("latest") => Ok(FeedAfter::Latest),
            Some(v) => v.parse::<u64>().map(FeedAfter::Offset).map_err(|_| {
                NotifyError::Invalid("after must be a cursor (decimal) or 'latest'".into())
            }),
        }
    }
}

pub fn channel_config() -> ChannelConfig {
    let mut c = ChannelConfig::new(SYSTEM_EVENTS_CHANNEL, ChannelMode::Stream);
    c.partitions = 1;
    c.retention = RetentionPolicy {
        max_age_secs: Some(FEED_RETENTION_SECS),
        max_bytes: None,
    };
    c.dlq_enabled = false;
    c
}

/// Creates the channel when missing and this node is the controller. `Ok(true)` = exists.
pub(crate) async fn ensure_channel(inner: &Inner) -> Result<bool, String> {
    ensure_system_channel(inner, channel_config()).await
}

/// Creates the system channel `cfg` when missing and this node is the controller (through the
/// cluster when clustered). `Ok(true)` = it exists (locally, or cluster-wide once created).
pub(crate) async fn ensure_system_channel(
    inner: &Inner,
    cfg: ChannelConfig,
) -> Result<bool, String> {
    let Some(broker) = &inner.broker else {
        return Ok(false);
    };
    let name = cfg.name.clone();
    if broker.channel(&name).is_some() {
        return Ok(true);
    }
    if let Some(c) = &inner.cluster
        && c.is_clustered()
        && c.metadata(std::slice::from_ref(&name))
            .channels
            .contains_key(&name)
    {
        // Placed cluster-wide; this node holds no replica of it.
        return Ok(true);
    }
    if !inner.leadership.is_controller() {
        return Ok(false);
    }
    let result = match &inner.cluster {
        // Bounded: a controller without a quorum can't commit it (retried by the callers).
        Some(c) => match tokio::time::timeout(CREATE_TIMEOUT, c.create_channel(cfg)).await {
            Ok(r) => r.map(drop).map_err(|e| e.to_string()),
            Err(_) => Err(format!("creating {name} timed out")),
        },
        None => match broker.create_channel(cfg) {
            Ok(_) | Err(CoreError::ChannelExists(_)) => Ok(()),
            Err(e) => Err(e.to_string()),
        },
    };
    match result {
        Ok(()) => Ok(true),
        Err(_) if broker.channel(&name).is_some() => Ok(true),
        Err(e) if e.contains("already exists") => Ok(true),
        Err(e) => Err(e),
    }
}

/// Keeps the channel present (retries until a controller created it) and appends items in
/// order. Bumps `appended` after every local append (wakes long-polls on this node).
pub(crate) async fn run_writer(
    inner: Arc<Inner>,
    mut rx: mpsc::UnboundedReceiver<FeedItem>,
    appended: watch::Sender<u64>,
    mut stop: watch::Receiver<bool>,
) {
    let mut next = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep_until(next) => {
                let exists = ensure_channel(&inner).await.unwrap_or_else(|e| {
                    debug!(error = %e, "system events channel not created yet");
                    false
                });
                next = next_ensure(exists, ENSURE_EVERY);
            }
            item = rx.recv() => {
                let Some(first) = item else { break };
                let mut batch = vec![first];
                while batch.len() < 256 {
                    match rx.try_recv() {
                        Ok(i) => batch.push(i),
                        Err(_) => break,
                    }
                }
                append(&inner, batch).await;
                appended.send_modify(|v| *v = v.wrapping_add(1));
            }
        }
    }
}

async fn append(inner: &Inner, items: Vec<FeedItem>) {
    let messages: Vec<PublishMessage> = items
        .iter()
        .filter_map(|i| {
            let payload = serde_json::to_vec(i).ok()?;
            let mut h = std::collections::BTreeMap::new();
            h.insert(headers::CONTENT_TYPE.to_string(), "application/json".into());
            h.insert(headers::EVENT_TYPE.to_string(), i.kind.clone());
            Some(PublishMessage {
                key: None,
                headers: h,
                payload: payload.into(),
            })
        })
        .collect();
    for attempt in 0..3u32 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
        }
        if let Err(e) = ensure_channel(inner).await {
            warn!(error = %e, "cannot create the system events channel");
            continue;
        }
        let result = match (&inner.cluster, &inner.broker) {
            (Some(c), _) => c
                .publish(SYSTEM_EVENTS_CHANNEL, None, messages.clone())
                .await
                .map(drop)
                .map_err(|e| e.message),
            (None, Some(b)) => b
                .publish(SYSTEM_EVENTS_CHANNEL, messages.clone())
                .await
                .map(drop)
                .map_err(|e| e.to_string()),
            (None, None) => return,
        };
        match result {
            Ok(()) => return,
            Err(e) => warn!(error = %e, attempt, "appending to the system events feed failed"),
        }
    }
    warn!(dropped = items.len(), "system events feed items dropped");
}

fn to_item(m: &Message) -> Option<FeedItem> {
    let mut item: FeedItem = serde_json::from_slice(&m.payload).ok()?;
    item.cursor = m.offset.to_string();
    Some(item)
}

fn read_from(broker: &Broker, from: u64, limit: u32) -> Result<Vec<Message>, CoreError> {
    let q = MessageQuery {
        partition: Some(0),
        offset: Some(from),
        limit: Some(limit),
        ..Default::default()
    };
    let (mut msgs, _, _) = broker.read_messages(SYSTEM_EVENTS_CHANNEL, &q)?;
    msgs.sort_by_key(|m| m.offset);
    Ok(msgs)
}

/// Next offset to be written (0 for an empty channel).
fn end_offset(broker: &Broker) -> Result<u64, CoreError> {
    let q = MessageQuery {
        partition: Some(0),
        limit: Some(1),
        newest_first: Some(true),
        ..Default::default()
    };
    let (msgs, _, _) = broker.read_messages(SYSTEM_EVENTS_CHANNEL, &q)?;
    Ok(msgs.iter().map(|m| m.offset + 1).max().unwrap_or(0))
}

/// Long-poll read. See `fluxmq_proto::notify` for semantics.
pub(crate) async fn read(
    broker: Broker,
    mut appended: watch::Receiver<u64>,
    after: FeedAfter,
    limit: u32,
    wait: Duration,
) -> Result<FeedPage, NotifyError> {
    let limit = limit.clamp(1, MAX_LIMIT);
    let deadline = tokio::time::Instant::now() + wait.min(MAX_WAIT);
    if broker.channel(SYSTEM_EVENTS_CHANNEL).is_none() {
        return Ok(FeedPage {
            items: vec![],
            next_cursor: match after {
                FeedAfter::Offset(o) => Some(o.to_string()),
                _ => None,
            },
            gap: false,
        });
    }
    let b = broker.clone();
    let (start, limit) = match after {
        FeedAfter::Start => (0, limit),
        FeedAfter::Offset(o) => (o.saturating_add(1), limit),
        FeedAfter::Latest => (blocking(move || end_offset(&b)).await?, limit),
        FeedAfter::Tail(n) => {
            let n = n.clamp(1, MAX_LIMIT);
            let end = blocking(move || end_offset(&b)).await?;
            (end.saturating_sub(u64::from(n)), n)
        }
    };
    // Cursor of an empty page: "continue after what the request already covered".
    let empty_cursor = match after {
        FeedAfter::Start => None,
        FeedAfter::Offset(o) => Some(o),
        FeedAfter::Latest => start.checked_sub(1),
        FeedAfter::Tail(_) => {
            let b = broker.clone();
            blocking(move || end_offset(&b)).await?.checked_sub(1)
        }
    };
    loop {
        appended.borrow_and_update();
        let b = broker.clone();
        let msgs = blocking(move || read_from(&b, start, limit)).await?;
        if !msgs.is_empty() || tokio::time::Instant::now() >= deadline {
            let gap = matches!(after, FeedAfter::Offset(_) | FeedAfter::Latest)
                && msgs.first().is_some_and(|m| m.offset > start);
            let items: Vec<FeedItem> = msgs.iter().filter_map(to_item).collect();
            let next_cursor = match msgs.last() {
                Some(m) => Some(m.offset.to_string()),
                None => empty_cursor.map(|o| o.to_string()),
            };
            return Ok(FeedPage {
                items,
                next_cursor,
                gap,
            });
        }
        let nap = POLL_EVERY.min(deadline.saturating_duration_since(tokio::time::Instant::now()));
        tokio::select! {
            r = appended.changed() => {
                if r.is_err() {
                    tokio::time::sleep(nap).await;
                }
            }
            () = tokio::time::sleep(nap) => {}
        }
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, CoreError> + Send + 'static,
) -> Result<T, NotifyError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| NotifyError::Internal(e.to_string()))?
        .map_err(NotifyError::Core)
}

//! The client handle: bootstrap, connection manager, metadata cache, request routing and the
//! publish retry loop.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fluxmq_proto::cluster::ClusterMetadata;
use fluxmq_proto::wire::{ClientRequest, ErrorCode, ServerResponse, WireError};
use fluxmq_proto::{ChannelConfig, MessageId, PublishMessage};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::conn::{self, Conn, EncodedRequest, ServerInfo, Signals};
use crate::consumer::{Consumer, SubscribeOptions};
use crate::error::{ClientError, Result};
use crate::metadata::{ClusterView, Topology, fix_addr, host_of, partition_for};
use crate::node::{self, NodeConn, Slot};
use crate::options::ClientOptions;
use crate::producer::{BatchingOptions, BatchingProducer, Producer};
use crate::records::EncodedRecords;

/// The messages of one publish request, in the form they were built in. `publish_routed`
/// converts to the other form only when the target connection needs it (a v4-encoded batch
/// meeting a v2/v3 broker, or the reverse).
pub(crate) enum PublishBody {
    /// `Publish` form (v2/v3 brokers, or built before the protocol was known).
    Messages(Vec<PublishMessage>),
    /// `PublishBatch` records section (v4).
    Records(EncodedRecords),
}

impl PublishBody {
    /// Builds the body in the form the client's latest handshake suggests.
    pub(crate) fn for_protocol(
        protocol: u16,
        messages: Vec<PublishMessage>,
        compression: fluxmq_proto::batch::Compression,
        mark: Option<String>,
    ) -> Result<Self> {
        if protocol >= 4 {
            Ok(PublishBody::Records(EncodedRecords::from_messages(
                &messages,
                compression,
                mark.as_deref(),
            )?))
        } else {
            Ok(PublishBody::Messages(messages))
        }
    }

    fn len(&self) -> usize {
        match self {
            PublishBody::Messages(m) => m.len(),
            PublishBody::Records(r) => r.section.record_count as usize,
        }
    }

    fn has_keys(&self) -> bool {
        match self {
            PublishBody::Messages(m) => m.iter().any(|m| m.key.is_some()),
            PublishBody::Records(r) => r.has_keys,
        }
    }
}

/// Retry backoff between publish attempts.
const PUBLISH_BACKOFF_INITIAL: Duration = Duration::from_millis(50);
const PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// Longest single wait for a node connection inside the publish loop; after that the leader
/// is re-resolved from (possibly refreshed) metadata.
const PUBLISH_CONN_WAIT: Duration = Duration::from_secs(2);
/// Minimum age of the metadata before a "channel not in metadata" lookup re-fetches it.
const MISSING_CHANNEL_REFRESH: Duration = Duration::from_millis(250);

/// Observable connection state, see [`Client::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Establishing the first connection.
    Connecting,
    /// Every connection the client needs is up.
    Connected,
    /// At least one connection was lost and is being re-established. Publishes wait/retry
    /// (up to `publish_timeout`), consumers resubscribe automatically.
    Reconnecting,
    /// Closed by [`Client::close`], or the reconnect policy gave up. Terminal.
    Closed,
}

/// Shared client state. Background tasks hold `Arc<Inner>` and stop when `cancel` fires
/// (the last [`Client`] clone dropped, [`Client::close`], or the bootstrap connection failed
/// for good).
pub(crate) struct Inner {
    pub(crate) opts: Arc<ClientOptions>,
    pub(crate) signals: Arc<Signals>,
    pub(crate) cancel: CancellationToken,
    state_tx: watch::Sender<ConnectionState>,
    closed_reason: Mutex<Option<String>>,
    reconnects: AtomicU64,
    seeds: Vec<String>,
    pub(crate) primary: Arc<NodeConn>,
    nodes: Mutex<HashMap<String, Arc<NodeConn>>>,
    topology: watch::Sender<Arc<Topology>>,
    meta_lock: tokio::sync::Mutex<()>,
    meta_gen: AtomicU64,
    last_refresh: Mutex<Option<Instant>>,
    rr: Mutex<HashMap<String, Arc<AtomicU64>>>,
    info: Mutex<ServerInfo>,
    /// Idempotent publishing: this client's producer id and its next batch number.
    producer_id: u64,
    batch_seq: AtomicU64,
}

impl Inner {
    /// A fresh `flux-pseq` mark for one batch (kept by its retries), or `None` when
    /// idempotent publishing is off.
    pub(crate) fn next_pseq(&self) -> Option<String> {
        self.opts.idempotent.then(|| {
            format!(
                "{:016x}.{}",
                self.producer_id,
                self.batch_seq.fetch_add(1, Ordering::Relaxed)
            )
        })
    }

    pub(crate) fn closed_reason(&self) -> Option<String> {
        self.closed_reason.lock().clone()
    }

    pub(crate) fn closed_error(&self) -> ClientError {
        ClientError::Closed(
            self.closed_reason()
                .unwrap_or_else(|| "client closed".into()),
        )
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub(crate) fn close_with(&self, reason: String) {
        {
            let mut r = self.closed_reason.lock();
            if r.is_none() {
                *r = Some(reason);
            }
        }
        self.cancel.cancel();
        self.state_tx.send_if_modified(|s| {
            let changed = *s != ConnectionState::Closed;
            *s = ConnectionState::Closed;
            changed
        });
    }

    /// Recomputes [`ConnectionState`] from the node slots.
    pub(crate) fn on_slot_change(&self) {
        if self.cancel.is_cancelled() {
            self.close_with("client closed".into());
            return;
        }
        let mut degraded = false;
        match self.primary.slot() {
            Slot::Failed(e) => {
                let reason = match e {
                    ClientError::Closed(r) => r,
                    other => other.to_string(),
                };
                self.close_with(reason);
                return;
            }
            Slot::Down { .. } => degraded = true,
            Slot::Up(_) => {}
        }
        for n in self.nodes.lock().values() {
            if let Slot::Down { attempts, ever, .. } = n.slot()
                && (attempts > 0 || ever)
            {
                degraded = true;
            }
        }
        let new = if degraded {
            ConnectionState::Reconnecting
        } else {
            ConnectionState::Connected
        };
        self.state_tx.send_if_modified(|s| {
            let changed = *s != new && *s != ConnectionState::Closed;
            if changed {
                *s = new;
            }
            changed
        });
    }

    pub(crate) fn on_connected(&self, node: &NodeConn, conn: &Conn, reconnected: bool) {
        if node.id.is_none() {
            *self.info.lock() = conn.info();
        }
        if reconnected {
            self.reconnects.fetch_add(1, Ordering::Relaxed);
            tracing::info!(node = %node.label(), addr = %conn.addr, "fluxmq reconnected");
        }
        self.on_slot_change();
        self.signals.request_refresh();
    }

    pub(crate) fn topology(&self) -> Arc<Topology> {
        self.topology.borrow().clone()
    }

    pub(crate) fn watch_topology(&self) -> watch::Receiver<Arc<Topology>> {
        self.topology.subscribe()
    }

    /// The (lazily created) connection manager for cluster node `id`.
    pub(crate) fn node(self: &Arc<Self>, id: &str) -> Option<Arc<NodeConn>> {
        let topo = self.topology();
        let mut nodes = self.nodes.lock();
        if let Some(n) = nodes.get(id)
            && !n.is_failed()
        {
            return Some(n.clone());
        }
        let Topology::Cluster(view) = &*topo else {
            return None;
        };
        let addr = view.addr_of(id)?;
        let fallback = self.primary_host();
        let n = NodeConn::new(
            Some(id.to_string()),
            vec![fix_addr(addr, fallback.as_deref())],
            self.cancel.child_token(),
            None,
        );
        if self.cancel.is_cancelled() {
            return None;
        }
        tokio::spawn(node::supervise(n.clone(), self.clone()));
        nodes.insert(id.to_string(), n.clone());
        Some(n)
    }

    /// The bootstrap connection is down (reconnecting) and its last handshake was with `id`,
    /// and no dedicated connection to `id` exists.
    fn primary_was(&self, id: &str) -> bool {
        self.primary.current().is_none()
            && !self.primary.is_failed()
            && self.info.lock().node_id == id
            && !self.nodes.lock().contains_key(id)
    }

    fn primary_host(&self) -> Option<String> {
        let c = self.primary.current()?;
        host_of(&c.addr).map(str::to_string)
    }

    /// The live connection for `target` right now (`None` = bootstrap), without waiting.
    /// Also returns a watch to wait on when it isn't up.
    pub(crate) fn try_conn_for(
        self: &Arc<Self>,
        target: Option<&str>,
    ) -> (Option<Arc<Conn>>, Option<watch::Receiver<Slot>>) {
        match target {
            None => (self.primary.current(), Some(self.primary.watch())),
            Some(id) => {
                if let Some(c) = self.primary.current()
                    && c.node_id() == id
                {
                    return (Some(c), None);
                }
                if self.primary_was(id) {
                    return (None, Some(self.primary.watch()));
                }
                match self.node(id) {
                    Some(n) => (n.current(), Some(n.watch())),
                    None => (None, None),
                }
            }
        }
    }

    /// Waits (until `deadline`) for a connection to `target` (`None` = bootstrap).
    pub(crate) async fn conn_for(
        self: &Arc<Self>,
        target: Option<&str>,
        deadline: Instant,
        fail_fast: bool,
    ) -> Result<Arc<Conn>> {
        if self.is_closed() {
            return Err(self.closed_error());
        }
        match target {
            None => self.primary.get(deadline, fail_fast).await,
            Some(id) => {
                if let Some(c) = self.primary.current()
                    && c.node_id() == id
                {
                    return Ok(c);
                }
                if self.primary_was(id) {
                    // The bootstrap connection is reconnecting and was on this node: wait for
                    // it rather than opening a second connection to the same node.
                    let c = self.primary.get(deadline, fail_fast).await?;
                    if c.node_id() == id {
                        return Ok(c);
                    }
                }
                match self.node(id) {
                    Some(n) => n.get(deadline, fail_fast).await,
                    None => {
                        self.signals.request_refresh();
                        Err(ClientError::Closed(format!(
                            "node {id} is not in the cluster metadata"
                        )))
                    }
                }
            }
        }
    }

    /// Request on the bootstrap connection. Waits for a connection up to `request_timeout`;
    /// with `idempotent`, a request lost with its connection is re-sent on the next one.
    pub(crate) async fn primary_request(
        &self,
        body: ClientRequest,
        idempotent: bool,
    ) -> Result<ServerResponse> {
        if self.is_closed() {
            return Err(self.closed_error());
        }
        let deadline = Instant::now() + self.opts.request_timeout;
        let enc = EncodedRequest::new(&body)?;
        loop {
            let conn = self.primary.get(deadline, false).await?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ClientError::Timeout);
            }
            match conn.request_encoded(&enc, None, remaining).await {
                Err(e) if idempotent && e.is_connection_error() && !self.is_closed() => {
                    tracing::debug!(error = %e, "request lost with its connection; re-sending");
                    continue;
                }
                r => return r,
            }
        }
    }

    // ---- metadata -------------------------------------------------------------------------

    /// Fetches metadata now (coalesced: concurrent callers share one fetch).
    pub(crate) async fn refresh_metadata(self: &Arc<Self>) -> Result<()> {
        let gen_before = self.meta_gen.load(Ordering::Acquire);
        let _g = self.meta_lock.lock().await;
        if self.meta_gen.load(Ordering::Acquire) != gen_before {
            return Ok(()); // someone else refreshed while we waited
        }
        let r = self.fetch_metadata().await;
        *self.last_refresh.lock() = Some(Instant::now());
        self.meta_gen.fetch_add(1, Ordering::AcqRel);
        r
    }

    /// Refreshes unless metadata was fetched less than `min_age` ago.
    pub(crate) async fn refresh_if_stale(self: &Arc<Self>, min_age: Duration) {
        let fresh = self
            .last_refresh
            .lock()
            .is_some_and(|t| t.elapsed() < min_age);
        if !fresh && let Err(e) = self.refresh_metadata().await {
            tracing::debug!(error = %e, "metadata refresh failed");
        }
    }

    async fn fetch_metadata(self: &Arc<Self>) -> Result<()> {
        let req = ClientRequest::Metadata {
            channels: Vec::new(),
        };
        match self.primary_request(req, true).await {
            Ok(ServerResponse::Metadata { json }) => {
                let meta: ClusterMetadata = serde_json::from_str(&json)
                    .map_err(|e| ClientError::Protocol(format!("bad metadata json: {e}")))?;
                self.apply_topology(Topology::Cluster(ClusterView::new(meta)));
                Ok(())
            }
            Ok(other) => Err(unexpected(other)),
            // Brokers without cluster support (or tokens not allowed to read metadata):
            // behave like v1 — everything via the bootstrap node, broker-side partitioning.
            Err(ClientError::Server(e))
                if matches!(
                    e.code,
                    ErrorCode::BadRequest
                        | ErrorCode::NotFound
                        | ErrorCode::Forbidden
                        | ErrorCode::Unauthorized
                ) =>
            {
                if self.topology().is_cluster() {
                    tracing::info!(reason = %e, "broker no longer serves metadata; single-node mode");
                } else {
                    tracing::debug!(reason = %e, "broker does not serve metadata; single-node mode");
                }
                self.apply_topology(Topology::Single);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn apply_topology(self: &Arc<Self>, t: Topology) {
        let fallback = self.primary_host();
        match &t {
            Topology::Cluster(v) => {
                let mut addrs = self.seeds.clone();
                for a in v.live_addrs() {
                    let a = fix_addr(&a, fallback.as_deref());
                    if !addrs.contains(&a) {
                        addrs.push(a);
                    }
                }
                self.primary.set_addrs(addrs);
                self.nodes.lock().retain(|id, n| match v.addr_of(id) {
                    Some(a) => {
                        n.set_addrs(vec![fix_addr(a, fallback.as_deref())]);
                        true
                    }
                    None => {
                        n.stop();
                        false
                    }
                });
            }
            Topology::Single => {
                self.primary.set_addrs(self.seeds.clone());
                for (_, n) in self.nodes.lock().drain() {
                    n.stop();
                }
            }
        }
        let changed = self.topology.send_if_modified(|cur| {
            if **cur != t {
                *cur = Arc::new(t);
                true
            } else {
                false
            }
        });
        if changed {
            tracing::debug!(epoch = self.topology().epoch(), "topology updated");
        }
        self.on_slot_change();
    }

    fn rr_counter(&self, channel: &str) -> Arc<AtomicU64> {
        let mut rr = self.rr.lock();
        if let Some(c) = rr.get(channel) {
            return c.clone();
        }
        let c = Arc::new(AtomicU64::new(0));
        rr.insert(channel.to_string(), c.clone());
        c
    }

    /// The topology for routing `channel`; re-fetches metadata (rate-limited) when a cluster
    /// doesn't know the channel yet (e.g. just created).
    pub(crate) async fn topology_for(self: &Arc<Self>, channel: &str) -> Arc<Topology> {
        let t = self.topology();
        if t.is_cluster() && t.partitions(channel).is_none() {
            self.refresh_if_stale(MISSING_CHANNEL_REFRESH).await;
            return self.topology();
        }
        t
    }

    /// Client-side partition for a message (`None` = let the broker decide: single node or a
    /// channel the metadata doesn't know).
    pub(crate) fn partition_of(
        &self,
        topo: &Topology,
        channel: &str,
        key: Option<&str>,
    ) -> Option<u32> {
        let n = topo.partitions(channel)?;
        let rr = if key.is_none() {
            Some(self.rr_counter(channel))
        } else {
            None
        };
        Some(partition_for(key, n, &mut || {
            rr.as_ref()
                .map(|c| c.fetch_add(1, Ordering::Relaxed))
                .unwrap_or(0)
        }))
    }

    /// Publishes `body` to `partition` (or broker-chosen with `None`) of `channel`,
    /// retrying on connection loss, `NotLeader` and `Overloaded` until `deadline`.
    ///
    /// The request form follows the connection it goes out on: `PublishBatch` on protocol v4
    /// (except a keyed batch without a partition, which only `Publish` lets the broker split),
    /// `Publish` otherwise. Each form is encoded at most once per partition and reused across
    /// retries.
    pub(crate) async fn publish_routed(
        self: &Arc<Self>,
        channel: &str,
        partition: Option<u32>,
        body: PublishBody,
        deadline: Instant,
    ) -> Result<Vec<MessageId>> {
        let n = body.len();
        let has_keys = body.has_keys();
        let (mut messages, mut records) = match body {
            PublishBody::Messages(m) => (Some(m), None),
            PublishBody::Records(r) => (None, Some(r)),
        };
        // (partition, records form?, encoding)
        let mut encoded: Option<(Option<u32>, bool, EncodedRequest)> = None;
        // Delayed messages (v0.12) must reach the broker as a `Publish` it decodes.
        // One mark for this logical batch, should it be encoded as records below.
        let conv_mark = if records.is_none() {
            self.next_pseq()
        } else {
            None
        };
        // So must priority messages without a known partition (the broker places each by level).
        let force_messages = messages.as_ref().is_some_and(|m| {
            m.iter().any(|m| {
                fluxmq_proto::scheduled::is_delayed(&m.headers)
                    || (partition.is_none()
                        && m.headers
                            .contains_key(fluxmq_proto::model::headers::PRIORITY))
            })
        });
        let mut attempt: u32 = 0;
        let mut hint: Option<String> = None;
        let mut rejected_by: Option<String> = None;
        let mut last_err: Option<ClientError> = None;
        loop {
            if self.is_closed() {
                return Err(self.closed_error());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(last_err.unwrap_or(ClientError::Timeout));
            }
            let topo = self.topology();
            let (target, part) = match partition {
                Some(p) if topo.is_cluster() => {
                    let leader = topo.leader(channel, p);
                    let hinted = hint.take().filter(|h| match &*topo {
                        Topology::Cluster(v) => v.has_node(h),
                        Topology::Single => false,
                    });
                    let chosen = match (leader, hinted) {
                        (Some(l), Some(h)) if rejected_by.as_ref() == Some(&l) => Some(h),
                        (Some(l), _) => Some(l),
                        (None, h) => h,
                    };
                    match chosen {
                        Some(l) => (Some(l), Some(p)),
                        None => {
                            last_err = Some(ClientError::Server(WireError::new(
                                ErrorCode::NotLeader,
                                format!("no leader for {channel}/{p}"),
                            )));
                            self.signals.request_refresh();
                            self.publish_backoff(&mut attempt, deadline).await;
                            continue;
                        }
                    }
                }
                _ => (None, None),
            };
            let wait_until = deadline.min(now + PUBLISH_CONN_WAIT);
            let conn = match self.conn_for(target.as_deref(), wait_until, false).await {
                Ok(c) => c,
                Err(e) if e.is_retryable() && !self.is_closed() => {
                    tracing::debug!(channel, ?target, error = %e, "publish: no connection yet");
                    last_err = Some(e);
                    if Instant::now() < wait_until {
                        // Failed fast (unknown node): don't spin.
                        self.publish_backoff(&mut attempt, deadline).await;
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let as_records =
                !force_messages && conn.protocol() >= 4 && !(part.is_none() && has_keys);
            if encoded.as_ref().map(|(p, r, _)| (*p, *r)) != Some((part, as_records)) {
                let enc = if as_records {
                    if records.is_none() {
                        let m = messages.as_deref().expect("one form is always present");
                        records = Some(EncodedRecords::from_messages(
                            m,
                            self.opts.compression,
                            conv_mark.as_deref(),
                        )?);
                    }
                    let r = records.as_ref().expect("built above");
                    EncodedRequest::publish_batch(channel, part, &r.section)?
                } else {
                    if messages.is_none() {
                        let r = records.as_ref().expect("one form is always present");
                        messages = Some(r.to_messages()?);
                    }
                    let m = messages.as_deref().expect("built above");
                    EncodedRequest::publish_legacy(channel, part, m)?
                };
                encoded = Some((part, as_records, enc));
            }
            let enc = &encoded.as_ref().expect("encoded above").2;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let timeout = remaining.min(self.opts.request_timeout);
            match conn.request_encoded(enc, None, timeout).await {
                Ok(ServerResponse::PublishOk { ids }) if ids.len() == n => return Ok(ids),
                Ok(ServerResponse::PublishOk { ids }) => {
                    return Err(ClientError::Protocol(format!(
                        "PublishOk returned {} ids for {n} messages",
                        ids.len()
                    )));
                }
                Ok(ServerResponse::PublishBatchOk {
                    partition,
                    base_offset,
                    count,
                }) if count as usize == n => {
                    return Ok((0..u64::from(count))
                        .map(|i| MessageId {
                            partition,
                            offset: base_offset + i,
                        })
                        .collect());
                }
                Ok(ServerResponse::PublishBatchOk { count, .. }) => {
                    return Err(ClientError::Protocol(format!(
                        "PublishBatchOk returned {count} ids for {n} messages"
                    )));
                }
                Ok(other) => return Err(unexpected(other)),
                Err(e) => match e.code() {
                    Some(ErrorCode::NotLeader) => {
                        let leader_hint = match &e {
                            ClientError::Server(w) => w.message.trim().to_string(),
                            _ => String::new(),
                        };
                        tracing::debug!(channel, ?part, node = %conn.node_id(), hint = %leader_hint, "publish: not leader");
                        rejected_by = Some(conn.node_id().to_string());
                        hint = (!leader_hint.is_empty() && leader_hint != conn.node_id())
                            .then_some(leader_hint);
                        let _ = self.refresh_metadata().await;
                        last_err = Some(e);
                        if attempt == 0 {
                            attempt = 1;
                            continue; // first redirect: retry immediately
                        }
                    }
                    Some(ErrorCode::Overloaded) => {
                        tracing::debug!(channel, error = %e, "publish: overloaded; retrying");
                        last_err = Some(e);
                    }
                    // A node that just took over / rejoined may serve requests before it
                    // created the channel locally: retry while the metadata still lists it.
                    Some(ErrorCode::NotFound) if part.is_some() => {
                        let _ = self.refresh_metadata().await;
                        if self.topology().partitions(channel).is_none() {
                            return Err(e);
                        }
                        tracing::debug!(channel, error = %e, "publish: channel not on leader yet; retrying");
                        last_err = Some(e);
                    }
                    // The node didn't answer in `request_timeout` (e.g. stalled waiting for
                    // replicas during a failover): outcome unknown, retry until the deadline.
                    _ if matches!(e, ClientError::Timeout) && !self.is_closed() => {
                        tracing::debug!(channel, node = %conn.node_id(), "publish: request timed out; retrying");
                        self.signals.request_refresh();
                        last_err = Some(e);
                    }
                    _ if e.is_connection_error() && !self.is_closed() => {
                        tracing::debug!(channel, error = %e, "publish: connection lost; retrying");
                        self.signals.request_refresh();
                        last_err = Some(e);
                    }
                    // BadRequest, Forbidden, Unauthorized, NotFound (single node), ...
                    _ => return Err(e),
                },
            }
            self.publish_backoff(&mut attempt, deadline).await;
        }
    }

    async fn publish_backoff(&self, attempt: &mut u32, deadline: Instant) {
        let d = crate::util::backoff(PUBLISH_BACKOFF_INITIAL, PUBLISH_BACKOFF_MAX, *attempt, 0.2);
        *attempt = attempt.saturating_add(1);
        let until = deadline.min(Instant::now() + d);
        tokio::select! {
            _ = tokio::time::sleep_until(until) => {}
            _ = self.cancel.cancelled() => {}
        }
    }
}

async fn metadata_task(inner: Arc<Inner>) {
    let every = inner.opts.metadata_refresh_interval;
    let mut tick = tokio::time::interval(if every.is_zero() {
        Duration::from_secs(3600 * 24 * 365)
    } else {
        every
    });
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        tokio::select! {
            _ = inner.cancel.cancelled() => break,
            _ = tick.tick(), if !every.is_zero() => {}
            _ = inner.signals.meta_refresh.notified() => {}
        }
        if let Err(e) = inner.refresh_metadata().await {
            tracing::debug!(error = %e, "metadata refresh failed");
            if inner.cancel.is_cancelled() {
                break;
            }
        }
    }
}

struct Handle {
    inner: Arc<Inner>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Last `Client` clone gone: writers flush what is queued and close their sockets.
        self.inner.close_with("closed by client".into());
    }
}

/// A connection manager for a FluxMQ broker or cluster. Cheap to clone; all clones share the
/// same connections.
///
/// * Connects to the first reachable seed, fetches cluster metadata and connects lazily to
///   partition leaders. Against a single node (or a broker without cluster support) everything
///   uses that one connection.
/// * Lost connections are re-established automatically with exponential backoff + jitter
///   ([`ClientOptions::reconnect`]); observe it with [`Client::state`].
/// * Publishes are retried on connection loss / timeout / `NotLeader` / `Overloaded` until
///   [`ClientOptions::publish_timeout`]. **A retried publish may be stored twice** (the broker
///   may have persisted it before the connection dropped) — delivery is at-least-once, so make
///   consumers idempotent (e.g. by a business key).
/// * Consumers resubscribe transparently after reconnects and leadership changes.
///
/// Everything is closed when the last clone (including those held by [`Consumer`]s and
/// producers) is dropped, or explicitly with [`Client::close`].
#[derive(Clone)]
pub struct Client {
    handle: Arc<Handle>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("client_name", &self.inner().opts.client_name)
            .field("server", &self.server_info())
            .field("state", &self.connection_state())
            .finish()
    }
}

impl Client {
    /// Connects to the first reachable seed, performs the `Hello` handshake, fetches cluster
    /// metadata and starts the background tasks (reconnect, metadata refresh).
    ///
    /// Fails if no seed is reachable (unless [`ReconnectPolicy::retry_initial_connect`]) or the
    /// handshake is rejected (e.g. `Unauthorized`).
    ///
    /// [`ReconnectPolicy::retry_initial_connect`]: crate::ReconnectPolicy::retry_initial_connect
    pub async fn connect(opts: ClientOptions) -> Result<Client> {
        let seeds = opts.seed_list();
        if seeds.is_empty() {
            return Err(ClientError::Io("no broker address configured".into()));
        }
        let opts = Arc::new(opts);
        let signals = Arc::new(Signals::default());
        let (conn, idx) = initial_connect(&seeds, &opts, &signals).await?;
        let cancel = CancellationToken::new();
        let info = conn.info();
        let primary = NodeConn::new(None, seeds.clone(), cancel.child_token(), Some((conn, idx)));
        let inner = Arc::new(Inner {
            opts,
            signals,
            cancel,
            state_tx: watch::Sender::new(ConnectionState::Connected),
            closed_reason: Mutex::new(None),
            reconnects: AtomicU64::new(0),
            seeds,
            primary: primary.clone(),
            nodes: Mutex::new(HashMap::new()),
            topology: watch::Sender::new(Arc::new(Topology::Single)),
            meta_lock: tokio::sync::Mutex::new(()),
            meta_gen: AtomicU64::new(0),
            last_refresh: Mutex::new(None),
            rr: Mutex::new(HashMap::new()),
            info: Mutex::new(info),
            producer_id: crate::util::fast_rand(),
            batch_seq: AtomicU64::new(1),
        });
        let client = Client {
            handle: Arc::new(Handle {
                inner: inner.clone(),
            }),
        };
        tokio::spawn(node::supervise(primary, inner.clone()));
        if let Err(e) = inner.refresh_metadata().await {
            tracing::debug!(error = %e, "initial metadata fetch failed; will retry");
        }
        tokio::spawn(metadata_task(inner));
        Ok(client)
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.handle.inner
    }

    /// Watch the connection state (`Connecting` → `Connected` ⇄ `Reconnecting` → `Closed`).
    pub fn state(&self) -> watch::Receiver<ConnectionState> {
        self.inner().state_tx.subscribe()
    }

    /// The current connection state.
    pub fn connection_state(&self) -> ConnectionState {
        *self.inner().state_tx.borrow()
    }

    /// Number of successful automatic reconnects (all nodes) since the client was created.
    pub fn reconnects(&self) -> u64 {
        self.inner().reconnects.load(Ordering::Relaxed)
    }

    /// Broker info from the latest handshake on the bootstrap connection.
    pub fn server_info(&self) -> ServerInfo {
        self.inner().info.lock().clone()
    }

    /// The name this client announced in `Hello`.
    pub fn client_name(&self) -> &str {
        &self.inner().opts.client_name
    }

    /// The options this client was created with.
    pub fn options(&self) -> &ClientOptions {
        &self.inner().opts
    }

    /// Cached cluster metadata; `None` when the broker runs as a single node without cluster
    /// support (everything then goes through the bootstrap connection).
    pub fn metadata(&self) -> Option<ClusterMetadata> {
        match &*self.inner().topology() {
            Topology::Cluster(v) => Some(v.meta.clone()),
            Topology::Single => None,
        }
    }

    /// Re-fetches cluster metadata now.
    pub async fn refresh_metadata(&self) -> Result<()> {
        self.inner().refresh_metadata().await
    }

    /// True once the client is closed for good ([`ConnectionState::Closed`]).
    pub fn is_closed(&self) -> bool {
        self.inner().is_closed()
    }

    /// Closes all connections: queued frames are flushed, then pending requests fail with
    /// [`ClientError::Closed`] and consumers end.
    pub fn close(&self) {
        self.inner().close_with("closed by client".into());
    }

    /// Creates a channel. Fails with `ErrorCode::AlreadyExists` if it exists; see
    /// [`Client::ensure_channel`]. Not retried if the connection drops mid-request (the
    /// error is then a connection error; `ensure_channel` is safe to call again).
    pub async fn create_channel(&self, config: ChannelConfig) -> Result<()> {
        match self
            .inner()
            .primary_request(ClientRequest::CreateChannel(config), false)
            .await?
        {
            ServerResponse::ChannelCreated | ServerResponse::Ok => {
                if self.inner().topology().is_cluster() {
                    let _ = self.inner().refresh_metadata().await;
                }
                Ok(())
            }
            other => Err(unexpected(other)),
        }
    }

    /// Creates the channel unless it already exists. Returns `true` if it was created.
    pub async fn ensure_channel(&self, config: ChannelConfig) -> Result<bool> {
        match self.create_channel(config).await {
            Ok(()) => Ok(true),
            Err(e) if e.code() == Some(ErrorCode::AlreadyExists) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Publishes one message and waits for the broker to assign its id. Retried as described
    /// on [`Client`] (may duplicate on retry).
    pub async fn publish(&self, channel: &str, message: PublishMessage) -> Result<MessageId> {
        let mut ids = self.publish_batch(channel, vec![message]).await?;
        ids.pop()
            .ok_or_else(|| ClientError::Protocol("PublishOk without ids".into()))
    }

    /// Publishes several messages. Ids are returned in order.
    ///
    /// In a cluster the batch is split by partition (same function as the broker:
    /// `crc32(key) % partitions`, round-robin without a key) and each part goes straight to its
    /// partition leader. If some parts succeed and another fails for good, the error is
    /// returned although the successful parts are stored.
    pub async fn publish_batch(
        &self,
        channel: &str,
        messages: Vec<PublishMessage>,
    ) -> Result<Vec<MessageId>> {
        let n = messages.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let inner = self.inner();
        let deadline = Instant::now() + inner.opts.publish_timeout;
        let protocol = inner.signals.protocol_hint();
        let compression = inner.opts.compression;
        // Delayed messages (v0.12) go as a plain `Publish` the broker reads (not as records it
        // stores untouched), routed by the broker.
        if messages
            .iter()
            .any(|m| fluxmq_proto::scheduled::is_delayed(&m.headers))
        {
            return inner
                .publish_routed(channel, None, PublishBody::Messages(messages), deadline)
                .await;
        }
        let topo = inner.topology_for(channel).await;
        let Some(partitions) = topo.partitions(channel) else {
            let body =
                PublishBody::for_protocol(protocol, messages, compression, inner.next_pseq())?;
            return inner.publish_routed(channel, None, body, deadline).await;
        };
        // Keyed messages go to their key's partition; keyless ones stick to one partition per
        // call (one batch instead of one per partition).
        let mut sticky: Option<u32> = None;
        let priority = topo.is_priority(channel);
        let mut groups: BTreeMap<u32, (Vec<usize>, Vec<PublishMessage>)> = BTreeMap::new();
        for (i, m) in messages.into_iter().enumerate() {
            let p = match m.key.as_deref() {
                // Priority queue: the partition is the level, whatever the key.
                _ if priority => fluxmq_proto::model::priority_level(&m.headers, partitions),
                Some(k) => fluxmq_proto::batch::key_partition(k, partitions),
                None => *sticky
                    .get_or_insert_with(|| inner.partition_of(&topo, channel, None).unwrap_or(0)),
            };
            let g = groups.entry(p).or_default();
            g.0.push(i);
            g.1.push(m);
        }
        if groups.len() == 1 {
            let (p, (_, msgs)) = groups.pop_first().expect("one group");
            let body = PublishBody::for_protocol(protocol, msgs, compression, inner.next_pseq())?;
            return inner.publish_routed(channel, Some(p), body, deadline).await;
        }
        let mut parts = Vec::with_capacity(groups.len());
        for (p, (idx, msgs)) in groups {
            parts.push((
                p,
                idx,
                PublishBody::for_protocol(protocol, msgs, compression, inner.next_pseq())?,
            ));
        }
        let results =
            futures::future::join_all(parts.into_iter().map(|(p, idx, body)| async move {
                (
                    idx,
                    inner.publish_routed(channel, Some(p), body, deadline).await,
                )
            }))
            .await;
        let mut out: Vec<Option<MessageId>> = vec![None; n];
        for (idx, r) in results {
            for (i, id) in idx.into_iter().zip(r?) {
                out[i] = Some(id);
            }
        }
        out.into_iter()
            .map(|id| id.ok_or_else(|| ClientError::Protocol("missing message id".into())))
            .collect()
    }

    /// Joins consumer group `group` on `channel` (on every node leading a partition of it).
    pub async fn subscribe(
        &self,
        channel: &str,
        group: &str,
        options: SubscribeOptions,
    ) -> Result<Consumer> {
        Consumer::subscribe(self.clone(), channel, group, options).await
    }

    /// A [`Producer`] for `channel` (adds the `flux-producer` header automatically, see
    /// [`ClientOptions::stamp_producer`]).
    pub fn producer(&self, channel: &str) -> Producer {
        Producer::new(self.clone(), channel)
    }

    /// An auto-batching producer for high throughput. See [`BatchingProducer`].
    pub fn batching_producer(&self, channel: &str, options: BatchingOptions) -> BatchingProducer {
        BatchingProducer::new(self.clone(), channel, options)
    }

    /// Round trip to the broker (bootstrap connection).
    pub async fn ping(&self) -> Result<Duration> {
        let start = std::time::Instant::now();
        match self
            .inner()
            .primary_request(ClientRequest::Ping, true)
            .await?
        {
            ServerResponse::Pong | ServerResponse::Ok => Ok(start.elapsed()),
            other => Err(unexpected(other)),
        }
    }
}

/// Tries every seed once per round; returns the connection and the seed index.
async fn initial_connect(
    seeds: &[String],
    opts: &Arc<ClientOptions>,
    signals: &Arc<Signals>,
) -> Result<(Arc<Conn>, usize)> {
    let policy = &opts.reconnect;
    let mut rounds: u32 = 0;
    loop {
        let mut last_err = None;
        for (i, addr) in seeds.iter().enumerate() {
            match conn::connect(addr, opts.clone(), signals.clone()).await {
                Ok(c) => return Ok((c, i)),
                Err(e) if e.is_connection_error() || matches!(e, ClientError::Timeout) => {
                    tracing::debug!(%addr, error = %e, "seed unreachable");
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        let err = last_err.unwrap_or_else(|| ClientError::Io("no seeds".into()));
        rounds += 1;
        if !policy.retry_initial_connect || policy.exhausted(rounds) {
            return Err(err);
        }
        tracing::warn!(error = %err, "no seed reachable; retrying");
        tokio::time::sleep(policy.delay(rounds - 1)).await;
    }
}

pub(crate) fn unexpected(resp: ServerResponse) -> ClientError {
    ClientError::Protocol(format!("unexpected response {resp:?}"))
}

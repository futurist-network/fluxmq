//! Consumers: one logical subscription spanning every node that leads a partition of the
//! channel, merged into one stream; ack/nack routed to the originating node; transparent
//! resubscription after reconnects and leadership changes; client stats per node subscription.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fluxmq_proto::wire::{
    self, AckMode, ClientRequest, ClientStats, ErrorCode, ServerResponse, WireError,
};
use fluxmq_proto::{Message, MessageId, StartFrom};
use futures::Stream;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::acks::{AckBuffer, AckFrames, effective_batch};
use crate::client::{Client, Inner, unexpected};
use crate::conn::{Conn, EncodedRequest, Inbound, RawDelivery};
use crate::error::{ClientError, Result};
use crate::metadata::Topology;
use crate::node::Slot;
use crate::procstats;
use crate::records::RecordsCursor;

/// Resubscribe retry backoff (errors other than "node not connected yet").
const RESUB_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const RESUB_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// Options for [`Client::subscribe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscribeOptions {
    /// Max unacked deliveries the broker keeps outstanding for this consumer (per node).
    pub prefetch: u32,
    /// Where a *new* group starts (ignored if the group exists — also on resubscribe).
    pub start: StartFrom,
    /// Acks are coalesced per subscription and sent as one `Ack` frame once this many are
    /// buffered (default 64). Capped at half the prefetch, so the broker — which frees a
    /// prefetch slot only when it sees the ack — always has credit to keep delivering.
    pub ack_batch: u32,
    /// Longest an ack may wait in the buffer before it is sent anyway. `None` (default) uses
    /// [`ClientOptions::ack_linger`](crate::ClientOptions::ack_linger) (1 ms by default);
    /// `Some(Duration::ZERO)` sends every ack immediately. Buffered acks are also flushed on
    /// [`Consumer::unsubscribe`], when the consumer is dropped and when the client is closed.
    pub ack_linger: Option<Duration>,
    /// How messages are settled (protocol v4 brokers; see [`AckMode`]). `Individual`
    /// (default): every delivery is acked or nacked on its own, with redelivery, ack timeouts,
    /// nack and DLQ. `Cumulative` (stream channels): [`Delivery::ack`] commits the position up
    /// to and including that message in its partition, and [`Consumer::commit`] commits
    /// everything received so far. Cheapest, but no per-message redelivery: after a crash or
    /// rebalance delivery resumes from the last committed position. Commits are coalesced like
    /// acks (`ack_batch` / `ack_linger`). On v2/v3 brokers cumulative mode is emulated with
    /// individual acks (same API and semantics, without the savings).
    pub ack_mode: AckMode,
    /// Receive stored record batches verbatim (`DeliverRecords`, protocol v4) instead of
    /// per-message deliveries. Default `true`: the broker does no per-message work and the SDK
    /// decodes lazily, payloads being slices of the received frame. The `Delivery` API is the
    /// same either way. Ignored on v2/v3 brokers.
    pub raw_records: bool,
    /// Flow control in bytes in addition to `prefetch` messages (protocol v4; `None` =
    /// messages only).
    pub prefetch_bytes: Option<u32>,
    /// Verify the CRC-32C of every received record batch (default `true`). A corrupt batch
    /// closes the connection (the SDK reconnects and the broker redelivers). Hardware CRC-32C
    /// costs well under 1 % of consumer CPU (see `docs/design/v0.7.md`, "Results — SDK").
    pub verify_crc: bool,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            prefetch: 64,
            start: StartFrom::Latest,
            ack_batch: 64,
            ack_linger: None,
            ack_mode: AckMode::Individual,
            raw_records: true,
            prefetch_bytes: None,
            verify_crc: true,
        }
    }
}

impl SubscribeOptions {
    /// Sets the prefetch.
    pub fn prefetch(mut self, prefetch: u32) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Sets the start position for a new group.
    pub fn start(mut self, start: StartFrom) -> Self {
        self.start = start;
        self
    }

    /// Sets the ack batch size (see [`SubscribeOptions::ack_batch`]).
    pub fn ack_batch(mut self, n: u32) -> Self {
        self.ack_batch = n;
        self
    }

    /// Sets the ack linger (see [`SubscribeOptions::ack_linger`]).
    pub fn ack_linger(mut self, linger: Duration) -> Self {
        self.ack_linger = Some(linger);
        self
    }

    /// Sets the ack mode (see [`SubscribeOptions::ack_mode`]).
    pub fn ack_mode(mut self, mode: AckMode) -> Self {
        self.ack_mode = mode;
        self
    }

    /// Shorthand for `ack_mode(AckMode::Cumulative)`.
    pub fn cumulative(self) -> Self {
        self.ack_mode(AckMode::Cumulative)
    }

    /// Sets [`SubscribeOptions::raw_records`].
    pub fn raw_records(mut self, on: bool) -> Self {
        self.raw_records = on;
        self
    }

    /// Sets [`SubscribeOptions::prefetch_bytes`].
    pub fn prefetch_bytes(mut self, bytes: u32) -> Self {
        self.prefetch_bytes = Some(bytes);
        self
    }

    /// Sets [`SubscribeOptions::verify_crc`].
    pub fn verify_crc(mut self, on: bool) -> Self {
        self.verify_crc = on;
        self
    }
}

/// One broker-side subscription on one connection.
struct Link {
    /// `None` = bootstrap connection (single node), else the leader node id.
    target: Option<String>,
    conn: Arc<Conn>,
    subscription_id: u64,
    /// False once the link was replaced (reconnect / leadership change): deliveries from it are
    /// stale, the broker redelivers them; acks become no-ops.
    alive: AtomicBool,
    acks: Arc<AckBuffer>,
    /// Processing time (delivery received → ack/nack) in µs, current report window.
    processing_us: Mutex<Histogram<u64>>,
    /// Cumulative mode on this link (`Some`), and how it is carried out.
    cumulative: Option<Cumulative>,
}

/// Cumulative-mode bookkeeping of one link.
enum Cumulative {
    /// v4: `Commit` positions. Holds the next offset per partition after everything handed to
    /// the application (what [`Consumer::commit`] commits).
    Commit(Mutex<HashMap<u32, u64>>),
    /// v2/v3 broker: emulated with individual acks. Delivery ids handed out but not settled
    /// yet, per partition in offset order: acking offset `o` acks every id up to `o`.
    Emulated(Mutex<HashMap<u32, VecDeque<(u64, u64)>>>),
}

impl Link {
    fn usable(&self) -> bool {
        self.alive.load(Ordering::Acquire) && !self.conn.is_dead()
    }

    /// Notes a delivery handed to the application (cumulative bookkeeping).
    fn handed_out(&self, d: &RawDelivery) {
        let (p, o) = (d.message.partition, d.message.offset);
        match &self.cumulative {
            None => {}
            Some(Cumulative::Commit(pos)) => {
                let mut pos = pos.lock();
                let e = pos.entry(p).or_insert(0);
                *e = (*e).max(o + 1);
            }
            Some(Cumulative::Emulated(q)) => {
                q.lock().entry(p).or_default().push_back((o, d.delivery_id));
            }
        }
    }

    /// Frames settling everything handed out so far (cumulative mode), for `commit()`.
    fn settle_all(&self) -> Option<AckFrames> {
        let mut out: Option<AckFrames> = None;
        match &self.cumulative {
            None => {}
            Some(Cumulative::Commit(pos)) => {
                let pos: Vec<(u32, u64)> = pos.lock().iter().map(|(p, o)| (*p, *o)).collect();
                for (p, o) in pos {
                    if let Some(r) = self.acks.push_commit(p, o) {
                        match &mut out {
                            Some(o) => o.extend(r),
                            None => out = Some(r),
                        }
                    }
                }
            }
            Some(Cumulative::Emulated(q)) => {
                let ids: Vec<u64> = q
                    .lock()
                    .values_mut()
                    .flat_map(|d| d.drain(..).map(|(_, id)| id))
                    .collect();
                out = self.acks.push_many(ids);
            }
        }
        out
    }

    fn record(&self, received: Instant) {
        let us = received.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.processing_us.lock().saturating_record(us.max(1));
    }

    /// Sends the buffered acks now and waits until every ack taken out of the buffer (also by
    /// its timer task or a concurrent close) is queued, so a following `Unsubscribe` cannot
    /// overtake them. Bounded: waits for room in the connection's write queue.
    async fn flush_acks(&self) {
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            let _ = self.acks.flush(&self.conn).await;
            self.acks.handed_over().await;
        })
        .await;
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        // Last handle gone (consumer and every Delivery): flush what is still buffered.
        self.acks.close();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Retire {
    /// Replaced: mark stale (acks no-op), unsubscribe if the connection is still up.
    Stale,
    /// Consumer dropped: unsubscribe but let late acks still go out (v0.1 behaviour).
    Leave,
}

async fn retire(link: &Link, mode: Retire) {
    if mode == Retire::Stale {
        link.alive.store(false, Ordering::Release);
    }
    link.conn.remove_subscription(link.subscription_id);
    if !link.conn.is_dead() {
        // Acks must reach the broker before the Unsubscribe, which returns every delivery
        // still unacked to the group (as a failed attempt).
        link.flush_acks().await;
        link.conn.try_send_detached(ClientRequest::Unsubscribe {
            subscription_id: link.subscription_id,
        });
    }
}

/// State shared by the consumer, its deliveries and its background tasks.
struct Core {
    inner: Arc<Inner>,
    channel: String,
    group: String,
    options: SubscribeOptions,
    last_error: Mutex<Option<String>>,
    links: Mutex<HashMap<Option<String>, Arc<Link>>>,
    /// `Consumer::unsubscribe` cleans up itself; the supervisor must not.
    manual_cleanup: AtomicBool,
}

enum Ctrl {
    Add(Arc<Link>, mpsc::Receiver<Inbound>),
    Fatal(ClientError),
}

#[derive(Default)]
struct Outcome {
    added: Vec<(Arc<Link>, mpsc::Receiver<Inbound>)>,
    /// Targets whose node isn't connected yet: wake when their slot changes.
    waits: Vec<watch::Receiver<Slot>>,
    /// Retry on a timer (subscribe failed with a retryable error).
    retry: bool,
    last_err: Option<ClientError>,
}

/// Nodes the subscription should span: leaders of the channel's partitions, or the bootstrap
/// connection when single-node / the channel isn't in the metadata.
fn desired_targets(topo: &Topology, channel: &str) -> Vec<Option<String>> {
    match topo.channel_leaders(channel) {
        Some(leaders) => leaders.into_iter().map(Some).collect(),
        None => vec![None],
    }
}

/// Whether a subscribe error ends the consumer.
async fn is_fatal(core: &Core, e: &ClientError) -> bool {
    match e.code() {
        Some(ErrorCode::NotLeader) => {
            let _ = core.inner.refresh_metadata().await;
            false
        }
        Some(ErrorCode::NotFound) => {
            // In a cluster a just-created channel may not exist on its leader yet; it's only
            // gone for good when the metadata no longer lists it.
            if !core.inner.topology().is_cluster() {
                return true;
            }
            let _ = core.inner.refresh_metadata().await;
            core.inner.topology().partitions(&core.channel).is_none()
        }
        Some(ErrorCode::Overloaded | ErrorCode::Internal) => false,
        Some(_) => true, // Forbidden, Unauthorized, BadRequest, ...
        None => matches!(e, ClientError::Protocol(_)) && core.inner.is_closed(),
    }
}

async fn subscribe_link(
    core: &Core,
    target: Option<String>,
    conn: Arc<Conn>,
) -> Result<(Arc<Link>, mpsc::Receiver<Inbound>)> {
    let prefetch = core.options.prefetch.max(1);
    let cap = (prefetch as usize)
        .saturating_mul(2)
        .saturating_add(16)
        .min(1 << 20);
    let (dtx, rx) = mpsc::channel(cap);
    let v4 = conn.protocol() >= 4;
    let o = &core.options;
    let body = if v4 {
        ClientRequest::SubscribeWith {
            channel: core.channel.clone(),
            group: core.group.clone(),
            prefetch,
            start: o.start,
            options: wire::SubscribeOptions {
                ack_mode: o.ack_mode,
                raw_records: o.raw_records,
                prefetch_bytes: o.prefetch_bytes,
            },
        }
    } else {
        ClientRequest::Subscribe {
            channel: core.channel.clone(),
            group: core.group.clone(),
            prefetch,
            start: o.start,
        }
    };
    let req = EncodedRequest::new(&body)?;
    let timeout = conn.opts.request_timeout;
    let subscription_id = match conn.request_encoded(&req, Some(dtx), timeout).await? {
        ServerResponse::Subscribed { subscription_id } => subscription_id,
        other => return Err(unexpected(other)),
    };
    let linger = core.options.ack_linger.unwrap_or(conn.opts.ack_linger);
    let batch = effective_batch(core.options.ack_batch, prefetch);
    let acks = AckBuffer::start(&conn, subscription_id, batch, linger);
    tracing::debug!(channel = %core.channel, group = %core.group, node = %conn.node_id(), subscription_id, "subscribed");
    Ok((
        Arc::new(Link {
            target,
            conn,
            subscription_id,
            alive: AtomicBool::new(true),
            acks,
            processing_us: Mutex::new(new_histogram()),
            cumulative: match (o.ack_mode, v4) {
                (AckMode::Individual, _) => None,
                (AckMode::Cumulative, true) => Some(Cumulative::Commit(Mutex::default())),
                (AckMode::Cumulative, false) => Some(Cumulative::Emulated(Mutex::default())),
            },
        }),
        rx,
    ))
}

/// Brings the set of links in line with the topology. `initial_deadline`: wait for a
/// connection (until then) as long as no link exists yet, instead of returning immediately.
async fn reconcile(core: &Core, initial_deadline: Option<tokio::time::Instant>) -> Result<Outcome> {
    let inner = &core.inner;
    let topo = inner.topology_for(&core.channel).await;
    let desired = desired_targets(&topo, &core.channel);
    let stale: Vec<Arc<Link>> = {
        let mut links = core.links.lock();
        let keys: Vec<Option<String>> = links
            .iter()
            .filter(|(t, l)| l.conn.is_dead() || !desired.contains(t))
            .map(|(t, _)| t.clone())
            .collect();
        keys.into_iter().filter_map(|k| links.remove(&k)).collect()
    };
    for l in stale {
        tracing::debug!(channel = %core.channel, node = ?l.target, subscription_id = l.subscription_id, "dropping stale subscription");
        retire(&l, Retire::Stale).await;
    }
    let mut out = Outcome::default();
    if initial_deadline.is_some() {
        // Start connecting to every leader in parallel before waiting on any of them.
        for t in &desired {
            let _ = inner.try_conn_for(t.as_deref());
        }
    }
    let mut have_any = !core.links.lock().is_empty();
    for t in desired {
        if core.links.lock().contains_key(&t) {
            continue;
        }
        let conn = match initial_deadline {
            // Initial subscribe: wait for each leader, but don't block on an unreachable node
            // once a link exists (the supervisor picks it up when it comes back).
            Some(d) => match inner.conn_for(t.as_deref(), d, have_any).await {
                Ok(c) => c,
                Err(e) => {
                    if inner.is_closed() || !e.is_retryable() {
                        return Err(e);
                    }
                    if let (_, Some(w)) = inner.try_conn_for(t.as_deref()) {
                        out.waits.push(w);
                    } else {
                        out.retry = true;
                    }
                    out.last_err = Some(e);
                    continue;
                }
            },
            None => {
                let (c, w) = inner.try_conn_for(t.as_deref());
                match c {
                    Some(c) => c,
                    None => {
                        match w {
                            Some(w) => out.waits.push(w),
                            None => out.retry = true,
                        }
                        continue;
                    }
                }
            }
        };
        match subscribe_link(core, t.clone(), conn).await {
            Ok((link, rx)) => {
                core.links.lock().insert(t, link.clone());
                out.added.push((link, rx));
                have_any = true;
            }
            Err(e) => {
                if is_fatal(core, &e).await {
                    return Err(e);
                }
                tracing::debug!(channel = %core.channel, node = ?t, error = %e, "subscribe failed; will retry");
                out.retry = true;
                out.last_err = Some(e);
            }
        }
    }
    Ok(out)
}

async fn send_ctrl(ctrl: &mpsc::Sender<Ctrl>, cancel: &CancellationToken, msg: Ctrl) -> bool {
    tokio::select! {
        r = ctrl.send(msg) => r.is_ok(),
        _ = cancel.cancelled() => false,
    }
}

async fn supervise(
    core: Arc<Core>,
    ctrl: mpsc::Sender<Ctrl>,
    cancel: CancellationToken,
    mut waits: Vec<watch::Receiver<Slot>>,
    mut retry: bool,
) {
    let inner = core.inner.clone();
    let mut topo_rx = inner.watch_topology();
    topo_rx.borrow_and_update();
    let mut attempt: u32 = 0;
    let mut first = true;
    'outer: loop {
        if cancel.is_cancelled() {
            break;
        }
        if inner.is_closed() {
            send_ctrl(&ctrl, &cancel, Ctrl::Fatal(inner.closed_error())).await;
            break;
        }
        if !first {
            match reconcile(&core, None).await {
                Err(e) => {
                    tracing::warn!(channel = %core.channel, group = %core.group, error = %e, "subscription failed for good");
                    send_ctrl(&ctrl, &cancel, Ctrl::Fatal(e)).await;
                    break;
                }
                Ok(out) => {
                    if !out.added.is_empty() {
                        attempt = 0;
                    }
                    for (l, rx) in out.added {
                        if !send_ctrl(&ctrl, &cancel, Ctrl::Add(l, rx)).await {
                            break 'outer;
                        }
                    }
                    waits = out.waits;
                    retry = out.retry;
                }
            }
        }
        first = false;
        let conns: Vec<Arc<Conn>> = core.links.lock().values().map(|l| l.conn.clone()).collect();
        let link_dead = async {
            if conns.is_empty() {
                std::future::pending::<()>().await
            } else {
                futures::future::select_all(conns.iter().map(|c| Box::pin(c.dead()))).await;
            }
        };
        let slot_change = async {
            if waits.is_empty() {
                std::future::pending::<()>().await
            } else {
                let _ =
                    futures::future::select_all(waits.iter_mut().map(|w| Box::pin(w.changed())))
                        .await;
            }
        };
        let delay = crate::util::backoff(RESUB_BACKOFF_INITIAL, RESUB_BACKOFF_MAX, attempt, 0.2);
        let timer = async {
            if retry {
                tokio::time::sleep(delay).await
            } else {
                std::future::pending::<()>().await
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = inner.cancel.cancelled() => {}
            r = topo_rx.changed() => if r.is_err() { break },
            _ = link_dead => {}
            _ = slot_change => {}
            _ = timer => attempt = attempt.saturating_add(1),
        }
    }
    if !core.manual_cleanup.load(Ordering::Acquire) {
        let links: Vec<Arc<Link>> = core.links.lock().drain().map(|(_, l)| l).collect();
        for l in links {
            retire(&l, Retire::Leave).await;
        }
    }
}

/// One link's delivery queue plus the `DeliverRecords` frame being decoded from it.
struct LinkStream {
    link: Arc<Link>,
    rx: mpsc::Receiver<Inbound>,
    cursor: Option<RecordsCursor>,
}

impl LinkStream {
    fn new((link, rx): (Arc<Link>, mpsc::Receiver<Inbound>)) -> Self {
        Self {
            link,
            rx,
            cursor: None,
        }
    }
}

/// A subscription to a consumer group. Yields [`Delivery`]s.
///
/// In a cluster the consumer subscribes on every node that leads ≥ 1 partition of the channel
/// and merges the deliveries into this one stream; acks/nacks go back to the node that sent the
/// delivery. After a reconnect or a leadership change it resubscribes transparently (same
/// group); deliveries still buffered from the old subscription are dropped (the broker
/// redelivers them — at-least-once), and acking a delivery handed out before is a no-op.
///
/// The stream yields `Err` and then ends only for terminal conditions: `Forbidden`/
/// `Unauthorized`, the channel was deleted (`NotFound`), or the client closed (explicitly or
/// because the reconnect policy gave up).
///
/// Dropping a consumer sends a best-effort `Unsubscribe`; [`Consumer::unsubscribe`] does it
/// gracefully (pending acks are flushed first).
pub struct Consumer {
    client: Client,
    core: Arc<Core>,
    streams: Vec<LinkStream>,
    ctrl_rx: mpsc::Receiver<Ctrl>,
    next: usize,
    finished: bool,
    unsubscribed: bool,
    cancel: CancellationToken,
    supervisor: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("subscriptions", &self.subscription_ids())
            .field("channel", &self.core.channel)
            .field("group", &self.core.group)
            .finish()
    }
}

impl Consumer {
    pub(crate) async fn subscribe(
        client: Client,
        channel: &str,
        group: &str,
        options: SubscribeOptions,
    ) -> Result<Consumer> {
        let inner = client.inner().clone();
        if inner.is_closed() {
            return Err(inner.closed_error());
        }
        let core = Arc::new(Core {
            inner: inner.clone(),
            channel: channel.to_string(),
            group: group.to_string(),
            options,
            last_error: Mutex::new(None),
            links: Mutex::new(HashMap::new()),
            manual_cleanup: AtomicBool::new(false),
        });
        let deadline = tokio::time::Instant::now() + inner.opts.request_timeout;
        let cleanup = |core: &Arc<Core>| {
            let links: Vec<Arc<Link>> = core.links.lock().drain().map(|(_, l)| l).collect();
            for l in links {
                l.conn.remove_subscription(l.subscription_id);
                l.conn.try_send_detached(ClientRequest::Unsubscribe {
                    subscription_id: l.subscription_id,
                });
            }
        };
        let out = match reconcile(&core, Some(deadline)).await {
            Ok(o) => o,
            Err(e) => {
                cleanup(&core);
                return Err(e);
            }
        };
        if out.added.is_empty()
            && let Some(e) = out.last_err
        {
            cleanup(&core);
            return Err(e);
        }
        let cancel = CancellationToken::new();
        let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
        let supervisor = tokio::spawn(supervise(
            core.clone(),
            ctrl_tx,
            cancel.clone(),
            out.waits,
            out.retry,
        ));
        tokio::spawn(stats_task(Arc::downgrade(&core), cancel.clone()));
        Ok(Consumer {
            client,
            core,
            streams: out.added.into_iter().map(LinkStream::new).collect(),
            ctrl_rx,
            next: 0,
            finished: false,
            unsubscribed: false,
            cancel,
            supervisor: Some(supervisor),
        })
    }

    /// Broker-assigned subscription id on the first node (the only one on a single node);
    /// 0 while not subscribed anywhere (e.g. reconnecting). See [`Consumer::subscription_ids`].
    pub fn subscription_id(&self) -> u64 {
        let links = self.core.links.lock();
        let mut v: Vec<_> = links.values().collect();
        v.sort_by(|a, b| a.target.cmp(&b.target));
        v.first().map(|l| l.subscription_id).unwrap_or(0)
    }

    /// Current subscriptions as `(node id, subscription id)` (node id of the connection's
    /// handshake; one entry per node leading a partition of the channel).
    pub fn subscription_ids(&self) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .core
            .links
            .lock()
            .values()
            .map(|l| (l.conn.node_id().to_string(), l.subscription_id))
            .collect();
        v.sort();
        v
    }

    /// Channel name.
    pub fn channel(&self) -> &str {
        &self.core.channel
    }

    /// Consumer group name.
    pub fn group(&self) -> &str {
        &self.core.group
    }

    /// The client this consumer runs on.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Next delivery; `None` after the subscription ended.
    pub async fn next(&mut self) -> Option<Result<Delivery>> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    /// Cumulative mode: commits everything this consumer has handed out so far (on every node)
    /// and waits until the broker confirmed it. In individual mode it flushes buffered acks.
    pub async fn commit(&self) -> Result<()> {
        let links: Vec<Arc<Link>> = self.core.links.lock().values().cloned().collect();
        for l in links {
            if !l.usable() {
                continue;
            }
            if let Some(reqs) = l.settle_all() {
                l.acks.send(&l.conn, reqs).await?;
            }
            l.acks.flush_confirmed(&l.conn).await?;
        }
        Ok(())
    }

    /// Flushes pending acks, then leaves the group on every node.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.unsubscribed = true;
        self.core.manual_cleanup.store(true, Ordering::Release);
        self.cancel.cancel();
        if let Some(h) = self.supervisor.take() {
            let _ = h.await;
        }
        let links: Vec<Arc<Link>> = self.core.links.lock().drain().map(|(_, l)| l).collect();
        let mut result = Ok(());
        for l in links {
            l.flush_acks().await;
            l.conn.remove_subscription(l.subscription_id);
            if l.conn.is_dead() {
                continue;
            }
            let r = l
                .conn
                .request(ClientRequest::Unsubscribe {
                    subscription_id: l.subscription_id,
                })
                .await;
            match r {
                Ok(ServerResponse::Ok) => {}
                Ok(other) => {
                    if result.is_ok() {
                        result = Err(unexpected(other));
                    }
                }
                Err(e) => {
                    if result.is_ok() {
                        result = Err(e);
                    }
                }
            }
        }
        result
    }

    fn end(&mut self, err: ClientError) -> Poll<Option<Result<Delivery>>> {
        self.finished = true;
        self.streams.clear();
        if self.unsubscribed {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Err(err)))
        }
    }
}

impl Stream for Consumer {
    type Item = Result<Delivery>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        loop {
            match self.ctrl_rx.poll_recv(cx) {
                Poll::Ready(Some(Ctrl::Add(link, rx))) => {
                    self.streams.push(LinkStream::new((link, rx)))
                }
                Poll::Ready(Some(Ctrl::Fatal(e))) => return self.end(e),
                Poll::Ready(None) => {
                    let e = if self.core.inner.is_closed() {
                        self.core.inner.closed_error()
                    } else {
                        ClientError::Closed("subscription ended".into())
                    };
                    return self.end(e);
                }
                Poll::Pending => break,
            }
        }
        let this = &mut *self;
        let n = this.streams.len();
        let mut dead: Vec<usize> = Vec::new();
        let mut found = None;
        let verify_crc = this.core.options.verify_crc;
        'streams: for i in 0..n {
            let idx = (this.next + i) % n;
            let ls = &mut this.streams[idx];
            loop {
                // Deliveries of a raw records frame first, decoded one at a time.
                if let Some(cur) = &mut ls.cursor {
                    match cur.next_delivery() {
                        Some(Ok(d)) => {
                            if !ls.link.usable() {
                                ls.cursor = None;
                                continue;
                            }
                            ls.link.handed_out(&d);
                            found = Some((idx, ls.link.clone(), d));
                            break 'streams;
                        }
                        Some(Err(e)) => {
                            // Corrupt or malformed batch: nothing of it was acked, so the
                            // broker redelivers it after the reconnect.
                            tracing::warn!(error = %e, node = %ls.link.conn.node_id(), "dropping connection");
                            ls.link.conn.fail(e.to_string());
                            ls.cursor = None;
                            continue;
                        }
                        None => ls.cursor = None,
                    }
                }
                match ls.rx.poll_recv(cx) {
                    Poll::Ready(Some(Inbound::Records(frame))) => {
                        if ls.link.usable() {
                            ls.cursor = Some(RecordsCursor::new(frame, verify_crc));
                        }
                        continue;
                    }
                    Poll::Ready(Some(Inbound::One(d))) => {
                        if !ls.link.usable() {
                            tracing::debug!(
                                delivery_id = d.delivery_id,
                                "dropping stale buffered delivery"
                            );
                            continue;
                        }
                        ls.link.handed_out(&d);
                        found = Some((idx, ls.link.clone(), d));
                        break 'streams;
                    }
                    Poll::Ready(None) => {
                        dead.push(idx);
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }
        let result = found.map(|(idx, link, d)| {
            this.next = idx + 1;
            Delivery {
                inner: d,
                link,
                core: this.core.clone(),
                received: Instant::now(),
            }
        });
        dead.sort_unstable();
        for idx in dead.into_iter().rev() {
            this.streams.swap_remove(idx);
        }
        match result {
            Some(d) => Poll::Ready(Some(Ok(d))),
            None => Poll::Pending,
        }
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // The supervisor unsubscribes (best effort). Ack tasks keep running until every
        // Delivery handle is gone, so late acks still go out.
        self.cancel.cancel();
    }
}

/// One delivered message. Call [`Delivery::ack`] or [`Delivery::nack`] when done.
///
/// Processing time (from receipt to ack/nack) is measured and reported to the broker.
/// Dropping a delivery without acking leaves it unacked until the channel's ack timeout,
/// after which the broker redelivers it.
pub struct Delivery {
    inner: RawDelivery,
    link: Arc<Link>,
    core: Arc<Core>,
    received: Instant,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery")
            .field("delivery_id", &self.inner.delivery_id)
            .field("attempt", &self.inner.attempt)
            .field("id", &self.id())
            .field("node", &self.node_id())
            .field("key", &self.inner.message.key)
            .field("payload_len", &self.inner.message.payload.len())
            .finish()
    }
}

impl Delivery {
    /// Broker delivery id (unique per channel+group on the delivering node).
    pub fn delivery_id(&self) -> u64 {
        self.inner.delivery_id
    }

    /// 1-based delivery attempt.
    pub fn attempt(&self) -> u32 {
        self.inner.attempt
    }

    /// Partition + offset of the message.
    pub fn id(&self) -> MessageId {
        MessageId {
            partition: self.inner.message.partition,
            offset: self.inner.message.offset,
        }
    }

    /// Node that delivered the message (acks go back to it).
    pub fn node_id(&self) -> &str {
        self.link.conn.node_id()
    }

    /// Subscription id the delivery arrived on.
    pub fn subscription_id(&self) -> u64 {
        self.link.subscription_id
    }

    /// Payload bytes.
    pub fn payload(&self) -> &Bytes {
        &self.inner.message.payload
    }

    /// Message key.
    pub fn key(&self) -> Option<&str> {
        self.inner.message.key.as_deref()
    }

    /// All headers.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.inner.message.headers
    }

    /// One header.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.inner.message.headers.get(name).map(String::as_str)
    }

    /// Broker timestamp (ms since epoch).
    pub fn timestamp_ms(&self) -> u64 {
        self.inner.message.timestamp_ms
    }

    /// The full stored message.
    pub fn message(&self) -> &Message {
        &self.inner.message
    }

    /// When the SDK received this delivery.
    pub fn received_at(&self) -> Instant {
        self.received
    }

    /// True if the subscription this delivery came from was replaced (reconnect / leadership
    /// change). The broker redelivers it; ack/nack are no-ops.
    pub fn is_stale(&self) -> bool {
        !self.link.usable()
    }

    /// `Some(result)` if ack/nack must not be sent (stale or closed).
    fn stale_result(&self, what: &str) -> Option<Result<()>> {
        if self.link.usable() {
            return None;
        }
        if self.core.inner.is_closed() {
            return Some(Err(self.core.inner.closed_error()));
        }
        tracing::debug!(
            delivery_id = self.inner.delivery_id,
            subscription_id = self.link.subscription_id,
            "{what} for a stale delivery ignored (the broker redelivers it)"
        );
        Some(Ok(()))
    }

    /// Acknowledges the message. Acks are coalesced per subscription into one frame
    /// (fire-and-forget) on the connection the delivery came from — sent once
    /// [`SubscribeOptions::ack_batch`] acks are buffered or after
    /// [`SubscribeOptions::ack_linger`] — so this usually returns at once; the call that fills
    /// the batch queues the frame. Consecutive delivery ids go out as ranges (`AckRanges`) on
    /// v0.7+ brokers.
    ///
    /// In [`AckMode::Cumulative`] this commits the position after this message in its
    /// partition, i.e. it also settles every earlier message of the partition.
    pub async fn ack(self) -> Result<()> {
        self.link.record(self.received);
        if let Some(r) = self.stale_result("ack") {
            return r;
        }
        let m = &self.inner.message;
        let full = match &self.link.cumulative {
            None => self.link.acks.push(self.inner.delivery_id),
            Some(Cumulative::Commit(_)) => self.link.acks.push_commit(m.partition, m.offset + 1),
            Some(Cumulative::Emulated(q)) => {
                let ids: Vec<u64> = {
                    let mut q = q.lock();
                    match q.get_mut(&m.partition) {
                        Some(d) => {
                            let n = d.iter().take_while(|(o, _)| *o <= m.offset).count();
                            d.drain(..n).map(|(_, id)| id).collect()
                        }
                        None => Vec::new(),
                    }
                };
                self.link.acks.push_many(ids)
            }
        };
        let Some(batch) = full else {
            return Ok(());
        };
        match self.link.acks.send(&self.link.conn, batch).await {
            Err(e) if e.is_connection_error() && !self.core.inner.is_closed() => {
                tracing::debug!(error = %e, "acks lost with their connection (redelivered)");
                Ok(())
            }
            r => r,
        }
    }

    /// Acknowledges several deliveries at once, like calling [`Delivery::ack`] on each in
    /// order, with less work per message: one clock read per call and, per run of deliveries
    /// from the same subscription, one processing-time lock and one ack-buffer lock (consecutive
    /// delivery ids collapse into one `AckRanges` range). Cumulative-mode deliveries are acked
    /// one by one. Returns the first error; the remaining deliveries are still acked.
    pub async fn ack_all(deliveries: impl IntoIterator<Item = Delivery>) -> Result<()> {
        let mut first_err: Option<ClientError> = None;
        let mut iter = deliveries.into_iter().peekable();
        let now = Instant::now();
        while let Some(d) = iter.next() {
            if d.link.cumulative.is_some() {
                if let Err(e) = d.ack().await {
                    first_err.get_or_insert(e);
                }
                continue;
            }
            // The run of deliveries from this delivery's link.
            let link = d.link.clone();
            let core = d.core.clone();
            let mut run = vec![d];
            while let Some(next) = iter.next_if(|n| Arc::ptr_eq(&n.link, &link)) {
                run.push(next);
            }
            {
                let mut h = link.processing_us.lock();
                for d in &run {
                    let us = now.saturating_duration_since(d.received).as_micros();
                    h.saturating_record((us.min(u64::MAX as u128) as u64).max(1));
                }
            }
            if !link.usable() {
                if core.inner.is_closed() {
                    first_err.get_or_insert(core.inner.closed_error());
                }
                continue;
            }
            let Some(batch) = link.acks.push_many(run.iter().map(|d| d.inner.delivery_id)) else {
                continue;
            };
            match link.acks.send(&link.conn, batch).await {
                Err(e) if e.is_connection_error() && !core.inner.is_closed() => {
                    tracing::debug!(error = %e, "acks lost with their connection (redelivered)");
                }
                Err(e) => {
                    first_err.get_or_insert(e);
                }
                Ok(()) => {}
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Negatively acknowledges the message. `retry = false` dead-letters it immediately.
    /// `reason` is shown in the DLQ inbox and as this consumer's last error. Not available in
    /// [`AckMode::Cumulative`] on v0.7+ brokers (`BadRequest`).
    pub async fn nack(self, reason: impl Into<String>, retry: bool) -> Result<()> {
        self.link.record(self.received);
        let reason = reason.into();
        *self.core.last_error.lock() = Some(reason.clone());
        if let Some(r) = self.stale_result("nack") {
            return r;
        }
        match &self.link.cumulative {
            Some(Cumulative::Commit(_)) => {
                return Err(ClientError::Server(WireError::new(
                    ErrorCode::BadRequest,
                    "nack is not available in cumulative ack mode",
                )));
            }
            Some(Cumulative::Emulated(q)) => {
                let m = &self.inner.message;
                if let Some(d) = q.lock().get_mut(&m.partition) {
                    d.retain(|(_, id)| *id != self.inner.delivery_id);
                }
            }
            None => {}
        }
        let r = self
            .link
            .conn
            .send_oneway(ClientRequest::Nack {
                subscription_id: self.link.subscription_id,
                delivery_id: self.inner.delivery_id,
                reason,
                retry,
            })
            .await;
        match r {
            Err(e) if e.is_connection_error() && !self.core.inner.is_closed() => {
                tracing::debug!(error = %e, "nack lost with its connection (redelivered)");
                Ok(())
            }
            r => r,
        }
    }

    /// The raw protocol delivery.
    pub fn into_inner(self) -> fluxmq_proto::wire::Delivery {
        self.inner
    }
}

fn new_histogram() -> Histogram<u64> {
    // 1 µs .. 1 h, 3 significant digits.
    Histogram::new_with_bounds(1, 3_600_000_000, 3).expect("static histogram bounds are valid")
}

async fn stats_task(core: Weak<Core>, cancel: CancellationToken) {
    let interval = match core.upgrade() {
        Some(c) => c.inner.opts.stats_interval,
        None => return,
    };
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // immediate first tick
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = cancel.cancelled() => return,
        }
        let Some(c) = core.upgrade() else { return };
        let links: Vec<Arc<Link>> = c.links.lock().values().cloned().collect();
        if links.is_empty() {
            continue;
        }
        let (cpu, mem) = procstats::sample().await;
        let last_error = c.last_error.lock().clone();
        for l in links {
            if !l.usable() {
                continue;
            }
            let (p50, p99) = {
                let mut h = l.processing_us.lock();
                let r = if h.is_empty() {
                    (None, None)
                } else {
                    (
                        Some(h.value_at_quantile(0.50) as f32 / 1000.0),
                        Some(h.value_at_quantile(0.99) as f32 / 1000.0),
                    )
                };
                h.reset();
                r
            };
            let stats = ClientStats {
                subscription_id: l.subscription_id,
                cpu_percent: cpu,
                memory_bytes: mem,
                processing_p50_ms: p50,
                processing_p99_ms: p99,
                last_error: last_error.clone(),
            };
            let _ = l.conn.send_oneway(ClientRequest::ReportStats(stats)).await;
        }
    }
}

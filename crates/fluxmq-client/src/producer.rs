//! Producers: a header-decorating [`Producer`] and an auto-batching [`BatchingProducer`].

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use fluxmq_proto::{MessageId, PublishMessage, headers};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::client::{Client, Inner, PublishBody};
use crate::error::{ClientError, Result};
use crate::metadata::Topology;
use crate::records::RecordsBuilder;

/// Builder for a [`PublishMessage`].
///
/// ```
/// use fluxmq_client::MessageBuilder;
/// let msg = MessageBuilder::new(r#"{"order_id":"8842"}"#)
///     .key("8842")
///     .event_type("OrderCreated")
///     .content_type("application/json")
///     .new_trace_id()
///     .build();
/// assert_eq!(msg.key.as_deref(), Some("8842"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct MessageBuilder {
    msg: PublishMessage,
}

impl MessageBuilder {
    /// A message with the given payload.
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            msg: PublishMessage {
                payload: payload.into(),
                ..Default::default()
            },
        }
    }

    /// Partitioning / ordering key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.msg.key = Some(key.into());
        self
    }

    /// Arbitrary header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.msg.headers.insert(name.into(), value.into());
        self
    }

    /// `flux-type` header (event type, shown in the UI).
    pub fn event_type(self, t: impl Into<String>) -> Self {
        self.header(headers::EVENT_TYPE, t)
    }

    /// `content-type` header.
    pub fn content_type(self, t: impl Into<String>) -> Self {
        self.header(headers::CONTENT_TYPE, t)
    }

    /// `flux-trace-id` header.
    pub fn trace_id(self, id: impl Into<String>) -> Self {
        self.header(headers::TRACE_ID, id)
    }

    /// `flux-trace-id` header with a fresh random id (see [`new_trace_id`]).
    pub fn new_trace_id(self) -> Self {
        self.trace_id(new_trace_id())
    }

    /// Priority on a priority queue (brokers v0.12+, `ChannelMode::Priority`): `0` is the
    /// lowest; values above the channel's top level count as the top level. Ignored by other
    /// channel modes.
    pub fn priority(self, level: u8) -> Self {
        self.header(headers::PRIORITY, level.to_string())
    }

    /// Delayed delivery (brokers v0.12+): the broker keeps the message and publishes it to the
    /// channel at `at` (by the broker's clock; at most 30 days ahead). A time in the past
    /// delivers at once. The id returned for it is its schedule id, not its channel position.
    /// See `fluxmq_proto::scheduled`.
    pub fn deliver_at(self, at: std::time::SystemTime) -> Self {
        let ms = at
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        self.header(fluxmq_proto::scheduled::DELIVER_AT_HEADER, ms.to_string())
    }

    /// Delayed delivery (brokers v0.12+): publish `after` from now, measured by the broker's
    /// clock when it receives the message (at most 30 days).
    pub fn delay(self, after: Duration) -> Self {
        self.header(
            fluxmq_proto::scheduled::DELAY_HEADER,
            (after.as_millis() as u64).to_string(),
        )
    }

    /// The finished message.
    pub fn build(self) -> PublishMessage {
        self.msg
    }
}

impl From<MessageBuilder> for PublishMessage {
    fn from(b: MessageBuilder) -> Self {
        b.msg
    }
}

/// Shorthand for [`MessageBuilder::new`].
pub fn message(payload: impl Into<Bytes>) -> MessageBuilder {
    MessageBuilder::new(payload)
}

/// A random 128-bit trace id as 32 lowercase hex chars (not cryptographically secure).
pub fn new_trace_id() -> String {
    format!(
        "{:016x}{:016x}",
        crate::util::fast_rand(),
        crate::util::fast_rand()
    )
}

/// Publishes to one channel. With [`ClientOptions::stamp_producer`](crate::ClientOptions)
/// (the default) it stamps the `flux-producer` header (= client name) on every message that
/// does not already carry one.
#[derive(Debug, Clone)]
pub struct Producer {
    client: Client,
    channel: Arc<str>,
}

impl Producer {
    pub(crate) fn new(client: Client, channel: &str) -> Self {
        Self {
            client,
            channel: channel.into(),
        }
    }

    /// Channel name.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    fn stamp(&self) -> Option<&str> {
        let o = &self.client.inner().opts;
        o.stamp_producer.then_some(o.client_name.as_str())
    }

    /// Publishes one message.
    pub async fn send(&self, msg: impl Into<PublishMessage>) -> Result<MessageId> {
        let msg = decorate(msg.into(), self.stamp());
        self.client.publish(&self.channel, msg).await
    }

    /// Publishes several messages in one request.
    pub async fn send_batch<I, M>(&self, msgs: I) -> Result<Vec<MessageId>>
    where
        I: IntoIterator<Item = M>,
        M: Into<PublishMessage>,
    {
        let name = self.stamp();
        let msgs = msgs.into_iter().map(|m| decorate(m.into(), name)).collect();
        self.client.publish_batch(&self.channel, msgs).await
    }
}

/// Adds `flux-producer: <producer>` unless stamping is off (`None`) or the message has one.
fn decorate(mut msg: PublishMessage, producer: Option<&str>) -> PublishMessage {
    if let Some(producer) = producer
        && !msg.headers.contains_key(headers::PRODUCER)
    {
        msg.headers
            .insert(headers::PRODUCER.to_string(), producer.to_string());
    }
    msg
}

/// Tuning for [`BatchingProducer`].
#[derive(Debug, Clone)]
pub struct BatchingOptions {
    /// How long a batch may wait for company **while the partition already has a batch in
    /// flight**. A message arriving when nothing of its partition is in flight goes out at
    /// once (adaptive linger: no added latency at low rates, natural batching under load);
    /// a lingering batch also goes out as soon as the partition's last in-flight batch
    /// completes. `ZERO` never waits.
    pub linger: Duration,
    /// Max messages per batch (`PublishBatch` / `Publish` frame).
    pub max_batch: usize,
    /// Max bytes per batch: encoded records-section bytes (before compression) on protocol
    /// v4, payload bytes on v2/v3. Keep well under the 16 MiB frame limit.
    pub max_batch_bytes: usize,
    /// Max publish requests outstanding at once across all partitions (pipelining).
    pub max_in_flight: usize,
    /// Capacity of each partition's submission queue; `send` waits when it is full
    /// (backpressure).
    pub queue_capacity: usize,
    /// At most one batch in flight per partition, so per-key order survives retries
    /// (a batch is retried until it succeeds or fails for good before the next one for the
    /// same partition is sent). Default `false`.
    pub ordered: bool,
}

impl Default for BatchingOptions {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(1),
            max_batch: 1000,
            max_batch_bytes: 1 << 20,
            max_in_flight: 8,
            queue_capacity: 16_384,
            ordered: false,
        }
    }
}

// Per-message completions use `futures`' oneshot, not tokio's: tokio's oneshot consumes the
// task's coop budget, so a caller awaiting thousands of sends in one `FuturesUnordered` stalls
// once the budget runs out and per-message cost grows with the number outstanding.
struct Item {
    msg: PublishMessage,
    done: futures::channel::oneshot::Sender<Result<MessageId>>,
}

/// Resolves to the id the broker assigned to one message sent via [`BatchingProducer::send`].
#[derive(Debug)]
pub struct PublishFuture {
    rx: futures::channel::oneshot::Receiver<Result<MessageId>>,
}

impl Future for PublishFuture {
    type Output = Result<MessageId>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(|r| match r {
            Ok(r) => r,
            Err(_) => Err(ClientError::Closed("batching producer stopped".into())),
        })
    }
}

/// Coalesces individually submitted messages into batches **per partition** (up to
/// `max_batch` messages / `max_batch_bytes`), with up to `max_in_flight` batches pipelined.
/// Each message gets its own [`PublishFuture`]. Lingering is adaptive (see
/// [`BatchingOptions::linger`]).
///
/// On protocol v4 brokers (v0.7+) each batch's records section is encoded into one buffer
/// while the batch lingers, compressed once when it is sent
/// ([`ClientOptions::compression`](crate::ClientOptions::compression)) and published as one
/// `PublishBatch`: the broker stores it without touching individual messages. On v2/v3 brokers
/// batches go out as `Publish`.
///
/// Partitioning: once the channel's partition count is known (from metadata, single node
/// included) every batch targets one partition. Keyed messages go to their key's partition
/// (`crc32(key) % partitions`, same function as the broker); keyless messages stick to one
/// partition until its batch is sent, then move on to the next (sticky partitioning: bigger
/// batches than per-message round-robin, same spread over time). In a cluster each partition's
/// batches go straight to its leader. Batches are retried like [`Client::publish_batch`]
/// (connection loss, `NotLeader`, `Overloaded`, until `publish_timeout`); a retried batch may
/// be stored twice.
///
/// Ordering: with [`BatchingOptions::ordered`] at most one batch per partition is in flight,
/// so per-key order is preserved even across retries (duplicates aside). Without it, a retried
/// batch can land after a later batch of the same partition. Messages sent one after another
/// from one task reach their partition's queue in that order.
///
/// ```no_run
/// # async fn demo(client: fluxmq_client::Client) -> fluxmq_client::Result<()> {
/// use fluxmq_client::{BatchingOptions, message};
/// let producer = client.batching_producer("orders.events", BatchingOptions::default());
/// let mut pending = Vec::new();
/// for i in 0..10_000 {
///     pending.push(producer.send(message(format!("event {i}"))).await?);
/// }
/// for p in pending {
///     let _id = p.await?;
/// }
/// producer.close().await;
/// # Ok(()) }
/// ```
pub struct BatchingProducer {
    client: Client,
    channel: Arc<str>,
    opts: BatchingOptions,
    /// `flux-producer` value to stamp (`None` = stamping off).
    stamp: Option<Arc<str>>,
    in_flight: Arc<Semaphore>,
    /// Current sticky partition for keyless messages (`u64::MAX` = none chosen yet).
    sticky: Arc<AtomicU64>,
    /// Partition (`None` = broker-chosen) → its worker's queue. Senders call straight into
    /// the worker queue (no dispatcher task in between).
    workers: Mutex<HashMap<Option<u32>, mpsc::Sender<Item>>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for BatchingProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchingProducer")
            .field("channel", &self.channel)
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl BatchingProducer {
    pub(crate) fn new(client: Client, channel: &str, options: BatchingOptions) -> Self {
        let o = &client.inner().opts;
        let stamp = o.stamp_producer.then(|| Arc::from(o.client_name.as_str()));
        Self {
            in_flight: Arc::new(Semaphore::new(options.max_in_flight.max(1))),
            client,
            channel: channel.into(),
            opts: options,
            stamp,
            sticky: Arc::new(AtomicU64::new(u64::MAX)),
            workers: Mutex::new(HashMap::new()),
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Queues a message; returns once it is queued. Await the returned future for its id.
    pub async fn send(&self, msg: impl Into<PublishMessage>) -> Result<PublishFuture> {
        let (done, rx) = futures::channel::oneshot::channel();
        // The `flux-producer` stamp is added by the worker (written straight into the records
        // section on v4, so it costs no per-message allocation).
        let msg = msg.into();
        if fluxmq_proto::scheduled::is_delayed(&msg.headers) {
            // Delayed messages bypass the record batches (the broker must read their headers):
            // one `Publish` each, in the background.
            let client = self.client.clone();
            let channel = self.channel.clone();
            let msg = decorate(msg, self.stamp.as_deref());
            tokio::spawn(async move {
                let r = client.publish(&channel, msg).await;
                let _ = done.send(r);
            });
            return Ok(PublishFuture { rx });
        }
        let inner = self.client.inner();
        let mut topo = inner.topology();
        if topo.is_cluster() && topo.partitions(&self.channel).is_none() {
            topo = inner.topology_for(&self.channel).await;
        }
        let priority = topo.is_priority(&self.channel);
        if !priority
            && msg.headers.contains_key(headers::PRIORITY)
            && topo.partitions(&self.channel).is_none()
        {
            // Priority not known to be routable here yet: let the broker place it.
            let client = self.client.clone();
            let channel = self.channel.clone();
            let msg = decorate(msg, self.stamp.as_deref());
            tokio::spawn(async move {
                let _ = done.send(client.publish(&channel, msg).await);
            });
            return Ok(PublishFuture { rx });
        }
        let part = match (msg.key.as_deref(), topo.partitions(&self.channel)) {
            // Priority queue: the partition is the level, whatever the key.
            (_, Some(n)) if priority => Some(fluxmq_proto::model::priority_level(&msg.headers, n)),
            (None, Some(n)) => Some(self.sticky_partition(inner, &topo, n)),
            (key, _) => inner.partition_of(&topo, &self.channel, key),
        };
        let closed = || ClientError::Closed("batching producer stopped".into());
        // Common case: room in the worker's queue, queued under the map lock (no sender
        // clone); otherwise wait for room outside the lock.
        let (tx, item) = {
            let mut workers = self.workers.lock();
            let tx = self.worker(&mut workers, part);
            match tx.try_send(Item { msg, done }) {
                Ok(()) => return Ok(PublishFuture { rx }),
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(closed()),
                Err(mpsc::error::TrySendError::Full(item)) => (tx.clone(), item),
            }
        };
        tx.send(item).await.map_err(|_| closed())?;
        Ok(PublishFuture { rx })
    }

    /// The sticky partition for a keyless message (chosen round-robin when unset or out of
    /// range; moved on by the partition's worker when it sends a batch).
    fn sticky_partition(&self, inner: &Arc<Inner>, topo: &Topology, n: u32) -> u32 {
        let cur = self.sticky.load(Ordering::Relaxed);
        if cur < u64::from(n) {
            return cur as u32;
        }
        let p = inner.partition_of(topo, &self.channel, None).unwrap_or(0);
        match self
            .sticky
            .compare_exchange(cur, u64::from(p), Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => p,
            Err(other) if other < u64::from(n) => other as u32,
            Err(_) => p,
        }
    }

    /// The queue of `part`'s worker, starting the worker on first use.
    fn worker<'a>(
        &self,
        workers: &'a mut HashMap<Option<u32>, mpsc::Sender<Item>>,
        part: Option<u32>,
    ) -> &'a mpsc::Sender<Item> {
        workers
            .entry(part)
            .or_insert_with(|| self.spawn_worker(part))
    }

    fn spawn_worker(&self, part: Option<u32>) -> mpsc::Sender<Item> {
        let (tx, rx) = mpsc::channel(self.opts.queue_capacity.max(1));
        let h = tokio::spawn(partition_worker(
            Worker {
                client: self.client.clone(),
                channel: self.channel.clone(),
                partition: part,
                opts: self.opts.clone(),
                in_flight: self.in_flight.clone(),
                stamp: self.stamp.clone(),
                sticky: self.sticky.clone(),
            },
            rx,
        ));
        self.handles.lock().push(h);
        tx
    }

    /// Queues a message and waits for its id.
    pub async fn publish(&self, msg: impl Into<PublishMessage>) -> Result<MessageId> {
        self.send(msg).await?.await
    }

    /// Stops accepting messages and waits until every queued message is published (or failed).
    pub async fn close(self) {
        drop(std::mem::take(&mut *self.workers.lock()));
        let handles = std::mem::take(&mut *self.handles.lock());
        for h in handles {
            let _ = h.await;
        }
    }
}

type BatchSend = Pin<Box<dyn Future<Output = ()> + Send>>;

/// An idle partition sends its first message at once, but first yields to the scheduler up to
/// this many times while that keeps bringing in more messages: tasks woken in the same tick
/// (typically the publishers whose previous batch was just acknowledged) get to add theirs,
/// so a closed-loop producer doesn't degenerate into alternating 1-message and N-message
/// batches. Costs microseconds, not a linger.
const IDLE_YIELDS: u32 = 3;

/// What a partition worker needs.
struct Worker {
    client: Client,
    channel: Arc<str>,
    partition: Option<u32>,
    opts: BatchingOptions,
    in_flight: Arc<Semaphore>,
    stamp: Option<Arc<str>>,
    sticky: Arc<AtomicU64>,
}

/// The batch a worker is building.
enum Body {
    /// v4: records section encoded as messages arrive.
    Records(RecordsBuilder),
    /// v2/v3 (or no partition known): `Publish` messages.
    Messages(Vec<PublishMessage>),
}

struct Batch {
    body: Body,
    /// Idempotent publishing mark for the first record (records form only).
    mark: Option<String>,
    dones: Vec<futures::channel::oneshot::Sender<Result<MessageId>>>,
    /// Payload bytes (`Messages` form; the records form measures its buffer).
    payload_bytes: usize,
    keyless: bool,
}

impl Batch {
    fn len(&self) -> usize {
        self.dones.len()
    }

    fn is_empty(&self) -> bool {
        self.dones.is_empty()
    }

    fn bytes(&self) -> usize {
        match &self.body {
            Body::Records(b) => b.len(),
            Body::Messages(_) => self.payload_bytes,
        }
    }
}

/// Builds batches in the form the client's latest handshake suggests and sizes the records
/// buffer from the previous batch, so a steady stream allocates one buffer per batch.
struct BatchFactory {
    records: bool,
    stamp: Option<Arc<str>>,
    last_len: usize,
    max_bytes: usize,
}

impl BatchFactory {
    fn new_batch(&self, inner: &Inner) -> Batch {
        let body = if self.records && inner.signals.protocol_hint() >= 4 {
            let cap = (self.last_len + self.last_len / 8 + 256).min(self.max_bytes + 64 * 1024);
            Body::Records(RecordsBuilder::with_capacity(cap))
        } else {
            Body::Messages(Vec::new())
        };
        let mark = matches!(body, Body::Records(_))
            .then(|| inner.next_pseq())
            .flatten();
        Batch {
            body,
            mark,
            dones: Vec::new(),
            payload_bytes: 0,
            keyless: false,
        }
    }

    fn push(&self, batch: &mut Batch, item: Item) {
        batch.keyless |= item.msg.key.is_none();
        match &mut batch.body {
            Body::Records(b) => {
                let extra = self.stamp.as_deref().map(|s| (headers::PRODUCER, s));
                let mark = if batch.dones.is_empty() {
                    batch.mark.as_deref()
                } else {
                    None
                };
                b.push_marked(&item.msg, extra, mark);
            }
            Body::Messages(v) => {
                batch.payload_bytes += item.msg.payload.len();
                v.push(decorate(item.msg, self.stamp.as_deref()));
            }
        }
        batch.dones.push(item.done);
    }
}

/// Batches one partition's queue. Batches in flight are driven by this task itself (no task
/// per batch). Ends when its queue is closed and drained and every batch has completed; it
/// holds a [`Client`] clone so the connections outlive the producer handle until then.
async fn partition_worker(w: Worker, mut rx: mpsc::Receiver<Item>) {
    let Worker {
        client,
        channel,
        partition,
        opts,
        in_flight,
        stamp,
        sticky,
    } = w;
    let inner = client.inner().clone();
    let max_batch = opts.max_batch.max(1);
    let max_bytes = opts.max_batch_bytes;
    let mut factory = BatchFactory {
        // Without a partition the broker may have to split keyed batches: `Publish` form.
        records: partition.is_some(),
        stamp,
        last_len: 0,
        max_bytes,
    };
    let mut running: FuturesUnordered<BatchSend> = FuturesUnordered::new();
    let mut batch = factory.new_batch(&inner);
    let mut deadline = Instant::now();
    let mut open = true;
    let mut idle_yields = 0u32;
    let mut seen = 0usize;
    // One timer for the whole worker, re-armed per batch (not one per loop iteration).
    let linger_timer = tokio::time::sleep_until(deadline);
    tokio::pin!(linger_timer);
    let mut armed_for = deadline;
    // Takes the current batch for sending and starts a new one; moves the sticky partition on
    // when the batch carried keyless messages.
    let take = |batch: &mut Batch, factory: &mut BatchFactory| -> Batch {
        let next = factory.new_batch(&inner);
        let b = std::mem::replace(batch, next);
        factory.last_len = b.bytes();
        if let (Some(p), true) = (partition, b.keyless) {
            rotate_sticky(&inner, &channel, &sticky, p);
        }
        b
    };
    loop {
        // Take whatever is queued right now (never waits).
        while open && batch.len() < max_batch && batch.bytes() < max_bytes {
            match rx.try_recv() {
                Ok(item) => {
                    if batch.is_empty() {
                        deadline = Instant::now() + opts.linger;
                    }
                    factory.push(&mut batch, item);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => open = false,
            }
        }
        if !open && batch.is_empty() && running.is_empty() {
            break;
        }
        let full = batch.len() >= max_batch || batch.bytes() >= max_bytes;
        // Adaptive linger: send at once when nothing of this partition is in flight; while
        // something is, wait for company until the batch is full or the linger expires.
        let ready = !batch.is_empty()
            && !(opts.ordered && !running.is_empty())
            && (full
                || !open
                || running.is_empty()
                || opts.linger.is_zero()
                || Instant::now() >= deadline);
        // Idle send: let messages published in the same tick join first (see IDLE_YIELDS).
        if ready
            && running.is_empty()
            && open
            && !full
            && !opts.linger.is_zero()
            && idle_yields < IDLE_YIELDS
            && (idle_yields == 0 || batch.len() > seen)
        {
            idle_yields += 1;
            seen = batch.len();
            tokio::task::yield_now().await;
            continue;
        }
        if ready && let Ok(permit) = in_flight.clone().try_acquire_owned() {
            idle_yields = 0;
            let b = take(&mut batch, &mut factory);
            running.push(Box::pin(send_batch(
                client.inner().clone(),
                channel.clone(),
                partition,
                b,
                permit,
            )));
            continue;
        }
        let lingering = !batch.is_empty() && !ready && !(opts.ordered && !running.is_empty());
        if lingering && armed_for != deadline {
            linger_timer.as_mut().reset(deadline);
            armed_for = deadline;
        }
        tokio::select! {
            biased;
            Some(()) = running.next(), if !running.is_empty() => {}
            permit = in_flight.clone().acquire_owned(), if ready => {
                let Ok(permit) = permit else { break };
                idle_yields = 0;
                let b = take(&mut batch, &mut factory);
                running.push(Box::pin(send_batch(
                    client.inner().clone(),
                    channel.clone(),
                    partition,
                    b,
                    permit,
                )));
            }
            m = rx.recv(), if open && !full => match m {
                Some(item) => {
                    if batch.is_empty() {
                        deadline = Instant::now() + opts.linger;
                    }
                    factory.push(&mut batch, item);
                }
                None => open = false,
            },
            _ = &mut linger_timer, if lingering => {}
        }
    }
    drop(client);
}

/// Moves the producer's sticky partition away from `p` (if it is still `p`).
fn rotate_sticky(inner: &Arc<Inner>, channel: &str, sticky: &AtomicU64, p: u32) {
    let topo = inner.topology();
    let Some(n) = topo.partitions(channel) else {
        return;
    };
    let mut next = inner.partition_of(&topo, channel, None).unwrap_or(0);
    if next == p && n > 1 {
        next = (p + 1) % n;
    }
    let _ = sticky.compare_exchange(
        u64::from(p),
        u64::from(next),
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
}

/// Publishes one batch (with the SDK's retries) and resolves each message's future.
async fn send_batch(
    inner: Arc<Inner>,
    channel: Arc<str>,
    partition: Option<u32>,
    batch: Batch,
    permit: OwnedSemaphorePermit,
) {
    let _permit = permit;
    let Batch { body, dones, .. } = batch;
    let body = match body {
        Body::Messages(m) => Ok(PublishBody::Messages(m)),
        Body::Records(b) => b.finish(inner.opts.compression).map(PublishBody::Records),
    };
    let deadline = Instant::now() + inner.opts.publish_timeout;
    let result = match body {
        Ok(body) => {
            inner
                .publish_routed(&channel, partition, body, deadline)
                .await
        }
        Err(e) => Err(e),
    };
    match result {
        Ok(ids) => {
            for (done, id) in dones.into_iter().zip(ids) {
                let _ = done.send(Ok(id));
            }
        }
        Err(e) => {
            for done in dones {
                let _ = done.send(Err(e.clone()));
            }
        }
    }
}

//! One TCP connection to one broker node: a writer task, a reader task, request correlation.
//!
//! A [`Conn`] lives exactly as long as its socket. When the socket dies every pending request
//! fails with [`ClientError::Closed`] and every subscription channel registered on it is
//! dropped; reconnecting means creating a new `Conn` (see `node.rs`).

use std::collections::{BTreeMap, HashMap};
use std::io::IoSlice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use fluxmq_proto::wire::{
    self, ClientFrame, ClientRequest, ErrorCode, MAX_FRAME_LEN, MIN_PROTOCOL_VERSION,
    PROTOCOL_VERSION, ServerFrame, ServerResponse,
};
use fluxmq_proto::{Message, PublishMessage};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;

use crate::acks::{AckBuffer, AckHolds};
use crate::error::{ClientError, Result};
use crate::options::{ClientOptions, TLS_SCHEME};
use crate::records::RecordsFrame;

/// Maximum number of queued frames coalesced into one socket write.
const MAX_FRAMES_PER_WRITE: usize = 1024;
/// On close, the writer waits at most this long for ack frames being handed over.
const CLOSE_ACK_WAIT: Duration = Duration::from_secs(1);
/// On close, the reader drains the socket at most this long, waiting for the broker's FIN.
const CLOSE_LINGER: Duration = Duration::from_secs(2);
/// Frame bodies up to this size are copied into the writer's staging buffer; larger ones
/// (publish batches) are handed to the socket as they are (vectored write, no copy).
const COPY_MAX: usize = 8 * 1024;
/// At most this many buffers per vectored write call.
const MAX_IOV: usize = 64;

/// What the broker told us in `HelloOk`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerInfo {
    /// Broker version string.
    pub broker_version: String,
    /// Node id of the broker.
    pub node_id: String,
    /// Id the broker assigned to this connection.
    pub connection_id: u64,
}

pub(crate) type RawDelivery = wire::Delivery;

/// What the reader task hands to a subscription: a decoded delivery, or a whole v4
/// `DeliverRecords` frame that the consumer task decodes lazily (zero-copy).
#[derive(Debug)]
pub(crate) enum Inbound {
    One(RawDelivery),
    Records(RecordsFrame),
}

/// How long a negotiated-down protocol version is reused for an address before the newest
/// version is tried again (a broker upgraded in place is picked up on the next reconnect).
const PROTOCOL_CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Copy)]
struct Downgrade {
    version: u16,
    at: Instant,
}

type ResponseTx = oneshot::Sender<Result<ServerResponse>>;

/// Client-wide signals raised by connections.
#[derive(Default)]
pub(crate) struct Signals {
    /// Highest `MetadataChanged` epoch seen on any connection.
    pub(crate) epoch_hint: AtomicU64,
    /// Wakes the metadata task.
    pub(crate) meta_refresh: Notify,
    /// Protocol version negotiated down per broker address (absent = [`PROTOCOL_VERSION`]).
    /// Cached so that reconnecting to an older broker doesn't pay a refused `Hello` each time.
    protocol: Mutex<HashMap<String, Downgrade>>,
    /// Addresses whose downgrade was already logged at `info` (logged once per address and
    /// version; repeats go to `debug`).
    logged: Mutex<HashMap<String, u16>>,
    /// Protocol version of the most recent successful handshake (0 = none yet): the producer's
    /// hint for which form to build batches in before it knows the target connection.
    last_protocol: std::sync::atomic::AtomicU16,
}

impl Signals {
    pub(crate) fn request_refresh(&self) {
        self.meta_refresh.notify_one();
    }

    fn protocol_for(&self, addr: &str) -> u16 {
        let mut map = self.protocol.lock();
        match map.get(addr) {
            Some(d) if d.at.elapsed() < PROTOCOL_CACHE_TTL => d.version,
            Some(_) => {
                map.remove(addr);
                PROTOCOL_VERSION
            }
            None => PROTOCOL_VERSION,
        }
    }

    fn set_protocol(&self, addr: &str, version: u16) {
        self.protocol.lock().insert(
            addr.to_string(),
            Downgrade {
                version,
                at: Instant::now(),
            },
        );
    }

    /// True the first time `addr` is downgraded to `version` (log at `info` once).
    fn first_downgrade(&self, addr: &str, version: u16) -> bool {
        self.logged.lock().insert(addr.to_string(), version) != Some(version)
    }

    /// Protocol version of the latest handshake on any connection ([`PROTOCOL_VERSION`] before
    /// the first one).
    pub(crate) fn protocol_hint(&self) -> u16 {
        match self.last_protocol.load(Ordering::Relaxed) {
            0 => PROTOCOL_VERSION,
            v => v,
        }
    }
}

/// When `e` is a broker refusing our `Hello` protocol version (a broker older than this SDK:
/// "unsupported protocol version 4 (broker speaks 2..=3)"), the version to try next: the
/// broker's newest version when the message names it, else one below `refused`. `None` = not
/// a version refusal, or nothing older left to try.
fn fallback_version(e: &ClientError, refused: u16) -> Option<u16> {
    let ClientError::Server(w) = e else {
        return None;
    };
    if w.code != ErrorCode::ProtocolViolation
        || !w.message.starts_with("unsupported protocol version")
    {
        return None;
    }
    // "... (broker speaks 2..=3)" / "(broker speaks 2)": the last number is its newest.
    let advertised = w.message.split("broker speaks").nth(1).and_then(|tail| {
        tail.split(|c: char| !c.is_ascii_digit())
            .rfind(|t| !t.is_empty())
            .and_then(|t| t.parse::<u16>().ok())
    });
    let next = match advertised {
        Some(v) if v < refused => v,
        _ => refused.checked_sub(1)?,
    };
    (next >= MIN_PROTOCOL_VERSION).then_some(next)
}

enum Pending {
    Plain(ResponseTx),
    /// The delivery sender is registered by the reader task *before* it processes the next
    /// frame, so no `Deliver` that follows `Subscribed` can be lost.
    Subscribe {
        tx: ResponseTx,
        deliveries: mpsc::Sender<Inbound>,
    },
}

#[derive(Default)]
struct State {
    pending: HashMap<u64, Pending>,
    subs: HashMap<u64, mpsc::Sender<Inbound>>,
    closed: Option<String>,
}

/// One queued outgoing frame: an optional inline head (length prefix + `req_id` varint of an
/// [`EncodedRequest`]) followed by `body`. Keeping the head separate lets a pre-encoded
/// request body go to the socket without being copied into a new frame buffer.
pub(crate) struct OutFrame {
    head: [u8; 14],
    head_len: u8,
    body: Bytes,
    /// Shared bytes after `body` (a `PublishBatch` records section), often empty.
    tail: Bytes,
}

impl OutFrame {
    /// A complete frame (length prefix included) in one buffer.
    fn whole(frame: Bytes) -> Self {
        Self {
            head: [0; 14],
            head_len: 0,
            body: frame,
            tail: Bytes::new(),
        }
    }

    fn head(&self) -> &[u8] {
        &self.head[..self.head_len as usize]
    }

    #[cfg(test)]
    fn to_bytes(&self) -> Bytes {
        let mut b = BytesMut::from(self.head());
        b.put_slice(&self.body);
        b.put_slice(&self.tail);
        b.freeze()
    }
}

/// Borrowing mirror of `ClientRequest::Publish`: same variant index (2) and field order, so
/// postcard encodes identical bytes (checked by a test).
#[derive(serde::Serialize)]
enum PublishShape<'a> {
    #[allow(dead_code)]
    Hello,
    #[allow(dead_code)]
    CreateChannel,
    Publish {
        channel: &'a str,
        partition: Option<u32>,
        messages: &'a [PublishMessage],
    },
}

fn publish_msg_hint(m: &PublishMessage) -> usize {
    16 + m.payload.len()
        + m.key.as_ref().map_or(0, |k| k.len() + 5)
        + m.headers
            .iter()
            .map(|(k, v)| k.len() + v.len() + 10)
            .sum::<usize>()
}

/// Generous upper estimate of the encoded size of `body`, so that encoding a publish batch
/// allocates once instead of growing (and copying) its buffer repeatedly.
fn encoded_size_hint(body: &ClientRequest) -> usize {
    match body {
        ClientRequest::Publish {
            channel, messages, ..
        } => 32 + channel.len() + messages.iter().map(publish_msg_hint).sum::<usize>(),
        _ => 128,
    }
}

/// A request body encoded once (postcard of [`ClientRequest`]) so that retries on other
/// connections don't have to clone or re-encode the messages. `tail` is a shared suffix of the
/// encoding (the records section of a `PublishBatch`), never copied.
#[derive(Clone)]
pub(crate) struct EncodedRequest {
    head: Bytes,
    tail: Bytes,
}

impl EncodedRequest {
    pub(crate) fn new(body: &ClientRequest) -> Result<Self> {
        let hint = encoded_size_hint(body).min(MAX_FRAME_LEN + 64);
        let b = postcard::to_extend(body, Vec::with_capacity(hint))
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        Self::checked(Bytes::from(b), Bytes::new())
    }

    fn checked(head: Bytes, tail: Bytes) -> Result<Self> {
        let len = head.len() + tail.len();
        if len + 10 > MAX_FRAME_LEN {
            return Err(ClientError::Protocol(format!(
                "request of {len} bytes exceeds the frame limit"
            )));
        }
        Ok(Self { head, tail })
    }

    /// `Publish` from borrowed messages (no clone of the messages).
    pub(crate) fn publish_legacy(
        channel: &str,
        partition: Option<u32>,
        messages: &[PublishMessage],
    ) -> Result<Self> {
        let shape = PublishShape::Publish {
            channel,
            partition,
            messages,
        };
        let hint = (32 + channel.len() + messages.iter().map(publish_msg_hint).sum::<usize>())
            .min(MAX_FRAME_LEN + 64);
        let b = postcard::to_extend(&shape, Vec::with_capacity(hint))
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        Self::checked(Bytes::from(b), Bytes::new())
    }

    /// `PublishBatch` without copying the records section into the encoding. postcard writes
    /// structs as their fields back to back and `Bytes` as `varint(len) ++ bytes`; the section
    /// body is the request's last field. So the encoding of the same request with an empty body
    /// ends in `varint(0)` (one zero byte); replacing it with `varint(len)` and appending the
    /// shared body gives exactly the reference encoding (checked by a test).
    pub(crate) fn publish_batch(
        channel: &str,
        partition: Option<u32>,
        records: &fluxmq_proto::batch::RecordsSection,
    ) -> Result<Self> {
        let empty = ClientRequest::PublishBatch {
            channel: channel.to_string(),
            partition,
            records: fluxmq_proto::batch::RecordsSection {
                compression: records.compression,
                record_count: records.record_count,
                body: Bytes::new(),
            },
        };
        let mut head = postcard::to_extend(&empty, Vec::with_capacity(channel.len() + 32))
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        debug_assert_eq!(head.last(), Some(&0));
        head.pop();
        fluxmq_proto::batch::put_varint(&mut head, records.body.len() as u64);
        Self::checked(Bytes::from(head), records.body.clone())
    }

    /// `u32 len | postcard(ClientFrame { req_id, body })`. postcard encodes a struct as the
    /// concatenation of its fields, so the frame is `varint(req_id) ++ body`. The body is
    /// shared, not copied.
    fn frame(&self, req_id: u64) -> OutFrame {
        let mut head = [0u8; 14];
        let id_len = postcard::to_slice(&req_id, &mut head[4..])
            .expect("u64 varint fits in 10 bytes")
            .len();
        let len = id_len + self.head.len() + self.tail.len();
        head[..4].copy_from_slice(&(len as u32).to_be_bytes());
        OutFrame {
            head,
            head_len: (4 + id_len) as u8,
            body: self.head.clone(),
            tail: self.tail.clone(),
        }
    }
}

/// One live (or dead) connection.
pub(crate) struct Conn {
    state: Mutex<State>,
    next_req: AtomicU64,
    tx: mpsc::Sender<OutFrame>,
    cancel: CancellationToken,
    info: OnceLock<ServerInfo>,
    /// Protocol version this connection negotiated in `Hello`.
    protocol: u16,
    pub(crate) addr: String,
    pub(crate) opts: Arc<ClientOptions>,
    signals: Arc<Signals>,
    /// Ack buffers of subscriptions on this connection, drained into the write queue when the
    /// connection is closed so that buffered acks still go out with the writer's final flush.
    ack_buffers: Mutex<Vec<Weak<AckBuffer>>>,
    /// Ack frames on their way into the write queue; the writer waits for them on close.
    ack_holds: Arc<AckHolds>,
}

impl Conn {
    fn next_req_id(&self) -> u64 {
        // Starts at 1; 0 means "no response wanted".
        self.next_req.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn info(&self) -> ServerInfo {
        self.info.get().cloned().unwrap_or_default()
    }

    /// Negotiated protocol version (4 = record batches, `AckRanges`, `Commit`).
    pub(crate) fn protocol(&self) -> u16 {
        self.protocol
    }

    pub(crate) fn node_id(&self) -> &str {
        self.info.get().map(|i| i.node_id.as_str()).unwrap_or("")
    }

    pub(crate) fn closed_reason(&self) -> Option<String> {
        self.state.lock().closed.clone()
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Resolves once the connection is dead.
    pub(crate) async fn dead(&self) {
        self.cancel.cancelled().await
    }

    fn closed_error(&self) -> ClientError {
        ClientError::Closed(
            self.closed_reason()
                .unwrap_or_else(|| "connection closed".into()),
        )
    }

    pub(crate) fn ack_holds(&self) -> &Arc<AckHolds> {
        &self.ack_holds
    }

    pub(crate) fn register_ack_buffer(&self, buf: &Arc<AckBuffer>) {
        let mut bufs = self.ack_buffers.lock();
        bufs.retain(|w| w.strong_count() > 0);
        bufs.push(Arc::downgrade(buf));
    }

    /// Queues every buffered ack (best effort, never waits).
    fn drain_ack_buffers(&self) {
        let bufs = std::mem::take(&mut *self.ack_buffers.lock());
        for buf in bufs.iter().filter_map(Weak::upgrade) {
            for req in buf.take_requests().reqs {
                if let Ok(frame) = Self::encode(0, req) {
                    let _ = self.tx.try_send(OutFrame::whole(frame));
                }
            }
        }
    }

    /// Marks the connection dead (idempotent): fails pending requests, ends subscriptions.
    /// Buffered acks are queued first; the writer flushes what is already queued, then closes
    /// the socket.
    pub(crate) fn fail(&self, reason: String) {
        if self.state.lock().closed.is_some() {
            return;
        }
        self.drain_ack_buffers();
        let (pending, subs) = {
            let mut st = self.state.lock();
            if st.closed.is_some() {
                return;
            }
            tracing::debug!(addr = %self.addr, %reason, "fluxmq connection closed");
            st.closed = Some(reason.clone());
            (
                std::mem::take(&mut st.pending),
                std::mem::take(&mut st.subs),
            )
        };
        for (_, p) in pending {
            let tx = match p {
                Pending::Plain(tx) | Pending::Subscribe { tx, .. } => tx,
            };
            let _ = tx.send(Err(ClientError::Closed(reason.clone())));
        }
        drop(subs);
        self.cancel.cancel();
    }

    fn encode(req_id: u64, body: ClientRequest) -> Result<Bytes> {
        wire::encode_frame(&ClientFrame { req_id, body })
            .map_err(|e| ClientError::Protocol(e.to_string()))
    }

    async fn enqueue(&self, frame: OutFrame) -> Result<()> {
        if self.tx.send(frame).await.is_err() {
            return Err(self.closed_error());
        }
        Ok(())
    }

    /// Sends a frame with `req_id` 0 (no response).
    pub(crate) async fn send_oneway(&self, body: ClientRequest) -> Result<()> {
        if let Some(r) = self.closed_reason() {
            return Err(ClientError::Closed(r));
        }
        let frame = Self::encode(0, body)?;
        self.enqueue(OutFrame::whole(frame)).await
    }

    /// Sends ack frames (`req_id` 0). Unlike [`Conn::send_oneway`] this still queues after
    /// [`Conn::fail`] while the writer has not made its final flush (see [`AckHolds`]).
    pub(crate) async fn send_ack(&self, body: ClientRequest) -> Result<()> {
        let frame = Self::encode(0, body)?;
        self.enqueue(OutFrame::whole(frame)).await
    }

    /// Best effort, never waits: used from `Drop`. The response (if any) is ignored.
    pub(crate) fn try_send_detached(&self, body: ClientRequest) {
        let req_id = self.next_req_id();
        if let Ok(frame) = Self::encode(req_id, body) {
            let _ = self.tx.try_send(OutFrame::whole(frame));
        }
    }

    pub(crate) async fn request(&self, body: ClientRequest) -> Result<ServerResponse> {
        let enc = EncodedRequest::new(&body)?;
        self.request_encoded(&enc, None, self.opts.request_timeout)
            .await
    }

    pub(crate) async fn request_encoded(
        &self,
        body: &EncodedRequest,
        deliveries: Option<mpsc::Sender<Inbound>>,
        timeout: Duration,
    ) -> Result<ServerResponse> {
        let req_id = self.next_req_id();
        let frame = body.frame(req_id);
        let (tx, rx) = oneshot::channel();
        {
            let mut st = self.state.lock();
            if let Some(r) = &st.closed {
                return Err(ClientError::Closed(r.clone()));
            }
            let p = match deliveries {
                Some(deliveries) => Pending::Subscribe { tx, deliveries },
                None => Pending::Plain(tx),
            };
            st.pending.insert(req_id, p);
        }
        let _guard = PendingGuard { conn: self, req_id };
        let fut = async {
            self.enqueue(frame).await?;
            match rx.await {
                Ok(r) => r,
                Err(_) => Err(self.closed_error()),
            }
        };
        match tokio::time::timeout(timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(ClientError::Timeout),
        }
    }

    pub(crate) fn remove_subscription(&self, subscription_id: u64) {
        self.state.lock().subs.remove(&subscription_id);
    }
}

/// Removes the pending entry when a request is cancelled or times out.
struct PendingGuard<'a> {
    conn: &'a Conn,
    req_id: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.conn.state.lock().pending.remove(&self.req_id);
    }
}

/// Opens a TCP connection to `addr`, performs the `Hello` handshake (bounded by
/// `connect_timeout`) and starts the I/O tasks.
pub(crate) async fn connect(
    addr: &str,
    opts: Arc<ClientOptions>,
    signals: Arc<Signals>,
) -> Result<Arc<Conn>> {
    let timeout = opts.connect_timeout;
    match tokio::time::timeout(timeout, connect_inner(addr, opts, signals)).await {
        Ok(r) => r,
        Err(_) => Err(ClientError::Timeout),
    }
}

/// Connects with the protocol version cached for `addr` (the newest by default). A broker
/// older than this SDK refuses the `Hello` version and closes the connection; the SDK then
/// reconnects with the broker's newest version (named in the refusal: v3 for a v0.6 broker,
/// v2 for v0.4/v0.5), or one version lower when the refusal doesn't say, until one is
/// accepted. The result is cached for `addr` (see [`PROTOCOL_CACHE_TTL`]) and logged once.
///
/// What each version changes for the SDK: v2 → no `DeliverBatch`; v3 → no record batches
/// (`Publish` instead of `PublishBatch`, individual `Ack`, no raw delivery, cumulative ack
/// emulated with individual acks). The public API is the same on every version.
async fn connect_inner(
    addr: &str,
    opts: Arc<ClientOptions>,
    signals: Arc<Signals>,
) -> Result<Arc<Conn>> {
    let mut version = signals.protocol_for(addr);
    loop {
        match connect_version(addr, opts.clone(), signals.clone(), version).await {
            Err(e) => {
                let Some(next) = fallback_version(&e, version) else {
                    return Err(e);
                };
                if signals.first_downgrade(addr, next) {
                    tracing::info!(
                        addr,
                        refused = version,
                        using = next,
                        "broker is older than this SDK: falling back to client protocol v{next}"
                    );
                } else {
                    tracing::debug!(addr, refused = version, using = next, "protocol fallback");
                }
                signals.set_protocol(addr, next);
                version = next;
            }
            Ok(c) => {
                signals.last_protocol.store(version, Ordering::Relaxed);
                return Ok(c);
            }
        }
    }
}

async fn connect_version(
    addr: &str,
    opts: Arc<ClientOptions>,
    signals: Arc<Signals>,
    version: u16,
) -> Result<Arc<Conn>> {
    let target = addr.strip_prefix(TLS_SCHEME).unwrap_or(addr);
    // TLS for `tls://` addresses and, when TLS is in effect (options or any tls:// seed), for
    // plain `host:port` ones too: addresses learned from cluster metadata carry no scheme.
    let tls = match opts.effective_tls() {
        Some(t) => Some((
            t.client_tls()
                .map_err(|e| ClientError::Protocol(format!("TLS configuration: {e}")))?,
            t.server_name.clone(),
        )),
        None => None,
    };
    let stream = TcpStream::connect(target).await?;
    stream.set_nodelay(true)?;
    match tls {
        None => {
            let (rd, wr) = stream.into_split();
            start(addr, rd, wr, opts, signals, version).await
        }
        Some((tls, server_name)) => {
            let name = match server_name {
                Some(n) => fluxmq_tls::pki_types::ServerName::try_from(n)
                    .map_err(|e| ClientError::Protocol(format!("TLS server name: {e}")))?,
                None => fluxmq_tls::server_name_for(target)
                    .map_err(|e| ClientError::Protocol(e.to_string()))?,
            };
            let tls = tls
                .connect_split(name, stream)
                .await
                .map_err(|e| ClientError::Io(format!("TLS handshake with {target}: {e}")))?;
            start(addr, tls.read, tls.write, opts, signals, version).await
        }
    }
}

/// Starts the I/O tasks over an established transport and performs `Hello`.
async fn start<R, W>(
    addr: &str,
    rd: R,
    wr: W,
    opts: Arc<ClientOptions>,
    signals: Arc<Signals>,
    protocol_version: u16,
) -> Result<Arc<Conn>>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(opts.write_queue_capacity.max(16));
    let hello = ClientRequest::Hello {
        protocol_version,
        client_name: opts.client_name.clone(),
        sdk_version: fluxmq_proto::VERSION.to_string(),
        auth_token: opts.auth_token.clone(),
    };
    let conn = Arc::new(Conn {
        state: Mutex::new(State::default()),
        next_req: AtomicU64::new(1),
        tx,
        cancel: CancellationToken::new(),
        info: OnceLock::new(),
        protocol: protocol_version,
        addr: addr.to_string(),
        opts,
        signals,
        ack_buffers: Mutex::new(Vec::new()),
        ack_holds: Arc::default(),
    });
    tokio::spawn(writer_task(conn.clone(), wr, rx));
    tokio::spawn(reader_task(conn.clone(), rd));
    // If this future is dropped (connect timeout), the guard kills the half-open connection.
    let guard = FailOnDrop(Some(conn.clone()));
    let result = conn.request(hello).await;
    let info = match result {
        Ok(ServerResponse::HelloOk {
            broker_version,
            node_id,
            connection_id,
        }) => ServerInfo {
            broker_version,
            node_id,
            connection_id,
        },
        Ok(other) => {
            return Err(ClientError::Protocol(format!(
                "expected HelloOk, got {other:?}"
            )));
        }
        Err(e) => return Err(e),
    };
    let _ = conn.info.set(info);
    guard.disarm();
    Ok(conn)
}

struct FailOnDrop(Option<Arc<Conn>>);

impl FailOnDrop {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for FailOnDrop {
    fn drop(&mut self) {
        if let Some(c) = self.0.take() {
            c.fail("handshake failed".into());
        }
    }
}

/// Coalesces queued frames into as few socket writes as possible: heads and small bodies are
/// copied into a staging buffer, large bodies are referenced as they are (vectored write).
#[derive(Default)]
struct FrameWriter {
    staging: BytesMut,
    segs: Vec<Bytes>,
}

impl FrameWriter {
    fn push(&mut self, f: OutFrame) {
        self.staging.put_slice(f.head());
        self.push_bytes(f.body);
        if !f.tail.is_empty() {
            self.push_bytes(f.tail);
        }
    }

    fn push_bytes(&mut self, b: Bytes) {
        if b.len() <= COPY_MAX {
            self.staging.put_slice(&b);
        } else {
            if !self.staging.is_empty() {
                self.segs.push(self.staging.split().freeze());
            }
            self.segs.push(b);
        }
    }

    /// Writes everything pushed so far and flushes.
    async fn write_to<W: AsyncWrite + Unpin>(&mut self, wr: &mut W) -> std::io::Result<()> {
        if !self.staging.is_empty() {
            self.segs.push(self.staging.split().freeze());
        }
        let (mut idx, mut off) = (0usize, 0usize);
        while idx < self.segs.len() {
            let n = {
                let iov: Vec<IoSlice<'_>> = self.segs[idx..]
                    .iter()
                    .take(MAX_IOV)
                    .enumerate()
                    .map(|(i, b)| IoSlice::new(if i == 0 { &b[off..] } else { &b[..] }))
                    .collect();
                wr.write_vectored(&iov).await?
            };
            if n == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            off += n;
            while idx < self.segs.len() && off >= self.segs[idx].len() {
                off -= self.segs[idx].len();
                idx += 1;
            }
        }
        self.segs.clear();
        wr.flush().await
    }
}

async fn writer_task<W: AsyncWrite + Unpin>(
    conn: Arc<Conn>,
    mut wr: W,
    mut rx: mpsc::Receiver<OutFrame>,
) {
    let mut out = FrameWriter::default();
    let result: std::io::Result<()> = async {
        loop {
            let first = tokio::select! {
                biased;
                m = rx.recv() => match m {
                    Some(b) => b,
                    None => break,
                },
                _ = conn.cancel.cancelled() => break,
            };
            out.push(first);
            for _ in 1..MAX_FRAMES_PER_WRITE {
                match rx.try_recv() {
                    Ok(b) => out.push(b),
                    Err(_) => break,
                }
            }
            out.write_to(&mut wr).await?;
        }
        // Graceful close: wait (bounded) for ack frames still being handed over, flush whatever
        // is queued (e.g. final acks), then FIN.
        let deadline = tokio::time::Instant::now() + CLOSE_ACK_WAIT;
        loop {
            let idle = conn.ack_holds.idle();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if conn.ack_holds.is_idle() {
                break;
            }
            tokio::select! {
                biased;
                m = rx.recv() => match m {
                    Some(b) => {
                        out.push(b);
                        out.write_to(&mut wr).await?;
                    }
                    None => break,
                },
                () = &mut idle => {}
                () = tokio::time::sleep_until(deadline) => break,
            }
        }
        while let Ok(b) = rx.try_recv() {
            out.push(b);
        }
        out.write_to(&mut wr).await?;
        wr.shutdown().await
    }
    .await;
    match result {
        Ok(()) => conn.fail("closed by client".into()),
        Err(e) => conn.fail(format!("write failed: {e}")),
    }
}

async fn reader_task<R: AsyncRead + Unpin>(conn: Arc<Conn>, rd: R) {
    let codec = LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(MAX_FRAME_LEN)
        .new_codec();
    let mut framed = FramedRead::new(rd, codec);
    let reason = loop {
        let next = tokio::select! {
            n = framed.next() => n,
            _ = conn.cancel.cancelled() => break "closed by client".to_string(),
        };
        let body = match next {
            None => break "connection closed by broker".to_string(),
            Some(Err(e)) => break format!("read failed: {e}"),
            Some(Ok(body)) => body,
        };
        let frame: ServerFrame = match decode_server_frame(body.freeze()) {
            Ok(f) => f,
            Err(e) => break format!("protocol error: undecodable frame: {e}"),
        };
        match frame {
            ServerFrame::Response { req_id, result } => {
                let mut st = conn.state.lock();
                match st.pending.remove(&req_id) {
                    Some(Pending::Plain(tx)) => {
                        drop(st);
                        let _ = tx.send(result.map_err(ClientError::Server));
                    }
                    Some(Pending::Subscribe { tx, deliveries }) => {
                        if let Ok(ServerResponse::Subscribed { subscription_id }) = &result {
                            st.subs.insert(*subscription_id, deliveries);
                        }
                        drop(st);
                        let _ = tx.send(result.map_err(ClientError::Server));
                    }
                    None => {
                        tracing::trace!(req_id, "response for unknown/expired request");
                    }
                }
            }
            ServerFrame::MetadataChanged { epoch } => {
                tracing::debug!(addr = %conn.addr, epoch, "metadata changed");
                conn.signals.epoch_hint.fetch_max(epoch, Ordering::Relaxed);
                conn.signals.request_refresh();
            }
            ServerFrame::Deliver {
                subscription_id,
                delivery,
            } => {
                let sender = conn.state.lock().subs.get(&subscription_id).cloned();
                match sender {
                    Some(tx) => {
                        if !forward(&conn, &tx, Inbound::One(delivery)).await {
                            break "closed by client".to_string();
                        }
                    }
                    None => tracing::trace!(subscription_id, "delivery for unknown subscription"),
                }
            }
            ServerFrame::DeliverBatch {
                subscription_id,
                deliveries,
            } => {
                let sender = conn.state.lock().subs.get(&subscription_id).cloned();
                match sender {
                    Some(tx) => {
                        let mut open = true;
                        for delivery in deliveries {
                            if !forward(&conn, &tx, Inbound::One(delivery)).await {
                                open = false;
                                break;
                            }
                        }
                        if !open {
                            break "closed by client".to_string();
                        }
                    }
                    None => tracing::trace!(subscription_id, "delivery for unknown subscription"),
                }
            }
            ServerFrame::Goodbye { reason } => break format!("broker goodbye: {reason}"),
            ServerFrame::DeliverRecords {
                subscription_id,
                partition,
                first_delivery_id,
                from_offset,
                count,
                envelopes,
            } => {
                if conn.protocol < 4 || count == 0 {
                    break "protocol error: unexpected DeliverRecords".to_string();
                }
                let sender = conn.state.lock().subs.get(&subscription_id).cloned();
                match sender {
                    Some(tx) => {
                        // Decoded lazily by the consumer task, off the reader.
                        let frame = RecordsFrame {
                            partition,
                            first_delivery_id,
                            from_offset,
                            count,
                            envelopes,
                        };
                        if !forward(&conn, &tx, Inbound::Records(frame)).await {
                            break "closed by client".to_string();
                        }
                    }
                    None => tracing::trace!(subscription_id, "delivery for unknown subscription"),
                }
            }
        }
    };
    let closed_here = conn.is_dead();
    conn.fail(reason);
    if closed_here {
        // Closed by this side: read (and discard) until the broker closes too. Closing the
        // socket with unread data makes the kernel send RST instead of FIN, and the broker
        // would lose the frames still in its receive buffer (e.g. the final acks).
        let _ = tokio::time::timeout(CLOSE_LINGER, async {
            while let Some(Ok(_)) = framed.next().await {}
        })
        .await;
    }
}

/// postcard variant indices of [`ServerFrame::Deliver`] and [`ServerFrame::DeliverBatch`]
/// (checked against the reference encoder by `delivery_frames_decode_like_proto`).
const DELIVER_VARIANT: u8 = 1;
const DELIVER_BATCH_VARIANT: u8 = 4;
const DELIVER_RECORDS_VARIANT: u8 = 5;

/// Borrowing mirror of the fields of [`ServerFrame::DeliverRecords`].
#[derive(Deserialize)]
struct DeliverRecordsRef<'a> {
    subscription_id: u64,
    partition: u32,
    first_delivery_id: u64,
    from_offset: u64,
    count: u32,
    #[serde(borrow)]
    envelopes: &'a [u8],
}

/// Borrowing mirror of [`wire::Delivery`]: same fields in the same order, so it decodes the
/// same bytes, but the payload is a slice of the frame instead of a fresh copy.
#[derive(Deserialize)]
struct DeliveryRef<'a> {
    delivery_id: u64,
    attempt: u32,
    #[serde(borrow)]
    message: MessageRef<'a>,
}

#[derive(Deserialize)]
struct MessageRef<'a> {
    partition: u32,
    offset: u64,
    key: Option<String>,
    headers: BTreeMap<String, String>,
    timestamp_ms: u64,
    #[serde(borrow)]
    payload: &'a [u8],
}

impl DeliveryRef<'_> {
    fn into_delivery(self, frame: &Bytes) -> RawDelivery {
        let m = self.message;
        RawDelivery {
            delivery_id: self.delivery_id,
            attempt: self.attempt,
            message: Message {
                partition: m.partition,
                offset: m.offset,
                key: m.key,
                headers: m.headers,
                timestamp_ms: m.timestamp_ms,
                payload: frame.slice_ref(m.payload),
            },
        }
    }
}

/// Decodes a server frame body. Delivery frames are decoded without copying payloads (each
/// payload is a slice of `body`); everything else goes through the reference decoder.
fn decode_server_frame(body: Bytes) -> std::result::Result<ServerFrame, wire::CodecError> {
    match body.first() {
        Some(&DELIVER_VARIANT) => {
            let (subscription_id, d): (u64, DeliveryRef<'_>) = postcard::from_bytes(&body[1..])?;
            Ok(ServerFrame::Deliver {
                subscription_id,
                delivery: d.into_delivery(&body),
            })
        }
        Some(&DELIVER_BATCH_VARIANT) => {
            let (subscription_id, ds): (u64, Vec<DeliveryRef<'_>>) =
                postcard::from_bytes(&body[1..])?;
            Ok(ServerFrame::DeliverBatch {
                subscription_id,
                deliveries: ds.into_iter().map(|d| d.into_delivery(&body)).collect(),
            })
        }
        Some(&DELIVER_RECORDS_VARIANT) => {
            let r: DeliverRecordsRef<'_> = postcard::from_bytes(&body[1..])?;
            Ok(ServerFrame::DeliverRecords {
                subscription_id: r.subscription_id,
                partition: r.partition,
                first_delivery_id: r.first_delivery_id,
                from_offset: r.from_offset,
                count: r.count,
                envelopes: body.slice_ref(r.envelopes),
            })
        }
        _ => wire::decode_body(&body),
    }
}

/// Hands one delivery to its subscription's queue. The queue is bounded but sized above the
/// prefetch, so the broker's flow control keeps it from filling and the non-blocking path is
/// the normal one; it only waits if the broker misbehaves. `false` = connection closed by the
/// client while waiting. A dropped consumer (receiver gone) just discards the delivery.
async fn forward(conn: &Conn, tx: &mpsc::Sender<Inbound>, delivery: Inbound) -> bool {
    match tx.try_send(delivery) {
        Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => true,
        Err(mpsc::error::TrySendError::Full(delivery)) => {
            tokio::select! {
                _ = tx.send(delivery) => true,
                _ = conn.cancel.cancelled() => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxmq_proto::PublishMessage;

    #[test]
    fn encoded_frame_matches_encode_frame() {
        let body = ClientRequest::Publish {
            channel: "orders".into(),
            partition: Some(3),
            messages: vec![PublishMessage {
                key: Some("k".into()),
                payload: Bytes::from_static(b"hello"),
                ..Default::default()
            }],
        };
        let enc = EncodedRequest::new(&body).unwrap();
        for req_id in [0u64, 1, 127, 128, 300, u64::MAX] {
            let a = enc.frame(req_id).to_bytes();
            let b = wire::encode_frame(&ClientFrame {
                req_id,
                body: body.clone(),
            })
            .unwrap();
            assert_eq!(a, b, "req_id {req_id}");
        }
    }

    fn delivery(id: u64, payload: &[u8]) -> wire::Delivery {
        wire::Delivery {
            delivery_id: id,
            attempt: 2,
            message: Message {
                partition: 3,
                offset: 1000 + id,
                key: id.is_multiple_of(2).then(|| format!("k{id}")),
                headers: [("flux-type".to_string(), "T".to_string())].into(),
                timestamp_ms: 1_700_000_000_000,
                payload: Bytes::copy_from_slice(payload),
            },
        }
    }

    /// The zero-copy decoder must agree with the reference decoder on every frame kind.
    #[test]
    fn delivery_frames_decode_like_proto() {
        let frames = vec![
            ServerFrame::Deliver {
                subscription_id: 300,
                delivery: delivery(1, b"hello"),
            },
            ServerFrame::DeliverBatch {
                subscription_id: 7,
                deliveries: (0..20)
                    .map(|i| delivery(i, &vec![i as u8; 100 * i as usize]))
                    .collect(),
            },
            ServerFrame::Response {
                req_id: 9,
                result: Ok(ServerResponse::Pong),
            },
            ServerFrame::Goodbye {
                reason: "bye".into(),
            },
            ServerFrame::MetadataChanged { epoch: 5 },
        ];
        for f in frames {
            let enc = wire::encode_frame(&f).unwrap();
            let body = enc.slice(4..);
            let ours = decode_server_frame(body.clone()).unwrap();
            assert_eq!(ours, f);
            let reference: ServerFrame = wire::decode_body(&body).unwrap();
            assert_eq!(ours, reference);
            // Payloads point into the frame (no copy).
            if let ServerFrame::DeliverBatch { deliveries, .. } = &ours {
                let range = body.as_ptr() as usize..body.as_ptr() as usize + body.len();
                for d in deliveries.iter().filter(|d| !d.message.payload.is_empty()) {
                    assert!(range.contains(&(d.message.payload.as_ptr() as usize)));
                }
            }
        }
        assert!(decode_server_frame(Bytes::from_static(&[DELIVER_BATCH_VARIANT, 0xff])).is_err());
    }

    #[tokio::test]
    async fn frame_writer_preserves_order_and_bytes() {
        let big = Bytes::from(vec![7u8; COPY_MAX * 3 + 5]);
        let body = ClientRequest::Publish {
            channel: "c".into(),
            partition: None,
            messages: vec![PublishMessage {
                payload: big,
                ..Default::default()
            }],
        };
        let enc = EncodedRequest::new(&body).unwrap();
        let small = wire::encode_frame(&ClientFrame {
            req_id: 0,
            body: ClientRequest::Ping,
        })
        .unwrap();
        let frames = vec![
            OutFrame::whole(small.clone()),
            enc.frame(1),
            enc.frame(200),
            OutFrame::whole(small.clone()),
            enc.frame(3),
        ];
        let mut expect = Vec::new();
        for f in &frames {
            expect.extend_from_slice(&f.to_bytes());
        }
        let mut w = FrameWriter::default();
        for f in frames {
            w.push(f);
        }
        let mut out: Vec<u8> = Vec::new();
        w.write_to(&mut out).await.unwrap();
        assert_eq!(out, expect);
        // Reusable after a write.
        w.push(OutFrame::whole(small.clone()));
        let mut out2: Vec<u8> = Vec::new();
        w.write_to(&mut out2).await.unwrap();
        assert_eq!(&out2[..], &small[..]);
    }

    #[test]
    fn publish_batch_and_legacy_encodings_match_reference() {
        use fluxmq_proto::batch::{Compression, RecordsSection};
        for (len, part) in [
            (0usize, None),
            (5, Some(3)),
            (200, Some(70_000)),
            (70_000, None),
        ] {
            let records = RecordsSection {
                compression: Compression::Lz4,
                record_count: 9,
                body: Bytes::from(vec![0xabu8; len]),
            };
            let enc = EncodedRequest::publish_batch("orders", part, &records).unwrap();
            let reference = ClientRequest::PublishBatch {
                channel: "orders".into(),
                partition: part,
                records: records.clone(),
            };
            for req_id in [0u64, 300] {
                let ours = enc.frame(req_id).to_bytes();
                let theirs = wire::encode_frame(&ClientFrame {
                    req_id,
                    body: reference.clone(),
                })
                .unwrap();
                assert_eq!(ours, theirs, "len {len}");
            }
            // The section is shared, not copied.
            if len > 0 {
                assert_eq!(enc.tail.as_ptr(), records.body.as_ptr());
            }
        }
        let msgs: Vec<PublishMessage> = (0..10)
            .map(|i| PublishMessage {
                key: (i % 2 == 0).then(|| format!("k{i}")),
                headers: [("h".to_string(), "v".to_string())].into(),
                payload: Bytes::from(vec![i as u8; i * 7]),
            })
            .collect();
        let ours = EncodedRequest::publish_legacy("c", Some(1), &msgs).unwrap();
        let theirs = EncodedRequest::new(&ClientRequest::Publish {
            channel: "c".into(),
            partition: Some(1),
            messages: msgs,
        })
        .unwrap();
        assert_eq!(ours.frame(5).to_bytes(), theirs.frame(5).to_bytes());
    }

    #[test]
    fn deliver_records_decodes_zero_copy() {
        let f = ServerFrame::DeliverRecords {
            subscription_id: 4,
            partition: 2,
            first_delivery_id: 1000,
            from_offset: 77,
            count: 3,
            envelopes: Bytes::from(vec![1u8; 300]),
        };
        let enc = wire::encode_frame(&f).unwrap();
        let body = enc.slice(4..);
        let ours = decode_server_frame(body.clone()).unwrap();
        assert_eq!(ours, f);
        assert_eq!(ours, wire::decode_body::<ServerFrame>(&body).unwrap());
        let ServerFrame::DeliverRecords { envelopes, .. } = ours else {
            unreachable!()
        };
        let range = body.as_ptr() as usize..body.as_ptr() as usize + body.len();
        assert!(range.contains(&(envelopes.as_ptr() as usize)));
    }

    #[test]
    fn version_fallback_steps_to_the_broker_version() {
        let refusal =
            |m: &str| ClientError::Server(wire::WireError::new(ErrorCode::ProtocolViolation, m));
        // v0.6 broker: "2..=3".
        let e = refusal("unsupported protocol version 4 (broker speaks 2..=3)");
        assert_eq!(fallback_version(&e, 4), Some(3));
        // Names only its newest: jump straight there.
        let e = refusal("unsupported protocol version 4 (broker speaks 2)");
        assert_eq!(fallback_version(&e, 4), Some(2));
        // Unparseable: one step down; never below the minimum.
        let e = refusal("unsupported protocol version 4");
        assert_eq!(fallback_version(&e, 4), Some(3));
        assert_eq!(fallback_version(&e, MIN_PROTOCOL_VERSION), None);
        // A broker claiming to be newer than what it refused: one step down.
        let e = refusal("unsupported protocol version 3 (broker speaks 2..=5)");
        assert_eq!(fallback_version(&e, 3), Some(2));
        // Other errors are not refusals.
        let e = ClientError::Server(wire::WireError::new(ErrorCode::Unauthorized, "bad"));
        assert_eq!(fallback_version(&e, 4), None);
        assert_eq!(fallback_version(&ClientError::Timeout, 4), None);
    }

    #[test]
    fn size_hint_covers_publish_encoding() {
        let body = ClientRequest::Publish {
            channel: "orders".into(),
            partition: Some(1),
            messages: (0..100)
                .map(|i| PublishMessage {
                    key: Some(format!("key-{i}")),
                    headers: [("a".to_string(), "b".to_string())].into(),
                    payload: Bytes::from(vec![0u8; i * 10]),
                })
                .collect(),
        };
        let exact = postcard::to_allocvec(&body).unwrap().len();
        assert!(encoded_size_hint(&body) >= exact);
    }
}

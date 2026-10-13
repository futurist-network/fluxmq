//! One client connection.
//!
//! Task layout:
//!
//! ```text
//!   socket read half ──► reader (this task): handshake, dispatch, in-flight request futures
//!                            │            │ SubCmd (ack/nack/stats)      ▲
//!                            │            ▼                               │
//!                            │      pump task per subscription ──────────┤ ServerFrame
//!                            ▼      (owns Subscription, calls next())    │ (bounded mpsc)
//!   socket write half ◄── writer task ◄──────────────────────────────────┘
//! ```
//!
//! `Subscription::next` needs `&mut` while `ack`/`nack` take `&self` and must be callable
//! while `next()` is waiting (acks are what free prefetch slots). The pump task therefore owns
//! the `Subscription` outright and `select!`s between its command queue (served first) and
//! `next()` (cancel-safe), so commands never wait for a delivery and there is no shared
//! mutable state. A delivery that can't be queued yet (writer backpressure) is parked while
//! commands keep being served, which avoids an ack ⇄ deliver deadlock.
//!
//! Batched delivery (protocol v3): for a connection that said `Hello` with v3 the pump pulls
//! everything ready for the subscription in one go (`Subscription::next_batch`, bounded by the
//! remaining prefetch credit, `MAX_DELIVER_BATCH` and `MAX_DELIVER_BATCH_BYTES`) and queues it
//! as one `DeliverBatch` frame. It never waits to fill a batch: a lone ready delivery goes out
//! at once (as a plain `Deliver`). Batches grow only when deliveries pile up — a burst, or
//! the writer being busy, in which case the parked batch is topped up right before it is
//! queued. v2 connections get one `Deliver` per delivery, as before.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;

use fluxmq_core::{
    Broker, CoreError, RawDelivery, SharedDelivery, SubMode, SubscriberInfo, Subscription,
};
use fluxmq_proto::wire::{
    AckMode, ClientFrame, ClientRequest, ClientStats, DeliveryRange, ErrorCode, MAX_DELIVER_BATCH,
    MAX_DELIVER_BATCH_BYTES, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION, ServerFrame, ServerResponse,
    SubscribeOptions, WireError,
};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;

/// Highest client protocol this broker accepts.
const MAX_ACCEPTED_PROTOCOL: u16 = PROTOCOL_VERSION;
const _: () = assert!(MAX_ACCEPTED_PROTOCOL <= PROTOCOL_VERSION);

use super::NetContext;
use super::codec::{Outbound, ServerCodec};
use super::writer::FrameWriter;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Queue depth of commands (ack/nack/stats) per subscription.
const SUB_COMMAND_QUEUE: usize = 256;
/// Records per poll of a raw-records subscription (in practice the prefetch credit and
/// `MAX_DELIVER_BATCH_BYTES` bound it first).
const MAX_RAW_RECORDS_PER_POLL: usize = 64 * 1024;
/// Max frames written before an explicit flush.
const WRITE_BATCH: usize = 64;
/// The writer's queue is written to the socket early only beyond this many bytes, so that
/// several `DeliverBatch`/`DeliverRecords` frames (up to `MAX_DELIVER_BATCH_BYTES` each)
/// still go out in one vectored write.
const WRITE_BACKPRESSURE_BYTES: usize = 2 * MAX_DELIVER_BATCH_BYTES;
const MAX_CLIENT_NAME: usize = 128;

type Reader<R> = FramedRead<R, ServerCodec>;
type ReplyTx = oneshot::Sender<Result<(), WireError>>;

/// Why a connection ended.
#[derive(Debug)]
enum Exit {
    /// Peer closed the socket.
    Closed,
    /// Broker shutting down: say Goodbye.
    Shutdown,
    /// Protocol violation or failed handshake: say Goodbye with this reason.
    Kick(String),
    /// Writer task ended (socket dead).
    WriterGone,
}

/// Keeps `Broker::connection_opened/closed` balanced even if the task panics.
struct ConnectionGuard(Broker);

impl ConnectionGuard {
    fn new(broker: &Broker) -> Self {
        broker.connection_opened();
        Self(broker.clone())
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connection_closed();
    }
}

/// Runs one plaintext connection to completion.
pub async fn run(
    stream: TcpStream,
    peer: SocketAddr,
    ctx: Arc<NetContext>,
    shutdown: CancellationToken,
) {
    let (rd, wr) = stream.into_split();
    run_io(rd, wr, peer, None, ctx, shutdown).await
}

/// Runs one connection to completion over any transport (plain TCP halves, TLS halves).
/// `tls_principal` is the subject CN of a verified client certificate (mTLS): it is shown as
/// `tls:<cn>` next to the remote address in connection info and logs. Without a `Hello`
/// token it authenticates through a certificate binding (v0.5); a token always wins.
pub async fn run_io<R, W>(
    rd: R,
    wr: W,
    peer: SocketAddr,
    tls_principal: Option<String>,
    ctx: Arc<NetContext>,
    shutdown: CancellationToken,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let _guard = ConnectionGuard::new(&ctx.broker);
    let mut reader = FramedRead::new(rd, ServerCodec::new());
    let writer = FrameWriter::new(wr);
    let (out_tx, out_rx) = mpsc::channel::<Outbound>(
        // A pump queues up to `MAX_RAW_FRAMES_PER_POLL` + 1 frames at once.
        ctx.limits
            .outbound_queue
            .max(fluxmq_core::MAX_RAW_FRAMES_PER_POLL + 1),
    );
    let delivery = super::delivery_runtime(ctx.limits.delivery_threads);
    let writer_fut = write_loop(writer, out_rx, ctx.limits.write_timeout);
    let writer_task = match &delivery {
        Some(rt) => rt.spawn(writer_fut),
        None => tokio::spawn(writer_fut),
    };

    let tls_cn = tls_principal.as_deref();
    let exit = match handshake(&mut reader, &out_tx, &ctx, connection_id, tls_cn, &shutdown).await {
        Ok((client_name, principal, protocol_version)) => {
            let tls = tls_principal.as_deref().map(|cn| format!("tls:{cn}"));
            tracing::debug!(connection_id, %peer, client_name, actor = %principal.actor(), tls = tls.as_deref().unwrap_or("-"), "client connected");
            let remote_addr = match &tls {
                Some(t) => format!("{peer} ({t})"),
                None => peer.to_string(),
            };
            let info = SubscriberInfo {
                client_name,
                remote_addr,
                connection_id,
            };
            let mut conn = Connection {
                ctx: ctx.clone(),
                info,
                out: out_tx.clone(),
                subs: HashMap::new(),
                in_flight: FuturesUnordered::new(),
                principal,
                protocol_version,
                delivery,
            };
            // Cluster: push `MetadataChanged` on topology/assignment changes (stage 9).
            let metadata_push = ctx.cluster.is_clustered().then(|| {
                tokio::spawn(push_metadata_changes(
                    ctx.cluster.watch_epoch(),
                    out_tx.clone(),
                ))
            });
            // v0.5: revocation / expiry of the token (or deletion of the certificate binding)
            // on any node closes the connection with `Goodbye { reason }`.
            let session_end = ctx.auth.session_watch(&conn.principal);
            let exit = tokio::select! {
                exit = conn.serve(&mut reader, &shutdown) => exit,
                end = session_end => Exit::Kick(end.reason().to_string()),
            };
            if let Some(task) = metadata_push {
                task.abort();
            }
            // Dropping the connection ends every pump (their command queues close), which
            // drops the Subscriptions and returns in-flight deliveries to their groups.
            drop(conn);
            exit
        }
        Err(exit) => exit,
    };
    tracing::debug!(connection_id, %peer, reason = ?exit, "client disconnected");

    let goodbye = match exit {
        Exit::Shutdown => Some("broker shutting down".to_string()),
        Exit::Kick(reason) => Some(reason),
        Exit::Closed | Exit::WriterGone => None,
    };
    if let Some(reason) = goodbye {
        let _ = timeout(
            Duration::from_secs(1),
            out_tx.send(ServerFrame::Goodbye { reason }.into()),
        )
        .await;
    }
    drop(out_tx);
    drop(reader);
    // The writer drains queued frames (bounded by its write timeout) and closes the socket.
    let _ = timeout(ctx.limits.write_timeout, writer_task).await;
}

fn reply(req_id: u64, result: Result<ServerResponse, WireError>) -> ServerFrame {
    ServerFrame::Response { req_id, result }
}

fn core_err(e: &CoreError) -> WireError {
    WireError::from(e)
}

/// Waits for `Hello`, validates version, authenticates `auth_token` once for the whole
/// connection (RBAC), answers `HelloOk`. Returns the client name and the principal.
async fn handshake<R: AsyncRead + Unpin>(
    reader: &mut Reader<R>,
    out: &mpsc::Sender<Outbound>,
    ctx: &NetContext,
    connection_id: u64,
    tls_cn: Option<&str>,
    shutdown: &CancellationToken,
) -> Result<(String, fluxmq_auth::Principal, u16), Exit> {
    let first = tokio::select! {
        () = shutdown.cancelled() => return Err(Exit::Shutdown),
        r = timeout(ctx.limits.handshake_timeout, reader.next()) => r,
    };
    let frame = match first {
        Err(_) => return Err(Exit::Kick("handshake timeout".into())),
        Ok(None) => return Err(Exit::Closed),
        Ok(Some(Err(e))) => return Err(Exit::Kick(format!("protocol error: {e}"))),
        Ok(Some(Ok(f))) => f,
    };
    let ClientFrame { req_id, body } = frame;
    let ClientRequest::Hello {
        protocol_version,
        client_name,
        sdk_version,
        auth_token,
    } = body
    else {
        let err = WireError::new(ErrorCode::ProtocolViolation, "first frame must be Hello");
        let _ = out.send(reply(req_id, Err(err)).into()).await;
        return Err(Exit::Kick("first frame must be Hello".into()));
    };
    if !(MIN_PROTOCOL_VERSION..=MAX_ACCEPTED_PROTOCOL).contains(&protocol_version) {
        let msg = format!(
            "unsupported protocol version {protocol_version} (broker speaks \
             {MIN_PROTOCOL_VERSION}..={MAX_ACCEPTED_PROTOCOL})"
        );
        let _ = out
            .send(
                reply(
                    req_id,
                    Err(WireError::new(ErrorCode::ProtocolViolation, msg.clone())),
                )
                .into(),
            )
            .await;
        return Err(Exit::Kick(msg));
    }
    let principal = match ctx.auth.authenticate_client(auth_token.as_deref(), tls_cn) {
        Ok(p) => p,
        Err(_) => {
            let msg = "invalid or missing auth token";
            let _ = out
                .send(reply(req_id, Err(WireError::new(ErrorCode::Unauthorized, msg))).into())
                .await;
            return Err(Exit::Kick(msg.into()));
        }
    };
    let client_name = sanitize_client_name(&client_name);
    tracing::trace!(sdk_version, "hello");
    let ok = ServerResponse::HelloOk {
        broker_version: fluxmq_proto::VERSION.to_string(),
        node_id: ctx.node_id.clone(),
        connection_id,
    };
    out.send(reply(req_id, Ok(ok)).into())
        .await
        .map_err(|_| Exit::WriterGone)?;
    Ok((client_name, principal, protocol_version))
}

fn sanitize_client_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_CLIENT_NAME)
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        "anonymous".to_string()
    } else {
        cleaned.to_string()
    }
}

/// Commands routed to a subscription's pump task.
enum SubCmd {
    Ack {
        delivery_ids: Vec<u64>,
        reply: Option<ReplyTx>,
    },
    Nack {
        delivery_id: u64,
        reason: String,
        retry: bool,
        reply: Option<ReplyTx>,
    },
    AckRanges {
        ranges: Vec<DeliveryRange>,
        reply: Option<ReplyTx>,
    },
    Commit {
        partition: u32,
        offset: u64,
        reply: Option<ReplyTx>,
    },
    Stats(ClientStats),
}

/// How a pump frames deliveries.
#[derive(Debug, Clone, Copy)]
struct PumpConfig {
    /// v3+: `DeliverBatch`.
    batch: bool,
    /// v4 raw-records subscription: `DeliverRecords` where possible.
    raw: bool,
    /// Byte bound per frame.
    frame_bytes: usize,
}

struct Connection {
    ctx: Arc<NetContext>,
    info: SubscriberInfo,
    out: mpsc::Sender<Outbound>,
    /// subscription_id → command queue of its pump task.
    subs: HashMap<u64, mpsc::Sender<SubCmd>>,
    /// Requests awaiting completion; each yields the response frame to send (if any).
    in_flight: FuturesUnordered<BoxFuture<'static, Option<ServerFrame>>>,
    /// Authenticated once at `Hello`; every request is authorized against it.
    principal: fluxmq_auth::Principal,
    /// Negotiated protocol: ≥ 3 deliveries go out as `DeliverBatch` frames; ≥ 4 enables the
    /// v4 requests and `DeliverRecords`.
    protocol_version: u16,
    /// Dedicated delivery runtime for pumps (`None` = the current runtime).
    delivery: Option<tokio::runtime::Handle>,
}

impl Connection {
    async fn serve<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut Reader<R>,
        shutdown: &CancellationToken,
    ) -> Exit {
        let max_in_flight = self.ctx.limits.max_in_flight_requests.max(1);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Exit::Shutdown,
                Some(done) = self.in_flight.next(), if !self.in_flight.is_empty() => {
                    if let Some(frame) = done
                        && let Err(exit) = self.send(frame).await {
                            return exit;
                        }
                }
                frame = reader.next(), if self.in_flight.len() < max_in_flight => match frame {
                    None => return Exit::Closed,
                    Some(Err(e)) => return Exit::Kick(format!("protocol error: {e}")),
                    Some(Ok(frame)) => {
                        if let Err(exit) = self.handle(frame).await {
                            return exit;
                        }
                    }
                },
            }
        }
    }

    async fn send(&mut self, frame: ServerFrame) -> Result<(), Exit> {
        self.out
            .send(frame.into())
            .await
            .map_err(|_| Exit::WriterGone)
    }

    async fn respond(
        &mut self,
        req_id: u64,
        result: Result<ServerResponse, WireError>,
    ) -> Result<(), Exit> {
        self.send(reply(req_id, result)).await
    }

    async fn handle(&mut self, frame: ClientFrame) -> Result<(), Exit> {
        let ClientFrame { req_id, body } = frame;
        if let Err(err) = self.authorize(&body) {
            if req_id == 0 {
                return Ok(());
            }
            return self.respond(req_id, Err(err)).await;
        }
        match body {
            ClientRequest::Hello { .. } => {
                let err = WireError::new(ErrorCode::ProtocolViolation, "already handshaken");
                self.respond(req_id, Err(err)).await
            }
            ClientRequest::Ping => self.respond(req_id, Ok(ServerResponse::Pong)).await,
            ClientRequest::CreateChannel(config) if self.ctx.cluster.is_clustered() => {
                // Through the controller; replies when the channel exists on this node.
                let cluster = self.ctx.cluster.clone();
                self.in_flight.push(
                    async move {
                        let result = cluster
                            .create_channel(config)
                            .await
                            .map(|_| ServerResponse::ChannelCreated)
                            .map_err(|e| fluxmq_cluster::wire_error(&e));
                        Some(reply(req_id, result))
                    }
                    .boxed(),
                );
                Ok(())
            }
            ClientRequest::CreateChannel(config) => {
                let result = self
                    .ctx
                    .broker
                    .create_channel(config)
                    .map(|_| ServerResponse::ChannelCreated);
                self.respond(req_id, result.map_err(|e| core_err(&e))).await
            }
            ClientRequest::Publish {
                channel,
                partition,
                messages,
            } => self.publish(req_id, channel, partition, messages).await,
            ClientRequest::Metadata { channels } => {
                let result = serde_json::to_string(&self.ctx.cluster.metadata(&channels))
                    .map(|json| ServerResponse::Metadata { json })
                    .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()));
                self.respond(req_id, result).await
            }
            ClientRequest::Subscribe {
                channel,
                group,
                prefetch,
                start,
            } => {
                self.subscribe(
                    req_id,
                    &channel,
                    &group,
                    prefetch,
                    start,
                    SubscribeOptions::default(),
                )
                .await
            }
            ClientRequest::SubscribeWith { .. }
            | ClientRequest::PublishBatch { .. }
            | ClientRequest::AckRanges { .. }
            | ClientRequest::Commit { .. }
                if self.protocol_version < 4 =>
            {
                let err = WireError::new(
                    ErrorCode::ProtocolViolation,
                    "v4 request on a connection that negotiated an older protocol",
                );
                self.respond(req_id, Err(err)).await
            }
            ClientRequest::SubscribeWith {
                channel,
                group,
                prefetch,
                start,
                options,
            } => {
                self.subscribe(req_id, &channel, &group, prefetch, start, options)
                    .await
            }
            ClientRequest::AckRanges {
                subscription_id,
                ranges,
            } => {
                self.route(req_id, subscription_id, |reply| SubCmd::AckRanges {
                    ranges,
                    reply,
                })
                .await
            }
            ClientRequest::Commit {
                subscription_id,
                partition,
                offset,
            } => {
                self.route(req_id, subscription_id, |reply| SubCmd::Commit {
                    partition,
                    offset,
                    reply,
                })
                .await
            }
            ClientRequest::PublishBatch {
                channel,
                partition,
                records,
            } => {
                self.publish_batch(req_id, channel, partition, records)
                    .await
            }
            ClientRequest::Unsubscribe { subscription_id } => {
                let result = match self.subs.remove(&subscription_id) {
                    Some(_) => Ok(ServerResponse::Ok),
                    None => Err(unknown_subscription(subscription_id)),
                };
                self.respond(req_id, result).await
            }
            ClientRequest::Ack {
                subscription_id,
                delivery_ids,
            } => {
                self.route(req_id, subscription_id, |reply| SubCmd::Ack {
                    delivery_ids,
                    reply,
                })
                .await
            }
            ClientRequest::Nack {
                subscription_id,
                delivery_id,
                reason,
                retry,
            } => {
                self.route(req_id, subscription_id, |reply| SubCmd::Nack {
                    delivery_id,
                    reason,
                    retry,
                    reply,
                })
                .await
            }
            ClientRequest::ReportStats(stats) => {
                let subscription_id = stats.subscription_id;
                let delivered = match self.subs.get(&subscription_id) {
                    Some(tx) => tx.send(SubCmd::Stats(stats)).await.is_ok(),
                    None => false,
                };
                if req_id == 0 {
                    return Ok(());
                }
                let result = if delivered {
                    Ok(ServerResponse::Ok)
                } else {
                    Err(unknown_subscription(subscription_id))
                };
                self.respond(req_id, result).await
            }
        }
    }

    async fn publish(
        &mut self,
        req_id: u64,
        channel: String,
        partition: Option<u32>,
        messages: Vec<fluxmq_proto::PublishMessage>,
    ) -> Result<(), Exit> {
        let ctx = self.ctx.clone();
        // Single node: `Broker::publish` as in v0.1; cluster: leader routing / forwarding;
        // messages with delay headers go to `$scheduled` instead (v0.12).
        let mut fut =
            async move { ctx.scheduler.publish(&channel, partition, messages).await }.boxed();
        // Poll once right away so the append is enqueued in request order; pipelined publishes
        // from one connection then keep their relative order while completing concurrently.
        match futures::poll!(fut.as_mut()) {
            Poll::Ready(result) => self.respond(req_id, publish_result(result)).await,
            Poll::Pending => {
                self.in_flight.push(
                    fut.map(move |r| Some(reply(req_id, publish_result(r))))
                        .boxed(),
                );
                Ok(())
            }
        }
    }

    /// Like [`Connection::publish`] for one producer-encoded batch (`PublishBatch`, v4).
    async fn publish_batch(
        &mut self,
        req_id: u64,
        channel: String,
        partition: Option<u32>,
        records: fluxmq_proto::batch::RecordsSection,
    ) -> Result<(), Exit> {
        let ctx = self.ctx.clone();
        let mut fut = async move {
            ctx.cluster
                .publish_batch(&channel, partition, records)
                .await
        }
        .boxed();
        let result = |r: Result<(u32, u64, u32), WireError>| {
            r.map(
                |(partition, base_offset, count)| ServerResponse::PublishBatchOk {
                    partition,
                    base_offset,
                    count,
                },
            )
        };
        match futures::poll!(fut.as_mut()) {
            Poll::Ready(r) => self.respond(req_id, result(r)).await,
            Poll::Pending => {
                self.in_flight
                    .push(fut.map(move |r| Some(reply(req_id, result(r)))).boxed());
                Ok(())
            }
        }
    }

    async fn subscribe(
        &mut self,
        req_id: u64,
        channel: &str,
        group: &str,
        prefetch: u32,
        start: fluxmq_proto::StartFrom,
        options: SubscribeOptions,
    ) -> Result<(), Exit> {
        // Forget subscriptions whose pump ended (channel deleted).
        self.subs.retain(|_, tx| !tx.is_closed());
        let limits = &self.ctx.limits;
        if self.subs.len() >= limits.max_subscriptions {
            let err = WireError::new(
                ErrorCode::Overloaded,
                format!(
                    "too many subscriptions on this connection (max {})",
                    limits.max_subscriptions
                ),
            );
            return self.respond(req_id, Err(err)).await;
        }
        if prefetch == 0 {
            return self
                .respond(
                    req_id,
                    Err(WireError::new(
                        ErrorCode::BadRequest,
                        "prefetch must be >= 1",
                    )),
                )
                .await;
        }
        if group.is_empty() {
            return self
                .respond(
                    req_id,
                    Err(WireError::new(
                        ErrorCode::BadRequest,
                        "group must not be empty",
                    )),
                )
                .await;
        }
        let prefetch = prefetch.min(limits.max_prefetch);
        // Cluster: a channel created through another node a moment ago may not exist here yet.
        self.ctx.cluster.await_local_channel(channel).await;
        let mode = SubMode {
            cumulative: options.ack_mode == AckMode::Cumulative,
            // Raw frames only go to connections that can decode them.
            raw_records: options.raw_records && self.protocol_version >= 4,
        };
        let sub = match self.ctx.broker.subscribe_with(
            channel,
            group,
            self.info.clone(),
            prefetch,
            start,
            mode,
        ) {
            Ok(s) => s,
            Err(CoreError::InvalidArgument(m)) => {
                return self
                    .respond(req_id, Err(WireError::new(ErrorCode::BadRequest, m)))
                    .await;
            }
            Err(e) => return self.respond(req_id, Err(core_err(&e))).await,
        };
        // `prefetch_bytes` bounds each delivery frame (and so the bytes taken per poll).
        let frame_bytes = options.prefetch_bytes.map_or(MAX_DELIVER_BATCH_BYTES, |b| {
            (b as usize).clamp(1, MAX_DELIVER_BATCH_BYTES)
        });
        let subscription_id = sub.id();
        // Queue the response before the pump can queue any Deliver for this subscription.
        self.respond(req_id, Ok(ServerResponse::Subscribed { subscription_id }))
            .await?;
        let (cmd_tx, cmd_rx) = mpsc::channel(SUB_COMMAND_QUEUE);
        let cfg = PumpConfig {
            batch: self.protocol_version >= 3,
            raw: mode.raw_records,
            frame_bytes,
        };
        let pump_fut = pump(sub, cmd_rx, self.out.clone(), cfg);
        match &self.delivery {
            Some(rt) => drop(rt.spawn(pump_fut)),
            None => drop(tokio::spawn(pump_fut)),
        }
        self.subs.insert(subscription_id, cmd_tx);
        Ok(())
    }

    /// Routes an ack/nack to its subscription. With `req_id == 0` nothing is answered.
    async fn route(
        &mut self,
        req_id: u64,
        subscription_id: u64,
        make: impl FnOnce(Option<ReplyTx>) -> SubCmd,
    ) -> Result<(), Exit> {
        let Some(tx) = self.subs.get(&subscription_id) else {
            return if req_id == 0 {
                Ok(())
            } else {
                self.respond(req_id, Err(unknown_subscription(subscription_id)))
                    .await
            };
        };
        if req_id == 0 {
            let _ = tx.send(make(None)).await;
            return Ok(());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx.send(make(Some(reply_tx))).await.is_err() {
            return self
                .respond(req_id, Err(unknown_subscription(subscription_id)))
                .await;
        }
        // Don't block the reader on the pump: answer when the pump has applied it.
        self.in_flight.push(
            async move {
                let result = reply_rx
                    .await
                    .unwrap_or_else(|_| Err(unknown_subscription(subscription_id)));
                Some(reply(req_id, result.map(|()| ServerResponse::Ok)))
            }
            .boxed(),
        );
        Ok(())
    }
}

fn publish_result(
    r: Result<Vec<fluxmq_proto::MessageId>, WireError>,
) -> Result<ServerResponse, WireError> {
    r.map(|ids| ServerResponse::PublishOk { ids })
}

impl Connection {
    /// RBAC for client-protocol requests. Ack/Nack/Unsubscribe/ReportStats ride on a
    /// subscription that was authorized at `Subscribe` (and only this connection's own
    /// subscriptions are addressable), so they need no further check.
    fn authorize(&self, body: &ClientRequest) -> Result<(), WireError> {
        use fluxmq_auth::Action;
        use fluxmq_proto::model::is_system_channel;
        // System channels (`$…`) are written by the broker only.
        match body {
            ClientRequest::CreateChannel(config) if is_system_channel(&config.name) => {
                return Err(WireError::new(
                    ErrorCode::BadRequest,
                    "channel names starting with '$' are reserved for system channels",
                ));
            }
            ClientRequest::Publish { channel, .. }
            | ClientRequest::PublishBatch { channel, .. }
                if is_system_channel(channel) =>
            {
                return Err(WireError::new(
                    ErrorCode::Forbidden,
                    format!("'{channel}' is a system channel and is read-only"),
                ));
            }
            _ => {}
        }
        let action = match body {
            ClientRequest::CreateChannel(config) => Action::CreateChannel(&config.name),
            ClientRequest::Publish { channel, .. }
            | ClientRequest::PublishBatch { channel, .. } => Action::Publish(channel),
            ClientRequest::Subscribe { channel, .. }
            | ClientRequest::SubscribeWith { channel, .. } => Action::Consume(channel),
            ClientRequest::Metadata { .. } => Action::Read,
            // Subscription-scoped (authorized at subscribe time, own subscriptions only) or
            // connection-level requests. Listed explicitly so a new request kind can't skip RBAC.
            ClientRequest::Hello { .. }
            | ClientRequest::Unsubscribe { .. }
            | ClientRequest::Ack { .. }
            | ClientRequest::AckRanges { .. }
            | ClientRequest::Nack { .. }
            | ClientRequest::Commit { .. }
            | ClientRequest::ReportStats(_)
            | ClientRequest::Ping => return Ok(()),
        };
        self.ctx
            .auth
            .authorize(&self.principal, action)
            .map_err(|e| match e {
                fluxmq_auth::AuthError::Forbidden(msg) => WireError::new(ErrorCode::Forbidden, msg),
                other => WireError::new(ErrorCode::Forbidden, other.to_string()),
            })
    }
}

/// Forwards metadata epoch changes to the client until the connection's writer is gone.
async fn push_metadata_changes(
    mut epoch: tokio::sync::watch::Receiver<u64>,
    out: mpsc::Sender<Outbound>,
) {
    epoch.mark_unchanged();
    while epoch.changed().await.is_ok() {
        let e = *epoch.borrow_and_update();
        if out
            .send(ServerFrame::MetadataChanged { epoch: e }.into())
            .await
            .is_err()
        {
            return;
        }
    }
}

fn unknown_subscription(id: u64) -> WireError {
    WireError::new(ErrorCode::NotFound, format!("unknown subscription {id}"))
}

/// Owns one [`Subscription`]: forwards deliveries to the writer and applies ack/nack/stats.
/// Ends when the command queue closes (unsubscribe / disconnect), the subscription ends
/// (channel deleted, broker shutdown) or the writer is gone. With `batch` (v3) ready
/// deliveries are coalesced into `DeliverBatch` frames (see the module docs); a raw-records
/// subscription (v4) also gets `DeliverRecords` frames, one per contiguous stored range.
async fn pump(
    mut sub: Subscription,
    mut cmds: mpsc::Receiver<SubCmd>,
    out: mpsc::Sender<Outbound>,
    cfg: PumpConfig,
) {
    let subscription_id = sub.id();
    let max = if cfg.batch { MAX_DELIVER_BATCH } else { 1 };
    // Raw subscriptions: records per poll are bounded by the prefetch credit and the frame
    // bytes, not by `MAX_DELIVER_BATCH` (which bounds the decoded `DeliverBatch` part only,
    // see `Group::poll_batch`). A smaller bound split stored envelopes over several polls and
    // sent each whole envelope once per part.
    let max_poll = if cfg.raw {
        MAX_RAW_RECORDS_PER_POLL
    } else {
        max
    };
    // Taken from the group but not queued yet (writer backpressure); empty = nothing parked.
    let mut parked: Vec<SharedDelivery> = Vec::new();
    let mut parked_raw: Vec<RawDelivery> = Vec::new();
    let mut parked_bytes = 0usize;
    loop {
        let has_parked = !parked.is_empty() || !parked_raw.is_empty();
        tokio::select! {
            biased;
            cmd = cmds.recv() => match cmd {
                Some(cmd) => apply(&sub, cmd),
                None => break,
            },
            permit = out.reserve_many(frames_needed(&parked, &parked_raw)), if has_parked => {
                let Ok(permits) = permit else { break };
                // Waited for the writer: add what became ready meanwhile (never waits) —
                // only to the delivery batch, which goes out as one frame.
                if parked_raw.is_empty() && !parked.is_empty() {
                    let more = max - parked.len();
                    if more > 0 {
                        let room = cfg.frame_bytes.saturating_sub(parked_bytes);
                        let _ = sub.try_next_batch(&mut parked, more, room);
                    }
                }
                for (permit, frame) in permits.zip(drain_frames(subscription_id, &mut parked, &mut parked_raw)) {
                    permit.send(frame);
                }
                parked_bytes = 0;
            },
            got = next(&mut sub, &mut parked, &mut parked_raw, max_poll, cfg), if !has_parked => {
                let Some(bytes) = got else { break };
                match out.try_reserve_many(frames_needed(&parked, &parked_raw)) {
                    Ok(permits) => {
                        for (permit, frame) in permits.zip(drain_frames(subscription_id, &mut parked, &mut parked_raw)) {
                            permit.send(frame);
                        }
                    }
                    Err(mpsc::error::TrySendError::Full(())) => parked_bytes = bytes,
                    Err(mpsc::error::TrySendError::Closed(())) => break,
                }
            },
        }
    }
    tracing::debug!(subscription_id, "subscription pump ended");
}

async fn next(
    sub: &mut Subscription,
    parked: &mut Vec<SharedDelivery>,
    parked_raw: &mut Vec<RawDelivery>,
    max: usize,
    cfg: PumpConfig,
) -> Option<usize> {
    if cfg.raw {
        sub.next_frames(parked, parked_raw, max, cfg.frame_bytes)
            .await
    } else {
        sub.next_batch(parked, max, cfg.frame_bytes).await
    }
}

fn frames_needed(parked: &[SharedDelivery], raw: &[RawDelivery]) -> usize {
    raw.len() + usize::from(!parked.is_empty())
}

/// Turns the parked deliveries into frames, leaving both empty: each raw range as one
/// `DeliverRecords`, then the deliveries as one `Deliver`/`DeliverBatch` (see [`Outbound`];
/// messages are encoded straight from the group's shared copies).
fn drain_frames(
    subscription_id: u64,
    parked: &mut Vec<SharedDelivery>,
    raw: &mut Vec<RawDelivery>,
) -> impl Iterator<Item = Outbound> {
    let raw_frames = std::mem::take(raw).into_iter().map(move |r| {
        Outbound::Frame(ServerFrame::DeliverRecords {
            subscription_id,
            partition: r.partition,
            first_delivery_id: r.first_delivery_id,
            from_offset: r.from_offset,
            count: r.count,
            envelopes: r.envelopes,
        })
    });
    let deliveries = std::mem::take(parked);
    let batch = (!deliveries.is_empty()).then_some(Outbound::Deliveries {
        subscription_id,
        deliveries,
    });
    raw_frames.chain(batch)
}

fn apply(sub: &Subscription, cmd: SubCmd) {
    match cmd {
        SubCmd::Ack {
            delivery_ids,
            reply,
        } => {
            let r = sub.ack(&delivery_ids).map_err(|e| core_err(&e));
            finish(r, reply);
        }
        SubCmd::Nack {
            delivery_id,
            reason,
            retry,
            reply,
        } => {
            let r = sub
                .nack(delivery_id, &reason, retry)
                .map_err(|e| core_err(&e));
            finish(r, reply);
        }
        SubCmd::AckRanges { ranges, reply } => {
            let r = sub.ack_ranges(&ranges).map_err(|e| core_err(&e));
            finish(r, reply);
        }
        SubCmd::Commit {
            partition,
            offset,
            reply,
        } => {
            let r = sub.commit(partition, offset).map_err(|e| match e {
                CoreError::InvalidArgument(m) => WireError::new(ErrorCode::BadRequest, m),
                e => core_err(&e),
            });
            finish(r, reply);
        }
        SubCmd::Stats(stats) => sub.report_stats(stats),
    }
}

fn finish(result: Result<(), WireError>, reply: Option<ReplyTx>) {
    match reply {
        Some(tx) => {
            let _ = tx.send(result);
        }
        None => {
            if let Err(e) = result {
                tracing::debug!(error = %e, "fire-and-forget ack/nack failed");
            }
        }
    }
}

/// Drains the outgoing queue into the socket, batching flushes. Ends after `Goodbye`, when
/// every sender is gone, or when the socket stays unwritable for `write_timeout`.
///
/// Frames are encoded as chunks (see [`FrameWriter`]): stored envelopes and large payloads
/// reach the socket through vectored writes without being copied. Up to `WRITE_BATCH` frames
/// already queued are written together; more than `WRITE_BACKPRESSURE_BYTES` queued are
/// written out before encoding further frames.
async fn write_loop<W: AsyncWrite + Unpin>(
    mut sink: FrameWriter<W>,
    mut rx: mpsc::Receiver<Outbound>,
    write_timeout: Duration,
) {
    let result: Result<(), String> = async {
        while let Some(frame) = rx.recv().await {
            let mut goodbye = frame.is_goodbye();
            feed(&mut sink, frame, write_timeout).await?;
            let mut batched = 1;
            while !goodbye && batched < WRITE_BATCH {
                match rx.try_recv() {
                    Ok(frame) => {
                        goodbye = frame.is_goodbye();
                        feed(&mut sink, frame, write_timeout).await?;
                        batched += 1;
                    }
                    Err(_) => break,
                }
            }
            match timeout(write_timeout, sink.flush()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e.to_string()),
                Err(_) => return Err("write timed out".into()),
            }
            if goodbye {
                break;
            }
        }
        Ok(())
    }
    .await;
    if let Err(e) = result {
        tracing::debug!(error = %e, "connection writer stopped");
    }
    let _ = timeout(Duration::from_secs(1), sink.shutdown()).await;
}

/// Encodes `frame` behind the queued ones. Only when more than the backpressure boundary is
/// queued does this wait for the socket (bounded by `write_timeout`); the common case arms
/// no timer.
async fn feed<W: AsyncWrite + Unpin>(
    sink: &mut FrameWriter<W>,
    frame: Outbound,
    write_timeout: Duration,
) -> Result<(), String> {
    if sink.queued() > WRITE_BACKPRESSURE_BYTES {
        let ready = match futures::poll!(futures::future::poll_fn(|cx| {
            sink.poll_write_until(cx, WRITE_BACKPRESSURE_BYTES)
        })) {
            Poll::Ready(r) => r,
            Poll::Pending => {
                match timeout(write_timeout, sink.write_until(WRITE_BACKPRESSURE_BYTES)).await {
                    Ok(r) => r,
                    Err(_) => return Err("write timed out".into()),
                }
            }
        };
        ready.map_err(|e| e.to_string())?;
    }
    sink.push(frame).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_names() {
        assert_eq!(sanitize_client_name("  "), "anonymous");
        assert_eq!(sanitize_client_name("api\n-1"), "api-1");
        assert_eq!(
            sanitize_client_name(&"x".repeat(500)).len(),
            MAX_CLIENT_NAME
        );
    }
}

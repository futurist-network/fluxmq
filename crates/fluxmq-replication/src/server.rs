//! Leader side: the replication listener. Each accepted connection is one follower node;
//! every request runs in its own task (fetches long-poll) and responses are funnelled through
//! a single writer task. Record payloads are read with positional reads on a blocking thread
//! and written to the socket without re-encoding.
//!
//! Follower reports (the fetch position and durable end of a `Fetch`, a `Progress`) are
//! applied inline in the connection's read loop, so the leader sees them in the order the
//! follower sent them; only the long-poll and the read of a fetch run in their own task.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fluxmq_core::{Broker, FetchError, FetchRequest, ReplicaHandle};
use tokio::io::BufWriter;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

use crate::protocol::{
    Capabilities, FetchExt, FetchReq, FetchResp, PROTOCOL_VERSION, ProgressReq, Request, Response,
    read_frame, secret_eq, write_frame,
};
use crate::throttle::{Grant, MAX_THROTTLE_WAIT, Throttles};
use crate::transport::Stream;

/// Upper bound for a follower-requested long-poll.
const MAX_WAIT_CAP: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct ServerCtx {
    pub broker: Broker,
    pub secret: Option<String>,
    pub node_id: String,
    pub fetch_max_bytes: u32,
    /// Catch-up throttle for followers outside the ISR (see `throttle`).
    pub throttles: Arc<Throttles>,
    /// Alerts for followers older than v0.7 asking for batch envelopes.
    pub versions: Arc<crate::NodeVersions>,
    /// Cluster mutual TLS (v0.4).
    pub tls: Option<fluxmq_tls::ClusterTls>,
}

pub(crate) async fn accept_loop(
    listener: TcpListener,
    ctx: Arc<ServerCtx>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            r = listener.accept() => match r {
                Ok((stream, peer)) => {
                    let ctx = ctx.clone();
                    let sd = shutdown.clone();
                    tokio::spawn(async move {
                        let stream = match Stream::accept(stream, ctx.tls.as_ref()).await {
                            Ok(s) => s,
                            Err(e) => {
                                warn!(%peer, error = %e, "replication TLS handshake failed");
                                return;
                            }
                        };
                        if let Err(e) = serve_conn(stream, ctx, sd).await {
                            debug!(%peer, error = %e, "replication connection ended");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "replication accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

async fn serve_conn(
    stream: Stream,
    ctx: Arc<ServerCtx>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), crate::protocol::FrameError> {
    let peer_cn = stream.peer_cn().map(str::to_string);
    let (mut rd, wr) = stream.into_split();
    let mut wr = BufWriter::with_capacity(256 * 1024, wr);
    let (peer, caps) =
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, read_frame::<_, Request>(&mut rd)).await {
            Ok(Ok(Some((
                _,
                Request::Hello {
                    node_id,
                    version,
                    secret,
                },
                caps,
            )))) => {
                let ok_secret = match (&ctx.secret, &secret) {
                    (None, _) => true,
                    (Some(want), Some(got)) => secret_eq(want, got),
                    (Some(_), None) => false,
                };
                let reject = if version != PROTOCOL_VERSION {
                    Some(format!(
                        "unsupported replication protocol version {version}"
                    ))
                } else if !ok_secret {
                    Some("bad cluster secret".to_string())
                } else {
                    None
                };
                if let Some(why) = reject {
                    warn!(peer = %node_id, reason = %why, "rejected replication peer");
                    write_frame(&mut wr, 0, &Response::HelloErr(why), &[]).await?;
                    tokio::io::AsyncWriteExt::flush(&mut wr).await?;
                    return Ok(());
                }
                write_frame(
                    &mut wr,
                    0,
                    &Response::HelloOk {
                        node_id: ctx.node_id.clone(),
                    },
                    &Capabilities::local().encode(),
                )
                .await?;
                tokio::io::AsyncWriteExt::flush(&mut wr).await?;
                (node_id, Capabilities::decode(&caps))
            }
            _ => return Ok(()),
        };
    if let Some(cn) = peer_cn.as_deref() {
        fluxmq_tls::cluster::warn_on_mismatch(&peer, Some(cn), "replication follower");
    }
    let (tx, mut rx) = mpsc::channel::<(u64, Response, Bytes)>(256);
    let writer = tokio::spawn(async move {
        while let Some((id, resp, payload)) = rx.recv().await {
            if write_frame(&mut wr, id, &resp, &payload).await.is_err() {
                return;
            }
            while let Ok((id, resp, payload)) = rx.try_recv() {
                if write_frame(&mut wr, id, &resp, &payload).await.is_err() {
                    return;
                }
            }
            if tokio::io::AsyncWriteExt::flush(&mut wr).await.is_err() {
                return;
            }
        }
    });
    let peer: Arc<str> = Arc::from(peer);
    // What the follower declared itself (not the registry: a pre-0.7 controller may drop
    // registry features, and that must not stall replication of envelopes already written).
    let peer_batches = caps.batch_records();
    let res = loop {
        let frame = tokio::select! {
            _ = shutdown.changed() => break Ok(()),
            f = read_frame::<_, Request>(&mut rd) => f,
        };
        let (id, req, payload) = match frame {
            Ok(Some(v)) => v,
            Ok(None) => break Ok(()),
            Err(e) => break Err(e),
        };
        match req {
            Request::Progress(p) => {
                let r = progress(&ctx, &peer, &p);
                if tx
                    .send((id, Response::Progress(r), Bytes::new()))
                    .await
                    .is_err()
                {
                    break Ok(());
                }
            }
            Request::Fetch(f) => match record_fetch(&ctx, &peer, f, &payload) {
                Err(e) => {
                    if tx
                        .send((id, Response::Fetch(Err(e)), Bytes::new()))
                        .await
                        .is_err()
                    {
                        break Ok(());
                    }
                }
                Ok(rec) => {
                    let ctx = ctx.clone();
                    let tx = tx.clone();
                    let peer = peer.clone();
                    tokio::spawn(async move {
                        let resp = match fetch(&ctx, &peer, peer_batches, rec).await {
                            Ok((resp, payload)) => (Response::Fetch(Ok(resp)), payload),
                            Err(e) => (Response::Fetch(Err(e)), Bytes::new()),
                        };
                        let _ = tx.send((id, resp.0, resp.1)).await;
                    });
                }
            },
            req => {
                let ctx = ctx.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let (resp, payload) = handle(&ctx, req).await;
                    let _ = tx.send((id, resp, payload)).await;
                });
            }
        }
    };
    drop(tx);
    writer.abort();
    res
}

/// Requests without per-follower ordering needs (fetches and progress reports are handled
/// in [`serve_conn`]).
async fn handle(ctx: &ServerCtx, req: Request) -> (Response, Bytes) {
    match req {
        Request::Fetch(_) | Request::Progress(_) => (
            Response::HelloErr("internal: misrouted request".into()),
            Bytes::new(),
        ),
        Request::EpochEndOffset {
            channel,
            partition,
            leader_epoch,
            requested,
        } => {
            let r = ctx
                .broker
                .replica(&channel, partition)
                .map_err(FetchError::from)
                .and_then(|h| h.epoch_end_offset(leader_epoch, requested));
            (Response::EpochEndOffset(r), Bytes::new())
        }
        Request::Hello { .. } => (Response::HelloErr("duplicate hello".into()), Bytes::new()),
    }
}

/// A v0.8 follower's durable end (see [`ProgressReq`]).
fn progress(ctx: &ServerCtx, peer: &str, p: &ProgressReq) -> Result<(), FetchError> {
    ctx.broker
        .replica(&p.channel, p.partition)
        .map_err(FetchError::from)?
        .record_progress(peer, p.epoch, p.durable_offset)
}

/// A fetch whose report was applied ([`ReplicaHandle::record_fetch`]), ready to long-poll
/// and read.
struct RecordedFetch {
    h: ReplicaHandle,
    f: FetchReq,
    req: FetchRequest,
    /// The leader's written end when the report was applied.
    leo: u64,
}

/// Applies the follower's report carried by a fetch (inline, in connection order).
fn record_fetch(
    ctx: &ServerCtx,
    peer: &str,
    f: FetchReq,
    payload: &[u8],
) -> Result<RecordedFetch, FetchError> {
    let ext = FetchExt::decode(payload).map_err(|e| FetchError::Internal(e.to_string()))?;
    let h = ctx
        .broker
        .replica(&f.channel, f.partition)
        .map_err(FetchError::from)?;
    let req = FetchRequest {
        replica: peer.to_string(),
        epoch: f.epoch,
        fetch_offset: f.fetch_offset,
        durable_offset: ext.map_or(f.fetch_offset, |e| e.durable_offset.min(f.fetch_offset)),
        max_bytes: f.max_bytes.clamp(1, ctx.fetch_max_bytes.max(1)) as usize,
        state: f.state,
    };
    let leo = h.record_fetch(&req)?;
    Ok(RecordedFetch { h, f, req, leo })
}

async fn fetch(
    ctx: &ServerCtx,
    peer: &str,
    peer_batches: bool,
    rec: RecordedFetch,
) -> Result<(FetchResp, Bytes), FetchError> {
    let RecordedFetch { h, f, mut req, leo } = rec;
    let mut throttled = false;
    if req.fetch_offset >= leo {
        let wait = Duration::from_millis(u64::from(f.max_wait_ms)).min(MAX_WAIT_CAP);
        h.wait_for_data(req.fetch_offset, req.epoch, wait).await;
    } else if !h.in_isr(peer) {
        // Catching up (not in the ISR): apply the partition's throttle, if any.
        let deadline = tokio::time::Instant::now() + MAX_THROTTLE_WAIT;
        loop {
            match ctx.throttles.grant(
                &f.channel,
                f.partition,
                peer,
                req.max_bytes,
                std::time::Instant::now(),
            ) {
                Grant::Unlimited => break,
                Grant::Bytes(n) => {
                    req.max_bytes = n;
                    throttled = true;
                    break;
                }
                Grant::Wait(d) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        // Out of budget: answer with metadata/state only.
                        req.max_bytes = 0;
                        break;
                    }
                    tokio::time::sleep(d.min(deadline - now)).await;
                }
            }
        }
    }
    let mut data = tokio::task::spawn_blocking(move || h.read_for_fetch(&req))
        .await
        .map_err(|e| FetchError::Internal(e.to_string()))??;
    if let Some(c) = &mut data.records
        && !peer_batches
        && crate::chunk::has_batches(&c.bytes)
    {
        // A pre-v0.7 follower would take an envelope for a corrupt record: serve the legacy
        // records before it, if any, and refuse the rest loudly.
        let (len, n) = crate::chunk::legacy_prefix(&c.bytes);
        if n == 0 {
            ctx.versions.alert_refused(peer, &f.channel, f.partition);
            return Err(FetchError::Internal(
                "partition holds batch records; this follower's release (< 0.7) can't read them"
                    .into(),
            ));
        }
        c.bytes = c.bytes.slice(..len);
        c.count = n;
    }
    if throttled && let Some(c) = &data.records {
        ctx.throttles
            .charge(&f.channel, f.partition, peer, c.bytes.len());
    }
    let (base_offset, record_count, payload) = match data.records {
        Some(c) => (c.base_offset, c.count, c.bytes),
        None => (f.fetch_offset, 0, Bytes::new()),
    };
    Ok((
        FetchResp {
            epoch: data.epoch,
            high_watermark: data.high_watermark,
            log_start_offset: data.log_start_offset,
            log_end_offset: data.log_end_offset,
            base_offset,
            record_count,
            epochs: data.epochs,
            state: data.state.map(|b| b.to_vec()),
        },
        payload,
    ))
}

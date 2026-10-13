//! Follower side: one fetcher task per followed partition.
//!
//! Lifecycle: (1) connect to the leader node; (2) epoch reconciliation — ask the leader for
//! the end offset of our last leader epoch and truncate the divergent suffix; (3) fetch loop
//! — long-poll for records from our LEO, append them (verified + fenced by epoch in core),
//! apply state records, adopt the leader's HW and epoch history. Every error backs off
//! exponentially and retries; the task ends only when its role is superseded (stale epoch),
//! the channel disappears, or it is aborted by `set_role` / shutdown.
//!
//! ## Fetch-ahead (v0.8)
//!
//! Against a leader that declares `durable-progress`, the fetch loop does not wait for a
//! chunk to be durable before fetching the next one: it hands the chunk to the writer
//! ([`ReplicaHandle::append_async`]) and fetches again from the end of what it queued, while
//! the writer fsyncs. The leader learns the durable end separately: in the [`FetchExt`] of
//! every fetch, and in a [`Request::Progress`] as soon as a queued chunk becomes durable.
//! Before v0.8 a chunk's fsync, the next fetch round trip and the report were strictly in
//! series, and data the leader wrote meanwhile waited for that whole cycle (measured: the
//! follower step was ~2/3 of the publish → ack time at 50k msg/s on 3 nodes).
//!
//! Durability is unchanged: only the durable end (`log_end_offset`, published after the
//! fsync) is ever reported, and the leader counts only reported durable ends for the HW.
//! On any error, truncation or reset the loop first waits for every queued chunk (they
//! complete or fail in the writer) so the log end is settled before it restarts.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fluxmq_core::{AppendTicket, CoreError, FetchError, RawChunk, ReplicaHandle};
use tracing::{debug, info, warn};

use crate::Inner;
use crate::chunk;
use crate::client::{ClientError, PeerClient};
use crate::protocol::{FetchExt, FetchReq, ProgressReq, Request, Response};

/// Fetch-ahead bounds per partition: chunks queued in the writer and not yet durable.
const AHEAD_MAX_CHUNKS: usize = 16;

const BACKOFF_MIN: Duration = Duration::from_millis(20);
const BACKOFF_MAX: Duration = Duration::from_secs(2);
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

enum Step {
    /// Retry after a backoff (transient problem).
    Retry(String),
    /// Re-run epoch reconciliation, then fetch again.
    Reconcile,
    /// Role superseded / channel gone: stop.
    Stop(String),
}

impl From<CoreError> for Step {
    fn from(e: CoreError) -> Self {
        match e {
            CoreError::StaleEpoch { .. }
            | CoreError::ChannelNotFound(_)
            | CoreError::ShuttingDown => Step::Stop(e.to_string()),
            CoreError::Corrupt(_) | CoreError::InvalidArgument(_) => Step::Reconcile,
            e => Step::Retry(e.to_string()),
        }
    }
}

pub(crate) async fn run(inner: Arc<Inner>, h: ReplicaHandle, epoch: u64, leader: String) {
    let mut backoff = BACKOFF_MIN;
    let mut reconcile = true;
    let tag = format!("{}/{}", h.channel(), h.partition());
    loop {
        let step = match inner.client_for(&leader).await {
            Err(e) => Step::Retry(e),
            Ok(client) => {
                let r = if reconcile {
                    reconcile_epochs(&h, &client, epoch).await
                } else {
                    Ok(())
                };
                match r {
                    Err(step) => step,
                    Ok(()) => {
                        reconcile = false;
                        fetch_loop(&inner, &h, &client, epoch, &mut backoff).await
                    }
                }
            }
        };
        match step {
            Step::Stop(why) => {
                debug!(partition = %tag, epoch, reason = %why, "fetcher stopped");
                return;
            }
            Step::Reconcile => {
                reconcile = true;
                backoff = BACKOFF_MIN;
            }
            Step::Retry(why) => {
                debug!(partition = %tag, leader = %leader, reason = %why, ?backoff, "fetch retry");
                inner.forget_client_if_dead(&leader);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

/// Truncates our log to the leader's end offset for our last epoch (Kafka KIP-279).
///
/// Always ends with a truncate *barrier* in the writer, even when nothing seems to need
/// truncating: the LEO observed here may still grow by publishes this node admitted while it
/// led the partition (queued, not yet committed when it was demoted). Deciding "no
/// truncation" from that stale LEO would let such a record be published at an offset the new
/// leader fills with different data (replica divergence). The barrier drains the writer and
/// clamps to `min(target, LEO)` after the drain.
async fn reconcile_epochs(h: &ReplicaHandle, client: &PeerClient, epoch: u64) -> Result<(), Step> {
    let leo = h.log_end_offset();
    let last = h.last_epoch();
    let req = Request::EpochEndOffset {
        channel: h.channel().to_string(),
        partition: h.partition(),
        leader_epoch: epoch,
        requested: last.unwrap_or(0),
    };
    let (resp, _) = client
        .call(req, RPC_TIMEOUT)
        .await
        .map_err(|e| Step::Retry(e.to_string()))?;
    let (leader_epoch, end) = match resp {
        Response::EpochEndOffset(Ok(v)) => v,
        Response::EpochEndOffset(Err(e)) => return Err(fetch_err(e)),
        other => return Err(Step::Retry(format!("unexpected response {other:?}"))),
    };
    let to = h.truncation_offset(leader_epoch, end);
    let after = h.truncate(epoch, to).await?;
    if after < leo || h.log_end_offset() != leo {
        info!(
            channel = h.channel(),
            partition = h.partition(),
            epoch,
            observed_leo = leo,
            truncated_to = after,
            "truncated divergent log suffix"
        );
    }
    Ok(())
}

fn fetch_err(e: FetchError) -> Step {
    match e {
        FetchError::OffsetOutOfRange { .. } => Step::Reconcile,
        // Our epoch is older than the leader's: a new role is on its way; keep retrying until
        // set_role replaces this task.
        e => Step::Retry(e.to_string()),
    }
}

async fn fetch_loop(
    inner: &Inner,
    h: &ReplicaHandle,
    client: &PeerClient,
    epoch: u64,
    backoff: &mut Duration,
) -> Step {
    if client.leader_caps.durable_progress() {
        let mut queued = VecDeque::new();
        let step = fetch_ahead_loop(inner, h, client, epoch, backoff, &mut queued).await;
        drain(&mut queued).await;
        step
    } else {
        fetch_serial_loop(inner, h, client, epoch, backoff).await
    }
}

/// A chunk in the writer, not yet durable.
struct Queued {
    ticket: AppendTicket,
    /// The leader's HW and epoch history from the fetch that brought the chunk.
    hw: u64,
    epochs: Vec<(u64, u64)>,
    bytes: usize,
}

/// Waits for every queued chunk (durable or failed; results are dropped).
async fn drain(queued: &mut VecDeque<Queued>) {
    while let Some(q) = queued.pop_front() {
        let _ = q.ticket.await;
    }
}

type FetchFuture<'a> =
    Pin<Box<dyn Future<Output = (u64, Result<(Response, Bytes), ClientError>)> + Send + 'a>>;

enum Event {
    /// The oldest queued chunk completed in the writer.
    Durable(fluxmq_core::Result<u64>),
    /// A fetch (sent at this offset) returned.
    Fetched(u64, Result<(Response, Bytes), ClientError>),
}

async fn front_durable(queued: &mut VecDeque<Queued>) -> fluxmq_core::Result<u64> {
    match queued.front_mut() {
        Some(q) => (&mut q.ticket).await,
        None => std::future::pending().await,
    }
}

async fn fetch_done(
    fetch: &mut Option<FetchFuture<'_>>,
) -> (u64, Result<(Response, Bytes), ClientError>) {
    match fetch.as_mut() {
        Some(f) => f.await,
        None => std::future::pending().await,
    }
}

/// v0.8 fetch loop (leader declared `durable-progress`), see the module docs.
async fn fetch_ahead_loop(
    inner: &Inner,
    h: &ReplicaHandle,
    client: &PeerClient,
    epoch: u64,
    backoff: &mut Duration,
    queued: &mut VecDeque<Queued>,
) -> Step {
    let max_wait = inner.config.fetch_max_wait;
    let max_bytes = inner.config.fetch_max_bytes;
    let ahead_bytes = (max_bytes as usize).saturating_mul(2).max(1 << 20);
    // Nothing is queued on entry, so our log end is the durable end.
    let mut next = h.log_end_offset();
    let mut queued_bytes = 0usize;
    // Durable end last sent to the leader (in a fetch or a progress report).
    let mut reported = next;
    let mut fetch: Option<FetchFuture<'_>> = None;
    loop {
        if h.is_deleted() {
            return Step::Stop("channel deleted".into());
        }
        if fetch.is_none() && queued.len() < AHEAD_MAX_CHUNKS && queued_bytes < ahead_bytes {
            let durable = h.log_end_offset();
            reported = durable;
            let req = Request::Fetch(FetchReq {
                channel: h.channel().to_string(),
                partition: h.partition(),
                epoch,
                fetch_offset: next,
                max_bytes: max_bytes
                    .min(u32::try_from(ahead_bytes - queued_bytes).unwrap_or(u32::MAX))
                    .max(1),
                max_wait_ms: max_wait.as_millis().min(u128::from(u32::MAX)) as u32,
                state: h.state_cursor(),
            });
            let ext = FetchExt {
                durable_offset: durable.min(next),
            }
            .encode();
            let at = next;
            fetch = Some(Box::pin(async move {
                (at, client.call_with(req, ext, max_wait + RPC_TIMEOUT).await)
            }));
        }
        let ev = tokio::select! {
            r = front_durable(queued) => Event::Durable(r),
            (at, r) = fetch_done(&mut fetch) => Event::Fetched(at, r),
        };
        match ev {
            Event::Durable(Err(e)) => {
                // Completed: never poll its ticket again (drain skips it).
                queued.pop_front();
                return e.into();
            }
            Event::Durable(Ok(mut leo)) => {
                let q = queued.pop_front().expect("a chunk completed");
                queued_bytes -= q.bytes;
                h.update_follower_meta(leo, q.hw, &q.epochs);
                // Take every other chunk that is durable already: one report for all.
                while let Some(front) = queued.front_mut() {
                    match front.ticket.try_wait() {
                        None => break,
                        Some(Err(e)) => {
                            queued.pop_front();
                            return e.into();
                        }
                        Some(Ok(l)) => {
                            let q = queued.pop_front().expect("front exists");
                            queued_bytes -= q.bytes;
                            leo = l;
                            h.update_follower_meta(leo, q.hw, &q.epochs);
                        }
                    }
                }
                // A fetch in flight may be parked in the leader's long-poll with an older
                // durable end: report now. Without one, the next fetch carries it.
                let durable = h.log_end_offset();
                if fetch.is_some() && durable > reported {
                    reported = durable;
                    let p = Request::Progress(ProgressReq {
                        channel: h.channel().to_string(),
                        partition: h.partition(),
                        epoch,
                        durable_offset: durable,
                    });
                    if let Err(e) = client.notify(p).await {
                        return Step::Retry(e.to_string());
                    }
                }
            }
            Event::Fetched(fetch_offset, r) => {
                fetch = None;
                let (resp, payload) = match r {
                    Ok(r) => r,
                    Err(e) => return Step::Retry(e.to_string()),
                };
                let f = match resp {
                    Response::Fetch(Ok(f)) => f,
                    Response::Fetch(Err(FetchError::OffsetOutOfRange { log_start, log_end })) => {
                        drain(queued).await;
                        queued_bytes = 0;
                        if fetch_offset < log_start {
                            warn!(
                                channel = h.channel(),
                                partition = h.partition(),
                                fetch_offset,
                                log_start,
                                log_end,
                                "behind leader log start; resetting replica"
                            );
                            if let Err(e) = h.reset_to(epoch, log_start).await {
                                return e.into();
                            }
                            next = h.log_end_offset();
                            reported = next.min(reported);
                            continue;
                        }
                        return Step::Reconcile;
                    }
                    Response::Fetch(Err(e)) => return fetch_err(e),
                    other => return Step::Retry(format!("unexpected response {other:?}")),
                };
                if f.record_count > 0 {
                    let info = match verified(h, fetch_offset, &f, &payload) {
                        Ok(i) => i,
                        Err(step) => return step,
                    };
                    if info.first_offset != fetch_offset {
                        // Anomalies are handled exactly as by the serial loop, on a settled
                        // log: wait for everything queued first.
                        drain(queued).await;
                        queued_bytes = 0;
                        match boundary_mismatch(h, epoch, fetch_offset, info.first_offset).await {
                            Ok(()) => {
                                next = h.log_end_offset();
                                reported = next.min(reported);
                                continue;
                            }
                            Err(step) => return step,
                        }
                    }
                    let chunk = RawChunk {
                        base_offset: f.base_offset,
                        count: f.record_count,
                        bytes: payload,
                    };
                    let bytes = chunk.bytes.len();
                    match h.append_async(epoch, &chunk).await {
                        Ok(ticket) => queued.push_back(Queued {
                            ticket,
                            hw: f.high_watermark,
                            epochs: f.epochs.clone(),
                            bytes,
                        }),
                        Err(e) => return e.into(),
                    }
                    queued_bytes += bytes;
                    next = f.base_offset + f.record_count;
                } else {
                    h.update_follower_meta(h.log_end_offset(), f.high_watermark, &f.epochs);
                }
                if let Some(state) = &f.state
                    && let Err(e) = h.apply_state(epoch, state)
                {
                    return e.into();
                }
                *backoff = BACKOFF_MIN;
            }
        }
    }
}

/// Verifies a fetch payload (framing, contiguity, CRC) against its header. Corruption on
/// the wire is retried (the same range is fetched again), never appended and never a
/// reason to truncate.
fn verified(
    h: &ReplicaHandle,
    fetch_offset: u64,
    f: &crate::protocol::FetchResp,
    payload: &Bytes,
) -> Result<chunk::ChunkInfo, Step> {
    match chunk::verify(payload) {
        Ok(i) if i.first_offset == f.base_offset && i.records() == f.record_count => Ok(i),
        Ok(i) => {
            warn!(
                channel = h.channel(),
                partition = h.partition(),
                fetch_offset,
                ?i,
                base_offset = f.base_offset,
                record_count = f.record_count,
                "fetch payload disagrees with its header; refetching"
            );
            Err(Step::Retry(
                "fetch payload disagrees with its header".into(),
            ))
        }
        Err(e) => {
            warn!(
                channel = h.channel(),
                partition = h.partition(),
                fetch_offset,
                error = %e,
                "corrupt fetch payload from leader; refetching"
            );
            Err(Step::Retry(format!("corrupt fetch payload: {e}")))
        }
    }
}

/// A chunk whose first entry is not at `fetch_offset` (our log end, nothing queued).
/// `Ok(())`: truncated, fetch again from the new log end.
async fn boundary_mismatch(
    h: &ReplicaHandle,
    epoch: u64,
    fetch_offset: u64,
    first_offset: u64,
) -> Result<(), Step> {
    if first_offset < fetch_offset && first_offset < h.high_watermark() {
        // Never drop records below our HW (they may be acked, and we may be elected
        // from the ISR before refetching them). Envelope boundaries are identical on
        // every replica of an epoch, so this means a bug or tampering: stay loud.
        tracing::error!(
            channel = h.channel(),
            partition = h.partition(),
            fetch_offset,
            envelope_base = first_offset,
            hw = h.high_watermark(),
            "leader batch starts below our HW inside our log; not truncating"
        );
        return Err(Step::Retry("leader envelope boundary below our HW".into()));
    }
    if first_offset < fetch_offset {
        // The leader's entry containing our LEO starts earlier: our log ends inside
        // one of its batches (divergent framing above our HW). Drop our partial copy
        // and refetch the whole envelope; the leader has the data.
        warn!(
            channel = h.channel(),
            partition = h.partition(),
            fetch_offset,
            envelope_base = first_offset,
            "log ends inside a leader batch; truncating to the batch start"
        );
        return match h.truncate(epoch, first_offset).await {
            Ok(leo) if leo <= first_offset => Ok(()),
            Ok(leo) => Err(Step::Retry(format!("truncation stopped at {leo}"))),
            Err(e) => Err(e.into()),
        };
    }
    Err(Step::Reconcile)
}

/// Fetch loop for leaders before v0.8: fetch from the durable end, append, wait until the
/// chunk is durable, repeat (the fetch offset is the durable end the leader counts).
async fn fetch_serial_loop(
    inner: &Inner,
    h: &ReplicaHandle,
    client: &PeerClient,
    epoch: u64,
    backoff: &mut Duration,
) -> Step {
    let max_wait = inner.config.fetch_max_wait;
    loop {
        if h.is_deleted() {
            return Step::Stop("channel deleted".into());
        }
        let fetch_offset = h.log_end_offset();
        let req = Request::Fetch(FetchReq {
            channel: h.channel().to_string(),
            partition: h.partition(),
            epoch,
            fetch_offset,
            max_bytes: inner.config.fetch_max_bytes,
            max_wait_ms: max_wait.as_millis().min(u128::from(u32::MAX)) as u32,
            state: h.state_cursor(),
        });
        let (resp, payload) = match client.call(req, max_wait + RPC_TIMEOUT).await {
            Ok(r) => r,
            Err(e) => return Step::Retry(e.to_string()),
        };
        let f = match resp {
            Response::Fetch(Ok(f)) => f,
            Response::Fetch(Err(FetchError::OffsetOutOfRange { log_start, log_end })) => {
                if fetch_offset < log_start {
                    // The leader's retention passed us: restart our log at its start.
                    warn!(
                        channel = h.channel(),
                        partition = h.partition(),
                        fetch_offset,
                        log_start,
                        log_end,
                        "behind leader log start; resetting replica"
                    );
                    if let Err(e) = h.reset_to(epoch, log_start).await {
                        return e.into();
                    }
                    continue;
                }
                return Step::Reconcile;
            }
            Response::Fetch(Err(e)) => return fetch_err(e),
            other => return Step::Retry(format!("unexpected response {other:?}")),
        };
        let res = if f.record_count > 0 {
            // v0.7: legacy records and batch envelopes, appended verbatim. Verify the framing,
            // contiguity and envelope CRC-32C first: corruption on the wire is retried (the
            // same range is fetched again), never appended and never a reason to truncate.
            let info = match verified(h, fetch_offset, &f, &payload) {
                Ok(i) => i,
                Err(step) => return step,
            };
            if info.first_offset != fetch_offset {
                match boundary_mismatch(h, epoch, fetch_offset, info.first_offset).await {
                    Ok(()) => continue,
                    Err(step) => return step,
                }
            }
            let chunk = RawChunk {
                base_offset: f.base_offset,
                count: f.record_count,
                bytes: payload,
            };
            h.append(epoch, &chunk, f.high_watermark, &f.epochs)
                .await
                .map(|_| ())
        } else {
            h.update_follower_meta(h.log_end_offset(), f.high_watermark, &f.epochs);
            Ok(())
        };
        if let Err(e) = res {
            return e.into();
        }
        if let Some(state) = &f.state
            && let Err(e) = h.apply_state(epoch, state)
        {
            return e.into();
        }
        *backoff = BACKOFF_MIN;
    }
}

//! One worker per enabled subscription: consumes as group `$webhook:<name>`, filters, batches
//! (with linger), delivers with at most `max_in_flight` concurrent requests and resolves every
//! delivery with ack / nack(retry) / nack(dead-letter).
//!
//! The [`Subscription`] stays owned by the worker loop (its `next` needs `&mut`); request tasks
//! report their verdicts over a bounded channel and the loop applies them.
//!
//! Ordering with `max_in_flight = 1`: the core retries a nacked stream message through a retry
//! queue while later messages of the same partition stay dispatchable. Within a partition a due
//! retry is always served before new messages, so after a retryable failure the worker stops
//! pulling until that retry is due — the next pull then yields the retried message first, and
//! per-partition order holds (other partitions pause for the backoff too: strict mode is a
//! single ordered stream). Exact with `batch_max = 1`; with batching, messages of a failed
//! batch can come back with different attempt counts (thus backoffs) and order is best-effort.
//!
//! Circuit breaker (`pause_after_failures` > 0): consecutive retryable failures open it; while
//! open the worker takes no messages (they wait in the channel, keeping their attempts) and
//! probes the endpoint with a synthetic delivery every [`PROBE_FIRST`], doubling up to
//! [`PROBE_MAX`]. A successful probe — or any in-flight request that succeeds — closes it.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fluxmq_core::{Broker, Subscription};
use fluxmq_proto::now_ms;
use fluxmq_proto::webhooks::{AttemptOutcome, WebhookAttempt, WebhookSubscription};
use fluxmq_proto::wire::Delivery;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::deliver::{self, Verdict};
use crate::ssrf::PrivatePolicy;
use crate::stats::Counters;

/// Extra time a stopping worker grants in-flight requests beyond their timeout.
const STOP_GRACE: Duration = Duration::from_secs(2);
/// Safety margin kept below the channel's ack timeout when holding for `Retry-After`.
const ACK_MARGIN: Duration = Duration::from_millis(500);
/// Slack added to a retry's due time before the strict-mode worker pulls again.
const DUE_SLACK: Duration = Duration::from_millis(5);
/// Circuit breaker probe interval: first, then doubling up to the max.
pub(crate) const PROBE_FIRST: Duration = Duration::from_secs(2);
pub(crate) const PROBE_MAX: Duration = Duration::from_secs(60);

pub(crate) struct Ctx {
    pub sub: Arc<WebhookSubscription>,
    pub client: reqwest::Client,
    pub policy: PrivatePolicy,
    pub counters: Arc<Counters>,
    pub broker: Broker,
}

enum Action {
    Ack,
    Nack { reason: String, retry: bool },
}

struct Done {
    items: Vec<(u64, u32)>, // (delivery id, attempt)
    action: Action,
    endpoint: Endpoint,
}

/// What a request says about the endpoint (circuit breaker input).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    /// Answered: 2xx, or a non-retryable status.
    Up,
    /// Retryable failure: no answer, 5xx, 408/425/429.
    Down,
    /// Says nothing about the endpoint (blocked by policy).
    Unknown,
}

/// Circuit breaker of one worker.
struct Breaker {
    threshold: u32,
    consecutive: u32,
    open: Option<Open>,
}

struct Open {
    cooldown: Duration,
    next_probe: Instant,
    probing: bool,
}

impl Breaker {
    fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// When the next probe is due (open and not probing).
    fn probe_at(&self) -> Option<Instant> {
        self.open
            .as_ref()
            .filter(|o| !o.probing)
            .map(|o| o.next_probe)
    }

    fn observe(&mut self, e: Endpoint, c: &Counters, name: &str) {
        match e {
            Endpoint::Up => {
                self.consecutive = 0;
                if self.open.is_some() {
                    self.close(c, name);
                }
            }
            Endpoint::Down => {
                self.consecutive = self.consecutive.saturating_add(1);
                if self.open.is_some() {
                    c.breaker_failures
                        .store(u64::from(self.consecutive), Ordering::Relaxed);
                } else if self.threshold > 0 && self.consecutive >= self.threshold {
                    warn!(webhook = %name, failures = self.consecutive, "endpoint failing: pausing delivery (circuit breaker open)");
                    let now = Instant::now();
                    self.open = Some(Open {
                        cooldown: PROBE_FIRST,
                        next_probe: now + PROBE_FIRST,
                        probing: false,
                    });
                    c.breaker_failures
                        .store(u64::from(self.consecutive), Ordering::Relaxed);
                    c.breaker_next_probe_ms
                        .store(now_ms() + PROBE_FIRST.as_millis() as u64, Ordering::Relaxed);
                    c.breaker_since_ms.store(now_ms(), Ordering::Relaxed);
                }
            }
            Endpoint::Unknown => {}
        }
    }

    fn probe_done(&mut self, ok: bool, c: &Counters, name: &str) {
        if ok {
            self.consecutive = 0;
            self.close(c, name);
            return;
        }
        let Some(o) = self.open.as_mut() else {
            return;
        };
        o.probing = false;
        o.cooldown = (o.cooldown * 2).min(PROBE_MAX);
        o.next_probe = Instant::now() + o.cooldown;
        self.consecutive = self.consecutive.saturating_add(1);
        c.breaker_failures
            .store(u64::from(self.consecutive), Ordering::Relaxed);
        c.breaker_next_probe_ms
            .store(now_ms() + o.cooldown.as_millis() as u64, Ordering::Relaxed);
    }

    fn close(&mut self, c: &Counters, name: &str) {
        if self.open.take().is_some() {
            info!(webhook = %name, "endpoint answers again: delivery resumed (circuit breaker closed)");
        }
        c.breaker_since_ms.store(0, Ordering::Relaxed);
    }
}

pub(crate) async fn run(ctx: Ctx, mut subn: Subscription, mut stop: watch::Receiver<bool>) {
    let sub = ctx.sub.clone();
    let max_in_flight = sub.max_in_flight.max(1) as usize;
    let batch_max = sub.batch_max.max(1) as usize;
    let linger = Duration::from_millis(u64::from(sub.batch_linger_ms));
    let strict = max_in_flight == 1;
    let ctx = Arc::new(ctx);
    let sem = Arc::new(Semaphore::new(max_in_flight));
    let (done_tx, mut done_rx) = mpsc::channel::<Done>(max_in_flight + 1);
    let mut tasks: JoinSet<()> = JoinSet::new();
    let mut batch: Vec<Delivery> = Vec::with_capacity(batch_max);
    let mut batch_started: Option<Instant> = None;
    let mut permit: Option<OwnedSemaphorePermit> = None;
    let mut pause_until: Option<Instant> = None;
    let mut outstanding: usize = 0;
    // Deliveries counted in `counters.in_flight` by this worker.
    let mut in_flight: u64 = 0;
    let mut stopping = false;
    let mut breaker = Breaker {
        threshold: sub.pause_after_failures,
        consecutive: 0,
        open: None,
    };
    let (probe_tx, mut probe_rx) = mpsc::channel::<bool>(1);

    loop {
        // Flush a ready batch.
        if !batch.is_empty() {
            let full = batch.len() >= batch_max;
            let lingered = batch_started.is_some_and(|t| t.elapsed() >= linger);
            if full || lingered || stopping {
                let p = permit
                    .take()
                    .expect("a batch is only filled while holding a permit");
                let started = batch_started.take().unwrap_or_else(Instant::now);
                let items = std::mem::take(&mut batch);
                if let Some(n) =
                    dispatch(&ctx, &subn, items, started, p, &done_tx, &stop, &mut tasks)
                {
                    outstanding += 1;
                    in_flight += n as u64;
                    ctx.counters
                        .in_flight
                        .fetch_add(n as u64, Ordering::Relaxed);
                }
                continue;
            }
        }
        if stopping {
            break;
        }
        let paused = pause_until.is_some_and(|p| Instant::now() < p);
        let can_pull = permit.is_some() && !paused && !breaker.is_open() && batch.len() < batch_max;
        let linger_at = batch_started.map(|t| t + linger);
        let probe_at = breaker.probe_at();
        tokio::select! {
            biased;
            r = stop.changed() => {
                let _ = r;
                stopping = true;
            }
            Some(done) = done_rx.recv() => {
                outstanding -= 1;
                in_flight -= done.items.len() as u64;
                breaker.observe(done.endpoint, &ctx.counters, &sub.name);
                if let Some(due) = apply(&ctx, &subn, done, strict) {
                    pause_until = Some(pause_until.map_or(due, |p| p.max(due)));
                }
            }
            Some(ok) = probe_rx.recv() => breaker.probe_done(ok, &ctx.counters, &sub.name),
            () = sleep_until_opt(probe_at), if probe_at.is_some() => {
                if let Some(o) = breaker.open.as_mut() {
                    o.probing = true;
                }
                let ctx = ctx.clone();
                let tx = probe_tx.clone();
                tasks.spawn(async move {
                    let _ = tx.send(probe(&ctx).await).await;
                });
            }
            p = sem.clone().acquire_owned(), if permit.is_none() && !paused => {
                permit = p.ok();
            }
            d = subn.next(), if can_pull => match d {
                Some(d) => {
                    batch.push(d);
                    batch_started.get_or_insert_with(Instant::now);
                    // Take whatever is ready right now without waiting (`next` is cancel-safe).
                    while batch.len() < batch_max {
                        let more = tokio::select! {
                            biased;
                            d = subn.next() => d,
                            () = std::future::ready(()) => None,
                        };
                        match more {
                            Some(d) => batch.push(d),
                            None => break,
                        }
                    }
                }
                // Channel deleted or broker shutting down.
                None => stopping = true,
            },
            () = sleep_until_opt(linger_at), if linger_at.is_some() => {}
            () = sleep_until_opt(pause_until), if paused => {}
        }
    }

    // Graceful stop: let in-flight requests finish (bounded), apply their verdicts. Anything
    // still unresolved returns to the group when the subscription drops.
    let budget = Duration::from_millis(u64::from(sub.timeout_ms)) + STOP_GRACE;
    let deadline = Instant::now() + budget;
    while outstanding > 0 {
        tokio::select! {
            Some(done) = done_rx.recv() => {
                outstanding -= 1;
                in_flight -= done.items.len() as u64;
                let _ = apply(&ctx, &subn, done, strict);
            }
            () = tokio::time::sleep_until(deadline) => {
                warn!(webhook = %sub.name, outstanding, "stopping with unfinished requests");
                break;
            }
        }
    }
    tasks.abort_all();
    ctx.counters
        .in_flight
        .fetch_sub(in_flight, Ordering::Relaxed);
    // A stopped worker holds nothing back (disabled, edited or the channel is gone).
    ctx.counters.breaker_since_ms.store(0, Ordering::Relaxed);
    debug!(webhook = %sub.name, "webhook worker ended");
}

async fn sleep_until_opt(t: Option<Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// Filters the batch (non-matching are acked and counted as skipped) and spawns the request.
/// Returns the number of deliveries now in flight, or `None` when nothing was sent.
#[allow(clippy::too_many_arguments)]
fn dispatch(
    ctx: &Arc<Ctx>,
    subn: &Subscription,
    items: Vec<Delivery>,
    started: Instant,
    permit: OwnedSemaphorePermit,
    done_tx: &mpsc::Sender<Done>,
    stop: &watch::Receiver<bool>,
    tasks: &mut JoinSet<()>,
) -> Option<usize> {
    let (send, skip): (Vec<Delivery>, Vec<Delivery>) = if ctx.sub.filter.is_empty() {
        (items, Vec::new())
    } else {
        items.into_iter().partition(|d| {
            let mc = fluxmq_router::filter::MatchContext::new(&d.message);
            fluxmq_router::filter::matches_in(&ctx.sub.filter, &mc)
        })
    };
    if !skip.is_empty() {
        let ids: Vec<u64> = skip.iter().map(|d| d.delivery_id).collect();
        if let Err(e) = subn.ack(&ids) {
            debug!(webhook = %ctx.sub.name, error = %e, "ack of skipped deliveries failed");
        }
        ctx.counters
            .skipped
            .fetch_add(skip.len() as u64, Ordering::Relaxed);
    }
    if send.is_empty() {
        return None;
    }
    let n = send.len();
    let ctx = ctx.clone();
    let done_tx = done_tx.clone();
    let stop = stop.clone();
    tasks.spawn(async move {
        let _permit = permit;
        let done = deliver_batch(&ctx, send, started, stop).await;
        let _ = done_tx.send(done).await;
    });
    // Reap finished task handles so the set stays small.
    while tasks.try_join_next().is_some() {}
    Some(n)
}

async fn deliver_batch(
    ctx: &Ctx,
    items: Vec<Delivery>,
    started: Instant,
    mut stop: watch::Receiver<bool>,
) -> Done {
    let sub = &ctx.sub;
    let refs: Vec<(&fluxmq_proto::Message, u32)> =
        items.iter().map(|d| (&d.message, d.attempt)).collect();
    let (body, headers) = deliver::build(sub, &refs);
    let r = deliver::send(&ctx.client, sub, &ctx.policy, body, headers, false).await;
    let verdict = deliver::classify(&r, sub.retry_on_4xx);
    let c = &ctx.counters;
    let n = items.len() as u64;
    let now = now_ms();
    c.last_delivery_ms.store(now, Ordering::Relaxed);
    // `last_status` is the last attempt's HTTP status: a transport-level failure (no
    // response) clears it, so `last_error` is what describes the last attempt.
    c.last_status
        .store(r.status.map_or(0, u64::from), Ordering::Relaxed);
    if r.status.is_some() {
        c.latency.record_us(r.latency.as_micros() as u64);
    }
    match &verdict {
        Verdict::Ack => {
            c.delivered.fetch_add(n, Ordering::Relaxed);
        }
        Verdict::Retry { reason, .. } | Verdict::Dead { reason } => {
            c.failed.fetch_add(n, Ordering::Relaxed);
            let detail = match (&r.error, &r.snippet) {
                (Some(e), _) => e.detail(),
                (None, Some(s)) if !s.trim().is_empty() => format!("{reason}: {}", s.trim()),
                _ => reason.clone(),
            };
            *c.last_error.lock() = Some((reason.clone(), detail));
        }
    }
    let (ok, outcome) = match &verdict {
        Verdict::Ack => (n, AttemptOutcome::Delivered),
        Verdict::Retry { .. } => (0, AttemptOutcome::Retry),
        Verdict::Dead { .. } => (0, AttemptOutcome::Dead),
    };
    let latency_us = r.status.map(|_| r.latency.as_micros() as u64);
    c.history.lock().record(now, ok, n - ok, latency_us);
    c.push_attempt(attempt_record(
        now,
        deliver::delivery_id(&sub.channel, &items[0].message),
        n as u32,
        items.iter().map(|d| d.attempt).min().unwrap_or(1),
        outcome,
        &r,
        &verdict,
    ));
    let endpoint = match &verdict {
        Verdict::Retry { .. } => Endpoint::Down,
        _ if r.status.is_some() => Endpoint::Up,
        _ => Endpoint::Unknown,
    };
    let ids: Vec<(u64, u32)> = items.iter().map(|d| (d.delivery_id, d.attempt)).collect();
    let action = match verdict {
        Verdict::Ack => Action::Ack,
        Verdict::Dead { reason } => Action::Nack {
            reason,
            retry: false,
        },
        Verdict::Retry {
            reason,
            retry_after,
        } => {
            if let Some(ra) = retry_after {
                let attempt = items.iter().map(|d| d.attempt).min().unwrap_or(1);
                if let Some(hold) =
                    retry_after_hold(&ctx.broker, &sub.channel, attempt, ra, started)
                    && !hold.is_zero()
                {
                    debug!(webhook = %sub.name, ?hold, "holding for Retry-After");
                    tokio::select! {
                        () = tokio::time::sleep(hold) => {}
                        _ = stop.changed() => {}
                    }
                }
            }
            Action::Nack {
                reason,
                retry: true,
            }
        }
    };
    Done {
        items: ids,
        action,
        endpoint,
    }
}

fn attempt_record(
    at_ms: u64,
    delivery_id: String,
    messages: u32,
    attempt: u32,
    outcome: AttemptOutcome,
    r: &deliver::SendResult,
    verdict: &Verdict,
) -> WebhookAttempt {
    let (error, error_detail) = match (verdict, &r.error) {
        (Verdict::Ack, _) => (None, None),
        (_, Some(e)) => (Some(e.reason().to_string()), Some(e.detail())),
        (Verdict::Retry { reason, .. } | Verdict::Dead { reason }, None) => (
            Some(reason.clone()),
            r.snippet
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| format!("{reason}: {s}")),
        ),
    };
    WebhookAttempt {
        at_ms,
        node: String::new(),
        delivery_id,
        messages,
        attempt,
        outcome,
        status: r.status,
        latency_ms: (r.latency.as_micros() as f64 / 10.0).round() / 100.0,
        error,
        error_detail,
    }
}

/// One circuit breaker probe: a synthetic delivery. Succeeds when the endpoint answers with
/// anything a real delivery would not retry (a receiver may reject the synthetic body with a
/// 4xx — it is up all the same).
async fn probe(ctx: &Ctx) -> bool {
    let sub = &ctx.sub;
    let now = now_ms();
    let id = format!("probe/{now}");
    let (body, headers) =
        deliver::synthetic(sub, &id, "FluxMQ webhook health probe (circuit breaker)");
    let r = deliver::send(&ctx.client, sub, &ctx.policy, body, headers, false).await;
    let verdict = deliver::classify(&r, sub.retry_on_4xx);
    let ok = r.status.is_some() && !matches!(verdict, Verdict::Retry { .. });
    let outcome = if ok {
        AttemptOutcome::ProbeOk
    } else {
        AttemptOutcome::ProbeFailed
    };
    ctx.counters
        .push_attempt(attempt_record(now, id, 0, 0, outcome, &r, &verdict));
    debug!(webhook = %sub.name, ok, status = ?r.status, "circuit breaker probe");
    ok
}

/// How long to hold a delivery before nacking so the redelivery happens after
/// `min(Retry-After, max backoff)`: that minus the backoff the nack will add, capped so the
/// delivery stays below the channel's ack timeout. `None` on the last attempt (dead-letters).
fn retry_after_hold(
    broker: &Broker,
    channel: &str,
    attempt: u32,
    retry_after: Duration,
    started: Instant,
) -> Option<Duration> {
    let cfg = broker.channel(channel)?;
    let pol = &cfg.retry;
    if attempt >= pol.max_attempts {
        return None;
    }
    let cap = Duration::from_millis(pol.backoff_ms.iter().copied().max().unwrap_or(0));
    let next = Duration::from_millis(next_backoff_ms(&pol.backoff_ms, attempt));
    let want = retry_after.min(cap).saturating_sub(next);
    let budget = Duration::from_millis(pol.ack_timeout_ms)
        .saturating_sub(started.elapsed())
        .saturating_sub(ACK_MARGIN);
    Some(want.min(budget))
}

/// Delay the core applies after failed attempt `attempt` (1-based).
fn next_backoff_ms(backoff: &[u64], attempt: u32) -> u64 {
    if backoff.is_empty() {
        return 0;
    }
    let i = (attempt.max(1) as usize - 1).min(backoff.len() - 1);
    backoff[i]
}

/// Applies a verdict. In strict mode returns when the group's earliest-due retry of these
/// deliveries becomes dispatchable (the worker doesn't pull before that).
fn apply(ctx: &Ctx, subn: &Subscription, done: Done, strict: bool) -> Option<Instant> {
    let n = done.items.len() as u64;
    ctx.counters.in_flight.fetch_sub(n, Ordering::Relaxed);
    match done.action {
        Action::Ack => {
            let ids: Vec<u64> = done.items.iter().map(|(id, _)| *id).collect();
            // UnknownDelivery: the ack timeout fired first (redelivered) — quietly ignored.
            if let Err(e) = subn.ack(&ids) {
                debug!(webhook = %ctx.sub.name, error = %e, "ack failed");
            }
            None
        }
        Action::Nack { reason, retry } => {
            let now = Instant::now();
            let mut due: Option<Instant> = None;
            let cfg = if retry && strict {
                ctx.broker.channel(&ctx.sub.channel)
            } else {
                None
            };
            for (id, attempt) in done.items {
                if let Err(e) = subn.nack(id, &reason, retry) {
                    debug!(webhook = %ctx.sub.name, error = %e, "nack failed");
                    continue;
                }
                if let Some(cfg) = &cfg
                    && attempt < cfg.retry.max_attempts
                {
                    let d = now
                        + Duration::from_millis(next_backoff_ms(&cfg.retry.backoff_ms, attempt))
                        + DUE_SLACK;
                    due = Some(due.map_or(d, |x: Instant| x.max(d)));
                }
            }
            due
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_index() {
        let b = [1_000, 10_000, 60_000];
        assert_eq!(next_backoff_ms(&b, 1), 1_000);
        assert_eq!(next_backoff_ms(&b, 2), 10_000);
        assert_eq!(next_backoff_ms(&b, 9), 60_000);
        assert_eq!(next_backoff_ms(&[], 3), 0);
    }
}

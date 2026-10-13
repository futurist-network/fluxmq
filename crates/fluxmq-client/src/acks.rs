//! Ack coalescing: one buffer per broker-side subscription.
//!
//! `Delivery::ack` only records the ack in the subscription's [`AckBuffer`]. The buffer goes
//! out as one fire-and-forget frame when
//!
//! * it reaches its size bound — sent inline by the acking task, so a fast consumer pays one
//!   frame per `batch` acks and gets natural backpressure from the connection queue;
//! * the oldest buffered ack has waited `linger` — sent by the subscription's timer task;
//! * the consumer unsubscribes, is dropped, the subscription is replaced (reconnect /
//!   leadership change), or the connection is closed by the client ([`Conn::fail`] drains every
//!   registered buffer into the write queue before the writer's final flush).
//!
//! Frames taken out of a buffer travel as [`AckFrames`], which hold the buffer's and the
//! connection's [`AckHolds`] until they are in the write queue. A flush that took frames just
//! before [`Conn::fail`] drained the buffer still gets them out: on close the writer waits
//! (bounded) for every hold to go before its final flush. An `Unsubscribe` waits for the
//! buffer's holds ([`AckBuffer::handed_over`]), so it never overtakes acks taken by the timer
//! task or a concurrent close.
//!
//! Frame forms: individual acks are kept as delivery id ranges (consecutive ids extend the last
//! range, so acking a `DeliverRecords` frame in order costs one range). On protocol v4 they go
//! out as `AckRanges` when that is smaller than listing the ids, else (and always on v2/v3) as
//! `Ack { delivery_ids }`. In cumulative mode the buffer keeps the highest position per
//! partition and sends one `Commit` per partition.
//!
//! Flow control: the broker frees a prefetch slot only when it sees the ack, so the size bound
//! is capped at half the prefetch. With at most `prefetch / 2` acks held back, at least half the
//! window is always available to the broker, and a consumer never stalls waiting on its own
//! buffered acks for longer than `linger`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use fluxmq_proto::wire::{ClientRequest, DeliveryRange};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::conn::Conn;

/// Hard cap of delivery ids per `Ack` frame (and of ranges per `AckRanges` frame).
pub(crate) const MAX_ACKS_PER_FRAME: usize = 4096;

/// Effective size bound for a subscription: the requested batch, at most half the prefetch,
/// at least 1, at most [`MAX_ACKS_PER_FRAME`].
pub(crate) fn effective_batch(requested: u32, prefetch: u32) -> usize {
    let half = (prefetch / 2).max(1);
    (requested.max(1).min(half) as usize).min(MAX_ACKS_PER_FRAME)
}

/// Counts [`AckFrames`] taken out of the buffers of one connection and not yet in its write
/// queue (see the module docs).
#[derive(Default)]
pub(crate) struct AckHolds {
    n: AtomicUsize,
    idle: Notify,
}

impl AckHolds {
    fn acquire(self: &Arc<Self>) -> Arc<Self> {
        self.n.fetch_add(1, Ordering::SeqCst);
        self.clone()
    }

    fn release(&self) {
        if self.n.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.idle.notify_waiters();
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.n.load(Ordering::SeqCst) == 0
    }

    /// Resolves after the last hold is released (create and `enable` it before checking
    /// [`AckHolds::is_idle`]).
    pub(crate) fn idle(&self) -> tokio::sync::futures::Notified<'_> {
        self.idle.notified()
    }
}

/// Holds a buffer's and its connection's [`AckHolds`].
struct AckHold([Arc<AckHolds>; 2]);

impl Drop for AckHold {
    fn drop(&mut self) {
        for h in &self.0 {
            h.release();
        }
    }
}

/// Frames taken out of an [`AckBuffer`], to be sent with [`AckBuffer::send`].
pub(crate) struct AckFrames {
    pub(crate) reqs: Vec<ClientRequest>,
    _hold: AckHold,
}

impl AckFrames {
    pub(crate) fn extend(&mut self, other: AckFrames) {
        self.reqs.extend(other.reqs);
    }
}

/// Buffered settlements of one subscription.
#[derive(Default)]
pub(crate) struct Pending {
    /// Individual acks as ranges, in ack order (may be unsorted / overlapping when acks come
    /// out of order; normalized when the frame is built).
    ranges: Vec<DeliveryRange>,
    /// Commit positions `(partition, offset)` (cumulative mode); one entry per partition.
    commits: Vec<(u32, u64)>,
    /// Settlements since the last flush (the size bound counts these).
    n: usize,
}

impl Pending {
    fn is_empty(&self) -> bool {
        self.ranges.is_empty() && self.commits.is_empty()
    }

    fn push_id(&mut self, id: u64) {
        match self.ranges.last_mut() {
            Some(r) if r.first + u64::from(r.count) == id && r.count < u32::MAX => r.count += 1,
            _ => self.ranges.push(DeliveryRange {
                first: id,
                count: 1,
            }),
        }
        self.n += 1;
    }

    fn push_commit(&mut self, partition: u32, offset: u64) {
        match self.commits.iter_mut().find(|(p, _)| *p == partition) {
            Some((_, o)) => *o = (*o).max(offset),
            None => self.commits.push((partition, offset)),
        }
        self.n += 1;
    }
}

/// Sorts and merges ranges (overlaps and adjacency), so that each id appears once.
pub(crate) fn normalize(mut ranges: Vec<DeliveryRange>) -> Vec<DeliveryRange> {
    if ranges
        .windows(2)
        .any(|w| w[1].first < w[0].first + u64::from(w[0].count))
    {
        ranges.sort_unstable_by_key(|r| r.first);
    }
    let mut out: Vec<DeliveryRange> = Vec::with_capacity(ranges.len());
    for r in ranges {
        if let Some(last) = out.last_mut() {
            let end = last.first + u64::from(last.count);
            if r.first <= end {
                let new_end = end.max(r.first + u64::from(r.count));
                if new_end - last.first <= u64::from(u32::MAX) {
                    last.count = (new_end - last.first) as u32;
                    continue;
                }
            }
        }
        out.push(r);
    }
    out
}

/// The frames for buffered settlements: `AckRanges` when `ranges_ok` (protocol v4) and it is
/// smaller than the id list, else `Ack`; one `Commit` per partition.
pub(crate) fn build_requests(
    subscription_id: u64,
    p: Pending,
    ranges_ok: bool,
) -> Vec<ClientRequest> {
    let mut out = Vec::new();
    if !p.ranges.is_empty() {
        let ranges = normalize(p.ranges);
        let ids: u64 = ranges.iter().map(|r| u64::from(r.count)).sum();
        if ranges_ok && (ranges.len() as u64) < ids {
            for chunk in ranges.chunks(MAX_ACKS_PER_FRAME) {
                out.push(ClientRequest::AckRanges {
                    subscription_id,
                    ranges: chunk.to_vec(),
                });
            }
        } else {
            let mut delivery_ids = Vec::with_capacity(ids.min(MAX_ACKS_PER_FRAME as u64) as usize);
            for r in ranges {
                for id in r.first..r.first + u64::from(r.count) {
                    delivery_ids.push(id);
                    if delivery_ids.len() == MAX_ACKS_PER_FRAME {
                        out.push(ClientRequest::Ack {
                            subscription_id,
                            delivery_ids: std::mem::take(&mut delivery_ids),
                        });
                    }
                }
            }
            if !delivery_ids.is_empty() {
                out.push(ClientRequest::Ack {
                    subscription_id,
                    delivery_ids,
                });
            }
        }
    }
    for (partition, offset) in p.commits {
        out.push(ClientRequest::Commit {
            subscription_id,
            partition,
            offset,
        });
    }
    out
}

pub(crate) struct AckBuffer {
    subscription_id: u64,
    batch: usize,
    linger: Duration,
    /// The connection speaks v4 (`AckRanges` / `Commit` available).
    v4: bool,
    pending: Mutex<Pending>,
    /// Frames taken out of this buffer and not yet queued.
    holds: Arc<AckHolds>,
    conn_holds: Arc<AckHolds>,
    /// Wakes the timer task: first ack into an empty buffer, or close.
    wake: Notify,
    closed: AtomicBool,
}

impl AckBuffer {
    /// Creates the buffer, registers it with `conn` (drained when the connection is closed)
    /// and starts its linger timer task.
    pub(crate) fn start(
        conn: &Arc<Conn>,
        subscription_id: u64,
        batch: usize,
        linger: Duration,
    ) -> Arc<AckBuffer> {
        let buf = Arc::new(AckBuffer {
            subscription_id,
            batch: batch.clamp(1, MAX_ACKS_PER_FRAME),
            linger,
            v4: conn.protocol() >= 4,
            pending: Mutex::new(Pending::default()),
            holds: Arc::default(),
            conn_holds: conn.ack_holds().clone(),
            wake: Notify::new(),
            closed: AtomicBool::new(false),
        });
        conn.register_ack_buffer(&buf);
        if !linger.is_zero() {
            tokio::spawn(timer_task(buf.clone(), conn.clone()));
        }
        buf
    }

    fn after_push(
        &self,
        p: parking_lot::MutexGuard<'_, Pending>,
        was_empty: bool,
    ) -> Option<AckFrames> {
        if p.n >= self.batch || self.linger.is_zero() {
            return Some(self.take_locked(p));
        }
        drop(p);
        if was_empty {
            self.wake.notify_one();
        }
        None
    }

    /// Buffers one ack; returns full-batch frames the caller must send.
    pub(crate) fn push(&self, delivery_id: u64) -> Option<AckFrames> {
        let mut p = self.pending.lock();
        let was_empty = p.is_empty();
        p.push_id(delivery_id);
        self.after_push(p, was_empty)
    }

    /// Several acks at once (cumulative-mode emulation on v2/v3 brokers).
    pub(crate) fn push_many(&self, ids: impl IntoIterator<Item = u64>) -> Option<AckFrames> {
        let mut p = self.pending.lock();
        let was_empty = p.is_empty();
        for id in ids {
            p.push_id(id);
        }
        if p.is_empty() {
            return None;
        }
        self.after_push(p, was_empty)
    }

    /// Buffers a commit position (cumulative mode, v4); returns frames the caller must send.
    pub(crate) fn push_commit(&self, partition: u32, offset: u64) -> Option<AckFrames> {
        let mut p = self.pending.lock();
        let was_empty = p.is_empty();
        p.push_commit(partition, offset);
        self.after_push(p, was_empty)
    }

    /// Takes everything buffered, as frames.
    pub(crate) fn take_requests(&self) -> AckFrames {
        self.take_locked(self.pending.lock())
    }

    /// The hold is taken under the buffer lock, so a concurrent [`Conn::fail`] either drains
    /// the acks itself or finds them held.
    fn take_locked(&self, mut p: parking_lot::MutexGuard<'_, Pending>) -> AckFrames {
        let hold = AckHold([self.holds.acquire(), self.conn_holds.acquire()]);
        let taken = std::mem::take(&mut *p);
        drop(p);
        AckFrames {
            reqs: build_requests(self.subscription_id, taken, self.v4),
            _hold: hold,
        }
    }

    /// Sends frames on `conn` (fire-and-forget). Still possible while the connection is being
    /// closed by the client, until the writer's final flush.
    pub(crate) async fn send(&self, conn: &Conn, frames: AckFrames) -> crate::Result<()> {
        for r in frames.reqs {
            conn.send_ack(r).await?;
        }
        Ok(())
    }

    /// Sends everything buffered now.
    pub(crate) async fn flush(&self, conn: &Conn) -> crate::Result<()> {
        let reqs = self.take_requests();
        self.send(conn, reqs).await
    }

    /// Sends everything buffered and waits until the broker confirmed the commits (acks stay
    /// fire-and-forget; frames on one connection are processed in order, so a confirmed commit
    /// also means earlier acks were seen).
    pub(crate) async fn flush_confirmed(&self, conn: &Conn) -> crate::Result<()> {
        for r in self.take_requests().reqs {
            match r {
                ClientRequest::Commit { .. } => match conn.request(r).await? {
                    fluxmq_proto::wire::ServerResponse::Ok => {}
                    other => return Err(crate::client::unexpected(other)),
                },
                r => conn.send_ack(r).await?,
            }
        }
        Ok(())
    }

    /// Resolves once every frame taken out of this buffer is in the write queue (or dropped).
    pub(crate) async fn handed_over(&self) {
        loop {
            let idle = self.holds.idle();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.holds.is_idle() {
                return;
            }
            idle.await;
        }
    }

    /// The subscription handle is gone: the timer task flushes what is left and exits.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Flushes a buffer `linger` after its first ack arrived (unless the size bound flushed it
/// first); flushes the remainder and exits on close or when the connection dies.
async fn timer_task(buf: Arc<AckBuffer>, conn: Arc<Conn>) {
    loop {
        tokio::select! {
            _ = buf.wake.notified() => {}
            _ = conn.dead() => return,
        }
        if !buf.is_closed() {
            tokio::select! {
                _ = tokio::time::sleep(buf.linger) => {}
                _ = conn.dead() => return,
            }
        }
        if buf.flush(&conn).await.is_err() {
            return;
        }
        if buf.is_closed() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(first: u64, count: u32) -> DeliveryRange {
        DeliveryRange { first, count }
    }

    #[test]
    fn batch_is_capped_by_half_the_prefetch() {
        assert_eq!(effective_batch(64, 2000), 64);
        assert_eq!(effective_batch(64, 64), 32);
        assert_eq!(effective_batch(64, 3), 1);
        assert_eq!(effective_batch(64, 1), 1);
        assert_eq!(effective_batch(0, 100), 1);
        assert_eq!(effective_batch(u32::MAX, u32::MAX), MAX_ACKS_PER_FRAME);
    }

    #[test]
    fn consecutive_acks_coalesce_into_ranges() {
        let mut p = Pending::default();
        for id in [10, 11, 12, 20, 21, 5] {
            p.push_id(id);
        }
        assert_eq!(p.ranges, vec![r(10, 3), r(20, 2), r(5, 1)]);
        assert_eq!(p.n, 6);
        let reqs = build_requests(7, p, true);
        assert_eq!(
            reqs,
            vec![ClientRequest::AckRanges {
                subscription_id: 7,
                ranges: vec![r(5, 1), r(10, 3), r(20, 2)],
            }]
        );
    }

    #[test]
    fn scattered_acks_and_v3_use_ack_frames() {
        let mut p = Pending::default();
        for id in [1, 3, 5] {
            p.push_id(id);
        }
        // No coalescing possible: the id list is smaller.
        assert_eq!(
            build_requests(1, p, true),
            vec![ClientRequest::Ack {
                subscription_id: 1,
                delivery_ids: vec![1, 3, 5],
            }]
        );
        let mut p = Pending::default();
        for id in 100..110 {
            p.push_id(id);
        }
        // v3 connection: always Ack.
        assert_eq!(
            build_requests(1, p, false),
            vec![ClientRequest::Ack {
                subscription_id: 1,
                delivery_ids: (100..110).collect(),
            }]
        );
    }

    #[test]
    fn normalize_merges_overlaps_and_duplicates() {
        assert_eq!(
            normalize(vec![r(10, 5), r(3, 2), r(12, 10), r(5, 5), r(40, 1)]),
            vec![r(3, 19), r(40, 1)]
        );
        assert_eq!(normalize(vec![r(1, 1), r(1, 1)]), vec![r(1, 1)]);
        assert!(normalize(Vec::new()).is_empty());
    }

    #[test]
    fn commits_keep_the_highest_position_per_partition() {
        let mut p = Pending::default();
        p.push_commit(0, 10);
        p.push_commit(1, 4);
        p.push_commit(0, 7);
        p.push_commit(0, 12);
        assert_eq!(p.n, 4);
        assert_eq!(
            build_requests(9, p, true),
            vec![
                ClientRequest::Commit {
                    subscription_id: 9,
                    partition: 0,
                    offset: 12
                },
                ClientRequest::Commit {
                    subscription_id: 9,
                    partition: 1,
                    offset: 4
                },
            ]
        );
    }

    #[test]
    fn huge_id_lists_are_chunked() {
        let mut p = Pending::default();
        for id in 0..(MAX_ACKS_PER_FRAME as u64 * 2 + 1) {
            p.push_id(id * 2);
        }
        let reqs = build_requests(1, p, true);
        assert_eq!(reqs.len(), 3);
        assert!(reqs.iter().all(|r| matches!(r, ClientRequest::Ack { delivery_ids, .. } if delivery_ids.len() <= MAX_ACKS_PER_FRAME)));
    }
}

//! Read-only access for live subscriptions (v0.10, `docs/design/v0.10-live-gateway.md`).
//!
//! A [`LiveReader`] reads a channel's partitions without any consumer-group state. It sees
//! exactly what a consumer may see: the committed end on a single node, `min(HW, high)` on a
//! replicated leader **or follower** (a follower adopts the leader's HW capped by its own
//! durable end; everything below it is on every electable replica and is never truncated).
//! Offline partitions are not served ([`LiveReader::visible_end`] is `None`).
//!
//! [`LiveWatch::changed`] wakes on every commit / HW move / role change / channel close
//! (`Channel::notify_live`); with no live reader the hot path pays one atomic load.

use std::io;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use fluxmq_proto::model::Message;

use crate::channel::Channel;
use crate::replica::ReplicaRole;

/// See the module docs. Cheap to clone (one `Arc`); reads are positional and lock-light.
#[derive(Clone)]
pub struct LiveReader {
    ch: Arc<Channel>,
}

/// Wake-ups of one [`LiveReader`] (see [`LiveReader::watch`]).
pub struct LiveWatch {
    rx: tokio::sync::watch::Receiver<u64>,
}

impl LiveWatch {
    /// Resolves when readable data may have moved (commit, HW, role change) or the channel
    /// closed. Cancel-safe.
    pub async fn changed(&mut self) {
        if self.rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// Marks the current generation seen (call before reading to avoid a spurious wake-up).
    pub fn mark_seen(&mut self) {
        self.rx.borrow_and_update();
    }
}

impl LiveReader {
    pub(crate) fn new(ch: Arc<Channel>) -> Self {
        Self { ch }
    }

    /// A wake-up source; subscribe it **before** reading so no change is missed.
    pub fn watch(&self) -> LiveWatch {
        LiveWatch {
            rx: self.ch.live.subscribe(),
        }
    }

    pub fn channel(&self) -> &str {
        &self.ch.name
    }

    pub fn partitions(&self) -> u32 {
        self.ch.partitions()
    }

    /// The channel was deleted: nothing more will arrive.
    pub fn is_closed(&self) -> bool {
        self.ch.deleted.load(Ordering::Acquire)
    }

    /// End (exclusive) of what may be delivered from `p`; `None` = this node does not serve
    /// the partition right now (replicated mode, no leader/follower role here).
    pub fn visible_end(&self, p: u32) -> Option<u64> {
        let pi = p as usize;
        let log = self.ch.logs.get(pi)?;
        if !self.ch.replicated.load(Ordering::Acquire) {
            return Some(log.high());
        }
        let r = &self.ch.repl[pi];
        r.serves_reads().then(|| r.high_watermark().min(log.high()))
    }

    /// End of the local log of `p` (committed, possibly above the HW).
    pub fn log_end(&self, p: u32) -> u64 {
        self.ch.logs.get(p as usize).map_or(0, |l| l.high())
    }

    /// `true` outside replicated mode.
    pub fn standalone(&self) -> bool {
        !self.ch.replicated.load(Ordering::Acquire)
    }

    /// First retained offset of `p`.
    pub fn low(&self, p: u32) -> u64 {
        self.ch.logs.get(p as usize).map_or(0, |l| l.low())
    }

    /// Current leader of `p` as known by this node's replica (followers only), for hints.
    pub fn leader_hint(&self, p: u32) -> Option<String> {
        if self.standalone() || p >= self.partitions() {
            return None;
        }
        match crate::replica::ReplicaHandle::new(self.ch.clone(), p)
            .status()
            .role
        {
            ReplicaRole::Follower { leader } if !leader.is_empty() => Some(leader),
            _ => None,
        }
    }

    /// The cached prefix of [`read`](Self::read): never blocks on I/O (may be short or empty
    /// even when more is readable).
    pub fn read_cached(
        &self,
        p: u32,
        from: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Vec<Arc<Message>> {
        match self.visible_end(p) {
            Some(end) if from < end => {
                self.ch.logs[p as usize].cached_range(from, end, max_records, max_bytes)
            }
            _ => Vec::new(),
        }
    }

    /// Up to `max_records` / about `max_bytes` (payload) messages of `p` from `from`, never at
    /// or beyond [`visible_end`](Self::visible_end). The first message may be above `from`
    /// when retention removed `[from, ..)` meanwhile (callers report that as a gap).
    /// Served from the recent-entry cache when possible, else positional reads (blocking).
    pub fn read(
        &self,
        p: u32,
        from: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> io::Result<Vec<Arc<Message>>> {
        let Some(end) = self.visible_end(p) else {
            return Ok(Vec::new());
        };
        if from >= end || max_records == 0 {
            return Ok(Vec::new());
        }
        let log = &self.ch.logs[p as usize];
        let mut out = log.cached_range(from, end, max_records, max_bytes);
        let mut bytes: usize = out.iter().map(|m| m.payload.len()).sum();
        let next = out.last().map_or(from, |m| m.offset + 1);
        if out.len() >= max_records || bytes >= max_bytes || next >= end {
            return Ok(out);
        }
        log.scan_until(next, end, |r| {
            bytes += r.payload.len();
            out.push(Arc::new(r.into_message(p)));
            if out.len() >= max_records || bytes >= max_bytes {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?;
        Ok(out)
    }

    /// First offset of `p` whose timestamp is `>= ts` (capped at the visible end). Blocking.
    pub fn offset_for_time(&self, p: u32, ts: u64) -> io::Result<u64> {
        let log = &self.ch.logs[p as usize];
        let end = self.visible_end(p).unwrap_or_else(|| log.high());
        Ok(log.offset_for_time(ts)?.min(end))
    }
}

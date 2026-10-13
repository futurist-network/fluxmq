//! Live tail: a rate-limited, sampled copy of newly committed messages per channel.
//!
//! Zero cost without receivers: the writer checks one relaxed atomic. With receivers, a token
//! bucket (refilled continuously, burst = rate/4) spreads samples evenly over time, and each
//! sample is `try_send`-ed to every receiver; slow receivers lose samples, never slow the
//! broker.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use fluxmq_proto::model::Message;
use parking_lot::Mutex;
use tokio::sync::mpsc;

const RECEIVER_BUFFER: usize = 1024;

struct Bucket {
    tokens: f64,
    last: Instant,
}

pub struct TailHub {
    receivers: AtomicUsize,
    senders: Mutex<Vec<mpsc::Sender<Message>>>,
    bucket: Mutex<Bucket>,
    rate: f64,
}

impl TailHub {
    pub fn new(rate: u32) -> Self {
        Self {
            receivers: AtomicUsize::new(0),
            senders: Mutex::new(Vec::new()),
            bucket: Mutex::new(Bucket {
                tokens: 0.0,
                last: Instant::now(),
            }),
            rate: f64::from(rate),
        }
    }

    pub fn subscribe(&self) -> mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel(RECEIVER_BUFFER);
        let mut senders = self.senders.lock();
        senders.push(tx);
        self.receivers.store(senders.len(), Ordering::Relaxed);
        rx
    }

    /// Called when a receiver is dropped.
    pub fn prune(&self) {
        let mut senders = self.senders.lock();
        senders.retain(|s| !s.is_closed());
        self.receivers.store(senders.len(), Ordering::Relaxed);
    }

    /// Ends all receivers (channel deleted).
    pub fn close(&self) {
        self.senders.lock().clear();
        self.receivers.store(0, Ordering::Relaxed);
    }

    /// Offers newly committed messages (writer thread).
    /// Whether anyone receives the live tail (lets the writer skip materializing messages).
    pub fn is_active(&self) -> bool {
        self.receivers.load(Ordering::Relaxed) > 0 && self.rate > 0.0
    }

    pub fn offer(&self, msgs: &[Arc<Message>]) {
        if self.receivers.load(Ordering::Relaxed) == 0 || msgs.is_empty() || self.rate <= 0.0 {
            return;
        }
        let mut take = {
            let mut b = self.bucket.lock();
            let now = Instant::now();
            let burst = (self.rate / 4.0).max(1.0);
            b.tokens = (b.tokens + now.duration_since(b.last).as_secs_f64() * self.rate).min(burst);
            b.last = now;
            let n = (b.tokens.floor() as usize).min(msgs.len());
            b.tokens -= n as f64;
            n
        };
        if take == 0 {
            return;
        }
        let senders = self.senders.lock();
        // Spread the samples across the batch.
        let step = msgs.len() / take;
        let mut i = 0;
        while take > 0 && i < msgs.len() {
            for s in senders.iter() {
                let _ = s.try_send((*msgs[i]).clone());
            }
            take -= 1;
            i += step.max(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn m(o: u64) -> Arc<Message> {
        Arc::new(Message {
            partition: 0,
            offset: o,
            key: None,
            headers: Default::default(),
            timestamp_ms: 0,
            payload: Bytes::new(),
        })
    }

    #[test]
    fn rate_limited_and_zero_cost_without_receivers() {
        let hub = TailHub::new(40);
        hub.offer(&[m(0)]);
        let mut rx = hub.subscribe();
        std::thread::sleep(std::time::Duration::from_millis(300));
        let batch: Vec<_> = (0..1000).map(m).collect();
        hub.offer(&batch);
        let mut got = 0;
        while rx.try_recv().is_ok() {
            got += 1;
        }
        // Burst is rate/4 = 10.
        assert!((1..=10).contains(&got), "got {got}");
        drop(rx);
        hub.prune();
        assert_eq!(hub.receivers.load(Ordering::Relaxed), 0);
    }
}

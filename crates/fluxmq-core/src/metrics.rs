//! Hot-path metrics: relaxed atomics for counters, mutex-guarded HDR histograms touched only
//! by paths that are already serialized (the channel writer, a group's dispatch lock).

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::stats::{LatencyHistogram, new_latency_histogram};

/// Highest value the canonical histogram accepts (µs).
const MAX_US: u64 = 60_000_000;

/// Records `v` µs, clamped into the canonical range (never fails).
pub fn record(h: &mut LatencyHistogram, v_us: u64) {
    let _ = h.record(v_us.clamp(1, MAX_US));
}

pub fn record_n(h: &mut LatencyHistogram, v_us: u64, n: u64) {
    let _ = h.record_n(v_us.clamp(1, MAX_US), n);
}

#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    #[inline]
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }
    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Per-channel cumulative counters and histograms.
pub struct ChannelMetrics {
    pub published: Counter,
    pub published_bytes: Counter,
    pub delivered: Counter,
    pub delivered_bytes: Counter,
    pub acked: Counter,
    pub nacked: Counter,
    pub timed_out: Counter,
    pub retries: Counter,
    pub dead_lettered: Counter,
    /// Written by the channel writer thread only.
    pub publish_latency: Mutex<LatencyHistogram>,
    /// Written under a group lock; shared by the channel's groups.
    pub delivery_latency: Mutex<LatencyHistogram>,
}

impl Default for ChannelMetrics {
    fn default() -> Self {
        Self {
            published: Counter::default(),
            published_bytes: Counter::default(),
            delivered: Counter::default(),
            delivered_bytes: Counter::default(),
            acked: Counter::default(),
            nacked: Counter::default(),
            timed_out: Counter::default(),
            retries: Counter::default(),
            dead_lettered: Counter::default(),
            publish_latency: Mutex::new(new_latency_histogram()),
            delivery_latency: Mutex::new(new_latency_histogram()),
        }
    }
}

/// Per-group cumulative counters (histograms live inside the group state).
#[derive(Debug, Default)]
pub struct GroupCounters {
    pub delivered: Counter,
    pub acked: Counter,
    pub nacked: Counter,
    pub dead_lettered: Counter,
    /// Bytes read from disk by catch-up read-ahead (a group behind the recent cache).
    pub catchup_read_bytes: Counter,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps() {
        let mut h = new_latency_histogram();
        record(&mut h, 0);
        record(&mut h, u64::MAX);
        record_n(&mut h, 500, 3);
        assert_eq!(h.len(), 5);
        assert!(h.max() >= MAX_US - MAX_US / 1000);
    }
}

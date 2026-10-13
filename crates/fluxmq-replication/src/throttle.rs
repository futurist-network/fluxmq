//! Catch-up throttle (v0.4 rebalancing): limits the fetch bytes a partition leader serves to
//! followers that are **not in the ISR** (new replicas of a move catching up, or a replica
//! recovering after an outage), so a move does not starve producers / consumers of disk and
//! network bandwidth. In-sync followers are never throttled.
//!
//! Enforced on the leader, per `(channel, partition, follower)`, with a token bucket of one
//! second's worth of bytes. A fetch is served once the bucket is positive; it reads at most
//! the bucket's balance (but always at least one record), and the bytes actually sent are
//! charged afterwards — the balance may go negative, and the next fetch waits for it to
//! refill. The long-run rate is therefore the configured one even when a single record is
//! larger than a second's budget. A fetch never waits longer than [`MAX_THROTTLE_WAIT`]; it
//! then returns no records (the follower simply fetches again).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Longest a throttled fetch is held before it is answered without records.
pub(crate) const MAX_THROTTLE_WAIT: Duration = Duration::from_millis(500);

#[derive(Debug)]
struct Bucket {
    rate: u64,
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn refill(&mut self, now: Instant) {
        let cap = self.rate as f64;
        let dt = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + dt * cap).min(cap);
        self.at = now;
    }
}

/// Throttle configuration and buckets, shared by the manager and the fetch server.
#[derive(Debug, Default)]
pub(crate) struct Throttles {
    /// `(channel, partition)` → bytes per second (absent = unlimited).
    rates: Mutex<HashMap<(String, u32), u64>>,
    buckets: Mutex<HashMap<(String, u32, String), Bucket>>,
}

/// What a throttled fetch may do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Grant {
    /// Not throttled.
    Unlimited,
    /// Read at most this many bytes (>= 1), then [`Throttles::charge`] what was sent.
    Bytes(usize),
    /// Out of budget: retry after this long.
    Wait(Duration),
}

impl Throttles {
    pub(crate) fn set(&self, channel: &str, partition: u32, bytes_per_sec: u64) {
        let key = (channel.to_string(), partition);
        if bytes_per_sec == 0 {
            self.rates.lock().remove(&key);
        } else {
            self.rates.lock().insert(key, bytes_per_sec);
        }
        // Existing buckets adopt the new rate (balance capped to the new burst).
        let mut buckets = self.buckets.lock();
        buckets.retain(|(c, p, _), b| {
            if c != channel || *p != partition {
                return true;
            }
            if bytes_per_sec == 0 {
                return false;
            }
            b.rate = bytes_per_sec;
            b.tokens = b.tokens.min(bytes_per_sec as f64);
            true
        });
    }

    pub(crate) fn get(&self, channel: &str, partition: u32) -> u64 {
        self.rates
            .lock()
            .get(&(channel.to_string(), partition))
            .copied()
            .unwrap_or(0)
    }

    /// Budget for one fetch of an out-of-ISR follower.
    pub(crate) fn grant(
        &self,
        channel: &str,
        partition: u32,
        follower: &str,
        max_bytes: usize,
        now: Instant,
    ) -> Grant {
        let rate = self.get(channel, partition);
        if rate == 0 {
            return Grant::Unlimited;
        }
        let mut buckets = self.buckets.lock();
        let b = buckets
            .entry((channel.to_string(), partition, follower.to_string()))
            .or_insert(Bucket {
                rate,
                tokens: rate as f64,
                at: now,
            });
        b.rate = rate;
        b.refill(now);
        if b.tokens >= 1.0 {
            Grant::Bytes((b.tokens as usize).clamp(1, max_bytes.max(1)))
        } else {
            let deficit = 1.0 - b.tokens;
            Grant::Wait(
                Duration::from_secs_f64(deficit / rate as f64).max(Duration::from_millis(1)),
            )
        }
    }

    /// Charges `bytes` actually sent to a throttled follower.
    pub(crate) fn charge(&self, channel: &str, partition: u32, follower: &str, bytes: usize) {
        let mut buckets = self.buckets.lock();
        if let Some(b) = buckets.get_mut(&(channel.to_string(), partition, follower.to_string())) {
            b.tokens -= bytes as f64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_without_rate() {
        let t = Throttles::default();
        assert_eq!(t.grant("c", 0, "n2", 100, Instant::now()), Grant::Unlimited);
        t.set("c", 0, 1000);
        assert_eq!(t.get("c", 0), 1000);
        assert_eq!(t.grant("c", 1, "n2", 100, Instant::now()), Grant::Unlimited);
        t.set("c", 0, 0);
        assert_eq!(t.grant("c", 0, "n2", 100, Instant::now()), Grant::Unlimited);
    }

    #[test]
    fn bucket_limits_long_run_rate() {
        let t = Throttles::default();
        t.set("c", 0, 1000);
        let start = Instant::now();
        // Initial burst: one second's worth, capped by max_bytes.
        assert_eq!(t.grant("c", 0, "n2", 4096, start), Grant::Bytes(1000));
        assert_eq!(t.grant("c", 0, "n2", 300, start), Grant::Bytes(300));
        // A record larger than the budget is still served once; the debt is waited out.
        t.charge("c", 0, "n2", 3000);
        match t.grant("c", 0, "n2", 4096, start) {
            Grant::Wait(d) => assert!(d >= Duration::from_millis(1900), "{d:?}"),
            g => panic!("{g:?}"),
        }
        // Two seconds later the debt is repaid; the balance never exceeds the burst.
        let later = start + Duration::from_millis(2_100);
        assert!(matches!(t.grant("c", 0, "n2", 4096, later), Grant::Bytes(b) if b <= 100));
        let much_later = start + Duration::from_secs(60);
        assert_eq!(t.grant("c", 0, "n2", 4096, much_later), Grant::Bytes(1000));
        // Buckets are per follower.
        assert_eq!(t.grant("c", 0, "n3", 4096, start), Grant::Bytes(1000));
    }

    #[test]
    fn simulated_rate_matches_configuration() {
        let t = Throttles::default();
        let rate = 50_000u64;
        t.set("c", 0, rate);
        let start = Instant::now();
        let mut now = start;
        let mut sent = 0u64;
        let end = start + Duration::from_secs(10);
        while now < end {
            match t.grant("c", 0, "n2", 64 * 1024, now) {
                Grant::Bytes(b) => {
                    // Records of 7000 bytes: chunks never split a record.
                    let n = (b as u64).div_ceil(7000) * 7000;
                    t.charge("c", 0, "n2", n as usize);
                    sent += n;
                    now += Duration::from_millis(1);
                }
                Grant::Wait(d) => now += d,
                Grant::Unlimited => unreachable!(),
            }
        }
        // 10 s at 50 kB/s plus one burst (+ one record of rounding).
        assert!(sent <= rate * 11 + 7000, "sent {sent}");
        assert!(sent >= rate * 9, "sent {sent}");
    }
}

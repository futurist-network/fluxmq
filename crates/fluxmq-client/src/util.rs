//! Small helpers: a fast non-cryptographic RNG and backoff computation.

use std::cell::Cell;
use std::time::Duration;

/// xorshift64* seeded per thread; good enough for trace ids and jitter.
pub(crate) fn fast_rand() -> u64 {
    thread_local! {
        static STATE: Cell<u64> = Cell::new({
            use std::hash::{BuildHasher, Hasher};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u64(fluxmq_proto::now_ms());
            h.finish() | 1
        });
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        s.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    })
}

/// Uniform in `[0, 1)`.
pub(crate) fn rand_f64() -> f64 {
    (fast_rand() >> 11) as f64 / (1u64 << 53) as f64
}

/// Exponential backoff: `initial * 2^attempt`, capped at `max`, then scaled by a random factor
/// in `[1 - jitter, 1 + jitter]` (jitter is clamped to `0..=1`).
pub(crate) fn backoff(initial: Duration, max: Duration, attempt: u32, jitter: f64) -> Duration {
    let base = initial
        .checked_mul(1u32 << attempt.min(20))
        .unwrap_or(max)
        .min(max);
    let j = jitter.clamp(0.0, 1.0);
    if j == 0.0 {
        return base;
    }
    let factor = 1.0 - j + 2.0 * j * rand_f64();
    base.mul_f64(factor.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let i = Duration::from_millis(100);
        let m = Duration::from_secs(2);
        assert_eq!(backoff(i, m, 0, 0.0), i);
        assert_eq!(backoff(i, m, 3, 0.0), Duration::from_millis(800));
        assert_eq!(backoff(i, m, 10, 0.0), m);
        assert_eq!(backoff(i, m, 40, 0.0), m);
        for _ in 0..100 {
            let d = backoff(i, m, 1, 0.5);
            assert!(d >= Duration::from_millis(100) && d <= Duration::from_millis(300));
        }
    }
}

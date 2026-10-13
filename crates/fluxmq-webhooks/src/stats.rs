//! Per-subscription delivery counters and a lock-free, mergeable latency histogram (so stats
//! can be summed across cluster nodes exactly), plus the recent-attempts log, the per-bucket
//! request history and the circuit breaker state (all node-local memory, mergeable).

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use fluxmq_proto::webhooks::{
    WebhookAttempt, WebhookBreaker, WebhookHistory, WebhookHistoryPoint, WebhookStats,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// Log-linear buckets over microseconds: values < 16 exact, then 8 sub-buckets per power of
/// two (≤ 12.5% relative error) up to 2^36 µs (~19 h).
const LINEAR: usize = 16;
const SUB: usize = 8;
const MAX_EXP: u32 = 36;
pub const BUCKETS: usize = LINEAR + (MAX_EXP as usize - 3) * SUB;

fn bucket_of(us: u64) -> usize {
    if us < LINEAR as u64 {
        return us as usize;
    }
    let exp = 63 - us.leading_zeros(); // >= 4
    if exp > MAX_EXP {
        return BUCKETS - 1;
    }
    let sub = ((us >> (exp - 3)) & 7) as usize;
    (LINEAR + (exp as usize - 4) * SUB + sub).min(BUCKETS - 1)
}

/// Midpoint of a bucket, in µs.
fn bucket_value(i: usize) -> f64 {
    if i < LINEAR {
        return i as f64;
    }
    let exp = ((i - LINEAR) / SUB) as u32 + 4;
    let sub = ((i - LINEAR) % SUB) as u64;
    let width = 1u64 << (exp - 3);
    let low = (1u64 << exp) + sub * width;
    low as f64 + width as f64 / 2.0
}

pub struct Histogram {
    buckets: Vec<AtomicU64>,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: (0..BUCKETS).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

impl Histogram {
    pub fn record_us(&self, us: u64) {
        self.buckets[bucket_of(us)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Vec<u64> {
        self.buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect()
    }
}

/// Quantile (0..1) of a bucket snapshot, in milliseconds (0 when empty).
pub fn quantile_ms(buckets: &[u64], q: f64) -> f64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return 0.0;
    }
    let rank = ((q * total as f64).ceil() as u64).clamp(1, total);
    let mut seen = 0u64;
    for (i, n) in buckets.iter().enumerate() {
        seen += n;
        if seen >= rank {
            return bucket_value(i).round() / 1000.0;
        }
    }
    0.0
}

#[derive(Default)]
pub struct Counters {
    pub delivered: AtomicU64,
    pub failed: AtomicU64,
    pub skipped: AtomicU64,
    pub in_flight: AtomicU64,
    /// 0 = none.
    pub last_status: AtomicU64,
    /// 0 = never.
    pub last_delivery_ms: AtomicU64,
    /// (short error, detail) of the last failed delivery.
    pub last_error: Mutex<Option<(String, String)>>,
    pub latency: Histogram,
    /// Circuit breaker: open since (0 = closed), failures, next probe (wall clock ms).
    pub breaker_since_ms: AtomicU64,
    pub breaker_failures: AtomicU64,
    pub breaker_next_probe_ms: AtomicU64,
    /// Last [`ATTEMPTS_KEPT`] requests, oldest first (`node` filled in by the service).
    pub attempts: Mutex<VecDeque<WebhookAttempt>>,
    pub history: Mutex<History>,
}

/// Requests kept per subscription per node.
pub const ATTEMPTS_KEPT: usize = 100;

impl Counters {
    pub fn push_attempt(&self, a: WebhookAttempt) {
        let mut q = self.attempts.lock();
        if q.len() >= ATTEMPTS_KEPT {
            q.pop_front();
        }
        q.push_back(a);
    }

    /// Newest first.
    pub fn recent_attempts(&self) -> Vec<WebhookAttempt> {
        self.attempts.lock().iter().rev().cloned().collect()
    }
}

impl Counters {
    pub fn raw(&self, id: &str) -> NodeWebhookStats {
        let status = self.last_status.load(Ordering::Relaxed);
        let last = self.last_delivery_ms.load(Ordering::Relaxed);
        let (last_error, last_error_detail) = self.last_error.lock().clone().unzip();
        NodeWebhookStats {
            id: id.to_string(),
            delivered: self.delivered.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            skipped_by_filter: self.skipped.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            last_status: (status > 0).then_some(status as u16),
            last_error,
            last_error_detail,
            last_delivery_ms: (last > 0).then_some(last),
            latency_buckets: self.latency.snapshot(),
            breaker: match self.breaker_since_ms.load(Ordering::Relaxed) {
                0 => None,
                since => Some(WebhookBreaker {
                    since_ms: since,
                    failures: self.breaker_failures.load(Ordering::Relaxed) as u32,
                    next_probe_ms: self.breaker_next_probe_ms.load(Ordering::Relaxed),
                    nodes: 1,
                }),
            },
        }
    }
}

/// Request history ranges: (`range` query value, bucket width, buckets).
pub const HISTORY_RANGES: [(&str, u64, usize); 3] = [
    ("15m", 10_000, 90),
    ("1h", 10_000, 360),
    ("24h", 300_000, 288),
];

/// One bucket of requests (mergeable: sums and max).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Slot {
    pub start: u64,
    pub delivered: u64,
    pub failed: u64,
    pub lat_sum_us: u64,
    pub lat_n: u64,
    pub lat_max_us: u64,
}

impl Slot {
    fn add(&mut self, o: &Slot) {
        self.delivered += o.delivered;
        self.failed += o.failed;
        self.lat_sum_us += o.lat_sum_us;
        self.lat_n += o.lat_n;
        self.lat_max_us = self.lat_max_us.max(o.lat_max_us);
    }
}

/// Ring of buckets per resolution (10 s for an hour, 5 min for a day).
pub struct History {
    tiers: Vec<(u64, Vec<Slot>)>,
}

impl Default for History {
    fn default() -> Self {
        Self {
            tiers: vec![
                (10_000, vec![Slot::default(); 360]),
                (300_000, vec![Slot::default(); 288]),
            ],
        }
    }
}

impl History {
    /// Records one request: `delivered` / `failed` messages, response latency if any.
    pub fn record(&mut self, now_ms: u64, delivered: u64, failed: u64, latency_us: Option<u64>) {
        for (res, slots) in &mut self.tiers {
            let start = now_ms / *res * *res;
            let i = ((now_ms / *res) % slots.len() as u64) as usize;
            let s = &mut slots[i];
            if s.start != start {
                *s = Slot {
                    start,
                    ..Slot::default()
                };
            }
            s.delivered += delivered;
            s.failed += failed;
            if let Some(us) = latency_us {
                s.lat_sum_us += us;
                s.lat_n += 1;
                s.lat_max_us = s.lat_max_us.max(us);
            }
        }
    }

    /// Non-empty buckets of `range` (see [`HISTORY_RANGES`]); `None` for an unknown range.
    pub fn slots(&self, range: &str, now_ms: u64) -> Option<(u64, Vec<Slot>)> {
        let &(_, res, n) = HISTORY_RANGES.iter().find(|(r, _, _)| *r == range)?;
        let (_, slots) = self.tiers.iter().find(|(r, _)| *r == res)?;
        let since = (now_ms / res).saturating_sub(n as u64 - 1) * res;
        let mut v: Vec<Slot> = slots
            .iter()
            .filter(|s| {
                s.start >= since && s.start <= now_ms && (s.delivered + s.failed + s.lat_n) > 0
            })
            .copied()
            .collect();
        v.sort_by_key(|s| s.start);
        Some((res, v))
    }
}

/// One node's history of one subscription (`GET /internal/v1/webhooks/{id}/history`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeHistory {
    pub bucket_ms: u64,
    pub slots: Vec<Slot>,
}

/// Sums node histories into a continuous series (empty buckets are zeros) over `range`.
pub fn merge_history(range: &str, now_ms: u64, nodes: Vec<NodeHistory>) -> Option<WebhookHistory> {
    let &(_, res, n) = HISTORY_RANGES.iter().find(|(r, _, _)| *r == range)?;
    let mut by: BTreeMap<u64, Slot> = BTreeMap::new();
    for node in nodes.into_iter().filter(|h| h.bucket_ms == res) {
        for s in node.slots {
            by.entry(s.start).or_default().add(&s);
        }
    }
    let first = (now_ms / res).saturating_sub(n as u64 - 1) * res;
    let points = (0..n as u64)
        .map(|i| {
            let ts = first + i * res;
            let s = by.get(&ts).copied().unwrap_or_default();
            WebhookHistoryPoint {
                ts_ms: ts,
                delivered: s.delivered,
                failed: s.failed,
                latency_avg_ms: if s.lat_n > 0 {
                    (s.lat_sum_us as f64 / s.lat_n as f64 / 10.0).round() / 100.0
                } else {
                    0.0
                },
                latency_max_ms: (s.lat_max_us as f64 / 10.0).round() / 100.0,
            }
        })
        .collect();
    Some(WebhookHistory {
        bucket_ms: res,
        points,
    })
}

/// Newest first, at most [`ATTEMPTS_KEPT`] over all nodes.
pub fn merge_attempts(nodes: Vec<Vec<WebhookAttempt>>) -> Vec<WebhookAttempt> {
    let mut all: Vec<WebhookAttempt> = nodes.into_iter().flatten().collect();
    all.sort_by_key(|a| std::cmp::Reverse(a.at_ms));
    all.truncate(ATTEMPTS_KEPT);
    all
}

/// One node's stats of one subscription (`GET /internal/v1/webhooks/stats`): mergeable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeWebhookStats {
    pub id: String,
    pub delivered: u64,
    pub failed: u64,
    pub skipped_by_filter: u64,
    pub in_flight: u64,
    pub last_status: Option<u16>,
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_error_detail: Option<String>,
    pub last_delivery_ms: Option<u64>,
    pub latency_buckets: Vec<u64>,
    /// Additive (v0.11).
    #[serde(default)]
    pub breaker: Option<WebhookBreaker>,
}

/// Sums node stats per id (counters and histograms added; `last_*` from the node with the
/// most recent delivery). `ids` fixes the output order and set (unknown ids are dropped,
/// missing ones are zeros).
pub fn merge(ids: &[String], nodes: Vec<NodeWebhookStats>) -> Vec<WebhookStats> {
    ids.iter()
        .map(|id| {
            let mut acc = NodeWebhookStats {
                id: id.clone(),
                delivered: 0,
                failed: 0,
                skipped_by_filter: 0,
                in_flight: 0,
                last_status: None,
                last_error: None,
                last_error_detail: None,
                last_delivery_ms: None,
                latency_buckets: vec![0; BUCKETS],
                breaker: None,
            };
            for n in nodes.iter().filter(|n| &n.id == id) {
                acc.delivered += n.delivered;
                acc.failed += n.failed;
                acc.skipped_by_filter += n.skipped_by_filter;
                acc.in_flight += n.in_flight;
                if n.last_delivery_ms >= acc.last_delivery_ms {
                    acc.last_delivery_ms = n.last_delivery_ms;
                    acc.last_status = n.last_status.or(acc.last_status);
                    if n.last_error.is_some() {
                        acc.last_error = n.last_error.clone();
                        acc.last_error_detail = n.last_error_detail.clone();
                    }
                }
                for (a, b) in acc.latency_buckets.iter_mut().zip(&n.latency_buckets) {
                    *a += b;
                }
                if let Some(b) = &n.breaker {
                    acc.breaker = Some(match acc.breaker.take() {
                        None => b.clone(),
                        Some(a) => WebhookBreaker {
                            since_ms: a.since_ms.min(b.since_ms),
                            failures: a.failures.max(b.failures),
                            next_probe_ms: a.next_probe_ms.min(b.next_probe_ms),
                            nodes: a.nodes + b.nodes,
                        },
                    });
                }
            }
            WebhookStats {
                id: acc.id,
                delivered: acc.delivered,
                failed: acc.failed,
                skipped_by_filter: acc.skipped_by_filter,
                in_flight: acc.in_flight,
                last_status: acc.last_status,
                last_error: acc.last_error,
                last_error_detail: acc.last_error_detail,
                last_delivery_ms: acc.last_delivery_ms,
                latency_p50_ms: quantile_ms(&acc.latency_buckets, 0.5),
                latency_p99_ms: quantile_ms(&acc.latency_buckets, 0.99),
                breaker: acc.breaker,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_accurate() {
        let mut prev = 0;
        for us in [0u64, 1, 15, 16, 17, 100, 1_000, 12_345, 1 << 30, u64::MAX] {
            let b = bucket_of(us);
            assert!(b >= prev, "{us}");
            prev = b;
            if (16..1 << 36).contains(&us) {
                let v = bucket_value(b);
                assert!((v - us as f64).abs() / us as f64 <= 0.125, "{us} -> {v}");
            }
        }
    }

    #[test]
    fn quantiles_and_merge() {
        let h = Histogram::default();
        for _ in 0..98 {
            h.record_us(1_000);
        }
        h.record_us(100_000);
        h.record_us(100_000);
        let snap = h.snapshot();
        let p50 = quantile_ms(&snap, 0.5);
        let p99 = quantile_ms(&snap, 0.99);
        assert!((0.9..1.1).contains(&p50), "{p50}");
        assert!((90.0..110.0).contains(&p99), "{p99}");

        let c = Counters::default();
        c.delivered.store(3, Ordering::Relaxed);
        let mut a = c.raw("x");
        a.last_delivery_ms = Some(10);
        a.last_status = Some(500);
        let mut b = a.clone();
        b.last_delivery_ms = Some(20);
        b.last_status = Some(200);
        let m = merge(&["x".into(), "y".into()], vec![a, b]);
        assert_eq!(m[0].delivered, 6);
        assert_eq!(m[0].last_status, Some(200));
        assert_eq!(m[1].delivered, 0);
        assert!(m[0].breaker.is_none());

        // Breakers merge: open if open anywhere, earliest times, node count.
        let open = |since, next| WebhookBreaker {
            since_ms: since,
            failures: 5,
            next_probe_ms: next,
            nodes: 1,
        };
        let mut a = c.raw("x");
        a.breaker = Some(open(100, 900));
        let mut b = c.raw("x");
        b.breaker = Some(open(50, 1_000));
        let m = merge(&["x".into()], vec![a, b, c.raw("x")]);
        assert_eq!(
            m[0].breaker,
            Some(WebhookBreaker {
                since_ms: 50,
                failures: 5,
                next_probe_ms: 900,
                nodes: 2
            })
        );
    }

    #[test]
    fn history_buckets_merge_into_a_continuous_series() {
        let mut h = History::default();
        let t0 = 1_000_000_000;
        h.record(t0, 10, 0, Some(2_000));
        h.record(t0 + 1_000, 5, 1, Some(4_000));
        h.record(t0 + 20_000, 0, 3, None);
        let (res, slots) = h.slots("1h", t0 + 25_000).unwrap();
        assert_eq!(res, 10_000);
        assert_eq!(slots.len(), 2);
        assert_eq!(
            (slots[0].delivered, slots[0].failed, slots[0].lat_n),
            (15, 1, 2)
        );
        assert!(h.slots("2h", t0).is_none());

        // A second node's bucket adds up; gaps are zeros; the series ends at `now`.
        let other = NodeHistory {
            bucket_ms: 10_000,
            slots: vec![Slot {
                start: t0,
                delivered: 1,
                ..Slot::default()
            }],
        };
        let m = merge_history(
            "1h",
            t0 + 25_000,
            vec![
                NodeHistory {
                    bucket_ms: res,
                    slots,
                },
                other,
            ],
        )
        .unwrap();
        assert_eq!(m.points.len(), 360);
        let last = m.points.last().unwrap();
        assert_eq!(last.ts_ms, (t0 + 25_000) / 10_000 * 10_000);
        let p = m.points.iter().find(|p| p.ts_ms == t0).unwrap();
        assert_eq!((p.delivered, p.failed), (16, 1));
        assert_eq!((p.latency_avg_ms, p.latency_max_ms), (3.0, 4.0));
        assert_eq!(m.points.iter().map(|p| p.failed).sum::<u64>(), 4);

        // The ring overwrites a bucket from a previous lap.
        h.record(t0 + 3_600_000, 7, 0, None);
        let (_, slots) = h.slots("1h", t0 + 3_600_000).unwrap();
        assert_eq!(slots.iter().map(|s| s.delivered).sum::<u64>(), 7);
        let (res, day) = h.slots("24h", t0 + 3_600_000).unwrap();
        assert_eq!(res, 300_000);
        assert_eq!(day.iter().map(|s| s.delivered).sum::<u64>(), 22);
    }

    #[test]
    fn attempts_ring_keeps_the_newest() {
        let c = Counters::default();
        for i in 0..(ATTEMPTS_KEPT as u64 + 5) {
            c.push_attempt(WebhookAttempt {
                at_ms: i,
                node: String::new(),
                delivery_id: format!("c/0/{i}"),
                messages: 1,
                attempt: 1,
                outcome: fluxmq_proto::webhooks::AttemptOutcome::Delivered,
                status: Some(200),
                latency_ms: 1.0,
                error: None,
                error_detail: None,
            });
        }
        let r = c.recent_attempts();
        assert_eq!(r.len(), ATTEMPTS_KEPT);
        assert_eq!(r[0].at_ms, ATTEMPTS_KEPT as u64 + 4);
        let m = merge_attempts(vec![r.clone(), r]);
        assert_eq!(m.len(), ATTEMPTS_KEPT);
        assert!(m.windows(2).all(|w| w[0].at_ms >= w[1].at_ms));
    }
}

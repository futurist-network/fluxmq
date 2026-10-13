//! In-memory metric history with rollups, optionally persisted.
//!
//! Every series has four tiers, fed once per second by the collector:
//!
//! | tier | resolution | span  | points |
//! |------|-----------|-------|--------|
//! | 0    | 1 s       | 15 min| 900    |
//! | 1    | 5 s       | 1 h   | 720    |
//! | 2    | 1 min     | 24 h  | 1440   |
//! | 3    | 30 min    | 7 d   | 336    |
//!
//! Rollups cascade: when a tier closes a bucket, that bucket's value is fed into the next
//! tier. Aggregation: latency percentiles (`*_p50`, `*_p95`, `*_p99`) use **max** (a spike
//! inside a coarse bucket stays visible — conservative for SLOs); everything else (rates,
//! gauges such as lag/consumers/storage) uses the **average** of the child buckets.
//!
//! Queries return the closed buckets of the matching tier plus the currently open (partial)
//! bucket, so coarse ranges include the most recent data.
//!
//! Persistence (when `history_dir` is set): the 1 min and 30 min tiers are written to
//! `history.json` atomically (temp file + rename). On load, the 30 min tier's open bucket is
//! rebuilt from 1 min points newer than its last closed bucket.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};

use fluxmq_proto::telemetry::{HistoryRange, HistorySeries, metrics};
use serde::{Deserialize, Serialize};

/// Per-channel metrics recorded as `channel.<name>.<metric>`.
pub const CHANNEL_METRICS: &[&str] = &[
    metrics::INGRESS_RATE,
    metrics::EGRESS_RATE,
    metrics::ACK_RATE,
    metrics::PUBLISH_P99,
    metrics::LAG,
    metrics::DLQ_RATE,
];

const TIERS: [(u64, usize); 4] = [
    (1_000, 900),
    (5_000, 720),
    (60_000, 1_440),
    (1_800_000, 336),
];
const SEVEN_DAYS_MS: u64 = 7 * 86_400_000;
pub const FILE_NAME: &str = "history.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    Avg,
    Max,
}

pub fn agg_for(metric: &str) -> Agg {
    if metric.ends_with("_p50") || metric.ends_with("_p95") || metric.ends_with("_p99") {
        Agg::Max
    } else {
        Agg::Avg
    }
}

/// Valid metric ids: everything in `metrics::ALL`, plus `channel.<name>.<m>` for
/// [`CHANNEL_METRICS`].
pub fn is_valid_metric(metric: &str) -> bool {
    if metrics::ALL.contains(&metric) {
        return true;
    }
    match metric
        .strip_prefix("channel.")
        .and_then(|rest| rest.rsplit_once('.'))
    {
        Some((name, m)) => !name.is_empty() && CHANNEL_METRICS.contains(&m),
        None => false,
    }
}

pub fn channel_metric(channel: &str, metric: &str) -> String {
    format!("channel.{channel}.{metric}")
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    start: u64,
    sum: f64,
    count: u32,
    max: f64,
}

impl Bucket {
    fn value(&self, agg: Agg) -> f64 {
        match agg {
            Agg::Avg => self.sum / self.count.max(1) as f64,
            Agg::Max => self.max,
        }
    }
}

#[derive(Debug, Clone)]
struct Tier {
    res_ms: u64,
    cap: usize,
    points: VecDeque<(u64, f64)>,
    open: Option<Bucket>,
}

impl Tier {
    fn new(res_ms: u64, cap: usize) -> Self {
        Self {
            res_ms,
            cap,
            points: VecDeque::new(),
            open: None,
        }
    }

    /// Adds a value; returns the bucket that closed as a result, if any.
    fn add(&mut self, ts: u64, v: f64, agg: Agg) -> Option<(u64, f64)> {
        if !v.is_finite() {
            return None;
        }
        let start = ts - ts % self.res_ms;
        let mut closed = None;
        match &mut self.open {
            Some(b) if b.start == start => {
                b.sum += v;
                b.count += 1;
                b.max = b.max.max(v);
                return None;
            }
            // Out of order (older than the open bucket): ignore.
            Some(b) if start < b.start => return None,
            Some(b) => {
                let point = (b.start, b.value(agg));
                self.points.push_back(point);
                while self.points.len() > self.cap {
                    self.points.pop_front();
                }
                closed = Some(point);
            }
            None => {}
        }
        self.open = Some(Bucket {
            start,
            sum: v,
            count: 1,
            max: v,
        });
        closed
    }

    fn query(&self, since: u64, agg: Agg) -> Vec<(u64, f64)> {
        let mut out: Vec<(u64, f64)> = self
            .points
            .iter()
            .copied()
            .filter(|(t, _)| *t >= since)
            .collect();
        if let Some(b) = &self.open
            && b.start >= since
        {
            out.push((b.start, b.value(agg)));
        }
        out
    }

    fn newest_ts(&self) -> Option<u64> {
        self.open
            .map(|b| b.start)
            .or_else(|| self.points.back().map(|p| p.0))
    }
}

#[derive(Debug, Clone)]
struct Series {
    agg: Agg,
    tiers: [Tier; 4],
}

impl Series {
    fn new(agg: Agg) -> Self {
        Self {
            agg,
            tiers: TIERS.map(|(res, cap)| Tier::new(res, cap)),
        }
    }

    fn record(&mut self, ts: u64, v: f64) {
        let mut input = Some((ts, v));
        for tier in &mut self.tiers {
            let Some((t, v)) = input else { break };
            input = tier.add(t, v, self.agg);
        }
    }
}

/// All series.
#[derive(Debug, Default)]
pub struct History {
    series: HashMap<String, Series>,
}

/// On-disk format.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PersistFile {
    pub version: u32,
    pub saved_at_ms: u64,
    /// metric → (1 min points, 30 min points).
    pub series: BTreeMap<String, PersistedSeries>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PersistedSeries {
    pub m1: Vec<(u64, f64)>,
    pub m30: Vec<(u64, f64)>,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, metric: &str, ts_ms: u64, value: f64) {
        self.series
            .entry(metric.to_string())
            .or_insert_with(|| Series::new(agg_for(metric)))
            .record(ts_ms, value);
    }

    /// Points for `metric` over `range` at `range.bucket_ms()` resolution, oldest first.
    pub fn query(
        &self,
        metric: &str,
        range: HistoryRange,
        now_ms: u64,
    ) -> Result<HistorySeries, String> {
        if !is_valid_metric(metric) {
            return Err(format!("unknown metric '{metric}'"));
        }
        let bucket_ms = range.bucket_ms();
        let since = now_ms.saturating_sub(range.duration_ms());
        let points = self
            .series
            .get(metric)
            .and_then(|s| {
                s.tiers
                    .iter()
                    .find(|t| t.res_ms == bucket_ms)
                    .map(|t| t.query(since, s.agg))
            })
            .unwrap_or_default();
        Ok(HistorySeries {
            metric: metric.to_string(),
            range,
            bucket_ms,
            points,
        })
    }

    /// Drops series with no data in the last 7 days (e.g. deleted channels).
    pub fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(SEVEN_DAYS_MS);
        self.series.retain(|_, s| {
            s.tiers
                .iter()
                .filter_map(Tier::newest_ts)
                .max()
                .is_some_and(|t| t >= cutoff)
        });
    }

    /// The coarse tiers, for persistence.
    pub fn to_persist(&self, now_ms: u64) -> PersistFile {
        let series = self
            .series
            .iter()
            .map(|(k, s)| {
                let m1 = s.tiers[2].points.iter().copied().collect();
                let m30 = s.tiers[3].points.iter().copied().collect();
                (k.clone(), PersistedSeries { m1, m30 })
            })
            .filter(|(_, p)| !p.m1.is_empty() || !p.m30.is_empty())
            .collect();
        PersistFile {
            version: 1,
            saved_at_ms: now_ms,
            series,
        }
    }

    /// Restores coarse tiers saved by [`History::to_persist`], dropping expired points.
    pub fn load(&mut self, file: PersistFile, now_ms: u64) {
        for (metric, p) in file.series {
            if !is_valid_metric(&metric) {
                continue;
            }
            let mut s = Series::new(agg_for(&metric));
            let (res1, cap1) = TIERS[2];
            let (res30, cap30) = TIERS[3];
            s.tiers[2].points =
                p.m1.iter()
                    .copied()
                    .filter(|(t, v)| *t + res1 * cap1 as u64 > now_ms && v.is_finite())
                    .collect();
            s.tiers[3].points = p
                .m30
                .iter()
                .copied()
                .filter(|(t, v)| *t + res30 * cap30 as u64 > now_ms && v.is_finite())
                .collect();
            // Rebuild the 30 min tier's open bucket from newer 1 min points.
            let next_m30 = s.tiers[3].points.back().map_or(0, |(t, _)| t + res30);
            let agg = s.agg;
            let tail: Vec<(u64, f64)> = s.tiers[2]
                .points
                .iter()
                .copied()
                .filter(|(t, _)| *t >= next_m30)
                .collect();
            for (t, v) in tail {
                // Closed buckets are pushed into the tier's points by `add` itself.
                let _ = s.tiers[3].add(t, v, agg);
            }
            self.series.insert(metric, s);
        }
    }

    pub fn series_count(&self) -> usize {
        self.series.len()
    }
}

/// Path of the history file in `dir`.
pub fn file_path(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

/// Blocking: atomically writes `file` into `dir`.
pub fn save_blocking(dir: &Path, file: &PersistFile) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let bytes = serde_json::to_vec(file).map_err(std::io::Error::other)?;
    let tmp = dir.join(format!("{FILE_NAME}.tmp"));
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, file_path(dir))
}

/// Blocking: reads the history file from `dir` (`Ok(None)` when absent).
pub fn load_blocking(dir: &Path) -> std::io::Result<Option<PersistFile>> {
    match std::fs::read(file_path(dir)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(std::io::Error::other),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_validation() {
        assert!(is_valid_metric("ingress_rate"));
        assert!(is_valid_metric("channel.orders.events.lag"));
        assert!(!is_valid_metric("channel.orders.consumers"));
        assert!(!is_valid_metric("channel..lag"));
        assert!(!is_valid_metric("nope"));
    }

    #[test]
    fn one_second_buckets_average_samples() {
        let mut h = History::new();
        h.record("ingress_rate", 1_000, 10.0);
        h.record("ingress_rate", 1_500, 20.0);
        h.record("ingress_rate", 2_000, 30.0);
        let s = h
            .query("ingress_rate", HistoryRange::M5, 2_500)
            .expect("valid");
        assert_eq!(s.bucket_ms, 1_000);
        assert_eq!(s.points, vec![(1_000, 15.0), (2_000, 30.0)]);
    }

    #[test]
    fn rollups_average_rates_and_max_latencies() {
        let mut h = History::new();
        for i in 0..=10u64 {
            h.record("ingress_rate", i * 1_000, i as f64);
            h.record("publish_p99", i * 1_000, if i == 3 { 100.0 } else { 1.0 });
        }
        let s = h
            .query("ingress_rate", HistoryRange::H1, 10_500)
            .expect("valid");
        // 5 s buckets: [0..5) avg(0..4)=2, open [5..10) avg(5..9)=7.
        // The 1 s bucket at 10 s is still open, so the 5 s tier has not seen it yet.
        assert_eq!(s.points, vec![(0, 2.0), (5_000, 7.0)]);
        let s = h
            .query("publish_p99", HistoryRange::H1, 10_500)
            .expect("valid");
        assert_eq!(s.points, vec![(0, 100.0), (5_000, 1.0)]);
    }

    #[test]
    fn range_filters_old_points_and_unknown_metric_errors() {
        let mut h = History::new();
        for i in 0..1_200u64 {
            h.record("lag", i * 1_000, 1.0);
        }
        let s = h.query("lag", HistoryRange::M5, 1_199_000).expect("valid");
        assert_eq!(s.points.len(), 301);
        let s = h.query("lag", HistoryRange::M15, 1_199_000).expect("valid");
        assert_eq!(s.points.len(), 901); // ring capacity + open bucket
        assert!(h.query("bogus", HistoryRange::M5, 0).is_err());
        assert!(
            h.query("channel.x.lag", HistoryRange::M5, 0)
                .expect("valid")
                .points
                .is_empty()
        );
    }

    #[test]
    fn minute_and_half_hour_tiers() {
        let mut h = History::new();
        // 2 hours of 1 Hz data, value = minute index.
        for s in 0..7_200u64 {
            h.record("consumers", s * 1_000, (s / 60) as f64);
        }
        let now = 7_200_000;
        let m = h.query("consumers", HistoryRange::H24, now).expect("valid");
        assert_eq!(m.bucket_ms, 60_000);
        assert!(m.points.len() >= 118);
        assert_eq!(m.points[5], (300_000, 5.0));
        let d = h.query("consumers", HistoryRange::D7, now).expect("valid");
        assert_eq!(d.bucket_ms, 1_800_000);
        // First 30 min bucket averages minutes 0..29 → 14.5.
        assert_eq!(d.points[0], (0, 14.5));
        assert!(d.points.len() >= 4);
    }

    #[test]
    fn persist_roundtrip() {
        let mut h = History::new();
        for s in 0..4_000u64 {
            h.record("channel.orders.ingress_rate", s * 1_000, 5.0);
        }
        let now = 4_000_000;
        let file = h.to_persist(now);
        let dir = tempfile::tempdir().expect("tempdir");
        save_blocking(dir.path(), &file).expect("save");
        let loaded = load_blocking(dir.path()).expect("load").expect("present");
        let mut h2 = History::new();
        h2.load(loaded, now);
        let a = h
            .query("channel.orders.ingress_rate", HistoryRange::D7, now)
            .expect("valid");
        let b = h2
            .query("channel.orders.ingress_rate", HistoryRange::D7, now)
            .expect("valid");
        assert_eq!(a.points, b.points);
        let a = h
            .query("channel.orders.ingress_rate", HistoryRange::H24, now)
            .expect("valid");
        let b = h2
            .query("channel.orders.ingress_rate", HistoryRange::H24, now)
            .expect("valid");
        // The open 1 min bucket is not persisted.
        assert_eq!(&a.points[..a.points.len() - 1], &b.points[..]);
        assert!(
            load_blocking(&dir.path().join("missing"))
                .expect("ok")
                .is_none()
        );
    }

    #[test]
    fn prune_drops_stale_series() {
        let mut h = History::new();
        h.record("channel.gone.lag", 0, 1.0);
        h.record("lag", SEVEN_DAYS_MS + 10_000, 1.0);
        h.prune(SEVEN_DAYS_MS + 10_000);
        assert_eq!(h.series_count(), 1);
    }
}

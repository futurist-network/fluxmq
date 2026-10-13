//! Rates and windowed percentiles from cumulative [`BrokerStats`] snapshots.
//!
//! The core only exposes cumulative counters and cumulative HDR histograms. [`Window`] keeps a
//! short ring of snapshots covering `rate_window`; [`derive`] diffs the newest snapshot against
//! the oldest one in the window:
//!
//! * rate = Δcounter / Δt (per second). A counter that went *down* (entity deleted and
//!   re-created under the same name) is treated as a reset: Δ = new value.
//! * windowed histogram = newest − baseline (`Histogram::subtract`); if the subtraction fails
//!   (histogram reset) the newest cumulative histogram is used as-is.
//! * an entity (channel / group / instance / partition) missing from the baseline was created
//!   inside the window, so its baseline is zero (counters and histograms start empty).
//! * entities that disappeared are simply not reported.
//! * with a single snapshot (first tick) there is no Δt: rates and percentiles are 0.
//!
//! Latencies are converted from µs to ms (f64) at the end.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use fluxmq_core::{BrokerStats, ChannelStats, GroupStats, LatencyHistogram};
use fluxmq_proto::telemetry::LatencySummary;

/// Ring of recent snapshots. To bound memory (each snapshot holds several HDR histograms) the
/// ring keeps at most one snapshot per `rate_window / 4`; the newest snapshot is always used as
/// the "current" side, so the effective window is between `rate_window` and `1.25 × rate_window`.
#[derive(Debug)]
pub struct Window {
    window_ms: u64,
    spacing_ms: u64,
    ring: VecDeque<Arc<BrokerStats>>,
    newest: Option<Arc<BrokerStats>>,
}

impl Window {
    /// `window_ms` is the rate window (e.g. 2000).
    pub fn new(window_ms: u64) -> Self {
        let window_ms = window_ms.max(1);
        Self {
            window_ms,
            spacing_ms: (window_ms / 4).max(1),
            ring: VecDeque::new(),
            newest: None,
        }
    }

    /// Adds a snapshot. Snapshots whose clock went backwards reset the window.
    pub fn push(&mut self, stats: Arc<BrokerStats>) {
        let now = stats.taken_at_ms;
        if self.newest.as_ref().is_some_and(|n| n.taken_at_ms > now) {
            self.ring.clear();
        }
        let keep = match self.ring.back() {
            None => true,
            Some(last) => now.saturating_sub(last.taken_at_ms) >= self.spacing_ms,
        };
        if keep {
            self.ring.push_back(stats.clone());
        }
        self.newest = Some(stats);
        // Drop snapshots that are no longer needed: keep exactly one at/before the window start.
        let start = now.saturating_sub(self.window_ms);
        while self.ring.len() >= 2 && self.ring[1].taken_at_ms <= start {
            self.ring.pop_front();
        }
    }

    pub fn newest(&self) -> Option<&Arc<BrokerStats>> {
        self.newest.as_ref()
    }

    /// Oldest snapshot strictly older than the newest one (the diff baseline).
    pub fn baseline(&self) -> Option<&Arc<BrokerStats>> {
        let newest = self.newest.as_ref()?;
        self.ring
            .front()
            .filter(|b| b.taken_at_ms < newest.taken_at_ms)
    }

    #[cfg(test)]
    pub fn retained(&self) -> usize {
        self.ring.len()
    }
}

/// Everything rate-like derived from one window, computed once per tick.
#[derive(Debug, Clone, Default)]
pub struct Derived {
    pub ts_ms: u64,
    /// Window length in seconds (0 when there is no baseline yet).
    pub dt_secs: f64,
    pub ingress_rate: f64,
    pub egress_rate: f64,
    /// Acks per second across all channels and groups ("consumed").
    pub ack_rate: f64,
    pub ingress_bytes_rate: f64,
    pub egress_bytes_rate: f64,
    pub retries_rate: f64,
    pub dlq_rate: f64,
    pub publish_latency: LatencySummary,
    pub delivery_latency: LatencySummary,
    pub fsync_p99_ms: f64,
    pub channels: BTreeMap<String, ChannelDerived>,
    /// Whether per-group / per-instance values were computed (see [`derive`]).
    pub has_consumers: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ChannelDerived {
    pub ingress_rate: f64,
    pub egress_rate: f64,
    pub ingress_bytes_rate: f64,
    pub egress_bytes_rate: f64,
    pub retries_rate: f64,
    pub dlq_rate: f64,
    pub ack_rate: f64,
    pub nack_rate: f64,
    pub publish_latency: LatencySummary,
    pub delivery_latency: LatencySummary,
    pub partition_ingress: BTreeMap<u32, f64>,
    pub groups: BTreeMap<String, GroupDerived>,
}

#[derive(Debug, Clone, Default)]
pub struct GroupDerived {
    pub egress_rate: f64,
    pub ack_rate: f64,
    pub nack_rate: f64,
    pub dlq_rate: f64,
    pub processing_latency: LatencySummary,
    pub instances: BTreeMap<String, InstanceDerived>,
}

#[derive(Debug, Clone, Default)]
pub struct InstanceDerived {
    pub egress_rate: f64,
    pub ack_rate: f64,
    pub nack_rate: f64,
    pub processing_p99_ms: f64,
    /// Number of acks recorded in the windowed processing histogram.
    pub processing_samples: u64,
}

/// Δ of a cumulative counter, reset-aware.
pub fn counter_delta(new: u64, old: u64) -> u64 {
    if new >= old { new - old } else { new }
}

fn rate(new: u64, old: u64, dt: f64) -> f64 {
    if dt <= 0.0 {
        0.0
    } else {
        counter_delta(new, old) as f64 / dt
    }
}

/// `newest − baseline`, falling back to `newest` on a reset; `None` baseline = created inside
/// the window (baseline is empty).
pub fn window_histogram(
    newest: &LatencyHistogram,
    baseline: Option<&LatencyHistogram>,
) -> LatencyHistogram {
    let mut h = newest.clone();
    if let Some(b) = baseline
        && !b.is_empty()
        && h.subtract(b).is_err()
    {
        // Subtraction failed: the histogram was reset; use the newest cumulative one.
        h = newest.clone();
    }
    h
}

/// Percentile summary in milliseconds (zeros for an empty histogram).
pub fn summarize(h: &LatencyHistogram) -> LatencySummary {
    if h.is_empty() {
        return LatencySummary::default();
    }
    let ms = |us: u64| round3(us as f64 / 1000.0);
    LatencySummary {
        p50: ms(h.value_at_quantile(0.50)),
        p95: ms(h.value_at_quantile(0.95)),
        p99: ms(h.value_at_quantile(0.99)),
        p999: ms(h.value_at_quantile(0.999)),
        max: ms(h.max()),
    }
}

/// p99 in ms of a histogram (0 when empty).
pub fn p99_ms(h: &LatencyHistogram) -> f64 {
    if h.is_empty() {
        0.0
    } else {
        round3(h.value_at_quantile(0.99) as f64 / 1000.0)
    }
}

/// Rounds to 2 decimals (rates): keeps deltas small and stable.
pub fn round2(v: f64) -> f64 {
    if v.is_finite() {
        (v * 100.0).round() / 100.0
    } else {
        0.0
    }
}

/// Rounds to 3 decimals (latencies in ms → µs precision).
pub fn round3(v: f64) -> f64 {
    if v.is_finite() {
        (v * 1000.0).round() / 1000.0
    } else {
        0.0
    }
}

/// Computes [`Derived`] for the current window. `with_consumers` controls whether per-group and
/// per-instance values (processing histograms) are computed — they are the expensive part and
/// are only needed at the 500 ms / 1 s cadences.
pub fn derive(window: &Window, with_consumers: bool) -> Derived {
    let Some(newest) = window.newest() else {
        return Derived::default();
    };
    let baseline = window.baseline();
    derive_pair(newest, baseline.map(|b| b.as_ref()), with_consumers)
}

/// [`derive`] over an explicit (newest, baseline) pair.
pub fn derive_pair(
    newest: &BrokerStats,
    baseline: Option<&BrokerStats>,
    with_consumers: bool,
) -> Derived {
    let dt = baseline.map_or(0.0, |b| {
        newest.taken_at_ms.saturating_sub(b.taken_at_ms) as f64 / 1000.0
    });
    let has_window = baseline.is_some() && dt > 0.0;
    let base_channels: HashMap<&str, &ChannelStats> = baseline
        .map(|b| {
            b.channels
                .iter()
                .map(|c| (c.config.name.as_str(), c))
                .collect()
        })
        .unwrap_or_default();

    let mut out = Derived {
        ts_ms: newest.taken_at_ms,
        dt_secs: dt,
        has_consumers: with_consumers,
        ..Default::default()
    };
    let mut cluster_publish = fluxmq_core::new_latency_histogram();
    let mut cluster_delivery = fluxmq_core::new_latency_histogram();
    let (mut ing, mut egr, mut ingb, mut egrb, mut retr, mut dlq) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    let mut acks = 0.0;

    for ch in &newest.channels {
        let base = base_channels.get(ch.config.name.as_str()).copied();
        let cd = derive_channel(ch, base, dt, has_window, with_consumers);
        if has_window {
            let pub_h =
                window_histogram(&ch.publish_latency_us, base.map(|b| &b.publish_latency_us));
            let del_h = window_histogram(
                &ch.delivery_latency_us,
                base.map(|b| &b.delivery_latency_us),
            );
            // Same bounds everywhere; `add` can only fail on incompatible ranges.
            let _ = cluster_publish.add(&pub_h);
            let _ = cluster_delivery.add(&del_h);
        }
        ing += cd.ingress_rate;
        egr += cd.egress_rate;
        acks += cd.ack_rate;
        ingb += cd.ingress_bytes_rate;
        egrb += cd.egress_bytes_rate;
        retr += cd.retries_rate;
        dlq += cd.dlq_rate;
        out.channels.insert(ch.config.name.clone(), cd);
    }
    out.ingress_rate = round2(ing);
    out.egress_rate = round2(egr);
    out.ack_rate = round2(acks);
    out.ingress_bytes_rate = round2(ingb);
    out.egress_bytes_rate = round2(egrb);
    out.retries_rate = round2(retr);
    out.dlq_rate = round2(dlq);
    out.publish_latency = summarize(&cluster_publish);
    out.delivery_latency = summarize(&cluster_delivery);
    if has_window {
        let fsync = window_histogram(
            &newest.fsync_latency_us,
            baseline.map(|b| &b.fsync_latency_us),
        );
        out.fsync_p99_ms = p99_ms(&fsync);
    }
    out
}

fn derive_channel(
    ch: &ChannelStats,
    base: Option<&ChannelStats>,
    dt: f64,
    has_window: bool,
    with_consumers: bool,
) -> ChannelDerived {
    let r = |new: u64, old: Option<u64>| {
        if has_window {
            round2(rate(new, old.unwrap_or(0), dt))
        } else {
            0.0
        }
    };
    let mut cd = ChannelDerived {
        ingress_rate: r(ch.published_total, base.map(|b| b.published_total)),
        egress_rate: r(ch.delivered_total, base.map(|b| b.delivered_total)),
        ingress_bytes_rate: r(
            ch.published_bytes_total,
            base.map(|b| b.published_bytes_total),
        ),
        egress_bytes_rate: r(
            ch.delivered_bytes_total,
            base.map(|b| b.delivered_bytes_total),
        ),
        retries_rate: r(ch.retries_total, base.map(|b| b.retries_total)),
        dlq_rate: r(ch.dead_lettered_total, base.map(|b| b.dead_lettered_total)),
        ack_rate: r(ch.acked_total, base.map(|b| b.acked_total)),
        nack_rate: r(ch.nacked_total, base.map(|b| b.nacked_total)),
        ..Default::default()
    };
    if has_window {
        cd.publish_latency = summarize(&window_histogram(
            &ch.publish_latency_us,
            base.map(|b| &b.publish_latency_us),
        ));
        cd.delivery_latency = summarize(&window_histogram(
            &ch.delivery_latency_us,
            base.map(|b| &b.delivery_latency_us),
        ));
    }
    for p in &ch.partitions {
        let old = base
            .and_then(|b| b.partitions.iter().find(|bp| bp.partition == p.partition))
            .map(|bp| bp.published_total);
        cd.partition_ingress
            .insert(p.partition, r(p.published_total, old));
    }
    if with_consumers {
        for g in &ch.groups {
            let bg = base.and_then(|b| b.groups.iter().find(|x| x.name == g.name));
            cd.groups
                .insert(g.name.clone(), derive_group(g, bg, dt, has_window));
        }
    }
    cd
}

fn derive_group(
    g: &GroupStats,
    base: Option<&GroupStats>,
    dt: f64,
    has_window: bool,
) -> GroupDerived {
    let r = |new: u64, old: Option<u64>| {
        if has_window {
            round2(rate(new, old.unwrap_or(0), dt))
        } else {
            0.0
        }
    };
    let mut gd = GroupDerived {
        egress_rate: r(g.delivered_total, base.map(|b| b.delivered_total)),
        ack_rate: r(g.acked_total, base.map(|b| b.acked_total)),
        nack_rate: r(g.nacked_total, base.map(|b| b.nacked_total)),
        dlq_rate: r(g.dead_lettered_total, base.map(|b| b.dead_lettered_total)),
        ..Default::default()
    };
    if has_window {
        gd.processing_latency = summarize(&window_histogram(
            &g.processing_latency_us,
            base.map(|b| &b.processing_latency_us),
        ));
    }
    for i in &g.instances {
        let bi = base.and_then(|b| b.instances.iter().find(|x| x.id == i.id));
        let mut id = InstanceDerived {
            egress_rate: r(i.delivered_total, bi.map(|b| b.delivered_total)),
            ack_rate: r(i.acked_total, bi.map(|b| b.acked_total)),
            nack_rate: r(i.nacked_total, bi.map(|b| b.nacked_total)),
            ..Default::default()
        };
        if has_window {
            let h = window_histogram(
                &i.processing_latency_us,
                bi.map(|b| &b.processing_latency_us),
            );
            id.processing_p99_ms = p99_ms(&h);
            id.processing_samples = h.len();
        }
        gd.instances.insert(i.id.clone(), id);
    }
    gd
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use fluxmq_core::{InstanceStats, PartitionStats, new_latency_histogram};
    use fluxmq_proto::model::{ChannelConfig, ChannelMode};

    pub fn empty_stats(ts: u64) -> BrokerStats {
        BrokerStats {
            taken_at_ms: ts,
            node_id: "n1".into(),
            cluster_name: "local".into(),
            started_at_ms: 0,
            connections: 0,
            channels: vec![],
            fsync_latency_us: new_latency_histogram(),
            commit_latency_us: new_latency_histogram(),
            storage_bytes: 0,
            storage_capacity_bytes: 0,
        }
    }

    pub fn channel(name: &str) -> ChannelStats {
        ChannelStats {
            duplicates_total: 0,
            config: ChannelConfig::new(name, ChannelMode::Stream),
            partitions: vec![PartitionStats {
                partition: 0,
                low_offset: 0,
                high_offset: 0,
                storage_bytes: 0,
                segments: 1,
                published_total: 0,
            }],
            published_total: 0,
            published_bytes_total: 0,
            delivered_total: 0,
            delivered_bytes_total: 0,
            acked_total: 0,
            nacked_total: 0,
            timed_out_total: 0,
            retries_total: 0,
            dead_lettered_total: 0,
            dlq_size: 0,
            publish_latency_us: new_latency_histogram(),
            delivery_latency_us: new_latency_histogram(),
            groups: vec![],
            oldest_timestamp_ms: None,
        }
    }

    pub fn group(name: &str) -> GroupStats {
        GroupStats {
            name: name.into(),
            lag: 0,
            in_flight: 0,
            retry_pending: 0,
            delivered_total: 0,
            acked_total: 0,
            nacked_total: 0,
            dead_lettered_total: 0,
            retention_skipped_total: 0,
            catchup_read_bytes_total: 0,
            processing_latency_us: new_latency_histogram(),
            instances: vec![],
        }
    }

    pub fn instance(id: &str) -> InstanceStats {
        InstanceStats {
            id: id.into(),
            subscription_id: 1,
            client_name: id.split('#').next().unwrap_or(id).into(),
            remote_addr: "127.0.0.1:5000".into(),
            connected_at_ms: 0,
            prefetch: 10,
            in_flight: 0,
            assigned_partitions: vec![0],
            lag: 0,
            delivered_total: 0,
            acked_total: 0,
            nacked_total: 0,
            retries_total: 0,
            processing_latency_us: new_latency_histogram(),
            last_error: None,
            last_error_at_ms: None,
            client_stats: None,
        }
    }

    fn with_channel(ts: u64, published: u64, latencies_us: &[u64]) -> BrokerStats {
        let mut s = empty_stats(ts);
        let mut c = channel("orders");
        c.published_total = published;
        c.partitions[0].published_total = published;
        for &l in latencies_us {
            c.publish_latency_us.record(l).expect("in range");
        }
        s.channels.push(c);
        s
    }

    #[test]
    fn first_snapshot_has_no_rates() {
        let mut w = Window::new(2000);
        w.push(Arc::new(with_channel(1000, 500, &[1000])));
        let d = derive(&w, true);
        assert_eq!(d.dt_secs, 0.0);
        assert_eq!(d.ingress_rate, 0.0);
        assert_eq!(d.publish_latency.p99, 0.0);
    }

    #[test]
    fn rate_over_window_and_windowed_percentiles() {
        let mut w = Window::new(2000);
        // Old latencies are huge; new ones are 2 ms. The window must exclude old ones.
        w.push(Arc::new(with_channel(0, 0, &[50_000; 10])));
        let mut lat = vec![50_000; 10];
        lat.extend([2_000; 100]);
        w.push(Arc::new(with_channel(2000, 400, &lat)));
        let d = derive(&w, true);
        assert_eq!(d.dt_secs, 2.0);
        assert_eq!(d.ingress_rate, 200.0);
        let c = &d.channels["orders"];
        assert_eq!(c.partition_ingress[&0], 200.0);
        assert!(
            (c.publish_latency.p99 - 2.0).abs() < 0.01,
            "{:?}",
            c.publish_latency
        );
        assert!((d.publish_latency.max - 2.0).abs() < 0.01);
    }

    #[test]
    fn window_slides_and_trims() {
        let mut w = Window::new(2000);
        for i in 0..=20u64 {
            w.push(Arc::new(with_channel(i * 250, i * 100, &[])));
        }
        // newest = 5000 ms; baseline must be at/before 3000 ms and recent.
        let b = w.baseline().expect("baseline").taken_at_ms;
        assert!((2500..=3000).contains(&b), "baseline {b}");
        assert!(w.retained() <= 6, "ring bounded, got {}", w.retained());
        let d = derive(&w, false);
        assert_eq!(d.ingress_rate, 400.0); // 100 per 250 ms
    }

    #[test]
    fn counter_reset_and_new_entities() {
        assert_eq!(counter_delta(10, 4), 6);
        assert_eq!(counter_delta(3, 10), 3);

        let mut old = empty_stats(0);
        let mut c = channel("orders");
        c.published_total = 1_000;
        c.acked_total = 200;
        let mut g = group("workers");
        g.acked_total = 100;
        let mut i = instance("svc#1");
        i.acked_total = 100;
        i.processing_latency_us.record(9_000).expect("in range");
        g.instances.push(i);
        c.groups.push(g);
        old.channels.push(c);

        let mut new = empty_stats(1000);
        // Channel re-created: counter went down → reset.
        let mut c = channel("orders");
        c.published_total = 50;
        c.acked_total = 280;
        let mut g = group("workers");
        g.acked_total = 150;
        // Old instance disappeared, new one created inside the window.
        let mut i = instance("svc#2");
        i.acked_total = 30;
        for _ in 0..30 {
            i.processing_latency_us.record(4_000).expect("in range");
        }
        g.instances.push(i);
        c.groups.push(g);
        new.channels.push(c);
        // Brand-new channel.
        let mut c2 = channel("fresh");
        c2.published_total = 10;
        c2.acked_total = 5;
        new.channels.push(c2);

        let d = derive_pair(&new, Some(&old), true);
        assert_eq!(d.channels["orders"].ingress_rate, 50.0);
        assert_eq!(d.channels["fresh"].ingress_rate, 10.0);
        let g = &d.channels["orders"].groups["workers"];
        assert_eq!(g.ack_rate, 50.0);
        assert!(!g.instances.contains_key("svc#1"));
        let i = &g.instances["svc#2"];
        assert_eq!(i.ack_rate, 30.0);
        assert!((i.processing_p99_ms - 4.0).abs() < 0.01);
        assert_eq!(i.processing_samples, 30);
        assert_eq!(d.ingress_rate, 60.0);
        // Cluster "consumed" = sum of channel ack rates (80 + 5).
        assert_eq!(d.channels["orders"].ack_rate, 80.0);
        assert_eq!(d.ack_rate, 85.0);
    }

    #[test]
    fn histogram_reset_falls_back_to_newest() {
        let mut old = new_latency_histogram();
        for _ in 0..10 {
            old.record(1_000).expect("in range");
        }
        let mut new = new_latency_histogram();
        new.record(5_000).expect("in range");
        let w = window_histogram(&new, Some(&old));
        assert_eq!(w.len(), 1);
        assert!((summarize(&w).max - 5.0).abs() < 0.01);
    }

    #[test]
    fn clock_going_backwards_resets_window() {
        let mut w = Window::new(2000);
        w.push(Arc::new(with_channel(5000, 100, &[])));
        w.push(Arc::new(with_channel(1000, 200, &[])));
        assert!(w.baseline().is_none());
    }
}

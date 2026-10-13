//! Health rules (docs/ARCHITECTURE.md, "Health rules"), evaluated every second.
//!
//! * Group `critical`: has lag and zero instances; or lag grew > 10× **and** by > 10k within
//!   5 min (versus the minimum lag seen in the last 5 min); or no acks for 30 s while lag > 0.
//!   `warning`: lag > 1k and growing for 60 s (lag now > lag 30 s ago > lag 60 s ago); lag
//!   ≥ 100k and not draining (not lower than 60 s ago); nack ratio > 5 % over the last minute
//!   (with ≥ 20 acks+nacks, to ignore noise); or the group dead-lettered messages in the last
//!   5 min.
//! * Instance `warning`: ack rate < 50 % of the group median (≥ 3 instances, median ≥ 1 msg/s),
//!   or processing p99 > 3× the group's (instance p99 ≥ 5 ms and ≥ 5 samples, to ignore noise).
//! * One roll-up rule: instance → group → channel → cluster, each takes the worst of its own
//!   conditions and its children (cluster also includes node health: disk over 90 % critical,
//!   over 80 % warning; fsync p99 > 50 ms warning). An open alert raises the object it is about
//!   to at least its severity ([`HealthReport::apply_alerts`]), so a status dot never says
//!   "healthy" next to an open alert.
//!
//! Interpretation notes: the "no acks for 30 s" clock only runs while the group has
//! deliverable work (`lag > retry_pending`), so an idle group that just received a message, or
//! a group only waiting for retry backoff, is not flagged as stalled.

use std::collections::{BTreeMap, HashMap, VecDeque};

use fluxmq_core::BrokerStats;
use fluxmq_proto::telemetry::{AlertsState, Health};

use crate::rates::{Derived, counter_delta};

pub const LAG_SPIKE_WINDOW_MS: u64 = 5 * 60_000;
pub const LAG_SPIKE_FACTOR: f64 = 10.0;
pub const LAG_SPIKE_MIN_GROWTH: u64 = 10_000;
pub const STALL_MS: u64 = 30_000;
pub const LAG_GROWING_MIN: u64 = 1_000;
pub const LAG_GROWING_MS: u64 = 60_000;
/// Absolute lag at which a group that is not draining becomes `warning`.
pub const LAG_HIGH_WARNING: u64 = 100_000;
pub const NACK_RATIO: f64 = 0.05;
pub const NACK_WINDOW_MS: u64 = 60_000;
pub const NACK_MIN_EVENTS: u64 = 20;
pub const DLQ_WINDOW_MS: u64 = 5 * 60_000;
pub const DISK_WARNING: f64 = 0.80;
pub const DISK_CRITICAL: f64 = 0.90;
pub const FSYNC_P99_WARNING_MS: f64 = 50.0;
pub const SLOW_INSTANCE_MIN_P99_MS: f64 = 5.0;

/// `(channel, group)`.
pub type GroupKey = (String, String);
/// `(channel, group, instance id)`.
pub type InstanceKey = (String, String, String);

/// Why a group is not healthy. Carries the numbers alert texts need.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupCondition {
    /// Lag > 0 and no instances.
    NoConsumers { lag: u64 },
    /// Lag grew > 10× and > 10k within 5 min.
    LagSpike { from: u64, to: u64, over_ms: u64 },
    /// No acks for ≥ 30 s while there is deliverable lag.
    Stalled {
        lag: u64,
        for_ms: u64,
        instances: usize,
    },
    /// Lag > 1k and growing for 60 s.
    LagGrowing { from: u64, to: u64, over_ms: u64 },
    /// Lag ≥ [`LAG_HIGH_WARNING`] and not draining (not lower than ~60 s ago).
    LagHigh { lag: u64 },
    /// Nack ratio over the last minute.
    HighNackRatio { ratio: f64, nacked: u64, total: u64 },
    /// Messages dead-lettered within the last 5 min.
    DlqGrowth { added: u64, over_ms: u64 },
}

impl GroupCondition {
    pub fn severity(&self) -> Health {
        match self {
            GroupCondition::NoConsumers { .. }
            | GroupCondition::LagSpike { .. }
            | GroupCondition::Stalled { .. } => Health::Critical,
            _ => Health::Warning,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InstanceCondition {
    LowAckRate { rate: f64, median: f64 },
    SlowProcessing { p99_ms: f64, group_p99_ms: f64 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeCondition {
    DiskFull {
        used: u64,
        total: u64,
        severity: Health,
    },
    SlowFsync {
        p99_ms: f64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupHealth {
    pub health: Health,
    pub conditions: Vec<GroupCondition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstanceHealth {
    pub health: Health,
    pub conditions: Vec<InstanceCondition>,
}

/// Node-level inputs (disk from sysinfo, fsync p99 from the broker histogram).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct NodeInputs {
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub fsync_p99_ms: f64,
}

/// Result of one evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct HealthReport {
    pub ts_ms: u64,
    pub groups: BTreeMap<GroupKey, GroupHealth>,
    pub instances: BTreeMap<InstanceKey, InstanceHealth>,
    pub channels: BTreeMap<String, Health>,
    pub node: Health,
    pub node_conditions: Vec<NodeCondition>,
    pub cluster: Health,
}

impl Default for HealthReport {
    fn default() -> Self {
        Self {
            ts_ms: 0,
            groups: BTreeMap::new(),
            instances: BTreeMap::new(),
            channels: BTreeMap::new(),
            node: Health::Healthy,
            node_conditions: vec![],
            cluster: Health::Healthy,
        }
    }
}

impl HealthReport {
    pub fn group(&self, channel: &str, group: &str) -> Health {
        self.groups
            .get(&(channel.to_string(), group.to_string()))
            .map_or(Health::Healthy, |g| g.health)
    }
    pub fn instance(&self, channel: &str, group: &str, id: &str) -> Health {
        self.instances
            .get(&(channel.to_string(), group.to_string(), id.to_string()))
            .map_or(Health::Healthy, |i| i.health)
    }
    pub fn channel(&self, channel: &str) -> Health {
        self.channels
            .get(channel)
            .copied()
            .unwrap_or(Health::Healthy)
    }

    /// The roll-up rule: group = worst(group, its instances), channel = worst(channel, its
    /// groups), cluster = worst(cluster, channels, node). Only ever raises.
    pub fn roll_up(&mut self) {
        for ((c, g, _), ih) in &self.instances {
            if let Some(gh) = self.groups.get_mut(&(c.clone(), g.clone())) {
                gh.health = worst(gh.health, ih.health);
            }
        }
        for ((c, _), gh) in &self.groups {
            let ch = self.channels.entry(c.clone()).or_insert(Health::Healthy);
            *ch = worst(*ch, gh.health);
        }
        self.cluster = self
            .channels
            .values()
            .copied()
            .fold(worst(self.cluster, self.node), worst);
    }

    /// An open alert raises the object it is about to at least its severity (`warning` at
    /// minimum): `params.instance` → that instance, `channel`+`group` → the group, `channel` →
    /// the channel, and every alert → the cluster. Then [`HealthReport::roll_up`].
    pub fn apply_alerts(&mut self, alerts: &AlertsState) {
        for a in alerts.values() {
            let sev = worst(a.severity, Health::Warning);
            if let Some(c) = &a.channel {
                if let Some(g) = &a.group {
                    if let Some(i) = a.params.get("instance").and_then(|v| v.as_str())
                        && let Some(ih) =
                            self.instances
                                .get_mut(&(c.clone(), g.clone(), i.to_string()))
                    {
                        ih.health = worst(ih.health, sev);
                    }
                    if let Some(gh) = self.groups.get_mut(&(c.clone(), g.clone())) {
                        gh.health = worst(gh.health, sev);
                    }
                }
                if let Some(ch) = self.channels.get_mut(c) {
                    *ch = worst(*ch, sev);
                }
            }
            self.cluster = worst(self.cluster, sev);
        }
        self.roll_up();
    }
}

fn worst(a: Health, b: Health) -> Health {
    a.max(b)
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    ts: u64,
    lag: u64,
    acked: u64,
    nacked: u64,
    dead: u64,
}

#[derive(Debug)]
struct GroupSeries {
    samples: VecDeque<Sample>,
    /// Last time the group made progress (an ack) or had no deliverable work.
    last_progress_ms: u64,
}

impl GroupSeries {
    /// Latest sample taken at or before `ts`.
    fn at_or_before(&self, ts: u64) -> Option<&Sample> {
        self.samples.iter().rev().find(|s| s.ts <= ts)
    }
}

/// Stateful evaluator: remembers ~5 min of per-group samples.
#[derive(Debug, Default)]
pub struct HealthTracker {
    groups: HashMap<GroupKey, GroupSeries>,
}

impl HealthTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a sample for every group and evaluates all rules. Call once per second with a
    /// derive that includes consumers.
    pub fn evaluate(
        &mut self,
        stats: &BrokerStats,
        derived: &Derived,
        node: &NodeInputs,
    ) -> HealthReport {
        let now = stats.taken_at_ms;
        let mut report = HealthReport {
            ts_ms: now,
            ..Default::default()
        };
        let mut seen: Vec<GroupKey> = Vec::new();

        for ch in &stats.channels {
            let cname = &ch.config.name;
            let mut ch_health = Health::Healthy;
            let ch_derived = derived.channels.get(cname);
            for g in &ch.groups {
                let key = (cname.clone(), g.name.clone());
                seen.push(key.clone());
                let series = self
                    .groups
                    .entry(key.clone())
                    .or_insert_with(|| GroupSeries {
                        samples: VecDeque::new(),
                        last_progress_ms: now,
                    });
                let prev_acked = series.samples.back().map(|s| s.acked);
                if prev_acked != Some(g.acked_total) && prev_acked.is_some() {
                    series.last_progress_ms = now;
                }
                let deliverable = g.lag > g.retry_pending;
                if !deliverable {
                    series.last_progress_ms = now;
                }
                series.samples.push_back(Sample {
                    ts: now,
                    lag: g.lag,
                    acked: g.acked_total,
                    nacked: g.nacked_total,
                    dead: g.dead_lettered_total,
                });
                let horizon = now.saturating_sub(LAG_SPIKE_WINDOW_MS.max(DLQ_WINDOW_MS) + 5_000);
                while series.samples.len() > 1 && series.samples[0].ts < horizon {
                    series.samples.pop_front();
                }

                let conditions =
                    group_conditions(series, now, g.lag, g.instances.len(), deliverable);
                let health = conditions
                    .iter()
                    .map(GroupCondition::severity)
                    .fold(Health::Healthy, worst);
                ch_health = worst(ch_health, health);
                report
                    .groups
                    .insert(key, GroupHealth { health, conditions });

                // Instances.
                let Some(gd) = ch_derived.and_then(|c| c.groups.get(&g.name)) else {
                    continue;
                };
                let mut rates: Vec<f64> = g
                    .instances
                    .iter()
                    .map(|i| gd.instances.get(&i.id).map_or(0.0, |d| d.ack_rate))
                    .collect();
                rates.sort_by(f64::total_cmp);
                let median = median_sorted(&rates);
                let group_p99 = gd.processing_latency.p99;
                for i in &g.instances {
                    let mut conds = Vec::new();
                    if let Some(d) = gd.instances.get(&i.id) {
                        if g.instances.len() >= 3 && median >= 1.0 && d.ack_rate < 0.5 * median {
                            conds.push(InstanceCondition::LowAckRate {
                                rate: d.ack_rate,
                                median,
                            });
                        }
                        if group_p99 > 0.0
                            && d.processing_samples >= 5
                            && d.processing_p99_ms >= SLOW_INSTANCE_MIN_P99_MS
                            && d.processing_p99_ms > 3.0 * group_p99
                        {
                            conds.push(InstanceCondition::SlowProcessing {
                                p99_ms: d.processing_p99_ms,
                                group_p99_ms: group_p99,
                            });
                        }
                    }
                    let health = if conds.is_empty() {
                        Health::Healthy
                    } else {
                        Health::Warning
                    };
                    report.instances.insert(
                        (cname.clone(), g.name.clone(), i.id.clone()),
                        InstanceHealth {
                            health,
                            conditions: conds,
                        },
                    );
                }
            }
            report.channels.insert(cname.clone(), ch_health);
        }
        self.groups.retain(|k, _| seen.contains(k));

        // Node.
        let mut node_health = Health::Healthy;
        if node.disk_total_bytes > 0 {
            let ratio = node.disk_used_bytes as f64 / node.disk_total_bytes as f64;
            let sev = if ratio > DISK_CRITICAL {
                Health::Critical
            } else if ratio > DISK_WARNING {
                Health::Warning
            } else {
                Health::Healthy
            };
            if sev != Health::Healthy {
                report.node_conditions.push(NodeCondition::DiskFull {
                    used: node.disk_used_bytes,
                    total: node.disk_total_bytes,
                    severity: sev,
                });
                node_health = worst(node_health, sev);
            }
        }
        if node.fsync_p99_ms > FSYNC_P99_WARNING_MS {
            report.node_conditions.push(NodeCondition::SlowFsync {
                p99_ms: node.fsync_p99_ms,
            });
            node_health = worst(node_health, Health::Warning);
        }
        report.node = node_health;
        report.cluster = node_health;
        report.roll_up();
        report
    }
}

fn median_sorted(v: &[f64]) -> f64 {
    match v.len() {
        0 => 0.0,
        n if n % 2 == 1 => v[n / 2],
        n => (v[n / 2 - 1] + v[n / 2]) / 2.0,
    }
}

fn group_conditions(
    series: &GroupSeries,
    now: u64,
    lag: u64,
    instances: usize,
    deliverable: bool,
) -> Vec<GroupCondition> {
    let mut out = Vec::new();
    if lag > 0 && instances == 0 {
        out.push(GroupCondition::NoConsumers { lag });
    }

    // Lag spike: versus the minimum lag (earliest occurrence) in the last 5 min.
    let window_start = now.saturating_sub(LAG_SPIKE_WINDOW_MS);
    let min = series
        .samples
        .iter()
        .filter(|s| s.ts >= window_start && s.ts < now)
        .fold(None::<Sample>, |acc, s| match acc {
            Some(a) if a.lag <= s.lag => Some(a),
            _ => Some(*s),
        });
    let mut spiked = false;
    if let Some(m) = min
        && lag as f64 > LAG_SPIKE_FACTOR * (m.lag.max(1) as f64)
        && lag.saturating_sub(m.lag) > LAG_SPIKE_MIN_GROWTH
    {
        out.push(GroupCondition::LagSpike {
            from: m.lag,
            to: lag,
            over_ms: now - m.ts,
        });
        spiked = true;
    }

    if instances > 0 && deliverable && now.saturating_sub(series.last_progress_ms) >= STALL_MS {
        out.push(GroupCondition::Stalled {
            lag,
            for_ms: now - series.last_progress_ms,
            instances,
        });
    }

    let mut growing = false;
    if !spiked && lag > LAG_GROWING_MIN {
        let s60 = series.at_or_before(now.saturating_sub(LAG_GROWING_MS));
        let s30 = series.at_or_before(now.saturating_sub(LAG_GROWING_MS / 2));
        if let (Some(s60), Some(s30)) = (s60, s30)
            && lag > s30.lag
            && s30.lag > s60.lag
        {
            out.push(GroupCondition::LagGrowing {
                from: s60.lag,
                to: lag,
                over_ms: now - s60.ts,
            });
            growing = true;
        }
    }

    if !spiked && !growing && lag >= LAG_HIGH_WARNING {
        let draining = series
            .at_or_before(now.saturating_sub(LAG_GROWING_MS))
            .is_some_and(|s60| lag < s60.lag);
        if !draining {
            out.push(GroupCondition::LagHigh { lag });
        }
    }

    let cur = series.samples.back().copied();
    let oldest = series.samples.front().copied();
    if let (Some(cur), Some(oldest)) = (cur, oldest) {
        let base = series
            .at_or_before(now.saturating_sub(NACK_WINDOW_MS))
            .copied()
            .unwrap_or(oldest);
        let acked = counter_delta(cur.acked, base.acked);
        let nacked = counter_delta(cur.nacked, base.nacked);
        let total = acked + nacked;
        if total >= NACK_MIN_EVENTS && nacked as f64 / total as f64 > NACK_RATIO {
            out.push(GroupCondition::HighNackRatio {
                ratio: nacked as f64 / total as f64,
                nacked,
                total,
            });
        }
        let base = series
            .at_or_before(now.saturating_sub(DLQ_WINDOW_MS))
            .copied()
            .unwrap_or(oldest);
        let added = counter_delta(cur.dead, base.dead);
        if added > 0 {
            out.push(GroupCondition::DlqGrowth {
                added,
                over_ms: now - base.ts,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rates::tests::{channel, empty_stats, group, instance};
    use crate::rates::{GroupDerived, InstanceDerived, derive_pair};
    use fluxmq_core::GroupStats;

    fn stats_with_group(ts: u64, g: GroupStats) -> BrokerStats {
        let mut s = empty_stats(ts);
        let mut c = channel("email");
        c.groups.push(g);
        s.channels.push(c);
        s
    }

    fn eval(t: &mut HealthTracker, s: &BrokerStats) -> HealthReport {
        let d = derive_pair(s, None, true);
        t.evaluate(s, &d, &NodeInputs::default())
    }

    fn workers(lag: u64, acked: u64, n_instances: usize) -> GroupStats {
        let mut g = group("workers");
        g.lag = lag;
        g.acked_total = acked;
        for i in 0..n_instances {
            g.instances.push(instance(&format!("svc#{i}")));
        }
        g
    }

    #[test]
    fn healthy_by_default() {
        let mut t = HealthTracker::new();
        let r = eval(&mut t, &stats_with_group(0, workers(0, 0, 1)));
        assert_eq!(r.group("email", "workers"), Health::Healthy);
        assert_eq!(r.cluster, Health::Healthy);
    }

    #[test]
    fn lag_without_instances_is_critical() {
        let mut t = HealthTracker::new();
        let r = eval(&mut t, &stats_with_group(0, workers(5, 0, 0)));
        assert_eq!(r.group("email", "workers"), Health::Critical);
        assert_eq!(r.channel("email"), Health::Critical);
        assert_eq!(r.cluster, Health::Critical);
        assert!(matches!(
            r.groups[&("email".into(), "workers".into())].conditions[0],
            GroupCondition::NoConsumers { lag: 5 }
        ));
    }

    #[test]
    fn lag_spike_is_critical() {
        let mut t = HealthTracker::new();
        let mut acked = 0;
        for (i, lag) in [1_000u64, 1_200, 2_000, 5_000, 18_000]
            .into_iter()
            .enumerate()
        {
            acked += 100; // making progress, so not "stalled"
            let r = eval(
                &mut t,
                &stats_with_group(i as u64 * 60_000, workers(lag, acked, 2)),
            );
            let g = &r.groups[&("email".into(), "workers".into())];
            if lag == 18_000 {
                assert_eq!(g.health, Health::Critical);
                assert!(g.conditions.contains(&GroupCondition::LagSpike {
                    from: 1_000,
                    to: 18_000,
                    over_ms: 240_000
                }));
            } else {
                assert!(
                    !g.conditions
                        .iter()
                        .any(|c| matches!(c, GroupCondition::LagSpike { .. }))
                );
            }
        }
    }

    #[test]
    fn stalled_after_30s_without_acks() {
        let mut t = HealthTracker::new();
        for ts in (0..=40_000).step_by(1_000) {
            let r = eval(&mut t, &stats_with_group(ts, workers(50, 10, 1)));
            let h = r.group("email", "workers");
            if ts < 30_000 {
                assert_eq!(h, Health::Healthy, "at {ts}");
            } else {
                assert_eq!(h, Health::Critical, "at {ts}");
            }
        }
        // An ack resets the clock.
        let r = eval(&mut t, &stats_with_group(41_000, workers(50, 11, 1)));
        assert_eq!(r.group("email", "workers"), Health::Healthy);
    }

    #[test]
    fn only_retry_pending_is_not_stalled() {
        let mut t = HealthTracker::new();
        let mut r = None;
        for ts in (0..=60_000).step_by(1_000) {
            let mut g = workers(5, 10, 1);
            g.retry_pending = 5;
            r = Some(eval(&mut t, &stats_with_group(ts, g)));
        }
        assert_eq!(
            r.expect("evaluated").group("email", "workers"),
            Health::Healthy
        );
    }

    #[test]
    fn growing_lag_is_warning() {
        let mut t = HealthTracker::new();
        let mut last = None;
        for (i, ts) in (0..=60_000u64).step_by(1_000).enumerate() {
            last = Some(eval(
                &mut t,
                &stats_with_group(ts, workers(1_500 + i as u64 * 20, i as u64, 2)),
            ));
        }
        let r = last.expect("evaluated");
        let g = &r.groups[&("email".into(), "workers".into())];
        assert_eq!(g.health, Health::Warning);
        assert!(matches!(
            g.conditions[0],
            GroupCondition::LagGrowing {
                from: 1_500,
                to: 2_700,
                ..
            }
        ));
    }

    #[test]
    fn nack_ratio_and_dlq_growth_are_warnings() {
        let mut t = HealthTracker::new();
        let mut g = workers(0, 0, 1);
        eval(&mut t, &stats_with_group(0, g.clone()));
        g.acked_total = 90;
        g.nacked_total = 10;
        let r = eval(&mut t, &stats_with_group(1_000, g.clone()));
        let gh = &r.groups[&("email".into(), "workers".into())];
        assert_eq!(gh.health, Health::Warning);
        assert!(matches!(
            gh.conditions[0],
            GroupCondition::HighNackRatio {
                nacked: 10,
                total: 100,
                ..
            }
        ));

        let mut t = HealthTracker::new();
        let mut g = workers(0, 0, 1);
        eval(&mut t, &stats_with_group(0, g.clone()));
        g.dead_lettered_total = 3;
        let r = eval(&mut t, &stats_with_group(1_000, g.clone()));
        assert_eq!(
            r.groups[&("email".into(), "workers".into())].conditions,
            vec![GroupCondition::DlqGrowth {
                added: 3,
                over_ms: 1_000
            }]
        );
        // 5+ minutes later without new dead letters → healthy again.
        for ts in (2_000..=302_000).step_by(10_000) {
            eval(&mut t, &stats_with_group(ts, g.clone()));
        }
        let r = eval(&mut t, &stats_with_group(303_000, g));
        assert_eq!(r.group("email", "workers"), Health::Healthy);
    }

    #[test]
    fn slow_and_low_rate_instances() {
        let mut t = HealthTracker::new();
        let s = stats_with_group(0, workers(0, 0, 3));
        let mut d = derive_pair(&s, None, true);
        let mut gd = GroupDerived::default();
        gd.processing_latency.p99 = 10.0;
        for (i, (rate, p99)) in [(100.0, 9.0), (90.0, 10.0), (20.0, 50.0)]
            .into_iter()
            .enumerate()
        {
            gd.instances.insert(
                format!("svc#{i}"),
                InstanceDerived {
                    ack_rate: rate,
                    processing_p99_ms: p99,
                    processing_samples: 100,
                    ..Default::default()
                },
            );
        }
        d.channels
            .get_mut("email")
            .expect("channel")
            .groups
            .insert("workers".into(), gd);
        let r = t.evaluate(&s, &d, &NodeInputs::default());
        assert_eq!(r.instance("email", "workers", "svc#0"), Health::Healthy);
        assert_eq!(r.instance("email", "workers", "svc#1"), Health::Healthy);
        let slow = &r.instances[&("email".into(), "workers".into(), "svc#2".into())];
        assert_eq!(slow.health, Health::Warning);
        assert_eq!(slow.conditions.len(), 2);
        // Roll-up: a slow instance makes its group, channel and the cluster `warning`.
        assert_eq!(r.group("email", "workers"), Health::Warning);
        assert_eq!(
            r.groups[&("email".into(), "workers".into())].conditions,
            vec![],
            "group's own conditions stay empty"
        );
        assert_eq!(r.channel("email"), Health::Warning);
        assert_eq!(r.cluster, Health::Warning);
    }

    #[test]
    fn high_lag_not_draining_is_warning() {
        let mut t = HealthTracker::new();
        let mut acked = 0;
        let mut last = None;
        // Flat at 150k for 70 s (consumers keep acking, so not stalled).
        for ts in (0..=70_000u64).step_by(1_000) {
            acked += 10;
            last = Some(eval(
                &mut t,
                &stats_with_group(ts, workers(150_000, acked, 2)),
            ));
        }
        let r = last.expect("evaluated");
        let g = &r.groups[&("email".into(), "workers".into())];
        assert_eq!(g.health, Health::Warning);
        assert_eq!(g.conditions, vec![GroupCondition::LagHigh { lag: 150_000 }]);

        // Draining: lower than 60 s ago → healthy.
        let mut t = HealthTracker::new();
        let mut last = None;
        for (i, ts) in (0..=70_000u64).step_by(1_000).enumerate() {
            last = Some(eval(
                &mut t,
                &stats_with_group(ts, workers(300_000 - i as u64 * 2_000, i as u64 * 2_000, 2)),
            ));
        }
        let r = last.expect("evaluated");
        assert_eq!(r.group("email", "workers"), Health::Healthy);
    }

    fn open_alert(
        id: &str,
        sev: Health,
        channel: Option<&str>,
        group: Option<&str>,
        instance: Option<&str>,
    ) -> (String, fluxmq_proto::telemetry::Alert) {
        let mut params = BTreeMap::new();
        if let Some(i) = instance {
            params.insert("instance".to_string(), serde_json::Value::from(i));
        }
        (
            id.to_string(),
            fluxmq_proto::telemetry::Alert {
                id: id.into(),
                severity: sev,
                kind: fluxmq_proto::telemetry::AlertKind::SlowInstance,
                title: "t".into(),
                detail: "d".into(),
                possible_reason: None,
                link: None,
                channel: channel.map(Into::into),
                group: group.map(Into::into),
                since_ms: 0,
                params,
            },
        )
    }

    #[test]
    fn open_alerts_raise_their_objects() {
        let mut t = HealthTracker::new();
        let mut r = eval(&mut t, &stats_with_group(0, workers(0, 0, 2)));
        assert_eq!(r.cluster, Health::Healthy);

        // Instance alert → instance, group, channel, cluster.
        let mut a = r.clone();
        a.apply_alerts(&AlertsState::from([open_alert(
            "slow_instance:email/workers/svc#1",
            Health::Warning,
            Some("email"),
            Some("workers"),
            Some("svc#1"),
        )]));
        assert_eq!(a.instance("email", "workers", "svc#1"), Health::Warning);
        assert_eq!(a.instance("email", "workers", "svc#0"), Health::Healthy);
        assert_eq!(a.group("email", "workers"), Health::Warning);
        assert_eq!(a.channel("email"), Health::Warning);
        assert_eq!(a.cluster, Health::Warning);

        // Channel-level alert → channel and cluster, groups untouched.
        let mut b = r.clone();
        b.apply_alerts(&AlertsState::from([open_alert(
            "high_publish_latency:email",
            Health::Warning,
            Some("email"),
            None,
            None,
        )]));
        assert_eq!(b.group("email", "workers"), Health::Healthy);
        assert_eq!(b.channel("email"), Health::Warning);
        assert_eq!(b.cluster, Health::Warning);

        // Node/cluster alert (no channel) → cluster only; critical stays critical.
        r.apply_alerts(&AlertsState::from([open_alert(
            "node_down:n3",
            Health::Critical,
            None,
            None,
            None,
        )]));
        assert_eq!(r.channel("email"), Health::Healthy);
        assert_eq!(r.cluster, Health::Critical);
    }

    #[test]
    fn node_rules() {
        let mut t = HealthTracker::new();
        let s = empty_stats(0);
        let d = derive_pair(&s, None, false);
        let r = t.evaluate(
            &s,
            &d,
            &NodeInputs {
                disk_used_bytes: 85,
                disk_total_bytes: 100,
                fsync_p99_ms: 1.0,
            },
        );
        assert_eq!(r.node, Health::Warning);
        let r = t.evaluate(
            &s,
            &d,
            &NodeInputs {
                disk_used_bytes: 95,
                disk_total_bytes: 100,
                fsync_p99_ms: 1.0,
            },
        );
        assert_eq!(r.node, Health::Critical);
        assert_eq!(r.cluster, Health::Critical);
        let r = t.evaluate(
            &s,
            &d,
            &NodeInputs {
                disk_used_bytes: 10,
                disk_total_bytes: 100,
                fsync_p99_ms: 80.0,
            },
        );
        assert_eq!(r.node, Health::Warning);
        assert_eq!(
            r.node_conditions,
            vec![NodeCondition::SlowFsync { p99_ms: 80.0 }]
        );
    }
}

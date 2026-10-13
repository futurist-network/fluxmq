//! Alert engine: turns health conditions into human-readable [`Alert`]s with hysteresis.
//!
//! Ids are stable per condition and subject: `{kind}:{channel}/{group}`,
//! `slow_instance:{channel}/{group}/{instance}`, `high_publish_latency:{channel}`,
//! `storage_nearly_full:{node}`, `high_publish_latency:node/{node}`.
//!
//! Hysteresis: a candidate must be present for [`AlertEngine::open_after`] consecutive
//! evaluations to open and absent for [`AlertEngine::resolve_after`] consecutive evaluations to
//! resolve. While open, the alert's text and severity follow the latest evaluation; `since_ms`
//! is when the condition was first observed.

use std::collections::{BTreeMap, HashMap};

use fluxmq_core::BrokerStats;
use fluxmq_proto::model::ChannelMode;
use fluxmq_proto::telemetry::{Alert, AlertKind, AlertsState, Health};

use crate::health::{GroupCondition, HealthReport, InstanceCondition, NodeCondition};
use crate::rates::Derived;

/// Channel publish p99 above which `HighPublishLatency` fires (only with traffic).
pub const PUBLISH_P99_ALERT_MS: f64 = 100.0;
/// Default number of consecutive evaluations (1 s each) to open an alert.
pub const DEFAULT_OPEN_AFTER: u32 = 3;
/// Default number of consecutive clean evaluations to resolve an alert.
pub const DEFAULT_RESOLVE_AFTER: u32 = 10;

/// An open/resolve transition, also surfaced in the events feed.
#[derive(Debug, Clone, PartialEq)]
pub enum Transition {
    Opened(Alert),
    Resolved(Alert),
}

#[derive(Debug)]
struct Tracked {
    hits: u32,
    misses: u32,
    first_seen_ms: u64,
    open: bool,
}

#[derive(Debug)]
pub struct AlertEngine {
    pub open_after: u32,
    pub resolve_after: u32,
    tracked: HashMap<String, Tracked>,
    open: AlertsState,
}

impl Default for AlertEngine {
    fn default() -> Self {
        Self::new(DEFAULT_OPEN_AFTER, DEFAULT_RESOLVE_AFTER)
    }
}

impl AlertEngine {
    pub fn new(open_after: u32, resolve_after: u32) -> Self {
        Self {
            open_after: open_after.max(1),
            resolve_after: resolve_after.max(1),
            tracked: HashMap::new(),
            open: BTreeMap::new(),
        }
    }

    /// Currently open alerts.
    pub fn state(&self) -> &AlertsState {
        &self.open
    }

    /// Feeds one evaluation's candidates (`since_ms` of candidates is ignored).
    pub fn evaluate(&mut self, now_ms: u64, candidates: Vec<Alert>) -> Vec<Transition> {
        let mut out = Vec::new();
        let mut present = std::collections::HashSet::new();
        for mut alert in candidates {
            present.insert(alert.id.clone());
            let t = self.tracked.entry(alert.id.clone()).or_insert(Tracked {
                hits: 0,
                misses: 0,
                first_seen_ms: now_ms,
                open: false,
            });
            t.hits = t.hits.saturating_add(1);
            t.misses = 0;
            alert.since_ms = t.first_seen_ms;
            if t.open {
                self.open.insert(alert.id.clone(), alert);
            } else if t.hits >= self.open_after {
                t.open = true;
                self.open.insert(alert.id.clone(), alert.clone());
                out.push(Transition::Opened(alert));
            }
        }
        let mut resolved = Vec::new();
        self.tracked.retain(|id, t| {
            if present.contains(id) {
                return true;
            }
            t.hits = 0;
            if !t.open {
                return false;
            }
            t.misses += 1;
            if t.misses >= self.resolve_after {
                resolved.push(id.clone());
                return false;
            }
            true
        });
        resolved.sort();
        for id in resolved {
            if let Some(a) = self.open.remove(&id) {
                out.push(Transition::Resolved(a));
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------------------
// Candidates
// ---------------------------------------------------------------------------------------

/// Builds alert candidates from a health report (plus derived values for latency alerts).
pub fn candidates(
    report: &HealthReport,
    stats: &BrokerStats,
    derived: &Derived,
    node_id: &str,
) -> Vec<Alert> {
    let mut out = Vec::new();
    let modes: HashMap<&str, ChannelMode> = stats
        .channels
        .iter()
        .map(|c| (c.config.name.as_str(), c.config.mode))
        .collect();

    for ((channel, group), gh) in &report.groups {
        let mode = modes
            .get(channel.as_str())
            .copied()
            .unwrap_or(ChannelMode::Stream);
        let name = display_name(channel);
        let backlog = if mode.is_queue() { "queue" } else { "backlog" };
        let consumers_link = Some(format!("/channels/{channel}/consumers"));
        let mk = |kind: AlertKind,
                  severity: Health,
                  title: String,
                  detail: String,
                  reason: &str,
                  link: Option<String>,
                  params: Params| Alert {
            id: format!("{}:{channel}/{group}", kind_str(kind)),
            severity,
            kind,
            title,
            detail,
            possible_reason: Some(reason.to_string()),
            link,
            channel: Some(channel.clone()),
            group: Some(group.clone()),
            since_ms: 0,
            params,
        };
        let lag_params = |variant: &str, rest: Params| {
            let mut p = rest;
            p.insert("variant".into(), variant.into());
            p.insert("mode".into(), mode.as_str().into());
            p
        };
        // LagSpike, LagGrowing and LagHigh share one id (`lag_growth:..`); the first wins
        // (they are pushed in that priority order by the health rules).
        let mut lag_done = false;
        for c in &gh.conditions {
            match c {
                GroupCondition::NoConsumers { lag } => out.push(mk(
                    AlertKind::NoConsumers,
                    Health::Critical,
                    format!("No consumers for {name}"),
                    format!("Group {group} has {} pending messages and no connected instances", fmt_count(*lag)),
                    "Consumer service is down or cannot connect to the broker.",
                    consumers_link.clone(),
                    params([("lag", (*lag).into())]),
                )),
                GroupCondition::LagSpike { from, to, over_ms } if !lag_done => {
                    lag_done = true;
                    out.push(mk(
                        AlertKind::LagGrowth,
                        Health::Critical,
                        format!("{name} {backlog} is growing rapidly"),
                        format!("Lag increased from {} → {} in {} ({group})", fmt_count(*from), fmt_count(*to), fmt_duration(*over_ms)),
                        "Consumers processing slower than producers.",
                        consumers_link.clone(),
                        lag_params("spike", params([("from", (*from).into()), ("to", (*to).into()), ("over_ms", (*over_ms).into())])),
                    ))
                }
                GroupCondition::LagGrowing { from, to, over_ms } if !lag_done => {
                    lag_done = true;
                    out.push(mk(
                        AlertKind::LagGrowth,
                        Health::Warning,
                        format!("{name} {backlog} is growing"),
                        format!("Lag increased from {} → {} in {} ({group})", fmt_count(*from), fmt_count(*to), fmt_duration(*over_ms)),
                        "Consumers processing slower than producers.",
                        consumers_link.clone(),
                        lag_params("growing", params([("from", (*from).into()), ("to", (*to).into()), ("over_ms", (*over_ms).into())])),
                    ))
                }
                GroupCondition::LagHigh { lag } if !lag_done => {
                    lag_done = true;
                    out.push(mk(
                        AlertKind::LagGrowth,
                        Health::Warning,
                        format!("{name} {backlog} is high"),
                        format!("Lag is {} messages ({group})", fmt_count(*lag)),
                        "Consumers can't keep up, or a replay/reset left a large backlog.",
                        consumers_link.clone(),
                        lag_params("high", params([("lag", (*lag).into())])),
                    ))
                }
                GroupCondition::Stalled { lag, for_ms, instances } => out.push(mk(
                    AlertKind::ConsumerStalled,
                    Health::Critical,
                    format!("{name} consumers stopped acknowledging"),
                    format!(
                        "No acks from {group} for {} while {} messages are pending ({instances} {})",
                        fmt_duration(*for_ms),
                        fmt_count(*lag),
                        if *instances == 1 { "instance" } else { "instances" }
                    ),
                    "Consumers may be stuck, deadlocked, or blocked on a downstream dependency.",
                    consumers_link.clone(),
                    params([("lag", (*lag).into()), ("for_ms", (*for_ms).into()), ("instances", (*instances).into())]),
                )),
                GroupCondition::HighNackRatio { ratio, nacked, total } => out.push(mk(
                    AlertKind::HighRetryRate,
                    Health::Warning,
                    format!("High failure rate on {name}"),
                    format!(
                        "{:.1}% of messages handled by {group} were nacked in the last minute ({} of {})",
                        ratio * 100.0,
                        fmt_count(*nacked),
                        fmt_count(*total)
                    ),
                    "Downstream dependency errors or malformed messages.",
                    consumers_link.clone(),
                    params([("ratio", (*ratio).into()), ("nacked", (*nacked).into()), ("total", (*total).into())]),
                )),
                GroupCondition::DlqGrowth { added, over_ms } => out.push(mk(
                    AlertKind::DlqGrowth,
                    Health::Warning,
                    format!("Messages are being dead-lettered on {name}"),
                    format!(
                        "{} {} moved to the DLQ in the last {} ({group})",
                        fmt_count(*added),
                        if *added == 1 { "message" } else { "messages" },
                        fmt_duration(*over_ms)
                    ),
                    "Consumers are rejecting messages or exhausting retries — check the DLQ reasons.",
                    Some(format!("/dlq?channel={channel}&group={group}")),
                    params([("added", (*added).into()), ("over_ms", (*over_ms).into())]),
                )),
                _ => {}
            }
        }
    }

    for ((channel, group, instance), ih) in &report.instances {
        let Some(c) = ih.conditions.first() else {
            continue;
        };
        let detail = ih
            .conditions
            .iter()
            .map(|c| match c {
                InstanceCondition::LowAckRate { rate, median } => {
                    format!(
                        "Ack rate {}/s vs group median {}/s",
                        fmt_rate(*rate),
                        fmt_rate(*median)
                    )
                }
                InstanceCondition::SlowProcessing {
                    p99_ms,
                    group_p99_ms,
                } => {
                    format!(
                        "Processing p99 {} vs group {}",
                        fmt_ms(*p99_ms),
                        fmt_ms(*group_p99_ms)
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("; ");
        let reason = match c {
            InstanceCondition::LowAckRate { .. } => {
                "Instance is overloaded, throttled, or running on a degraded host."
            }
            InstanceCondition::SlowProcessing { .. } => {
                "Slow downstream calls or resource contention on this instance."
            }
        };
        let mut p = params([("instance", instance.as_str().into())]);
        for c in &ih.conditions {
            match c {
                InstanceCondition::LowAckRate { rate, median } => {
                    p.insert("rate".into(), (*rate).into());
                    p.insert("median_rate".into(), (*median).into());
                }
                InstanceCondition::SlowProcessing {
                    p99_ms,
                    group_p99_ms,
                } => {
                    p.insert("p99_ms".into(), (*p99_ms).into());
                    p.insert("group_p99_ms".into(), (*group_p99_ms).into());
                }
            }
        }
        out.push(Alert {
            id: format!("slow_instance:{channel}/{group}/{instance}"),
            severity: Health::Warning,
            kind: AlertKind::SlowInstance,
            title: format!("Slow consumer {instance}"),
            detail: format!("{detail} ({group} on {channel})"),
            possible_reason: Some(reason.into()),
            link: Some(format!("/channels/{channel}/consumers")),
            channel: Some(channel.clone()),
            group: Some(group.clone()),
            since_ms: 0,
            params: p,
        });
    }

    for c in &report.node_conditions {
        match c {
            NodeCondition::DiskFull {
                used,
                total,
                severity,
            } => out.push(Alert {
                id: format!("storage_nearly_full:{node_id}"),
                severity: *severity,
                kind: AlertKind::StorageNearlyFull,
                title: "Storage nearly full".into(),
                detail: format!(
                    "Disk {:.0}% used ({} of {}) on {node_id}",
                    *used as f64 * 100.0 / (*total).max(1) as f64,
                    fmt_bytes(*used),
                    fmt_bytes(*total)
                ),
                possible_reason: Some(
                    "Retention too long, or consumers not keeping up with queue channels.".into(),
                ),
                link: Some("/nodes".into()),
                channel: None,
                group: None,
                since_ms: 0,
                params: params([
                    ("node", node_id.into()),
                    ("used", (*used).into()),
                    ("total", (*total).into()),
                ]),
            }),
            NodeCondition::SlowFsync { p99_ms } => out.push(Alert {
                id: format!("high_publish_latency:node/{node_id}"),
                severity: Health::Warning,
                kind: AlertKind::HighPublishLatency,
                title: "Disk latency is slowing down publishes".into(),
                detail: format!("fsync p99 is {} on {node_id}", fmt_ms(*p99_ms)),
                possible_reason: Some(
                    "Saturated or slow disk, or noisy neighbours on shared storage.".into(),
                ),
                link: Some("/nodes".into()),
                channel: None,
                group: None,
                since_ms: 0,
                params: params([("node", node_id.into()), ("fsync_p99_ms", (*p99_ms).into())]),
            }),
        }
    }

    for (channel, cd) in &derived.channels {
        if cd.ingress_rate > 0.0 && cd.publish_latency.p99 > PUBLISH_P99_ALERT_MS {
            out.push(Alert {
                id: format!("high_publish_latency:{channel}"),
                severity: Health::Warning,
                kind: AlertKind::HighPublishLatency,
                title: format!("High publish latency on {}", display_name(channel)),
                detail: format!(
                    "Publish p99 is {} at {} msg/s",
                    fmt_ms(cd.publish_latency.p99),
                    fmt_rate(cd.ingress_rate)
                ),
                possible_reason: Some("Slow disk fsync or large batches.".into()),
                link: Some(format!("/channels/{channel}")),
                channel: Some(channel.clone()),
                group: None,
                since_ms: 0,
                params: params([
                    ("p99_ms", cd.publish_latency.p99.into()),
                    ("rate", cd.ingress_rate.into()),
                ]),
            });
        }
    }
    out
}

/// Cluster alert candidates (clustered only): node down, under-replicated / offline partitions.
/// Ids: `node_down:{node}`, `under_replicated_partitions:cluster`, `offline_partitions:cluster`,
/// plus the live peers' node-local alerts (`tls_*:{node}/{surface}`).
pub fn cluster_candidates(info: &crate::cluster_source::ClusterInfo) -> Vec<Alert> {
    // Peers' node-local alerts (TLS certificates): every node shows them, the controller
    // notifies once. Ids are node-qualified, so they never collide with local candidates.
    let mut out = info.peer_alerts.clone();
    for (node, since_secs, error) in &info.down_detail {
        let mut p = params([("node", node.as_str().into())]);
        if let Some(s) = since_secs {
            p.insert("since_secs".into(), (*s).into());
        }
        if let Some(e) = error {
            p.insert("error".into(), e.as_str().into());
        }
        let detail = match (since_secs, error) {
            (Some(s), Some(e)) => format!("No stats from {node} for {s}s ({e})"),
            (Some(s), None) => format!("No stats from {node} for {s}s"),
            (None, Some(e)) => format!("{node} has not answered since this node started ({e})"),
            (None, None) => format!("{node} has not answered since this node started"),
        };
        out.push(Alert {
            id: format!("node_down:{node}"),
            severity: Health::Critical,
            kind: AlertKind::NodeDown,
            title: format!("Node {node} is down"),
            detail,
            possible_reason: Some(
                "Process stopped, host unreachable or network partition. Partitions it led fail \
                 over to in-sync replicas."
                    .into(),
            ),
            link: Some("/nodes".into()),
            channel: None,
            group: None,
            since_ms: 0,
            params: p,
        });
    }
    let f = &info.facts;
    if f.under_replicated > 0 {
        out.push(Alert {
            id: "under_replicated_partitions:cluster".into(),
            severity: Health::Warning,
            kind: AlertKind::UnderReplicatedPartitions,
            title: format!(
                "{} under-replicated partition{}",
                f.under_replicated,
                if f.under_replicated == 1 { "" } else { "s" }
            ),
            detail: format!(
                "{} partition{} fewer in-sync replicas than the replication factor ({})",
                f.under_replicated,
                if f.under_replicated == 1 {
                    " has"
                } else {
                    "s have"
                },
                f.replication_factor
            ),
            possible_reason: Some(
                "A replica node is down, or followers are falling behind the leader.".into(),
            ),
            link: Some("/nodes".into()),
            channel: None,
            group: None,
            since_ms: 0,
            params: params([
                ("count", f.under_replicated.into()),
                ("replication_factor", f.replication_factor.into()),
            ]),
        });
    }
    if f.offline > 0 {
        out.push(Alert {
            id: "offline_partitions:cluster".into(),
            severity: Health::Critical,
            kind: AlertKind::OfflinePartitions,
            title: format!(
                "{} partition{} offline",
                f.offline,
                if f.offline == 1 { "" } else { "s" }
            ),
            detail: format!(
                "{} partition{} no leader: publishing to and consuming from {} fails",
                f.offline,
                if f.offline == 1 { " has" } else { "s have" },
                if f.offline == 1 { "it" } else { "them" }
            ),
            possible_reason: Some("All in-sync replicas of these partitions are down.".into()),
            link: Some("/nodes".into()),
            channel: None,
            group: None,
            since_ms: 0,
            params: params([("count", f.offline.into())]),
        });
    }
    out
}

/// Structured alert/event parameters (see [`Alert::params`]).
pub type Params = BTreeMap<String, serde_json::Value>;

/// Builds a [`Params`] map from `(key, value)` pairs.
pub fn params<const N: usize>(pairs: [(&str, serde_json::Value); N]) -> Params {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub fn kind_str(kind: AlertKind) -> &'static str {
    match kind {
        AlertKind::LagGrowth => "lag_growth",
        AlertKind::ConsumerStalled => "consumer_stalled",
        AlertKind::NoConsumers => "no_consumers",
        AlertKind::DlqGrowth => "dlq_growth",
        AlertKind::HighRetryRate => "high_retry_rate",
        AlertKind::SlowInstance => "slow_instance",
        AlertKind::StorageNearlyFull => "storage_nearly_full",
        AlertKind::HighPublishLatency => "high_publish_latency",
        AlertKind::NodeDown => "node_down",
        AlertKind::UnderReplicatedPartitions => "under_replicated_partitions",
        AlertKind::OfflinePartitions => "offline_partitions",
        AlertKind::TlsCertExpiring => "tls_cert_expiring",
        AlertKind::TlsReloadFailed => "tls_reload_failed",
    }
}

// ---------------------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------------------

/// `email` → `Email`; names with separators (`payments.events`) are kept verbatim.
pub fn display_name(channel: &str) -> String {
    if !channel.is_empty() && channel.chars().all(|c| c.is_ascii_lowercase()) {
        let mut s = channel.to_string();
        s[..1].make_ascii_uppercase();
        s
    } else {
        channel.to_string()
    }
}

/// `950` → `950`, `1200` → `1.2k`, `18000` → `18k`, `2_500_000` → `2.5M`.
pub fn fmt_count(n: u64) -> String {
    fn short(v: f64, suffix: &str) -> String {
        if v < 10.0 {
            let s = format!("{v:.1}");
            format!("{}{suffix}", s.trim_end_matches(".0"))
        } else {
            format!("{v:.0}{suffix}")
        }
    }
    match n {
        0..1_000 => n.to_string(),
        1_000..1_000_000 => short(n as f64 / 1e3, "k"),
        1_000_000..1_000_000_000 => short(n as f64 / 1e6, "M"),
        _ => short(n as f64 / 1e9, "B"),
    }
}

/// `45_000` → `45 s`, `360_000` → `6 min`, `7_200_000` → `2 h`.
pub fn fmt_duration(ms: u64) -> String {
    let s = ms / 1000;
    if s < 90 {
        format!("{s} s")
    } else if s < 90 * 60 {
        format!("{} min", (s + 30) / 60)
    } else {
        format!("{} h", (s + 1800) / 3600)
    }
}

pub fn fmt_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.1} s", ms / 1000.0)
    } else if ms >= 10.0 {
        format!("{ms:.0} ms")
    } else {
        format!("{ms:.1} ms")
    }
}

pub fn fmt_rate(r: f64) -> String {
    if r >= 1000.0 {
        fmt_count(r.round() as u64)
    } else if r >= 10.0 {
        format!("{r:.0}")
    } else {
        format!("{r:.1}")
    }
}

/// Binary units with the UI's labels (`ui/src/lib/format.ts` `fmtBytes`), so alert text and
/// the gauges next to it show the same numbers.
pub fn fmt_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.0} {}", UNITS[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{GroupHealth, InstanceHealth};
    use crate::rates::tests::{channel, empty_stats};

    fn alert(id: &str, sev: Health) -> Alert {
        Alert {
            id: id.into(),
            severity: sev,
            kind: AlertKind::LagGrowth,
            title: "t".into(),
            detail: "d".into(),
            possible_reason: None,
            link: None,
            channel: None,
            group: None,
            since_ms: 0,
            params: Default::default(),
        }
    }

    #[test]
    fn formatting() {
        assert_eq!(fmt_count(950), "950");
        assert_eq!(fmt_count(1_200), "1.2k");
        assert_eq!(fmt_count(1_000), "1k");
        assert_eq!(fmt_count(18_000), "18k");
        assert_eq!(fmt_count(2_500_000), "2.5M");
        assert_eq!(fmt_duration(45_000), "45 s");
        assert_eq!(fmt_duration(360_000), "6 min");
        assert_eq!(fmt_duration(7_200_000), "2 h");
        assert_eq!(display_name("email"), "Email");
        assert_eq!(display_name("payments.events"), "payments.events");
        assert_eq!(fmt_bytes(182 * 1024 * 1024 * 1024), "182 GB");
        assert_eq!(fmt_bytes(494_384_795_648), "460 GB");
    }

    #[test]
    fn hysteresis_open_and_resolve() {
        let mut e = AlertEngine::new(3, 2);
        let a = || vec![alert("lag_growth:email/w", Health::Warning)];
        assert!(e.evaluate(1_000, a()).is_empty());
        assert!(e.evaluate(2_000, a()).is_empty());
        let t = e.evaluate(3_000, a());
        assert_eq!(t.len(), 1);
        match &t[0] {
            Transition::Opened(al) => assert_eq!(al.since_ms, 1_000),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(e.state().len(), 1);
        // Still open: no new transition, content follows latest.
        let t = e.evaluate(4_000, vec![alert("lag_growth:email/w", Health::Critical)]);
        assert!(t.is_empty());
        assert_eq!(e.state()["lag_growth:email/w"].severity, Health::Critical);
        // One miss does not resolve; a hit resets the miss counter.
        assert!(e.evaluate(5_000, vec![]).is_empty());
        assert!(e.evaluate(6_000, a()).is_empty());
        assert!(e.evaluate(7_000, vec![]).is_empty());
        let t = e.evaluate(8_000, vec![]);
        assert!(matches!(&t[..], [Transition::Resolved(a)] if a.id == "lag_growth:email/w"));
        assert!(e.state().is_empty());
    }

    #[test]
    fn flapping_condition_never_opens() {
        let mut e = AlertEngine::new(3, 2);
        for i in 0..20 {
            let c = if i % 2 == 0 {
                vec![alert("x", Health::Warning)]
            } else {
                vec![]
            };
            assert!(e.evaluate(i * 1000, c).is_empty());
        }
    }

    #[test]
    fn candidates_from_report() {
        let mut stats = empty_stats(0);
        let mut c = channel("email");
        c.config.mode = ChannelMode::Queue;
        stats.channels.push(c);
        let mut report = HealthReport::default();
        report.groups.insert(
            ("email".into(), "workers".into()),
            GroupHealth {
                health: Health::Critical,
                conditions: vec![
                    GroupCondition::LagSpike {
                        from: 1_200,
                        to: 18_000,
                        over_ms: 360_000,
                    },
                    GroupCondition::LagGrowing {
                        from: 1_200,
                        to: 18_000,
                        over_ms: 60_000,
                    },
                    GroupCondition::DlqGrowth {
                        added: 24,
                        over_ms: 300_000,
                    },
                ],
            },
        );
        report.instances.insert(
            ("email".into(), "workers".into(), "svc#3".into()),
            InstanceHealth {
                health: Health::Warning,
                conditions: vec![InstanceCondition::SlowProcessing {
                    p99_ms: 420.0,
                    group_p99_ms: 95.0,
                }],
            },
        );
        report.node_conditions.push(NodeCondition::DiskFull {
            used: 91,
            total: 100,
            severity: Health::Critical,
        });
        let d = Derived::default();
        let cs = candidates(&report, &stats, &d, "n1");
        let lag = cs
            .iter()
            .find(|a| a.id == "lag_growth:email/workers")
            .expect("lag alert");
        assert_eq!(lag.title, "Email queue is growing rapidly");
        assert_eq!(
            lag.detail,
            "Lag increased from 1.2k → 18k in 6 min (workers)"
        );
        assert_eq!(
            lag.possible_reason.as_deref(),
            Some("Consumers processing slower than producers.")
        );
        assert_eq!(lag.link.as_deref(), Some("/channels/email/consumers"));
        assert_eq!(
            cs.iter().filter(|a| a.kind == AlertKind::LagGrowth).count(),
            1
        );
        let dlq = cs
            .iter()
            .find(|a| a.id == "dlq_growth:email/workers")
            .expect("dlq alert");
        assert_eq!(
            dlq.link.as_deref(),
            Some("/dlq?channel=email&group=workers")
        );
        assert_eq!(dlq.params["added"], 24);
        assert_eq!(dlq.params["over_ms"], 300_000);
        assert_eq!(lag.params["variant"], "spike");
        assert_eq!(lag.params["mode"], "queue");
        assert_eq!(lag.params["from"], 1_200);
        assert_eq!(lag.params["to"], 18_000);
        let slow = cs
            .iter()
            .find(|a| a.id == "slow_instance:email/workers/svc#3")
            .expect("slow instance");
        assert_eq!(slow.params["instance"], "svc#3");
        assert_eq!(slow.params["p99_ms"], 420.0);
        assert_eq!(slow.params["group_p99_ms"], 95.0);
        let disk = cs
            .iter()
            .find(|a| a.id == "storage_nearly_full:n1")
            .expect("disk");
        assert_eq!(disk.severity, Health::Critical);
        assert_eq!(disk.link.as_deref(), Some("/nodes"));
        assert_eq!(disk.params["node"], "n1");
        assert_eq!(disk.params["used"], 91);
    }

    #[test]
    fn lag_high_alert() {
        let mut stats = empty_stats(0);
        stats.channels.push(channel("email"));
        let mut report = HealthReport::default();
        report.groups.insert(
            ("email".into(), "workers".into()),
            GroupHealth {
                health: Health::Warning,
                conditions: vec![GroupCondition::LagHigh { lag: 150_000 }],
            },
        );
        let cs = candidates(&report, &stats, &Derived::default(), "n1");
        assert_eq!(cs.len(), 1);
        let a = &cs[0];
        assert_eq!(a.id, "lag_growth:email/workers");
        assert_eq!(a.severity, Health::Warning);
        assert_eq!(a.title, "Email backlog is high");
        assert_eq!(a.detail, "Lag is 150k messages (workers)");
        assert_eq!(a.params["variant"], "high");
        assert_eq!(a.params["lag"], 150_000);
        assert_eq!(a.params["mode"], "stream");
    }
}

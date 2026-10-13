//! Tests for the node-stats wire format, the cluster merge, stale-peer handling, cluster
//! alerts/events in the collector and the Prometheus exposition.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use fluxmq_core::BrokerStats;
use fluxmq_proto::cluster::{NodeInfo, NodeStatus};
use fluxmq_proto::telemetry::{
    AlertKind, AlertsState, ClusterOverview, EventKind, EventsFeed, Health, NodesState, topics,
};
use serde_json::Value;

use crate::cluster_source::{ClusterFacts, ClusterState, STALE_AFTER};
use crate::node::NodeSample;
use crate::prometheus::{ClusterGauges, NodeExtras, render_stats};
use crate::rates::tests::{channel, empty_stats, group, instance};
use crate::snapshot::{
    DecodedSnapshot, Leads, NodeInput, NodeMeta, NodeStatsSnapshot, Signature, merge,
};
use crate::source::Source;
use crate::{TelemetryConfig, TelemetryHub};

fn node_stats(node: &str, ts: u64, published: u64, lag: u64, lat_us: &[u64]) -> BrokerStats {
    let mut s = empty_stats(ts);
    s.node_id = node.into();
    s.connections = 2;
    s.storage_bytes = 1000;
    let mut c = channel("orders");
    c.config.partitions = 2;
    c.partitions.push(c.partitions[0].clone());
    c.partitions[1].partition = 1;
    c.published_total = published;
    c.acked_total = published / 2;
    c.dlq_size = 1;
    for &l in lat_us {
        c.publish_latency_us.record(l).expect("in range");
    }
    let mut g = group("billing");
    g.lag = lag;
    g.in_flight = 1;
    g.acked_total = published / 2;
    g.processing_latency_us.record(500).expect("in range");
    let mut i = instance("svc#1");
    i.acked_total = published / 2;
    i.processing_latency_us.record(700).expect("in range");
    g.instances.push(i);
    c.groups.push(g);
    s.channels.push(c);
    s
}

fn meta(node: &str, ts: u64) -> NodeMeta {
    NodeMeta::local(
        &{
            let mut s = empty_stats(ts);
            s.node_id = node.into();
            s
        },
        &format!("{node}.host"),
        &NodeSample {
            cpu_percent: 12.5,
            memory_bytes: 10,
            memory_total_bytes: 100,
            disk_used_bytes: 5,
            disk_total_bytes: 50,
        },
        1.5,
        Health::Healthy,
    )
}

fn leads(parts: &[u32]) -> Leads {
    [("orders".to_string(), parts.to_vec())]
        .into_iter()
        .collect()
}

fn decoded(node: &str, ts: u64, published: u64, lag: u64, parts: &[u32]) -> DecodedSnapshot {
    let stats = node_stats(node, ts, published, lag, &[1000, 2000]);
    NodeStatsSnapshot::build(&stats, meta(node, ts), Some(leads(parts)))
        .into_decoded()
        .expect("decode")
}

fn info(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.into(),
        client_addr: format!("{id}:7000"),
        http_addr: format!("{id}:8080"),
        status: NodeStatus::Up,
        is_controller: false,
        is_voter: true,
        version: fluxmq_proto::VERSION.into(),
    }
}

// ---------------------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------------------

#[test]
fn snapshot_round_trip() {
    let stats = node_stats("n2", 5_000, 42, 7, &[100, 250, 10_000, 1_000_000]);
    let snap = NodeStatsSnapshot::build(&stats, meta("n2", 5_000), Some(leads(&[1])));
    let bytes = snap.encode().expect("encode");
    let back = NodeStatsSnapshot::decode(&bytes).expect("decode");
    assert_eq!(back, snap);
    let d = back.into_decoded().expect("histograms");
    assert_eq!(d.meta.node_id, "n2");
    assert_eq!(d.leads, Some(leads(&[1])));
    let (a, b) = (&stats.channels[0], &d.stats.channels[0]);
    assert_eq!(b.published_total, 42);
    assert_eq!(b.dlq_size, 1);
    assert_eq!(b.partitions.len(), 2);
    assert_eq!(b.publish_latency_us, a.publish_latency_us);
    assert_eq!(b.publish_latency_us.len(), 4);
    assert_eq!(
        b.groups[0].instances[0].processing_latency_us,
        a.groups[0].instances[0].processing_latency_us
    );
    assert_eq!(b.groups[0].lag, 7);
    assert_eq!(b.config, a.config);
    // Empty histograms cost nothing on the wire.
    assert!(snap.fsync_latency.0.is_empty());
    assert!(d.stats.fsync_latency_us.is_empty());
}

#[test]
fn snapshot_rejects_other_format_and_garbage() {
    let stats = node_stats("n2", 1, 1, 0, &[]);
    let mut snap = NodeStatsSnapshot::build(&stats, meta("n2", 1), None);
    snap.format = 99;
    let bytes = snap.encode().expect("encode");
    assert!(NodeStatsSnapshot::decode(&bytes).is_err());
    assert!(NodeStatsSnapshot::decode(b"\x01\x02garbage").is_err());
}

// ---------------------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------------------

#[test]
fn merge_sums_counters_histograms_and_lag() {
    let a = node_stats("n1", 10, 100, 5, &[1000]);
    let b = node_stats("n2", 10, 60, 3, &[2000, 3000]);
    let la = leads(&[0]);
    let lb = leads(&[1]);
    let m = merge(
        NodeInput {
            stats: &a,
            leads: Some(&la),
        },
        &[NodeInput {
            stats: &b,
            leads: Some(&lb),
        }],
    );
    assert_eq!(m.node_id, "n1");
    assert_eq!(m.connections, 4);
    assert_eq!(m.storage_bytes, 2000);
    let c = &m.channels[0];
    assert_eq!(c.published_total, 160);
    assert_eq!(c.acked_total, 80);
    assert_eq!(c.dlq_size, 2);
    assert_eq!(c.publish_latency_us.len(), 3);
    assert_eq!(
        c.publish_latency_us.max(),
        b.channels[0].publish_latency_us.max()
    );
    let g = &c.groups[0];
    assert_eq!(g.lag, 8);
    assert_eq!(g.in_flight, 2);
    assert_eq!(g.processing_latency_us.len(), 2);
    let ids: Vec<_> = g.instances.iter().map(|i| i.id.as_str()).collect();
    assert_eq!(ids, ["n1/svc#1", "n2/svc#1"]);
    assert_eq!(c.partitions.len(), 2);
}

#[test]
fn merge_counts_only_partition_leaders() {
    let mut a = node_stats("n1", 10, 100, 5, &[1000]);
    a.channels[0].partitions[0].high_offset = 100;
    a.channels[0].partitions[1].high_offset = 90; // stale follower copy of p1
    let mut b = node_stats("n2", 10, 50, 2, &[1000]);
    b.channels[0].partitions[0].high_offset = 95; // follower copy of p0
    b.channels[0].partitions[1].high_offset = 120;
    // n3 follows everything: its counters must not be added.
    let c3 = node_stats("n3", 10, 999, 999, &[1000]);
    let (la, lb, l3) = (leads(&[0]), leads(&[1]), Leads::new());
    let m = merge(
        NodeInput {
            stats: &a,
            leads: Some(&la),
        },
        &[
            NodeInput {
                stats: &b,
                leads: Some(&lb),
            },
            NodeInput {
                stats: &c3,
                leads: Some(&l3),
            },
        ],
    );
    let c = &m.channels[0];
    assert_eq!(c.published_total, 150);
    assert_eq!(c.groups[0].lag, 7);
    assert_eq!(c.partitions[0].high_offset, 100);
    assert_eq!(c.partitions[1].high_offset, 120);
    // Connections / storage are node properties: always summed.
    assert_eq!(m.connections, 6);
}

#[test]
fn merge_without_leads_is_identity_for_one_node() {
    let a = node_stats("n1", 10, 100, 5, &[1000]);
    let m = merge(
        NodeInput {
            stats: &a,
            leads: None,
        },
        &[],
    );
    assert_eq!(m.channels[0].published_total, 100);
    assert_eq!(m.channels[0].groups[0].lag, 5);
    assert_eq!(
        m.channels[0].publish_latency_us,
        a.channels[0].publish_latency_us
    );
    assert_eq!(m.channels[0].groups[0].instances[0].id, "n1/svc#1");
}

#[test]
fn signature_changes_on_membership_and_restart() {
    let a = node_stats("n1", 10, 1, 0, &[]);
    let b = node_stats("n2", 10, 1, 0, &[]);
    let one = Signature::of(&[NodeInput {
        stats: &a,
        leads: None,
    }]);
    let two = Signature::of(&[
        NodeInput {
            stats: &a,
            leads: None,
        },
        NodeInput {
            stats: &b,
            leads: None,
        },
    ]);
    assert_ne!(one, two);
    let mut b2 = b.clone();
    b2.started_at_ms = 99;
    let restarted = Signature::of(&[
        NodeInput {
            stats: &a,
            leads: None,
        },
        NodeInput {
            stats: &b2,
            leads: None,
        },
    ]);
    assert_ne!(two, restarted);
}

fn tls_alert(node: &str) -> fluxmq_proto::telemetry::Alert {
    fluxmq_proto::telemetry::Alert {
        id: format!("tls_cert_expiring:{node}/cluster"),
        severity: Health::Warning,
        kind: AlertKind::TlsCertExpiring,
        title: format!("TLS certificate for cluster on {node} expires in 5 days"),
        detail: "CN=n2".into(),
        possible_reason: None,
        link: Some("/settings/security".into()),
        channel: None,
        group: None,
        since_ms: 0,
        params: Default::default(),
    }
}

/// Node-local alerts ride as a trailer: they round-trip, and a snapshot without the trailer
/// (a v0.3 peer) still decodes, with no alerts.
#[test]
fn snapshot_alert_trailer_is_backward_compatible() {
    let stats = node_stats("n2", 5_000, 42, 7, &[100]);
    let mut snap = NodeStatsSnapshot::build(&stats, meta("n2", 5_000), Some(leads(&[1])));
    let plain = snap.encode().expect("encode");
    snap.alerts = vec![tls_alert("n2")];
    let with_alerts = snap.encode().expect("encode");
    assert!(with_alerts.len() > plain.len());
    let back = NodeStatsSnapshot::decode(&with_alerts).expect("decode");
    assert_eq!(back.alerts, vec![tls_alert("n2")]);
    let d = back.into_decoded().expect("decoded");
    assert_eq!(d.alerts.len(), 1);
    assert!(
        NodeStatsSnapshot::decode(&plain)
            .expect("decode")
            .alerts
            .is_empty()
    );
}

// ---------------------------------------------------------------------------------------
// Peer table / stale peers
// ---------------------------------------------------------------------------------------

fn cluster_state(now: u64) -> ClusterState {
    let mut st = ClusterState::default();
    st.set_members(vec![info("n2"), info("n3")]);
    st.facts = ClusterFacts {
        controller: Some("n2".into()),
        local_leads: Some(leads(&[0])),
        replication_factor: 3,
        ..Default::default()
    };
    st.record_ok("n2", decoded("n2", now, 50, 2, &[1]), now);
    st.record_ok("n3", decoded("n3", now, 70, 4, &[]), now);
    st
}

#[test]
fn stale_peer_is_marked_down_and_excluded() {
    let now = 1_000_000;
    let mut st = cluster_state(now);
    let local = node_stats("n1", now, 100, 5, &[1000]);
    let mut tracker = (Signature::default(), 0);

    let v = st.view(&local, now + 500, &mut tracker);
    assert!(!v.info.partial);
    assert!(v.info.down.is_empty());
    assert_eq!(v.info.peers.len(), 2);
    assert_eq!(v.stats.channels[0].published_total, 150); // n3 leads nothing
    assert_eq!(v.stats.channels[0].groups[0].lag, 7);
    let g1 = v.info.generation;
    let n2 = v.info.peers.iter().find(|p| p.id == "n2").expect("n2");
    assert!(n2.is_controller);
    assert_eq!(n2.address, "n2.host");
    assert_eq!(n2.disk_latency_p99_ms, 1.5);

    // Same composition → same generation.
    let v = st.view(&local, now + 900, &mut tracker);
    assert_eq!(v.info.generation, g1);

    // n2 keeps answering, n3 goes silent.
    let later = now + STALE_AFTER.as_millis() as u64 + 1_000;
    st.record_ok("n2", decoded("n2", later, 55, 2, &[1]), later);
    st.record_err("n3", "connection refused".into());
    let v = st.view(&local, later, &mut tracker);
    assert!(v.info.partial);
    assert_eq!(v.info.down, ["n3"]);
    let n3 = v
        .info
        .peers
        .iter()
        .find(|p| p.id == "n3")
        .expect("n3 listed");
    assert_eq!(n3.status, NodeStatus::Down);
    assert_eq!(n3.health, Health::Critical);
    let (_, secs, err) = &v.info.down_detail[0];
    assert_eq!(*secs, Some((later - now) / 1000));
    assert_eq!(err.as_deref(), Some("connection refused"));
    assert!(
        v.info.generation > g1,
        "composition changed → window restart"
    );
    // n3 contributed no counters anyway (follower), but its instances are gone.
    let ids: Vec<_> = v.stats.channels[0].groups[0]
        .instances
        .iter()
        .map(|i| i.id.clone())
        .collect();
    assert_eq!(ids, ["n1/svc#1", "n2/svc#1"]);

    // Controller-reported down wins over a fresh pull.
    let mut members = vec![info("n2"), info("n3")];
    members[0].status = NodeStatus::Down;
    st.set_members(members);
    let v = st.view(&local, later, &mut tracker);
    assert!(v.info.down.contains(&"n2".to_string()));
}

#[test]
fn never_seen_peer_is_down_and_departed_peer_is_forgotten() {
    let now = 5_000;
    let mut st = ClusterState::default();
    st.set_members(vec![info("n2")]);
    let local = node_stats("n1", now, 1, 0, &[]);
    let mut tracker = (Signature::default(), 0);
    let v = st.view(&local, now, &mut tracker);
    assert_eq!(v.info.down, ["n2"]);
    assert_eq!(v.info.down_detail[0].1, None);
    st.set_members(vec![]);
    let v = st.view(&local, now, &mut tracker);
    assert!(v.info.peers.is_empty());
    assert!(!v.info.partial);
}

// ---------------------------------------------------------------------------------------
// Collector integration
// ---------------------------------------------------------------------------------------

struct StaticSource(BrokerStats);

impl Source for StaticSource {
    fn stats(&self) -> BrokerStats {
        self.0.clone()
    }
    fn channel(&self, _: &str) -> Option<fluxmq_proto::model::ChannelConfig> {
        None
    }
    fn events(&self) -> Option<tokio::sync::broadcast::Receiver<fluxmq_core::BrokerEvent>> {
        None
    }
    fn tail(&self, _: &str) -> Result<Box<dyn crate::source::TailFeed>, String> {
        Err("no".into())
    }
}

fn hub() -> (TelemetryHub, crate::collector::Collector) {
    let src = Arc::new(StaticSource(empty_stats(0)));
    let config = TelemetryConfig {
        tick: Duration::from_secs(1),
        rate_window: Duration::from_secs(2),
        history_dir: None,
    };
    let (hub, mut c) = TelemetryHub::build(src, "n1".into(), None, config);
    c.set_alert_hysteresis(1, 1);
    (hub, c)
}

fn doc<T: serde::de::DeserializeOwned>(hub: &TelemetryHub, topic: &str) -> T {
    let v: Value = (*hub.shared.topic(topic).expect("topic").doc).clone();
    serde_json::from_value(v).expect("typed doc")
}

#[test]
fn single_node_documents_unchanged() {
    let (hub, mut c) = hub();
    assert!(c.tick(node_stats("n1", 1_000, 10, 1, &[]), vec![], None));
    let o: ClusterOverview = doc(&hub, topics::CLUSTER);
    assert_eq!(
        (o.nodes_total, o.nodes_up, o.nodes_healthy, o.partial),
        (1, 1, 1, false)
    );
    let nodes: NodesState = doc(&hub, topics::NODES);
    assert_eq!(nodes.len(), 1);
    assert!(nodes["n1"].is_controller);
    assert_eq!(nodes["n1"].status, NodeStatus::Up);
    // Instance ids are not prefixed on a single node.
    let s = hub.shared.latest.read().clone().expect("latest");
    assert_eq!(s.stats.channels[0].groups[0].instances[0].id, "svc#1");
    assert!(!hub.is_clustered());
    // Node stats are available for /internal even before clustering (handler gates on it).
    let bytes = hub.node_stats().expect("encoded");
    let d = NodeStatsSnapshot::decode(&bytes).expect("decode");
    assert_eq!(d.meta.node_id, "n1");
    assert_eq!(d.leads, None);
}

#[test]
fn collector_merges_cluster_view_and_raises_cluster_alerts() {
    let (hub, mut c) = hub();
    let now = 1_000_000;
    let mut st = cluster_state(now);
    st.facts.under_replicated = 2;
    let mut tracker = (Signature::default(), 0);

    let local = node_stats("n1", now, 100, 5, &[1000]);
    let v = st.view(&local, now, &mut tracker);
    assert!(c.tick_with(local, vec![], None, Some(v)));
    let o: ClusterOverview = doc(&hub, topics::CLUSTER);
    assert_eq!(
        (o.nodes_total, o.nodes_up, o.nodes_healthy, o.partial),
        (3, 3, 3, false)
    );
    assert_eq!(o.total_lag, 7);
    assert_eq!(o.health, Health::Warning); // under-replicated
    let nodes: NodesState = doc(&hub, topics::NODES);
    assert_eq!(nodes.len(), 3);
    assert!(nodes["n2"].is_controller);
    assert!(!nodes["n1"].is_controller);
    let alerts: AlertsState = doc(&hub, topics::ALERTS);
    let a = &alerts["under_replicated_partitions:cluster"];
    assert_eq!(a.kind, AlertKind::UnderReplicatedPartitions);
    assert_eq!(a.link.as_deref(), Some("/nodes"));
    assert_eq!(a.title, "2 under-replicated partitions");
    // The local (un-merged) stats are what peers and /metrics see.
    let local = hub.shared.local_stats.read().clone().expect("local");
    assert_eq!(local.channels[0].published_total, 100);

    // n3 goes silent, controller moves to n1, a partition goes offline.
    let later = now + STALE_AFTER.as_millis() as u64 + 1_000;
    st.record_ok("n2", decoded("n2", later, 55, 2, &[1]), later);
    st.facts.controller = Some("n1".into());
    st.facts.local_is_controller = true;
    st.facts.offline = 1;
    let local = node_stats("n1", later, 120, 5, &[1000]);
    let v = st.view(&local, later, &mut tracker);
    assert!(c.tick_with(local, vec![], None, Some(v)));

    let o: ClusterOverview = doc(&hub, topics::CLUSTER);
    assert_eq!(
        (o.nodes_total, o.nodes_up, o.nodes_healthy, o.partial),
        (3, 2, 2, true)
    );
    assert_eq!(o.health, Health::Critical);
    let nodes: NodesState = doc(&hub, topics::NODES);
    assert_eq!(nodes["n3"].status, NodeStatus::Down);
    assert!(nodes["n1"].is_controller);
    let alerts: AlertsState = doc(&hub, topics::ALERTS);
    assert_eq!(alerts["node_down:n3"].kind, AlertKind::NodeDown);
    assert_eq!(alerts["node_down:n3"].severity, Health::Critical);
    assert_eq!(
        alerts["offline_partitions:cluster"].title,
        "1 partition offline"
    );
    let feed: EventsFeed = doc(&hub, topics::EVENTS);
    let kinds: Vec<EventKind> = feed.events.iter().map(|e| e.kind).collect();
    assert!(kinds.contains(&EventKind::ControllerChanged));
    assert!(kinds.contains(&EventKind::NodeDown));
    let cc = feed
        .events
        .iter()
        .find(|e| e.kind == EventKind::ControllerChanged)
        .expect("event");
    assert_eq!(cc.subject, "n1");

    // n3 comes back.
    let back = later + 1_000;
    st.record_ok("n2", decoded("n2", back, 56, 2, &[1]), back);
    st.record_ok("n3", decoded("n3", back, 71, 4, &[]), back);
    let local = node_stats("n1", back, 130, 5, &[1000]);
    let v = st.view(&local, back, &mut tracker);
    assert!(c.tick_with(local, vec![], None, Some(v)));
    let feed: EventsFeed = doc(&hub, topics::EVENTS);
    let up = feed
        .events
        .iter()
        .find(|e| e.kind == EventKind::NodeUp)
        .expect("node up event");
    assert_eq!(up.title, "Node n3 is back");
    assert!(
        feed.events
            .iter()
            .any(|e| e.kind == EventKind::AlertResolved && e.title == "Resolved: Node n3 is down")
    );
}

#[test]
fn generation_change_restarts_rate_window() {
    let (hub, mut c) = hub();
    let mut st = ClusterState::default();
    st.set_members(vec![info("n2")]);
    st.facts.local_leads = Some(leads(&[0, 1]));
    let mut tracker = (Signature::default(), 0);
    // t=0, 1 s: n1 alone (n2 never answered) publishes 10/s.
    for (t, p) in [(0u64, 0u64), (1_000, 10)] {
        let local = node_stats("n1", 1_000_000 + t, p, 0, &[]);
        let v = st.view(&local, 1_000_000 + t, &mut tracker);
        c.tick_with(local, vec![], None, Some(v));
    }
    // n2 appears with a large lifetime total: no spike, the window restarts.
    let t = 1_002_000;
    let mut d = decoded("n2", t, 1_000_000, 0, &[]);
    d.leads = Some(leads(&[1]));
    st.facts.local_leads = Some(leads(&[0]));
    st.record_ok("n2", d, t);
    let local = node_stats("n1", t, 20, 0, &[]);
    let v = st.view(&local, t, &mut tracker);
    c.tick_with(local, vec![], None, Some(v));
    let o: ClusterOverview = doc(&hub, topics::CLUSTER);
    assert_eq!(
        o.ingress_rate, 0.0,
        "first tick after a restart has no baseline"
    );
}

// ---------------------------------------------------------------------------------------
// Prometheus
// ---------------------------------------------------------------------------------------

/// A strict-enough parser for the 0.0.4 text format: returns family → type and all samples.
#[allow(clippy::type_complexity)]
fn parse_prom(
    text: &str,
) -> (
    BTreeMap<String, String>,
    Vec<(String, BTreeMap<String, String>, f64)>,
) {
    let mut types = BTreeMap::new();
    let mut samples = Vec::new();
    let mut seen_done: Vec<String> = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        assert!(!line.is_empty(), "no blank lines expected");
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').expect("type line");
            assert!(
                ["counter", "gauge", "histogram", "summary", "untyped"].contains(&kind),
                "{kind}"
            );
            assert!(
                types.insert(name.to_string(), kind.to_string()).is_none(),
                "dup {name}"
            );
            if let Some(c) = current.replace(name.to_string()) {
                seen_done.push(c);
            }
            continue;
        }
        if line.starts_with("# HELP ") {
            continue;
        }
        assert!(!line.starts_with('#'), "unexpected comment {line}");
        let (head, value) = line.rsplit_once(' ').expect("sample");
        let v: f64 = match value {
            "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            v => v.parse().unwrap_or_else(|_| panic!("bad value in {line}")),
        };
        let (name, labels) = match head.split_once('{') {
            None => (head.to_string(), BTreeMap::new()),
            Some((n, l)) => {
                let l = l.strip_suffix('}').expect("closing brace");
                let mut m = BTreeMap::new();
                for kv in l.split("\",") {
                    let (k, v) = kv.split_once("=\"").expect("label");
                    m.insert(k.to_string(), v.trim_end_matches('"').to_string());
                }
                (n.to_string(), m)
            }
        };
        assert!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':'),
            "{name}"
        );
        let fam = current.clone().expect("sample before TYPE");
        let ok = name == fam
            || (types[&fam] == "histogram"
                && [
                    format!("{fam}_bucket"),
                    format!("{fam}_sum"),
                    format!("{fam}_count"),
                ]
                .contains(&name));
        assert!(ok, "sample {name} outside its family {fam}");
        assert!(!seen_done.contains(&name), "family {name} not contiguous");
        samples.push((name, labels, v));
    }
    (types, samples)
}

#[test]
fn prometheus_exposition_is_well_formed() {
    let mut stats = node_stats("n1", 10_000, 100, 5, &[50, 300, 2_000, 40_000, 45_000_000]);
    stats.channels[0].config.name = "we\"ird\\name".into();
    stats.fsync_latency_us.record(800).expect("in range");
    let x = NodeExtras {
        sample: NodeSample {
            cpu_percent: 3.25,
            memory_bytes: 1,
            memory_total_bytes: 2,
            disk_used_bytes: 3,
            disk_total_bytes: 4,
        },
        fsync_p99_ms: 0.8,
        node_health: Some(Health::Healthy),
        cluster_health: Some(Health::Warning),
        alerts: vec![(Health::Warning, 2)],
        cluster: Some(ClusterGauges {
            is_controller: true,
            nodes_total: 3,
            nodes_up: 2,
            under_replicated: 1,
            offline: 0,
            raft_term: 7,
            metadata_epoch: 9,
        }),
    };
    let text = render_stats(&stats, &x);
    let (types, samples) = parse_prom(&text);
    assert_eq!(types["fluxmq_channel_published_messages_total"], "counter");
    assert_eq!(types["fluxmq_channel_publish_latency_seconds"], "histogram");
    assert_eq!(types["fluxmq_group_lag_messages"], "gauge");
    assert_eq!(types["fluxmq_cluster_is_controller"], "gauge");
    for (name, _, _) in &samples {
        if types.get(name.as_str()).is_some_and(|t| t == "counter") {
            assert!(name.ends_with("_total"), "{name}");
        }
    }
    let get = |name: &str, want: &[(&str, &str)]| -> f64 {
        samples
            .iter()
            .find(|(n, l, _)| {
                n == name
                    && want
                        .iter()
                        .all(|(k, v)| l.get(*k).map(String::as_str) == Some(*v))
            })
            .unwrap_or_else(|| panic!("missing {name} {want:?}"))
            .2
    };
    let ch = "we\\\"ird\\\\name"; // escaped as it appears in the text
    assert_eq!(
        get(
            "fluxmq_channel_published_messages_total",
            &[("channel", ch)]
        ),
        100.0
    );
    assert_eq!(
        get(
            "fluxmq_group_lag_messages",
            &[("channel", ch), ("group", "billing")]
        ),
        5.0
    );
    assert_eq!(get("fluxmq_alerts_open", &[("severity", "warning")]), 2.0);
    assert_eq!(get("fluxmq_cluster_raft_term", &[("node", "n1")]), 7.0);
    assert_eq!(get("fluxmq_node_cpu_percent", &[]), 3.3);
    assert_eq!(get("fluxmq_health", &[]), 1.0);

    // Histogram: cumulative, monotonic, +Inf == count.
    let buckets: Vec<(String, f64)> = samples
        .iter()
        .filter(|(n, l, _)| {
            n == "fluxmq_channel_publish_latency_seconds_bucket"
                && l.get("channel").map(String::as_str) == Some(ch)
        })
        .map(|(_, l, v)| (l["le"].clone(), *v))
        .collect();
    assert_eq!(buckets.len(), crate::prometheus::LATENCY_BUCKETS.len() + 1);
    assert!(buckets.windows(2).all(|w| w[0].1 <= w[1].1));
    let by_le: HashMap<_, _> = buckets.iter().cloned().collect();
    assert_eq!(by_le["0.0001"], 1.0); // 50 µs
    assert_eq!(by_le["0.0005"], 2.0); // + 300 µs
    assert_eq!(by_le["0.05"], 4.0); // + 2 ms, 40 ms
    assert_eq!(by_le["30"], 4.0);
    assert_eq!(by_le["60"], 5.0); // + 45 s
    assert_eq!(by_le["+Inf"], 5.0);
    assert_eq!(
        get(
            "fluxmq_channel_publish_latency_seconds_count",
            &[("channel", ch)]
        ),
        5.0
    );
    let sum = get(
        "fluxmq_channel_publish_latency_seconds_sum",
        &[("channel", ch)],
    );
    assert!((sum - 45.04).abs() < 0.1, "{sum}");
    assert_eq!(
        get("fluxmq_node_fsync_latency_seconds_count", &[("node", "n1")]),
        1.0
    );
}

#[test]
fn prometheus_single_node_has_no_cluster_families() {
    let stats = node_stats("n1", 10_000, 1, 0, &[]);
    let text = render_stats(&stats, &NodeExtras::default());
    let (types, _) = parse_prom(&text);
    assert!(!types.keys().any(|k| k.starts_with("fluxmq_cluster_")));
    assert!(types.contains_key("fluxmq_node_info"));
}

#[test]
fn hub_prometheus_uses_local_stats() {
    let (hub, mut c) = hub();
    assert!(c.tick(node_stats("n1", 1_000, 33, 0, &[]), vec![], None));
    let text = hub.prometheus();
    assert!(text.contains("fluxmq_channel_published_messages_total{channel=\"orders\"} 33\n"));
    parse_prom(&text);
}

// ---------------------------------------------------------------------------------------
// HTTP pull
// ---------------------------------------------------------------------------------------

/// Minimal HTTP/1.1 server: answers one request per connection with the snapshot when the
/// cluster-secret header matches, 401 otherwise.
async fn serve_snapshot(body: Vec<u8>, secret: &'static str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let mut req = Vec::new();
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = sock.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    req.extend_from_slice(&buf[..n]);
                }
                let head = String::from_utf8_lossy(&req).to_ascii_lowercase();
                let ok = head.starts_with("get /internal/v1/node-stats ")
                    && head.contains(&format!("x-fluxmq-cluster-secret: {secret}\r\n"));
                let resp = if ok {
                    let mut r = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        crate::snapshot::CONTENT_TYPE,
                        body.len()
                    )
                    .into_bytes();
                    r.extend_from_slice(&body);
                    r
                } else {
                    b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        .to_vec()
                };
                let _ = sock.write_all(&resp).await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn pull_fetches_and_decodes_with_secret() {
    let stats = node_stats("n2", 7_000, 12, 3, &[1500]);
    let bytes = NodeStatsSnapshot::build(&stats, meta("n2", 7_000), Some(leads(&[0])))
        .encode()
        .unwrap();
    let addr = serve_snapshot(bytes, "s3cret").await;
    let rpc = |secret: &str| {
        fluxmq_cluster::InternalHttp::new(
            fluxmq_cluster::rpc::RpcClient::new(Some(secret.into())),
            Duration::from_secs(2),
        )
    };
    let addr = addr.trim_start_matches("http://").to_string();

    let d = crate::cluster_source::pull(&rpc("s3cret"), &addr)
        .await
        .expect("pull");
    assert_eq!(d.meta.node_id, "n2");
    assert_eq!(d.stats.channels[0].published_total, 12);
    assert_eq!(d.stats.channels[0].publish_latency_us.len(), 1);

    let err = crate::cluster_source::pull(&rpc("wrong"), &addr)
        .await
        .expect_err("wrong secret");
    assert!(err.contains("401"), "{err}");
    let err = crate::cluster_source::pull(&rpc("s3cret"), "")
        .await
        .expect_err("no address");
    assert!(err.contains("no HTTP address"));
}

#[test]
fn rebalance_plan_transitions_become_events() {
    use fluxmq_cluster::rebalance::{PlanDoc, PlanOutcome};
    use fluxmq_proto::cluster::{MoveReason, MoveState, PartitionMove, RebalanceState};
    let (_hub, mut c) = hub();
    let mv = |p: u32, state: MoveState| PartitionMove {
        channel: "orders".into(),
        partition: p,
        reason: MoveReason::Repair,
        from_replicas: vec!["n1".into(), "n2".into()],
        to_replicas: vec!["n1".into(), "n3".into()],
        state,
        lag: None,
        initial_lag: None,
        started_at_ms: None,
        finished_at_ms: None,
        error: None,
    };
    let mut plan = PlanDoc {
        id: 7,
        state: RebalanceState::Running,
        trigger: Some("node n2 down 605s".into()),
        moves: vec![mv(0, MoveState::Pending), mv(1, MoveState::Pending)],
        started_at_ms: Some(1),
        finished_at_ms: None,
        outcome: None,
        repair_nodes: vec!["n2".into()],
    };
    // First observation is a baseline: an already running plan emits nothing.
    c.rebalance_events(1, Some(&plan));
    assert!(c.events_feed().events.is_empty());
    // Progress of a known plan: one move done.
    plan.moves[0].state = MoveState::Done;
    c.rebalance_events(2, Some(&plan));
    let kinds = |c: &crate::collector::Collector| -> Vec<EventKind> {
        c.events_feed().events.iter().map(|e| e.kind).collect()
    };
    assert_eq!(kinds(&c), vec![EventKind::PartitionMoved]);
    assert_eq!(c.events_feed().events[0].subject, "orders/0");
    assert_eq!(c.events_feed().events[0].title, "n1,n2 → n1,n3");
    // Finished with a rollback → failed; not repeated on the next tick.
    plan.moves[1].state = MoveState::RolledBack;
    plan.state = RebalanceState::Idle;
    plan.outcome = Some(PlanOutcome::Failed);
    c.rebalance_events(3, Some(&plan));
    c.rebalance_events(4, Some(&plan));
    assert_eq!(
        kinds(&c),
        vec![EventKind::RebalanceFailed, EventKind::PartitionMoved]
    );
    assert_eq!(
        c.events_feed().events[0].title,
        "Rebalance failed: 1 done, 1 rolled back"
    );
    // A new plan: started + repair started, then completed with its move.
    let mut next = plan.clone();
    next.id = 8;
    next.state = RebalanceState::Running;
    next.outcome = None;
    next.moves = vec![mv(2, MoveState::Pending)];
    c.rebalance_events(5, Some(&next));
    let k = kinds(&c);
    assert_eq!(k[0], EventKind::ReplicaRepairStarted);
    assert_eq!(k[1], EventKind::RebalanceStarted);
    assert_eq!(c.events_feed().events[1].subject, "node n2 down 605s");
    next.moves[0].state = MoveState::Done;
    next.state = RebalanceState::Idle;
    next.outcome = Some(PlanOutcome::Completed);
    c.rebalance_events(6, Some(&next));
    let k = kinds(&c);
    assert_eq!(k[0], EventKind::RebalanceCompleted);
    assert_eq!(k[1], EventKind::PartitionMoved);
}

#[test]
fn decommission_transitions_become_events() {
    use fluxmq_cluster::DecommissionRecord;
    use fluxmq_proto::cluster::{DecommissionState, DecommissionStatus};
    let (_hub, mut c) = hub();
    let rec =
        |node: &str, state: DecommissionState, started: u64, forget: bool| DecommissionRecord {
            status: DecommissionStatus {
                node: node.into(),
                state,
                replicas_remaining: 0,
                initial_replicas: if forget { 0 } else { 4 },
                started_at_ms: started,
                finished_at_ms: None,
                error: None,
            },
            forget,
        };
    let kinds = |c: &crate::collector::Collector| -> Vec<EventKind> {
        c.events_feed().events.iter().map(|e| e.kind).collect()
    };
    // Baseline: an old finished record emits nothing.
    c.decommission_events(1, &[rec("n9", DecommissionState::Removed, 1, false)]);
    assert!(c.events_feed().events.is_empty());
    // A new decommission: started; progress emits nothing; removed once.
    let mut d = rec("n4", DecommissionState::MovingReplicas, 10, false);
    c.decommission_events(2, std::slice::from_ref(&d));
    assert_eq!(kinds(&c), vec![EventKind::NodeDecommissionStarted]);
    assert_eq!(c.events_feed().events[0].subject, "n4");
    assert_eq!(
        c.events_feed().events[0].title,
        "Decommissioning node n4: 4 replicas to move"
    );
    d.status.state = DecommissionState::LeavingMembership;
    c.decommission_events(3, std::slice::from_ref(&d));
    d.status.state = DecommissionState::Removed;
    c.decommission_events(4, std::slice::from_ref(&d));
    c.decommission_events(5, std::slice::from_ref(&d));
    assert_eq!(
        kinds(&c),
        vec![
            EventKind::NodeDecommissioned,
            EventKind::NodeDecommissionStarted
        ]
    );
    // A retry after a failure is a new record (new start time); failure carries the error.
    let mut f = rec("n4", DecommissionState::MovingReplicas, 20, false);
    c.decommission_events(6, std::slice::from_ref(&f));
    f.status.state = DecommissionState::Failed;
    f.status.error = Some("cancelled".into());
    c.decommission_events(7, std::slice::from_ref(&f));
    assert_eq!(kinds(&c)[0], EventKind::NodeDecommissionFailed);
    assert_eq!(kinds(&c)[1], EventKind::NodeDecommissionStarted);
    assert_eq!(
        c.events_feed().events[0].title,
        "Decommission of node n4 failed: cancelled"
    );
    assert_eq!(c.events_feed().events[0].severity, Health::Critical);
    // Forget: no "started"; a record first seen already removed still reports once.
    c.decommission_events(
        8,
        &[f.clone(), rec("n5", DecommissionState::Removed, 30, true)],
    );
    c.decommission_events(9, &[f, rec("n5", DecommissionState::Removed, 30, true)]);
    assert_eq!(kinds(&c)[0], EventKind::NodeForgotten);
    assert_eq!(kinds(&c)[1], EventKind::NodeDecommissionFailed);
    assert_eq!(c.events_feed().events[0].title, "Node n5 forgotten");
}

/// A TLS alert raised on a peer (node-local) shows up in this node's alerts and opens through
/// the normal transition path, so the controller notifies it once.
#[test]
fn peer_tls_alerts_join_the_cluster_alerts() {
    let (hub, mut c) = hub();
    let mut signals = hub.notifications();
    let now = 1_000_000;
    let mut st = cluster_state(now);
    let mut d = decoded("n2", now, 50, 2, &[1]);
    d.alerts = vec![tls_alert("n2")];
    st.record_ok("n2", d, now);
    let mut tracker = (Signature::default(), 0);
    let local = node_stats("n1", now, 100, 5, &[1000]);
    let v = st.view(&local, now, &mut tracker);
    assert_eq!(v.info.peer_alerts.len(), 1);
    assert!(c.tick_with(local, vec![], None, Some(v)));
    let alerts: AlertsState = doc(&hub, topics::ALERTS);
    let a = &alerts["tls_cert_expiring:n2/cluster"];
    assert_eq!(a.kind, AlertKind::TlsCertExpiring);
    let mut opened = false;
    while let Ok(sig) = signals.try_recv() {
        if let crate::TelemetrySignal::AlertOpened(a) = sig {
            opened |= a.id == "tls_cert_expiring:n2/cluster";
        }
    }
    assert!(opened, "alert transition signalled");

    // The peer renews its certificate: the alert resolves.
    let later = now + 1_000;
    st.record_ok("n2", decoded("n2", later, 55, 2, &[1]), later);
    st.record_ok("n3", decoded("n3", later, 71, 4, &[]), later);
    let local = node_stats("n1", later, 120, 5, &[1000]);
    let v = st.view(&local, later, &mut tracker);
    assert!(c.tick_with(local, vec![], None, Some(v)));
    let alerts: AlertsState = doc(&hub, topics::ALERTS);
    assert!(!alerts.contains_key("tls_cert_expiring:n2/cluster"));
}

/// Alert params never reach the postcard trailer: they are dropped in transit, and a trailer
/// in the frozen legacy layout (what older peers send) decodes.
#[test]
fn alert_trailer_drops_params_and_reads_legacy_layout() {
    let stats = node_stats("n2", 5_000, 42, 7, &[100]);
    let mut snap = NodeStatsSnapshot::build(&stats, meta("n2", 1), None);
    let mut with_params = tls_alert("n2");
    with_params
        .params
        .insert("node".into(), serde_json::Value::from("n2"));
    snap.alerts = vec![with_params];
    let back = NodeStatsSnapshot::decode(&snap.encode().expect("encode")).expect("decode");
    assert_eq!(
        back.alerts,
        vec![tls_alert("n2")],
        "params dropped, rest intact"
    );

    // Legacy layout: a plain struct with the pre-params fields, serialized by hand.
    #[derive(serde::Serialize)]
    struct Legacy {
        id: String,
        severity: Health,
        kind: fluxmq_proto::telemetry::AlertKind,
        title: String,
        detail: String,
        possible_reason: Option<String>,
        link: Option<String>,
        channel: Option<String>,
        group: Option<String>,
        since_ms: u64,
    }
    let a = tls_alert("n3");
    let legacy = vec![Legacy {
        id: a.id.clone(),
        severity: a.severity,
        kind: a.kind,
        title: a.title.clone(),
        detail: a.detail.clone(),
        possible_reason: a.possible_reason.clone(),
        link: a.link.clone(),
        channel: a.channel.clone(),
        group: a.group.clone(),
        since_ms: a.since_ms,
    }];
    snap.alerts = vec![];
    let mut bytes = snap.encode().expect("encode");
    bytes.extend_from_slice(&postcard::to_stdvec(&legacy).expect("legacy"));
    let back = NodeStatsSnapshot::decode(&bytes).expect("decode legacy");
    assert_eq!(back.alerts, vec![a]);
}

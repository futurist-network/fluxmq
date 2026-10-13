//! Cluster topology cache and client-side partitioning.

use std::collections::HashMap;

use fluxmq_proto::cluster::{ClusterMetadata, NodeStatus};

/// What the client knows about the broker topology.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) enum Topology {
    /// The broker doesn't serve metadata (single node / v1 behaviour) or we couldn't fetch it
    /// yet: everything goes to the bootstrap connection with `partition: None`.
    #[default]
    Single,
    Cluster(ClusterView),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClusterView {
    pub(crate) meta: ClusterMetadata,
    /// node id → client address.
    addrs: HashMap<String, String>,
}

impl ClusterView {
    pub(crate) fn new(meta: ClusterMetadata) -> Self {
        let addrs = meta
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.client_addr.clone()))
            .collect();
        Self { meta, addrs }
    }

    pub(crate) fn addr_of(&self, node: &str) -> Option<&str> {
        self.addrs.get(node).map(String::as_str)
    }

    pub(crate) fn has_node(&self, node: &str) -> bool {
        self.addrs.contains_key(node)
    }

    /// Client addresses of nodes not known to be down.
    pub(crate) fn live_addrs(&self) -> Vec<String> {
        self.meta
            .nodes
            .iter()
            .filter(|n| n.status != NodeStatus::Down)
            .map(|n| n.client_addr.clone())
            .collect()
    }
}

impl Topology {
    pub(crate) fn epoch(&self) -> u64 {
        match self {
            Topology::Single => 0,
            Topology::Cluster(v) => v.meta.epoch,
        }
    }

    pub(crate) fn is_cluster(&self) -> bool {
        matches!(self, Topology::Cluster(_))
    }

    /// Partition count of `channel`, if the topology knows it (cluster mode only).
    pub(crate) fn partitions(&self, channel: &str) -> Option<u32> {
        match self {
            Topology::Single => None,
            Topology::Cluster(v) => v
                .meta
                .channels
                .get(channel)
                .filter(|p| !p.is_empty())
                .map(|p| p.len() as u32),
        }
    }

    /// `channel` is a `Priority` queue (v0.12): its partition is the message's priority level.
    pub(crate) fn is_priority(&self, channel: &str) -> bool {
        match self {
            Topology::Single => false,
            Topology::Cluster(v) => v.meta.priority_channels.iter().any(|c| c == channel),
        }
    }

    /// Leader of `channel`/`partition` (`None` while an election is in progress or unknown).
    pub(crate) fn leader(&self, channel: &str, partition: u32) -> Option<String> {
        match self {
            Topology::Single => None,
            Topology::Cluster(v) => v
                .meta
                .channels
                .get(channel)?
                .iter()
                .find(|a| a.partition == partition)
                .or_else(|| v.meta.channels.get(channel)?.get(partition as usize))
                .and_then(|a| a.leader.clone()),
        }
    }

    /// Nodes that lead ≥ 1 partition of `channel` (sorted, deduplicated). `None` = the channel
    /// is not in the metadata (or single node).
    pub(crate) fn channel_leaders(&self, channel: &str) -> Option<Vec<String>> {
        match self {
            Topology::Single => None,
            Topology::Cluster(v) => {
                let parts = v.meta.channels.get(channel)?;
                let mut leaders: Vec<String> =
                    parts.iter().filter_map(|a| a.leader.clone()).collect();
                leaders.sort();
                leaders.dedup();
                Some(leaders)
            }
        }
    }
}

/// The broker's partitioning function: the contract key hash
/// ([`fluxmq_proto::batch::key_partition`], `crc32(key) % partitions`), `rr` without a key.
pub(crate) fn partition_for(
    key: Option<&str>,
    partitions: u32,
    rr: &mut dyn FnMut() -> u64,
) -> u32 {
    let n = partitions.max(1);
    match key {
        Some(k) => fluxmq_proto::batch::key_partition(k, n),
        None => (rr() % u64::from(n)) as u32,
    }
}

/// Replaces an unroutable advertised host (`0.0.0.0`, `[::]`, empty) with `fallback_host`.
pub(crate) fn fix_addr(addr: &str, fallback_host: Option<&str>) -> String {
    let Some((host, port)) = addr.rsplit_once(':') else {
        return addr.to_string();
    };
    let unroutable = matches!(host, "" | "0.0.0.0" | "[::]" | "::");
    match (unroutable, fallback_host) {
        (true, Some(h)) => format!("{h}:{port}"),
        (true, None) => format!("127.0.0.1:{port}"),
        _ => addr.to_string(),
    }
}

/// Host part of `host:port` (also `tls://host:port`).
pub(crate) fn host_of(addr: &str) -> Option<&str> {
    let addr = addr
        .strip_prefix(crate::options::TLS_SCHEME)
        .unwrap_or(addr);
    addr.rsplit_once(':').map(|(h, _)| h)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitioning_matches_core() {
        // crc32("order-8842") computed independently of the implementation.
        let k = "order-8842";
        let expected = crc32fast::hash(k.as_bytes()) % 8;
        let mut rr = || 0;
        assert_eq!(partition_for(Some(k), 8, &mut rr), expected);
        let mut c = 0u64;
        let mut rr = || {
            c += 1;
            c - 1
        };
        let got: Vec<u32> = (0..6).map(|_| partition_for(None, 4, &mut rr)).collect();
        assert_eq!(got, vec![0, 1, 2, 3, 0, 1]);
        assert_eq!(partition_for(Some("x"), 0, &mut || 0), 0);
    }

    #[test]
    fn fixes_unroutable_hosts() {
        assert_eq!(fix_addr("0.0.0.0:7420", Some("10.0.0.5")), "10.0.0.5:7420");
        assert_eq!(fix_addr("0.0.0.0:7420", None), "127.0.0.1:7420");
        assert_eq!(fix_addr("n1:7420", Some("x")), "n1:7420");
    }
}

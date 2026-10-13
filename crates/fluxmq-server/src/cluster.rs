//! Cluster wiring: picks [`Cluster::start`] when peers are configured, else
//! [`Cluster::single_node`], and derives the advertised addresses.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Context as _;
use fluxmq_cluster::{Cluster, ClusterConfig};
use fluxmq_core::Broker;

use crate::ServerConfig;

/// `host:port` others use to reach a listener bound at `bound`.
pub fn advertise(host: Option<&str>, bound: SocketAddr) -> String {
    let host = match host {
        Some(h) => h.to_string(),
        None if bound.ip().is_unspecified() => Ipv4Addr::LOCALHOST.to_string(),
        None => bound.ip().to_string(),
    };
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]:{}", bound.port()),
        _ => format!("{host}:{}", bound.port()),
    }
}

pub async fn open(
    config: &ServerConfig,
    broker: &Broker,
    client_addr: SocketAddr,
    http_addr: SocketAddr,
    tls: Option<fluxmq_tls::ClusterTls>,
) -> anyhow::Result<Cluster> {
    let Some(c) = &config.cluster else {
        return Ok(Cluster::single_node(
            broker.clone(),
            &config.data_dir,
            &config.cluster_name,
            advertise(None, client_addr),
            advertise(None, http_addr),
        )
        .await?);
    };
    // `ServerConfig::validate` refuses a missing advertise host when peers are remote.
    let host = c.advertise_host.as_deref();
    let cfg = ClusterConfig {
        node_id: broker.node_id().to_string(),
        cluster_name: config.cluster_name.clone(),
        data_dir: config.data_dir.clone(),
        raft_addr: c.raft_addr,
        replication_addr: c.replication_addr,
        advertise_client_addr: advertise(host, client_addr),
        advertise_http_addr: advertise(host, http_addr),
        advertise_raft_addr: advertise(host, c.raft_addr),
        advertise_replication_addr: advertise(host, c.replication_addr),
        peers: c.peers.clone(),
        replication_factor: c.replication_factor,
        min_isr: c.min_isr,
        cluster_secret: c.cluster_secret.clone(),
        node_timeout: c.node_timeout,
        auto_rebalance: config.auto_rebalance,
        rebalance_interval: config.rebalance_interval,
        tls,
    };
    Cluster::start(cfg, broker.clone())
        .await
        .context("starting cluster node")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_addresses() {
        let any: SocketAddr = "0.0.0.0:7420".parse().unwrap();
        assert_eq!(advertise(None, any), "127.0.0.1:7420");
        assert_eq!(advertise(Some("node-a"), any), "node-a:7420");
        assert_eq!(advertise(Some("::1"), any), "[::1]:7420");
        let lo: SocketAddr = "10.1.2.3:9".parse().unwrap();
        assert_eq!(advertise(None, lo), "10.1.2.3:9");
    }
}

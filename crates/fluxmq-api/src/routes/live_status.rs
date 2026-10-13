//! `GET /api/live/status` (v0.11): the live subscription gateway on every node — limits,
//! connections, subscriptions per channel, counters. Read-only; nothing secret in it.

use axum::extract::State;
use fluxmq_proto::live::{LiveNodeStatus, LiveStatus};

use crate::state::AppState;

const INTERNAL_STATUS_PATH: &str = "/internal/v1/live/status";

/// This node first, then the peers that answered (unreachable nodes are left out).
pub async fn status(State(s): State<AppState>) -> axum::Json<LiveStatus> {
    let mut nodes = vec![s.live.status(s.broker.node_id())];
    if s.cluster.is_clustered() {
        let mut peers =
            super::webhooks::from_peers::<_, LiveNodeStatus>(&s, INTERNAL_STATUS_PATH, &()).await;
        peers.sort_by(|a, b| a.node.cmp(&b.node));
        nodes.extend(peers);
    }
    axum::Json(LiveStatus { nodes })
}

async fn internal_status(State(s): State<AppState>) -> axum::Json<LiveNodeStatus> {
    axum::Json(s.live.status(s.broker.node_id()))
}

/// Node-to-node routes, merged into `cluster::internal`.
pub fn internal() -> axum::Router<AppState> {
    axum::Router::new().route(INTERNAL_STATUS_PATH, axum::routing::get(internal_status))
}

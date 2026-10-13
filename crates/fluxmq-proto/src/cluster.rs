//! Cluster contract (roadmap stage 9).
//!
//! Architecture: every node runs a Raft member (openraft) for **metadata** — channel configs,
//! partition assignments, RBAC tokens, routes, node registry — exposed through the
//! `fluxmq-meta` `MetaStore`. The Raft leader is the **controller**: it assigns partition
//! leaders/replicas and performs failover. **Data** is replicated per partition Kafka-style:
//! followers fetch from the partition leader; the leader tracks the in-sync replica set (ISR)
//! and the high watermark (HW = min log end offset over the ISR). A publish is acknowledged
//! once durable on the leader and replicated to the ISR (`acks=all`); consumers only see
//! offsets below the HW. Leader changes bump the partition `epoch`; a demoted leader
//! truncates to the new leader's HW for its epoch before following.
//!
//! A single node is simply a cluster of one: replication factor 1, no network replication.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Up,
    /// Missed heartbeats; partitions it led are being failed over.
    Down,
    /// Joining / catching up.
    Joining,
    /// Graceful leave in progress.
    Leaving,
    /// v0.5: being removed permanently — the rebalancer moves every replica away; once the
    /// node holds none it is removed from Raft membership and the registry.
    Decommissioning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    /// Address clients use for the binary protocol.
    pub client_addr: String,
    /// Address for REST / WS / UI.
    pub http_addr: String,
    pub status: NodeStatus,
    /// Currently the Raft leader / controller.
    pub is_controller: bool,
    /// Raft voter (false = learner).
    pub is_voter: bool,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub partition: u32,
    pub leader: Option<String>,
    /// All replicas incl. the leader, preferred leader first.
    pub replicas: Vec<String>,
    pub isr: Vec<String>,
    pub epoch: u64,
}

/// Returned by the wire `Metadata` request and `GET /api/cluster/metadata`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub cluster_name: String,
    /// Increases on every topology/assignment change.
    pub epoch: u64,
    pub controller: Option<String>,
    pub nodes: Vec<NodeInfo>,
    /// channel → assignment per partition (index = partition).
    pub channels: BTreeMap<String, Vec<PartitionAssignment>>,
    /// v0.12: channels in `Priority` mode (the SDK places messages by priority level there).
    /// Omitted when empty; older clients ignore it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub priority_channels: Vec<String>,
}

/// `GET /api/cluster/status` — operator view incl. replication health.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub metadata: ClusterMetadata,
    pub replication_factor: u32,
    pub under_replicated_partitions: u32,
    pub offline_partitions: u32,
    pub raft: RaftStatus,
    pub partitions: Vec<PartitionReplicationStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RaftStatus {
    pub term: u64,
    pub leader: Option<String>,
    pub last_log_index: u64,
    pub committed_index: u64,
    pub voters: Vec<String>,
    pub learners: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionReplicationStatus {
    pub channel: String,
    pub partition: u32,
    pub leader: Option<String>,
    pub epoch: u64,
    pub high_watermark: u64,
    pub log_end_offset: u64,
    /// follower node → messages behind the leader.
    pub follower_lag: BTreeMap<String, u64>,
    pub isr: Vec<String>,
}

/// `POST /api/cluster/partitions/reassign` (admin).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReassignRequest {
    pub channel: String,
    pub partition: u32,
    pub replicas: Vec<String>,
}

/// `POST /api/cluster/nodes/{id}/drain` moves leadership away before maintenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainResponse {
    pub moved_leaderships: u32,
}

// ---------------------------------------------------------------------------------------------
// Automatic rebalancing (v0.4)
// ---------------------------------------------------------------------------------------------
//
// The controller keeps replica placement and leadership balanced without operator action:
//
// * **Repair** — a replica on a node that has been `Down` longer than `replace_after_secs` is
//   replaced by a replica on a live node (restores the replication factor). A channel created
//   while fewer nodes were alive (RF capped) grows to its configured RF when nodes appear.
// * **Balance** — when a node joins (stable for `rebalance_stable`) or replica counts drift,
//   the planner computes a target placement that balances replicas per node and preferred
//   leaders per node, moving as few replicas as possible (sticky; never two replicas of one
//   partition on one node). Triggered when `max - min` replicas per live node exceeds
//   `imbalance_threshold` (or any repair is needed).
// * **Leaders** — preferred-leader election (already in v0.2) moves leadership back to
//   replica 0 once it is in the ISR; the planner chooses replica 0 to spread leaders evenly.
//
// A move never lowers durability: (1) add the target replicas (`replicas = old ∪ new`, old
// preferred first) and wait until every new replica is in the ISR; (2) commit the final
// replica list (target order, preferred first) — removed replicas leave the ISR and delete
// their local copy; (3) preferred-leader election moves leadership if needed. A move whose
// new replica doesn't reach the ISR within `move_timeout_secs` is rolled back to the old
// replica list. At most `max_concurrent_moves` partitions move at once cluster-wide; follower
// catch-up for moving replicas is throttled to `throttle_bytes_per_sec` (0 = unlimited).
//
// The plan lives in meta (`cluster/rebalance/plan`), config in `cluster/rebalance/config`,
// so a new controller resumes an interrupted plan. Every step is an ordinary assignment write
// (epoch bump), so clients and replication follow it the same way as failover.
//
// REST: `GET /api/cluster/rebalance` (viewer) → `RebalanceStatus`;
// `PUT /api/cluster/rebalance/config` (admin, `RebalanceConfig`);
// `POST /api/cluster/rebalance` (admin, `RebalanceRequest`) → `RebalanceStatus` — dry run
// returns the plan without executing; `POST /api/cluster/rebalance/pause|resume|cancel`
// (admin). Cancel stops after in-flight moves finish (or roll back); it never leaves a
// partition below its old replica set; pending moves become `cancelled`. Single node: `GET`
// returns `mode = off`, `state = idle`, mutations return 400 `unsupported_single_node`.
// Every mutation (`PUT config`, `POST`, pause/resume/cancel) responds with the resulting
// `RebalanceStatus`. Executing `POST` while a plan is running/paused/cancelling → 409
// `conflict` (a dry run is always allowed). `RebalanceRequest.channels` are exact channel
// names (unknown name → 404).
//
// Events (telemetry `EventKind`, which has no detail field — the information goes into
// `title`/`subject`): `RebalanceStarted` (subject = trigger), `RebalanceCompleted` (title
// says "completed" or "cancelled", with done/rolled-back counts), `RebalanceFailed`,
// `PartitionMoved` (subject = `channel/partition`, title = `n1,n2 → n2,n4`),
// `ReplicaRepairStarted` (subject = down node id).

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceMode {
    /// No automatic moves (manual `POST` still works). Preferred-leader election stays on.
    Off,
    /// Only repair (restore RF after `replace_after_secs`, grow capped RF).
    Repair,
    /// Repair + balance replicas and leaders.
    #[default]
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceConfig {
    pub mode: RebalanceMode,
    /// Default 2.
    pub max_concurrent_moves: u32,
    /// Max allowed `max - min` replicas per live node before balancing. Default 1.
    pub imbalance_threshold: u32,
    /// Replace replicas on a node down for this long. Default 600; 0 = never.
    pub replace_after_secs: u64,
    /// Catch-up throttle per moving replica. Default 0 (unlimited).
    pub throttle_bytes_per_sec: u64,
    /// Default 1800.
    pub move_timeout_secs: u64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            mode: RebalanceMode::Full,
            max_concurrent_moves: 2,
            imbalance_threshold: 1,
            replace_after_secs: 600,
            throttle_bytes_per_sec: 0,
            move_timeout_secs: 1800,
        }
    }
}

/// `POST /api/cluster/rebalance`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalanceRequest {
    /// Compute and return the plan only.
    #[serde(default)]
    pub dry_run: bool,
    /// Limit to these channels (empty = all).
    #[serde(default)]
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceState {
    Idle,
    Running,
    Paused,
    /// Cancel requested; waiting for in-flight moves.
    Cancelling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveReason {
    /// Restore RF (replica on a long-down node, or capped RF growth).
    Repair,
    /// Even out replicas / leaders.
    Balance,
    /// Operator `POST /api/cluster/rebalance`.
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveState {
    Pending,
    /// New replicas added, catching up.
    CatchingUp,
    /// Final replica list being committed / leader moving.
    Finalizing,
    Done,
    /// Timed out or failed; old replica list restored.
    RolledBack,
    /// Dropped by cancel before it started (never touched the assignment).
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionMove {
    pub channel: String,
    pub partition: u32,
    pub reason: MoveReason,
    pub from_replicas: Vec<String>,
    /// Target order, preferred leader first.
    pub to_replicas: Vec<String>,
    pub state: MoveState,
    /// Messages the slowest new replica is behind the leader (`CatchingUp`).
    pub lag: Option<u64>,
    /// `lag` when catch-up started — progress = 1 - lag / initial_lag.
    pub initial_lag: Option<u64>,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLoad {
    pub node: String,
    /// Every registered node is listed; `Down`/`Leaving` nodes still show the replicas
    /// assigned to them (repair moves them away). Imbalance counts only `Up` nodes.
    pub status: NodeStatus,
    pub replicas: u32,
    pub leaders: u32,
    /// Preferred leaderships (replica 0) — where leaders settle after elections.
    pub preferred_leaders: u32,
    pub storage_bytes: u64,
}

/// `GET /api/cluster/rebalance`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceStatus {
    pub config: RebalanceConfig,
    pub state: RebalanceState,
    /// Current load of every registered node (see `NodeLoad::status`).
    pub load: Vec<NodeLoad>,
    /// `max - min` replicas per `Up` node.
    pub imbalance: u32,
    /// Partitions with fewer live replicas than their RF.
    pub under_replicated: u32,
    /// Current (or last) plan; for a dry run, the proposed plan.
    pub moves: Vec<PartitionMove>,
    /// Why the current plan exists (`node n4 joined`, `node n2 down 600s`, `manual`).
    pub trigger: Option<String>,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    /// Load expected after the plan completes.
    pub projected_load: Vec<NodeLoad>,
}

// ---------------------------------------------------------------------------------------------
// Node decommission (v0.5)
// ---------------------------------------------------------------------------------------------
//
// `POST /api/cluster/nodes/{id}/decommission` (admin) → `DecommissionStatus`: marks the node
// `Decommissioning` (never a rebalance target, leadership drained), plans `Repair`-reason
// moves for all its replicas (needs enough other `Up` nodes for every channel's RF, else 409
// `conflict` with the reason), runs them through the normal rebalance executor (a running plan
// → 409), then removes it from Raft voters/learners and from `cluster/nodes/*`. The process
// may then be stopped; restarting it with the same node id joins as a new empty node.
// `DELETE /api/cluster/nodes/{id}` (admin) forgets a node that is `Down` and holds no
// replicas (409 otherwise) — the path for a dead machine after repair finished.
// `GET /api/cluster/nodes/{id}/decommission` (viewer) → progress. Removing the controller
// first transfers leadership (it steps down after its replicas are gone). Removing below 1
// voter or below a Raft quorum of the remaining configured voters is refused (409).
// Status lives in meta `cluster/decommission/<id>` and stays readable for 24 h after it ends;
// `GET /api/cluster/decommissions` (viewer) lists all current and recent ones. Cancel =
// `POST /api/cluster/rebalance/cancel` while `moving_replicas`: in-flight moves finish, the
// decommission ends `failed` ("cancelled"), the node returns to `Up` (replicas already moved
// stay moved). Any `failed` decommission returns the node to `Up`. Events: the rebalance
// events cover the moves; start/finish are audit entries (`node.decommission`).

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecommissionState {
    /// Moving replicas away (see the rebalance plan).
    MovingReplicas,
    /// Replicas gone; leaving Raft membership.
    LeavingMembership,
    /// Removed from the cluster; safe to stop the process.
    Removed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecommissionStatus {
    pub node: String,
    pub state: DecommissionState,
    /// Replicas still on the node.
    pub replicas_remaining: u32,
    /// Replicas on the node when the decommission started (progress baseline).
    pub initial_replicas: u32,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
}

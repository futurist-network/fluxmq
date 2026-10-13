//! Cluster HTTP surface (stage 9).
//!
//! Public (RBAC table in `auth.rs`):
//! * `GET /api/cluster/metadata` — topology + assignments ([`ClusterMetadata`]).
//! * `GET /api/cluster/status` — plus Raft and replication health; partition replication
//!   status is gathered from every node (each reports the partitions it leads).
//! * `POST /api/cluster/partitions/reassign` (admin), `POST /api/cluster/nodes/{id}/drain`
//!   (admin).
//! * v0.4 rebalancing: `GET /api/cluster/rebalance` (viewer), `PUT …/rebalance/config`,
//!   `POST …/rebalance` (dry run / channels), `POST …/rebalance/{pause,resume,cancel}`
//!   (admin). Executed by the controller (other nodes forward through the cluster RPC, like
//!   reassign / drain). Single node: `GET` reports mode off / idle; mutations answer 400
//!   `unsupported_single_node`.
//! * v0.5 node removal: `POST /api/cluster/nodes/{id}/decommission` (admin) →
//!   [`DecommissionStatus`], `GET …/decommission` (viewer, from the replicated record on any
//!   node), `DELETE /api/cluster/nodes/{id}` (admin, forget a down node holding no replicas).
//!   Mutations run on the controller (forwarded like drain); refusals are 409 `conflict`.
//!   Single node: 400 `unsupported_single_node`.
//!
//! Internal, node-to-node (`/internal/v1/*`, cluster-secret header, outside RBAC):
//! replication status, forwarded publishes, and the per-node halves of fan-out reads —
//! messages / message detail / DLQ list, summary and actions — each answering only for the
//! partitions the node currently leads, so replicated copies on followers are never counted
//! twice. The public messages and DLQ handlers call [`fanout_messages`] & co. when clustered.

use std::collections::{BTreeMap, BTreeSet};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use fluxmq_cluster::rpc::{
    FORWARD_CONTENT_TYPE, FORWARD_PUBLISH_BATCH_PATH, FORWARD_PUBLISH_PATH, ForwardPublish,
    ForwardPublishBatch, secret_matches,
};
use fluxmq_cluster::{CLUSTER_SECRET_HEADER, ClusterError, PartitionRoute};
use fluxmq_core::DlqSelector;
use fluxmq_proto::MessageId;
use fluxmq_proto::api::ERROR_UNSUPPORTED_SINGLE_NODE;
use fluxmq_proto::api::{
    DeadLetterView, DlqActionRequest, DlqActionResponse, DlqPage, DlqQuery, DlqReasonGroup,
    DlqSummary, MessageDetail, MessagePage, MessageQuery,
};
use fluxmq_proto::cluster::{
    ClusterMetadata, ClusterStatus, DecommissionStatus, DrainResponse, PartitionReplicationStatus,
    ReassignRequest, RebalanceConfig, RebalanceRequest, RebalanceStatus,
};
use fluxmq_proto::wire::{ErrorCode, WireError};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Json, OptionalJson, Path, Query};
use crate::payload;
use crate::state::AppState;

pub const REPLICATION_STATUS_PATH: &str = "/internal/v1/replication-status";
const DLQ_SCAN_CAP: usize = 100_000;

impl From<ClusterError> for ApiErr {
    fn from(e: ClusterError) -> Self {
        match e {
            ClusterError::Core(c) => c.into(),
            ClusterError::Invalid(m) => ApiErr::bad_request(m),
            ClusterError::Conflict(m) => ApiErr::conflict(m),
            ClusterError::NotFound(m) => ApiErr::not_found(m),
            ClusterError::NotController(_) | ClusterError::Meta(_) | ClusterError::Raft(_) => {
                ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "unavailable",
                    e.to_string(),
                )
            }
            ClusterError::Io(_) => ApiErr::internal(e.to_string()),
        }
    }
}

/// HTTP mapping of a wire error from a cluster publish.
pub fn wire_to_api(e: WireError) -> ApiErr {
    match e.code {
        ErrorCode::BadRequest => ApiErr::bad_request(e.message),
        ErrorCode::NotFound => ApiErr::not_found(e.message),
        ErrorCode::AlreadyExists => ApiErr::conflict(e.message),
        ErrorCode::NotLeader => unavailable(if e.message.is_empty() {
            "partition leader unavailable (election in progress); retry".to_string()
        } else {
            format!("partition led by {}; retry", e.message)
        }),
        ErrorCode::Overloaded => unavailable(e.message),
        _ => ApiErr::internal(e.message),
    }
}

fn unavailable(msg: impl Into<String>) -> ApiErr {
    ApiErr::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", msg)
}

// ---------------------------------------------------------------------------------------------
// Public
// ---------------------------------------------------------------------------------------------

pub async fn metadata(State(s): State<AppState>) -> axum::Json<ClusterMetadata> {
    axum::Json(s.cluster.metadata(&[]))
}

pub async fn status(State(s): State<AppState>) -> axum::Json<ClusterStatus> {
    let mut status = s.cluster.status();
    if s.cluster.is_clustered() {
        let http = s.cluster.internal_http();
        let calls = s.cluster.peers().into_iter().map(|n| {
            let http = http.clone();
            async move {
                http.get::<_, Vec<PartitionReplicationStatus>>(
                    &n.http_addr,
                    REPLICATION_STATUS_PATH,
                    &(),
                )
                .await
                .ok()
                .map(|rows| (n.id, rows))
            }
        });
        // Every node reports every partition it holds, followers included (with `leader` =
        // the leader they follow). Only a node's rows for partitions it leads itself carry
        // the ISR and follower lag: a follower row that won the dedup below showed a stale
        // LEO and an empty ISR while a replica caught up.
        let me = s.cluster.node_id().to_string();
        // (Rows without a leader stay: an offline partition is reported that way.)
        status
            .partitions
            .retain(|p| p.leader.as_deref().is_none_or(|l| l == me));
        for (node, rows) in futures::future::join_all(calls).await.into_iter().flatten() {
            status.partitions.extend(
                rows.into_iter()
                    .filter(|p| p.leader.as_deref().is_none_or(|l| l == node)),
            );
        }
        // Each partition is reported by its leader; keep one row per partition.
        let leaders: BTreeMap<(String, u32), Option<String>> = status
            .metadata
            .channels
            .iter()
            .flat_map(|(c, parts)| {
                parts
                    .iter()
                    .map(move |p| ((c.clone(), p.partition), p.leader.clone()))
            })
            .collect();
        status.partitions.retain(|p| {
            leaders
                .get(&(p.channel.clone(), p.partition))
                .is_none_or(|l| l.as_ref() == p.leader.as_ref())
        });
        status
            .partitions
            .sort_by(|a, b| (&a.channel, a.partition).cmp(&(&b.channel, b.partition)));
        status
            .partitions
            .dedup_by(|a, b| a.channel == b.channel && a.partition == b.partition);
    }
    axum::Json(status)
}

pub async fn reassign(
    State(s): State<AppState>,
    Json(req): Json<ReassignRequest>,
) -> ApiResult<axum::Json<ClusterMetadata>> {
    if !s.cluster.is_clustered() {
        return Err(ApiErr::bad_request("not clustered"));
    }
    s.cluster
        .reassign(&req.channel, req.partition, req.replicas.clone())
        .await?;
    s.audit(
        "cluster.reassign",
        &format!("{}/{}", req.channel, req.partition),
        json!({ "replicas": req.replicas }),
    );
    Ok(axum::Json(s.cluster.metadata(&[req.channel])))
}

pub async fn drain(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<DrainResponse>> {
    if !s.cluster.is_clustered() {
        return Err(ApiErr::bad_request("not clustered"));
    }
    let moved = s.cluster.drain(&id).await?;
    s.audit("cluster.drain", &id, json!({ "moved_leaderships": moved }));
    Ok(axum::Json(DrainResponse {
        moved_leaderships: moved,
    }))
}

// ---------------------------------------------------------------------------------------------
// Rebalancing (v0.4)
// ---------------------------------------------------------------------------------------------

fn single_node_unsupported() -> ApiErr {
    ApiErr::new(
        StatusCode::BAD_REQUEST,
        ERROR_UNSUPPORTED_SINGLE_NODE,
        "rebalancing needs a cluster; this broker runs as a single node",
    )
}

pub async fn rebalance_status(State(s): State<AppState>) -> axum::Json<RebalanceStatus> {
    axum::Json(s.cluster.rebalance_status().await)
}

pub async fn rebalance_config(
    State(s): State<AppState>,
    Json(cfg): Json<RebalanceConfig>,
) -> ApiResult<axum::Json<RebalanceStatus>> {
    if !s.cluster.is_clustered() {
        return Err(single_node_unsupported());
    }
    let status = s.cluster.set_rebalance_config(cfg.clone()).await?;
    s.audit(
        "cluster.rebalance.config",
        "rebalance",
        serde_json::to_value(&cfg).unwrap_or_default(),
    );
    Ok(axum::Json(status))
}

pub async fn rebalance(
    State(s): State<AppState>,
    OptionalJson(body): OptionalJson<RebalanceRequest>,
) -> ApiResult<axum::Json<RebalanceStatus>> {
    if !s.cluster.is_clustered() {
        return Err(single_node_unsupported());
    }
    let req = body.unwrap_or_default();
    let status = s.cluster.rebalance(req.clone()).await?;
    if !req.dry_run {
        s.audit(
            "cluster.rebalance",
            "rebalance",
            json!({ "channels": req.channels, "moves": status.moves.len() }),
        );
    }
    Ok(axum::Json(status))
}

async fn rebalance_action(
    s: AppState,
    action: fluxmq_cluster::RebalanceAction,
    what: &str,
) -> ApiResult<axum::Json<RebalanceStatus>> {
    if !s.cluster.is_clustered() {
        return Err(single_node_unsupported());
    }
    let status = s.cluster.rebalance_control(action).await?;
    s.audit(what, "rebalance", json!({}));
    Ok(axum::Json(status))
}

pub async fn rebalance_pause(State(s): State<AppState>) -> ApiResult<axum::Json<RebalanceStatus>> {
    rebalance_action(
        s,
        fluxmq_cluster::RebalanceAction::Pause,
        "cluster.rebalance.pause",
    )
    .await
}

pub async fn rebalance_resume(State(s): State<AppState>) -> ApiResult<axum::Json<RebalanceStatus>> {
    rebalance_action(
        s,
        fluxmq_cluster::RebalanceAction::Resume,
        "cluster.rebalance.resume",
    )
    .await
}

pub async fn rebalance_cancel(State(s): State<AppState>) -> ApiResult<axum::Json<RebalanceStatus>> {
    rebalance_action(
        s,
        fluxmq_cluster::RebalanceAction::Cancel,
        "cluster.rebalance.cancel",
    )
    .await
}

// ---------------------------------------------------------------------------------------------
// Decommission / forget (v0.5)
// ---------------------------------------------------------------------------------------------

fn single_node_no_decommission() -> ApiErr {
    ApiErr::new(
        StatusCode::BAD_REQUEST,
        ERROR_UNSUPPORTED_SINGLE_NODE,
        "node removal needs a cluster; this broker runs as a single node",
    )
}

#[derive(Debug, Default, Deserialize)]
pub struct DecommissionQuery {
    #[serde(default)]
    pub dry_run: bool,
}

/// `POST …/decommission` → `DecommissionStatus`; with `?dry_run=true` → the `RebalanceStatus`
/// of the plan it would run (same checks, nothing written).
pub async fn decommission(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<DecommissionQuery>,
) -> ApiResult<Response> {
    if !s.cluster.is_clustered() {
        return Err(single_node_no_decommission());
    }
    if q.dry_run {
        let plan = s.cluster.decommission_dry_run(&id).await?;
        return Ok(axum::Json(plan).into_response());
    }
    let status = s.cluster.decommission(&id).await?;
    s.audit(
        "node.decommission",
        &id,
        json!({ "phase": "start", "replicas": status.initial_replicas }),
    );
    Ok(axum::Json(status).into_response())
}

/// `GET /api/cluster/decommissions`: current and recent (24 h) decommissions, newest first.
pub async fn decommissions(State(s): State<AppState>) -> axum::Json<Vec<DecommissionStatus>> {
    axum::Json(s.cluster.decommissions())
}

pub async fn decommission_status(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<DecommissionStatus>> {
    if !s.cluster.is_clustered() {
        return Err(single_node_no_decommission());
    }
    Ok(axum::Json(s.cluster.decommission_status(&id)?))
}

pub async fn forget_node(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<DecommissionStatus>> {
    if !s.cluster.is_clustered() {
        return Err(single_node_no_decommission());
    }
    let status = s.cluster.forget_node(&id).await?;
    s.audit(
        "node.decommission",
        &id,
        json!({ "phase": "forget", "state": status.state }),
    );
    Ok(axum::Json(status))
}

// ---------------------------------------------------------------------------------------------
// Internal routes + auth
// ---------------------------------------------------------------------------------------------

/// `/internal/v1/*` (incl. telemetry's node-stats), behind the cluster-secret check.
pub fn internal(state: AppState) -> Router<AppState> {
    Router::new()
        .route(REPLICATION_STATUS_PATH, get(replication_status))
        .route(FORWARD_PUBLISH_PATH, post(forward_publish))
        .route(FORWARD_PUBLISH_BATCH_PATH, post(forward_publish_batch))
        .route(
            "/internal/v1/channels/{name}/messages",
            get(internal_messages),
        )
        .route(
            "/internal/v1/channels/{name}/messages/{partition}/{offset}",
            get(internal_detail),
        )
        .route(
            "/internal/v1/channels/{name}/groups/{group}/reset",
            post(internal_group_reset),
        )
        .route(
            "/internal/v1/channels/{name}/groups/{group}",
            axum::routing::delete(internal_group_delete),
        )
        .route("/internal/v1/channels/{name}/read", post(internal_read_ids))
        .route("/internal/v1/dlq", get(internal_dlq_list))
        .route("/internal/v1/dlq/summary", get(internal_dlq_summary))
        .route("/internal/v1/dlq/retry", post(internal_dlq_retry))
        .route("/internal/v1/dlq/discard", post(internal_dlq_discard))
        .merge(super::metrics::internal())
        .merge(super::webhooks::internal())
        .merge(super::live_status::internal())
        .merge(super::scheduled::internal())
        .merge(super::notify::internal())
        .merge(super::backups::internal())
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            require_cluster_secret,
        ))
}

/// The controller node when this node is clustered and is not the controller itself
/// (`None` on a single node, on the controller, or while no controller is known).
pub fn remote_controller(s: &AppState) -> Option<fluxmq_proto::cluster::NodeInfo> {
    if !s.cluster.is_clustered() {
        return None;
    }
    // A channel filter matching nothing: only the topology is needed.
    let md = s.cluster.metadata(&["\0".to_string()]);
    let controller = md.controller?;
    if controller == s.cluster.node_id() {
        return None;
    }
    md.nodes.into_iter().find(|n| n.id == controller)
}

/// Constant-time check of [`CLUSTER_SECRET_HEADER`]; 404 on a single node.
pub async fn require_cluster_secret(
    State(s): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    if !s.cluster.is_clustered() {
        // Single node: there are no internal endpoints.
        return ApiErr::not_found("not clustered").into_response();
    }
    let presented = headers
        .get(CLUSTER_SECRET_HEADER)
        .and_then(|v| v.to_str().ok());
    if secret_matches(presented, s.cluster.cluster_secret()) {
        next.run(req).await
    } else {
        ApiErr::unauthorized("invalid or missing cluster secret").into_response()
    }
}

async fn replication_status(
    State(s): State<AppState>,
) -> axum::Json<Vec<PartitionReplicationStatus>> {
    axum::Json(s.cluster.status().partitions)
}

async fn forward_publish(State(s): State<AppState>, body: Bytes) -> Response {
    let result: Result<Vec<MessageId>, WireError> = async {
        let req: ForwardPublish = postcard::from_bytes(&body)
            .map_err(|e| WireError::new(ErrorCode::BadRequest, e.to_string()))?;
        match s.cluster.route_partition(&req.channel, req.partition) {
            PartitionRoute::Local => {}
            PartitionRoute::Remote(n) => return Err(WireError::new(ErrorCode::NotLeader, n.id)),
            PartitionRoute::Unavailable => return Err(WireError::new(ErrorCode::NotLeader, "")),
        }
        s.broker
            .publish_to_partition(&req.channel, req.partition, req.messages)
            .await
            .map_err(|e| WireError::from(&e))
    }
    .await;
    match postcard::to_allocvec(&result) {
        Ok(bytes) => ([(header::CONTENT_TYPE, FORWARD_CONTENT_TYPE)], bytes).into_response(),
        Err(e) => ApiErr::internal(e.to_string()).into_response(),
    }
}

/// v0.7: a partition-less `PublishBatch` forwarded raw by another node. Same leader rules
/// as a client `PublishBatch { partition: Some(p) }` (`NotLeader` when not the leader).
async fn forward_publish_batch(State(s): State<AppState>, body: Bytes) -> Response {
    let result: Result<(u64, u32), WireError> = async {
        let req: ForwardPublishBatch = postcard::from_bytes(&body)
            .map_err(|e| WireError::new(ErrorCode::BadRequest, e.to_string()))?;
        s.cluster
            .publish_batch(&req.channel, Some(req.partition), req.records)
            .await
            .map(|(_, base, count)| (base, count))
    }
    .await;
    match postcard::to_allocvec(&result) {
        Ok(bytes) => ([(header::CONTENT_TYPE, FORWARD_CONTENT_TYPE)], bytes).into_response(),
        Err(e) => ApiErr::internal(e.to_string()).into_response(),
    }
}

/// Partitions of `channel` this node currently leads.
/// Partitions of `channel` this node currently serves as leader (holds a lease, has the
/// channel locally).
fn led_partitions(s: &AppState, channel: &str) -> BTreeSet<u32> {
    let me = s.cluster.node_id();
    s.cluster
        .metadata(&[channel.to_string()])
        .channels
        .get(channel)
        .map(|parts| {
            parts
                .iter()
                .filter(|p| p.leader.as_deref() == Some(me))
                .map(|p| p.partition)
                .filter(|p| s.cluster.route_partition(channel, *p) == PartitionRoute::Local)
                .collect()
        })
        .unwrap_or_default()
}

/// Internal answer of a fan-out messages read: the page plus the partitions it covers.
#[derive(Debug, Serialize, Deserialize)]
pub struct LedPage {
    pub page: MessagePage,
    pub partitions: BTreeSet<u32>,
}

/// (channel, partition) pairs this node leads, for every channel.
fn led_all(s: &AppState) -> BTreeSet<(String, u32)> {
    let me = s.cluster.node_id();
    s.cluster
        .metadata(&[])
        .channels
        .into_iter()
        .flat_map(|(c, parts)| {
            parts
                .into_iter()
                .filter(|p| p.leader.as_deref() == Some(me))
                .map(move |p| (c.clone(), p.partition))
        })
        .filter(|(c, p)| s.cluster.route_partition(c, *p) == PartitionRoute::Local)
        .collect()
}

/// Merges per-partition pages into one page honouring `limit` and `newest_first`.
fn merge_pages(pages: Vec<MessagePage>, q: &MessageQuery) -> MessagePage {
    let limit = q.limit.unwrap_or(super::messages::DEFAULT_LIMIT) as usize;
    let mut scanned = 0;
    let mut has_more = false;
    let mut all = Vec::new();
    for p in pages {
        scanned += p.scanned;
        has_more |= p.has_more;
        all.extend(p.messages);
    }
    all.sort_by_key(|m| (m.timestamp_ms, m.partition, m.offset));
    if all.len() > limit {
        has_more = true;
        if q.newest_first.unwrap_or(false) {
            all.drain(..all.len() - limit);
        } else {
            all.truncate(limit);
        }
    }
    MessagePage {
        messages: all,
        scanned,
        has_more,
    }
}

/// Local half of a fan-out read: query each led partition (restricted to `q.partition`).
async fn local_led_messages(s: &AppState, name: &str, q: &MessageQuery) -> ApiResult<LedPage> {
    let mut pages = Vec::new();
    let mut partitions = BTreeSet::new();
    for p in led_partitions(s, name) {
        if q.partition.is_some_and(|want| want != p) {
            continue;
        }
        partitions.insert(p);
        let mut qp = q.clone();
        qp.partition = Some(p);
        let page =
            super::messages::query_local(State(s.clone()), Path(name.to_string()), Query(qp))
                .await?
                .0;
        pages.push(page);
    }
    Ok(LedPage {
        page: merge_pages(pages, q),
        partitions,
    })
}

async fn internal_messages(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<MessageQuery>,
) -> ApiResult<axum::Json<LedPage>> {
    Ok(axum::Json(local_led_messages(&s, &name, &q).await?))
}

async fn internal_detail(
    State(s): State<AppState>,
    Path((name, partition, offset)): Path<(String, u32, u64)>,
) -> ApiResult<axum::Json<MessageDetail>> {
    super::messages::detail_local(State(s), Path((name, partition, offset))).await
}

/// Public messages query in a cluster: every node leading a requested partition answers for
/// its partitions; results are merged.
pub async fn fanout_messages(
    s: AppState,
    name: String,
    q: MessageQuery,
) -> ApiResult<axum::Json<MessagePage>> {
    let md = s.cluster.metadata(std::slice::from_ref(&name));
    let Some(parts) = md.channels.get(&name) else {
        return super::messages::query_local(State(s), Path(name), Query(q)).await;
    };
    let me = s.cluster.node_id().to_string();
    let mut nodes: BTreeSet<String> = BTreeSet::new();
    let mut expected: BTreeSet<u32> = BTreeSet::new();
    let mut offline = false;
    for p in parts {
        if q.partition.is_some_and(|want| want != p.partition) {
            continue;
        }
        match &p.leader {
            Some(l) => {
                nodes.insert(l.clone());
                expected.insert(p.partition);
            }
            None => offline = true,
        }
    }
    if q.partition.is_some() && nodes.is_empty() && !offline {
        // Unknown partition: the local broker reports it.
        return super::messages::query_local(State(s), Path(name), Query(q)).await;
    }
    let http = s.cluster.internal_http();
    let path = format!("/internal/v1/channels/{name}/messages");
    let calls = nodes.into_iter().map(|node| {
        let (s, http, q, name, path, me) = (
            s.clone(),
            http.clone(),
            q.clone(),
            name.clone(),
            path.clone(),
            me.clone(),
        );
        async move {
            if node == me {
                return local_led_messages(&s, &name, &q).await;
            }
            let addr = s
                .cluster
                .node(&node)
                .map(|n| n.http_addr)
                .ok_or_else(|| unavailable(format!("unknown node '{node}'")))?;
            http.get::<_, LedPage>(&addr, &path, &q)
                .await
                .map_err(|e| unavailable(format!("node {node}: {e}")))
        }
    });
    let mut pages = Vec::new();
    let mut covered = BTreeSet::new();
    for r in futures::future::join_all(calls).await {
        let led = r?;
        covered.extend(led.partitions);
        pages.push(led.page);
    }
    if !expected.is_subset(&covered) {
        // Leadership moved (or a leader is fenced) between metadata and the reads.
        let missing: Vec<u32> = expected.difference(&covered).copied().collect();
        return Err(unavailable(format!(
            "partitions {missing:?} of '{name}' are changing leaders; retry"
        )));
    }
    let mut page = merge_pages(pages, &q);
    page.has_more |= offline;
    Ok(axum::Json(page))
}

/// Public message detail in a cluster: served by the partition leader.
pub async fn fanout_detail(
    s: AppState,
    name: String,
    partition: u32,
    offset: u64,
) -> ApiResult<axum::Json<MessageDetail>> {
    match s.cluster.route_partition(&name, partition) {
        PartitionRoute::Local => {
            super::messages::detail_local(State(s), Path((name, partition, offset))).await
        }
        PartitionRoute::Unavailable => Err(unavailable(format!(
            "partition {partition} of '{name}' has no leader right now"
        ))),
        PartitionRoute::Remote(n) => {
            let path = format!("/internal/v1/channels/{name}/messages/{partition}/{offset}");
            match s
                .cluster
                .internal_http()
                .get::<_, MessageDetail>(&n.http_addr, &path, &())
                .await
            {
                Ok(d) => Ok(axum::Json(d)),
                Err(fluxmq_cluster::TransportError::Status(404, _)) => Err(ApiErr::not_found(
                    format!("message {partition}/{offset} not found in channel '{name}'"),
                )),
                Err(e) => Err(unavailable(format!("node {}: {e}", n.id))),
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// DLQ fan-out
// ---------------------------------------------------------------------------------------------

/// (channel, partition) pairs, the unit of DLQ coverage.
pub type PartSet = BTreeSet<(String, u32)>;

/// A per-node answer of a fan-out DLQ call plus the partitions it covers.
#[derive(Debug, Serialize, Deserialize)]
pub struct Led<T> {
    pub data: T,
    pub partitions: PartSet,
}

/// Who must answer a fan-out over `channel` (every channel when `None`): the partition
/// leaders, and the partitions they must cover between them. Offline partitions (no leader)
/// are skipped — their dead letters are unavailable until the partition is back.
pub struct LeaderPlan {
    pub nodes: BTreeSet<String>,
    pub expected: PartSet,
}

pub fn leader_plan(s: &AppState, channel: Option<&str>) -> LeaderPlan {
    let names: Vec<String> = channel.map(|c| vec![c.to_string()]).unwrap_or_default();
    let mut plan = LeaderPlan {
        nodes: BTreeSet::new(),
        expected: BTreeSet::new(),
    };
    for (c, parts) in s.cluster.metadata(&names).channels {
        if channel.is_some_and(|want| want != c) {
            continue;
        }
        for p in parts {
            if let Some(l) = p.leader {
                plan.nodes.insert(l);
                plan.expected.insert((c.clone(), p.partition));
            }
        }
    }
    plan
}

/// 503 unless the answers covered every expected partition (leadership moved, or a leader is
/// fenced, between reading the metadata and the calls) — same rule as message reads.
pub fn check_coverage(expected: &PartSet, covered: &PartSet) -> ApiResult<()> {
    let missing: Vec<String> = expected
        .difference(covered)
        .map(|(c, p)| format!("{c}/{p}"))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(unavailable(format!(
            "partitions {} are changing leaders; retry",
            missing.join(", ")
        )))
    }
}

/// HTTP address of cluster node `node`.
fn addr_of(s: &AppState, node: &str) -> ApiResult<String> {
    s.cluster
        .node(node)
        .map(|n| n.http_addr)
        .ok_or_else(|| unavailable(format!("unknown node '{node}'")))
}

/// Calls `path` on every node of `nodes` except this one (in parallel). A leader that can't
/// be reached is a 503 (it still leads partitions per the metadata).
async fn on_leaders<Q, R>(
    s: &AppState,
    nodes: &BTreeSet<String>,
    path: &str,
    query: Option<&Q>,
    body: Option<&Q>,
) -> ApiResult<Vec<R>>
where
    Q: Serialize + Sync,
    R: for<'de> Deserialize<'de>,
{
    let http = s.cluster.internal_http();
    let me = s.cluster.node_id();
    let calls = nodes.iter().filter(|n| n.as_str() != me).map(|node| {
        let http = http.clone();
        async move {
            let addr = addr_of(s, node)?;
            let r = match (body, query) {
                (Some(b), _) => http.post::<_, R>(&addr, path, b).await,
                (None, Some(q)) => http.get::<_, R>(&addr, path, q).await,
                (None, None) => http.get::<_, R>(&addr, path, &()).await,
            };
            r.map_err(|e| peer_err(node, e))
        }
    });
    futures::future::join_all(calls).await.into_iter().collect()
}

/// Dead letters of led partitions matching `q` (newest first, at most `cap`), the matching
/// total, and the led partitions (restricted to `q.channel`) they cover.
pub async fn local_led_dlq(
    s: &AppState,
    q: &DlqQuery,
    cap: usize,
) -> ApiResult<(Vec<DeadLetterView>, u64, PartSet)> {
    let mut led = led_all(s);
    if let Some(c) = &q.channel {
        led.retain(|(lc, _)| lc == c);
    }
    let broker = s.broker.clone();
    let (channel, reason, before) = (q.channel.clone(), q.reason.clone(), q.before);
    let (entries, _) = tokio::task::spawn_blocking(move || {
        broker.dlq_list(channel.as_deref(), reason.as_deref(), before, DLQ_SCAN_CAP)
    })
    .await?;
    let mine: Vec<_> = entries
        .into_iter()
        .filter(|e| led.contains(&(e.channel.clone(), e.message.partition)))
        .collect();
    let total = mine.len() as u64;
    Ok((
        mine.into_iter()
            .take(cap)
            .map(payload::dead_letter_view)
            .collect(),
        total,
        led,
    ))
}

async fn internal_dlq_list(
    State(s): State<AppState>,
    Query(q): Query<DlqQuery>,
) -> ApiResult<axum::Json<Led<DlqPage>>> {
    let limit = q
        .limit
        .unwrap_or(super::dlq::DEFAULT_LIMIT)
        .clamp(1, super::dlq::MAX_LIMIT) as usize;
    let (entries, total, partitions) = local_led_dlq(&s, &q, limit).await?;
    Ok(axum::Json(Led {
        data: DlqPage { entries, total },
        partitions,
    }))
}

fn summarize(entries: &[DeadLetterView]) -> DlqSummary {
    let mut groups: BTreeMap<String, DlqReasonGroup> = BTreeMap::new();
    for e in entries {
        let g = groups
            .entry(e.reason.clone())
            .or_insert_with(|| DlqReasonGroup {
                reason: e.reason.clone(),
                count: 0,
                channels: BTreeMap::new(),
                first_ms: e.dead_at_ms,
                last_ms: e.dead_at_ms,
            });
        g.count += 1;
        *g.channels.entry(e.channel.clone()).or_insert(0) += 1;
        g.first_ms = g.first_ms.min(e.dead_at_ms);
        g.last_ms = g.last_ms.max(e.dead_at_ms);
    }
    merge_summaries(vec![DlqSummary {
        total: entries.len() as u64,
        groups: groups.into_values().collect(),
    }])
}

fn merge_summaries(parts: Vec<DlqSummary>) -> DlqSummary {
    let mut groups: BTreeMap<String, DlqReasonGroup> = BTreeMap::new();
    let mut total = 0;
    for s in parts {
        total += s.total;
        for g in s.groups {
            match groups.get_mut(&g.reason) {
                Some(m) => {
                    m.count += g.count;
                    for (c, n) in g.channels {
                        *m.channels.entry(c).or_insert(0) += n;
                    }
                    m.first_ms = m.first_ms.min(g.first_ms);
                    m.last_ms = m.last_ms.max(g.last_ms);
                }
                None => {
                    groups.insert(g.reason.clone(), g);
                }
            }
        }
    }
    let mut groups: Vec<DlqReasonGroup> = groups.into_values().collect();
    groups.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));
    DlqSummary { total, groups }
}

async fn internal_dlq_summary(State(s): State<AppState>) -> ApiResult<axum::Json<Led<DlqSummary>>> {
    let (entries, _, partitions) = local_led_dlq(&s, &DlqQuery::default(), DLQ_SCAN_CAP).await?;
    Ok(axum::Json(Led {
        data: summarize(&entries),
        partitions,
    }))
}

/// Applies a DLQ action to the matching entries of partitions this node leads.
async fn local_dlq_action(
    s: &AppState,
    req: DlqActionRequest,
    retry: bool,
) -> ApiResult<(u64, PartSet)> {
    let sel = super::dlq::selector(req)?;
    let q = match &sel {
        DlqSelector::Reason { reason, channel } => DlqQuery {
            channel: channel.clone(),
            reason: Some(reason.clone()),
            ..Default::default()
        },
        DlqSelector::Channel(c) => DlqQuery {
            channel: Some(c.clone()),
            ..Default::default()
        },
        DlqSelector::Ids(_) | DlqSelector::All => DlqQuery::default(),
    };
    let (mine, _, led) = local_led_dlq(s, &q, DLQ_SCAN_CAP).await?;
    let wanted: Option<BTreeSet<(String, u64)>> = match &sel {
        DlqSelector::Ids(ids) => Some(ids.iter().cloned().collect()),
        _ => None,
    };
    let ids: Vec<(String, u64)> = mine
        .into_iter()
        .map(|e| (e.channel, e.id))
        .filter(|k| wanted.as_ref().is_none_or(|w| w.contains(k)))
        .collect();
    if ids.is_empty() {
        return Ok((0, led));
    }
    let broker = s.broker.clone();
    let n = tokio::task::spawn_blocking(move || {
        if retry {
            broker.dlq_retry(DlqSelector::Ids(ids))
        } else {
            broker.dlq_discard(DlqSelector::Ids(ids))
        }
    })
    .await??;
    Ok((n, led))
}

async fn internal_dlq_retry(
    State(s): State<AppState>,
    Json(req): Json<DlqActionRequest>,
) -> ApiResult<axum::Json<Led<DlqActionResponse>>> {
    let (affected, partitions) = local_dlq_action(&s, req, true).await?;
    Ok(axum::Json(Led {
        data: DlqActionResponse { affected },
        partitions,
    }))
}

async fn internal_dlq_discard(
    State(s): State<AppState>,
    Json(req): Json<DlqActionRequest>,
) -> ApiResult<axum::Json<Led<DlqActionResponse>>> {
    let (affected, partitions) = local_dlq_action(&s, req, false).await?;
    Ok(axum::Json(Led {
        data: DlqActionResponse { affected },
        partitions,
    }))
}

/// Public DLQ list in a cluster: every partition leader answers for its partitions; 503 while
/// some partition's leadership is moving (coverage check).
pub async fn fanout_dlq_list(s: AppState, q: DlqQuery) -> ApiResult<axum::Json<DlqPage>> {
    let limit = q
        .limit
        .unwrap_or(super::dlq::DEFAULT_LIMIT)
        .clamp(1, super::dlq::MAX_LIMIT) as usize;
    let plan = leader_plan(&s, q.channel.as_deref());
    let (mut entries, mut total, mut covered) = local_led_dlq(&s, &q, limit).await?;
    let led: Vec<Led<DlqPage>> =
        on_leaders(&s, &plan.nodes, "/internal/v1/dlq", Some(&q), None).await?;
    for page in led {
        entries.extend(page.data.entries);
        total += page.data.total;
        covered.extend(page.partitions);
    }
    check_coverage(&plan.expected, &covered)?;
    entries.sort_by(|a, b| {
        b.dead_at_ms
            .cmp(&a.dead_at_ms)
            .then_with(|| b.id.cmp(&a.id))
    });
    entries.truncate(limit);
    Ok(axum::Json(DlqPage { entries, total }))
}

pub async fn fanout_dlq_summary(s: AppState) -> ApiResult<axum::Json<DlqSummary>> {
    let plan = leader_plan(&s, None);
    let (entries, _, mut covered) = local_led_dlq(&s, &DlqQuery::default(), DLQ_SCAN_CAP).await?;
    let mut parts = vec![summarize(&entries)];
    let led: Vec<Led<DlqSummary>> =
        on_leaders::<(), _>(&s, &plan.nodes, "/internal/v1/dlq/summary", None, None).await?;
    for l in led {
        parts.push(l.data);
        covered.extend(l.partitions);
    }
    check_coverage(&plan.expected, &covered)?;
    Ok(axum::Json(merge_summaries(parts)))
}

/// Retry/discard on every partition leader. The coverage check runs after the action: a 503
/// means it was applied on the partitions that were reachable — retrying is safe (entries
/// already retried/discarded are gone).
pub async fn fanout_dlq_action(
    s: AppState,
    req: DlqActionRequest,
    retry: bool,
) -> ApiResult<axum::Json<DlqActionResponse>> {
    // Validate once up front (400 before touching any node).
    super::dlq::selector(req.clone())?;
    let channel = match (&req.ids, &req.channel) {
        (None, Some(c)) => Some(c.clone()),
        _ => None,
    };
    let plan = leader_plan(&s, channel.as_deref());
    let (mut affected, mut covered) = local_dlq_action(&s, req.clone(), retry).await?;
    let path = if retry {
        "/internal/v1/dlq/retry"
    } else {
        "/internal/v1/dlq/discard"
    };
    let led: Vec<Led<DlqActionResponse>> =
        on_leaders(&s, &plan.nodes, path, None, Some(&req)).await?;
    for r in led {
        affected += r.data.affected;
        covered.extend(r.partitions);
    }
    if channel.is_some() {
        covered.retain(|(c, _)| Some(c) == channel.as_ref());
    }
    check_coverage(&plan.expected, &covered)?;
    Ok(axum::Json(DlqActionResponse { affected }))
}

/// One page of `channel`'s dead letters from partition leader `node` (newest first, ids below
/// `before`), checked to cover every partition of `channel` that `node` leads per `plan`.
pub async fn led_dlq_page(
    s: &AppState,
    node: &str,
    q: &DlqQuery,
    expected: &PartSet,
) -> ApiResult<Vec<DeadLetterView>> {
    let (entries, covered) = if node == s.cluster.node_id() {
        let limit = q.limit.unwrap_or(super::dlq::DEFAULT_LIMIT) as usize;
        let (e, _, led) = local_led_dlq(s, q, limit).await?;
        (e, led)
    } else {
        let addr = addr_of(s, node)?;
        let led: Led<DlqPage> = s
            .cluster
            .internal_http()
            .get(&addr, "/internal/v1/dlq", q)
            .await
            .map_err(|e| peer_err(node, e))?;
        (led.data.entries, led.partitions)
    };
    check_coverage(expected, &covered)?;
    Ok(entries)
}

// ---------------------------------------------------------------------------------------------
// Replay (reads on partition leaders, publish through the cluster publish path)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct ReadIds {
    ids: Vec<MessageId>,
}

/// Messages a leader read for a replay, plus the partitions it leads (coverage).
#[derive(Debug, Serialize, Deserialize)]
struct LedMessages {
    messages: Vec<fluxmq_proto::api::MessageView>,
    partitions: BTreeSet<u32>,
}

async fn local_read_ids(s: &AppState, name: &str, ids: Vec<MessageId>) -> ApiResult<LedMessages> {
    let led = led_partitions(s, name);
    let ids: Vec<MessageId> = ids
        .into_iter()
        .filter(|id| led.contains(&id.partition))
        .collect();
    let broker = s.broker.clone();
    let channel = name.to_string();
    let msgs = tokio::task::spawn_blocking(move || broker.read_by_ids(&channel, &ids)).await??;
    Ok(LedMessages {
        messages: msgs.into_iter().map(payload::message_view).collect(),
        partitions: led,
    })
}

async fn internal_read_ids(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<ReadIds>,
) -> ApiResult<axum::Json<LedMessages>> {
    Ok(axum::Json(local_read_ids(&s, &name, req.ids).await?))
}

/// `POST /api/channels/{name}/replay` in a cluster: each partition leader reads the requested
/// messages of its partitions (only offsets below the HW, like every read); the copies are
/// then published to the target through the cluster publish path (partition leaders,
/// `acks=all`). Missing ids are skipped, as on a single node; 503 while a needed partition
/// has no leader or its leadership moves.
pub async fn fanout_replay(
    s: &AppState,
    name: &str,
    ids: &[MessageId],
    target: &str,
) -> ApiResult<Vec<MessageId>> {
    let parts = s
        .cluster
        .metadata(&[name.to_string()])
        .channels
        .remove(name)
        .unwrap_or_default();
    let mut by_node: BTreeMap<String, Vec<MessageId>> = BTreeMap::new();
    let mut expected = BTreeSet::new();
    for id in ids {
        // Unknown partitions are skipped (like missing offsets).
        let Some(p) = parts.iter().find(|p| p.partition == id.partition) else {
            continue;
        };
        let Some(leader) = &p.leader else {
            return Err(unavailable(format!(
                "partition {} of '{name}' has no leader right now",
                id.partition
            )));
        };
        by_node.entry(leader.clone()).or_default().push(*id);
        expected.insert(id.partition);
    }
    let me = s.cluster.node_id().to_string();
    let http = s.cluster.internal_http();
    let path = format!("/internal/v1/channels/{name}/read");
    let calls = by_node.into_iter().map(|(node, ids)| {
        let (http, path, me) = (http.clone(), path.clone(), me.clone());
        async move {
            if node == me {
                return local_read_ids(s, name, ids).await;
            }
            http.post::<_, LedMessages>(&addr_of(s, &node)?, &path, &ReadIds { ids })
                .await
                .map_err(|e| peer_err(&node, e))
        }
    });
    let mut found: BTreeMap<(u32, u64), fluxmq_proto::api::MessageView> = BTreeMap::new();
    let mut covered = BTreeSet::new();
    for r in futures::future::join_all(calls).await {
        let led = r?;
        covered.extend(led.partitions);
        for m in led.messages {
            found.insert((m.partition, m.offset), m);
        }
    }
    if !expected.is_subset(&covered) {
        let missing: Vec<u32> = expected.difference(&covered).copied().collect();
        return Err(unavailable(format!(
            "partitions {missing:?} of '{name}' are changing leaders; retry"
        )));
    }
    // Request order, like `Broker::replay`.
    let mut copies = Vec::new();
    for id in ids {
        if let Some(m) = found.get(&(id.partition, id.offset)) {
            let mut headers = m.headers.clone();
            headers.insert(
                fluxmq_core::REPLAYED_FROM_HEADER.to_string(),
                format!("{name}/{}/{}", m.partition, m.offset),
            );
            copies.push(fluxmq_proto::model::PublishMessage {
                key: m.key.clone(),
                headers,
                payload: payload::decode(m.payload.clone())?,
            });
        }
    }
    if copies.is_empty() {
        return Ok(Vec::new());
    }
    let count = copies.len() as u64;
    let out = s
        .cluster
        .publish(target, None, copies)
        .await
        .map_err(wire_to_api)?;
    s.broker.record_replayed(target, count);
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Consumer-group mutations (owned by partition leaders)
// ---------------------------------------------------------------------------------------------

/// A group mutation that must run on every node leading a partition of the channel.
#[derive(Debug, Clone, Copy)]
pub enum GroupOp {
    Reset(fluxmq_proto::StartFrom),
    Delete,
}

fn apply_group_op(s: &AppState, name: &str, group: &str, op: GroupOp) -> ApiResult<()> {
    match op {
        GroupOp::Reset(to) => s.broker.reset_offsets(name, group, to)?,
        GroupOp::Delete => {
            check_group_deletable(s, name, group)?;
            // A subscriber that raced in after the check is still refused by core.
            s.broker.delete_group(name, group).map_err(|e| match e {
                fluxmq_core::CoreError::InvalidArgument(m) => ApiErr::conflict(m),
                e => e.into(),
            })?;
        }
    }
    Ok(())
}

/// Local half of a group deletion check: 404 when the group doesn't exist on this node,
/// 409 while it has active subscriptions here.
pub fn check_group_deletable(s: &AppState, name: &str, group: &str) -> ApiResult<()> {
    match s.broker.group_subscribers(name, group)? {
        None => Err(ApiErr::not_found(format!(
            "consumer group '{group}' not found on channel '{name}'"
        ))),
        Some(0) => Ok(()),
        Some(n) => Err(ApiErr::conflict(format!(
            "consumer group '{group}' has {n} active consumer(s); stop them first"
        ))),
    }
}

async fn internal_group_reset(
    State(s): State<AppState>,
    Path((name, group)): Path<(String, String)>,
    Json(body): Json<fluxmq_proto::api::ResetOffsetsRequest>,
) -> ApiResult<StatusCode> {
    apply_group_op(&s, &name, &group, GroupOp::Reset(body.to))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GroupDeleteParams {
    /// Only check (404 absent / 409 busy / 204 deletable), don't delete.
    #[serde(default)]
    check: bool,
}

async fn internal_group_delete(
    State(s): State<AppState>,
    Path((name, group)): Path<(String, String)>,
    Query(p): Query<GroupDeleteParams>,
) -> ApiResult<StatusCode> {
    if p.check {
        check_group_deletable(&s, &name, &group)?;
    } else {
        apply_group_op(&s, &name, &group, GroupOp::Delete)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Maps a peer's HTTP error back to the API error it returned (404/409/400 keep their meaning,
/// anything else — including transport failures — is 503 "retry").
fn peer_err(node: &str, e: fluxmq_cluster::TransportError) -> ApiErr {
    if let fluxmq_cluster::TransportError::Status(code, body) = &e {
        let message = serde_json::from_str::<fluxmq_proto::api::ApiError>(body)
            .map(|b| b.message)
            .unwrap_or_else(|_| body.clone());
        match code {
            404 => return ApiErr::not_found(message),
            409 => return ApiErr::conflict(message),
            400 => return ApiErr::bad_request(message),
            _ => {}
        }
    }
    unavailable(format!("node {node}: {e}"))
}

/// Runs `op` on every node that leads at least one partition of `name` (a follower-local
/// change would be overwritten by the leader's replicated group state). Fails with 503 if a
/// leader can't be reached or a partition has no leader.
///
/// `Delete` is two-phase: every leader first checks (404 absent / 409 active consumers),
/// and only if no leader refuses is the group deleted on the leaders that have it — so a
/// group with consumers on one node is never half-deleted. 404 only if no leader has it.
pub async fn fanout_group_op(s: &AppState, name: &str, group: &str, op: GroupOp) -> ApiResult<()> {
    let md = s.cluster.metadata(&[name.to_string()]);
    let Some(parts) = md.channels.get(name) else {
        return apply_group_op(s, name, group, op);
    };
    let mut leaders = BTreeSet::new();
    for p in parts {
        match &p.leader {
            Some(l) => {
                leaders.insert(l.clone());
            }
            None => {
                return Err(unavailable(format!(
                    "partition {} of '{name}' has no leader right now",
                    p.partition
                )));
            }
        }
    }
    let http = s.cluster.internal_http();
    let path = format!("/internal/v1/channels/{name}/groups/{group}");
    let me = s.cluster.node_id().to_string();
    if let GroupOp::Delete = op {
        let mut holders = Vec::new();
        for node in &leaders {
            let r = if *node == me {
                check_group_deletable(s, name, group)
            } else {
                http.delete(&addr_of(s, node)?, &format!("{path}?check=true"))
                    .await
                    .map(|_| ())
                    .map_err(|e| peer_err(node, e))
            };
            match r {
                Ok(()) => holders.push(node.clone()),
                Err(e) if e.status == StatusCode::NOT_FOUND => {}
                Err(e) => return Err(e),
            }
        }
        if holders.is_empty() {
            return Err(ApiErr::not_found(format!(
                "consumer group '{group}' not found on channel '{name}'"
            )));
        }
        for node in holders {
            if node == me {
                apply_group_op(s, name, group, op)?;
            } else {
                http.delete(&addr_of(s, &node)?, &path)
                    .await
                    .map_err(|e| peer_err(&node, e))?;
            }
        }
        return Ok(());
    }
    for node in leaders {
        if node == me {
            apply_group_op(s, name, group, op)?;
            continue;
        }
        let GroupOp::Reset(to) = op else {
            unreachable!("delete handled above")
        };
        http.post_bytes(
            &addr_of(s, &node)?,
            &format!("{path}/reset"),
            "application/json",
            serde_json::to_vec(&json!({ "to": to })).unwrap_or_default(),
        )
        .await
        .map_err(|e| peer_err(&node, e))?;
    }
    Ok(())
}

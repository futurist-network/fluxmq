//! Cluster RPC listener (`--raft-addr`): Raft RPCs, forwarded metadata writes, heartbeats,
//! controller commands and the bootstrap probe. Every route requires the cluster secret.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::CLUSTER_SECRET_HEADER;
use crate::meta::WRITE_PATH;
use crate::raft::network::{APPEND_PATH, SNAPSHOT_PATH, VOTE_PATH};
use crate::raft::{MetaCommand, MetaReply, NodeId, TypeConfig};
use crate::rpc::{
    ControllerOk, ControllerOp, Heartbeat, HeartbeatAck, InfoResponse, RpcError, secret_matches,
};
use crate::runtime::{CONTROLLER_PATH, HEARTBEAT_PATH, INFO_PATH, Runtime};

type St = State<Arc<Runtime>>;

pub(crate) fn router(rt: Arc<Runtime>) -> Router {
    Router::new()
        .route(APPEND_PATH, post(append))
        .route(VOTE_PATH, post(vote))
        .route(SNAPSHOT_PATH, post(snapshot))
        .route(WRITE_PATH, post(write))
        .route(HEARTBEAT_PATH, post(heartbeat))
        .route(CONTROLLER_PATH, post(controller))
        .route(INFO_PATH, get(info))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .route_layer(axum::middleware::from_fn_with_state(rt.clone(), auth))
        .with_state(rt)
}

async fn auth(State(rt): St, req: Request, next: Next) -> Response {
    let presented = req
        .headers()
        .get(CLUSTER_SECRET_HEADER)
        .and_then(|v| v.to_str().ok());
    if secret_matches(presented, rt.cfg.cluster_secret.as_deref()) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "invalid cluster secret").into_response()
    }
}

async fn append(
    State(rt): St,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>> {
    Json(rt.raft.append_entries(req).await)
}

async fn vote(
    State(rt): St,
    Json(req): Json<VoteRequest<NodeId>>,
) -> Json<Result<VoteResponse<NodeId>, RaftError<NodeId>>> {
    Json(rt.raft.vote(req).await)
}

async fn snapshot(
    State(rt): St,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>> {
    Json(rt.raft.install_snapshot(req).await)
}

async fn write(State(rt): St, Json(cmd): Json<MetaCommand>) -> Json<Result<MetaReply, RpcError>> {
    if !rt.is_leader() {
        return Json(Err(RpcError::not_leader(rt.leader_hint())));
    }
    Json(rt.meta.propose_local(cmd).await)
}

async fn heartbeat(
    State(rt): St,
    Json(hb): Json<Heartbeat>,
) -> Json<Result<HeartbeatAck, RpcError>> {
    Json(crate::runtime::handle_heartbeat(&rt, hb).await)
}

async fn controller(
    State(rt): St,
    Json(op): Json<ControllerOp>,
) -> Json<Result<ControllerOk, RpcError>> {
    Json(crate::controller::execute(&rt, op).await)
}

async fn info(State(rt): St) -> Json<InfoResponse> {
    let initialized = rt.raft.is_initialized().await.unwrap_or(false);
    Json(InfoResponse {
        node_id: rt.node_id().to_string(),
        initialized,
        leader: rt.leader().map(|(_, n)| n.name),
    })
}

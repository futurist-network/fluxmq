//! Raft transport, client side: one [`NetworkConn`] per target, JSON over HTTP to the target's
//! cluster RPC listener (`POST /raft/{append,vote,snapshot}`), authenticated with the cluster
//! secret. Server side: [`crate::server`].

use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{NodeId, RaftNode, TypeConfig};
use crate::rpc::{RpcClient, TransportError};

pub const APPEND_PATH: &str = "/raft/append";
pub const VOTE_PATH: &str = "/raft/vote";
pub const SNAPSHOT_PATH: &str = "/raft/snapshot";

#[derive(Clone)]
pub struct Network {
    pub rpc: RpcClient,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConn;

    async fn new_client(&mut self, target: NodeId, node: &RaftNode) -> NetworkConn {
        NetworkConn {
            rpc: self.rpc.clone(),
            target,
            node: node.clone(),
        }
    }
}

pub struct NetworkConn {
    rpc: RpcClient,
    target: NodeId,
    node: RaftNode,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Msg(String);

impl NetworkConn {
    async fn call<Q, R, E>(
        &self,
        path: &str,
        req: &Q,
        ttl: Duration,
    ) -> Result<R, RPCError<NodeId, RaftNode, RaftError<NodeId, E>>>
    where
        Q: Serialize,
        R: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let res: Result<Result<R, RaftError<NodeId, E>>, TransportError> =
            self.rpc.post(&self.node.addr, path, req, ttl).await;
        match res {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.node.clone(),
                e,
            ))),
            // Unreachable = openraft backs off; Network = immediate retry (only for
            // transient per-request failures such as a timeout).
            Err(e @ (TransportError::Connect(_) | TransportError::Status(..))) => {
                Err(RPCError::Unreachable(Unreachable::new(&Msg(e.to_string()))))
            }
            Err(e) => Err(RPCError::Network(NetworkError::new(&Msg(e.to_string())))),
        }
    }
}

impl RaftNetwork<TypeConfig> for NetworkConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        self.call(APPEND_PATH, &rpc, option.hard_ttl()).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, RaftNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.call(SNAPSHOT_PATH, &rpc, option.hard_ttl()).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        self.call(VOTE_PATH, &rpc, option.hard_ttl()).await
    }
}

//! [`RaftMetaStore`]: the cluster-wide [`MetaStore`]. Reads come from the local state machine
//! replica; writes are proposed to the Raft leader (forwarded over the cluster RPC listener
//! when this node is a follower) and retried internally across elections up to a deadline.

use std::sync::Arc;
use std::time::Duration;

use fluxmq_meta::{
    MetaError, MetaEvent, MetaFuture, MetaResult, MetaStore, Precondition, Versioned, validate,
};
use openraft::error::{ClientWriteError, RaftError};
use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::raft::store::SmShared;
use crate::raft::{MetaCommand, MetaReply, NodeId, Raft, RaftNode};
use crate::rpc::{RpcClient, RpcError, RpcErrorKind};

pub const WRITE_PATH: &str = "/cluster/write";

/// Total time a write keeps retrying while there is no reachable leader.
const WRITE_DEADLINE: Duration = Duration::from_secs(8);
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on one local `client_write` (this node is the leader). openraft's `client_write`
/// waits for the entry to commit with no bound: a restarted node that restored itself as a
/// leader of a membership whose other voters are gone (or a leader partitioned from its
/// quorum) never commits, and the caller (often holding the controller lock) blocked forever.
/// Kept below [`FORWARD_TIMEOUT`] so a forwarding follower gets the leader's answer.
pub(crate) const LOCAL_PROPOSE_TIMEOUT: Duration = Duration::from_millis(4_500);
/// How long `put`/`delete` wait for the local replica to apply their own write.
const READ_YOUR_WRITES_WAIT: Duration = Duration::from_secs(3);
const WAIT_FOR_DEADLINE: Duration = Duration::from_secs(10);

pub struct RaftMetaStore {
    pub(crate) sm: Arc<SmShared>,
    pub(crate) raft: Raft,
    pub(crate) raft_id: NodeId,
    pub(crate) rpc: RpcClient,
}

/// Current leader as (raft id, membership node) according to local metrics.
pub(crate) fn current_leader(raft: &Raft) -> Option<(NodeId, RaftNode)> {
    let m = raft.metrics();
    let m = m.borrow();
    let id = m.current_leader?;
    // openraft reports the leader of the last *committed vote*: a restarted former leader
    // sees itself here while it is only a follower/candidate. Trust "self" only as Leader.
    if id == m.id && m.state != openraft::ServerState::Leader {
        return None;
    }
    let node = m.membership_config.membership().get_node(&id)?.clone();
    Some((id, node))
}

fn reply_to_result(r: MetaReply) -> MetaResult<u64> {
    match r {
        MetaReply::Applied { revision } => Ok(revision),
        MetaReply::Conflict { expected, current } => Err(MetaError::Conflict { expected, current }),
    }
}

impl RaftMetaStore {
    /// Proposes `cmd` on this node, which must be the leader. `Err(NotLeader)` otherwise.
    /// Bounded by [`LOCAL_PROPOSE_TIMEOUT`]: no commit in time (no quorum) →
    /// `Err(Unavailable)` with an unknown outcome (the entry may still commit later).
    pub(crate) async fn propose_local(&self, cmd: MetaCommand) -> Result<MetaReply, RpcError> {
        self.propose_local_within(cmd, LOCAL_PROPOSE_TIMEOUT).await
    }

    pub(crate) async fn propose_local_within(
        &self,
        cmd: MetaCommand,
        bound: Duration,
    ) -> Result<MetaReply, RpcError> {
        let Ok(res) = tokio::time::timeout(bound, self.raft.client_write(cmd)).await else {
            return Err(RpcError::new(
                RpcErrorKind::Unavailable,
                format!(
                    "metadata write not committed within {bound:?}: no Raft quorum reachable \
                     (outcome unknown)"
                ),
            ));
        };
        match res {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => Err(
                RpcError::not_leader(f.leader_node.map(|n| (n.name, n.addr))),
            ),
            Err(RaftError::APIError(e)) => {
                Err(RpcError::new(RpcErrorKind::Internal, e.to_string()))
            }
            Err(RaftError::Fatal(e)) => {
                Err(RpcError::new(RpcErrorKind::Unavailable, e.to_string()))
            }
        }
    }

    /// Proposes `cmd` through the current leader, retrying across leader changes until
    /// [`WRITE_DEADLINE`]. Only retries when the write certainly was not applied (no leader,
    /// connect failure, explicit "not leader"); an ambiguous failure (timeout) is reported.
    pub(crate) async fn propose(&self, cmd: MetaCommand) -> MetaResult<u64> {
        let deadline = Instant::now() + WRITE_DEADLINE;
        let mut backoff = Duration::from_millis(20);
        let mut last_err = String::from("no metadata leader");
        loop {
            match current_leader(&self.raft) {
                Some((id, _)) if id == self.raft_id => {
                    match self.propose_local(cmd.clone()).await {
                        Ok(reply) => return reply_to_result(reply),
                        Err(e) if e.kind == RpcErrorKind::NotLeader => last_err = e.message,
                        Err(e) => return Err(MetaError::Unavailable(e.message)),
                    }
                }
                Some((_, node)) => {
                    let res: Result<Result<MetaReply, RpcError>, _> = self
                        .rpc
                        .post(&node.addr, WRITE_PATH, &cmd, FORWARD_TIMEOUT)
                        .await;
                    match res {
                        Ok(Ok(reply)) => return reply_to_result(reply),
                        Ok(Err(e)) if e.kind == RpcErrorKind::NotLeader => last_err = e.message,
                        Ok(Err(e)) => return Err(MetaError::Unavailable(e.message)),
                        Err(e) if e.is_connect() => last_err = e.to_string(),
                        Err(e) => {
                            return Err(MetaError::Unavailable(format!(
                                "write forwarded to {} failed (outcome unknown): {e}",
                                node.name
                            )));
                        }
                    }
                }
                None => {}
            }
            if Instant::now() + backoff > deadline {
                return Err(MetaError::Unavailable(last_err));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(400));
        }
    }

    async fn write(&self, cmd: MetaCommand) -> MetaResult<u64> {
        let revision = self.propose(cmd).await?;
        // Read-your-writes on this node; the write is committed either way.
        let _ = tokio::time::timeout(READ_YOUR_WRITES_WAIT, self.wait_applied(revision)).await;
        Ok(revision)
    }

    pub(crate) async fn wait_applied(&self, revision: u64) {
        let mut rx = self.sm.applied.subscribe();
        let _ = rx.wait_for(|r| *r >= revision).await;
    }
}

impl MetaStore for RaftMetaStore {
    fn get(&self, key: &str) -> Option<Versioned> {
        self.sm.state.read().data.entries.get(key).cloned()
    }

    fn list(&self, prefix: &str) -> Vec<(String, Versioned)> {
        let st = self.sm.state.read();
        st.data
            .entries
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn revision(&self) -> u64 {
        self.sm.revision()
    }

    fn put(
        &self,
        key: &str,
        value: serde_json::Value,
        precondition: Option<Precondition>,
    ) -> MetaFuture<'_, u64> {
        let key = key.to_string();
        Box::pin(async move {
            validate(&key, Some(&value))?;
            self.write(MetaCommand::Put {
                key,
                value,
                precondition,
            })
            .await
        })
    }

    fn delete(&self, key: &str, precondition: Option<Precondition>) -> MetaFuture<'_, u64> {
        let key = key.to_string();
        Box::pin(async move {
            validate(&key, None)?;
            self.write(MetaCommand::Delete { key, precondition }).await
        })
    }

    fn watch(&self) -> broadcast::Receiver<MetaEvent> {
        self.sm.events.subscribe()
    }

    fn wait_for(&self, revision: u64) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            tokio::time::timeout(WAIT_FOR_DEADLINE, self.wait_applied(revision))
                .await
                .map_err(|_| {
                    MetaError::Unavailable(format!(
                        "revision {revision} not applied locally within {WAIT_FOR_DEADLINE:?}"
                    ))
                })
        })
    }
}

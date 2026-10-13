//! Raft plumbing for the metadata store: openraft type config, persistent log/vote storage,
//! the KV state machine (which *is* the cluster [`fluxmq_meta::MetaStore`]) and the HTTP
//! transport.

// openraft's `StorageError`/`RPCError` are large by design and fixed by its traits.
#![allow(clippy::result_large_err)]

pub mod network;
pub mod store;

use std::fmt;
use std::io::Cursor;

use fluxmq_meta::Precondition;
use serde::{Deserialize, Serialize};

/// Raft node ids are `u64`; FluxMQ node ids are strings. The mapping is a stable 64-bit
/// FNV-1a hash (collisions are checked when a node joins).
pub type NodeId = u64;

/// Membership entry for one node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftNode {
    /// FluxMQ node id (string).
    pub name: String,
    /// `host:port` of the node's Raft/cluster RPC listener.
    pub addr: String,
}

impl fmt::Display for RaftNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.name, self.addr)
    }
}

/// A replicated metadata mutation. Preconditions are evaluated by the state machine at apply
/// time, so a conditional write is atomic with respect to every other write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetaCommand {
    Put {
        key: String,
        value: serde_json::Value,
        precondition: Option<Precondition>,
    },
    Delete {
        key: String,
        precondition: Option<Precondition>,
    },
}

/// State machine answer to one log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaReply {
    /// Applied: the store-wide revision after the write (the current one for non-data entries).
    Applied { revision: u64 },
    /// Precondition failed; nothing changed.
    Conflict { expected: u64, current: u64 },
}

openraft::declare_raft_types!(
    /// openraft type configuration of the metadata Raft group.
    pub TypeConfig:
        D = MetaCommand,
        R = MetaReply,
        NodeId = NodeId,
        Node = RaftNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type StorageError = openraft::StorageError<NodeId>;
pub type StoredMembership = openraft::StoredMembership<NodeId, RaftNode>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, RaftNode>;

/// Stable 64-bit FNV-1a hash of a FluxMQ node id.
pub fn raft_id(name: &str) -> NodeId {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_ids_are_stable_and_distinct() {
        assert_eq!(raft_id("n1"), raft_id("n1"));
        assert_ne!(raft_id("n1"), raft_id("n2"));
        // Known FNV-1a vector.
        assert_eq!(raft_id("a"), 0xaf63_dc4c_8601_ec8c);
    }
}

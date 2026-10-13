use fluxmq_proto::wire::{ErrorCode, WireError};

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("channel '{0}' not found")]
    ChannelNotFound(String),
    #[error("consumer group '{group}' not found on channel '{channel}'")]
    GroupNotFound { channel: String, group: String },
    #[error("channel '{0}' already exists")]
    ChannelExists(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("unknown delivery {0}")]
    UnknownDelivery(u64),
    #[error("broker is shutting down")]
    ShuttingDown,
    #[error("storage: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// Replicated mode: this node does not lead the partition (it follows it, or holds no
    /// replica). `leader` is the current leader's node id when known.
    #[error("partition {channel}/{partition} is not led by this node (leader: {})", leader.as_deref().unwrap_or("unknown"))]
    NotLeader {
        channel: String,
        partition: u32,
        leader: Option<String>,
    },
    /// Replicated mode: the in-sync replica set is smaller than `min_isr`, so the publish
    /// cannot be acknowledged with `acks=all` durability (it may still be stored).
    #[error("not enough in-sync replicas for {channel}/{partition}: isr {isr} < min_isr {min_isr}")]
    NotEnoughReplicas {
        channel: String,
        partition: u32,
        isr: u32,
        min_isr: u32,
    },
    /// Replicated mode: the ISR did not acknowledge the publish within the ack timeout. The
    /// message is stored on the leader and may still commit (retry may duplicate).
    #[error("replication ack timed out after {waited_ms} ms for {channel}/{partition}")]
    AckTimeout {
        channel: String,
        partition: u32,
        waited_ms: u64,
    },
    /// A replication operation carried an epoch older than the partition's current one.
    #[error("stale epoch {got} < {current}")]
    StaleEpoch { got: u64, current: u64 },
}

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

impl From<&CoreError> for WireError {
    fn from(e: &CoreError) -> Self {
        let code = match e {
            CoreError::ChannelNotFound(_) | CoreError::GroupNotFound { .. } => ErrorCode::NotFound,
            CoreError::ChannelExists(_) => ErrorCode::AlreadyExists,
            CoreError::InvalidArgument(_) | CoreError::UnknownDelivery(_) => ErrorCode::BadRequest,
            CoreError::ShuttingDown => ErrorCode::Overloaded,
            CoreError::Io(_) | CoreError::Corrupt(_) => ErrorCode::Internal,
            // Wire contract: `message` = leader node id ("" while unknown).
            CoreError::NotLeader { leader, .. } => {
                return WireError::new(ErrorCode::NotLeader, leader.clone().unwrap_or_default());
            }
            CoreError::StaleEpoch { .. } => return WireError::new(ErrorCode::NotLeader, ""),
            CoreError::NotEnoughReplicas { .. } | CoreError::AckTimeout { .. } => {
                ErrorCode::Overloaded
            }
        };
        WireError::new(code, e.to_string())
    }
}

//! FluxMQ broker core.
//!
//! The public surface of this crate is the [`Broker`] handle. Everything the network layer,
//! the REST API and the telemetry plane need goes through it. Internals (storage, groups,
//! retry scheduler, metrics) are private modules.
//!
//! **Data plane vs observability plane:** hot paths only touch atomics and per-shard HDR
//! histograms. Observers pull [`stats::BrokerStats`] snapshots (cumulative counters and
//! cumulative histograms) and compute rates/percentiles by diffing. Live tail and events
//! are delivered through bounded channels with `try_send` — a slow observer loses data,
//! it never slows the broker.
//!
//! ## Lock order (internal invariant)
//!
//! Locks are only ever acquired in this order; a guard is never held while acquiring a lock
//! earlier in the list, and no guard is held across an `.await`:
//!
//! 1. `Broker` admin mutex (create/update/delete), then the channel registry `RwLock` —
//!    the registry guard is released immediately after cloning the `Arc<Channel>`s.
//! 2. `Channel::groups` map `RwLock` (released before group locks except for short
//!    read-only walks: offsets snapshot, retention floor).
//! 3. `Channel::config` `RwLock` (read).
//! 4. A group's `GroupState` mutex.
//! 5. Leaf locks, each taken and released within one statement/scope and never nested with
//!    one another: partition segment list / recent cache, attempt-history shards, DLQ state,
//!    channel histograms (`publish_latency`, `delivery_latency`), broker `fsync_latency`,
//!    dead-letter event accumulator, tail hub senders/bucket, `ChannelFiles` state.
//!    Backup cuts take only the segment-list read guard (to clone + pin segments), then group
//!    locks one at a time after releasing the `groups` registry guard, then the DLQ lock.
//! 6. `PartitionReplica::state` (replicated mode only) — innermost: it may be taken while
//!    holding a group lock or the DLQ state lock (DLQ mutations are recorded for followers
//!    under the DLQ lock so they replicate in file order), and nothing is acquired while it is
//!    held (wake-ups, group notifications and ISR events happen after release).
//!
//! Task-private locks (e.g. the checkpoint task's last-written cache) are never shared and
//! are outside this order.
//!
//! The writer thread only takes leaf locks, replica state locks and a `groups` read guard (to
//! notify), never a group lock. When it re-creates released storage (v0.4, replicated mode: a
//! channel none of whose partitions is held locally has no directory) it additionally reads
//! `Channel::config` (level 3) to write `channel.json`, releasing that guard before taking the
//! `ChannelFiles` lock. `Broker::stats` snapshots every group before touching channel histograms.
//!
//! v0.4 adds no lock: removed-replica cleanup (`ReplicaHandle::remove_local_data`) runs its
//! log deletion and storage release as writer commands, and clears group positions / DLQ
//! entries through the existing follower paths (`apply_replicated_offsets`,
//! `Dlq::replace_partition`) from async context with no guard held.

mod ackset;
pub mod backup;
mod broker;
mod channel;
pub mod config;
pub mod dedup;
mod dlq;
pub mod error;
pub mod fsync_pool;
mod group;
mod history;
pub mod live;
mod metrics;
mod persist;
mod reads;
pub mod replica;
pub mod stats;
mod storage;
mod tail;
mod util;

pub use broker::{
    Broker, DlqSelector, MessageWithDeliveries, REPLAYED_FROM_HEADER, SubscriberInfo, Subscription,
    TailReceiver, replay_copies,
};
pub use channel::StickyPartitioner;
pub use config::{BrokerConfig, FsyncPolicy};
pub use error::{CoreError, Result};
pub use group::{
    MAX_NACK_REASON_LEN, MAX_RAW_FRAMES_PER_POLL, RawDelivery, SharedDelivery, SubMode,
    delivery_wire_size, truncate_str,
};
pub use live::{LiveReader, LiveWatch};
pub use replica::{
    AppendTicket, FetchData, FetchError, FetchRequest, IsrUpdate, LeaderParams, ReplicaHandle,
    ReplicaRole, ReplicaStatus, StateCursor,
};
pub use stats::*;
pub use storage::{
    EnvelopeChunk, KeyRoute, RawChunk, SectionInfo, decode_chunk, section_messages,
    validate_section,
};

/// Broker lifecycle/state events for the UI events feed and alerting.
#[derive(Debug, Clone, PartialEq)]
pub enum BrokerEvent {
    ChannelCreated {
        channel: String,
    },
    ChannelDeleted {
        channel: String,
    },
    ChannelUpdated {
        channel: String,
    },
    ConsumerConnected {
        channel: String,
        group: String,
        instance: String,
    },
    ConsumerDisconnected {
        channel: String,
        group: String,
        instance: String,
    },
    /// Batched: emitted at most once per second per (channel, group, reason).
    DeadLettered {
        channel: String,
        group: String,
        reason: String,
        count: u64,
    },
    DlqRetried {
        count: u64,
    },
    DlqDiscarded {
        count: u64,
    },
    MessagesReplayed {
        channel: String,
        count: u64,
    },
}

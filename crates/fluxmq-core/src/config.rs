use std::path::PathBuf;
use std::time::Duration;

/// When appends become durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Group commit: publishes are acknowledged after the batch containing them is fsynced.
    /// The writer fsyncs at most every `interval` (and immediately when idle).
    GroupCommit { interval: Duration },
    /// Acknowledge after write(2); let the OS flush. Fastest, loses data on power failure.
    OsBuffered,
}

/// Default segment roll size: 64 MiB (128 MiB before v0.6). Smaller segments make retention
/// delete in smaller, more frequent steps (one 128 MiB unlink per partition, all partitions at
/// once, stalled acks for 0.5–1 s in the v0.5 benchmark) while keeping file counts modest:
/// 16 segments (16 open read descriptors) per GiB per partition.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
/// Default [`BrokerConfig::commit_pipeline_depth`].
pub const DEFAULT_COMMIT_PIPELINE_DEPTH: usize = 1;
/// Default [`BrokerConfig::partition_cache_bytes`] (16 MiB).
pub const DEFAULT_PARTITION_CACHE_BYTES: usize = crate::storage::DEFAULT_CACHE_BYTES;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    /// Stable node id (persisted in `data_dir/node_id` on first start when `None`).
    pub node_id: Option<String>,
    pub cluster_name: String,
    /// Nominal segment roll size (default [`DEFAULT_SEGMENT_BYTES`]). Each partition rolls
    /// within ±10% of it when `segment_jitter` is on. Changing it only affects segments
    /// created afterwards; existing segments of any size stay readable.
    pub segment_bytes: u64,
    /// Deterministic ±10% roll-size jitter per partition and segment (default on), so the
    /// partitions of a channel don't roll and get deleted by retention at the same moment.
    pub segment_jitter: bool,
    pub fsync: FsyncPolicy,
    /// Bound of the broker-wide fsync thread pool (0 = twice the CPUs, 4..=32).
    pub fsync_threads: usize,
    /// Pipelined group commit (default on since v0.9): a channel writer appends, writes and
    /// submits batch N+1 while earlier batches are being fsynced, up to
    /// `commit_pipeline_depth` batches in flight. Each partition's records become visible
    /// and are acked only after their own fsync and in order. Off = the writer waits for
    /// each batch's fsyncs before taking the next (v0.8 default behaviour). See
    /// docs/design/v0.9-commit-path.md.
    pub commit_pipeline: bool,
    /// Max batches in flight per channel writer when `commit_pipeline` is on (default
    /// [`DEFAULT_COMMIT_PIPELINE_DEPTH`]; 1 = one batch fsyncing while the next accumulates,
    /// the v0.6 pipeline).
    pub commit_pipeline_depth: usize,
    /// How often consumer group offsets are checkpointed to disk.
    pub offset_checkpoint_interval: Duration,
    /// Max delivery-attempt histories kept in memory per channel (retry timeline).
    pub attempt_history_capacity: usize,
    /// Max live-tail messages per second per channel handed to tail receivers.
    pub tail_max_rate: u32,
    /// Reported as `storage_capacity_bytes` (0 = detect from filesystem).
    pub storage_capacity_bytes: u64,
    /// Bytes of recently published entries (batch envelopes) kept in memory per partition,
    /// so caught-up consumers are served without disk reads (default 16 MiB).
    pub partition_cache_bytes: usize,
}

impl BrokerConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            node_id: None,
            cluster_name: "local".into(),
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            segment_jitter: true,
            fsync: FsyncPolicy::GroupCommit {
                interval: Duration::from_millis(2),
            },
            fsync_threads: 0,
            commit_pipeline: true,
            commit_pipeline_depth: DEFAULT_COMMIT_PIPELINE_DEPTH,
            offset_checkpoint_interval: Duration::from_secs(1),
            attempt_history_capacity: 100_000,
            tail_max_rate: 200,
            storage_capacity_bytes: 0,
            partition_cache_bytes: DEFAULT_PARTITION_CACHE_BYTES,
        }
    }
}

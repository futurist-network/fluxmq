//! Client protocol server (TCP, `fluxmq_proto::wire`).
//!
//! * [`listener`] — accept loop, per-connection task tracking, graceful drain.
//! * [`connection`] — one task per connection: handshake, request dispatch, subscription
//!   pumps, and a writer task fed by a bounded queue of outgoing frames.
//! * [`codec`] — length-prefixed postcard framing; zero-copy encoding of outgoing frames.
//! * [`writer`] — vectored writes of encoded frame chunks.

pub mod codec;
pub mod connection;
pub mod listener;
pub mod writer;

use std::time::Duration;

use fluxmq_core::Broker;

/// Per-connection limits.
#[derive(Debug, Clone)]
pub struct ConnectionLimits {
    /// Max simultaneous subscriptions on one connection.
    pub max_subscriptions: usize,
    /// Max requests awaiting completion (e.g. publishes waiting for fsync) before the
    /// connection stops reading new frames.
    pub max_in_flight_requests: usize,
    /// Prefetch requested by clients is clamped to this.
    pub max_prefetch: u32,
    /// Capacity of the outgoing frame queue between request/pump tasks and the writer.
    pub outbound_queue: usize,
    /// Time allowed for the first frame (`Hello`).
    pub handshake_timeout: Duration,
    /// A socket that stays unwritable this long is considered dead.
    pub write_timeout: Duration,
    /// Threads of the dedicated delivery runtime (subscription pumps and connection
    /// writers); 0 = run them on the main runtime. See [`delivery_runtime`].
    pub delivery_threads: usize,
}

/// Default [`ConnectionLimits::delivery_threads`].
pub const DEFAULT_DELIVERY_THREADS: usize = 2;

/// The process-wide delivery runtime (created on first use with `threads` workers; later
/// calls reuse it whatever they ask for). `None` for `threads == 0`.
///
/// Delivery work (polling groups, encoding `DeliverBatch`/`DeliverRecords`, socket writes)
/// runs on its own OS threads, so the OS scheduler gives it a CPU share of its own: a broker
/// saturated by producers (request decode + append on the main runtime) can't starve its
/// consumers. Producers are never throttled for it. Sockets stay registered with the main
/// runtime's reactor; tokio I/O resources may be polled from any runtime.
pub fn delivery_runtime(threads: usize) -> Option<tokio::runtime::Handle> {
    static RT: std::sync::OnceLock<Option<tokio::runtime::Runtime>> = std::sync::OnceLock::new();
    if threads == 0 {
        return None;
    }
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("fluxmq-delivery")
            .enable_all()
            .build()
            .map_err(|e| tracing::warn!(error = %e, "delivery runtime unavailable; using the main runtime"))
            .ok()
    })
    .as_ref()
    .map(|rt| rt.handle().clone())
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            max_subscriptions: 1024,
            max_in_flight_requests: 1024,
            max_prefetch: 65_536,
            outbound_queue: 1024,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(30),
            delivery_threads: DEFAULT_DELIVERY_THREADS,
        }
    }
}

/// Shared by every connection.
pub struct NetContext {
    pub broker: Broker,
    pub node_id: String,
    pub limits: ConnectionLimits,
    /// RBAC: authenticates `Hello.auth_token` and authorizes requests (stage 7).
    pub auth: fluxmq_auth::AuthService,
    /// Partition routing / metadata (stage 9).
    pub cluster: fluxmq_cluster::Cluster,
    /// Publish entry point with delayed delivery (v0.12).
    pub scheduler: fluxmq_scheduler::SchedulerService,
}

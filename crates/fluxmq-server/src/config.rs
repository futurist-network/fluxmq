//! Command line / environment configuration.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Parser, ValueEnum};
use fluxmq_cluster::PeerSpec;
use fluxmq_core::{BrokerConfig, FsyncPolicy};

use crate::net::ConnectionLimits;

/// FluxMQ broker.
#[derive(Debug, Clone, Parser)]
#[command(name = "fluxmq", version, about = "FluxMQ message broker")]
pub struct Cli {
    /// TOML file with any of these settings, keyed by flag name (`data-dir = "/var/lib/fluxmq"`).
    /// Flags and environment variables override it. Read by [`Cli::parse_with_config_file`].
    #[arg(long, env = "FLUXMQ_CONFIG")]
    pub config: Option<PathBuf>,
    /// Data directory (messages, offsets, DLQ, audit log).
    #[arg(long, env = "FLUXMQ_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,
    /// Client protocol (TCP) listen address.
    #[arg(long, env = "FLUXMQ_CLIENT_ADDR", default_value = "0.0.0.0:7420")]
    pub client_addr: SocketAddr,
    /// HTTP (REST, WebSocket telemetry, UI) listen address.
    #[arg(long, env = "FLUXMQ_HTTP_ADDR", default_value = "0.0.0.0:7421")]
    pub http_addr: SocketAddr,
    /// Cluster name shown in the UI.
    #[arg(long, env = "FLUXMQ_CLUSTER_NAME", default_value = "local")]
    pub cluster_name: String,
    /// Web console on the HTTP port.
    #[arg(long, env = "FLUXMQ_UI", value_enum, default_value_t = Toggle::On)]
    pub ui: Toggle,
    /// Serve the console from this directory instead of the copy built into the binary.
    #[arg(long, env = "FLUXMQ_UI_DIR")]
    pub ui_dir: Option<PathBuf>,
    /// Require this token for the REST API, WebSocket and client protocol.
    #[arg(long, env = "FLUXMQ_ADMIN_TOKEN", hide_env_values = true)]
    pub admin_token: Option<String>,
    /// Read `--admin-token` from this file (Docker / Kubernetes secrets, systemd credentials).
    #[arg(long, env = "FLUXMQ_ADMIN_TOKEN_FILE", conflicts_with = "admin_token")]
    pub admin_token_file: Option<PathBuf>,
    /// When appends become durable.
    #[arg(long, env = "FLUXMQ_FSYNC", value_enum, default_value_t = FsyncMode::GroupCommit)]
    pub fsync: FsyncMode,
    /// Segment roll size in bytes (1 MiB ..= 2 GiB; each partition rolls within ±10% of it).
    /// Applies to segments created from now on; existing segments of any size stay readable.
    #[arg(long, env = "FLUXMQ_SEGMENT_BYTES", default_value_t = fluxmq_core::config::DEFAULT_SEGMENT_BYTES,
          value_parser = clap::value_parser!(u64).range(MIN_SEGMENT_BYTES..=MAX_SEGMENT_BYTES))]
    pub segment_bytes: u64,
    /// fsync thread pool size shared by all channels (0 = twice the CPUs, 4..=32).
    #[arg(long, env = "FLUXMQ_FSYNC_THREADS", default_value_t = 0)]
    pub fsync_threads: usize,
    /// Pipelined group commit (default on since v0.9): write and submit the next batches
    /// while earlier ones are fsynced, up to `--commit-pipeline-depth` in flight; each
    /// partition is acked only after its own fsync, in order. `off` = wait for every
    /// batch's fsyncs (v0.8 default).
    #[arg(long, env = "FLUXMQ_COMMIT_PIPELINE", value_enum, default_value_t = Toggle::On)]
    pub commit_pipeline: Toggle,
    /// Max group-commit batches in flight per channel when the pipeline is on (1..=16).
    #[arg(long, env = "FLUXMQ_COMMIT_PIPELINE_DEPTH",
          default_value_t = fluxmq_core::config::DEFAULT_COMMIT_PIPELINE_DEPTH,
          value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=16))]
    pub commit_pipeline_depth: usize,
    /// Bytes of recently published record batches kept in memory per partition, so
    /// caught-up consumers are served without disk reads (0 disables the cache).
    #[arg(long, env = "FLUXMQ_PARTITION_CACHE_BYTES", default_value_t = fluxmq_core::config::DEFAULT_PARTITION_CACHE_BYTES)]
    pub partition_cache_bytes: usize,
    /// Threads of the dedicated delivery runtime: subscription pumps and connection writers
    /// (encoding and socket writes of deliveries) run there, apart from request handling
    /// (publish decode and append), so consumers keep a CPU share when producers saturate
    /// the broker. 0 = share the main runtime (v0.6 behaviour).
    #[arg(long, env = "FLUXMQ_DELIVERY_THREADS", default_value_t = crate::net::DEFAULT_DELIVERY_THREADS)]
    pub delivery_threads: usize,
    /// Log filter (tracing EnvFilter syntax, e.g. `info,fluxmq_core=debug`). Defaults to
    /// `RUST_LOG`, then `info`.
    #[arg(long, env = "FLUXMQ_LOG")]
    pub log: Option<String>,
    /// Live subscriptions (v0.10, WebSocket `/api/channels/{name}/live` + SSE `/events`):
    /// connections per node (over → 503).
    #[arg(long, env = "FLUXMQ_LIVE_MAX_CONNECTIONS", default_value_t = 10_000)]
    pub live_max_connections: usize,
    /// Live connections per token (over → 429; the bootstrap admin and open mode are exempt).
    #[arg(
        long,
        env = "FLUXMQ_LIVE_MAX_CONNECTIONS_PER_TOKEN",
        default_value_t = 1_000
    )]
    pub live_max_connections_per_token: usize,
    /// Live subscriptions per channel on this node (over → 429).
    #[arg(
        long,
        env = "FLUXMQ_LIVE_MAX_CONNECTIONS_PER_CHANNEL",
        default_value_t = 10_000
    )]
    pub live_max_connections_per_channel: usize,
    /// Subscriptions per live WebSocket connection.
    #[arg(long, env = "FLUXMQ_LIVE_MAX_SUBSCRIPTIONS", default_value_t = 16)]
    pub live_max_subscriptions: usize,
    /// Messages per second per live connection (throttles, never drops; 0 = unlimited).
    #[arg(long, env = "FLUXMQ_LIVE_MAX_MSGS_PER_SEC", default_value_t = 10_000)]
    pub live_max_msgs_per_sec: u64,
    /// Bytes per second per live connection (throttles; 0 = unlimited).
    #[arg(long, env = "FLUXMQ_LIVE_MAX_BYTES_PER_SEC", default_value_t = 16 << 20)]
    pub live_max_bytes_per_sec: u64,
    /// Larger payloads are truncated in live events (`truncated: true`, full size given).
    #[arg(long, env = "FLUXMQ_LIVE_MAX_PAYLOAD_BYTES", default_value_t = 64 << 10)]
    pub live_max_payload_bytes: usize,
    /// Messages per live `messages` event.
    #[arg(long, env = "FLUXMQ_LIVE_MAX_BATCH", default_value_t = 256)]
    pub live_max_batch: usize,
    /// Outbound events queued per live connection.
    #[arg(long, env = "FLUXMQ_LIVE_SEND_BUFFER", default_value_t = 64)]
    pub live_send_buffer: usize,
    /// Seconds a live client may not drain its connection before it is closed (`slow`).
    #[arg(long, env = "FLUXMQ_LIVE_SEND_TIMEOUT", default_value_t = 10)]
    pub live_send_timeout: u64,
    /// WebSocket ping / SSE heartbeat interval, seconds.
    #[arg(long, env = "FLUXMQ_LIVE_HEARTBEAT", default_value_t = 15)]
    pub live_heartbeat: u64,
    /// WebSocket: close when nothing (not even a pong) was received for this many seconds.
    #[arg(long, env = "FLUXMQ_LIVE_IDLE_TIMEOUT", default_value_t = 60)]
    pub live_idle_timeout: u64,
    /// Batches buffered per channel in the shared live fan-out ring.
    #[arg(long, env = "FLUXMQ_LIVE_FANOUT_BUFFER", default_value_t = 1024)]
    pub live_fanout_buffer: usize,
    /// Browser origins allowed to open live subscriptions besides same-origin
    /// (comma-separated exact origins like `https://app.example.com`, or `*`).
    #[arg(long, env = "FLUXMQ_LIVE_ALLOWED_ORIGINS", value_delimiter = ',')]
    pub live_allowed_origins: Vec<String>,

    // ---- Cluster (stage 9). Without `--peers` the broker runs as a single node. ----
    /// This node's id (required with `--peers`; overrides the id persisted in the data dir).
    #[arg(long, env = "FLUXMQ_NODE_ID")]
    pub node_id: Option<String>,
    /// Initial cluster voters as `id@raft_host:port,…` (may include this node). A node not in
    /// the list joins the cluster through these seeds as a learner.
    #[arg(long, env = "FLUXMQ_PEERS", value_parser = parse_peers_arg)]
    pub peers: Option<Peers>,
    /// Raft / cluster RPC listen address.
    #[arg(long, env = "FLUXMQ_RAFT_ADDR", default_value = "0.0.0.0:7422")]
    pub raft_addr: SocketAddr,
    /// Partition replication listen address.
    #[arg(long, env = "FLUXMQ_REPLICATION_ADDR", default_value = "0.0.0.0:7423")]
    pub replication_addr: SocketAddr,
    /// Host name / IP other nodes and clients use to reach this node (default: the listen IP,
    /// or 127.0.0.1 when listening on all interfaces).
    #[arg(long, env = "FLUXMQ_ADVERTISE_HOST")]
    pub advertise_host: Option<String>,
    /// Default replication factor of new channels (capped at the number of live nodes).
    #[arg(long, env = "FLUXMQ_REPLICATION_FACTOR", default_value_t = 3)]
    pub replication_factor: u32,
    /// Minimum in-sync replicas for a publish to be acknowledged.
    #[arg(long, env = "FLUXMQ_MIN_ISR", default_value_t = 1)]
    pub min_isr: u32,
    /// Shared secret authenticating node-to-node traffic (required with `--peers`).
    #[arg(long, env = "FLUXMQ_CLUSTER_SECRET", hide_env_values = true)]
    pub cluster_secret: Option<String>,
    /// Read `--cluster-secret` from this file.
    #[arg(
        long,
        env = "FLUXMQ_CLUSTER_SECRET_FILE",
        conflicts_with = "cluster_secret"
    )]
    pub cluster_secret_file: Option<PathBuf>,

    /// Public base URL of the web console (e.g. `https://mq.example.com`); notifications
    /// link back to it. Unset: messages carry no links.
    #[arg(long, env = "FLUXMQ_PUBLIC_URL")]
    pub public_url: Option<String>,

    /// Let webhooks target loopback / private / link-local addresses (`true`/`false`). Unset:
    /// allowed while auth is in open (dev) mode, denied once auth is enforced.
    #[arg(long, env = "FLUXMQ_WEBHOOKS_ALLOW_PRIVATE", num_args = 0..=1, default_missing_value = "true")]
    pub webhooks_allow_private: Option<bool>,

    /// Automatic rebalancing (v0.4) when the cluster has no stored rebalance config yet:
    /// `off` (manual only), `repair` (restore the replication factor) or `full` (repair +
    /// balance). Afterwards `PUT /api/cluster/rebalance/config` is the source of truth.
    #[arg(long, env = "FLUXMQ_AUTO_REBALANCE", value_enum, default_value_t = AutoRebalance::Full)]
    pub auto_rebalance: AutoRebalance,

    // ---- TLS (v0.4, `fluxmq_proto::tls`). Without these flags every port is plaintext. ----
    /// PEM certificate chain enabling TLS on the client port and (unless overridden or
    /// `--http-tls off`) the HTTP port. Requires `--tls-key`.
    #[arg(long, env = "FLUXMQ_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    /// PEM private key for `--tls-cert`.
    #[arg(long, env = "FLUXMQ_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
    /// PEM CA bundle: require client certificates signed by it on the client port (mTLS).
    #[arg(long, env = "FLUXMQ_TLS_CLIENT_CA")]
    pub tls_client_ca: Option<PathBuf>,
    /// PEM certificate chain for the HTTP port (default: `--tls-cert`).
    #[arg(long, env = "FLUXMQ_HTTP_TLS_CERT")]
    pub http_tls_cert: Option<PathBuf>,
    /// PEM private key for `--http-tls-cert`.
    #[arg(long, env = "FLUXMQ_HTTP_TLS_KEY")]
    pub http_tls_key: Option<PathBuf>,
    /// `off` keeps the HTTP port plaintext even with `--tls-cert` (e.g. behind a TLS-terminating
    /// proxy); `on` requires a certificate. Default: TLS whenever a certificate is configured.
    #[arg(long, env = "FLUXMQ_HTTP_TLS", value_enum)]
    pub http_tls: Option<Toggle>,
    /// This node's PEM certificate for node-to-node mutual TLS (Raft, replication, internal
    /// HTTP). Needs `--cluster-tls-key` and `--cluster-tls-ca`; every node must agree.
    #[arg(long, env = "FLUXMQ_CLUSTER_TLS_CERT")]
    pub cluster_tls_cert: Option<PathBuf>,
    /// PEM private key for `--cluster-tls-cert`.
    #[arg(long, env = "FLUXMQ_CLUSTER_TLS_KEY")]
    pub cluster_tls_key: Option<PathBuf>,
    /// PEM CA bundle every node certificate must chain to.
    #[arg(long, env = "FLUXMQ_CLUSTER_TLS_CA")]
    pub cluster_tls_ca: Option<PathBuf>,

    /// Offline tools (`backup`, `restore`); without a subcommand `fluxmq` runs the broker.
    #[command(subcommand)]
    pub command: Option<crate::backup_cli::Command>,
}

impl Cli {
    /// Parses the command line, filling in settings from the `--config` / `FLUXMQ_CONFIG` file
    /// that neither a flag nor an environment variable sets. Exits on bad input like
    /// [`Parser::parse`].
    pub fn parse_with_config_file() -> anyhow::Result<Self> {
        let args = with_config_file(std::env::args_os().collect(), |k| std::env::var_os(k))?;
        Ok(Self::parse_from(args))
    }

    /// Replaces `--admin-token-file` / `--cluster-secret-file` with the files' contents.
    pub fn read_secret_files(&mut self) -> anyhow::Result<()> {
        for (file, value) in [
            (self.admin_token_file.take(), &mut self.admin_token),
            (self.cluster_secret_file.take(), &mut self.cluster_secret),
        ] {
            if let Some(path) = non_empty(file) {
                let s = std::fs::read_to_string(&path)
                    .with_context(|| format!("reading {}", path.display()))?;
                *value = Some(s.trim_end_matches(['\r', '\n']).to_string());
            }
        }
        Ok(())
    }
}

/// `args` with `--key=value` inserted for every config file entry not already given as a flag
/// or (non-empty) environment variable, so the precedence is flag > env > file > default.
fn with_config_file(
    mut args: Vec<std::ffi::OsString>,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> anyhow::Result<Vec<std::ffi::OsString>> {
    use clap::CommandFactory;

    let given = |long: &str| {
        let flag = format!("--{long}");
        args.iter().skip(1).any(|a| {
            a.to_str()
                .is_some_and(|a| a == flag || a.starts_with(&format!("{flag}=")))
        })
    };
    let path = match args.iter().position(|a| a == "--config") {
        Some(i) => args.get(i + 1).map(PathBuf::from),
        None => args
            .iter()
            .find_map(|a| a.to_str()?.strip_prefix("--config=").map(PathBuf::from))
            .or_else(|| {
                env("FLUXMQ_CONFIG")
                    .filter(|v| !v.is_empty())
                    .map(PathBuf::from)
            }),
    };
    let Some(path) = path else { return Ok(args) };
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let table: toml::Table =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;

    let cmd = Cli::command();
    let mut extra = Vec::new();
    for (key, value) in table {
        let long = key.replace('_', "-");
        let arg = cmd
            .get_arguments()
            .find(|a| a.get_long() == Some(long.as_str()) && long != "config")
            .with_context(|| {
                format!(
                    "{}: unknown setting '{key}' (keys are flag names, see `fluxmq --help`)",
                    path.display()
                )
            })?;
        let env_set = arg
            .get_env()
            .and_then(|e| env(e.to_str()?))
            .is_some_and(|v| !v.is_empty());
        if given(&long) || env_set {
            continue;
        }
        let scalar = |v: &toml::Value| match v {
            toml::Value::String(s) => Ok(s.clone()),
            toml::Value::Integer(_) | toml::Value::Float(_) | toml::Value::Boolean(_) => {
                Ok(v.to_string())
            }
            _ => Err(anyhow::anyhow!(
                "{}: '{key}' must be a string, number, boolean or list of them",
                path.display()
            )),
        };
        let value = match &value {
            toml::Value::Array(items) => items
                .iter()
                .map(scalar)
                .collect::<Result<Vec<_>, _>>()?
                .join(","),
            v => scalar(v)?,
        };
        extra.push(format!("--{long}={value}").into());
    }
    args.splice(1..1, extra);
    Ok(args)
}

/// `on` / `off`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Toggle {
    On,
    Off,
}

/// TLS settings (v0.4). The default (everything `None`) keeps every port plaintext.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsSettings {
    /// `--tls-cert` / `--tls-key`: client port and (by default) HTTP port.
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    /// `--tls-client-ca`: mTLS on the client port.
    pub client_ca: Option<PathBuf>,
    /// `--http-tls-cert` / `--http-tls-key`: HTTP port override.
    pub http_cert: Option<PathBuf>,
    pub http_key: Option<PathBuf>,
    /// `--http-tls on|off`; `None` = TLS iff a certificate is configured.
    pub http: Option<bool>,
    /// `--cluster-tls-cert/key/ca`: node-to-node mutual TLS.
    pub cluster_cert: Option<PathBuf>,
    pub cluster_key: Option<PathBuf>,
    pub cluster_ca: Option<PathBuf>,
}

impl TlsSettings {
    /// Certificate/key of the client port, when TLS is enabled there.
    pub fn client_pair(&self) -> Option<(&PathBuf, &PathBuf)> {
        Some((self.cert.as_ref()?, self.key.as_ref()?))
    }

    /// Certificate/key of the HTTP port, when TLS is enabled there.
    pub fn http_pair(&self) -> Option<(&PathBuf, &PathBuf)> {
        if self.http == Some(false) {
            return None;
        }
        match (&self.http_cert, &self.http_key) {
            (Some(c), Some(k)) => Some((c, k)),
            _ => self.client_pair(),
        }
    }

    /// Node certificate, key and cluster CA, when cluster TLS is enabled.
    pub fn cluster_files(&self) -> Option<(&PathBuf, &PathBuf, &PathBuf)> {
        Some((
            self.cluster_cert.as_ref()?,
            self.cluster_key.as_ref()?,
            self.cluster_ca.as_ref()?,
        ))
    }

    pub fn any_enabled(&self) -> bool {
        self.client_pair().is_some() || self.http_pair().is_some() || self.cluster_files().is_some()
    }

    /// Rejects incomplete or contradictory combinations.
    pub fn validate(&self, clustered: bool) -> anyhow::Result<()> {
        if self.cert.is_some() != self.key.is_some() {
            anyhow::bail!("--tls-cert and --tls-key must be given together");
        }
        if self.http_cert.is_some() != self.http_key.is_some() {
            anyhow::bail!("--http-tls-cert and --http-tls-key must be given together");
        }
        if self.client_ca.is_some() && self.cert.is_none() {
            anyhow::bail!("--tls-client-ca (mTLS) needs --tls-cert/--tls-key on the client port");
        }
        match self.http {
            Some(true) if self.http_pair().is_none() => anyhow::bail!(
                "--http-tls on needs a certificate: --http-tls-cert/--http-tls-key or \
                 --tls-cert/--tls-key"
            ),
            Some(false) if self.http_cert.is_some() => {
                anyhow::bail!("--http-tls off contradicts --http-tls-cert")
            }
            _ => {}
        }
        let cluster = [&self.cluster_cert, &self.cluster_key, &self.cluster_ca];
        let given = cluster.iter().filter(|p| p.is_some()).count();
        if given != 0 && given != 3 {
            anyhow::bail!(
                "cluster TLS needs all three of --cluster-tls-cert, --cluster-tls-key and \
                 --cluster-tls-ca"
            );
        }
        if given == 3 && !clustered {
            anyhow::bail!("--cluster-tls-* only apply in cluster mode (--peers)");
        }
        if clustered && self.http_pair().is_some() && given == 0 {
            anyhow::bail!(
                "TLS on the HTTP port in a cluster needs cluster TLS (--cluster-tls-cert/key/ca): \
                 nodes call each other's /internal/* API on that port. Enable cluster TLS, or \
                 keep the HTTP port plaintext with --http-tls off"
            );
        }
        Ok(())
    }
}

/// Parsed `--peers` list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peers(pub Vec<PeerSpec>);

fn parse_peers_arg(s: &str) -> Result<Peers, String> {
    fluxmq_cluster::parse_peers(s).map(Peers)
}

/// Cluster settings; `ServerConfig::cluster = None` runs a single node.
#[derive(Debug, Clone)]
pub struct ClusterSettings {
    pub peers: Vec<PeerSpec>,
    pub raft_addr: SocketAddr,
    pub replication_addr: SocketAddr,
    pub advertise_host: Option<String>,
    pub replication_factor: u32,
    pub min_isr: u32,
    pub cluster_secret: Option<String>,
    pub node_timeout: Duration,
}

/// `--auto-rebalance`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AutoRebalance {
    Off,
    Repair,
    Full,
}

impl From<AutoRebalance> for fluxmq_proto::cluster::RebalanceMode {
    fn from(m: AutoRebalance) -> Self {
        match m {
            AutoRebalance::Off => Self::Off,
            AutoRebalance::Repair => Self::Repair,
            AutoRebalance::Full => Self::Full,
        }
    }
}

/// `--segment-bytes` bounds.
pub const MIN_SEGMENT_BYTES: u64 = 1 << 20;
pub const MAX_SEGMENT_BYTES: u64 = 2 << 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FsyncMode {
    /// fsync batches every 2 ms; publishes are acknowledged once durable.
    GroupCommit,
    /// Acknowledge after write(2); the OS flushes. Fastest, loses data on power failure.
    Os,
}

impl FsyncMode {
    pub fn policy(self) -> FsyncPolicy {
        match self {
            FsyncMode::GroupCommit => FsyncPolicy::GroupCommit {
                interval: Duration::from_millis(2),
            },
            FsyncMode::Os => FsyncPolicy::OsBuffered,
        }
    }
}

/// Everything [`crate::Server`] needs. Built from [`Cli`] or directly (tests).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub data_dir: PathBuf,
    pub client_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub cluster_name: String,
    /// Web console: built in, from a directory, or off.
    pub ui: fluxmq_api::UiSource,
    pub admin_token: Option<String>,
    pub fsync: FsyncMode,
    /// `--segment-bytes`.
    pub segment_bytes: u64,
    /// `--fsync-threads` (0 = auto).
    pub fsync_threads: usize,
    /// `--commit-pipeline`.
    pub commit_pipeline: bool,
    /// `--commit-pipeline-depth`.
    pub commit_pipeline_depth: usize,
    /// `--partition-cache-bytes`.
    pub partition_cache_bytes: usize,
    /// Overrides the persisted node id.
    pub node_id: Option<String>,
    pub limits: ConnectionLimits,
    /// Upper bound for draining connections on shutdown before the broker is flushed.
    pub shutdown_timeout: Duration,
    /// Cluster mode (stage 9); `None` = single node.
    pub cluster: Option<ClusterSettings>,
    /// `--public-url`: console base URL for links in notifications.
    pub public_url: Option<String>,
    /// `--webhooks-allow-private`; `None` = follow the auth mode (open → allowed).
    pub webhooks_allow_private: Option<bool>,
    /// `--auto-rebalance` (cluster only): seeds `cluster/rebalance/config` when absent.
    pub auto_rebalance: fluxmq_proto::cluster::RebalanceMode,
    /// How often the controller evaluates automatic rebalance triggers (at most); 30 s,
    /// shorter in tests.
    pub rebalance_interval: Duration,
    /// TLS (v0.4); default = plaintext everywhere.
    pub tls: TlsSettings,
    /// Live subscription gateway (v0.10).
    pub live: fluxmq_api::LiveConfig,
}

impl ServerConfig {
    /// Defaults matching the CLI, listening on all interfaces.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            client_addr: SocketAddr::from(([0, 0, 0, 0], fluxmq_proto::DEFAULT_CLIENT_PORT)),
            http_addr: SocketAddr::from(([0, 0, 0, 0], fluxmq_proto::DEFAULT_HTTP_PORT)),
            cluster_name: "local".into(),
            ui: fluxmq_api::UiSource::Embedded,
            admin_token: None,
            fsync: FsyncMode::GroupCommit,
            segment_bytes: fluxmq_core::config::DEFAULT_SEGMENT_BYTES,
            fsync_threads: 0,
            commit_pipeline: true,
            commit_pipeline_depth: fluxmq_core::config::DEFAULT_COMMIT_PIPELINE_DEPTH,
            partition_cache_bytes: fluxmq_core::config::DEFAULT_PARTITION_CACHE_BYTES,
            node_id: None,
            limits: ConnectionLimits::default(),
            shutdown_timeout: Duration::from_secs(10),
            cluster: None,
            public_url: None,
            webhooks_allow_private: None,
            auto_rebalance: fluxmq_proto::cluster::RebalanceMode::Full,
            rebalance_interval: fluxmq_cluster::DEFAULT_REBALANCE_INTERVAL,
            tls: TlsSettings::default(),
            live: fluxmq_api::LiveConfig::default(),
        }
    }

    /// Loopback, OS-assigned ports — for tests.
    pub fn ephemeral(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..Self::new(data_dir)
        }
    }

    /// Rejects inconsistent settings (cluster mode needs a node id and a cluster secret).
    pub fn validate(&self) -> anyhow::Result<()> {
        self.tls.validate(self.cluster.is_some())?;
        if let Some(u) = &self.public_url {
            let u = u.trim();
            if !(u.starts_with("http://") || u.starts_with("https://")) || u.len() < 10 {
                anyhow::bail!("--public-url must be an absolute http(s) URL, got '{u}'");
            }
        }
        if let Some(c) = &self.cluster {
            if c.peers.is_empty() {
                anyhow::bail!("--peers must list at least one node");
            }
            if self.node_id.is_none() {
                anyhow::bail!("--node-id is required in cluster mode (--peers)");
            }
            if c.cluster_secret.is_none() {
                anyhow::bail!(
                    "--cluster-secret (FLUXMQ_CLUSTER_SECRET) is required in cluster mode: it \
                     authenticates Raft, replication and /internal/* traffic between nodes"
                );
            }
            let remote_peers = c
                .peers
                .iter()
                .any(|p| !p.raft_addr.starts_with("127.") && !p.raft_addr.starts_with("localhost"));
            if c.advertise_host.is_none() && self.client_addr.ip().is_unspecified() && remote_peers
            {
                anyhow::bail!(
                    "--advertise-host (FLUXMQ_ADVERTISE_HOST) is required when peers run on other \
                     hosts: without it this node would advertise 127.0.0.1, which they cannot reach"
                );
            }
        }
        Ok(())
    }

    pub fn broker_config(&self) -> BrokerConfig {
        let mut c = BrokerConfig::new(&self.data_dir);
        c.cluster_name = self.cluster_name.clone();
        c.fsync = self.fsync.policy();
        c.segment_bytes = self.segment_bytes;
        c.fsync_threads = self.fsync_threads;
        c.commit_pipeline = self.commit_pipeline;
        c.commit_pipeline_depth = self.commit_pipeline_depth;
        c.partition_cache_bytes = self.partition_cache_bytes;
        c.node_id = self.node_id.clone();
        c
    }
}

impl From<Cli> for ServerConfig {
    fn from(cli: Cli) -> Self {
        Self {
            data_dir: cli.data_dir,
            client_addr: cli.client_addr,
            http_addr: cli.http_addr,
            cluster_name: cli.cluster_name,
            ui: match (cli.ui, non_empty(cli.ui_dir)) {
                (Toggle::Off, _) => fluxmq_api::UiSource::Off,
                (Toggle::On, Some(dir)) => fluxmq_api::UiSource::Dir(dir),
                (Toggle::On, None) => fluxmq_api::UiSource::Embedded,
            },
            admin_token: cli.admin_token.filter(|t| !t.is_empty()),
            fsync: cli.fsync,
            segment_bytes: cli.segment_bytes,
            fsync_threads: cli.fsync_threads,
            commit_pipeline: cli.commit_pipeline == Toggle::On,
            commit_pipeline_depth: cli.commit_pipeline_depth,
            partition_cache_bytes: cli.partition_cache_bytes,
            limits: crate::net::ConnectionLimits {
                delivery_threads: cli.delivery_threads,
                ..Default::default()
            },
            node_id: cli.node_id.filter(|n| !n.is_empty()),
            cluster: cli.peers.map(|Peers(peers)| ClusterSettings {
                peers,
                raft_addr: cli.raft_addr,
                replication_addr: cli.replication_addr,
                advertise_host: cli.advertise_host.filter(|h| !h.is_empty()),
                replication_factor: cli.replication_factor.max(1),
                min_isr: cli.min_isr.max(1),
                cluster_secret: cli.cluster_secret.filter(|s| !s.is_empty()),
                node_timeout: fluxmq_cluster::DEFAULT_NODE_TIMEOUT,
            }),
            webhooks_allow_private: cli.webhooks_allow_private,
            auto_rebalance: cli.auto_rebalance.into(),
            tls: TlsSettings {
                cert: non_empty(cli.tls_cert),
                key: non_empty(cli.tls_key),
                client_ca: non_empty(cli.tls_client_ca),
                http_cert: non_empty(cli.http_tls_cert),
                http_key: non_empty(cli.http_tls_key),
                http: cli.http_tls.map(|t| t == Toggle::On),
                cluster_cert: non_empty(cli.cluster_tls_cert),
                cluster_key: non_empty(cli.cluster_tls_key),
                cluster_ca: non_empty(cli.cluster_tls_ca),
            },
            public_url: cli.public_url.filter(|u| !u.trim().is_empty()),
            live: fluxmq_api::LiveConfig {
                max_connections: cli.live_max_connections,
                max_connections_per_token: cli.live_max_connections_per_token,
                max_connections_per_channel: cli.live_max_connections_per_channel,
                max_subscriptions: cli.live_max_subscriptions.max(1),
                max_msgs_per_sec: cli.live_max_msgs_per_sec,
                max_bytes_per_sec: cli.live_max_bytes_per_sec,
                max_payload_bytes: cli.live_max_payload_bytes.max(1),
                max_batch: cli.live_max_batch.max(1),
                send_buffer: cli.live_send_buffer.max(1),
                send_timeout: Duration::from_secs(cli.live_send_timeout.max(1)),
                heartbeat: Duration::from_secs(cli.live_heartbeat.max(1)),
                idle_timeout: Duration::from_secs(cli.live_idle_timeout.max(1)),
                fanout_buffer: cli.live_fanout_buffer.max(1),
                allowed_origins: cli
                    .live_allowed_origins
                    .into_iter()
                    .map(|o| o.trim().to_string())
                    .filter(|o| !o.is_empty())
                    .collect(),
                ..Default::default()
            },
            ..Self::new(PathBuf::new())
        }
    }
}

/// Empty env values (`FLUXMQ_TLS_CERT=`) count as unset.
fn non_empty(p: Option<PathBuf>) -> Option<PathBuf> {
    p.filter(|p| !p.as_os_str().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_flags() {
        let cfg = |args: &[&str]| ServerConfig::from(Cli::try_parse_from(args).unwrap());
        let plain = cfg(&["fluxmq"]);
        assert_eq!(plain.tls, TlsSettings::default());
        assert!(!plain.tls.any_enabled());
        assert!(plain.validate().is_ok());

        let c = cfg(&["fluxmq", "--tls-cert", "s.crt", "--tls-key", "s.key"]);
        assert!(c.tls.client_pair().is_some());
        assert_eq!(c.tls.http_pair().unwrap().0, &PathBuf::from("s.crt"));
        assert!(c.validate().is_ok());

        let off = cfg(&[
            "fluxmq",
            "--tls-cert",
            "s.crt",
            "--tls-key",
            "s.key",
            "--http-tls",
            "off",
        ]);
        assert!(off.tls.http_pair().is_none() && off.tls.client_pair().is_some());

        let over = cfg(&[
            "fluxmq",
            "--tls-cert",
            "s.crt",
            "--tls-key",
            "s.key",
            "--http-tls-cert",
            "h.crt",
            "--http-tls-key",
            "h.key",
            "--tls-client-ca",
            "ca.crt",
        ]);
        assert_eq!(over.tls.http_pair().unwrap().0, &PathBuf::from("h.crt"));
        assert!(over.validate().is_ok());

        // Incomplete / contradictory.
        assert!(cfg(&["fluxmq", "--tls-cert", "s.crt"]).validate().is_err());
        assert!(
            cfg(&["fluxmq", "--tls-client-ca", "ca.crt"])
                .validate()
                .is_err()
        );
        assert!(cfg(&["fluxmq", "--http-tls", "on"]).validate().is_err());
        assert!(
            cfg(&[
                "fluxmq",
                "--cluster-tls-cert",
                "n.crt",
                "--cluster-tls-key",
                "n.key"
            ])
            .validate()
            .is_err()
        );
        // Cluster TLS outside cluster mode.
        let cluster_only = [
            "fluxmq",
            "--cluster-tls-cert",
            "n.crt",
            "--cluster-tls-key",
            "n.key",
            "--cluster-tls-ca",
            "ca.crt",
        ];
        assert!(cfg(&cluster_only).validate().is_err());
        let mut args = cluster_only.to_vec();
        args.extend([
            "--node-id",
            "n1",
            "--peers",
            "n1@127.0.0.1:7422",
            "--cluster-secret",
            "s",
        ]);
        assert!(cfg(&args).validate().is_ok());
        // HTTPS in a cluster needs cluster TLS.
        let https_cluster = cfg(&[
            "fluxmq",
            "--tls-cert",
            "s.crt",
            "--tls-key",
            "s.key",
            "--node-id",
            "n1",
            "--peers",
            "n1@127.0.0.1:7422",
            "--cluster-secret",
            "s",
        ]);
        assert!(https_cluster.validate().is_err());
    }

    #[test]
    fn config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fluxmq.toml");
        std::fs::write(
            &path,
            r#"
            data-dir = "/srv/fluxmq"
            http_addr = "127.0.0.1:9000"
            fsync = "os"
            segment-bytes = 16777216
            live-allowed-origins = ["https://a.example", "https://b.example"]
            ui = "off"
            "#,
        )
        .unwrap();
        let parse = |args: &[&str], env: &[(&str, &str)]| {
            let args = args.iter().map(Into::into).collect();
            let env = |k: &str| env.iter().find(|(n, _)| *n == k).map(|(_, v)| v.into());
            let cli = Cli::try_parse_from(with_config_file(args, env).unwrap()).unwrap();
            ServerConfig::from(cli)
        };
        let p = path.to_str().unwrap();

        let c = parse(&["fluxmq", "--config", p], &[]);
        assert_eq!(c.data_dir, PathBuf::from("/srv/fluxmq"));
        assert_eq!(c.http_addr.port(), 9000);
        assert_eq!(c.fsync, FsyncMode::Os);
        assert_eq!(c.segment_bytes, 16 << 20);
        assert_eq!(c.live.allowed_origins.len(), 2);
        assert_eq!(c.ui, fluxmq_api::UiSource::Off);

        // Flag > env > file; the file is also found through FLUXMQ_CONFIG.
        let c = parse(
            &["fluxmq", "--data-dir", "/flag"],
            &[("FLUXMQ_CONFIG", p), ("FLUXMQ_FSYNC", "group-commit")],
        );
        assert_eq!(c.data_dir, PathBuf::from("/flag"));
        assert_eq!(c.fsync, FsyncMode::GroupCommit);
        assert_eq!(c.http_addr.port(), 9000);

        // No file: arguments unchanged.
        let args: Vec<std::ffi::OsString> = vec!["fluxmq".into()];
        assert_eq!(with_config_file(args.clone(), |_| None).unwrap(), args);

        // Typos and nested tables are errors, not silently ignored.
        for bad in [
            "data-directory = \"x\"",
            "[cluster]\nnode-id = \"n1\"",
            "config = \"x\"",
        ] {
            std::fs::write(&path, bad).unwrap();
            let args = vec!["fluxmq".into(), format!("--config={p}").into()];
            assert!(with_config_file(args, |_| None).is_err(), "{bad}");
        }
    }

    #[test]
    fn secret_files() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, "s3cret\n").unwrap();
        let f = file.to_str().unwrap();
        let mut cli = Cli::try_parse_from(["fluxmq", "--admin-token-file", f]).unwrap();
        cli.read_secret_files().unwrap();
        assert_eq!(cli.admin_token.as_deref(), Some("s3cret"));
        assert!(
            Cli::try_parse_from(["fluxmq", "--admin-token", "x", "--admin-token-file", f]).is_err()
        );
        let mut missing =
            Cli::try_parse_from(["fluxmq", "--cluster-secret-file", "/nonexistent"]).unwrap();
        assert!(missing.read_secret_files().is_err());
    }

    #[test]
    fn remote_peers_need_advertise_host() {
        let cfg = |extra: &[&str]| {
            let mut args = vec![
                "fluxmq",
                "--node-id",
                "n1",
                "--cluster-secret",
                "s",
                "--peers",
                "n1@10.0.0.1:7422,n2@10.0.0.2:7422",
            ];
            args.extend(extra);
            ServerConfig::from(Cli::try_parse_from(args).unwrap())
        };
        assert!(cfg(&[]).validate().is_err());
        assert!(cfg(&["--advertise-host", "10.0.0.1"]).validate().is_ok());
        assert!(cfg(&["--client-addr", "10.0.0.1:7420"]).validate().is_ok());
    }

    #[test]
    fn cli_defaults() {
        let cli = Cli::try_parse_from(["fluxmq"]).unwrap();
        assert_eq!(cli.client_addr.port(), 7420);
        assert_eq!(cli.http_addr.port(), 7421);
        assert_eq!(cli.fsync, FsyncMode::GroupCommit);
        let cfg = ServerConfig::from(cli);
        assert_eq!(cfg.ui, fluxmq_api::UiSource::Embedded);
        assert!(cfg.admin_token.is_none());
    }

    #[test]
    fn storage_flags() {
        let cfg = ServerConfig::from(Cli::try_parse_from(["fluxmq"]).unwrap()).broker_config();
        assert_eq!(cfg.segment_bytes, 64 << 20);
        assert!(cfg.commit_pipeline);
        assert_eq!(
            cfg.commit_pipeline_depth,
            fluxmq_core::config::DEFAULT_COMMIT_PIPELINE_DEPTH
        );
        assert_eq!(cfg.fsync_threads, 0);
        assert_eq!(cfg.partition_cache_bytes, 16 << 20);
        let cli = Cli::try_parse_from([
            "fluxmq",
            "--segment-bytes",
            "16777216",
            "--fsync-threads",
            "6",
            "--commit-pipeline",
            "off",
            "--commit-pipeline-depth",
            "2",
            "--partition-cache-bytes",
            "1048576",
        ])
        .unwrap();
        let cfg = ServerConfig::from(cli).broker_config();
        assert_eq!(cfg.segment_bytes, 16 << 20);
        assert_eq!(cfg.fsync_threads, 6);
        assert!(!cfg.commit_pipeline);
        assert_eq!(cfg.commit_pipeline_depth, 2);
        assert_eq!(cfg.partition_cache_bytes, 1 << 20);
        assert!(Cli::try_parse_from(["fluxmq", "--commit-pipeline-depth", "0"]).is_err());
        assert!(Cli::try_parse_from(["fluxmq", "--segment-bytes", "4096"]).is_err());
        assert!(Cli::try_parse_from(["fluxmq", "--segment-bytes", "64MiB"]).is_err());
    }

    #[test]
    fn cli_flags() {
        let cli = Cli::try_parse_from([
            "fluxmq",
            "--data-dir",
            "/tmp/x",
            "--fsync",
            "os",
            "--admin-token",
            "s3cret",
            "--http-addr",
            "127.0.0.1:9000",
        ])
        .unwrap();
        let cfg = ServerConfig::from(cli);
        assert_eq!(cfg.fsync, FsyncMode::Os);
        assert_eq!(cfg.admin_token.as_deref(), Some("s3cret"));
        assert_eq!(cfg.http_addr.port(), 9000);
        assert_eq!(cfg.broker_config().fsync, FsyncPolicy::OsBuffered);
        assert!(Cli::try_parse_from(["fluxmq", "--fsync", "never"]).is_err());
        assert!(cfg.cluster.is_none());
    }

    #[test]
    fn webhooks_allow_private_flag() {
        let parse = |args: &[&str]| {
            ServerConfig::from(Cli::try_parse_from(args).unwrap()).webhooks_allow_private
        };
        assert_eq!(parse(&["fluxmq"]), None);
        assert_eq!(parse(&["fluxmq", "--webhooks-allow-private"]), Some(true));
        assert_eq!(
            parse(&["fluxmq", "--webhooks-allow-private=false"]),
            Some(false)
        );
    }

    #[test]
    fn public_url_flag() {
        let cfg = ServerConfig::from(
            Cli::try_parse_from(["fluxmq", "--public-url", "https://mq.example.com/"]).unwrap(),
        );
        assert_eq!(cfg.public_url.as_deref(), Some("https://mq.example.com/"));
        assert!(cfg.validate().is_ok());
        let bad = ServerConfig::from(
            Cli::try_parse_from(["fluxmq", "--public-url", "mq.example.com"]).unwrap(),
        );
        assert!(bad.validate().is_err());
    }

    #[test]
    fn cluster_flags() {
        let cli = Cli::try_parse_from([
            "fluxmq",
            "--node-id",
            "n2",
            "--peers",
            "n1@10.0.0.1:7422,n2@10.0.0.2:7422",
            "--advertise-host",
            "10.0.0.2",
            "--replication-factor",
            "2",
            "--cluster-secret",
            "s",
        ])
        .unwrap();
        let cfg = ServerConfig::from(cli);
        assert_eq!(cfg.node_id.as_deref(), Some("n2"));
        let c = cfg.cluster.unwrap();
        assert_eq!(c.peers.len(), 2);
        assert_eq!(c.raft_addr.port(), 7422);
        assert_eq!(c.replication_addr.port(), 7423);
        assert_eq!(c.replication_factor, 2);
        assert_eq!(c.min_isr, 1);
        assert_eq!(c.cluster_secret.as_deref(), Some("s"));
        assert!(Cli::try_parse_from(["fluxmq", "--peers", "bad"]).is_err());
        use fluxmq_proto::cluster::RebalanceMode;
        assert_eq!(cfg.auto_rebalance, RebalanceMode::Full);
        let parse = |m: &str| {
            ServerConfig::from(Cli::try_parse_from(["fluxmq", "--auto-rebalance", m]).unwrap())
                .auto_rebalance
        };
        assert_eq!(parse("off"), RebalanceMode::Off);
        assert_eq!(parse("repair"), RebalanceMode::Repair);
        assert!(Cli::try_parse_from(["fluxmq", "--auto-rebalance", "sometimes"]).is_err());
    }
}

//! Client configuration.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use fluxmq_proto::batch::Compression;
use fluxmq_tls::{ClientTls, ClientTlsBuilder, PemSource};

/// How the client re-establishes lost connections.
///
/// Backoff for attempt `n` (0-based) is `min(max_backoff, initial_backoff * 2^n)`, multiplied by a
/// random factor in `[1 - jitter, 1 + jitter]` so that many clients restarting together don't
/// reconnect in lockstep.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconnectPolicy {
    /// Delay before the first retry after a failed attempt (default 100 ms).
    pub initial_backoff: Duration,
    /// Upper bound for the delay between attempts (default 5 s).
    pub max_backoff: Duration,
    /// Relative random jitter, `0.0..=1.0` (default 0.2 = ±20 %).
    pub jitter: f64,
    /// Consecutive failed attempts after which a node is given up. `None` (default) = retry
    /// forever. `Some(0)` disables reconnection: the first connection loss closes the client
    /// (v0.1 behaviour). When the *bootstrap* connection gives up, the whole client closes:
    /// pending requests fail and consumer streams end with [`ClientError::Closed`].
    ///
    /// [`ClientError::Closed`]: crate::ClientError::Closed
    pub max_attempts: Option<u32>,
    /// If `true`, [`Client::connect`](crate::Client::connect) keeps retrying (with this policy)
    /// until a seed answers instead of failing on the first unreachable round of seeds.
    /// Authentication errors still fail immediately. Default `false`.
    pub retry_initial_connect: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            jitter: 0.2,
            max_attempts: None,
            retry_initial_connect: false,
        }
    }
}

impl ReconnectPolicy {
    /// No automatic reconnection: a connection loss closes the client.
    pub fn disabled() -> Self {
        Self {
            max_attempts: Some(0),
            ..Default::default()
        }
    }

    /// Retry forever (the default).
    pub fn infinite() -> Self {
        Self::default()
    }

    /// Sets `max_attempts`.
    pub fn max_attempts(mut self, attempts: Option<u32>) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// Sets initial and maximum backoff.
    pub fn backoff(mut self, initial: Duration, max: Duration) -> Self {
        self.initial_backoff = initial;
        self.max_backoff = max.max(initial);
        self
    }

    /// Sets the jitter fraction.
    pub fn jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter;
        self
    }

    /// Sets `retry_initial_connect`.
    pub fn retry_initial_connect(mut self, retry: bool) -> Self {
        self.retry_initial_connect = retry;
        self
    }

    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        crate::util::backoff(
            self.initial_backoff,
            self.max_backoff.max(self.initial_backoff),
            attempt,
            self.jitter,
        )
    }

    pub(crate) fn exhausted(&self, failed_attempts: u32) -> bool {
        self.max_attempts.is_some_and(|m| failed_attempts >= m)
    }
}

/// Address prefix selecting TLS (`tls://host:port`).
pub const TLS_SCHEME: &str = "tls://";

/// Connection settings for [`Client::connect`](crate::Client::connect).
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Broker address, `host:port` (default `127.0.0.1:7420`). May also be a comma-separated
    /// list (`"n1:7420,n2:7420"`). Ignored when [`ClientOptions::seeds`] is non-empty.
    pub addr: String,
    /// Bootstrap addresses. The client connects to the first one that answers, learns the
    /// cluster topology from it and connects to partition leaders directly. On reconnect it
    /// cycles through the seeds and every node address learned from metadata.
    /// Empty (default) = use [`ClientOptions::addr`].
    pub seeds: Vec<String>,
    /// Shown in the UI as the producer / consumer instance name (e.g. `order-api-3`).
    pub client_name: String,
    /// Required when the broker runs with an admin token / RBAC.
    pub auth_token: Option<String>,
    /// Deadline for every request/response round trip (including time spent queued and time
    /// spent waiting for a connection).
    pub request_timeout: Duration,
    /// Deadline for TCP connect + handshake (per attempt).
    pub connect_timeout: Duration,
    /// Overall deadline for one publish call including retries (connection loss, `NotLeader`,
    /// `Overloaded`). Default 30 s.
    pub publish_timeout: Duration,
    /// Automatic reconnection.
    pub reconnect: ReconnectPolicy,
    /// How often cluster metadata is re-fetched even without a `MetadataChanged` hint
    /// (default 30 s). `ZERO` disables periodic refresh.
    pub metadata_refresh_interval: Duration,
    /// Capacity (in frames) of the outbound queue feeding the writer task (per connection).
    pub write_queue_capacity: usize,
    /// Default for [`SubscribeOptions::ack_linger`](crate::SubscribeOptions::ack_linger): an
    /// ack waits at most this long in its subscription's buffer before the buffer is sent as
    /// one `Ack` frame (it goes earlier once `ack_batch` acks are buffered). `ZERO` sends every
    /// ack immediately.
    pub ack_linger: Duration,
    /// How often consumers send `ReportStats` (processing time, CPU, memory, last error).
    pub stats_interval: Duration,
    /// TLS for every connection (seeds, reconnects and cluster nodes learned from metadata).
    /// `None` = plaintext, unless an address is written `tls://host:port` — then TLS with
    /// the OS trust store.
    pub tls: Option<TlsOptions>,
    /// Stamp the `flux-producer` header (= `client_name`) on every message published through
    /// a [`Producer`](crate::Producer) or [`BatchingProducer`](crate::BatchingProducer) that
    /// does not carry one (shown in the UI's message inspector). Default `true`. Turning it
    /// off saves one header per message on the wire, in storage and in every delivery.
    pub stamp_producer: bool,
    /// Idempotent publishing (brokers v0.12+): every `PublishBatch` carries
    /// `flux-pseq: <client id>.<batch number>` on its first record, kept across retries, so a
    /// batch retried after a lost answer (timeout, reconnect, failover) is stored once. Default
    /// `true`; costs about 30 bytes per batch. Older brokers store the header unchanged.
    pub idempotent: bool,
    /// Per-batch compression for publishes on protocol v4 brokers (v0.7+): `None` (default),
    /// `Lz4` or `Zstd` (level 1). The producer compresses each batch's records section once; the
    /// broker stores and delivers it compressed and consumers decompress, so compression saves
    /// network, disk and replication bandwidth for producer CPU (and some consumer CPU). A batch
    /// that doesn't get smaller is sent uncompressed. Ignored on v2/v3 brokers.
    pub compression: Compression,
}

/// TLS settings of a [`ClientOptions`] (v0.4, rustls with aws-lc-rs).
///
/// ```no_run
/// use fluxmq_client::{ClientOptions, TlsOptions};
/// let opts = ClientOptions::new("tls://mq.internal:7420", "order-api-1").tls(
///     TlsOptions::new()
///         .ca_file("/etc/fluxmq/ca.pem")
///         .identity_files("/etc/fluxmq/client.pem", "/etc/fluxmq/client-key.pem"),
/// );
/// ```
///
/// The built rustls configuration is cached and shared by clones, so reconnects don't re-read
/// the files.
#[derive(Clone, Default)]
pub struct TlsOptions {
    /// CA certificate(s) the broker certificate must chain to. `None` = OS trust store.
    pub ca_pem: Option<PemSource>,
    /// Trust the OS trust store in addition to `ca_pem`.
    pub use_system_roots: bool,
    /// Client certificate chain + key for mTLS (`--tls-client-ca` on the broker).
    pub client_cert: Option<PemSource>,
    pub client_key: Option<PemSource>,
    /// Name the broker certificate must be valid for. Default: the host part of each address.
    pub server_name: Option<String>,
    /// Accept any broker certificate. **Development only.**
    pub danger_accept_invalid_certs: bool,
    built: Arc<OnceLock<Result<ClientTls, String>>>,
}

impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsOptions")
            .field("ca_pem", &self.ca_pem)
            .field("use_system_roots", &self.use_system_roots)
            .field("client_cert", &self.client_cert)
            .field(
                "client_key",
                &self.client_key.as_ref().map(|_| "<redacted>"),
            )
            .field("server_name", &self.server_name)
            .field(
                "danger_accept_invalid_certs",
                &self.danger_accept_invalid_certs,
            )
            .finish()
    }
}

impl PartialEq for TlsOptions {
    fn eq(&self, o: &Self) -> bool {
        self.ca_pem == o.ca_pem
            && self.use_system_roots == o.use_system_roots
            && self.client_cert == o.client_cert
            && self.client_key == o.client_key
            && self.server_name == o.server_name
            && self.danger_accept_invalid_certs == o.danger_accept_invalid_certs
    }
}

impl TlsOptions {
    /// OS trust store, no client certificate.
    pub fn new() -> Self {
        Self::default()
    }

    fn with(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self.built = Arc::default();
        self
    }

    /// Trust the CA certificate(s) in this PEM file.
    pub fn ca_file(self, path: impl Into<PathBuf>) -> Self {
        let p = PemSource::Path(path.into());
        self.with(|o| o.ca_pem = Some(p))
    }

    /// Trust the CA certificate(s) in these PEM bytes.
    pub fn ca_pem(self, pem: impl Into<Vec<u8>>) -> Self {
        let p = PemSource::Pem(pem.into());
        self.with(|o| o.ca_pem = Some(p))
    }

    /// Also trust the OS trust store.
    pub fn use_system_roots(self, on: bool) -> Self {
        self.with(|o| o.use_system_roots = on)
    }

    /// Client certificate (chain) and private key files for mTLS.
    pub fn identity_files(self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        let (c, k) = (PemSource::Path(cert.into()), PemSource::Path(key.into()));
        self.with(|o| {
            o.client_cert = Some(c);
            o.client_key = Some(k);
        })
    }

    /// Client certificate (chain) and private key as PEM bytes for mTLS.
    pub fn identity_pem(self, cert: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Self {
        let (c, k) = (PemSource::Pem(cert.into()), PemSource::Pem(key.into()));
        self.with(|o| {
            o.client_cert = Some(c);
            o.client_key = Some(k);
        })
    }

    /// Verify the broker certificate against this name instead of each address' host.
    pub fn server_name(self, name: impl Into<String>) -> Self {
        let n = name.into();
        self.with(|o| o.server_name = Some(n))
    }

    /// Accept any broker certificate. **Development only.**
    pub fn danger_accept_invalid_certs(self, on: bool) -> Self {
        self.with(|o| o.danger_accept_invalid_certs = on)
    }

    /// For CLI tools (`--tls-ca` / `--tls-cert` / `--tls-key`): `None` when no flag is given
    /// (plaintext), else TLS with that CA (OS trust store without one) and, with both
    /// certificate and key, a client identity.
    pub fn from_files(
        ca: Option<PathBuf>,
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
    ) -> Result<Option<TlsOptions>, String> {
        if ca.is_none() && cert.is_none() && key.is_none() {
            return Ok(None);
        }
        let mut t = TlsOptions::new();
        if let Some(ca) = ca {
            t = t.ca_file(ca);
        }
        match (cert, key) {
            (Some(c), Some(k)) => t = t.identity_files(c, k),
            (None, None) => {}
            _ => return Err("--tls-cert and --tls-key must be given together".into()),
        }
        Ok(Some(t))
    }

    /// The rustls configuration (built once, then shared).
    pub(crate) fn client_tls(&self) -> Result<ClientTls, String> {
        self.built
            .get_or_init(|| {
                ClientTlsBuilder {
                    ca: self.ca_pem.iter().cloned().collect(),
                    use_system_roots: self.use_system_roots,
                    client_cert: self.client_cert.clone(),
                    client_key: self.client_key.clone(),
                    danger_accept_invalid_certs: self.danger_accept_invalid_certs,
                    alpn: Vec::new(),
                }
                .build()
                .map_err(|e| e.to_string())
            })
            .clone()
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            addr: format!("127.0.0.1:{}", fluxmq_proto::DEFAULT_CLIENT_PORT),
            seeds: Vec::new(),
            client_name: "fluxmq-rust-client".into(),
            auth_token: None,
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            publish_timeout: Duration::from_secs(30),
            reconnect: ReconnectPolicy::default(),
            metadata_refresh_interval: Duration::from_secs(30),
            write_queue_capacity: 8192,
            ack_linger: Duration::from_millis(1),
            stats_interval: Duration::from_secs(2),
            tls: None,
            stamp_producer: true,
            idempotent: true,
            compression: Compression::None,
        }
    }
}

impl ClientOptions {
    /// Options with the given address and client name, defaults for everything else.
    pub fn new(addr: impl Into<String>, client_name: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            client_name: client_name.into(),
            ..Default::default()
        }
    }

    /// Options with several bootstrap addresses.
    pub fn with_seeds<I, S>(seeds: I, client_name: impl Into<String>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            seeds: seeds.into_iter().map(Into::into).collect(),
            client_name: client_name.into(),
            ..Default::default()
        }
    }

    /// Sets the bootstrap addresses.
    pub fn seeds<I, S>(mut self, seeds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.seeds = seeds.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the auth token sent in `Hello`.
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Sets the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Sets the publish timeout (overall deadline including retries).
    pub fn publish_timeout(mut self, timeout: Duration) -> Self {
        self.publish_timeout = timeout;
        self
    }

    /// Sets the reconnect policy.
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// Sets [`ClientOptions::stamp_producer`].
    pub fn stamp_producer(mut self, on: bool) -> Self {
        self.stamp_producer = on;
        self
    }

    /// Sets [`ClientOptions::idempotent`].
    pub fn idempotent(mut self, on: bool) -> Self {
        self.idempotent = on;
        self
    }

    /// Sets [`ClientOptions::compression`].
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Enables TLS for every connection.
    pub fn tls(mut self, tls: TlsOptions) -> Self {
        self.tls = Some(tls);
        self
    }

    /// The TLS settings in effect: [`ClientOptions::tls`], or the OS trust store when some
    /// address is written `tls://…`; `None` = plaintext.
    pub(crate) fn effective_tls(&self) -> Option<TlsOptions> {
        if let Some(t) = &self.tls {
            return Some(t.clone());
        }
        static SYSTEM: OnceLock<TlsOptions> = OnceLock::new();
        self.seed_list()
            .iter()
            .any(|s| s.starts_with(TLS_SCHEME))
            .then(|| SYSTEM.get_or_init(TlsOptions::new).clone())
    }

    /// Sets the periodic metadata refresh interval.
    pub fn metadata_refresh_interval(mut self, interval: Duration) -> Self {
        self.metadata_refresh_interval = interval;
        self
    }

    /// The effective bootstrap list: `seeds`, or `addr` split on commas.
    pub fn seed_list(&self) -> Vec<String> {
        let src: Vec<String> = if self.seeds.is_empty() {
            self.addr.split(',').map(str::to_string).collect()
        } else {
            self.seeds
                .iter()
                .flat_map(|s| s.split(',').map(str::to_string).collect::<Vec<_>>())
                .collect()
        };
        let mut out: Vec<String> = Vec::new();
        for s in src {
            let s = s.trim().to_string();
            if !s.is_empty() && !out.contains(&s) {
                out.push(s);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_selection() {
        assert!(ClientOptions::new("a:1", "x").effective_tls().is_none());
        assert!(
            ClientOptions::new("a:1,tls://b:2", "x")
                .effective_tls()
                .is_some()
        );
        let t = TlsOptions::new()
            .ca_pem(b"x".to_vec())
            .server_name("broker");
        let o = ClientOptions::new("a:1", "x").tls(t.clone());
        assert_eq!(o.effective_tls(), Some(t.clone()));
        // Bad CA: building fails once, the error is cached and shared by clones.
        assert!(t.client_tls().is_err());
        assert!(t.clone().client_tls().is_err());
        // Debug never prints key material.
        let k = TlsOptions::new().identity_pem(b"CERT".to_vec(), b"SECRET-KEY".to_vec());
        assert!(!format!("{k:?}").contains("SECRET"));
        assert!(
            TlsOptions::new()
                .danger_accept_invalid_certs(true)
                .client_tls()
                .is_ok()
        );
    }

    #[test]
    fn seed_list_prefers_seeds_and_splits_commas() {
        assert_eq!(
            ClientOptions::new("a:1, b:2,a:1", "x").seed_list(),
            vec!["a:1", "b:2"]
        );
        assert_eq!(
            ClientOptions::new("a:1", "x")
                .seeds(["c:3", "d:4,e:5"])
                .seed_list(),
            vec!["c:3", "d:4", "e:5"]
        );
    }
}

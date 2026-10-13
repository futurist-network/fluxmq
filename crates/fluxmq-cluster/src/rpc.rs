//! Node-to-node HTTP: message types of the cluster RPC listener (`--raft-addr`) and a small
//! JSON client that authenticates with the cluster secret ([`crate::CLUSTER_SECRET_HEADER`]).
//!
//! The same client serves the HTTP API's fan-out reads to other nodes' `/internal/v1/*`
//! routes (see [`InternalHttp`]).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fluxmq_proto::ChannelConfig;
use fluxmq_proto::cluster::NodeStatus;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::CLUSTER_SECRET_HEADER;

/// The node registry document at `cluster/nodes/<id>` (written by the controller).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: String,
    pub client_addr: String,
    pub http_addr: String,
    pub raft_addr: String,
    pub replication_addr: String,
    pub version: String,
    pub status: NodeStatus,
    /// Process start time (ms); a new value means the node restarted.
    pub incarnation: u64,
    /// v0.7: data-format features this process supports ([`FEATURE_BATCH_RECORDS`]).
    /// Missing on nodes before v0.7, and dropped when a pre-0.7 controller rewrites the
    /// record: both read as "no features", the conservative answer during an upgrade.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
}

/// Feature: stores and replicates v0.7 batch envelopes (`fluxmq_proto::batch`). A partition
/// leader writes envelopes only while every replica of the partition advertises it.
pub const FEATURE_BATCH_RECORDS: &str = "batch-records";

/// Features of this build (sent in every heartbeat).
pub fn local_features() -> Vec<String> {
    vec![FEATURE_BATCH_RECORDS.to_string()]
}

static FEATURE_OVERRIDES: parking_lot::Mutex<BTreeMap<String, Vec<String>>> =
    parking_lot::Mutex::new(BTreeMap::new());

/// Test hook (mixed-version clusters in one process): the features node `node` advertises
/// in its heartbeats from now on (`None` = this build's). Never used outside tests.
#[doc(hidden)]
pub fn override_features_for_tests(node: &str, features: Option<Vec<String>>) {
    let mut o = FEATURE_OVERRIDES.lock();
    match features {
        Some(f) => o.insert(node.to_string(), f),
        None => o.remove(node),
    };
}

/// Features node `node` (this process) advertises.
pub(crate) fn features_for(node: &str) -> Vec<String> {
    FEATURE_OVERRIDES
        .lock()
        .get(node)
        .cloned()
        .unwrap_or_else(local_features)
}

impl NodeRecord {
    /// Equal ignoring status.
    pub fn same_identity(&self, other: &NodeRecord) -> bool {
        self.id == other.id
            && self.client_addr == other.client_addr
            && self.http_addr == other.http_addr
            && self.raft_addr == other.raft_addr
            && self.replication_addr == other.replication_addr
            && self.version == other.version
            && self.incarnation == other.incarnation
            && self.features == other.features
    }

    /// Advertises [`FEATURE_BATCH_RECORDS`].
    pub fn supports_batches(&self) -> bool {
        self.features.iter().any(|f| f == FEATURE_BATCH_RECORDS)
    }
}

/// `POST /cluster/heartbeat` — sent every second to the controller (= Raft leader). Also the
/// join request of a node that isn't a Raft member yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub record: NodeRecord,
    /// Bytes of partition data on the node (v0.4, rebalancing); refreshed every few beats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatAck {
    /// Controller's applied metadata revision when it confirmed its leadership.
    pub revision: u64,
    pub controller: String,
}

/// `GET /cluster/info` — bootstrap probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoResponse {
    pub node_id: String,
    pub initialized: bool,
    pub leader: Option<String>,
}

/// Operations executed by the controller (`POST /cluster/controller`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControllerOp {
    CreateChannel(ChannelConfig),
    DeleteChannel(String),
    Reassign {
        channel: String,
        partition: u32,
        replicas: Vec<String>,
    },
    Drain(String),
    /// Graceful shutdown of `node` (process `incarnation`): mark it `Down` now, fail over what
    /// it still leads and drop it from every ISR, and ignore its further heartbeats — so peers
    /// don't wait for `node_timeout`.
    Leave {
        node: String,
        incarnation: u64,
    },
    /// v0.4 rebalancing (`fluxmq_proto::cluster` contract): status with live progress.
    RebalanceStatus,
    RebalanceConfig(fluxmq_proto::cluster::RebalanceConfig),
    Rebalance(fluxmq_proto::cluster::RebalanceRequest),
    RebalanceControl(RebalanceAction),
    /// v0.5: start (or report) the decommission of a node (`fluxmq_proto::cluster` contract).
    Decommission(String),
    /// v0.5: forget a `Down` node that holds no replicas (Raft membership + registry).
    Forget(String),
    /// v0.5: the checks and the plan of a decommission, nothing written (rebalance status).
    DecommissionDryRun(String),
}

/// `POST /api/cluster/rebalance/{pause,resume,cancel}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceAction {
    Pause,
    Resume,
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerOk {
    /// Revision to wait for locally before reading the effect.
    pub revision: u64,
    /// `Drain` / `Leave`: leaderships moved (`Leave`: + partitions whose ISR shrank).
    pub count: u32,
    /// Rebalance ops: the resulting status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebalance: Option<Box<fluxmq_proto::cluster::RebalanceStatus>>,
    /// Decommission / forget: the resulting status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decommission: Option<fluxmq_proto::cluster::DecommissionStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcErrorKind {
    /// Not the Raft leader; `leader`/`leader_addr` are hints.
    NotLeader,
    AlreadyExists,
    NotFound,
    Invalid,
    Conflict,
    Unavailable,
    Internal,
    /// v0.5: heartbeat refused because this process was removed from the cluster
    /// (decommissioned / forgotten). It stops its cluster roles; a restart joins as a new node.
    Removed,
}

/// Error body of every cluster RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct RpcError {
    pub kind: RpcErrorKind,
    pub message: String,
    pub leader: Option<String>,
    pub leader_addr: Option<String>,
}

impl RpcError {
    pub fn new(kind: RpcErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            leader: None,
            leader_addr: None,
        }
    }

    pub fn not_leader(leader: Option<(String, String)>) -> Self {
        let (leader, leader_addr) = leader.unzip();
        Self {
            kind: RpcErrorKind::NotLeader,
            message: "not the controller".into(),
            leader,
            leader_addr,
        }
    }
}

/// Transport-level failure (the request may or may not have been processed, except for
/// `Connect`, where it certainly was not).
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connect: {0}")]
    Connect(String),
    #[error("request timed out")]
    Timeout,
    #[error("http {0}: {1}")]
    Status(u16, String),
    #[error("transport: {0}")]
    Other(String),
}

impl TransportError {
    /// The request never reached the peer: safe to retry anything.
    pub fn is_connect(&self) -> bool {
        matches!(self, TransportError::Connect(_))
    }
}

impl From<reqwest::Error> for TransportError {
    fn from(e: reqwest::Error) -> Self {
        if e.is_connect() {
            TransportError::Connect(e.to_string())
        } else if e.is_timeout() {
            TransportError::Timeout
        } else if let Some(s) = e.status() {
            TransportError::Status(s.as_u16(), e.to_string())
        } else {
            TransportError::Other(e.to_string())
        }
    }
}

/// JSON-over-HTTP client for node-to-node calls. Cheap to clone.
#[derive(Clone)]
pub struct RpcClient {
    http: reqwest::Client,
    secret: Option<Arc<str>>,
    /// `http`, or `https` with cluster mutual TLS (v0.4).
    scheme: &'static str,
}

impl RpcClient {
    pub fn new(secret: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(1000))
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_nodelay(true)
            .build()
            .expect("building HTTP client");
        Self {
            http,
            secret: secret.filter(|s| !s.is_empty()).map(Arc::from),
            scheme: "http",
        }
    }

    /// Like [`new`](Self::new); with cluster TLS every call goes over `https` presenting the
    /// node certificate and verifying the peer against the cluster CA.
    pub fn with_tls(secret: Option<String>, tls: Option<&fluxmq_tls::ClusterTls>) -> Self {
        let Some(tls) = tls else {
            return Self::new(secret);
        };
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(1000))
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_nodelay(true)
            .tls_backend_preconfigured(tls.http_client_config())
            .build()
            .expect("building HTTPS client");
        Self {
            http,
            secret: secret.filter(|s| !s.is_empty()).map(Arc::from),
            scheme: "https",
        }
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.secret {
            Some(s) => rb.header(CLUSTER_SECRET_HEADER, s.as_ref()),
            None => rb,
        }
    }

    async fn finish<T: DeserializeOwned>(rb: reqwest::RequestBuilder) -> Result<T, TransportError> {
        let resp = rb.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(TransportError::Status(status.as_u16(), body));
        }
        Ok(resp.json::<T>().await?)
    }

    /// `POST {scheme}://{addr}{path}` with a JSON body.
    pub async fn post<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        addr: &str,
        path: &str,
        body: &Q,
        timeout: Duration,
    ) -> Result<R, TransportError> {
        let rb = self
            .http
            .post(format!("{}://{addr}{path}", self.scheme))
            .timeout(timeout)
            .json(body);
        Self::finish(self.auth(rb)).await
    }

    /// `POST {scheme}://{addr}{path}` with a raw body; returns the raw response body.
    pub async fn post_bytes(
        &self,
        addr: &str,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, TransportError> {
        let rb = self
            .http
            .post(format!("{}://{addr}{path}", self.scheme))
            .timeout(timeout)
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(body);
        let resp = self.auth(rb).send().await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            return Err(TransportError::Status(
                status.as_u16(),
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }
        Ok(bytes.to_vec())
    }

    /// `GET {scheme}://{addr}{path}?{query}`.
    pub async fn get<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        addr: &str,
        path: &str,
        query: &Q,
        timeout: Duration,
    ) -> Result<R, TransportError> {
        let rb = self
            .http
            .get(format!("{}://{addr}{path}", self.scheme))
            .timeout(timeout)
            .query(query);
        Self::finish(self.auth(rb)).await
    }
}

/// Authenticated JSON client for other nodes' `/internal/v1/*` HTTP routes (fan-out reads).
/// Obtained from [`crate::Cluster::internal_http`].
#[derive(Clone)]
pub struct InternalHttp {
    pub(crate) rpc: RpcClient,
    pub(crate) timeout: Duration,
}

impl InternalHttp {
    /// A client over `rpc` (normally obtained from [`crate::Cluster::internal_http`]; public
    /// for tests and tools).
    pub fn new(rpc: RpcClient, timeout: Duration) -> Self {
        Self { rpc, timeout }
    }

    /// `GET {scheme}://{http_addr}{path}`; returns the raw body (binary internal formats such
    /// as the telemetry node-stats snapshot). Same scheme / TLS identity / secret as every
    /// other internal call.
    pub async fn get_bytes(&self, http_addr: &str, path: &str) -> Result<Vec<u8>, TransportError> {
        let rb = self
            .rpc
            .http
            .get(format!("{}://{http_addr}{path}", self.rpc.scheme))
            .timeout(self.timeout);
        Self::raw(self.rpc.auth(rb)).await
    }

    async fn raw(rb: reqwest::RequestBuilder) -> Result<Vec<u8>, TransportError> {
        let resp = rb.send().await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            return Err(TransportError::Status(
                status.as_u16(),
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }
        Ok(bytes.to_vec())
    }

    /// The same client with another per-request timeout (long-running internal calls such as
    /// a backup partial).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// `GET {scheme}://{http_addr}{path}?{query}`.
    pub async fn get<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        http_addr: &str,
        path: &str,
        query: &Q,
    ) -> Result<R, TransportError> {
        self.rpc.get(http_addr, path, query, self.timeout).await
    }

    /// `POST {scheme}://{http_addr}{path}` with a JSON body.
    pub async fn post<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        http_addr: &str,
        path: &str,
        body: &Q,
    ) -> Result<R, TransportError> {
        self.rpc.post(http_addr, path, body, self.timeout).await
    }

    /// `DELETE {scheme}://{http_addr}{path}`; returns the raw response body. (Before the v0.4
    /// integration this hard-coded `http://`, so group deletion fan-out failed with cluster
    /// TLS.)
    pub async fn delete(&self, http_addr: &str, path: &str) -> Result<Vec<u8>, TransportError> {
        let rb = self
            .rpc
            .http
            .delete(format!("{}://{http_addr}{path}", self.rpc.scheme))
            .timeout(self.timeout);
        Self::raw(self.rpc.auth(rb)).await
    }

    /// `POST {scheme}://{http_addr}{path}` with a binary body (e.g. postcard).
    pub async fn post_bytes(
        &self,
        http_addr: &str,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, TransportError> {
        self.rpc
            .post_bytes(http_addr, path, content_type, body, self.timeout)
            .await
    }
}

/// `POST /internal/v1/publish` (HTTP port): a publish forwarded by the node that received a
/// partition-less `Publish` to the partition's leader. Body and response are postcard
/// ([`FORWARD_CONTENT_TYPE`]); the response is `Result<Vec<MessageId>, WireError>`.
pub const FORWARD_PUBLISH_PATH: &str = "/internal/v1/publish";
pub const FORWARD_CONTENT_TYPE: &str = "application/x-fluxmq-postcard";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForwardPublish {
    pub channel: String,
    pub partition: u32,
    pub messages: Vec<fluxmq_proto::PublishMessage>,
}

/// `POST /internal/v1/publish_batch` (HTTP port, v0.7): a partition-less `PublishBatch`
/// forwarded raw to the partition's leader. Postcard body ([`FORWARD_CONTENT_TYPE`]); the
/// response is `Result<(u64 /* base_offset */, u32 /* count */), WireError>`.
pub const FORWARD_PUBLISH_BATCH_PATH: &str = "/internal/v1/publish_batch";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForwardPublishBatch {
    pub channel: String,
    pub partition: u32,
    pub records: fluxmq_proto::batch::RecordsSection,
}

/// Constant-time comparison of a presented secret with the configured one. Without a
/// configured secret nothing matches (internal endpoints are closed).
pub fn secret_matches(presented: Option<&str>, expected: Option<&str>) -> bool {
    match expected {
        None => false,
        Some(expected) => presented.is_some_and(|p| {
            let (a, b) = (p.as_bytes(), expected.as_bytes());
            let mut diff = a.len() ^ b.len();
            for i in 0..a.len().max(b.len()) {
                diff |=
                    usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
            }
            diff == 0
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_compare() {
        assert!(!secret_matches(None, None));
        assert!(!secret_matches(Some("x"), None));
        assert!(secret_matches(Some("s3"), Some("s3")));
        assert!(!secret_matches(Some("s4"), Some("s3")));
        assert!(!secret_matches(Some("s3x"), Some("s3")));
        assert!(!secret_matches(None, Some("s3")));
    }
}

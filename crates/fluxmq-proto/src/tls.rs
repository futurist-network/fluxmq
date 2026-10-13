//! TLS contract (v0.4).
//!
//! Three independently configurable surfaces, all rustls (aws-lc-rs provider, TLS 1.2+1.3):
//!
//! * **client** — the binary protocol port (7420). Wire framing is unchanged; TLS wraps the
//!   TCP stream before `Hello`. Optional mTLS: with a client CA configured, a client
//!   certificate is required and verified; its subject CN is exposed as the principal hint
//!   (`tls:<cn>`) in audit/connection info. Token auth still applies on top (mTLS ≠ RBAC).
//! * **http** — REST / WS / UI port (7421). Same certificate as `client` unless overridden.
//! * **cluster** — node-to-node traffic: Raft (7422), replication (7423) and internal HTTP
//!   (`/internal/v1/*`). Always mutual: every node presents its node certificate and verifies
//!   peers against the cluster CA. The cluster secret header stays required (defense in depth).
//!
//! Plain and TLS are not mixed on one port: a TLS-enabled listener rejects plaintext.
//! Certificates and keys are PEM files; they are re-read when their mtime changes
//! (checked every 30 s) and on SIGHUP — new handshakes use the new chain, live connections
//! are kept. A reload that fails to parse keeps the previous certificate and raises an
//! alert / event (`tls_reload_failed`).
//!
//! Server flags / env (names are part of the contract):
//! `--tls-cert/--tls-key` (`FLUXMQ_TLS_CERT/KEY`) enable TLS on client + http,
//! `--tls-client-ca` (`FLUXMQ_TLS_CLIENT_CA`) enables mTLS on the client port,
//! `--http-tls-cert/--http-tls-key` override http, `--http-tls off` keeps http plaintext,
//! `--cluster-tls-cert/--cluster-tls-key/--cluster-tls-ca` (`FLUXMQ_CLUSTER_TLS_*`) enable
//! cluster TLS (all three required; all nodes of a cluster must agree — a node without
//! cluster TLS can't join a TLS cluster and vice versa).
//!
//! SDK: `ClientOptions::tls(TlsOptions)`; address forms `host:port` (plaintext unless
//! `tls` is set) and `tls://host:port`. Server name defaults to the host part.
//! `fluxmq tls gen-dev --out <dir> [--nodes n1,n2,n3] [--hosts localhost,127.0.0.1]` writes a
//! development CA plus server/node/client certificates (never for production). Output files
//! (contract): `ca.pem`, `ca-key.pem`, `server.pem`, `server-key.pem` (SANs = `--hosts`),
//! `client.pem`, `client-key.pem` (CN `client`), and per node `node-<id>.pem`,
//! `node-<id>-key.pem` (CN = node id, SANs = `--hosts`). The CA signs all of them, so one CA
//! serves as client CA and cluster CA in development. User guide: `docs/GUIDE.ru.md`, section
//! "TLS".

use serde::{Deserialize, Serialize};

/// `GET /api/tls` (viewer) — what is enabled and when certificates expire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsStatus {
    /// The node that answered — TLS state is node-local (each node has its own certificates).
    pub node_id: String,
    pub client: TlsEndpointStatus,
    pub http: TlsEndpointStatus,
    pub cluster: TlsEndpointStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsEndpointStatus {
    pub enabled: bool,
    /// Client certificates required (always true for `cluster` when enabled).
    pub mutual: bool,
    /// Leaf certificate currently served; `None` when disabled.
    pub certificate: Option<CertificateInfo>,
    /// Last successful (re)load, ms since epoch.
    pub loaded_at_ms: Option<u64>,
    /// Last reload failure (short error), cleared on the next success.
    pub last_error: Option<String>,
    /// Failed handshakes since start (bad cert, plaintext on a TLS port, protocol mismatch).
    pub handshake_failures_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertificateInfo {
    pub subject: String,
    pub issuer: String,
    /// DNS / IP subject alternative names.
    pub sans: Vec<String>,
    pub not_before_ms: u64,
    pub not_after_ms: u64,
    /// SHA-256 of the DER leaf, lowercase hex.
    pub sha256_fingerprint: String,
}

/// Days before `not_after` at which the `tls_cert_expiring` alert fires (warning; critical
/// at 3 days / expired).
pub const CERT_EXPIRY_WARNING_DAYS: u64 = 14;

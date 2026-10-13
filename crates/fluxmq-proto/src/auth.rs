//! RBAC contract (roadmap stage 7). REST bodies for `/api/auth/*` and the principal model.
//!
//! Roles are cumulative: `subscriber` ⊂ `viewer` ⊂ `operator` ⊂ `admin`.
//!
//! | role       | REST/WS                                                               | client protocol            |
//! |------------|-----------------------------------------------------------------------|----------------------------|
//! | subscriber | (v0.10) only live subscriptions on channels matching `scopes.subscribe` | nothing                    |
//! | viewer   | all reads (`GET`), WebSocket telemetry, message browsing                | only what `scopes` allow   |
//! | operator | + DLQ retry/discard/export, replay, reset offsets, REST publish         | publish + consume anything |
//! | admin    | + channel create/update/delete, routes, tokens, cluster operations      | + create channels          |
//!
//! `scopes` grant data-plane rights to any token (typically a `viewer` service token):
//! glob patterns over channel names (`orders.*`, `*`).
//!
//! **Live subscriptions (v0.10).** `GET /api/channels/{name}/live` (WebSocket) and
//! `/api/channels/{name}/events` (SSE) need `viewer`+ or a `scopes.subscribe` pattern matching
//! the channel. A `subscriber` token must have at least one `subscribe` pattern and no
//! `publish`/`consume` patterns; it cannot call any other route, cannot use the client
//! protocol and cannot be bound to a certificate. See `docs/design/v0.10-live-gateway.md`.
//!
//! REST permissions in detail (the central table is `fluxmq-api/src/auth.rs`): every `GET`
//! needs viewer, except `GET /api/dlq/export` (operator: bulk payload dump) and
//! `GET /api/auth/tokens` (admin). `POST /api/routes/test`, `/api/filter/parse` and
//! `/api/filter/render` are read-only (viewer). Deleting a consumer group is admin.
//!
//! **Modes and the first token.** The broker is [`AuthMode::Open`] iff it has no bootstrap
//! `--admin-token` and no stored token: every caller is the anonymous admin. In open mode the
//! first `POST /api/auth/tokens` must create an **admin** token (any other role → 400
//! `bad_request`, so nobody can lock themselves out with a viewer token). Creating it switches
//! the broker to enforced mode immediately and the response has `adopt = true`: the UI stores
//! that token and uses it from then on. Revoking the last stored token is refused (400) when
//! there is no bootstrap token, so a broker never silently falls back to open mode.
//!
//! **Token names** are 1–64 characters; `admin` and `anonymous` (any case) are reserved for
//! the bootstrap principal and open mode.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// v0.10: live subscriptions only (on `scopes.subscribe` channels). Lowest role.
    Subscriber,
    Viewer,
    Operator,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClientScopes {
    /// Channel globs this token may publish to.
    #[serde(default)]
    pub publish: Vec<String>,
    /// Channel globs this token may consume from.
    #[serde(default)]
    pub consume: Vec<String>,
    /// v0.10: channel globs this token may open live subscriptions (WS/SSE) on. Omitted from
    /// JSON when empty, so stored tokens without it stay byte-identical.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscribe: Vec<String>,
}

/// Stored token metadata (never contains the secret).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenInfo {
    pub id: String,
    pub name: String,
    pub role: Role,
    pub scopes: ClientScopes,
    pub created_at_ms: u64,
    pub created_by: String,
    pub expires_at_ms: Option<u64>,
    pub last_used_ms: Option<u64>,
}

/// `POST /api/auth/tokens` (admin).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTokenRequest {
    pub name: String,
    pub role: Role,
    #[serde(default)]
    pub scopes: ClientScopes,
    /// Lifetime; `None` = no expiry.
    pub ttl_secs: Option<u64>,
}

/// The secret is shown exactly once. Format: `fmq_<id>_<secret>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTokenResponse {
    pub token: String,
    pub info: TokenInfo,
    /// True when this token switched the broker from open to enforced mode (first token):
    /// the caller must adopt it, since anonymous access stopped working.
    #[serde(default)]
    pub adopt: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// No bootstrap admin token and no stored tokens: everyone is an anonymous admin (dev).
    Open,
    Enforced,
}

/// `GET /api/auth/me`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhoAmI {
    pub mode: AuthMode,
    /// Token name, `"admin"` for the bootstrap token, `"anonymous"` in open mode.
    pub name: String,
    pub token_id: Option<String>,
    pub role: Role,
    pub scopes: ClientScopes,
}

// ---------------------------------------------------------------------------------------------
// v0.5: revocation, last-admin guard, certificate bindings
// ---------------------------------------------------------------------------------------------
//
// * Revoking (or expiry of) a token closes every client-protocol connection and WS session
//   authenticated with it within 1 s, on every node (connections watch the token in meta):
//   client gets `Goodbye { reason: "token revoked" | "token expired" }`, WS closes with code
//   4401. HTTP requests are already checked per request.
// * Last-admin guard: revoking the last active (unexpired) `admin` token while other tokens
//   exist and no bootstrap `--admin-token` is configured → 409 with
//   `error = ERROR_LAST_ADMIN`. (Revoking the very last token — returning to open mode — stays
//   refused too: open mode is only for a broker that never had tokens.)
// * Certificate bindings map a verified mTLS client certificate subject CN to a principal,
//   so a client can authenticate with its certificate alone (no `auth_token`). A token in
//   `Hello` still wins when both are present. Stored in meta `auth/cert-bindings/<cn>`.
//   REST (admin): `GET/POST /api/auth/cert-bindings`, `DELETE /api/auth/cert-bindings/{cn}`.
//   The principal's actor in audit is `cert:<cn>`. Bindings apply to the client port only
//   (HTTP keeps token auth). Without client-port mTLS bindings are inert (listed with a
//   warning flag in `GET`).

/// `ApiError.error` when an operation would remove the last admin.
pub const ERROR_LAST_ADMIN: &str = "last_admin";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertBinding {
    /// Exact certificate subject CN.
    pub cn: String,
    pub role: Role,
    #[serde(default)]
    pub scopes: ClientScopes,
    pub created_at_ms: u64,
    pub created_by: String,
    pub last_used_ms: Option<u64>,
}

/// `POST /api/auth/cert-bindings`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCertBindingRequest {
    pub cn: String,
    pub role: Role,
    #[serde(default)]
    pub scopes: ClientScopes,
}

/// `GET /api/auth/cert-bindings`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertBindingList {
    pub bindings: Vec<CertBinding>,
    /// False when the client port has no mTLS (`--tls-client-ca`): bindings are inert.
    pub client_mtls_enabled: bool,
}

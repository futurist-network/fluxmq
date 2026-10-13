//! RBAC for FluxMQ (roadmap stage 7). Contract: `fluxmq_proto::auth` (roles, scopes, REST bodies).
//!
//! * Tokens are `fmq_<id>_<secret>`; only `sha256(secret)` is stored, under meta keys
//!   `auth/tokens/<id>` (replicated cluster-wide when the meta store is Raft-backed).
//! * The bootstrap admin token (`--admin-token`) is never stored; it always authenticates as
//!   principal `admin` with role admin.
//! * Mode is [`AuthMode::Open`] iff there is no bootstrap token AND no stored token — everyone
//!   is an anonymous admin (dev). The first token created in open mode must be an admin token
//!   (else 400); it switches the broker to enforced mode and the response has `adopt = true`
//!   so the UI stores the new token.
//! * Authorization: see the role table in `fluxmq_proto::auth`. Scopes are channel globs
//!   (`*` and `?`), checked for `Publish`/`Consume` when the role alone doesn't grant it.
//!
//! Implementation notes:
//!
//! * `id` is 12 chars of `[a-z2-7]` (60 bits, not secret: shown in listings and the audit log);
//!   the secret is 32 bytes from the OS CSPRNG, hex-encoded (256 bits).
//! * Every comparison involving secret material is constant-time ([`subtle`]); the bootstrap
//!   token is compared via its SHA-256 so even its length does not leak.
//! * The in-memory index is the source of truth for the hot path (no I/O per request). It is
//!   kept in sync with the meta store by a watch task (full reload on `Lagged`), and updated
//!   directly by this node's own writes (read-your-writes). Per-key revisions make stale
//!   events harmless (a revoked token can never be resurrected by a late event).
//! * `last_used_ms` is persisted lazily: at most once per minute per token, by a spawned
//!   task with a compare-and-set on the key's revision (never recreates a revoked token).
//!
//! v0.5 (contract: the v0.5 block of `fluxmq_proto::auth`):
//!
//! * **Live sessions.** Every index change (local write, replicated meta event, full reload)
//!   bumps a generation counter. [`AuthService::session_watch`] returns a future a
//!   long-lived session (client connection, WS) races against its work: it re-checks the
//!   principal's credential on every bump and sleeps until the token's expiry in between, so
//!   a revoke, an expiry or a binding deletion on any node ends the session well within 1 s.
//!   The bootstrap admin and open-mode principals never end.
//! * **Last-admin guard.** Revoking the last active admin token while other tokens exist and
//!   no bootstrap token is configured → [`AuthError::LastAdmin`]. When exactly one other
//!   active admin remains, the delete carries a precondition on that admin's key, so two
//!   nodes revoking the last two admins concurrently cannot both succeed.
//! * **Certificate bindings** (`auth/cert-bindings/<cn>`) map a verified client-certificate
//!   CN to a principal (actor `cert:<cn>`) for the client port; see
//!   [`AuthService::authenticate_client`]. `last_used_ms` is persisted like tokens'.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use fluxmq_meta::{LocalMetaStore, MetaError, Precondition, SharedMetaStore};
use fluxmq_proto::auth::{
    AuthMode, CertBinding, ClientScopes, CreateCertBindingRequest, CreateTokenRequest,
    CreateTokenResponse, Role, TokenInfo, WhoAmI,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::broadcast::error::RecvError;

/// Meta key prefix of stored tokens.
pub const TOKEN_PREFIX: &str = "auth/tokens/";
/// Meta key prefix of certificate bindings (`auth/cert-bindings/<cn>`).
pub const CERT_BINDING_PREFIX: &str = "auth/cert-bindings/";
/// Max certificate-binding CN length.
pub const MAX_CN_LEN: usize = 256;
/// Upper bound on how long a session sleeps between credential re-checks when nothing
/// changes (guards against wall-clock jumps around a token's expiry).
pub const SESSION_RECHECK_MAX: std::time::Duration = std::time::Duration::from_secs(30);
/// Length of the public token id.
pub const TOKEN_ID_LEN: usize = 12;
/// Bytes of entropy in a token secret.
pub const SECRET_BYTES: usize = 32;
/// Max token name length.
pub const MAX_NAME_LEN: usize = 64;
/// Max patterns per scope list.
pub const MAX_SCOPE_PATTERNS: usize = 64;
/// Max token lifetime (10 years).
pub const MAX_TTL_SECS: u64 = 10 * 365 * 24 * 3600;
/// `last_used_ms` is persisted at most this often per token.
pub const LAST_USED_WRITE_INTERVAL_MS: u64 = 60_000;

const ID_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const RESERVED_NAMES: [&str; 2] = ["admin", "anonymous"];

/// An authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// Token name; `"admin"` for the bootstrap token, `"anonymous"` in open mode,
    /// `"cert:<cn>"` for a certificate binding.
    pub name: String,
    pub token_id: Option<String>,
    pub role: Role,
    pub scopes: ClientScopes,
    /// Set when authenticated by a certificate binding (client port mTLS, v0.5).
    pub cert_cn: Option<String>,
}

impl Principal {
    pub fn anonymous_admin() -> Self {
        Self {
            name: "anonymous".into(),
            token_id: None,
            role: Role::Admin,
            scopes: ClientScopes::default(),
            cert_cn: None,
        }
    }

    pub fn bootstrap_admin() -> Self {
        Self {
            name: "admin".into(),
            token_id: None,
            role: Role::Admin,
            scopes: ClientScopes::default(),
            cert_cn: None,
        }
    }

    /// The principal of a certificate binding: actor `cert:<cn>`.
    pub fn from_binding(binding: &CertBinding) -> Self {
        Self {
            name: format!("cert:{}", binding.cn),
            token_id: None,
            role: binding.role,
            scopes: binding.scopes.clone(),
            cert_cn: Some(binding.cn.clone()),
        }
    }

    /// Audit-log actor: `name`, `name#token_id`, or `cert:<cn>` for a certificate binding.
    pub fn actor(&self) -> String {
        match &self.token_id {
            Some(id) => format!("{}#{id}", self.name),
            None => self.name.clone(),
        }
    }
}

/// What a caller wants to do. HTTP routes map to these in `fluxmq-api`'s permission table;
/// client-protocol requests map in `fluxmq-server`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action<'a> {
    /// Any read: REST GETs, WS telemetry, message browsing.
    Read,
    /// DLQ retry/discard/export, replay, reset offsets, REST publish.
    Operate,
    /// Channel/route/token/cluster administration.
    Admin,
    Publish(&'a str),
    Consume(&'a str),
    CreateChannel(&'a str),
    /// v0.10: open a live subscription (WebSocket / SSE) on a channel.
    Subscribe(&'a str),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// Missing/invalid/expired token → HTTP 401 / wire `Unauthorized`.
    #[error("invalid or missing auth token")]
    Unauthenticated,
    /// Valid token, insufficient rights → HTTP 403 / wire `Forbidden`.
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("token not found")]
    NotFound,
    #[error("certificate binding not found")]
    BindingNotFound,
    /// Would remove the last active admin → HTTP 409 `last_admin`.
    #[error("{0}")]
    LastAdmin(String),
    /// Duplicate / concurrent modification → HTTP 409 `conflict`.
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Meta(#[from] MetaError),
}

/// What is persisted under `auth/tokens/<id>`: the public metadata plus `sha256(secret)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredToken {
    #[serde(flatten)]
    pub info: TokenInfo,
    /// Lowercase hex SHA-256 of the secret part of the token.
    pub secret_sha256: String,
}

struct Entry {
    info: TokenInfo,
    hash: [u8; 32],
    /// Last successful use seen on this node (ms); `0` = never.
    last_used: AtomicU64,
    /// When this node last scheduled a `last_used_ms` write (ms).
    last_persisted: AtomicU64,
}

impl Entry {
    fn from_stored(stored: StoredToken) -> Option<Self> {
        let hash = decode_hash(&stored.secret_sha256)?;
        let used = stored.info.last_used_ms.unwrap_or(0);
        Some(Self {
            info: stored.info,
            hash,
            last_used: AtomicU64::new(used),
            last_persisted: AtomicU64::new(used),
        })
    }

    fn info(&self) -> TokenInfo {
        let mut info = self.info.clone();
        let seen = self.last_used.load(Ordering::Relaxed);
        if seen > info.last_used_ms.unwrap_or(0) {
            info.last_used_ms = Some(seen);
        }
        info
    }
}

/// A certificate binding plus this node's last-use bookkeeping.
struct BindingEntry {
    binding: CertBinding,
    last_used: AtomicU64,
    last_persisted: AtomicU64,
}

impl BindingEntry {
    fn new(binding: CertBinding) -> Self {
        let used = binding.last_used_ms.unwrap_or(0);
        Self {
            binding,
            last_used: AtomicU64::new(used),
            last_persisted: AtomicU64::new(used),
        }
    }

    fn binding(&self) -> CertBinding {
        let mut b = self.binding.clone();
        let seen = self.last_used.load(Ordering::Relaxed);
        if seen > b.last_used_ms.unwrap_or(0) {
            b.last_used_ms = Some(seen);
        }
        b
    }
}

#[derive(Default)]
struct Index {
    tokens: HashMap<String, Arc<Entry>>,
    /// Certificate bindings by CN.
    bindings: HashMap<String, Arc<BindingEntry>>,
    /// Last applied revision per binding CN (tombstones included), like `revs`.
    binding_revs: HashMap<String, u64>,
    /// Last applied revision per token id, including deletions (tombstones), so a stale
    /// event can never resurrect a revoked token.
    revs: HashMap<String, u64>,
    /// Events at or below this revision are already reflected (set by full loads).
    floor: u64,
}

impl Index {
    fn apply(&mut self, id: &str, revision: u64, value: Option<StoredToken>) {
        if revision <= self.floor || self.revs.get(id).is_some_and(|r| revision <= *r) {
            return;
        }
        self.revs.insert(id.to_string(), revision);
        match value.and_then(Entry::from_stored) {
            Some(entry) => {
                // Keep this node's knowledge of recent use across metadata updates.
                if let Some(old) = self.tokens.get(id) {
                    let used = old.last_used.load(Ordering::Relaxed);
                    entry.last_used.fetch_max(used, Ordering::Relaxed);
                    let persisted = old.last_persisted.load(Ordering::Relaxed);
                    entry.last_persisted.fetch_max(persisted, Ordering::Relaxed);
                }
                self.tokens.insert(id.to_string(), Arc::new(entry));
            }
            None => {
                self.tokens.remove(id);
            }
        }
    }

    fn apply_binding(&mut self, cn: &str, revision: u64, value: Option<CertBinding>) {
        if revision <= self.floor || self.binding_revs.get(cn).is_some_and(|r| revision <= *r) {
            return;
        }
        self.binding_revs.insert(cn.to_string(), revision);
        match value {
            Some(binding) => {
                let entry = BindingEntry::new(binding);
                if let Some(old) = self.bindings.get(cn) {
                    entry
                        .last_used
                        .fetch_max(old.last_used.load(Ordering::Relaxed), Ordering::Relaxed);
                    entry.last_persisted.fetch_max(
                        old.last_persisted.load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                }
                self.bindings.insert(cn.to_string(), Arc::new(entry));
            }
            None => {
                self.bindings.remove(cn);
            }
        }
    }

    /// Why a session authenticated as `principal` must end now, if it must.
    fn session_state(&self, principal: &Principal, now: u64) -> SessionState {
        if let Some(id) = &principal.token_id {
            return match self.tokens.get(id) {
                None => SessionState::Ended(SessionEnd::TokenRevoked),
                Some(e) => match e.info.expires_at_ms {
                    Some(exp) if exp <= now => SessionState::Ended(SessionEnd::TokenExpired),
                    Some(exp) => SessionState::Until(exp),
                    None => SessionState::Valid,
                },
            };
        }
        if let Some(cn) = &principal.cert_cn {
            return if self.bindings.contains_key(cn) {
                SessionState::Valid
            } else {
                SessionState::Ended(SessionEnd::BindingRevoked)
            };
        }
        // Bootstrap admin / open-mode anonymous: never ends.
        SessionState::Forever
    }
}

/// Why a live session was closed by [`AuthService::session_watch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEnd {
    TokenRevoked,
    TokenExpired,
    /// The certificate binding the session authenticated with was deleted.
    BindingRevoked,
}

impl SessionEnd {
    /// `Goodbye.reason` / WS close reason.
    pub fn reason(self) -> &'static str {
        match self {
            SessionEnd::TokenRevoked => "token revoked",
            SessionEnd::TokenExpired => "token expired",
            SessionEnd::BindingRevoked => "certificate binding revoked",
        }
    }
}

enum SessionState {
    Ended(SessionEnd),
    /// Valid until this wall-clock time (ms).
    Until(u64),
    Valid,
    /// Not tied to a revocable credential.
    Forever,
}

/// Cheap to clone.
#[derive(Clone)]
pub struct AuthService {
    inner: Arc<Inner>,
}

struct Inner {
    /// `sha256(bootstrap token)`; compared in constant time.
    bootstrap_hash: Option<[u8; 32]>,
    meta: SharedMetaStore,
    /// `false` for the in-memory stores used by tests and dev defaults.
    persistent: bool,
    index: RwLock<Index>,
    /// Serializes this node's create/revoke (the last-token guard).
    write_lock: tokio::sync::Mutex<()>,
    /// Bumped on every index change; live sessions re-check their credential on it.
    changes: tokio::sync::watch::Sender<u64>,
    /// The client port verifies client certificates (`--tls-client-ca`): bindings are live.
    client_mtls: AtomicBool,
}

impl Inner {
    fn notify_changed(&self) {
        self.changes.send_modify(|g| *g = g.wrapping_add(1));
    }

    fn apply_token(&self, id: &str, revision: u64, value: Option<StoredToken>) {
        self.index.write().apply(id, revision, value);
        self.notify_changed();
    }

    fn apply_binding(&self, cn: &str, revision: u64, value: Option<CertBinding>) {
        self.index.write().apply_binding(cn, revision, value);
        self.notify_changed();
    }
}

impl AuthService {
    /// Loads stored tokens from `meta` and keeps them in sync via `meta.watch()`.
    pub async fn open(
        meta: SharedMetaStore,
        bootstrap_admin_token: Option<String>,
    ) -> Result<Self, AuthError> {
        let svc = Self::build(meta, bootstrap_admin_token, true);
        // Subscribe before loading so no change between the load and the watch is lost.
        let rx = svc.inner.meta.watch();
        svc.reload();
        let weak = Arc::downgrade(&svc.inner);
        tokio::spawn(watch_loop(weak, rx));
        Ok(svc)
    }

    /// Open mode, no persistence (defaults for tests / `AppState::new`).
    pub fn disabled() -> Self {
        Self::in_memory(None)
    }

    /// Memory-only token store with an optional bootstrap admin token (tests, dev defaults).
    /// Works without a Tokio runtime; tokens created through it live as long as the service.
    pub fn in_memory(bootstrap_admin_token: Option<String>) -> Self {
        Self::build(
            Arc::new(LocalMetaStore::in_memory()),
            bootstrap_admin_token,
            false,
        )
    }

    fn build(meta: SharedMetaStore, bootstrap: Option<String>, persistent: bool) -> Self {
        let bootstrap_hash = bootstrap
            .filter(|t| !t.is_empty())
            .map(|t| sha256(t.as_bytes()));
        Self {
            inner: Arc::new(Inner {
                bootstrap_hash,
                meta,
                persistent,
                index: RwLock::new(Index::default()),
                write_lock: tokio::sync::Mutex::new(()),
                changes: tokio::sync::watch::Sender::new(0),
                client_mtls: AtomicBool::new(false),
            }),
        }
    }

    /// `true` when tokens are stored in a durable/replicated meta store (see [`Self::open`]).
    pub fn is_persistent(&self) -> bool {
        self.inner.persistent
    }

    /// `true` when a bootstrap `--admin-token` is configured.
    pub fn has_bootstrap_token(&self) -> bool {
        self.inner.bootstrap_hash.is_some()
    }

    pub fn mode(&self) -> AuthMode {
        if self.inner.bootstrap_hash.is_some() || !self.inner.index.read().tokens.is_empty() {
            AuthMode::Enforced
        } else {
            AuthMode::Open
        }
    }

    /// Resolves a presented bearer token (without the `Bearer ` prefix). Updates
    /// `last_used_ms` lazily (at most once a minute per token). Constant-time comparisons.
    pub fn authenticate(&self, presented: Option<&str>) -> Result<Principal, AuthError> {
        if self.mode() == AuthMode::Open {
            return Ok(Principal::anonymous_admin());
        }
        let presented = presented
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or(AuthError::Unauthenticated)?;

        if let Some(expected) = &self.inner.bootstrap_hash
            && bool::from(sha256(presented.as_bytes()).ct_eq(expected))
        {
            return Ok(Principal::bootstrap_admin());
        }

        let (id, secret) = parse_token(presented).ok_or(AuthError::Unauthenticated)?;
        let entry = self.inner.index.read().tokens.get(id).cloned();
        // Hash even for unknown ids so the work done doesn't depend on the lookup (ids are
        // public anyway; this just keeps the code path uniform).
        let got = sha256(secret.as_bytes());
        let Some(entry) = entry else {
            return Err(AuthError::Unauthenticated);
        };
        if !bool::from(got.ct_eq(&entry.hash)) {
            return Err(AuthError::Unauthenticated);
        }
        let now = fluxmq_proto::now_ms();
        if entry.info.expires_at_ms.is_some_and(|exp| exp <= now) {
            return Err(AuthError::Unauthenticated);
        }
        self.touch(&entry, now);
        Ok(Principal {
            name: entry.info.name.clone(),
            token_id: Some(entry.info.id.clone()),
            role: entry.info.role,
            scopes: entry.info.scopes.clone(),
            cert_cn: None,
        })
    }

    /// Client-port authentication (v0.5): a token in `Hello` wins; without one, the CN of a
    /// verified client certificate (`cert_cn`, mTLS) maps to its certificate binding. Open
    /// mode stays the anonymous admin. No token and no binding → `Unauthenticated`.
    pub fn authenticate_client(
        &self,
        token: Option<&str>,
        cert_cn: Option<&str>,
    ) -> Result<Principal, AuthError> {
        let has_token = token.is_some_and(|t| !t.trim().is_empty());
        if self.mode() == AuthMode::Open || has_token {
            return self.authenticate(token);
        }
        let entry = cert_cn.and_then(|cn| self.inner.index.read().bindings.get(cn).cloned());
        let Some(entry) = entry else {
            return Err(AuthError::Unauthenticated);
        };
        self.touch_binding(&entry, fluxmq_proto::now_ms());
        Ok(Principal::from_binding(&entry.binding))
    }

    /// Records whether the client port verifies client certificates (`--tls-client-ca`);
    /// without it certificate bindings are inert (reported by `GET /api/auth/cert-bindings`).
    pub fn set_client_mtls(&self, enabled: bool) {
        self.inner.client_mtls.store(enabled, Ordering::Relaxed);
    }

    pub fn client_mtls_enabled(&self) -> bool {
        self.inner.client_mtls.load(Ordering::Relaxed)
    }

    /// A future that resolves when the session authenticated as `principal` must end: its
    /// token was revoked (on any node) or expired, or its certificate binding was deleted.
    /// Never resolves for the bootstrap admin and the open-mode anonymous admin. Long-lived
    /// sessions race it against their work (cancel-safe; holds only a weak reference).
    pub fn session_watch(
        &self,
        principal: &Principal,
    ) -> impl Future<Output = SessionEnd> + Send + use<> {
        let inner = Arc::downgrade(&self.inner);
        let mut changes = self.inner.changes.subscribe();
        let principal = principal.clone();
        async move {
            loop {
                // Subscribed before the check: a change racing it still wakes us.
                let state = match inner.upgrade() {
                    Some(inner) => inner
                        .index
                        .read()
                        .session_state(&principal, fluxmq_proto::now_ms()),
                    None => SessionState::Forever,
                };
                let sleep = match state {
                    SessionState::Ended(end) => return end,
                    SessionState::Forever => return std::future::pending().await,
                    SessionState::Valid => SESSION_RECHECK_MAX,
                    SessionState::Until(exp) => {
                        let left = exp.saturating_sub(fluxmq_proto::now_ms()).saturating_add(1);
                        std::time::Duration::from_millis(left).min(SESSION_RECHECK_MAX)
                    }
                };
                tokio::select! {
                    r = changes.changed() => {
                        if r.is_err() {
                            // Service dropped: nothing can revoke anymore.
                            return std::future::pending().await;
                        }
                    }
                    () = tokio::time::sleep(sleep) => {}
                }
            }
        }
    }

    /// Certificate bindings, sorted by CN.
    pub fn list_cert_bindings(&self) -> Vec<CertBinding> {
        let mut out: Vec<CertBinding> = self
            .inner
            .index
            .read()
            .bindings
            .values()
            .map(|e| e.binding())
            .collect();
        out.sort_by(|a, b| a.cn.cmp(&b.cn));
        out
    }

    /// Admin only. A CN that already has a binding → [`AuthError::Conflict`].
    pub async fn create_cert_binding(
        &self,
        req: CreateCertBindingRequest,
        by: &Principal,
    ) -> Result<CertBinding, AuthError> {
        self.authorize(by, Action::Admin)?;
        let cn = validate_cn(&req.cn)?;
        if req.role == Role::Subscriber {
            return Err(AuthError::Invalid(
                "role subscriber only applies to live subscriptions over HTTP; certificate \
                 bindings authenticate the client port"
                    .into(),
            ));
        }
        let scopes = validate_client_scopes(req.role, &req.scopes)?;
        let binding = CertBinding {
            cn: cn.clone(),
            role: req.role,
            scopes,
            created_at_ms: fluxmq_proto::now_ms(),
            created_by: by.actor(),
            last_used_ms: None,
        };
        let _guard = self.inner.write_lock.lock().await;
        let duplicate = || AuthError::Conflict(format!("a binding for CN '{cn}' already exists"));
        if self.inner.index.read().bindings.contains_key(&cn) {
            return Err(duplicate());
        }
        let key = format!("{CERT_BINDING_PREFIX}{cn}");
        let revision = match self
            .inner
            .meta
            .put(
                &key,
                serde_json::to_value(&binding).map_err(MetaError::from)?,
                Some(Precondition {
                    key: key.clone(),
                    expected_mod_revision: 0,
                }),
            )
            .await
        {
            Ok(r) => r,
            Err(MetaError::Conflict { .. }) => return Err(duplicate()),
            Err(e) => return Err(e.into()),
        };
        self.inner.meta.wait_for(revision).await?;
        self.inner
            .apply_binding(&cn, revision, Some(binding.clone()));
        tracing::info!(cn = %cn, role = ?binding.role, created_by = %binding.created_by, "certificate binding created");
        Ok(binding)
    }

    /// Deletes a binding; sessions authenticated with it are closed (see
    /// [`Self::session_watch`]).
    pub async fn delete_cert_binding(&self, cn: &str) -> Result<CertBinding, AuthError> {
        let _guard = self.inner.write_lock.lock().await;
        let Some(existing) = self
            .inner
            .index
            .read()
            .bindings
            .get(cn)
            .map(|e| e.binding())
        else {
            return Err(AuthError::BindingNotFound);
        };
        let key = format!("{CERT_BINDING_PREFIX}{cn}");
        let Some(current) = self.inner.meta.get(&key) else {
            return Err(AuthError::BindingNotFound);
        };
        let revision = self
            .inner
            .meta
            .delete(
                &key,
                Some(Precondition {
                    key: key.clone(),
                    expected_mod_revision: current.mod_revision,
                }),
            )
            .await
            .map_err(|e| match e {
                MetaError::Conflict { .. } => {
                    AuthError::Conflict("the binding changed concurrently; retry".into())
                }
                e => e.into(),
            })?;
        self.inner.meta.wait_for(revision).await?;
        self.inner.apply_binding(cn, revision, None);
        tracing::info!(cn = %cn, "certificate binding deleted");
        Ok(existing)
    }

    pub fn authorize(&self, principal: &Principal, action: Action<'_>) -> Result<(), AuthError> {
        let role = principal.role;
        let allowed = match action {
            // `subscriber` tokens (v0.10) can only open live subscriptions.
            Action::Read => role >= Role::Viewer,
            Action::Operate => role >= Role::Operator,
            Action::Admin | Action::CreateChannel(_) => role >= Role::Admin,
            Action::Publish(channel) => {
                role >= Role::Operator || matches_any(&principal.scopes.publish, channel)
            }
            Action::Consume(channel) => {
                role >= Role::Operator || matches_any(&principal.scopes.consume, channel)
            }
            // Viewers may already read every message over REST; a live stream is no more.
            Action::Subscribe(channel) => {
                role >= Role::Viewer || matches_any(&principal.scopes.subscribe, channel)
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(AuthError::Forbidden(describe(action)))
        }
    }

    /// Admin only. Returns the secret once.
    pub async fn create_token(
        &self,
        req: CreateTokenRequest,
        by: &Principal,
    ) -> Result<CreateTokenResponse, AuthError> {
        self.authorize(by, Action::Admin)?;
        let name = validate_name(&req.name)?;
        let scopes = validate_client_scopes(req.role, &req.scopes)?;
        let now = fluxmq_proto::now_ms();
        let expires_at_ms = match req.ttl_secs {
            None => None,
            Some(0) => return Err(AuthError::Invalid("ttl_secs must be > 0".into())),
            Some(t) if t > MAX_TTL_SECS => {
                return Err(AuthError::Invalid(format!(
                    "ttl_secs must be at most {MAX_TTL_SECS}"
                )));
            }
            Some(t) => Some(now.saturating_add(t.saturating_mul(1000))),
        };

        let _guard = self.inner.write_lock.lock().await;
        // First token in open mode: must be admin (a viewer/operator first token would lock
        // everyone out of administration), and the caller must adopt it.
        let adopt = self.mode() == AuthMode::Open;
        if adopt && req.role != Role::Admin {
            return Err(AuthError::Invalid(
                "the first token must have role admin: creating it switches the broker from \
                 open to enforced mode"
                    .into(),
            ));
        }
        let mut secret_bytes = [0u8; SECRET_BYTES];
        let mut id_bytes = [0u8; TOKEN_ID_LEN];
        fill_random(&mut secret_bytes)?;
        fill_random(&mut id_bytes)?;
        let id: String = id_bytes
            .iter()
            .map(|b| ID_ALPHABET[usize::from(b & 31)] as char)
            .collect();
        let secret = hex(&secret_bytes);
        secret_bytes.fill(0);

        let info = TokenInfo {
            id: id.clone(),
            name,
            role: req.role,
            scopes,
            created_at_ms: now,
            created_by: by.actor(),
            expires_at_ms,
            last_used_ms: None,
        };
        let stored = StoredToken {
            info: info.clone(),
            secret_sha256: hex(&sha256(secret.as_bytes())),
        };
        let key = format!("{TOKEN_PREFIX}{id}");
        // Create-only: a (astronomically unlikely) id collision fails instead of overwriting.
        let revision = self
            .inner
            .meta
            .put(
                &key,
                serde_json::to_value(&stored).map_err(MetaError::from)?,
                Some(Precondition {
                    key: key.clone(),
                    expected_mod_revision: 0,
                }),
            )
            .await?;
        self.inner.meta.wait_for(revision).await?;
        self.inner.apply_token(&id, revision, Some(stored));
        tracing::info!(token_id = %id, role = ?info.role, created_by = %info.created_by, "auth token created");
        Ok(CreateTokenResponse {
            token: format!("fmq_{id}_{secret}"),
            info,
            adopt,
        })
    }

    pub fn list_tokens(&self) -> Vec<TokenInfo> {
        let mut out: Vec<TokenInfo> = self
            .inner
            .index
            .read()
            .tokens
            .values()
            .map(|e| e.info())
            .collect();
        out.sort_by(|a, b| {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
        out
    }

    /// Revokes a token. Live sessions using it are closed on every node (see
    /// [`Self::session_watch`]). Refused with [`AuthError::LastAdmin`] when it is the last
    /// active admin token while other tokens exist and no bootstrap token is configured, and
    /// with [`AuthError::Invalid`] when it is the very last token (would reopen the broker).
    pub async fn revoke_token(&self, id: &str) -> Result<(), AuthError> {
        if !valid_id(id) {
            return Err(AuthError::NotFound);
        }
        let _guard = self.inner.write_lock.lock().await;
        let key = format!("{TOKEN_PREFIX}{id}");
        // A precondition on the one remaining admin can fail spuriously (its `last_used_ms`
        // was persisted meanwhile): re-evaluate a few times.
        let mut attempts = 0;
        let revision = loop {
            attempts += 1;
            let guard_key = self.check_revoke(id)?;
            let Some(current) = self.inner.meta.get(&key) else {
                return Err(AuthError::NotFound);
            };
            let precondition = match &guard_key {
                // Exactly one other active admin: it must still exist, unchanged.
                Some(other) => {
                    let Some(v) = self.inner.meta.get(other) else {
                        // Gone from meta but not yet from our index: re-evaluate.
                        self.wait_index_caught_up().await;
                        if attempts < 5 {
                            continue;
                        }
                        return Err(last_admin_error());
                    };
                    Precondition {
                        key: other.clone(),
                        expected_mod_revision: v.mod_revision,
                    }
                }
                None => Precondition {
                    key: key.clone(),
                    expected_mod_revision: current.mod_revision,
                },
            };
            match self.inner.meta.delete(&key, Some(precondition)).await {
                Ok(r) => break r,
                Err(MetaError::Conflict { .. }) if attempts < 5 => {
                    self.wait_index_caught_up().await;
                }
                Err(MetaError::Conflict { .. }) => {
                    return Err(AuthError::Conflict(
                        "tokens changed concurrently; retry".into(),
                    ));
                }
                Err(e) => return Err(e.into()),
            }
        };
        self.inner.meta.wait_for(revision).await?;
        self.inner.apply_token(id, revision, None);
        tracing::info!(token_id = %id, "auth token revoked");
        Ok(())
    }

    /// Guards of [`Self::revoke_token`]. `Ok(Some(key))` = the meta key of the only other
    /// active admin token (the delete must be conditional on it).
    fn check_revoke(&self, id: &str) -> Result<Option<String>, AuthError> {
        let index = self.inner.index.read();
        let Some(target) = index.tokens.get(id) else {
            return Err(AuthError::NotFound);
        };
        if self.inner.bootstrap_hash.is_some() {
            return Ok(None);
        }
        // Revoking the last token without a bootstrap token would silently switch the
        // broker to open mode (everyone admin) — refuse.
        if index.tokens.len() == 1 {
            return Err(AuthError::Invalid(
                "cannot revoke the last token: the broker would switch to open mode \
                 (create another admin token first, or start with --admin-token)"
                    .into(),
            ));
        }
        let now = fluxmq_proto::now_ms();
        let active_admin = |e: &Entry| {
            e.info.role == Role::Admin && e.info.expires_at_ms.is_none_or(|exp| exp > now)
        };
        if !active_admin(target) {
            return Ok(None);
        }
        let others: Vec<&String> = index
            .tokens
            .iter()
            .filter(|(other, e)| other.as_str() != id && active_admin(e))
            .map(|(other, _)| other)
            .collect();
        match others.as_slice() {
            [] => Err(last_admin_error()),
            [only] => Ok(Some(format!("{TOKEN_PREFIX}{only}"))),
            _ => Ok(None),
        }
    }

    /// After a precondition conflict: give replication a moment, then rebuild the index
    /// from the (local view of the) meta store so the guards are re-evaluated on fresh data.
    async fn wait_index_caught_up(&self) {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        reload_index(&self.inner);
    }

    pub fn whoami(&self, principal: &Principal) -> WhoAmI {
        WhoAmI {
            mode: self.mode(),
            name: principal.name.clone(),
            token_id: principal.token_id.clone(),
            role: principal.role,
            scopes: principal.scopes.clone(),
        }
    }

    /// Rebuilds the index from the meta store (startup and after a lagged watch).
    fn reload(&self) {
        reload_index(&self.inner);
    }

    /// Records a successful use; schedules a persisted update at most once a minute.
    fn touch(&self, entry: &Arc<Entry>, now: u64) {
        entry.last_used.fetch_max(now, Ordering::Relaxed);
        let last = entry.last_persisted.load(Ordering::Relaxed);
        if now.saturating_sub(last) < LAST_USED_WRITE_INTERVAL_MS {
            return;
        }
        // Exactly one caller wins the slot for this interval.
        if entry
            .last_persisted
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let inner = Arc::downgrade(&self.inner);
        let id = entry.info.id.clone();
        handle.spawn(async move {
            if let Some(inner) = inner.upgrade() {
                persist_last_used(&inner, &id, now).await;
            }
        });
    }

    /// [`Self::touch`] for certificate bindings.
    fn touch_binding(&self, entry: &Arc<BindingEntry>, now: u64) {
        entry.last_used.fetch_max(now, Ordering::Relaxed);
        let last = entry.last_persisted.load(Ordering::Relaxed);
        if now.saturating_sub(last) < LAST_USED_WRITE_INTERVAL_MS
            || entry
                .last_persisted
                .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let inner = Arc::downgrade(&self.inner);
        let cn = entry.binding.cn.clone();
        handle.spawn(async move {
            if let Some(inner) = inner.upgrade() {
                persist_binding_last_used(&inner, &cn, now).await;
            }
        });
    }
}

fn last_admin_error() -> AuthError {
    AuthError::LastAdmin(
        "cannot revoke the last active admin token: create another admin token first \
         (or start with --admin-token)"
            .into(),
    )
}

async fn persist_binding_last_used(inner: &Inner, cn: &str, now: u64) {
    let key = format!("{CERT_BINDING_PREFIX}{cn}");
    let Some(current) = inner.meta.get(&key) else {
        return; // deleted meanwhile
    };
    let Ok(mut binding) = serde_json::from_value::<CertBinding>(current.value) else {
        return;
    };
    if binding.last_used_ms.is_some_and(|t| t >= now) {
        return;
    }
    binding.last_used_ms = Some(now);
    let Ok(value) = serde_json::to_value(&binding) else {
        return;
    };
    let pre = Precondition {
        key: key.clone(),
        expected_mod_revision: current.mod_revision,
    };
    match inner.meta.put(&key, value, Some(pre)).await {
        Ok(_) | Err(MetaError::Conflict { .. }) => {}
        Err(e) => tracing::debug!(cn, error = %e, "persisting binding last_used_ms failed"),
    }
}

async fn persist_last_used(inner: &Inner, id: &str, now: u64) {
    let key = format!("{TOKEN_PREFIX}{id}");
    let Some(current) = inner.meta.get(&key) else {
        return; // revoked meanwhile
    };
    let Ok(mut stored) = serde_json::from_value::<StoredToken>(current.value) else {
        return;
    };
    if stored.info.last_used_ms.is_some_and(|t| t >= now) {
        return;
    }
    stored.info.last_used_ms = Some(now);
    let Ok(value) = serde_json::to_value(&stored) else {
        return;
    };
    // CAS on the revision: never resurrects a token revoked in between.
    let pre = Precondition {
        key: key.clone(),
        expected_mod_revision: current.mod_revision,
    };
    match inner.meta.put(&key, value, Some(pre)).await {
        Ok(_) | Err(MetaError::Conflict { .. }) => {}
        Err(e) => tracing::debug!(token_id = %id, error = %e, "persisting last_used_ms failed"),
    }
}

fn reload_index(inner: &Inner) {
    // Revision first: everything up to it is reflected in the listing that follows.
    let floor = inner.meta.revision();
    let mut tokens = HashMap::new();
    let mut revs = HashMap::new();
    for (key, v) in inner.meta.list(TOKEN_PREFIX) {
        let Some(id) = key.strip_prefix(TOKEN_PREFIX) else {
            continue;
        };
        let entry = serde_json::from_value::<StoredToken>(v.value)
            .ok()
            .and_then(Entry::from_stored);
        match entry {
            Some(e) => {
                revs.insert(id.to_string(), v.mod_revision);
                tokens.insert(id.to_string(), Arc::new(e));
            }
            None => tracing::warn!(key, "ignoring malformed stored auth token"),
        }
    }
    let mut bindings = HashMap::new();
    let mut binding_revs = HashMap::new();
    for (key, v) in inner.meta.list(CERT_BINDING_PREFIX) {
        let Some(cn) = key.strip_prefix(CERT_BINDING_PREFIX) else {
            continue;
        };
        match serde_json::from_value::<CertBinding>(v.value) {
            Ok(b) => {
                binding_revs.insert(cn.to_string(), v.mod_revision);
                bindings.insert(cn.to_string(), Arc::new(BindingEntry::new(b)));
            }
            Err(_) => tracing::warn!(key, "ignoring malformed certificate binding"),
        }
    }
    let mut index = inner.index.write();
    for (cn, e) in &bindings {
        if let Some(old) = index.bindings.get(cn) {
            e.last_used
                .fetch_max(old.last_used.load(Ordering::Relaxed), Ordering::Relaxed);
            e.last_persisted.fetch_max(
                old.last_persisted.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
    }
    index.bindings = bindings;
    index.binding_revs = binding_revs;
    // Carry this node's last-use knowledge over.
    for (id, e) in &tokens {
        if let Some(old) = index.tokens.get(id) {
            e.last_used
                .fetch_max(old.last_used.load(Ordering::Relaxed), Ordering::Relaxed);
            e.last_persisted.fetch_max(
                old.last_persisted.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
    }
    index.tokens = tokens;
    index.revs = revs;
    index.floor = floor;
    drop(index);
    inner.notify_changed();
}

async fn watch_loop(
    inner: Weak<Inner>,
    mut rx: tokio::sync::broadcast::Receiver<fluxmq_meta::MetaEvent>,
) {
    loop {
        let event = rx.recv().await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        match event {
            Ok(ev) => {
                if let Some(cn) = ev.key.strip_prefix(CERT_BINDING_PREFIX) {
                    let value = ev.value.and_then(|v| {
                        serde_json::from_value::<CertBinding>(v)
                            .inspect_err(|e| tracing::warn!(key = %ev.key, error = %e, "malformed certificate binding event"))
                            .ok()
                    });
                    inner.apply_binding(cn, ev.revision, value);
                    continue;
                }
                let Some(id) = ev.key.strip_prefix(TOKEN_PREFIX) else {
                    continue;
                };
                let value = match ev.value {
                    None => None,
                    Some(v) => match serde_json::from_value::<StoredToken>(v) {
                        Ok(s) => Some(s),
                        Err(e) => {
                            tracing::warn!(key = %ev.key, error = %e, "malformed auth token event");
                            None
                        }
                    },
                };
                inner.apply_token(id, ev.revision, value);
            }
            Err(RecvError::Lagged(n)) => {
                tracing::warn!(skipped = n, "auth token watch lagged; reloading");
                reload_index(&inner);
            }
            Err(RecvError::Closed) => return,
        }
    }
}

/// Splits `fmq_<id>_<secret>`; `None` if malformed.
fn parse_token(token: &str) -> Option<(&str, &str)> {
    let rest = token.strip_prefix("fmq_")?;
    let (id, secret) = rest.split_once('_')?;
    (valid_id(id) && !secret.is_empty()).then_some((id, secret))
}

fn valid_id(id: &str) -> bool {
    id.len() == TOKEN_ID_LEN && id.bytes().all(|b| ID_ALPHABET.contains(&b))
}

fn validate_name(name: &str) -> Result<String, AuthError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > MAX_NAME_LEN {
        return Err(AuthError::Invalid(format!(
            "name must be 1-{MAX_NAME_LEN} characters"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ' '))
    {
        return Err(AuthError::Invalid(
            "name may only contain [a-zA-Z0-9._- ]".into(),
        ));
    }
    if RESERVED_NAMES.iter().any(|r| r.eq_ignore_ascii_case(name)) {
        return Err(AuthError::Invalid(format!("name '{name}' is reserved")));
    }
    Ok(name.to_string())
}

/// A certificate subject CN as stored: exact (not trimmed), 1–[`MAX_CN_LEN`] bytes, no
/// control characters, no `/` (it is part of the meta key and the REST path).
fn validate_cn(cn: &str) -> Result<String, AuthError> {
    if cn.trim().is_empty() || cn.len() > MAX_CN_LEN {
        return Err(AuthError::Invalid(format!(
            "cn must be a non-empty certificate subject CN of at most {MAX_CN_LEN} bytes"
        )));
    }
    if cn.chars().any(|c| c.is_control() || c == '/') {
        return Err(AuthError::Invalid(
            "cn must not contain control characters or '/'".into(),
        ));
    }
    Ok(cn.to_string())
}

/// Validates every scope list; a `subscriber` (v0.10) needs `subscribe` patterns and may hold
/// no `publish`/`consume` ones (it must never reach the client protocol's data plane).
fn validate_client_scopes(role: Role, scopes: &ClientScopes) -> Result<ClientScopes, AuthError> {
    let out = ClientScopes {
        publish: validate_scopes("publish", &scopes.publish)?,
        consume: validate_scopes("consume", &scopes.consume)?,
        subscribe: validate_scopes("subscribe", &scopes.subscribe)?,
    };
    if role == Role::Subscriber {
        if out.subscribe.is_empty() {
            return Err(AuthError::Invalid(
                "role subscriber needs at least one subscribe scope (channel glob)".into(),
            ));
        }
        if !out.publish.is_empty() || !out.consume.is_empty() {
            return Err(AuthError::Invalid(
                "role subscriber may only have subscribe scopes (no publish/consume)".into(),
            ));
        }
    }
    Ok(out)
}

fn validate_scopes(kind: &str, patterns: &[String]) -> Result<Vec<String>, AuthError> {
    if patterns.len() > MAX_SCOPE_PATTERNS {
        return Err(AuthError::Invalid(format!(
            "at most {MAX_SCOPE_PATTERNS} {kind} scopes"
        )));
    }
    let mut out: Vec<String> = Vec::with_capacity(patterns.len());
    for p in patterns {
        let p = p.trim();
        if p.is_empty() || p.len() > 128 {
            return Err(AuthError::Invalid(format!(
                "{kind} scope must be 1-128 characters"
            )));
        }
        if !p
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '*' | '?'))
        {
            return Err(AuthError::Invalid(format!(
                "{kind} scope '{p}' may only contain [a-zA-Z0-9._-*?]"
            )));
        }
        if !out.iter().any(|q| q == p) {
            out.push(p.to_string());
        }
    }
    Ok(out)
}

fn matches_any(patterns: &[String], channel: &str) -> bool {
    patterns.iter().any(|p| glob_match(p, channel))
}

/// Glob match: `*` = any run of characters (including none and `.`), `?` = exactly one
/// character. Linear-time two-pointer algorithm (no backtracking blow-up).
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<usize> = None;
    let mut mark = 0;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn describe(action: Action<'_>) -> String {
    match action {
        Action::Read => "read access required".into(),
        Action::Operate => "operator role required".into(),
        Action::Admin => "admin role required".into(),
        Action::CreateChannel(c) => format!("admin role required to create channel '{c}'"),
        Action::Publish(c) => format!("not allowed to publish to '{c}'"),
        Action::Consume(c) => format!("not allowed to consume from '{c}'"),
        Action::Subscribe(c) => format!("not allowed to subscribe to '{c}'"),
    }
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn decode_hash(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16)?;
        let lo = (chunk[1] as char).to_digit(16)?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Some(out)
}

fn fill_random(buf: &mut [u8]) -> Result<(), AuthError> {
    use rand::TryRngCore;
    rand::rngs::OsRng
        .try_fill_bytes(buf)
        .map_err(|e| AuthError::Meta(MetaError::Unavailable(format!("os rng: {e}"))))
}

#[cfg(test)]
mod tests;

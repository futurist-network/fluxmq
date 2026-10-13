//! Shared application state handed to every handler.

use std::time::{Duration, Instant};

use fluxmq_auth::AuthService;
use fluxmq_backup::BackupService;
use fluxmq_cluster::Cluster;
use fluxmq_core::Broker;
use fluxmq_notify::NotifyService;
use fluxmq_router::RouterService;
use fluxmq_telemetry::TelemetryHub;
use fluxmq_webhooks::WebhookService;
use tokio_util::sync::CancellationToken;

use crate::audit::{AuditLog, DEFAULT_AUDIT_CAPACITY};

/// WebSocket gateway limits.
#[derive(Debug, Clone)]
pub struct WsLimits {
    /// Max size of one inbound message (client frames are tiny JSON commands).
    pub max_message_bytes: usize,
    /// Max topics one session may be subscribed to at once.
    pub max_subscriptions: usize,
    /// A client whose socket stays unwritable this long is dropped.
    pub send_timeout: Duration,
    /// Server → client ping interval (detects dead peers behind NAT/proxies).
    pub ping_interval: Duration,
    /// Close the connection when nothing has been received for this long.
    pub idle_timeout: Duration,
}

impl Default for WsLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024,
            max_subscriptions: 256,
            send_timeout: Duration::from_secs(10),
            ping_interval: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

/// Everything the HTTP layer needs. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    pub broker: Broker,
    pub telemetry: TelemetryHub,
    /// RBAC (stage 7). Open mode by default.
    pub auth: AuthService,
    /// Content-based routing (stage 8).
    pub router: RouterService,
    /// HTTP push webhooks (v0.3).
    pub webhooks: WebhookService,
    /// Backups (v0.3); memory-only and unscheduled by default.
    pub backups: BackupService,
    /// Notifications + `$system.events` feed (v0.3); disabled by default.
    pub notify: NotifyService,
    /// Delayed messages (v0.12): the publish entry point; disabled by default (delay headers
    /// refused).
    pub scheduler: fluxmq_scheduler::SchedulerService,
    /// Cluster membership/metadata (stage 9); a standalone single node by default.
    pub cluster: Cluster,
    /// Web console: built in (default), from a directory, or off.
    pub ui: crate::static_ui::UiSource,
    pub audit: AuditLog,
    pub ws: WsLimits,
    /// Live subscription gateway (v0.10): limits, shared feeds, metrics.
    pub live: crate::live::LiveService,
    /// Max accepted request body (publish batches).
    pub max_body_bytes: usize,
    /// Cancelled on graceful shutdown; long-lived WebSocket sessions close when it fires.
    pub shutdown: CancellationToken,
    pub started_at: Instant,
    /// TLS state per surface (v0.4); disabled by default.
    pub tls: fluxmq_tls::TlsRegistry,
}

impl AppState {
    /// State with dev defaults: no auth, no UI dir, memory-only audit log.
    pub fn new(broker: Broker, telemetry: TelemetryHub) -> Self {
        Self {
            auth: AuthService::disabled(),
            router: RouterService::disabled(broker.clone()),
            webhooks: WebhookService::disabled(broker.clone()),
            backups: BackupService::disabled(broker.clone()),
            notify: NotifyService::disabled(),
            scheduler: fluxmq_scheduler::SchedulerService::disabled(broker.clone()),
            cluster: Cluster::standalone(broker.clone()),
            broker,
            telemetry,
            ui: Default::default(),
            audit: AuditLog::in_memory(DEFAULT_AUDIT_CAPACITY),
            ws: WsLimits::default(),
            live: crate::live::LiveService::default(),
            max_body_bytes: fluxmq_proto::wire::MAX_FRAME_LEN,
            shutdown: CancellationToken::new(),
            started_at: Instant::now(),
            tls: fluxmq_tls::TlsRegistry::disabled(),
        }
    }

    /// TLS state behind `GET /api/tls` and the TLS metrics (v0.4).
    pub fn with_tls(mut self, tls: fluxmq_tls::TlsRegistry) -> Self {
        self.tls = tls;
        self
    }

    /// Bootstrap admin token (tests, embedded use): unless a persistent [`AuthService`] was
    /// installed with [`AppState::with_auth`], installs a memory-only auth service with that
    /// bootstrap token. Empty strings count as "no token". The server passes the token to its
    /// persistent [`AuthService`] instead.
    pub fn with_admin_token(mut self, token: Option<String>) -> Self {
        let token = token.filter(|t| !t.is_empty());
        if !self.auth.is_persistent() {
            self.auth = AuthService::in_memory(token);
        }
        self
    }

    pub fn with_ui(mut self, ui: crate::static_ui::UiSource) -> Self {
        self.ui = ui;
        self
    }

    pub fn with_audit(mut self, audit: AuditLog) -> Self {
        self.audit = audit;
        self
    }

    pub fn with_auth(mut self, auth: AuthService) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_router(mut self, router: RouterService) -> Self {
        self.router = router;
        self
    }

    pub fn with_scheduler(mut self, scheduler: fluxmq_scheduler::SchedulerService) -> Self {
        self.scheduler = scheduler;
        self
    }

    pub fn with_webhooks(mut self, webhooks: WebhookService) -> Self {
        self.webhooks = webhooks;
        self
    }

    pub fn with_backups(mut self, backups: BackupService) -> Self {
        self.backups = backups;
        self
    }

    pub fn with_notify(mut self, notify: NotifyService) -> Self {
        self.notify = notify;
        self
    }

    pub fn with_cluster(mut self, cluster: Cluster) -> Self {
        self.cluster = cluster;
        self
    }

    /// Live subscription limits (`--live-*` flags).
    pub fn with_live(mut self, cfg: crate::live::LiveConfig) -> Self {
        self.live = crate::live::LiveService::new(cfg);
        self
    }

    pub fn with_shutdown(mut self, token: CancellationToken) -> Self {
        self.shutdown = token;
        self
    }

    /// Audit actor of the request being handled: the authenticated principal's name plus its
    /// token id (`name#id`), `admin` for the bootstrap token, `anonymous` in open mode.
    /// Outside a request (background jobs) it is `system`.
    pub fn actor(&self) -> String {
        crate::auth::current_principal().map_or_else(|| "system".to_string(), |p| p.actor())
    }

    /// Records a mutating call in the audit log, attributed to [`AppState::actor`].
    pub fn audit(&self, action: &str, target: &str, detail: serde_json::Value) {
        self.audit.record(&self.actor(), action, target, detail);
    }
}

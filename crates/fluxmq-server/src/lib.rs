//! The FluxMQ broker process: wires [`fluxmq_core::Broker`], the telemetry plane, the HTTP
//! surface and the client protocol server together, and owns graceful shutdown.
//!
//! Exposed as a library so end-to-end tests can run a full server in-process:
//!
//! ```ignore
//! let server = Server::bind(ServerConfig::ephemeral(dir)).await?;
//! let (client, http) = (server.client_addr(), server.http_addr());
//! let token = CancellationToken::new();
//! tokio::spawn(server.run(token.clone().cancelled_owned()));
//! ```

pub mod backup_cli;
pub mod cluster;
pub mod config;
pub mod net;
pub mod shutdown;
pub mod tls;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context as _;
use fluxmq_api::{AppState, AuditLog};
use fluxmq_core::Broker;
use fluxmq_telemetry::{TelemetryConfig, TelemetryHub};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub use config::{Cli, ClusterSettings, FsyncMode, ServerConfig, TlsSettings};

/// A bound, ready-to-run server.
pub struct Server {
    config: ServerConfig,
    broker: Broker,
    telemetry: TelemetryHub,
    cluster: fluxmq_cluster::Cluster,
    auth: fluxmq_auth::AuthService,
    router: fluxmq_router::RouterService,
    webhooks: fluxmq_webhooks::WebhookService,
    backups: fluxmq_backup::BackupService,
    notify: fluxmq_notify::NotifyService,
    scheduler: fluxmq_scheduler::SchedulerService,
    audit: AuditLog,
    client_listener: TcpListener,
    http_listener: TcpListener,
    tls: tls::TlsRuntime,
}

impl Server {
    /// Binds both listeners, opens the broker (recovering the data dir) and starts telemetry.
    pub async fn bind(config: ServerConfig) -> anyhow::Result<Server> {
        config.validate()?;
        // TLS material first: a bad certificate path fails before anything is bound/opened.
        let tls = tls::TlsRuntime::build(&config.tls, config.node_id.as_deref().unwrap_or(""))
            .context("TLS configuration")?;
        let client_listener = TcpListener::bind(config.client_addr)
            .await
            .with_context(|| format!("binding client address {}", config.client_addr))?;
        let http_listener = TcpListener::bind(config.http_addr)
            .await
            .with_context(|| format!("binding HTTP address {}", config.http_addr))?;

        let broker = Broker::open(config.broker_config())
            .await
            .with_context(|| format!("opening data directory {}", config.data_dir.display()))?;

        let audit_path = config.data_dir.join("audit.log");
        let audit = tokio::task::spawn_blocking(move || {
            AuditLog::open(&audit_path, fluxmq_api::audit::DEFAULT_AUDIT_CAPACITY)
        })
        .await?
        .context("opening audit log")?;

        let telemetry_config = TelemetryConfig {
            history_dir: Some(config.data_dir.join("telemetry")),
            ..TelemetryConfig::default()
        };
        let telemetry = TelemetryHub::start(broker.clone(), telemetry_config);
        tls.registry.set_node_id(broker.node_id());

        // Stage 7–9 subsystems: a single node, or Raft-backed metadata + replication when
        // cluster peers are configured. Auth and routes live in the meta store.
        let cluster = cluster::open(
            &config,
            &broker,
            client_listener.local_addr()?,
            http_listener.local_addr()?,
            tls.cluster.clone(),
        )
        .await
        .context("starting cluster")?;
        // Cluster-wide telemetry aggregation (no-op on a single node).
        telemetry.attach_cluster(cluster.clone());
        let auth = fluxmq_auth::AuthService::open(cluster.meta(), config.admin_token.clone())
            .await
            .context("loading auth tokens")?;
        // Certificate bindings (v0.5) are live only when the client port verifies client
        // certificates.
        auth.set_client_mtls(tls.client.is_some() && config.tls.client_ca.is_some());
        let router = fluxmq_router::RouterService::start(broker.clone(), cluster.meta())
            .await
            .context("starting router")?;
        // Notifications (controller dispatches) + the `$system.events` pull feed.
        let notify = fluxmq_notify::NotifyService::start(
            broker.clone(),
            cluster.meta(),
            &telemetry,
            cluster.clone(),
            fluxmq_notify::NotifyConfig::default().with_public_url(config.public_url.clone()),
        )
        .await
        .context("starting notifications")?;
        let webhooks = fluxmq_webhooks::WebhookService::start(
            broker.clone(),
            cluster.meta(),
            webhook_config(config.webhooks_allow_private, &auth),
        )
        .await
        .context("starting webhooks")?;
        // Delayed messages (v0.12): workers for the `$scheduled` partitions this node leads.
        let scheduler = fluxmq_scheduler::SchedulerService::start(broker.clone(), cluster.clone());
        // Backups: scheduler on this node (in a cluster: on the controller, every leader
        // uploads its partitions).
        let backup_hook: Option<std::sync::Arc<dyn fluxmq_backup::cluster::ClusterHook>> =
            cluster.is_clustered().then(|| {
                std::sync::Arc::new(fluxmq_api::routes::backups::ClusterBackupHook(
                    cluster.clone(),
                )) as _
            });
        let backups = fluxmq_backup::BackupService::start(
            broker.clone(),
            cluster.meta(),
            fluxmq_backup::ServiceOptions {
                cluster: backup_hook,
                cluster_name: config.cluster_name.clone(),
                ..fluxmq_backup::ServiceOptions::default()
            },
        );

        Ok(Server {
            config,
            broker,
            telemetry,
            cluster,
            auth,
            router,
            webhooks,
            backups,
            notify,
            scheduler,
            audit,
            client_listener,
            http_listener,
            tls,
        })
    }

    /// TLS state per surface (status, counters).
    pub fn tls(&self) -> &fluxmq_tls::TlsRegistry {
        &self.tls.registry
    }

    /// Actual client protocol address (useful with port 0).
    pub fn client_addr(&self) -> SocketAddr {
        self.client_listener
            .local_addr()
            .unwrap_or(self.config.client_addr)
    }

    /// Actual HTTP address (useful with port 0).
    pub fn http_addr(&self) -> SocketAddr {
        self.http_listener
            .local_addr()
            .unwrap_or(self.config.http_addr)
    }

    pub fn broker(&self) -> &Broker {
        &self.broker
    }

    /// Cluster handle (single node or clustered).
    pub fn cluster(&self) -> &fluxmq_cluster::Cluster {
        &self.cluster
    }

    /// Serves until `shutdown` resolves (or a listener fails), then shuts down gracefully:
    /// leave the cluster (drain leaderships, leave ISRs), stop accepting, `Goodbye` to
    /// clients, close WebSockets, stop telemetry, flush broker.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) -> anyhow::Result<()> {
        let Server {
            config,
            broker,
            telemetry,
            cluster,
            auth,
            router,
            webhooks,
            backups,
            notify,
            scheduler,
            audit,
            client_listener,
            http_listener,
            tls,
        } = self;
        let client_addr = client_listener.local_addr()?;
        let http_addr = http_listener.local_addr()?;
        let token = CancellationToken::new();

        let net_ctx = Arc::new(net::NetContext {
            broker: broker.clone(),
            node_id: broker.node_id().to_string(),
            limits: config.limits.clone(),
            auth: auth.clone(),
            cluster: cluster.clone(),
            scheduler: scheduler.clone(),
        });
        let mut net_task = tokio::spawn(net::listener::serve(
            client_listener,
            net_ctx,
            token.clone(),
            tls.client.clone(),
        ));
        // TLS: reload loop (30 s mtime watch + SIGHUP), alerts and reload-failure events.
        let tls_tasks = tls_tasks(&tls, &telemetry, broker.node_id(), &token);

        let auth_mode = auth_banner(&auth);
        let reachable = |a: SocketAddr| !a.ip().is_loopback();
        if auth.mode() == fluxmq_proto::auth::AuthMode::Open
            && (reachable(client_addr) || reachable(http_addr))
        {
            tracing::warn!(
                "auth is OPEN and the broker listens beyond localhost: anyone who reaches these \
                 ports is an admin. Set --admin-token (or --admin-token-file), or create an admin \
                 token in the console (Settings → Tokens)"
            );
        }
        let backup_audit = backup_audit_task(&backups, audit.clone());
        let decommission_audit = decommission_audit_task(&cluster, audit.clone());
        let state = AppState::new(broker.clone(), telemetry.clone())
            .with_ui(config.ui.clone())
            .with_audit(audit)
            .with_auth(auth)
            .with_router(router.clone())
            .with_webhooks(webhooks.clone())
            .with_backups(backups.clone())
            .with_notify(notify.clone())
            .with_scheduler(scheduler.clone())
            .with_cluster(cluster.clone())
            .with_tls(tls.registry.clone())
            .with_live(config.live.clone())
            .with_shutdown(token.clone());
        let mut app = fluxmq_api::router(state);
        if tls.cluster.is_some() {
            // `/internal/*` additionally requires a peer certificate from the cluster CA.
            app = app.layer(axum::middleware::from_fn(
                fluxmq_tls::http::require_cluster_peer,
            ));
        }
        let http_shutdown = token.clone();
        let http_mode = tls.http.clone();
        let mut http_task = tokio::spawn(async move {
            fluxmq_tls::http::serve(http_listener, app, http_mode, http_shutdown).await
        });

        banner(&config, &broker, client_addr, http_addr, auth_mode, &tls);

        let mut failure = None;
        tokio::select! {
            () = shutdown => tracing::info!("shutting down"),
            r = &mut net_task => {
                failure = Some(anyhow::anyhow!("client listener stopped unexpectedly: {r:?}"));
            }
            r = &mut http_task => {
                failure = Some(match r {
                    Ok(Ok(())) => anyhow::anyhow!("HTTP server stopped unexpectedly"),
                    Ok(Err(e)) => anyhow::Error::new(e).context("HTTP server failed"),
                    Err(e) => anyhow::Error::new(e).context("HTTP server task panicked"),
                });
            }
        }
        if let Some(e) = &failure {
            tracing::error!(error = %e, "shutting down after failure");
        }

        // Cluster: hand leadership over and leave the ISRs first, while connections are still
        // up (clients get NotLeader and move on), so peers don't wait for the Down timeout.
        if failure.is_none() {
            cluster.leave(LEAVE_TIMEOUT).await;
        }
        // Stop accepting; connections say Goodbye; WebSocket sessions close.
        token.cancel();
        telemetry.shutdown();
        router.shutdown();
        notify.shutdown();
        scheduler.shutdown().await;
        backups.shutdown();
        backup_audit.abort();
        if let Some(t) = decommission_audit {
            t.abort();
        }
        for t in tls_tasks {
            t.abort();
        }
        // In-flight webhook requests finish (bounded) and are acked/nacked.
        webhooks.shutdown(config.shutdown_timeout).await;
        let drain = async {
            if !net_task.is_finished() {
                let _ = (&mut net_task).await;
            }
            if !http_task.is_finished() {
                let _ = (&mut http_task).await;
            }
        };
        if tokio::time::timeout(config.shutdown_timeout, drain)
            .await
            .is_err()
        {
            tracing::warn!(timeout = ?config.shutdown_timeout, "connections did not drain in time; closing anyway");
            net_task.abort();
            http_task.abort();
        }
        cluster.shutdown().await;
        if tokio::time::timeout(config.shutdown_timeout, broker.shutdown())
            .await
            .is_err()
        {
            tracing::error!(
                "broker shutdown timed out; data written since the last fsync may be lost"
            );
        }
        tracing::info!("bye");
        failure.map_or(Ok(()), Err)
    }
}

/// Starts the TLS background tasks (nothing when TLS is not configured).
fn tls_tasks(
    tls: &tls::TlsRuntime,
    telemetry: &TelemetryHub,
    node_id: &str,
    token: &CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let registry = &tls.registry;
    if !registry.any_enabled() {
        return Vec::new();
    }
    let mut tasks = Vec::new();
    if let Some(t) =
        registry.spawn_reloader(fluxmq_tls::registry::RELOAD_CHECK_INTERVAL, token.clone())
    {
        tasks.push(t);
    }
    let alerts = registry.clone();
    let node = node_id.to_string();
    telemetry.set_alert_source(move |now| alerts.alerts(&node, now));
    let mut failures = registry.subscribe_failures();
    let telemetry = telemetry.clone();
    let node = node_id.to_string();
    tasks.push(tokio::spawn(async move {
        loop {
            match failures.recv().await {
                Ok(f) => telemetry.push_event(
                    fluxmq_proto::telemetry::EventKind::TlsReloadFailed,
                    format!("TLS certificate reload failed for {} on {node}", f.surface),
                    f.surface.as_str(),
                    fluxmq_proto::telemetry::Health::Warning,
                ),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    }));
    tasks
}

/// Budget for the graceful cluster leave (drain + leave) on shutdown.
const LEAVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);

/// Convenience: [`Server::bind`] + [`Server::run`].
pub async fn run(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
) -> anyhow::Result<()> {
    Server::bind(config).await?.run(shutdown).await
}

/// Records scheduled/finished backups and retention deletions in the audit log (actor
/// `system`; API-triggered actions are audited by their handlers).
/// v0.5: decommission starts / ends observed by this node as controller → `node.decommission`
/// audit entries (the operator's request is audited by the API handler).
fn decommission_audit_task(
    cluster: &fluxmq_cluster::Cluster,
    audit: AuditLog,
) -> Option<tokio::task::JoinHandle<()>> {
    let mut rx = cluster.decommission_events()?;
    Some(tokio::spawn(async move {
        loop {
            let st = match rx.recv().await {
                Ok(st) => st,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            };
            audit.record(
                "system",
                "node.decommission",
                &st.node,
                serde_json::json!({
                    "state": st.state,
                    "initial_replicas": st.initial_replicas,
                    "replicas_remaining": st.replicas_remaining,
                    "error": st.error,
                }),
            );
        }
    }))
}

fn backup_audit_task(
    backups: &fluxmq_backup::BackupService,
    audit: AuditLog,
) -> tokio::task::JoinHandle<()> {
    use fluxmq_backup::BackupEvent;
    let mut rx = backups.events();
    tokio::spawn(async move {
        loop {
            let ev = match rx.recv().await {
                Ok(ev) => ev,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            };
            match ev {
                BackupEvent::Started(i) if i.trigger == "scheduled" => audit.record(
                    "system",
                    "backup.run",
                    &i.id,
                    serde_json::json!({ "trigger": i.trigger }),
                ),
                BackupEvent::Started(_) => {}
                BackupEvent::Succeeded(i) => audit.record(
                    "system",
                    "backup.succeeded",
                    &i.id,
                    serde_json::json!({
                        "trigger": i.trigger,
                        "bytes_uploaded": i.bytes_uploaded,
                        "segments_uploaded": i.segments_uploaded,
                        "segments_reused": i.segments_reused,
                        "total_bytes": i.total_bytes,
                    }),
                ),
                BackupEvent::Failed(i) => audit.record(
                    "system",
                    "backup.failed",
                    &i.id,
                    serde_json::json!({ "trigger": i.trigger, "error": i.error }),
                ),
                BackupEvent::Deleted {
                    id,
                    retention: true,
                } => {
                    audit.record("system", "backup.expired", &id, serde_json::json!({}));
                }
                BackupEvent::Deleted { .. } => {}
            }
        }
    })
}

/// `--webhooks-allow-private` when given, else "allowed while auth is open".
fn webhook_config(
    allow_private: Option<bool>,
    auth: &fluxmq_auth::AuthService,
) -> fluxmq_webhooks::WebhookConfig {
    let private = match allow_private {
        Some(b) => fluxmq_webhooks::PrivatePolicy::Fixed(b),
        None => {
            let auth = auth.clone();
            fluxmq_webhooks::PrivatePolicy::Dynamic(Arc::new(move || {
                auth.mode() == fluxmq_proto::auth::AuthMode::Open
            }))
        }
    };
    fluxmq_webhooks::WebhookConfig { private }
}

fn auth_banner(auth: &fluxmq_auth::AuthService) -> String {
    let tokens = auth.list_tokens().len();
    match (auth.has_bootstrap_token(), tokens) {
        (false, 0) => "open (dev mode: no admin token, no access tokens)".to_string(),
        (true, n) => format!("enforced (bootstrap admin token + {n} access token(s))"),
        (false, n) => format!("enforced ({n} access token(s))"),
    }
}

fn banner(
    config: &ServerConfig,
    broker: &Broker,
    client_addr: SocketAddr,
    http_addr: SocketAddr,
    auth: String,
    tls: &tls::TlsRuntime,
) {
    let ui_host = if http_addr.ip().is_unspecified() {
        SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), http_addr.port())
    } else {
        http_addr
    };
    let ui = match &config.ui {
        fluxmq_api::UiSource::Off => "off".to_string(),
        _ => format!("{}://{ui_host}/", tls.http_scheme()),
    };
    let reg = &tls.registry;
    let client_scheme = if tls.client.is_some() { "tls" } else { "tcp" };
    let http_scheme = tls.http_scheme();
    let tls_line = if reg.any_enabled() {
        let st = reg.status();
        let s = |e: &fluxmq_proto::tls::TlsEndpointStatus, label: &str| match &e.certificate {
            None => format!("{label} off"),
            Some(c) => format!(
                "{label} {}{}",
                c.subject,
                if e.mutual { " (mTLS)" } else { "" }
            ),
        };
        format!(
            "\n  tls:         {}; {}; {}",
            s(&st.client, "client"),
            s(&st.http, "http"),
            s(&st.cluster, "cluster")
        )
    } else {
        String::new()
    };
    tracing::info!(
        "FluxMQ {} started\n  node:        {} (cluster \"{}\")\n  data dir:    {}\n  clients:     {}://{}\n  http:        {}://{}\n  ui:          {}\n  auth:        {}\n  fsync:       {:?}{}",
        fluxmq_proto::VERSION,
        broker.node_id(),
        config.cluster_name,
        config.data_dir.display(),
        client_scheme,
        client_addr,
        http_scheme,
        http_addr,
        ui,
        auth,
        config.fsync,
        tls_line,
    );
}

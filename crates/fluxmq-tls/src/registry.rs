//! [`TlsRegistry`]: what is enabled on each surface, the certificates served, handshake
//! failure counters, the reload loop and reload-failure notifications. Backs `GET /api/tls`,
//! the TLS Prometheus families and the `tls_cert_expiring` / `tls_reload_failed` alerts.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fluxmq_proto::telemetry::{Alert, AlertKind, Health};
use fluxmq_proto::tls::{CERT_EXPIRY_WARNING_DAYS, TlsEndpointStatus, TlsStatus};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::reload::{ReloadOutcome, ReloadableCert};

/// How often certificate files are checked for changes.
pub const RELOAD_CHECK_INTERVAL: Duration = Duration::from_secs(30);
/// Days before expiry at which `tls_cert_expiring` turns critical.
pub const CERT_EXPIRY_CRITICAL_DAYS: u64 = 3;

const DAY_MS: u64 = 24 * 3600 * 1000;

/// A TLS surface of the broker ([`fluxmq_proto::tls`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Surface {
    Client,
    Http,
    Cluster,
}

impl Surface {
    pub const ALL: [Surface; 3] = [Surface::Client, Surface::Http, Surface::Cluster];

    pub fn as_str(self) -> &'static str {
        match self {
            Surface::Client => "client",
            Surface::Http => "http",
            Surface::Cluster => "cluster",
        }
    }

    fn idx(self) -> usize {
        self as usize
    }
}

impl std::fmt::Display for Surface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Failed-handshake counter of one surface (plaintext on a TLS port, bad or missing client
/// certificate, protocol mismatch, handshake timeout). Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct HandshakeCounter(Arc<AtomicU64>);

impl HandshakeCounter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A reload that failed (the previous certificate is still served).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadFailure {
    pub surface: Surface,
    pub error: String,
}

#[derive(Debug, Default)]
struct SurfaceState {
    cert: Option<Arc<ReloadableCert>>,
    mutual: bool,
    failures: HandshakeCounter,
}

#[derive(Debug)]
struct Inner {
    surfaces: [SurfaceState; 3],
    failures_tx: broadcast::Sender<ReloadFailure>,
    /// Reported as `TlsStatus::node_id` (set once the node id is known).
    node_id: parking_lot::RwLock<String>,
}

/// Per-surface TLS state. Cheap to clone. Built once at startup with [`TlsRegistry::builder`].
#[derive(Debug, Clone)]
pub struct TlsRegistry {
    inner: Arc<Inner>,
}

impl Default for TlsRegistry {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Builder for [`TlsRegistry`].
#[derive(Debug, Default)]
pub struct TlsRegistryBuilder {
    surfaces: [SurfaceState; 3],
}

impl TlsRegistryBuilder {
    /// Enables `surface` serving `cert`; `mutual` = client certificates required.
    pub fn surface(mut self, surface: Surface, cert: Arc<ReloadableCert>, mutual: bool) -> Self {
        let s = &mut self.surfaces[surface.idx()];
        s.cert = Some(cert);
        s.mutual = mutual;
        self
    }

    pub fn build(self) -> TlsRegistry {
        TlsRegistry {
            inner: Arc::new(Inner {
                surfaces: self.surfaces,
                failures_tx: broadcast::channel(64).0,
                node_id: parking_lot::RwLock::new(String::new()),
            }),
        }
    }
}

impl TlsRegistry {
    pub fn builder() -> TlsRegistryBuilder {
        TlsRegistryBuilder::default()
    }

    /// Nothing enabled.
    pub fn disabled() -> Self {
        Self::builder().build()
    }

    /// The node id reported in [`TlsStatus`].
    pub fn set_node_id(&self, node_id: impl Into<String>) {
        *self.inner.node_id.write() = node_id.into();
    }

    pub fn enabled(&self, surface: Surface) -> bool {
        self.inner.surfaces[surface.idx()].cert.is_some()
    }

    pub fn any_enabled(&self) -> bool {
        Surface::ALL.iter().any(|s| self.enabled(*s))
    }

    pub fn cert(&self, surface: Surface) -> Option<Arc<ReloadableCert>> {
        self.inner.surfaces[surface.idx()].cert.clone()
    }

    pub fn counter(&self, surface: Surface) -> HandshakeCounter {
        self.inner.surfaces[surface.idx()].failures.clone()
    }

    /// Reload failures as they happen.
    pub fn subscribe_failures(&self) -> broadcast::Receiver<ReloadFailure> {
        self.inner.failures_tx.subscribe()
    }

    fn endpoint(&self, surface: Surface) -> TlsEndpointStatus {
        let s = &self.inner.surfaces[surface.idx()];
        match &s.cert {
            None => TlsEndpointStatus {
                enabled: false,
                mutual: false,
                certificate: None,
                loaded_at_ms: None,
                last_error: None,
                handshake_failures_total: s.failures.get(),
            },
            Some(c) => TlsEndpointStatus {
                enabled: true,
                mutual: s.mutual,
                certificate: Some(c.info()),
                loaded_at_ms: Some(c.loaded_at_ms()),
                last_error: c.last_error(),
                handshake_failures_total: s.failures.get(),
            },
        }
    }

    /// `GET /api/tls`.
    pub fn status(&self) -> TlsStatus {
        TlsStatus {
            node_id: self.inner.node_id.read().clone(),
            client: self.endpoint(Surface::Client),
            http: self.endpoint(Surface::Http),
            cluster: self.endpoint(Surface::Cluster),
        }
    }

    fn apply(&self, surface: Surface, outcome: ReloadOutcome, forced: bool) {
        match outcome {
            ReloadOutcome::Unchanged => {
                if forced {
                    tracing::info!(%surface, "TLS certificate re-read: unchanged");
                }
            }
            ReloadOutcome::Reloaded => {
                if let Some(c) = self.cert(surface) {
                    let i = c.info();
                    tracing::info!(
                        %surface,
                        subject = %i.subject,
                        not_after_ms = i.not_after_ms,
                        fingerprint = %i.sha256_fingerprint,
                        "TLS certificate reloaded"
                    );
                }
            }
            ReloadOutcome::Failed(error) => {
                tracing::error!(%surface, %error, "TLS certificate reload failed; keeping the previous certificate");
                let _ = self
                    .inner
                    .failures_tx
                    .send(ReloadFailure { surface, error });
            }
        }
    }

    /// Reloads every surface whose files changed (the 30 s watch).
    pub fn check_all(&self) {
        for s in Surface::ALL {
            if let Some(c) = self.cert(s) {
                let outcome = c.check();
                self.apply(s, outcome, false);
            }
        }
    }

    /// Re-reads every surface's files (SIGHUP).
    pub fn reload_all(&self) {
        for s in Surface::ALL {
            if let Some(c) = self.cert(s) {
                let outcome = c.reload();
                self.apply(s, outcome, true);
            }
        }
    }

    /// Spawns the reload loop: [`check_all`](Self::check_all) every `interval` and
    /// [`reload_all`](Self::reload_all) on SIGHUP (Unix). Installs the SIGHUP handler only
    /// when something is enabled, so a broker without TLS keeps the default SIGHUP action.
    pub fn spawn_reloader(
        &self,
        interval: Duration,
        shutdown: CancellationToken,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !self.any_enabled() {
            return None;
        }
        let reg = self.clone();
        Some(tokio::spawn(async move {
            #[cfg(unix)]
            let mut hup = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::hangup(),
            ) {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!(error = %e, "cannot listen for SIGHUP; TLS reloads only on file changes");
                    None
                }
            };
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                #[cfg(unix)]
                let hup_fut = async {
                    match hup.as_mut() {
                        Some(h) => h.recv().await,
                        None => std::future::pending().await,
                    }
                };
                #[cfg(not(unix))]
                let hup_fut = std::future::pending::<Option<()>>();
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = tick.tick() => {
                        let reg = reg.clone();
                        let _ = tokio::task::spawn_blocking(move || reg.check_all()).await;
                    }
                    _ = hup_fut => {
                        tracing::info!("SIGHUP: reloading TLS certificates");
                        let reg = reg.clone();
                        let _ = tokio::task::spawn_blocking(move || reg.reload_all()).await;
                    }
                }
            }
        }))
    }

    /// Seconds until the served certificate of `surface` expires (negative once expired).
    pub fn expiry_seconds(&self, surface: Surface, now_ms: u64) -> Option<f64> {
        let c = self.cert(surface)?;
        Some((c.info().not_after_ms as f64 - now_ms as f64) / 1000.0)
    }

    /// Alert candidates for this node: `tls_cert_expiring:{node}/{surface}` (warning within
    /// [`CERT_EXPIRY_WARNING_DAYS`], critical within [`CERT_EXPIRY_CRITICAL_DAYS`] or expired)
    /// and `tls_reload_failed:{node}/{surface}` (while the last reload failed).
    pub fn alerts(&self, node_id: &str, now_ms: u64) -> Vec<Alert> {
        let mut out = Vec::new();
        for s in Surface::ALL {
            let Some(c) = self.cert(s) else { continue };
            let info = c.info();
            let left_ms = info.not_after_ms as i128 - now_ms as i128;
            if left_ms < (CERT_EXPIRY_WARNING_DAYS * DAY_MS) as i128 {
                let expired = left_ms <= 0;
                let critical = expired || left_ms < (CERT_EXPIRY_CRITICAL_DAYS * DAY_MS) as i128;
                let when = if expired {
                    "has expired".to_string()
                } else {
                    let days = (left_ms as f64 / DAY_MS as f64).floor() as u64;
                    match days {
                        0 => "expires in less than a day".to_string(),
                        1 => "expires in 1 day".to_string(),
                        d => format!("expires in {d} days"),
                    }
                };
                out.push(Alert {
                    id: format!("tls_cert_expiring:{node_id}/{s}"),
                    severity: if critical {
                        Health::Critical
                    } else {
                        Health::Warning
                    },
                    kind: AlertKind::TlsCertExpiring,
                    title: format!("TLS certificate for {s} on {node_id} {when}"),
                    detail: format!(
                        "{} (issuer {}) is valid until {}",
                        info.subject,
                        info.issuer,
                        fmt_utc(info.not_after_ms)
                    ),
                    possible_reason: Some(
                        "Renew the certificate and replace the PEM files; the broker reloads \
                         them within 30 s (or on SIGHUP) without a restart."
                            .into(),
                    ),
                    link: Some("/settings/security".into()),
                    channel: None,
                    group: None,
                    since_ms: 0,
                    params: {
                        let mut p = tls_params(node_id, s);
                        if expired {
                            p.insert("expired".into(), true.into());
                        } else {
                            let days = (left_ms as f64 / DAY_MS as f64).floor() as u64;
                            p.insert("days_left".into(), days.into());
                        }
                        p
                    },
                });
            }
            if let Some(err) = c.last_error() {
                out.push(Alert {
                    id: format!("tls_reload_failed:{node_id}/{s}"),
                    severity: Health::Warning,
                    kind: AlertKind::TlsReloadFailed,
                    title: format!("TLS certificate reload failed for {s} on {node_id}"),
                    detail: err,
                    possible_reason: Some(
                        "The new certificate or key could not be loaded (bad PEM, or key and \
                         certificate don't match); the previous certificate is still served."
                            .into(),
                    ),
                    link: Some("/settings/security".into()),
                    channel: None,
                    group: None,
                    since_ms: 0,
                    params: tls_params(node_id, s),
                });
            }
        }
        out
    }
}

/// `YYYY-MM-DD HH:MM UTC`.
fn fmt_utc(ms: u64) -> String {
    match time::OffsetDateTime::from_unix_timestamp((ms / 1000) as i64) {
        Ok(t) => format!(
            "{:04}-{:02}-{:02} {:02}:{:02} UTC",
            t.year(),
            u8::from(t.month()),
            t.day(),
            t.hour(),
            t.minute()
        ),
        Err(_) => format!("{ms} ms"),
    }
}

/// Alert params shared by the TLS alerts (`{node, surface}`), for client-side localisation.
fn tls_params(
    node_id: &str,
    surface: impl std::fmt::Display,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    let mut p = std::collections::BTreeMap::new();
    p.insert("node".into(), node_id.into());
    p.insert("surface".into(), surface.to_string().into());
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev::{DevCa, Usage, write_file};

    fn cert_valid_for(dir: &std::path::Path, days: i64) -> Arc<ReloadableCert> {
        let ca = DevCa::generate("CA").unwrap();
        let now = time::OffsetDateTime::now_utc();
        let p = ca
            .issue_with_validity(
                "srv",
                &["localhost".into()],
                Usage::Server,
                now - time::Duration::days(30),
                now + time::Duration::days(days),
            )
            .unwrap();
        let c = dir.join(format!("c{days}.pem"));
        let k = dir.join(format!("k{days}.pem"));
        write_file(&c, p.cert_pem.as_bytes(), false).unwrap();
        write_file(&k, p.key_pem.as_bytes(), true).unwrap();
        ReloadableCert::load(c, k).unwrap()
    }

    #[test]
    fn status_and_alerts() {
        let dir = tempfile::tempdir().unwrap();
        let reg = TlsRegistry::disabled();
        assert!(!reg.any_enabled());
        let st = reg.status();
        assert!(!st.client.enabled && !st.http.enabled && !st.cluster.enabled);
        assert!(reg.alerts("n1", fluxmq_proto::now_ms()).is_empty());

        let reg = TlsRegistry::builder()
            .surface(Surface::Client, cert_valid_for(dir.path(), 100), true)
            .surface(Surface::Http, cert_valid_for(dir.path(), 10), false)
            .surface(Surface::Cluster, cert_valid_for(dir.path(), 1), true)
            .build();
        reg.counter(Surface::Client).inc();
        let st = reg.status();
        assert!(st.client.enabled && st.client.mutual);
        assert_eq!(st.client.handshake_failures_total, 1);
        assert!(st.http.enabled && !st.http.mutual);
        assert!(
            st.client
                .certificate
                .as_ref()
                .unwrap()
                .subject
                .contains("srv")
        );

        let now = fluxmq_proto::now_ms();
        let alerts = reg.alerts("n1", now);
        assert_eq!(alerts.len(), 2, "{alerts:?}");
        let http = alerts
            .iter()
            .find(|a| a.id == "tls_cert_expiring:n1/http")
            .unwrap();
        assert_eq!(http.severity, Health::Warning);
        assert!(
            http.title.contains("expires in 9 days") || http.title.contains("expires in 10 days")
        );
        let cluster = alerts
            .iter()
            .find(|a| a.id == "tls_cert_expiring:n1/cluster")
            .unwrap();
        assert_eq!(cluster.severity, Health::Critical);
        // Expired.
        let later = now + 2 * DAY_MS;
        let a = reg.alerts("n1", later);
        assert!(a.iter().any(|a| a.title.contains("has expired")));
        assert!(reg.expiry_seconds(Surface::Cluster, later).unwrap() < 0.0);

        // Reload failure → alert + event.
        let mut rx = reg.subscribe_failures();
        let c = reg.cert(Surface::Client).unwrap();
        write_file(c.key_path(), b"broken", true).unwrap();
        reg.check_all();
        let f = rx.try_recv().unwrap();
        assert_eq!(f.surface, Surface::Client);
        assert!(reg.status().client.last_error.is_some());
        assert!(
            reg.alerts("n1", now)
                .iter()
                .any(|a| a.id == "tls_reload_failed:n1/client"
                    && a.kind == AlertKind::TlsReloadFailed)
        );
    }
}

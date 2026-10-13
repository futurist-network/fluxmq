//! `GET /api/tls` (viewer) and the TLS Prometheus families (v0.4).

use axum::Json;
use axum::extract::State;
use fluxmq_proto::tls::TlsStatus;
use fluxmq_telemetry::prometheus::Writer;
use fluxmq_tls::registry::Surface;

use crate::state::AppState;

/// `GET /api/tls` — what is enabled per surface, the served certificates and their expiry,
/// the last reload error and failed handshakes. Node-local.
pub async fn status(State(s): State<AppState>) -> Json<TlsStatus> {
    let mut st = s.tls.status();
    st.node_id = s.broker.node_id().to_string();
    Json(st)
}

/// `fluxmq_tls_handshake_failures_total{surface}` (every surface) and
/// `fluxmq_tls_cert_expiry_seconds{surface}` (enabled surfaces; negative once expired).
/// Empty when TLS is not configured at all (the v0.3 exposition is unchanged).
pub fn metrics(s: &AppState) -> String {
    if !s.tls.any_enabled() {
        return String::new();
    }
    let mut w = Writer::default();
    let node = s.broker.node_id();
    w.family(
        "fluxmq_tls_handshake_failures_total",
        "counter",
        "Failed TLS handshakes (plaintext on a TLS port, bad or missing client certificate, protocol mismatch, timeout).",
    );
    for surface in Surface::ALL {
        w.sample(
            "fluxmq_tls_handshake_failures_total",
            &[("node", node), ("surface", surface.as_str())],
            s.tls.counter(surface).get() as f64,
        );
    }
    let now = fluxmq_proto::now_ms();
    let expiring: Vec<(Surface, f64)> = Surface::ALL
        .into_iter()
        .filter_map(|surface| Some((surface, s.tls.expiry_seconds(surface, now)?)))
        .collect();
    if !expiring.is_empty() {
        w.family(
            "fluxmq_tls_cert_expiry_seconds",
            "gauge",
            "Seconds until the served TLS certificate expires (negative once expired).",
        );
        for (surface, secs) in expiring {
            w.sample(
                "fluxmq_tls_cert_expiry_seconds",
                &[("node", node), ("surface", surface.as_str())],
                secs.round(),
            );
        }
    }
    w.finish()
}

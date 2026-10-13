//! `GET /api/audit` and `GET /api/health`.

use axum::extract::State;
use fluxmq_proto::api::{AuditEntry, HealthResponse};
use serde::Deserialize;

use crate::extract::Query;
use crate::state::AppState;

pub const DEFAULT_LIMIT: usize = 100;

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    pub limit: Option<usize>,
}

pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<AuditQuery>,
) -> axum::Json<Vec<AuditEntry>> {
    let limit = q
        .limit
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, s.audit.capacity());
    axum::Json(s.audit.recent(limit))
}

fn build_features() -> Vec<String> {
    let mut f = Vec::new();
    if cfg!(feature = "profiling") {
        f.push("profiling".to_string());
    }
    f
}

pub async fn health(State(s): State<AppState>) -> axum::Json<HealthResponse> {
    axum::Json(HealthResponse {
        status: "ok".into(),
        version: fluxmq_proto::VERSION.into(),
        node_id: s.broker.node_id().to_string(),
        uptime_secs: s.started_at.elapsed().as_secs(),
        features: build_features(),
    })
}

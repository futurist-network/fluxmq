//! Backups (v0.3, contract `fluxmq_proto::backup`): configuration, status/progress, manual
//! runs, target probe, history and deletion. Reads never return secrets (redacted per the
//! uniform rule); permissions live in the central table (`auth.rs`).
//!
//! Cluster: the controller runs backups. `POST /api/backups/run` and `GET /api/backups/status`
//! on another node are answered by the controller (internal API); every node answers
//! `/internal/v1/backup/partial` with the partitions it leads ([`ClusterBackupHook`] is the
//! controller-side transport).

use std::collections::BTreeMap;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use fluxmq_backup::BackupError;
use fluxmq_backup::cluster::{ClusterHook, INTERNAL_PARTIAL_PATH, Partial, PartialRequest};
use fluxmq_cluster::{Cluster, TransportError};
use fluxmq_proto::api::ERROR_UNSUPPORTED_IN_CLUSTER;
use fluxmq_proto::backup::{
    BackupConfig, BackupInfo, BackupManifest, BackupOverview, BackupTarget,
};
use fluxmq_proto::cluster::NodeStatus;
use fluxmq_proto::notify::TestResult;
use futures::future::BoxFuture;
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Json, Path};
use crate::state::AppState;

/// BackupError → HTTP: 400 invalid / not configured / unsupported (cluster), 404, 409
/// running, 502 target errors, 500 otherwise.
pub fn backup_err(e: BackupError) -> ApiErr {
    match e {
        BackupError::Invalid(m) => ApiErr::bad_request(m),
        BackupError::NotConfigured => {
            ApiErr::bad_request("backups are not configured (PUT /api/backups/config first)")
        }
        BackupError::Unsupported(m) => ApiErr::bad_request(m),
        BackupError::UnsupportedInCluster(m) => {
            ApiErr::new(StatusCode::BAD_REQUEST, ERROR_UNSUPPORTED_IN_CLUSTER, m)
        }
        BackupError::NotController(m) => {
            ApiErr::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", m)
        }
        BackupError::NotFound(m) => ApiErr::not_found(m),
        BackupError::Conflict(m) => ApiErr::conflict(m),
        BackupError::Store(e) => ApiErr::new(
            StatusCode::BAD_GATEWAY,
            "internal",
            format!("backup target: {e}"),
        ),
        BackupError::Meta(fluxmq_meta::MetaError::Unavailable(m)) => ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal",
            format!("metadata store unavailable: {m}"),
        ),
        e => {
            tracing::error!(error = %e, "backup error");
            ApiErr::internal(e.to_string())
        }
    }
}

fn target_kind(t: &BackupTarget) -> &'static str {
    match t {
        BackupTarget::S3 { .. } => "s3",
        BackupTarget::Filesystem { .. } => "filesystem",
    }
}

/// `GET /api/backups/config` — redacted config, `null` when unset.
pub async fn get_config(State(s): State<AppState>) -> ApiResult<axum::Json<Option<BackupConfig>>> {
    s.backups
        .config_redacted()
        .map(axum::Json)
        .map_err(backup_err)
}

/// `PUT /api/backups/config` — `REDACTED` keeps a stored secret, `""` removes it.
pub async fn put_config(
    State(s): State<AppState>,
    Json(cfg): Json<BackupConfig>,
) -> ApiResult<axum::Json<BackupConfig>> {
    let saved = s.backups.put_config(cfg).await.map_err(backup_err)?;
    s.audit(
        "backup.config",
        target_kind(&saved.target),
        json!({
            "enabled": saved.enabled,
            "interval_secs": saved.interval_secs,
            "keep_last": saved.keep_last,
            "keep_days": saved.keep_days,
            "include_messages": saved.include_messages,
            "encrypted": saved.encryption_key.is_some(),
        }),
    );
    Ok(axum::Json(saved))
}

/// `GET /api/backups/status` — in a cluster the controller's (it runs the backups); falls
/// back to this node's view when the controller can't be reached.
pub async fn status(State(s): State<AppState>) -> axum::Json<BackupOverview> {
    if let Some(ctrl) = super::cluster::remote_controller(&s) {
        match s
            .cluster
            .internal_http()
            .get::<_, BackupOverview>(&ctrl.http_addr, INTERNAL_STATUS_PATH, &())
            .await
        {
            Ok(o) => return axum::Json(o),
            Err(e) => {
                tracing::debug!(controller = %ctrl.id, error = %e, "backup status proxy failed")
            }
        }
    }
    axum::Json(s.backups.overview())
}

/// `POST /api/backups/run` — starts a backup; 202 with its initial info, 409 if one runs.
/// In a cluster it is started on the controller.
pub async fn run(State(s): State<AppState>) -> ApiResult<(StatusCode, axum::Json<BackupInfo>)> {
    let info = match super::cluster::remote_controller(&s) {
        Some(ctrl) => s
            .cluster
            .internal_http()
            .post::<_, BackupInfo>(&ctrl.http_addr, INTERNAL_RUN_PATH, &())
            .await
            .map_err(|e| proxied_err(&ctrl.id, e))?,
        None => s.backups.run("manual").map_err(backup_err)?,
    };
    s.audit("backup.run", &info.id, json!({ "trigger": "manual" }));
    Ok((StatusCode::ACCEPTED, axum::Json(info)))
}

/// A peer's error response re-emitted as is (status + `ApiError` body, incl. the code).
fn proxied_err(node: &str, e: TransportError) -> ApiErr {
    if let TransportError::Status(code, body) = &e
        && let Ok(b) = serde_json::from_str::<fluxmq_proto::api::ApiError>(body)
        && let Ok(status) = StatusCode::from_u16(*code)
    {
        return ApiErr { status, body: b };
    }
    ApiErr::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        format!("controller {node}: {e}"),
    )
}

/// `POST /api/backups/test-target` — write + read + delete probe; nothing is stored.
pub async fn test_target(
    State(s): State<AppState>,
    Json(target): Json<BackupTarget>,
) -> axum::Json<TestResult> {
    let kind = target_kind(&target);
    let res = s.backups.test_target(target).await;
    s.audit(
        "backup.test_target",
        kind,
        json!({ "ok": res.ok, "error": res.error }),
    );
    axum::Json(res)
}

/// `GET /api/backups` — history, newest first (empty when not configured).
pub async fn list(State(s): State<AppState>) -> ApiResult<axum::Json<Vec<BackupInfo>>> {
    match s.backups.list().await {
        Ok(v) => Ok(axum::Json(v)),
        Err(BackupError::NotConfigured) => Ok(axum::Json(Vec::new())),
        Err(e) => Err(backup_err(e)),
    }
}

/// `GET /api/backups/{id}` — what the backup holds ([`BackupManifest`]: info, encryption
/// fingerprint, per-channel summary); `latest` resolves to the newest backup.
pub async fn get(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<axum::Json<BackupManifest>> {
    let m = s.backups.get(&id).await.map_err(backup_err)?;
    Ok(axum::Json(m.summary()))
}

pub async fn delete(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult<StatusCode> {
    s.backups.delete(&id).await.map_err(backup_err)?;
    s.audit("backup.delete", &id, json!({}));
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------------------

const INTERNAL_RUN_PATH: &str = "/internal/v1/backup/run";
const INTERNAL_STATUS_PATH: &str = "/internal/v1/backup/status";
/// A node's partial may upload a lot of data: generous per-request timeout.
const PARTIAL_TIMEOUT: Duration = Duration::from_secs(6 * 3600);
/// Oldest version that answers the internal backup API.
const MIN_BACKUP_VERSION: (u64, u64) = (0, 3);

async fn internal_partial(
    State(s): State<AppState>,
    Json(req): Json<PartialRequest>,
) -> ApiResult<axum::Json<Partial>> {
    s.backups
        .partial(req)
        .await
        .map(axum::Json)
        .map_err(backup_err)
}

async fn internal_run(
    State(s): State<AppState>,
) -> ApiResult<(StatusCode, axum::Json<BackupInfo>)> {
    let info = s.backups.run("manual").map_err(backup_err)?;
    Ok((StatusCode::ACCEPTED, axum::Json(info)))
}

async fn internal_status(State(s): State<AppState>) -> axum::Json<BackupOverview> {
    axum::Json(s.backups.overview())
}

/// Node-to-node routes, merged into `cluster::internal`.
pub fn internal() -> axum::Router<AppState> {
    axum::Router::new()
        .route(INTERNAL_PARTIAL_PATH, axum::routing::post(internal_partial))
        .route(INTERNAL_RUN_PATH, axum::routing::post(internal_run))
        .route(INTERNAL_STATUS_PATH, axum::routing::get(internal_status))
}

/// `major.minor` of a version string (`0.3.0`, `0.3.0-dev`).
fn major_minor(v: &str) -> Option<(u64, u64)> {
    let mut it = v.split(['.', '-']);
    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
}

/// Controller-side transport of cluster backups.
pub struct ClusterBackupHook(pub Cluster);

impl ClusterHook for ClusterBackupHook {
    fn is_controller(&self) -> bool {
        let md = self.0.metadata(&["\0".to_string()]);
        md.controller.as_deref() == Some(self.0.node_id())
    }

    fn unsupported(&self) -> Option<String> {
        let old: Vec<String> = self
            .0
            .metadata(&["\0".to_string()])
            .nodes
            .into_iter()
            .filter(|n| major_minor(&n.version).is_none_or(|v| v < MIN_BACKUP_VERSION))
            .map(|n| format!("{} ({})", n.id, n.version))
            .collect();
        (!old.is_empty()).then(|| {
            format!(
                "this cluster can't run backups: node(s) {} predate 0.3.0 and don't support \
                 the internal backup API; upgrade every node",
                old.join(", ")
            )
        })
    }

    fn expected(&self) -> BTreeMap<String, u32> {
        self.0
            .metadata(&[])
            .channels
            .into_iter()
            .map(|(c, parts)| (c, parts.len() as u32))
            .collect()
    }

    fn remote_partials(
        &self,
        req: PartialRequest,
    ) -> BoxFuture<'static, fluxmq_backup::Result<Vec<Partial>>> {
        let http = self.0.internal_http().with_timeout(PARTIAL_TIMEOUT);
        // Down / leaving nodes lead nothing (their partitions failed over); a partition no
        // node reports fails the coverage check instead.
        let peers: Vec<_> = self
            .0
            .peers()
            .into_iter()
            .filter(|n| matches!(n.status, NodeStatus::Up | NodeStatus::Joining))
            .collect();
        Box::pin(async move {
            let calls = peers.into_iter().map(|n| {
                let (http, req) = (http.clone(), req.clone());
                async move {
                    http.post::<_, Partial>(&n.http_addr, INTERNAL_PARTIAL_PATH, &req)
                        .await
                        .map_err(|e| {
                            let msg = match &e {
                                TransportError::Status(_, body) => {
                                    serde_json::from_str::<fluxmq_proto::api::ApiError>(body)
                                        .map(|b| b.message)
                                        .unwrap_or_else(|_| body.clone())
                                }
                                other => other.to_string(),
                            };
                            BackupError::Other(format!("node {}: {msg}", n.id))
                        })
                }
            });
            futures::future::try_join_all(calls).await
        })
    }
}

//! `/api/auth/*`: who am I, access-token CRUD (admin), certificate bindings (admin, v0.5).
//! Secrets are returned exactly once by `POST /api/auth/tokens` and never logged or audited.

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use fluxmq_auth::Principal;
use fluxmq_proto::auth::{
    CertBinding, CertBindingList, CreateCertBindingRequest, CreateTokenRequest,
    CreateTokenResponse, TokenInfo, WhoAmI,
};
use serde_json::json;

use crate::error::ApiResult;
use crate::extract::{Json, Path};
use crate::state::AppState;

/// `GET /api/auth/me`.
pub async fn me(
    State(s): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> axum::Json<WhoAmI> {
    axum::Json(s.auth.whoami(&principal))
}

/// `GET /api/auth/tokens` (admin).
pub async fn list(State(s): State<AppState>) -> axum::Json<Vec<TokenInfo>> {
    axum::Json(s.auth.list_tokens())
}

/// `POST /api/auth/tokens` (admin). The response carries the secret; it is shown once.
pub async fn create(
    State(s): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(req): Json<CreateTokenRequest>,
) -> ApiResult<(StatusCode, axum::Json<CreateTokenResponse>)> {
    let ttl_secs = req.ttl_secs;
    let created = s.auth.create_token(req, &principal).await?;
    let info = &created.info;
    s.audit(
        "token.create",
        &info.id,
        json!({
            "name": info.name,
            "role": info.role,
            "scopes": info.scopes,
            "ttl_secs": ttl_secs,
            "expires_at_ms": info.expires_at_ms,
        }),
    );
    Ok((StatusCode::CREATED, axum::Json(created)))
}

/// `DELETE /api/auth/tokens/{id}` (admin).
pub async fn revoke(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult<StatusCode> {
    let name = s
        .auth
        .list_tokens()
        .into_iter()
        .find(|t| t.id == id)
        .map(|t| t.name);
    s.auth.revoke_token(&id).await?;
    s.audit("token.revoke", &id, json!({ "name": name }));
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/auth/cert-bindings` (admin). `client_mtls_enabled = false` means the client port
/// does not verify client certificates, so the bindings are inert.
pub async fn list_cert_bindings(State(s): State<AppState>) -> axum::Json<CertBindingList> {
    axum::Json(CertBindingList {
        bindings: s.auth.list_cert_bindings(),
        client_mtls_enabled: s.auth.client_mtls_enabled(),
    })
}

/// `POST /api/auth/cert-bindings` (admin). A CN that already has a binding → 409 `conflict`.
pub async fn create_cert_binding(
    State(s): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(req): Json<CreateCertBindingRequest>,
) -> ApiResult<(StatusCode, axum::Json<CertBinding>)> {
    let binding = s.auth.create_cert_binding(req, &principal).await?;
    s.audit(
        "cert_binding.create",
        &binding.cn,
        json!({ "role": binding.role, "scopes": binding.scopes }),
    );
    Ok((StatusCode::CREATED, axum::Json(binding)))
}

/// `DELETE /api/auth/cert-bindings/{cn}` (admin). Sessions authenticated with the binding are
/// closed on every node.
pub async fn delete_cert_binding(
    State(s): State<AppState>,
    Path(cn): Path<String>,
) -> ApiResult<StatusCode> {
    let removed = s.auth.delete_cert_binding(&cn).await?;
    s.audit("cert_binding.delete", &cn, json!({ "role": removed.role }));
    Ok(StatusCode::NO_CONTENT)
}

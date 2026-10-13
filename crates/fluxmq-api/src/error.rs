//! Uniform error responses (`ApiError { error, message, position? }`) and the [`CoreError`] mapping.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use fluxmq_core::CoreError;
use fluxmq_proto::api::ApiError;

/// An error returned by a handler: an HTTP status plus the JSON [`ApiError`] body.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiErr {
    pub status: StatusCode,
    pub body: ApiError,
}

/// Handler result alias.
pub type ApiResult<T> = Result<T, ApiErr>;

impl ApiErr {
    pub fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiError {
                error: code.to_string(),
                message: message.into(),
                position: None,
            },
        }
    }

    /// 400 for a filter DSL syntax error, with `ApiError.position` = byte offset of the error.
    pub fn filter_syntax(e: &fluxmq_router::filter::FilterError) -> Self {
        let mut err = Self::bad_request(e.to_string());
        err.body.position = Some(e.position);
        err
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
}

impl From<CoreError> for ApiErr {
    fn from(e: CoreError) -> Self {
        match &e {
            CoreError::ChannelNotFound(_) | CoreError::GroupNotFound { .. } => {
                Self::not_found(e.to_string())
            }
            CoreError::ChannelExists(_) => Self::conflict(e.to_string()),
            CoreError::InvalidArgument(_) | CoreError::UnknownDelivery(_) => {
                Self::bad_request(e.to_string())
            }
            // Transient: the error code stays in the documented set, the status tells
            // clients to retry later.
            CoreError::ShuttingDown => {
                Self::new(StatusCode::SERVICE_UNAVAILABLE, "internal", e.to_string())
            }
            CoreError::Io(_) | CoreError::Corrupt(_) => {
                tracing::error!(error = %e, "core error");
                Self::internal(e.to_string())
            }
            // Replicated mode (transient: leadership moves / ISR recovers).
            CoreError::NotLeader { .. }
            | CoreError::NotEnoughReplicas { .. }
            | CoreError::AckTimeout { .. }
            | CoreError::StaleEpoch { .. } => {
                Self::new(StatusCode::SERVICE_UNAVAILABLE, "internal", e.to_string())
            }
        }
    }
}

impl From<tokio::task::JoinError> for ApiErr {
    fn from(e: tokio::task::JoinError) -> Self {
        tracing::error!(error = %e, "blocking task failed");
        Self::internal("internal task failed")
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_error_mapping() {
        let cases = [
            (
                CoreError::ChannelNotFound("a".into()),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                CoreError::ChannelExists("a".into()),
                StatusCode::CONFLICT,
                "conflict",
            ),
            (
                CoreError::InvalidArgument("x".into()),
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            (
                CoreError::UnknownDelivery(1),
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            (
                CoreError::Corrupt("x".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
            ),
            (
                CoreError::ShuttingDown,
                StatusCode::SERVICE_UNAVAILABLE,
                "internal",
            ),
        ];
        for (err, status, code) in cases {
            let e = ApiErr::from(err);
            assert_eq!(e.status, status);
            assert_eq!(e.body.error, code);
        }
    }
}

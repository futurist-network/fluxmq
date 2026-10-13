//! Extractors that report rejections as [`ApiErr`] (JSON `bad_request`) instead of axum's
//! plain-text defaults.

use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use crate::error::ApiErr;

/// JSON request body.
pub struct Json<T>(pub T);

impl<S, T> FromRequest<S> for Json<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiErr;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(axum::Json(v)) => Ok(Json(v)),
            Err(e) => Err(ApiErr::bad_request(e.body_text())),
        }
    }
}

/// Optional JSON request body: an empty (or whitespace-only) body is `None`, anything else
/// must be valid JSON of `T` (400 otherwise). Content-Type is not required.
pub struct OptionalJson<T>(pub Option<T>);

impl<S, T> FromRequest<S> for OptionalJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiErr;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = axum::body::Bytes::from_request(req, state)
            .await
            .map_err(|e| ApiErr::bad_request(e.body_text()))?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(OptionalJson(None));
        }
        serde_json::from_slice(&bytes)
            .map(|v| OptionalJson(Some(v)))
            .map_err(|e| ApiErr::bad_request(format!("invalid JSON body: {e}")))
    }
}

/// Query string.
pub struct Query<T>(pub T);

impl<S, T> FromRequestParts<S> for Query<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiErr;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Query::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Query(v)) => Ok(Query(v)),
            Err(e) => Err(ApiErr::bad_request(e.body_text())),
        }
    }
}

/// Path parameters.
pub struct Path<T>(pub T);

impl<S, T> FromRequestParts<S> for Path<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiErr;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Path::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Path(v)) => Ok(Path(v)),
            Err(e) => Err(ApiErr::bad_request(e.body_text())),
        }
    }
}

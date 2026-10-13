//! Admission of a live connection (design §4, §5): origin check, authentication (the handler
//! does it itself: tokens may arrive in a WS subprotocol or, for SSE, the query string),
//! `Subscribe(channel)` authorization, parameters, start position, connection limits.
//! Everything here happens before the upgrade / stream start, so failures are HTTP statuses.

use std::pin::Pin;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, RawQuery, State};
use axum::http::header::{AUTHORIZATION, HOST, ORIGIN, SEC_WEBSOCKET_PROTOCOL};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use fluxmq_auth::{Action, Principal, SessionEnd};
use fluxmq_core::LiveReader;

use super::envelope::Transport;
use super::params::{self, SubSpec};
use super::sub::{self, Start, StartError};
use super::{ChannelSlot, ConnSlot, Reject};
use crate::error::ApiErr;
use crate::state::AppState;

/// The WS subprotocol the server speaks (and the only one it echoes).
pub const SUBPROTOCOL: &str = "fluxmq.live.v1";
/// Browsers pass the token as a second offered subprotocol `fluxmq.token.<token>`.
pub const TOKEN_PROTOCOL_PREFIX: &str = "fluxmq.token.";
/// Max inbound WS message (client commands are small JSON).
const MAX_CLIENT_MESSAGE: usize = 64 * 1024;

/// An admitted connection with its first subscription (the one the URL describes).
pub struct Admitted {
    pub principal: Principal,
    pub reader: LiveReader,
    pub spec: SubSpec,
    pub start: Start,
    pub conn: ConnSlot,
    pub chan: ChannelSlot,
    pub ended: Pin<Box<dyn Future<Output = SessionEnd> + Send>>,
    /// `Origin` of an allowed cross-origin request (SSE echoes it for CORS).
    pub cors_origin: Option<String>,
}

fn reject(state: &AppState, r: Reject, e: ApiErr) -> ApiErr {
    state.live.metrics().reject(r);
    e
}

/// `None` = allowed. Requests without `Origin` (non-browser clients) pass; browsers must be
/// same-origin (`Origin` host:port = `Host`) or listed in `--live-allowed-origins`.
fn check_origin(headers: &HeaderMap, allowed: &[String]) -> Result<Option<String>, ()> {
    let Some(origin) = headers.get(ORIGIN).and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let host = headers
        .get(HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let origin_host = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
        .unwrap_or("");
    if !host.is_empty() && origin_host.eq_ignore_ascii_case(host) {
        return Ok(None);
    }
    if allowed
        .iter()
        .any(|a| a == "*" || a.trim_end_matches('/').eq_ignore_ascii_case(origin))
    {
        return Ok(Some(origin.to_string()));
    }
    Err(())
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let v = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = v.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim().to_string())
        .filter(|t| !t.is_empty())
}

/// The token offered as `fluxmq.token.<token>` in `Sec-WebSocket-Protocol`.
fn protocol_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .find_map(|p| {
            p.trim()
                .strip_prefix(TOKEN_PROTOCOL_PREFIX)
                .map(str::to_string)
        })
        .filter(|t| !t.is_empty())
}

fn query_pairs(query: Option<&str>) -> Vec<(String, String)> {
    let Some(q) = query else {
        return Vec::new();
    };
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                crate::auth::percent_decode(k),
                crate::auth::percent_decode(v),
            )
        })
        .collect()
}

pub fn start_error(e: StartError) -> ApiErr {
    match e {
        StartError::Io(m) => ApiErr::internal(m),
        other => ApiErr::bad_request(other.to_string()),
    }
}

async fn admit(
    state: &AppState,
    channel: &str,
    headers: &HeaderMap,
    query: Option<&str>,
    transport: Transport,
) -> Result<Admitted, ApiErr> {
    let cfg = state.live.config();
    let cors_origin = check_origin(headers, &cfg.allowed_origins).map_err(|()| {
        reject(
            state,
            Reject::Origin,
            ApiErr::new(
                StatusCode::FORBIDDEN,
                "origin_not_allowed",
                "this origin may not open live subscriptions (--live-allowed-origins)",
            ),
        )
    })?;
    let (spec, query_token) =
        params::from_query(query_pairs(query)).map_err(ApiErr::bad_request)?;
    let token = bearer(headers).or_else(|| match transport {
        Transport::Ws => protocol_token(headers),
        // `EventSource` cannot set headers. Query tokens are not accepted on the WS endpoint.
        Transport::Sse => query_token,
    });
    let principal = state
        .auth
        .authenticate(token.as_deref())
        .map_err(|e| reject(state, Reject::Unauthorized, ApiErr::from(e)))?;
    state
        .auth
        .authorize(&principal, Action::Subscribe(channel))
        .map_err(|e| reject(state, Reject::Forbidden, ApiErr::from(e)))?;
    let reader = state.broker.live_reader(channel).map_err(ApiErr::from)?;
    if !reader.standalone() && (0..reader.partitions()).all(|p| reader.visible_end(p).is_none()) {
        return Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "this node serves no partition of the channel; connect to a node holding a replica",
        ));
    }
    let start = sub::resolve(&reader, &spec).await.map_err(start_error)?;
    let conn = state
        .live
        .acquire_conn(principal.token_id.as_deref(), transport)
        .map_err(limit_error)?;
    let chan = state.live.acquire_channel(channel).map_err(limit_error)?;
    let ended = Box::pin(state.auth.session_watch(&principal));
    Ok(Admitted {
        principal,
        reader,
        spec,
        start,
        conn,
        chan,
        ended,
        cors_origin,
    })
}

pub fn limit_error(r: Reject) -> ApiErr {
    match r {
        Reject::NodeLimit => ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "limit",
            "live connection limit of this node reached",
        ),
        _ => ApiErr::new(
            StatusCode::TOO_MANY_REQUESTS,
            "limit",
            format!("live {} reached", r.label().replace('_', " ")),
        ),
    }
}

/// `GET /api/channels/{name}/live` — WebSocket.
pub async fn ws(
    State(state): State<AppState>,
    Path(name): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let admitted = match admit(&state, &name, &headers, query.as_deref(), Transport::Ws).await {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };
    upgrade
        .protocols([SUBPROTOCOL])
        .max_message_size(MAX_CLIENT_MESSAGE)
        .max_frame_size(MAX_CLIENT_MESSAGE)
        .on_upgrade(move |socket| super::ws::run(socket, state, admitted))
}

/// `GET /api/channels/{name}/events` — Server-Sent Events.
pub async fn sse(
    State(state): State<AppState>,
    Path(name): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    // `Last-Event-ID` (the browser's automatic reconnect) wins over `cursor=`.
    let mut query = query;
    if let Some(id) = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty())
    {
        let q = query.get_or_insert_with(String::new);
        if !q.is_empty() {
            q.push('&');
        }
        q.push_str("cursor=");
        q.push_str(id.trim());
    }
    match admit(&state, &name, &headers, query.as_deref(), Transport::Sse).await {
        Ok(a) => super::sse::response(state, a),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn h(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.append(*k, HeaderValue::from_str(v).unwrap());
        }
        m
    }

    #[test]
    fn origins() {
        let allowed = vec!["https://app.example.com".to_string()];
        assert_eq!(check_origin(&h(&[]), &allowed), Ok(None));
        assert_eq!(
            check_origin(
                &h(&[("origin", "http://b:7421"), ("host", "b:7421")]),
                &allowed
            ),
            Ok(None)
        );
        assert_eq!(
            check_origin(
                &h(&[("origin", "https://app.example.com"), ("host", "b:7421")]),
                &allowed
            ),
            Ok(Some("https://app.example.com".into()))
        );
        assert!(
            check_origin(
                &h(&[("origin", "https://evil.example"), ("host", "b:7421")]),
                &allowed
            )
            .is_err()
        );
        assert!(check_origin(&h(&[("origin", "null"), ("host", "b:7421")]), &[]).is_err());
        assert!(check_origin(&h(&[("origin", "https://x"), ("host", "b")]), &["*".into()]).is_ok());
    }

    #[test]
    fn token_sources() {
        let hs = h(&[("sec-websocket-protocol", "fluxmq.live.v1, fluxmq.token.abc")]);
        assert_eq!(protocol_token(&hs).as_deref(), Some("abc"));
        assert_eq!(
            protocol_token(&h(&[("sec-websocket-protocol", "fluxmq.live.v1")])),
            None
        );
        assert_eq!(
            bearer(&h(&[("authorization", "Bearer t1")])).as_deref(),
            Some("t1")
        );
        let q = query_pairs(Some("from=earliest&header.type=a%20b&access_token=x"));
        assert_eq!(q[1], ("header.type".into(), "a b".into()));
    }
}

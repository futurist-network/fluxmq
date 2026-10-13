//! Building and sending one webhook request, and mapping its outcome (module docs of
//! `fluxmq_proto::webhooks`).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use bytes::Bytes;
use fluxmq_proto::Message;
use fluxmq_proto::api::{JsonPayload, short_error};
use fluxmq_proto::webhooks::{PayloadFormat, WebhookEnvelope, WebhookSubscription};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};

use crate::signature;
use crate::ssrf::{GuardedResolver, PrivatePolicy, check_literal};

pub const H_DELIVERY_ID: &str = "x-fluxmq-delivery-id";
pub const H_ATTEMPT: &str = "x-fluxmq-attempt";
pub const H_SIGNATURE: &str = "x-fluxmq-signature";
/// Number of envelopes in a batched request (only set when batching).
pub const H_BATCH_SIZE: &str = "x-fluxmq-batch-size";
pub const H_CHANNEL: &str = "x-fluxmq-channel";
pub const H_PARTITION: &str = "x-fluxmq-partition";
pub const H_OFFSET: &str = "x-fluxmq-offset";
pub const H_KEY: &str = "x-fluxmq-key";
pub const H_TIMESTAMP: &str = "x-fluxmq-timestamp";
pub const H_HEADER_PREFIX: &str = "x-fluxmq-header-";
pub const USER_AGENT: &str = concat!("FluxMQ-Webhooks/", env!("CARGO_PKG_VERSION"));
/// Response bytes kept for `test` results / errors.
const SNIPPET: usize = 512;

/// Shared HTTP client: connection pooling, guarded DNS, no redirects, no proxy, rustls.
pub fn client(policy: PrivatePolicy) -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .dns_resolver(std::sync::Arc::new(GuardedResolver { policy }))
        .pool_idle_timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(64)
        .connect_timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .build()
        .expect("building the webhook HTTP client")
}

pub fn delivery_id(channel: &str, m: &Message) -> String {
    format!("{channel}/{}/{}", m.partition, m.offset)
}

pub fn json_payload(payload: &[u8]) -> JsonPayload {
    match std::str::from_utf8(payload) {
        Ok(s) => JsonPayload::Utf8(s.to_string()),
        Err(_) => JsonPayload::Base64(base64::engine::general_purpose::STANDARD.encode(payload)),
    }
}

pub fn envelope(sub: &WebhookSubscription, m: &Message, attempt: u32) -> WebhookEnvelope {
    WebhookEnvelope {
        delivery_id: delivery_id(&sub.channel, m),
        subscription: sub.name.clone(),
        channel: sub.channel.clone(),
        partition: m.partition,
        offset: m.offset,
        key: m.key.clone(),
        headers: m.headers.clone(),
        timestamp_ms: m.timestamp_ms,
        attempt,
        payload: json_payload(&m.payload),
    }
}

/// Percent-encodes bytes that are not visible ASCII (header values must be).
fn header_value(s: &str) -> HeaderValue {
    if s.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        return HeaderValue::from_str(s).expect("visible ASCII is a valid header value");
    }
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        if (0x20..0x7f).contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    HeaderValue::from_str(&out).expect("percent-encoded value is visible ASCII")
}

/// Request body + broker headers for a batch of `(message, attempt)` (non-empty).
pub fn build(sub: &WebhookSubscription, items: &[(&Message, u32)]) -> (Bytes, HeaderMap) {
    let mut h = HeaderMap::new();
    let (first, _) = items[0];
    let attempt = items.iter().map(|(_, a)| *a).max().unwrap_or(1);
    let body: Bytes = match sub.format {
        PayloadFormat::Raw => {
            let m = first;
            h.insert(H_CHANNEL, header_value(&sub.channel));
            h.insert(H_PARTITION, HeaderValue::from(m.partition));
            h.insert(H_OFFSET, HeaderValue::from(m.offset));
            if let Some(k) = &m.key {
                h.insert(H_KEY, header_value(k));
            }
            h.insert(H_TIMESTAMP, HeaderValue::from(m.timestamp_ms));
            for (k, v) in &m.headers {
                if let Ok(name) = HeaderName::from_bytes(format!("{H_HEADER_PREFIX}{k}").as_bytes())
                {
                    h.insert(name, header_value(v));
                }
            }
            m.payload.clone()
        }
        PayloadFormat::Envelope if sub.batch_max > 1 => {
            let envs: Vec<WebhookEnvelope> =
                items.iter().map(|(m, a)| envelope(sub, m, *a)).collect();
            h.insert(H_BATCH_SIZE, HeaderValue::from(envs.len()));
            Bytes::from(serde_json::to_vec(&envs).expect("envelopes serialize"))
        }
        PayloadFormat::Envelope => Bytes::from(
            serde_json::to_vec(&envelope(sub, first, attempt)).expect("envelope serializes"),
        ),
    };
    h.insert(
        H_DELIVERY_ID,
        header_value(&delivery_id(&sub.channel, first)),
    );
    h.insert(H_ATTEMPT, HeaderValue::from(attempt));
    (body, h)
}

/// One synthetic delivery (test button, circuit breaker probe): an envelope (or raw body) with
/// `{"test": true, …}`, a `flux-test: true` header and the given `delivery_id`.
pub fn synthetic(sub: &WebhookSubscription, delivery_id: &str, what: &str) -> (Bytes, HeaderMap) {
    let now = fluxmq_proto::now_ms();
    let payload = serde_json::json!({
        "test": true,
        "message": what,
        "subscription": sub.name,
    })
    .to_string();
    let msg = Message {
        partition: 0,
        offset: 0,
        key: None,
        headers: std::collections::BTreeMap::from([("flux-test".to_string(), "true".to_string())]),
        timestamp_ms: now,
        payload: Bytes::from(payload),
    };
    let (mut body, mut headers) = build(sub, &[(&msg, 1)]);
    if sub.format == PayloadFormat::Envelope {
        let mut env = envelope(sub, &msg, 1);
        env.delivery_id = delivery_id.to_string();
        body = if sub.batch_max > 1 {
            serde_json::to_vec(&[env])
        } else {
            serde_json::to_vec(&env)
        }
        .expect("envelope serializes")
        .into();
    }
    headers.insert(H_DELIVERY_ID, header_value(delivery_id));
    (body, headers)
}

/// A transport-level failure (no HTTP status): a short class (`fluxmq_proto::api::short_error`)
/// plus the full cause chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendError {
    /// Short class: `timeout`, `connection refused`, `connection failed`, `TLS error`,
    /// `DNS error`, `blocked by policy`, `request failed`.
    pub class: &'static str,
    /// Full cause chain (URL stripped).
    pub detail: String,
}

impl SendError {
    pub fn new(class: &'static str, detail: impl Into<String>) -> Self {
        Self {
            class,
            detail: detail.into(),
        }
    }

    /// Blocked by the SSRF policy at request time.
    pub fn blocked(detail: impl Into<String>) -> Self {
        Self::new(short_error::BLOCKED, detail)
    }

    /// Classifies a reqwest error.
    pub fn from_reqwest(e: reqwest::Error) -> Self {
        let (timeout, connect) = (e.is_timeout(), e.is_connect());
        let e = e.without_url();
        let detail = chain(&e);
        Self::new(short_error::classify(&detail, timeout, connect), detail)
    }

    /// DLQ / nack reason (stable, groups well in the DLQ inbox).
    pub fn reason(&self) -> &'static str {
        self.class
    }

    /// Never retried: the policy decides, not the network.
    pub fn permanent(&self) -> bool {
        self.class == short_error::BLOCKED
    }

    pub fn detail(&self) -> String {
        self.detail.clone()
    }
}

#[derive(Debug, Clone)]
pub struct SendResult {
    pub status: Option<u16>,
    pub retry_after: Option<Duration>,
    pub error: Option<SendError>,
    pub latency: Duration,
    pub snippet: Option<String>,
}

/// Full error chain (reqwest hides the cause, e.g. "connection refused", in sources).
fn chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        let cs = c.to_string();
        if !s.contains(&cs) {
            s.push_str(": ");
            s.push_str(&cs);
        }
        cur = c.source();
    }
    s
}

/// POSTs one request. Never panics; transport errors are classified.
pub async fn send(
    client: &reqwest::Client,
    sub: &WebhookSubscription,
    policy: &PrivatePolicy,
    body: Bytes,
    mut headers: HeaderMap,
    keep_snippet: bool,
) -> SendResult {
    let started = Instant::now();
    let fail = |e: SendError| SendResult {
        status: None,
        retry_after: None,
        error: Some(e),
        latency: started.elapsed(),
        snippet: None,
    };
    // IP-literal hosts bypass the resolver: check them here (the policy may have changed).
    if let Ok(u) = reqwest::Url::parse(&sub.url)
        && let Some(host) = u.host_str()
        && let Err(m) = check_literal(host, policy)
    {
        return fail(SendError::blocked(m));
    }
    for (k, v) in &sub.headers {
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(v),
        ) {
            headers.insert(n, v);
        }
    }
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(secret) = &sub.secret {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        headers.insert(
            H_SIGNATURE,
            header_value(&signature::sign(secret, t, &body)),
        );
    }
    let req = client
        .post(&sub.url)
        .headers(headers)
        .timeout(Duration::from_millis(u64::from(sub.timeout_ms)))
        .body(body);
    let mut resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return fail(SendError::from_reqwest(e)),
    };
    let status = resp.status().as_u16();
    let retry_after = resp
        .headers()
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_retry_after);
    let latency = started.elapsed();
    // Read a bounded prefix of the body (lets the connection be reused for small responses;
    // a large body is dropped, closing that connection).
    let mut snippet: Vec<u8> = Vec::new();
    let limit = if keep_snippet { SNIPPET } else { 16 * 1024 };
    let deadline = Duration::from_millis(u64::from(sub.timeout_ms)).saturating_sub(latency);
    let _ = tokio::time::timeout(deadline, async {
        while snippet.len() < limit {
            match resp.chunk().await {
                Ok(Some(c)) => snippet.extend_from_slice(&c),
                _ => break,
            }
        }
    })
    .await;
    let snippet = (keep_snippet || !(200..300).contains(&status)).then(|| {
        let n = snippet.len().min(SNIPPET);
        String::from_utf8_lossy(&snippet[..n]).into_owned()
    });
    SendResult {
        status: Some(status),
        retry_after,
        error: None,
        latency,
        snippet,
    }
}

/// `Retry-After`: delay-seconds or an HTTP-date.
pub fn parse_retry_after(v: &str) -> Option<Duration> {
    let v = v.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let at = httpdate::parse_http_date(v).ok()?;
    Some(
        at.duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

/// What to do with the deliveries of a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Ack,
    Retry {
        reason: String,
        retry_after: Option<Duration>,
    },
    Dead {
        reason: String,
    },
}

/// Outcome mapping of the contract.
pub fn classify(r: &SendResult, retry_on_4xx: bool) -> Verdict {
    match (r.status, &r.error) {
        (Some(s), _) if (200..300).contains(&s) => Verdict::Ack,
        (Some(s), _) => {
            let reason = format!("HTTP {s}");
            let retryable = matches!(s, 408 | 425 | 429)
                || s >= 500
                || (retry_on_4xx && (400..500).contains(&s));
            if retryable {
                Verdict::Retry {
                    reason,
                    retry_after: r.retry_after,
                }
            } else {
                Verdict::Dead { reason }
            }
        }
        (None, Some(e)) if e.permanent() => Verdict::Dead {
            reason: e.reason().to_string(),
        },
        (None, Some(e)) => Verdict::Retry {
            reason: e.reason().to_string(),
            retry_after: None,
        },
        (None, None) => Verdict::Retry {
            reason: "request failed".into(),
            retry_after: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn res(status: Option<u16>, error: Option<SendError>) -> SendResult {
        SendResult {
            status,
            retry_after: None,
            error,
            latency: Duration::ZERO,
            snippet: None,
        }
    }

    #[test]
    fn outcome_mapping() {
        let dead = |s: &str| Verdict::Dead { reason: s.into() };
        let retry = |s: &str| Verdict::Retry {
            reason: s.into(),
            retry_after: None,
        };
        assert_eq!(classify(&res(Some(200), None), false), Verdict::Ack);
        assert_eq!(classify(&res(Some(204), None), false), Verdict::Ack);
        assert_eq!(classify(&res(Some(500), None), false), retry("HTTP 500"));
        assert_eq!(classify(&res(Some(503), None), false), retry("HTTP 503"));
        for s in [408, 425, 429] {
            assert_eq!(
                classify(&res(Some(s), None), false),
                retry(&format!("HTTP {s}"))
            );
        }
        assert_eq!(classify(&res(Some(400), None), false), dead("HTTP 400"));
        assert_eq!(classify(&res(Some(404), None), false), dead("HTTP 404"));
        assert_eq!(classify(&res(Some(404), None), true), retry("HTTP 404"));
        assert_eq!(classify(&res(Some(301), None), false), dead("HTTP 301"));
        assert_eq!(classify(&res(Some(301), None), true), dead("HTTP 301"));
        let timeout = SendError::new(short_error::TIMEOUT, "operation timed out");
        assert_eq!(classify(&res(None, Some(timeout)), false), retry("timeout"));
        let refused = SendError::new(short_error::CONNECTION_REFUSED, "tcp connect error");
        assert_eq!(
            classify(&res(None, Some(refused)), false),
            retry("connection refused")
        );
        assert_eq!(
            classify(
                &res(None, Some(SendError::blocked("blocked address"))),
                true
            ),
            dead("blocked by policy")
        );
    }

    #[test]
    fn retry_after_forms() {
        assert_eq!(parse_retry_after("3"), Some(Duration::from_secs(3)));
        assert_eq!(parse_retry_after(" 0 "), Some(Duration::ZERO));
        let future = SystemTime::now() + Duration::from_secs(120);
        let d = parse_retry_after(&httpdate::fmt_http_date(future)).unwrap();
        assert!(d > Duration::from_secs(100) && d <= Duration::from_secs(120));
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(Duration::ZERO)
        );
        assert_eq!(parse_retry_after("soon"), None);
    }

    #[test]
    fn header_values_are_encoded() {
        assert_eq!(header_value("abc").to_str().unwrap(), "abc");
        assert_eq!(header_value("a\nb").to_str().unwrap(), "a%0Ab");
        assert_eq!(
            header_value("ключ").to_str().unwrap(),
            "%D0%BA%D0%BB%D1%8E%D1%87"
        );
    }
}

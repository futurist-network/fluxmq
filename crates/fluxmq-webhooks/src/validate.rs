//! Validation, defaults and secret redaction of subscriptions.

use std::collections::BTreeMap;

use fluxmq_core::Broker;
use fluxmq_proto::notify::REDACTED;
use fluxmq_proto::webhooks::{
    CreateWebhookRequest, PayloadFormat, UpdateWebhookRequest, WebhookSubscription,
};
use fluxmq_proto::{StartFrom, now_ms};
use reqwest::Url;
use reqwest::header::{HeaderName, HeaderValue};

use crate::WebhookError;
use crate::ssrf::{PrivatePolicy, check_host};

pub const DEFAULT_TIMEOUT_MS: u32 = 10_000;
pub const DEFAULT_MAX_IN_FLIGHT: u32 = 4;
pub const DEFAULT_BATCH_MAX: u32 = 1;
pub const TIMEOUT_RANGE: (u32, u32) = (100, 60_000);
pub const IN_FLIGHT_RANGE: (u32, u32) = (1, 64);
pub const BATCH_RANGE: (u32, u32) = (1, 500);
pub const MAX_LINGER_MS: u32 = 10_000;
/// `pause_after_failures`: 0 = breaker off.
pub const MAX_PAUSE_AFTER: u32 = 1_000;
pub const MAX_HEADERS: usize = 32;
pub const MAX_HEADER_VALUE: usize = 4096;
pub const MAX_URL_LEN: usize = 2048;
pub const MAX_SECRET_LEN: usize = 1024;

/// Header names the broker sets itself (or that would break the request framing).
const RESERVED_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "transfer-encoding",
    "host",
    "connection",
    "te",
    "upgrade",
    "expect",
];

fn invalid(m: impl Into<String>) -> WebhookError {
    WebhookError::Invalid(m.into())
}

/// `[a-z0-9._-]{1,64}`.
pub fn validate_name(name: &str) -> Result<String, WebhookError> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b));
    if !ok {
        return Err(invalid(
            "name must be 1-64 characters of a-z, 0-9, '.', '_' or '-'",
        ));
    }
    Ok(name.to_string())
}

/// Parses an http(s) URL; returns `(host, port)` for the SSRF check.
pub fn validate_url(url: &str) -> Result<(String, u16), WebhookError> {
    if url.len() > MAX_URL_LEN {
        return Err(invalid(format!(
            "url must be at most {MAX_URL_LEN} characters"
        )));
    }
    let u = Url::parse(url).map_err(|e| invalid(format!("invalid url: {e}")))?;
    if u.scheme() != "http" && u.scheme() != "https" {
        return Err(invalid("url must use http or https"));
    }
    let host = u
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| invalid("url has no host"))?;
    let port = u.port_or_known_default().unwrap_or(80);
    Ok((host.to_string(), port))
}

pub fn validate_headers(headers: &BTreeMap<String, String>) -> Result<(), WebhookError> {
    if headers.len() > MAX_HEADERS {
        return Err(invalid(format!(
            "at most {MAX_HEADERS} headers are allowed"
        )));
    }
    for (k, v) in headers {
        let name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|_| invalid(format!("invalid header name '{k}'")))?;
        let lower = name.as_str();
        if RESERVED_HEADERS.contains(&lower) || lower.starts_with("x-fluxmq-") {
            return Err(invalid(format!("header '{k}' is reserved")));
        }
        if v.len() > MAX_HEADER_VALUE || HeaderValue::from_str(v).is_err() {
            return Err(invalid(format!("invalid value for header '{k}'")));
        }
    }
    Ok(())
}

/// Ranges, raw ⇒ no batching, request timeout (+ linger) below the channel's ack timeout,
/// filter conditions, header syntax. Channel must exist.
pub fn validate_static(broker: &Broker, s: &WebhookSubscription) -> Result<(), WebhookError> {
    let range = |what: &str, v: u32, (lo, hi): (u32, u32)| {
        if v < lo || v > hi {
            Err(invalid(format!("{what} must be {lo}..{hi}")))
        } else {
            Ok(())
        }
    };
    range("timeout_ms", s.timeout_ms, TIMEOUT_RANGE)?;
    range("max_in_flight", s.max_in_flight, IN_FLIGHT_RANGE)?;
    range("batch_max", s.batch_max, BATCH_RANGE)?;
    range("batch_linger_ms", s.batch_linger_ms, (0, MAX_LINGER_MS))?;
    range(
        "pause_after_failures",
        s.pause_after_failures,
        (0, MAX_PAUSE_AFTER),
    )?;
    if s.format == PayloadFormat::Raw && s.batch_max != 1 {
        return Err(invalid(
            "format 'raw' cannot be combined with batching (batch_max must be 1)",
        ));
    }
    if s.secret.as_ref().is_some_and(|x| x.len() > MAX_SECRET_LEN) {
        return Err(invalid(format!(
            "secret must be at most {MAX_SECRET_LEN} characters"
        )));
    }
    validate_headers(&s.headers)?;
    validate_url(&s.url)?;
    let ch = broker
        .channel(&s.channel)
        .ok_or_else(|| WebhookError::ChannelNotFound(s.channel.clone()))?;
    let budget = u64::from(s.timeout_ms) + u64::from(s.batch_linger_ms);
    if budget >= ch.retry.ack_timeout_ms {
        return Err(invalid(format!(
            "timeout_ms + batch_linger_ms ({budget}) must stay below the channel's ack timeout \
             ({} ms)",
            ch.retry.ack_timeout_ms
        )));
    }
    Ok(())
}

/// Full validation incl. the SSRF check (resolves the host).
pub async fn validate(
    broker: &Broker,
    policy: &PrivatePolicy,
    s: &WebhookSubscription,
) -> Result<(), WebhookError> {
    validate_static(broker, s)?;
    let (host, port) = validate_url(&s.url)?;
    check_host(&host, port, policy)
        .await
        .map_err(WebhookError::Invalid)
}

/// Builds the subscription a create request describes (id empty, not yet validated).
pub fn from_create(req: CreateWebhookRequest) -> Result<WebhookSubscription, WebhookError> {
    let filter = fluxmq_router::validate_conditions(req.filter).map_err(|e| match e {
        fluxmq_router::RouterError::Invalid(m) => invalid(format!("filter: {m}")),
        e => invalid(e.to_string()),
    })?;
    let now = now_ms();
    Ok(WebhookSubscription {
        id: String::new(),
        name: validate_name(req.name.trim())?,
        channel: req.channel,
        url: req.url.trim().to_string(),
        enabled: req.enabled,
        headers: req.headers,
        secret: req.secret.filter(|s| !s.is_empty()),
        filter,
        format: req.format,
        timeout_ms: req.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
        max_in_flight: req.max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT),
        batch_max: req.batch_max.unwrap_or(DEFAULT_BATCH_MAX),
        batch_linger_ms: req.batch_linger_ms.unwrap_or(0),
        retry_on_4xx: req.retry_on_4xx.unwrap_or(false),
        pause_after_failures: req.pause_after_failures.unwrap_or(0),
        start: req.start.unwrap_or(StartFrom::Latest),
        created_at_ms: now,
        updated_at_ms: now,
    })
}

/// Applies an update (secret rule: `REDACTED` keeps, `""` removes, other values replace —
/// per header value too; a header map replaces the whole set).
pub fn apply_update(
    mut s: WebhookSubscription,
    req: UpdateWebhookRequest,
) -> Result<WebhookSubscription, WebhookError> {
    if let Some(url) = req.url {
        s.url = url.trim().to_string();
    }
    if let Some(e) = req.enabled {
        s.enabled = e;
    }
    if let Some(headers) = req.headers {
        let mut out = BTreeMap::new();
        for (k, v) in headers {
            if v == REDACTED {
                let kept = s
                    .headers
                    .iter()
                    .find(|(sk, _)| sk.eq_ignore_ascii_case(&k))
                    .map(|(_, sv)| sv.clone())
                    .ok_or_else(|| invalid(format!("header '{k}' has no stored value to keep")))?;
                out.insert(k, kept);
            } else if !v.is_empty() {
                out.insert(k, v);
            }
        }
        s.headers = out;
    }
    match req.secret.as_deref() {
        None => {}
        Some(REDACTED) => {}
        Some("") => s.secret = None,
        Some(v) => s.secret = Some(v.to_string()),
    }
    if let Some(f) = req.filter {
        s.filter = fluxmq_router::validate_conditions(f).map_err(|e| match e {
            fluxmq_router::RouterError::Invalid(m) => invalid(format!("filter: {m}")),
            e => invalid(e.to_string()),
        })?;
    }
    if let Some(f) = req.format {
        s.format = f;
    }
    if let Some(v) = req.timeout_ms {
        s.timeout_ms = v;
    }
    if let Some(v) = req.max_in_flight {
        s.max_in_flight = v;
    }
    if let Some(v) = req.batch_max {
        s.batch_max = v;
    }
    if let Some(v) = req.batch_linger_ms {
        s.batch_linger_ms = v;
    }
    if let Some(v) = req.retry_on_4xx {
        s.retry_on_4xx = v;
    }
    if let Some(v) = req.pause_after_failures {
        s.pause_after_failures = v;
    }
    s.updated_at_ms = now_ms().max(s.created_at_ms);
    Ok(s)
}

/// The API view: secret and every header value replaced by `REDACTED`.
pub fn redact(mut s: WebhookSubscription) -> WebhookSubscription {
    if s.secret.is_some() {
        s.secret = Some(REDACTED.to_string());
    }
    for v in s.headers.values_mut() {
        *v = REDACTED.to_string();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub() -> WebhookSubscription {
        from_create(CreateWebhookRequest {
            name: "orders-hook".into(),
            channel: "orders".into(),
            url: "https://example.com/hook".into(),
            enabled: true,
            headers: BTreeMap::from([("Authorization".into(), "Bearer x".into())]),
            secret: Some("s".into()),
            filter: vec![],
            format: PayloadFormat::Envelope,
            timeout_ms: None,
            max_in_flight: None,
            batch_max: None,
            batch_linger_ms: None,
            retry_on_4xx: None,
            pause_after_failures: None,
            start: None,
        })
        .unwrap()
    }

    #[test]
    fn names_urls_headers() {
        assert!(validate_name("a.b_c-1").is_ok());
        for bad in ["", "Upper", "sp ace", "$x", &"x".repeat(65)] {
            assert!(validate_name(bad).is_err(), "{bad}");
        }
        assert_eq!(
            validate_url("https://h:8443/x").unwrap(),
            ("h".into(), 8443)
        );
        assert_eq!(validate_url("http://h/x").unwrap().1, 80);
        assert!(validate_url("ftp://h/x").is_err());
        assert!(validate_url("not a url").is_err());
        let h = |k: &str| BTreeMap::from([(k.to_string(), "v".to_string())]);
        assert!(validate_headers(&h("X-Api-Key")).is_ok());
        assert!(validate_headers(&h("Content-Type")).is_err());
        assert!(validate_headers(&h("X-FluxMQ-Attempt")).is_err());
        assert!(validate_headers(&h("bad name")).is_err());
    }

    #[test]
    fn redaction_and_secret_rule() {
        let s = sub();
        assert_eq!(s.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(s.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        let r = redact(s.clone());
        assert_eq!(r.secret.as_deref(), Some(REDACTED));
        assert_eq!(r.headers["Authorization"], REDACTED);

        // REDACTED keeps, "" removes, other replaces.
        let kept = apply_update(
            s.clone(),
            UpdateWebhookRequest {
                secret: Some(REDACTED.into()),
                headers: Some(BTreeMap::from([
                    ("Authorization".into(), REDACTED.into()),
                    ("X-New".into(), "n".into()),
                    ("X-Gone".into(), "".into()),
                ])),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(kept.secret.as_deref(), Some("s"));
        assert_eq!(kept.headers["Authorization"], "Bearer x");
        assert_eq!(kept.headers["X-New"], "n");
        assert!(!kept.headers.contains_key("X-Gone"));
        let removed = apply_update(
            s.clone(),
            UpdateWebhookRequest {
                secret: Some(String::new()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(removed.secret, None);
        assert_eq!(removed.headers["Authorization"], "Bearer x");
        assert!(
            apply_update(
                s,
                UpdateWebhookRequest {
                    headers: Some(BTreeMap::from([("X-Unknown".into(), REDACTED.into())])),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
}

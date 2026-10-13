//! Generic webhook: `POST <url>` with the JSON [`NotificationPayload`] and, when a secret is
//! configured, `X-FluxMQ-Signature: t=<unix seconds>,v1=<hex hmac-sha256(secret, "<t>.<body>")>`.

use std::collections::BTreeMap;

use fluxmq_proto::notify::NotificationPayload;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{HttpResponse, SendError, execute};

pub const SIGNATURE_HEADER: &str = "X-FluxMQ-Signature";

/// `t=<ts>,v1=<hex>` for `body` signed at `ts` (unix seconds).
pub fn sign(secret: &str, ts: u64, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(ts.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("t={ts},v1={}", hex::encode(mac.finalize().into_bytes()))
}

/// Receiver-side check (used by tests and as the reference for docs): parses the header,
/// verifies the HMAC in constant time and that `t` is within `tolerance_secs` of `now_secs`.
pub fn verify_signature(
    secret: &str,
    header: &str,
    body: &[u8],
    now_secs: u64,
    tolerance_secs: u64,
) -> bool {
    let mut ts = None;
    let mut sig = None;
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", v)) => ts = v.parse::<u64>().ok(),
            Some(("v1", v)) => sig = hex::decode(v).ok(),
            _ => {}
        }
    }
    let (Some(ts), Some(sig)) = (ts, sig) else {
        return false;
    };
    if ts.abs_diff(now_secs) > tolerance_secs {
        return false;
    }
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(ts.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.verify_slice(&sig).is_ok()
}

pub(crate) async fn send(
    http: &reqwest::Client,
    url: &str,
    secret: Option<&str>,
    headers: &BTreeMap<String, String>,
    payload: &NotificationPayload,
) -> Result<HttpResponse, SendError> {
    let body = serde_json::to_vec(payload)
        .map_err(|e| SendError::permanent(format!("serialize payload: {e}")))?;
    let mut req = http
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if let Some(secret) = secret.filter(|s| !s.is_empty()) {
        let ts = fluxmq_proto::now_ms() / 1000;
        req = req.header(SIGNATURE_HEADER, sign(secret, ts, &body));
    }
    execute(req.body(body)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify() {
        let body = br#"{"id":"x"}"#;
        let h = sign("s3cret", 1_700_000_000, body);
        assert!(h.starts_with("t=1700000000,v1="));
        assert!(verify_signature("s3cret", &h, body, 1_700_000_010, 300));
        assert!(!verify_signature("other", &h, body, 1_700_000_010, 300));
        assert!(!verify_signature("s3cret", &h, b"{}", 1_700_000_010, 300));
        assert!(!verify_signature("s3cret", &h, body, 1_700_001_000, 300));
        // Known vector: printf '1700000000.{"id":"x"}' | openssl dgst -sha256 -hmac s3cret
        assert_eq!(
            h,
            "t=1700000000,v1=2382c1d0968a5e57238c68de957f44d87e83d3049a5588ec251442ca372b8aec"
        );
    }
}

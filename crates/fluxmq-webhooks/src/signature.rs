//! `X-FluxMQ-Signature: t=<unix_ts>,v1=<hex hmac-sha256(secret, "<t>.<body>")>` — signing and a
//! receiver-side verifier (constant-time comparison, timestamp tolerance against replays).

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Default accepted clock skew / replay window for [`verify`].
pub const DEFAULT_TOLERANCE_SECS: u64 = 300;

/// Header value for `body` signed at `timestamp_secs`.
pub fn sign(secret: &str, timestamp_secs: u64, body: &[u8]) -> String {
    format!(
        "t={timestamp_secs},v1={}",
        hex::encode(mac(secret, timestamp_secs, body))
    )
}

fn mac(secret: &str, t: u64, body: &[u8]) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    m.update(t.to_string().as_bytes());
    m.update(b".");
    m.update(body);
    m.finalize().into_bytes().to_vec()
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignatureError {
    #[error("malformed signature header")]
    Malformed,
    #[error("timestamp outside the tolerance window")]
    Expired,
    #[error("signature mismatch")]
    Mismatch,
}

/// Verifies a received `X-FluxMQ-Signature` header over the raw request body. Accepts any of
/// several `v1=` entries (future secret rotation). `now_secs` is the receiver's clock.
pub fn verify(
    secret: &str,
    header: &str,
    body: &[u8],
    tolerance_secs: u64,
    now_secs: u64,
) -> Result<(), SignatureError> {
    let mut t: Option<u64> = None;
    let mut sigs: Vec<Vec<u8>> = Vec::new();
    for part in header.split(',') {
        let (k, v) = part
            .trim()
            .split_once('=')
            .ok_or(SignatureError::Malformed)?;
        match k {
            "t" => t = Some(v.parse().map_err(|_| SignatureError::Malformed)?),
            "v1" => sigs.push(hex::decode(v).map_err(|_| SignatureError::Malformed)?),
            _ => {}
        }
    }
    let t = t.ok_or(SignatureError::Malformed)?;
    if sigs.is_empty() {
        return Err(SignatureError::Malformed);
    }
    if now_secs.abs_diff(t) > tolerance_secs {
        return Err(SignatureError::Expired);
    }
    for s in sigs {
        let mut m =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
        m.update(t.to_string().as_bytes());
        m.update(b".");
        m.update(body);
        if m.verify_slice(&s).is_ok() {
            return Ok(());
        }
    }
    Err(SignatureError::Mismatch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify() {
        let h = sign("s3cret", 1_700_000_000, b"{\"a\":1}");
        assert!(h.starts_with("t=1700000000,v1="));
        assert_eq!(
            verify("s3cret", &h, b"{\"a\":1}", 300, 1_700_000_100),
            Ok(())
        );
        assert_eq!(
            verify("other", &h, b"{\"a\":1}", 300, 1_700_000_100),
            Err(SignatureError::Mismatch)
        );
        assert_eq!(
            verify("s3cret", &h, b"{\"a\":2}", 300, 1_700_000_100),
            Err(SignatureError::Mismatch)
        );
        assert_eq!(
            verify("s3cret", &h, b"{\"a\":1}", 300, 1_700_001_000),
            Err(SignatureError::Expired)
        );
        assert_eq!(
            verify("s3cret", "garbage", b"", 300, 0),
            Err(SignatureError::Malformed)
        );
        assert_eq!(
            verify("s3cret", "t=1", b"", 300, 1),
            Err(SignatureError::Malformed)
        );
    }

    /// Known-answer test: python3 -c 'import hmac,hashlib;
    /// print(hmac.new(b"key", b"1.body", hashlib.sha256).hexdigest())'
    #[test]
    fn known_answer() {
        assert_eq!(
            sign("key", 1, b"body"),
            "t=1,v1=91b5374b153842ad05b2c4eab9349b8321b14703165bd3fb8b034dfb8be98ae5"
        );
    }
}

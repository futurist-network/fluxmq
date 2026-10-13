//! Payload conversion between raw bytes and [`JsonPayload`], and message → view mapping.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use fluxmq_proto::Message;
use fluxmq_proto::api::{DeadLetterView, JsonPayload, MessageView};
use fluxmq_proto::model::DeadLetter;

use crate::error::ApiErr;

/// UTF-8 text when the bytes are valid UTF-8, otherwise base64.
pub fn encode(bytes: &[u8]) -> JsonPayload {
    match std::str::from_utf8(bytes) {
        Ok(s) => JsonPayload::Utf8(s.to_string()),
        Err(_) => JsonPayload::Base64(STANDARD.encode(bytes)),
    }
}

/// Accepts both encodings.
pub fn decode(payload: JsonPayload) -> Result<Bytes, ApiErr> {
    match payload {
        JsonPayload::Utf8(s) => Ok(Bytes::from(s)),
        JsonPayload::Base64(s) => STANDARD
            .decode(s.as_bytes())
            .map(Bytes::from)
            .map_err(|e| ApiErr::bad_request(format!("invalid base64 payload: {e}"))),
    }
}

pub fn message_view(m: Message) -> MessageView {
    MessageView {
        partition: m.partition,
        offset: m.offset,
        key: m.key,
        headers: m.headers,
        timestamp_ms: m.timestamp_ms,
        size: u32::try_from(m.payload.len()).unwrap_or(u32::MAX),
        payload: encode(&m.payload),
    }
}

pub fn dead_letter_view(d: DeadLetter) -> DeadLetterView {
    DeadLetterView {
        id: d.id,
        channel: d.channel,
        group: d.group,
        reason: d.reason,
        dead_at_ms: d.dead_at_ms,
        message: message_view(d.message),
        attempts: d.attempts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_roundtrip() {
        let p = encode(b"{\"a\":1}");
        assert_eq!(p, JsonPayload::Utf8("{\"a\":1}".into()));
        assert_eq!(decode(p).unwrap(), Bytes::from_static(b"{\"a\":1}"));
    }

    #[test]
    fn binary_roundtrip() {
        let raw = [0xff_u8, 0x00, 0xfe];
        let p = encode(&raw);
        assert!(matches!(p, JsonPayload::Base64(_)));
        assert_eq!(&decode(p).unwrap()[..], &raw);
    }

    #[test]
    fn invalid_base64_rejected() {
        let err = decode(JsonPayload::Base64("!!!".into())).unwrap_err();
        assert_eq!(err.body.error, "bad_request");
    }

    #[test]
    fn json_shape() {
        let v = serde_json::to_value(encode(b"hi")).unwrap();
        assert_eq!(v, serde_json::json!({"encoding": "utf8", "data": "hi"}));
    }
}

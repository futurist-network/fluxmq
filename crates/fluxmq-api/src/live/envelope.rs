//! Message → JSON envelope and event frames (WS text / SSE event).

use std::collections::BTreeMap;

use axum::extract::ws::Utf8Bytes;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use fluxmq_proto::Message;
use serde::Serialize;

#[derive(Serialize)]
struct Envelope<'a> {
    partition: u32,
    offset: u64,
    key: Option<&'a str>,
    headers: &'a BTreeMap<String, String>,
    timestamp: u64,
    encoding: &'static str,
    payload: std::borrow::Cow<'a, str>,
    size: usize,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
}

/// The JSON envelope of `m` (see the design doc §1.2); payloads above `max_payload` bytes are
/// truncated (on a UTF-8 character boundary for text) with `truncated: true`.
pub fn envelope_json(m: &Message, max_payload: usize) -> String {
    let full = &m.payload[..];
    let truncated = full.len() > max_payload;
    let bytes = if truncated {
        &full[..max_payload]
    } else {
        full
    };
    let (encoding, payload) = match std::str::from_utf8(bytes) {
        Ok(s) => ("utf8", std::borrow::Cow::Borrowed(s)),
        // A cut multi-byte character: keep the valid prefix as text when the whole payload is
        // text (the cut is ours, not the producer's).
        Err(e) if truncated && e.error_len().is_none() && std::str::from_utf8(full).is_ok() => {
            let valid = &bytes[..e.valid_up_to()];
            (
                "utf8",
                std::borrow::Cow::Borrowed(std::str::from_utf8(valid).unwrap_or_default()),
            )
        }
        Err(_) => ("base64", std::borrow::Cow::Owned(STANDARD.encode(bytes))),
    };
    let env = Envelope {
        partition: m.partition,
        offset: m.offset,
        key: m.key.as_deref(),
        headers: &m.headers,
        timestamp: m.timestamp_ms,
        encoding,
        payload,
        size: full.len(),
        truncated,
    };
    serde_json::to_string(&env).unwrap_or_else(|_| "{}".into())
}

/// Transport of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Ws,
    Sse,
}

impl Transport {
    pub fn label(self) -> &'static str {
        match self {
            Transport::Ws => "ws",
            Transport::Sse => "sse",
        }
    }
}

/// An outbound frame, encoded for its connection's transport.
#[derive(Debug, Clone)]
pub enum FrameData {
    Ws(Utf8Bytes),
    Sse(Bytes),
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub data: FrameData,
    /// Messages carried (metrics).
    pub msgs: u32,
}

impl Frame {
    pub fn len(&self) -> usize {
        match &self.data {
            FrameData::Ws(t) => t.len(),
            FrameData::Sse(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Builds an event frame from its JSON. SSE: `id: <cursor>` (when given), `event: <ty>`,
/// `data: <json>` (the JSON never contains a raw newline: serde escapes them).
pub fn frame(
    transport: Transport,
    ty: &str,
    cursor: Option<&str>,
    json: String,
    msgs: u32,
) -> Frame {
    let data = match transport {
        Transport::Ws => FrameData::Ws(Utf8Bytes::from(json)),
        Transport::Sse => FrameData::Sse(Bytes::from(sse_event(ty, cursor, &json))),
    };
    Frame { data, msgs }
}

pub fn sse_event(ty: &str, cursor: Option<&str>, json: &str) -> String {
    let mut s = String::with_capacity(json.len() + ty.len() + 64);
    if let Some(c) = cursor {
        s.push_str("id: ");
        s.push_str(c);
        s.push('\n');
    }
    s.push_str("event: ");
    s.push_str(ty);
    s.push_str("\ndata: ");
    s.push_str(json);
    s.push_str("\n\n");
    s
}

/// `{"type":"messages","sub":<sub>,"cursor":<cursor>,"messages":[<envelopes>]}`.
pub fn messages_json<'a>(
    sub: &str,
    cursor: &str,
    envelopes: impl Iterator<Item = &'a str>,
    size_hint: usize,
) -> String {
    let mut s = String::with_capacity(size_hint + sub.len() + cursor.len() + 64);
    s.push_str(r#"{"type":"messages","sub":"#);
    s.push_str(&serde_json::to_string(sub).unwrap_or_else(|_| "\"\"".into()));
    s.push_str(r#","cursor":""#);
    s.push_str(cursor); // base64url + "c1.": no escaping needed
    s.push_str(r#"","messages":["#);
    for (i, e) in envelopes.enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(e);
    }
    s.push_str("]}");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(payload: &[u8]) -> Message {
        Message {
            partition: 1,
            offset: 7,
            key: Some("k".into()),
            headers: [("h".to_string(), "v".to_string())].into_iter().collect(),
            timestamp_ms: 99,
            payload: Bytes::copy_from_slice(payload),
        }
    }

    #[test]
    fn envelope_shapes() {
        let v: serde_json::Value =
            serde_json::from_str(&envelope_json(&m(b"{\"a\":1}"), 100)).unwrap();
        assert_eq!(v["encoding"], "utf8");
        assert_eq!(v["payload"], "{\"a\":1}");
        assert_eq!(v["size"], 7);
        assert_eq!(v["key"], "k");
        assert_eq!(v["headers"]["h"], "v");
        assert_eq!(v["timestamp"], 99);
        assert!(v.get("truncated").is_none());

        let v: serde_json::Value =
            serde_json::from_str(&envelope_json(&m(&[0xff, 0, 1]), 100)).unwrap();
        assert_eq!(v["encoding"], "base64");
        assert_eq!(v["payload"], STANDARD.encode([0xff, 0, 1]));

        // Truncated on a character boundary: "é" is 2 bytes.
        let v: serde_json::Value =
            serde_json::from_str(&envelope_json(&m("aé".as_bytes()), 2)).unwrap();
        assert_eq!(v["encoding"], "utf8");
        assert_eq!(v["payload"], "a");
        assert_eq!(v["truncated"], true);
        assert_eq!(v["size"], 3);
    }

    #[test]
    fn frames() {
        let j = messages_json("0", "c1.x", ["{}", "{}"].into_iter(), 4);
        let v: serde_json::Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["messages"].as_array().unwrap().len(), 2);
        assert_eq!(
            sse_event("gap", Some("c1.a"), "{}"),
            "id: c1.a\nevent: gap\ndata: {}\n\n"
        );
    }
}

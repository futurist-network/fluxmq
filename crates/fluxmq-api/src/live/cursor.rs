//! Resume cursors: `c1.` + base64url(no pad) of `crc32(channel)` (4 bytes LE) followed by
//! `(varint partition, varint next offset)` pairs. Opaque for clients (see the design doc §2).

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

const PREFIX: &str = "c1.";
/// Upper bound on encoded partitions (guards decoding of hostile input).
const MAX_PARTS: usize = 4096;

/// Position of a subscription: next offset per partition (sorted by partition).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cursor {
    pub positions: Vec<(u32, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    #[error("malformed cursor")]
    Malformed,
    #[error("cursor belongs to another channel")]
    WrongChannel,
}

pub fn channel_crc(channel: &str) -> u32 {
    crc32fast::hash(channel.as_bytes())
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let b = *buf.get(*pos)?;
        *pos += 1;
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

/// Encodes `positions` (any order; written sorted) for the channel with CRC `crc`.
pub fn encode(crc: u32, positions: impl IntoIterator<Item = (u32, u64)>) -> String {
    let mut pos: Vec<(u32, u64)> = positions.into_iter().collect();
    pos.sort_unstable_by_key(|&(p, _)| p);
    let mut buf = Vec::with_capacity(4 + pos.len() * 4);
    buf.extend_from_slice(&crc.to_le_bytes());
    for (p, o) in pos {
        put_varint(&mut buf, u64::from(p));
        put_varint(&mut buf, o);
    }
    let mut s = String::with_capacity(PREFIX.len() + buf.len() * 4 / 3 + 4);
    s.push_str(PREFIX);
    URL_SAFE_NO_PAD.encode_string(&buf, &mut s);
    s
}

/// Decodes a cursor of `channel`.
pub fn decode(channel: &str, s: &str) -> Result<Cursor, CursorError> {
    let body = s
        .trim()
        .strip_prefix(PREFIX)
        .ok_or(CursorError::Malformed)?;
    if body.len() > 64 * 1024 {
        return Err(CursorError::Malformed);
    }
    let buf = URL_SAFE_NO_PAD
        .decode(body.as_bytes())
        .map_err(|_| CursorError::Malformed)?;
    if buf.len() < 4 {
        return Err(CursorError::Malformed);
    }
    let crc = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if crc != channel_crc(channel) {
        return Err(CursorError::WrongChannel);
    }
    let mut pos = 4;
    let mut positions: Vec<(u32, u64)> = Vec::new();
    while pos < buf.len() {
        let p = get_varint(&buf, &mut pos).ok_or(CursorError::Malformed)?;
        let o = get_varint(&buf, &mut pos).ok_or(CursorError::Malformed)?;
        let p = u32::try_from(p).map_err(|_| CursorError::Malformed)?;
        if positions.last().is_some_and(|&(q, _)| q >= p) || positions.len() >= MAX_PARTS {
            return Err(CursorError::Malformed);
        }
        positions.push((p, o));
    }
    Ok(Cursor { positions })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_channel_binding() {
        let crc = channel_crc("orders");
        let c = encode(crc, [(2, 1u64 << 40), (0, 0), (1, 127), (3, 128)]);
        assert!(c.starts_with("c1."));
        assert!(
            c.chars()
                .all(|ch| ch.is_ascii_alphanumeric() || "._-".contains(ch))
        );
        let d = decode("orders", &c).unwrap();
        assert_eq!(d.positions, vec![(0, 0), (1, 127), (2, 1 << 40), (3, 128)]);
        assert_eq!(decode("other", &c), Err(CursorError::WrongChannel));
        assert_eq!(decode("orders", "x"), Err(CursorError::Malformed));
        assert_eq!(decode("orders", "c1.!!"), Err(CursorError::Malformed));
        // Truncated varint.
        let mut raw = crc.to_le_bytes().to_vec();
        raw.push(0x80);
        let bad = format!("c1.{}", URL_SAFE_NO_PAD.encode(raw));
        assert_eq!(decode("orders", &bad), Err(CursorError::Malformed));
        // Empty position list is valid (no partitions).
        assert_eq!(
            decode("orders", &encode(crc, [])).unwrap(),
            Cursor::default()
        );
    }
}

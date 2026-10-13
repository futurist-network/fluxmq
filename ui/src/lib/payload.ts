/** Payload decoding for the message inspector: JSON pretty-print, text, hex dump. */
import type { JsonPayload } from '../api/types';

export interface DecodedPayload {
  bytes: Uint8Array;
  /** UTF-8 text when the payload is valid text. */
  text: string | null;
  /** Parsed JSON when the text is JSON. */
  json: unknown;
  isJson: boolean;
}

const utf8 = new TextEncoder();
const strictDecoder = new TextDecoder('utf-8', { fatal: true });

export function base64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export function bytesToBase64(bytes: Uint8Array): string {
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(s);
}

function tryJson(text: string): { ok: boolean; value: unknown } {
  const t = text.trimStart();
  if (!t || !'{["-0123456789tfn'.includes(t[0]!)) return { ok: false, value: undefined };
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch {
    return { ok: false, value: undefined };
  }
}

export function decodePayload(p: JsonPayload | string): DecodedPayload {
  let bytes: Uint8Array;
  let text: string | null;
  if (typeof p === 'string') {
    text = p;
    bytes = utf8.encode(p);
  } else if (p.encoding === 'utf8') {
    text = p.data;
    bytes = utf8.encode(p.data);
  } else {
    try {
      bytes = base64ToBytes(p.data);
    } catch {
      bytes = new Uint8Array();
    }
    try {
      text = strictDecoder.decode(bytes);
      // Mostly-control-character text is binary in practice.
      // eslint-disable-next-line no-control-regex
      const ctrl = text.match(/[\u0000-\u0008\u000e-\u001f]/g)?.length ?? 0;
      if (ctrl > text.length * 0.05) text = null;
    } catch {
      text = null;
    }
  }
  const j = text != null ? tryJson(text) : { ok: false, value: undefined };
  return { bytes, text, json: j.value, isJson: j.ok && typeof j.value === 'object' && j.value !== null };
}

/** Classic hex dump: `00000000  7b 22 6f 72  ...  |{"or...|`. */
export function hexDump(bytes: Uint8Array, maxBytes = 64 * 1024): string {
  const lines: string[] = [];
  const n = Math.min(bytes.length, maxBytes);
  for (let off = 0; off < n; off += 16) {
    const row = bytes.subarray(off, Math.min(off + 16, n));
    let hex = '';
    let ascii = '';
    for (let i = 0; i < 16; i++) {
      if (i < row.length) {
        const b = row[i]!;
        hex += b.toString(16).padStart(2, '0') + ' ';
        ascii += b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.';
      } else {
        hex += '   ';
      }
      if (i === 7) hex += ' ';
    }
    lines.push(`${off.toString(16).padStart(8, '0')}  ${hex} |${ascii}|`);
  }
  if (bytes.length > n) lines.push(`… ${bytes.length - n} more bytes`);
  return lines.join('\n');
}

export type JsonTokenKind = 'key' | 'string' | 'number' | 'bool' | 'null' | 'punct' | 'ws';
export interface JsonToken {
  kind: JsonTokenKind;
  text: string;
}

const TOKEN_RE = /("(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*")(\s*:)?|\b(true|false)\b|\b(null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|([{}[\],:])|(\s+)/g;

/** Tokenize (pretty-printed) JSON text for syntax highlighting. No HTML is produced. */
export function tokenizeJson(text: string): JsonToken[] {
  const out: JsonToken[] = [];
  let last = 0;
  TOKEN_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = TOKEN_RE.exec(text))) {
    if (m.index > last) out.push({ kind: 'punct', text: text.slice(last, m.index) });
    if (m[1] != null) {
      if (m[2] != null) {
        out.push({ kind: 'key', text: m[1] });
        out.push({ kind: 'punct', text: m[2] });
      } else out.push({ kind: 'string', text: m[1] });
    } else if (m[3] != null) out.push({ kind: 'bool', text: m[3] });
    else if (m[4] != null) out.push({ kind: 'null', text: m[4] });
    else if (m[5] != null) out.push({ kind: 'number', text: m[5] });
    else if (m[6] != null) out.push({ kind: 'punct', text: m[6] });
    else out.push({ kind: 'ws', text: m[0] });
    last = TOKEN_RE.lastIndex;
  }
  if (last < text.length) out.push({ kind: 'punct', text: text.slice(last) });
  return out;
}

/**
 * What a payload is (JSON, XML, HTML, form data, CSV, plain text, binary) and how to show it:
 * pretty-printing and syntax tokens for XML/HTML, parsed form fields and CSV rows. A
 * `content-type` header wins over sniffing when it names a known format. Pure, no DOM.
 */
import type { JsonPayload } from '../api/types';
import { decodePayload, type DecodedPayload } from './payload';

export type PayloadFormat = 'json' | 'xml' | 'html' | 'form' | 'csv' | 'text' | 'binary';

/** Short label for chips and the inspector (technical names, the same in every language). */
export const FORMAT_LABEL: Record<Exclude<PayloadFormat, 'text' | 'binary'>, string> = {
  json: 'JSON',
  xml: 'XML',
  html: 'HTML',
  form: 'Form',
  csv: 'CSV',
};

/** The `content-type` header of a message, whatever its case. */
export function contentTypeOf(headers: Record<string, string> | undefined): string | undefined {
  if (!headers) return undefined;
  for (const [k, v] of Object.entries(headers)) if (k.toLowerCase() === 'content-type') return v;
  return undefined;
}

function fromContentType(ct: string | undefined): PayloadFormat | null {
  if (!ct) return null;
  const c = ct.toLowerCase().split(';')[0]!.trim();
  if (c === 'application/json' || c.endsWith('+json')) return 'json';
  if (c === 'text/html') return 'html';
  if (c === 'application/xml' || c === 'text/xml' || c.endsWith('+xml')) return 'xml';
  if (c === 'application/x-www-form-urlencoded') return 'form';
  if (c === 'text/csv') return 'csv';
  if (c.startsWith('text/')) return 'text';
  return null;
}

const FORM_RE = /^[\w.\-~%[\]]+=[^&\s]*(?:&[\w.\-~%[\]]+=[^&\s]*)*$/;

function looksLikeCsv(text: string): boolean {
  const lines = text.trim().split(/\r?\n/).slice(0, 20);
  if (lines.length < 2) return false;
  for (const sep of [',', ';', '\t']) {
    const counts = lines.map((l) => splitCsvLine(l, sep).length);
    if (counts[0]! >= 2 && counts.every((c) => c === counts[0])) return true;
  }
  return false;
}

/** The format of a decoded payload. */
export function detectFormat(d: DecodedPayload, contentType?: string): PayloadFormat {
  if (d.text == null) return 'binary';
  const byHeader = fromContentType(contentType);
  if (byHeader === 'json' && d.isJson) return 'json';
  if (byHeader && byHeader !== 'json') return byHeader;
  if (d.isJson) return 'json';
  const t = d.text.trim();
  if (t.startsWith('<') && t.endsWith('>')) {
    if (/^<!doctype html|^<html[\s>]/i.test(t)) return 'html';
    if (/^<\?xml\s/.test(t) || /<\/[\w:.-]+\s*>$|\/>$/.test(t)) return 'xml';
  }
  if (!t.includes('\n') && t.length <= 8192 && FORM_RE.test(t) && t.includes('&')) return 'form';
  if (looksLikeCsv(t)) return 'csv';
  return 'text';
}

/** Format of a message payload (cheap enough per list row: previews are small). */
export function payloadFormat(payload: JsonPayload | string, headers?: Record<string, string>): PayloadFormat {
  return detectFormat(decodePayload(payload), contentTypeOf(headers));
}

// ───────────── XML / HTML ─────────────

export type XmlTokenKind = 'tag' | 'attr' | 'string' | 'punct' | 'comment' | 'decl' | 'text' | 'ws';
export interface XmlToken {
  kind: XmlTokenKind;
  text: string;
}

const MARKUP_RE = /(<!--[\s\S]*?-->)|(<!\[CDATA\[[\s\S]*?\]\]>)|(<[?!][\s\S]*?>)|(<\/?[\w:.-]+)((?:\s+[\w:.-]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'>]+))?)*)\s*(\/?>)|([^<]+)/g;
const ATTR_RE = /(\s+)([\w:.-]+)(?:(\s*=\s*)("[^"]*"|'[^']*'|[^\s"'>]+))?/g;

/** Syntax tokens of XML/HTML text (no HTML is produced; React renders the text). */
export function tokenizeXml(text: string): XmlToken[] {
  const out: XmlToken[] = [];
  MARKUP_RE.lastIndex = 0;
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = MARKUP_RE.exec(text))) {
    if (m.index > last) out.push({ kind: 'text', text: text.slice(last, m.index) });
    if (m[1] != null) out.push({ kind: 'comment', text: m[1] });
    else if (m[2] != null) out.push({ kind: 'text', text: m[2] });
    else if (m[3] != null) out.push({ kind: 'decl', text: m[3] });
    else if (m[4] != null) {
      out.push({ kind: 'tag', text: m[4] });
      const attrs = m[5] ?? '';
      ATTR_RE.lastIndex = 0;
      let a: RegExpExecArray | null;
      while ((a = ATTR_RE.exec(attrs))) {
        out.push({ kind: 'ws', text: a[1]! });
        out.push({ kind: 'attr', text: a[2]! });
        if (a[3] != null) {
          out.push({ kind: 'punct', text: a[3] });
          out.push({ kind: 'string', text: a[4]! });
        }
      }
      // Whitespace before `>` / `/>` that the attribute pattern did not consume.
      const consumed = m[4].length + attrs.length;
      const gap = m[0].length - consumed - m[6]!.length;
      if (gap > 0) out.push({ kind: 'ws', text: m[0].slice(consumed, consumed + gap) });
      out.push({ kind: 'tag', text: m[6]! });
    } else out.push({ kind: /^\s+$/.test(m[7]!) ? 'ws' : 'text', text: m[7]! });
    last = MARKUP_RE.lastIndex;
  }
  if (last < text.length) out.push({ kind: 'text', text: text.slice(last) });
  return out;
}

const VOID_HTML = new Set(['area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'source', 'track', 'wbr']);

/**
 * Indents markup one element per line (text-only elements stay on one line). Returns the input
 * unchanged when it is already multi-line or does not tokenize cleanly.
 */
export function prettyXml(text: string, html = false): string {
  const src = text.trim();
  if (src.split('\n').length > 3) return text;
  const toks = tokenizeXml(src);
  // Rebuild elements: [kind, raw] where kind is open/close/self/other/text.
  type Node = { kind: 'open' | 'close' | 'self' | 'other' | 'text'; raw: string; name?: string };
  const nodes: Node[] = [];
  let i = 0;
  while (i < toks.length) {
    const tk = toks[i]!;
    if (tk.kind === 'tag' && tk.text.startsWith('<')) {
      let raw = tk.text;
      let j = i + 1;
      while (j < toks.length && !(toks[j]!.kind === 'tag' && toks[j]!.text.endsWith('>'))) raw += toks[j++]!.text;
      if (j >= toks.length) return text;
      const end = toks[j]!.text;
      raw += end;
      const name = tk.text.replace(/^<\/?/, '');
      const closing = tk.text.startsWith('</');
      const self = end === '/>' || (html && !closing && VOID_HTML.has(name.toLowerCase()));
      nodes.push({ kind: closing ? 'close' : self ? 'self' : 'open', raw, name });
      i = j + 1;
    } else if (tk.kind === 'ws') i++;
    else {
      nodes.push({ kind: tk.kind === 'text' ? 'text' : 'other', raw: tk.text.trim() });
      i++;
    }
  }
  const lines: string[] = [];
  let depth = 0;
  for (let k = 0; k < nodes.length; k++) {
    const n = nodes[k]!;
    const pad = '  '.repeat(Math.max(0, depth));
    if (n.kind === 'open' && nodes[k + 1]?.kind === 'text' && nodes[k + 2]?.kind === 'close' && nodes[k + 2]!.name === n.name) {
      lines.push(pad + n.raw + nodes[k + 1]!.raw + nodes[k + 2]!.raw);
      k += 2;
    } else if (n.kind === 'open' && nodes[k + 1]?.kind === 'close' && nodes[k + 1]!.name === n.name) {
      lines.push(pad + n.raw + nodes[k + 1]!.raw);
      k += 1;
    } else if (n.kind === 'open') {
      lines.push(pad + n.raw);
      depth++;
    } else if (n.kind === 'close') {
      depth--;
      lines.push('  '.repeat(Math.max(0, depth)) + n.raw);
    } else lines.push(pad + n.raw);
  }
  return depth === 0 ? lines.join('\n') : text;
}

// ───────────── form data / CSV ─────────────

function decodeComponent(s: string): string {
  try {
    return decodeURIComponent(s.replace(/\+/g, ' '));
  } catch {
    return s;
  }
}

/** `a=1&b=x%20y` → [['a','1'],['b','x y']]. */
export function parseForm(text: string): [string, string][] {
  return text
    .trim()
    .split('&')
    .filter(Boolean)
    .map((pair) => {
      const eq = pair.indexOf('=');
      return eq < 0 ? [decodeComponent(pair), ''] : [decodeComponent(pair.slice(0, eq)), decodeComponent(pair.slice(eq + 1))];
    });
}

/** One CSV line, honouring double quotes ("a,b" and "" escapes). */
export function splitCsvLine(line: string, sep: string): string[] {
  const out: string[] = [];
  let cur = '';
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i]!;
    if (q) {
      if (c === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (c === '"') q = false;
      else cur += c;
    } else if (c === '"') q = true;
    else if (c === sep) {
      out.push(cur);
      cur = '';
    } else cur += c;
  }
  out.push(cur);
  return out;
}

/** CSV rows (separator guessed from `,` `;` tab), at most `max` rows. */
export function parseCsv(text: string, max = 200): string[][] {
  const lines = text.trim().split(/\r?\n/);
  const first = lines[0] ?? '';
  const sep = [',', ';', '\t'].sort((a, b) => splitCsvLine(first, b).length - splitCsvLine(first, a).length)[0]!;
  return lines.slice(0, max).map((l) => splitCsvLine(l, sep));
}

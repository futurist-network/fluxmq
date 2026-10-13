/**
 * A deliberately small Markdown dialect for the built-in docs (no dependency, no HTML
 * passthrough — the renderer builds React elements, so content can never inject markup).
 *
 * Blocks: `##`–`####` headings (optional `{#id}`), paragraphs, `-`/`*`/`1.` lists (nested by
 * indentation, items may contain any block), GFM pipe tables, fenced code with a language,
 * `> [!NOTE|TIP|WARNING]` callouts (plain `>` = note), `---`, and `:::diagram <name>`.
 * Inline: `code`, **strong**, *em*, [links](href), <kbd>keys</kbd>, `\` escapes.
 */

export type Inline =
  | { t: 'text'; v: string }
  | { t: 'code'; v: string }
  | { t: 'kbd'; v: string }
  | { t: 'strong'; c: Inline[] }
  | { t: 'em'; c: Inline[] }
  | { t: 'link'; href: string; c: Inline[] };

export type CalloutKind = 'note' | 'tip' | 'warning';

export interface ListItem {
  text: string;
  children: Block[];
}

export type Block =
  | { t: 'h'; level: 2 | 3 | 4; text: string; id: string }
  | { t: 'p'; text: string }
  | { t: 'code'; lang: string; code: string }
  | { t: 'list'; ordered: boolean; start: number; items: ListItem[] }
  | { t: 'table'; head: string[]; rows: string[][] }
  | { t: 'callout'; kind: CalloutKind; blocks: Block[] }
  | { t: 'hr' }
  | { t: 'diagram'; name: string };

// ── Inline ────────────────────────────────────────────────────────────────

/** Parses inline markup. Unclosed markers are kept as literal text. */
export function parseInline(src: string): Inline[] {
  const out: Inline[] = [];
  let buf = '';
  const flush = () => {
    if (buf) out.push({ t: 'text', v: buf });
    buf = '';
  };
  let i = 0;
  while (i < src.length) {
    const ch = src[i]!;
    if (ch === '\\' && i + 1 < src.length && /[\\`*_[\]()<>|#{}!-]/.test(src[i + 1]!)) {
      buf += src[i + 1];
      i += 2;
      continue;
    }
    if (ch === '`') {
      // Longest run of backticks opens; the same run closes (lets code contain a backtick).
      let n = 1;
      while (src[i + n] === '`') n++;
      const fence = '`'.repeat(n);
      const end = src.indexOf(fence, i + n);
      if (end > i) {
        flush();
        let v = src.slice(i + n, end);
        if (n > 1 && v.startsWith(' ') && v.endsWith(' ') && v.trim()) v = v.slice(1, -1);
        out.push({ t: 'code', v });
        i = end + n;
        continue;
      }
    }
    if (ch === '<' && src.startsWith('<kbd>', i)) {
      const end = src.indexOf('</kbd>', i + 5);
      if (end > i) {
        flush();
        out.push({ t: 'kbd', v: src.slice(i + 5, end) });
        i = end + 6;
        continue;
      }
    }
    if (ch === '*' && src[i + 1] === '*') {
      const end = findClose(src, '**', i + 2);
      if (end > i + 2) {
        flush();
        out.push({ t: 'strong', c: parseInline(src.slice(i + 2, end)) });
        i = end + 2;
        continue;
      }
    }
    if (ch === '*' && src[i + 1] !== '*' && src[i + 1] !== ' ' && i + 1 < src.length) {
      const end = findClose(src, '*', i + 1);
      if (end > i + 1 && src[end - 1] !== ' ') {
        flush();
        out.push({ t: 'em', c: parseInline(src.slice(i + 1, end)) });
        i = end + 1;
        continue;
      }
    }
    if (ch === '[') {
      const close = matchBracket(src, i);
      if (close > 0 && src[close + 1] === '(') {
        const end = src.indexOf(')', close + 2);
        if (end > close) {
          flush();
          out.push({ t: 'link', href: src.slice(close + 2, end).trim(), c: parseInline(src.slice(i + 1, close)) });
          i = end + 1;
          continue;
        }
      }
    }
    buf += ch;
    i++;
  }
  flush();
  return out;
}

/** Index of the closing `marker` at/after `from`, skipping code spans and escapes; -1 if none. */
function findClose(src: string, marker: string, from: number): number {
  let i = from;
  while (i < src.length) {
    if (src[i] === '\\') {
      i += 2;
      continue;
    }
    if (src[i] === '`') {
      const end = src.indexOf('`', i + 1);
      if (end < 0) return -1;
      i = end + 1;
      continue;
    }
    if (src.startsWith(marker, i)) {
      // `**` inside a `*…*` run must not close the single marker.
      // `***` while closing `**`: the inner `*…*` ends first, `**` takes the last two.
      if (marker === '**' && src[i + 2] === '*') return i + 1;
      if (marker === '*' && src[i + 1] === '*') {
        const strongEnd = findClose(src, '**', i + 2);
        if (strongEnd < 0) return -1;
        i = strongEnd + 2;
        continue;
      }
      return i;
    }
    i++;
  }
  return -1;
}

function matchBracket(src: string, open: number): number {
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    const c = src[i];
    if (c === '\\') {
      i++;
      continue;
    }
    if (c === '`') {
      const end = src.indexOf('`', i + 1);
      if (end < 0) return -1;
      i = end;
      continue;
    }
    if (c === '[') depth++;
    else if (c === ']') {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/** Inline markup → plain text (for search, heading ids and the TOC). */
export function inlineText(nodes: Inline[]): string {
  let s = '';
  for (const n of nodes) {
    if (n.t === 'text' || n.t === 'code' || n.t === 'kbd') s += n.v;
    else s += inlineText(n.c);
  }
  return s;
}

export const stripInline = (src: string): string => inlineText(parseInline(src));

// ── Slugs ─────────────────────────────────────────────────────────────────

/** Heading id: lower-case letters (any script) and digits joined by `-`. */
export function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[^\p{L}\p{N}]+/gu, '-')
    .replace(/^-+|-+$/g, '');
}

// ── Blocks ────────────────────────────────────────────────────────────────

const LIST_RE = /^( *)([-*]|\d+[.)])\s+(.*)$/;
const FENCE_RE = /^ *(`{3,}|~{3,})\s*([\w+-]*)\s*$/;
const HEADING_RE = /^(#{2,4})\s+(.+?)\s*(?:\{#([\w-]+)\})?\s*$/;
const DIAGRAM_RE = /^:::\s*diagram\s+([\w-]+)\s*$/;
const HR_RE = /^ {0,3}(?:-{3,}|\*{3,})\s*$/;

const indentOf = (line: string) => line.length - line.trimStart().length;
const isBlank = (line: string) => line.trim() === '';

function isTableStart(lines: string[], i: number): boolean {
  const a = lines[i];
  const b = lines[i + 1];
  return !!a && !!b && a.trim().startsWith('|') && /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/.test(b);
}

/** Splits a `| a | b |` row, honouring `\|` and pipes inside code spans. */
export function splitRow(line: string): string[] {
  let s = line.trim();
  if (s.startsWith('|')) s = s.slice(1);
  if (s.endsWith('|') && !s.endsWith('\\|')) s = s.slice(0, -1);
  const cells: string[] = [];
  let cur = '';
  let inCode = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    if (c === '\\' && s[i + 1] === '|') {
      cur += '\\|';
      i++;
      continue;
    }
    if (c === '`') inCode = !inCode;
    if (c === '|' && !inCode) {
      cells.push(cur.trim());
      cur = '';
      continue;
    }
    cur += c;
  }
  cells.push(cur.trim());
  // `\|` stays escaped for parseInline, except inside code spans where it must read as `|`.
  return cells.map((cell) => cell.replace(/`[^`]*`/g, (code) => code.replace(/\\\|/g, '|')));
}

function startsBlock(lines: string[], i: number): boolean {
  const l = lines[i]!;
  return HEADING_RE.test(l) || FENCE_RE.test(l) || LIST_RE.test(l) || l.trimStart().startsWith('>') || HR_RE.test(l) || DIAGRAM_RE.test(l.trim()) || isTableStart(lines, i);
}

/** Parses a document body into blocks. Heading ids are unique within the call tree. */
export function parseBlocks(src: string): Block[] {
  const used = new Map<string, number>();
  return parseLines(src.replace(/\r\n?/g, '\n').split('\n'), used);
}

function uniqueId(base: string, used: Map<string, number>): string {
  const b = base || 'section';
  const n = used.get(b) ?? 0;
  used.set(b, n + 1);
  return n === 0 ? b : `${b}-${n + 1}`;
}

function parseLines(lines: string[], used: Map<string, number>): Block[] {
  const blocks: Block[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i]!;
    if (isBlank(line)) {
      i++;
      continue;
    }

    const fence = FENCE_RE.exec(line);
    if (fence) {
      const marker = fence[1]!;
      const ind = indentOf(line);
      const body: string[] = [];
      i++;
      while (i < lines.length && !(lines[i]!.trim().startsWith(marker) && lines[i]!.trim().replace(/[`~]/g, '') === '')) {
        const l = lines[i]!;
        body.push(indentOf(l) >= ind ? l.slice(ind) : l.trimStart());
        i++;
      }
      i++; // closing fence (or EOF)
      blocks.push({ t: 'code', lang: fence[2] ?? '', code: body.join('\n') });
      continue;
    }

    const h = HEADING_RE.exec(line);
    if (h) {
      const level = h[1]!.length as 2 | 3 | 4;
      const text = h[2]!;
      blocks.push({ t: 'h', level, text, id: uniqueId(h[3] ?? slugify(stripInline(text)), used) });
      i++;
      continue;
    }

    const dia = DIAGRAM_RE.exec(line.trim());
    if (dia) {
      blocks.push({ t: 'diagram', name: dia[1]! });
      i++;
      continue;
    }

    if (HR_RE.test(line)) {
      blocks.push({ t: 'hr' });
      i++;
      continue;
    }

    if (line.trimStart().startsWith('>')) {
      const inner: string[] = [];
      while (i < lines.length && lines[i]!.trimStart().startsWith('>')) {
        inner.push(lines[i]!.trimStart().replace(/^> ?/, ''));
        i++;
      }
      let kind: CalloutKind = 'note';
      const m = /^\[!(NOTE|TIP|WARNING|IMPORTANT|CAUTION)\]\s*$/i.exec(inner[0] ?? '');
      if (m) {
        const k = m[1]!.toUpperCase();
        kind = k === 'TIP' ? 'tip' : k === 'NOTE' || k === 'IMPORTANT' ? 'note' : 'warning';
        inner.shift();
      }
      blocks.push({ t: 'callout', kind, blocks: parseLines(inner, used) });
      continue;
    }

    if (isTableStart(lines, i)) {
      const head = splitRow(line);
      i += 2;
      const rows: string[][] = [];
      while (i < lines.length && lines[i]!.trim().startsWith('|')) {
        const cells = splitRow(lines[i]!);
        while (cells.length < head.length) cells.push('');
        rows.push(cells.slice(0, head.length));
        i++;
      }
      blocks.push({ t: 'table', head, rows });
      continue;
    }

    const li = LIST_RE.exec(line);
    if (li) {
      const [list, next] = parseList(lines, i, used);
      blocks.push(list);
      i = next;
      continue;
    }

    // Paragraph: until a blank line or the start of another block.
    const para: string[] = [line.trim()];
    i++;
    while (i < lines.length && !isBlank(lines[i]!) && !startsBlock(lines, i)) {
      para.push(lines[i]!.trim());
      i++;
    }
    blocks.push({ t: 'p', text: para.join(' ') });
  }
  return blocks;
}

function parseList(lines: string[], start: number, used: Map<string, number>): [Block, number] {
  const first = LIST_RE.exec(lines[start]!)!;
  const baseIndent = first[1]!.length;
  const ordered = /\d/.test(first[2]!);
  const items: ListItem[] = [];
  let i = start;
  while (i < lines.length) {
    const m = LIST_RE.exec(lines[i]!);
    if (!m || m[1]!.length !== baseIndent || /\d/.test(m[2]!) !== ordered) break;
    const contentIndent = baseIndent + m[2]!.length + 1;
    const sub: string[] = [];
    let text = m[3]!;
    i++;
    // Lazy continuation lines of the first paragraph.
    while (i < lines.length && !isBlank(lines[i]!) && indentOf(lines[i]!) > baseIndent && !LIST_RE.test(lines[i]!) && !FENCE_RE.test(lines[i]!)) {
      text += ' ' + lines[i]!.trim();
      i++;
    }
    // Nested content: lines indented past the marker (blank lines allowed in between).
    while (i < lines.length) {
      const l = lines[i]!;
      if (isBlank(l)) {
        let j = i + 1;
        while (j < lines.length && isBlank(lines[j]!)) j++;
        if (j < lines.length && indentOf(lines[j]!) >= contentIndent - 1 && indentOf(lines[j]!) > baseIndent) {
          for (; i < j; i++) sub.push('');
          continue;
        }
        break;
      }
      if (indentOf(l) <= baseIndent) break;
      sub.push(l.slice(Math.min(indentOf(l), contentIndent)));
      i++;
    }
    items.push({ text, children: sub.length ? parseLines(sub, used) : [] });
    // A blank line between items keeps the list going.
    if (i < lines.length && isBlank(lines[i]!)) {
      let j = i;
      while (j < lines.length && isBlank(lines[j]!)) j++;
      const n = j < lines.length ? LIST_RE.exec(lines[j]!) : null;
      if (n && n[1]!.length === baseIndent && /\d/.test(n[2]!) === ordered) i = j;
      else break;
    }
  }
  return [{ t: 'list', ordered, start: ordered ? parseInt(first[2]!, 10) : 1, items }, i];
}

// ── Front matter ──────────────────────────────────────────────────────────

/** `---\nkey: value\n---\nbody` → fields + body. Values are plain strings (no YAML). */
export function parseFrontMatter(src: string): { meta: Record<string, string>; body: string } {
  const text = src.replace(/\r\n?/g, '\n');
  if (!text.startsWith('---\n')) return { meta: {}, body: text };
  const end = text.indexOf('\n---', 4);
  if (end < 0) return { meta: {}, body: text };
  const meta: Record<string, string> = {};
  for (const line of text.slice(4, end).split('\n')) {
    const m = /^([\w-]+):\s*(.*)$/.exec(line);
    if (m) meta[m[1]!] = m[2]!.trim().replace(/^(['"])(.*)\1$/, '$2');
  }
  const after = text.indexOf('\n', end + 4);
  return { meta, body: after < 0 ? '' : text.slice(after + 1) };
}

/** Plain text of a block tree (for search). */
export function blocksText(blocks: Block[]): string {
  const parts: string[] = [];
  const walk = (bs: Block[]) => {
    for (const b of bs) {
      switch (b.t) {
        case 'h':
        case 'p':
          parts.push(stripInline(b.text));
          break;
        case 'code':
          parts.push(b.code);
          break;
        case 'list':
          for (const it of b.items) {
            parts.push(stripInline(it.text));
            walk(it.children);
          }
          break;
        case 'table':
          parts.push(b.head.map(stripInline).join(' '));
          for (const r of b.rows) parts.push(r.map(stripInline).join(' '));
          break;
        case 'callout':
          walk(b.blocks);
          break;
        default:
      }
    }
  };
  walk(blocks);
  return parts.join('\n');
}

/**
 * Mock broker's filter DSL — TypeScript port of crates/fluxmq-router/src/filter.rs (lexer,
 * parser, renderer). Serves `POST /api/filter/parse`, `POST /api/filter/render` and message
 * search `filter=` in the mock. The console itself has no DSL code: it always asks the server.
 * Keep in sync with filter.rs: `parse(render(c))` must equal `c`.
 */
import type { Condition, FieldRef, JsonValue, Op } from '../src/api/types';

export class FilterSyntaxError extends Error {
  /** Byte offset into the UTF-8 input (as the Rust parser reports it). */
  readonly position: number;
  readonly detail: string;
  constructor(position: number, detail: string) {
    super(`filter syntax error at ${position}: ${detail}`);
    this.position = position;
    this.detail = detail;
  }
}

export const OP_TEXT: Record<Op, string> = {
  eq: '=',
  ne: '!=',
  gt: '>',
  gte: '>=',
  lt: '<',
  lte: '<=',
  in: 'in',
  not_in: 'not in',
  prefix: 'prefix',
  contains: 'contains',
  exists: 'exists',
  not_exists: 'not exists',
};

// ───────────── render ─────────────

const IDENT_START = /[\p{L}\p{N}_\-.$@/:]/u;
const IDENT_CHAR = /[\p{L}\p{N}_\-.$@/:+]/u;

function isBare(s: string): boolean {
  const chars = [...s];
  if (!chars.length || !IDENT_START.test(chars[0]!)) return false;
  return chars.every((c) => IDENT_CHAR.test(c));
}

function renderString(s: string): string {
  let out = '"';
  for (const c of s) {
    const code = c.codePointAt(0)!;
    if (c === '"') out += '\\"';
    else if (c === '\\') out += '\\\\';
    else if (c === '\n') out += '\\n';
    else if (c === '\r') out += '\\r';
    else if (c === '\t') out += '\\t';
    else if (code < 0x20) out += `\\u${code.toString(16).padStart(4, '0')}`;
    else out += c;
  }
  return out + '"';
}

export function renderField(f: FieldRef): string {
  switch (f.kind) {
    case 'key':
      return 'key';
    case 'partition':
      return 'partition';
    case 'header':
      return `header.${isBare(f.name) ? f.name : renderString(f.name)}`;
    case 'payload': {
      const lower = f.name.toLowerCase();
      const reserved = ['key', 'partition', 'and', 'or', 'header.', 'payload.'].includes(lower) || lower.startsWith('header.') || lower.startsWith('payload.');
      if (isBare(f.name) && !reserved) return f.name;
      return `payload.${isBare(f.name) ? f.name : renderString(f.name)}`;
    }
  }
}

export function renderValue(v: JsonValue | undefined): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : 'null';
  if (typeof v === 'string') return renderString(v);
  if (Array.isArray(v)) return `[${v.map(renderValue).join(', ')}]`;
  return renderString(JSON.stringify(v));
}

export function renderCondition(c: Condition): string {
  let out = `${renderField(c.field)} ${OP_TEXT[c.op]}`;
  if (c.op !== 'exists' && c.op !== 'not_exists') {
    const v = c.value;
    if ((c.op === 'in' || c.op === 'not_in') && !Array.isArray(v)) out += ` [${renderValue(v ?? null)}]`;
    else out += ` ${renderValue(v ?? null)}`;
  }
  return out;
}

/** Canonical DSL text (same as the Rust `render`). */
export function render(conditions: readonly Condition[]): string {
  return conditions.map(renderCondition).join(' AND ');
}

// ───────────── lex ─────────────

type Tok = { t: 'word'; v: string } | { t: 'num'; v: number; raw: string } | { t: 'str'; v: string } | { t: 'sym'; v: string } | { t: 'eof' };

interface Token {
  tok: Tok;
  /** byte offsets */
  start: number;
  end: number;
}

const enc = new TextEncoder();
const blen = (s: string) => enc.encode(s).length;

const NUM_RE = /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/;

function lex(input: string): Token[] {
  const out: Token[] = [];
  const chars = [...input];
  // byte offset of each char index
  const off: number[] = [];
  let b = 0;
  for (const c of chars) {
    off.push(b);
    b += blen(c);
  }
  off.push(b);
  let i = 0;
  while (i < chars.length) {
    const c = chars[i]!;
    if (/\s/u.test(c)) {
      i++;
      continue;
    }
    const two = c + (chars[i + 1] ?? '');
    const sym = (v: string, n: number) => {
      out.push({ tok: { t: 'sym', v }, start: off[i]!, end: off[i + n]! });
      i += n;
    };
    if (c === '"' || c === "'") {
      const start = i;
      let s = '';
      let j = i + 1;
      let closed = false;
      while (j < chars.length) {
        const ch = chars[j]!;
        if (ch === c) {
          closed = true;
          break;
        }
        if (ch !== '\\') {
          s += ch;
          j++;
          continue;
        }
        const e = chars[j + 1];
        if (e === undefined) break;
        const at = off[j]!;
        if (e === '"' || e === "'" || e === '\\' || e === '/') s += e;
        else if (e === 'n') s += '\n';
        else if (e === 'r') s += '\r';
        else if (e === 't') s += '\t';
        else if (e === 'b') s += '\b';
        else if (e === 'f') s += '\f';
        else if (e === 'u') {
          const hex = chars.slice(j + 2, j + 6).join('');
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) throw new FilterSyntaxError(at, 'invalid \\u escape (expected 4 hex digits)');
          let code = parseInt(hex, 16);
          j += 4;
          if (code >= 0xd800 && code < 0xdc00) {
            const lo = chars.slice(j + 2, j + 8).join('');
            if (!/^\\u[0-9a-fA-F]{4}$/.test(lo)) throw new FilterSyntaxError(at, 'invalid \\u escape (expected 4 hex digits)');
            const l = parseInt(lo.slice(2), 16);
            if (l < 0xdc00 || l >= 0xe000) throw new FilterSyntaxError(at, 'invalid \\u escape (expected 4 hex digits)');
            code = 0x10000 + ((code - 0xd800) << 10) + (l - 0xdc00);
            j += 6;
          }
          s += String.fromCodePoint(code);
        } else throw new FilterSyntaxError(at, `invalid escape '\\${e}' in string`);
        j += 2;
      }
      if (!closed) throw new FilterSyntaxError(off[start]!, 'unterminated string');
      out.push({ tok: { t: 'str', v: s }, start: off[start]!, end: off[j + 1]! });
      i = j + 1;
      continue;
    }
    if (c === '!') {
      if (two === '!=') sym('!=', 2);
      else throw new FilterSyntaxError(off[i]!, "unexpected '!' (did you mean '!='?)");
      continue;
    }
    if (c === '=') {
      sym('=', two === '==' ? 2 : 1);
      continue;
    }
    if (c === '<') {
      if (two === '<=') sym('<=', 2);
      else if (two === '<>') sym('!=', 2);
      else sym('<', 1);
      continue;
    }
    if (c === '>') {
      if (two === '>=') sym('>=', 2);
      else sym('>', 1);
      continue;
    }
    if ('[](),'.includes(c)) {
      sym(c, 1);
      continue;
    }
    if (IDENT_START.test(c)) {
      let j = i;
      while (j < chars.length && IDENT_CHAR.test(chars[j]!)) j++;
      const raw = chars.slice(i, j).join('');
      const tok: Tok = NUM_RE.test(raw) && Number.isFinite(Number(raw)) ? { t: 'num', v: Number(raw), raw } : { t: 'word', v: raw };
      out.push({ tok, start: off[i]!, end: off[j]! });
      i = j;
      continue;
    }
    throw new FilterSyntaxError(off[i]!, `unexpected character '${c}'`);
  }
  out.push({ tok: { t: 'eof' }, start: b, end: b });
  return out;
}

// ───────────── parse ─────────────

const wordIs = (t: Tok, kw: string) => t.t === 'word' && t.v.toLowerCase() === kw;

function describe(t: Tok): string {
  switch (t.t) {
    case 'word':
      return `'${t.v}'`;
    case 'num':
      return `'${t.raw}'`;
    case 'str':
      return `string "${t.v}"`;
    case 'sym':
      return `'${t.v}'`;
    case 'eof':
      return 'end of filter';
  }
}

/** DSL text → conditions. Throws `FilterSyntaxError` with a byte position. */
export function parse(input: string): Condition[] {
  const tokens = lex(input);
  const inputLen = blen(input);
  let pos = 0;
  const peek = () => tokens[Math.min(pos, tokens.length - 1)]!;
  const next = () => {
    const t = peek();
    if (pos < tokens.length - 1) pos++;
    return t;
  };

  const scalar = (): JsonValue => {
    const t = next();
    const k = t.tok;
    if (k.t === 'num') return k.v;
    if (k.t === 'str') return k.v;
    if (k.t === 'word') {
      const l = k.v.toLowerCase();
      if (l === 'true') return true;
      if (l === 'false') return false;
      if (l === 'null') return null;
      if (l === 'and' || l === 'or') throw new FilterSyntaxError(t.start, `expected a value, found '${k.v}' (quote it to use it as text)`);
      return k.v;
    }
    if (k.t === 'sym' && (k.v === '[' || k.v === '(')) throw new FilterSyntaxError(t.start, 'lists are only allowed after IN / NOT IN');
    throw new FilterSyntaxError(t.start, `expected a value, found ${describe(k)}`);
  };

  const list = (): JsonValue => {
    const t = next();
    if (t.tok.t !== 'sym' || (t.tok.v !== '[' && t.tok.v !== '(')) throw new FilterSyntaxError(t.start, `expected '[' to start a list, found ${describe(t.tok)}`);
    const close = t.tok.v === '[' ? ']' : ')';
    const items: JsonValue[] = [];
    for (;;) {
      items.push(scalar());
      const n = next();
      if (n.tok.t === 'sym' && n.tok.v === ',') continue;
      if (n.tok.t === 'sym' && n.tok.v === close) return items;
      throw new FilterSyntaxError(n.start, `expected ',' or '${close}', found ${describe(n.tok)}`);
    }
  };

  const field = (): FieldRef => {
    const t = next();
    const quotedAfter = (prefixEnd: number, what: string): string => {
      const n = peek();
      if (n.tok.t === 'str' && n.start === prefixEnd) {
        next();
        return n.tok.v;
      }
      throw new FilterSyntaxError(prefixEnd, `expected ${what} name`);
    };
    const k = t.tok;
    if (k.t === 'word') {
      const lower = k.v.toLowerCase();
      if (lower === 'key') return { kind: 'key' };
      if (lower === 'partition') return { kind: 'partition' };
      if (lower.startsWith('header.')) {
        const rest = k.v.slice('header.'.length);
        return { kind: 'header', name: rest || quotedAfter(t.end, 'header') };
      }
      if (lower.startsWith('payload.')) {
        const rest = k.v.slice('payload.'.length);
        return { kind: 'payload', name: rest || quotedAfter(t.end, 'payload path') };
      }
      if (lower === 'and' || lower === 'or') throw new FilterSyntaxError(t.start, `expected a field, found '${k.v}'`);
      return { kind: 'payload', name: k.v };
    }
    if (k.t === 'num') return { kind: 'payload', name: k.raw };
    if (k.t === 'str') return { kind: 'payload', name: k.v };
    throw new FilterSyntaxError(t.start, `expected a field (key, partition, header.<name> or a payload path), found ${describe(k)}`);
  };

  const condition = (): Condition => {
    const f = field();
    const t = next();
    const k = t.tok;
    let op: Op;
    if (k.t === 'sym' && k.v === '=') op = 'eq';
    else if (k.t === 'sym' && k.v === '!=') op = 'ne';
    else if (k.t === 'sym' && k.v === '>') op = 'gt';
    else if (k.t === 'sym' && k.v === '>=') op = 'gte';
    else if (k.t === 'sym' && k.v === '<') op = 'lt';
    else if (k.t === 'sym' && k.v === '<=') op = 'lte';
    else if (wordIs(k, 'in')) op = 'in';
    else if (wordIs(k, 'prefix')) op = 'prefix';
    else if (wordIs(k, 'contains')) op = 'contains';
    else if (wordIs(k, 'exists')) op = 'exists';
    else if (wordIs(k, 'not')) {
      const n = next();
      if (wordIs(n.tok, 'in')) op = 'not_in';
      else if (wordIs(n.tok, 'exists')) op = 'not_exists';
      else throw new FilterSyntaxError(n.start, `expected IN or EXISTS after NOT, found ${describe(n.tok)}`);
    } else {
      throw new FilterSyntaxError(t.start, `expected an operator (=, !=, >, >=, <, <=, in, not in, prefix, contains, exists, not exists), found ${describe(k)}`);
    }
    if (op === 'exists' || op === 'not_exists') return { field: f, op, value: null };
    return { field: f, op, value: op === 'in' || op === 'not_in' ? list() : scalar() };
  };

  const out: Condition[] = [];
  if (peek().tok.t === 'eof') return out;
  for (;;) {
    out.push(condition());
    const t = next();
    if (t.tok.t === 'eof') return out;
    if (wordIs(t.tok, 'and')) {
      if (peek().tok.t === 'eof') throw new FilterSyntaxError(inputLen, 'expected a condition after AND');
      continue;
    }
    if (wordIs(t.tok, 'or')) throw new FilterSyntaxError(t.start, 'OR is not supported: all conditions are combined with AND (use several routes or searches instead)');
    throw new FilterSyntaxError(t.start, `expected AND or end of filter, found ${describe(t.tok)}`);
  }
}

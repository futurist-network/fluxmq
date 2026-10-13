/**
 * Filter evaluation for the mock — semantics of crates/fluxmq-router/src/filter.rs:
 * absent fields fail every positive op and satisfy `!=`, `not in`, `not exists`; payload
 * conditions never match non-JSON payloads; `eq` between a number and a numeric string matches;
 * `contains` = substring for strings, element equality for arrays; `prefix`/`contains` use the
 * textual form of numbers/bools; headers match exactly, then case-insensitively.
 */
import type { Condition, JsonValue, MessageView } from '../src/api/types';

const ABSENT = Symbol('absent');
type Got = JsonValue | typeof ABSENT;

function payloadDoc(m: MessageView, cache: { doc?: unknown; parsed?: boolean }): unknown {
  if (!cache.parsed) {
    cache.parsed = true;
    try {
      const text = m.payload.encoding === 'utf8' ? m.payload.data : Buffer.from(m.payload.data, 'base64').toString('utf8');
      cache.doc = JSON.parse(text);
    } catch {
      cache.doc = ABSENT;
    }
  }
  return cache.doc;
}

function resolve(c: Condition, m: MessageView, cache: { doc?: unknown; parsed?: boolean }): Got | 'nojson' {
  switch (c.field.kind) {
    case 'key':
      return m.key ?? ABSENT;
    case 'partition':
      return m.partition;
    case 'header': {
      const name = c.field.name;
      if (name in m.headers) return m.headers[name]!;
      const k = Object.keys(m.headers).find((h) => h.toLowerCase() === name.toLowerCase());
      return k ? m.headers[k]! : ABSENT;
    }
    case 'payload': {
      const doc = payloadDoc(m, cache);
      if (doc === ABSENT) return 'nojson';
      let cur: unknown = doc;
      for (const seg of c.field.name.split('.')) {
        if (Array.isArray(cur) && /^\d+$/.test(seg)) cur = cur[Number(seg)];
        else if (cur && typeof cur === 'object' && !Array.isArray(cur) && seg in (cur as object)) cur = (cur as Record<string, unknown>)[seg];
        else return ABSENT;
        if (cur === undefined) return ABSENT;
      }
      return cur as JsonValue;
    }
  }
}

const isNumStr = (s: string) => /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/.test(s);

function eq(a: JsonValue, b: JsonValue): boolean {
  if (typeof a === 'number' && typeof b === 'number') return a === b;
  if (typeof a === 'number' && typeof b === 'string') return isNumStr(b) && Number(b) === a;
  if (typeof a === 'string' && typeof b === 'number') return isNumStr(a) && Number(a) === b;
  if (Array.isArray(a) || Array.isArray(b) || (typeof a === 'object' && a !== null) || (typeof b === 'object' && b !== null)) return JSON.stringify(a) === JSON.stringify(b);
  return a === b;
}

function cmp(a: JsonValue, b: JsonValue): number | null {
  const na = typeof a === 'number' ? a : typeof a === 'string' && isNumStr(a) && typeof b === 'number' ? Number(a) : null;
  const nb = typeof b === 'number' ? b : typeof b === 'string' && isNumStr(b) && typeof a === 'number' ? Number(b) : null;
  if (na != null && nb != null) return na - nb;
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0;
  return null;
}

const text = (v: JsonValue) => (typeof v === 'string' ? v : typeof v === 'number' || typeof v === 'boolean' ? String(v) : null);

function evalOne(c: Condition, m: MessageView, cache: { doc?: unknown; parsed?: boolean }): boolean {
  const got = resolve(c, m, cache);
  if (got === 'nojson') return false;
  const v = (c.value ?? null) as JsonValue;
  const present = got !== ABSENT;
  switch (c.op) {
    case 'exists':
      return present;
    case 'not_exists':
      return !present;
    case 'eq':
      return present && eq(got, v);
    case 'ne':
      return !present || !eq(got, v);
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte': {
      if (!present) return false;
      const r = cmp(got, v);
      if (r == null) return false;
      return c.op === 'gt' ? r > 0 : c.op === 'gte' ? r >= 0 : c.op === 'lt' ? r < 0 : r <= 0;
    }
    case 'in':
    case 'not_in': {
      const list = Array.isArray(v) ? v : [v];
      const hit = present && list.some((x) => eq(got, x));
      return c.op === 'in' ? hit : !hit;
    }
    case 'prefix': {
      if (!present) return false;
      const a = text(got);
      const b = text(v);
      return a != null && b != null && a.startsWith(b);
    }
    case 'contains': {
      if (!present) return false;
      if (Array.isArray(got)) return got.some((x) => eq(x, v));
      const a = text(got);
      const b = text(v);
      return a != null && b != null && a.includes(b);
    }
  }
}

/** All conditions must hold (cheap fields first, payload parsed at most once). */
export function matches(conditions: readonly Condition[], m: MessageView): boolean {
  const cache = {};
  const cheap = conditions.filter((c) => c.field.kind !== 'payload');
  const payload = conditions.filter((c) => c.field.kind === 'payload');
  return cheap.every((c) => evalOne(c, m, cache)) && payload.every((c) => evalOne(c, m, cache));
}

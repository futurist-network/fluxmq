/**
 * Route editor model: structured WHEN rows ⇄ `Condition[]` (crates/fluxmq-proto/src/routing.rs).
 *
 * A row keeps the field and value as the user typed them, in DSL syntax:
 *   field — `key`, `partition`, `header.<name>`, `payload.<path>` or a bare payload path
 *   value — a DSL scalar (`8842`, `"8842"`, `MD`, `true`, `null`); after `in`/`not in` a
 *           comma-separated list of scalars. Key and header values are always strings,
 *           partition values always integers.
 *
 * This is the row model only — not a DSL parser or renderer. Filter text is always parsed and
 * rendered by the server (`POST /api/filter/parse`, `/api/filter/render`).
 */
import type { Condition, FieldRef, JsonValue, Op } from '../api/types';
import { t } from '../i18n';

export interface Row {
  id: string;
  field: string;
  op: Op;
  value: string;
}

/** Value type of a payload field, learned from recent messages (drives the operator list). */
export type FieldType = 'string' | 'number' | 'boolean' | 'array' | 'object' | 'null' | 'unknown';

let rowSeq = 0;
export const newRowId = () => `r${++rowSeq}`;

export function emptyRow(field = ''): Row {
  return { id: newRowId(), field, op: 'eq', value: '' };
}

// ───────────── field ─────────────

export function fieldFromText(text: string): FieldRef | null {
  const t = text.trim();
  if (!t) return null;
  const lower = t.toLowerCase();
  if (lower === 'key') return { kind: 'key' };
  if (lower === 'partition') return { kind: 'partition' };
  if (lower.startsWith('header.')) {
    const name = unquote(t.slice(7));
    return name ? { kind: 'header', name } : null;
  }
  if (lower.startsWith('payload.')) {
    const name = unquote(t.slice(8));
    return name ? { kind: 'payload', name } : null;
  }
  return { kind: 'payload', name: unquote(t) };
}

/** A name that reads back unchanged through `fieldFromText` without quotes. */
const PLAIN_NAME = /^[^\s"',]+$/u;

/**
 * Field → row text (what `fieldFromText` reads back). `key`/`partition` always mean the
 * message key/partition, so a payload field with such a name gets the `payload.` prefix.
 */
export function fieldToText(f: FieldRef): string {
  switch (f.kind) {
    case 'key':
      return 'key';
    case 'partition':
      return 'partition';
    case 'header':
      return `header.${PLAIN_NAME.test(f.name) ? f.name : JSON.stringify(f.name)}`;
    case 'payload': {
      const lower = f.name.toLowerCase();
      const clash = lower === 'key' || lower === 'partition' || lower.startsWith('header.') || lower.startsWith('payload.');
      if (!PLAIN_NAME.test(f.name)) return `payload.${JSON.stringify(f.name)}`;
      return clash ? `payload.${f.name}` : f.name;
    }
  }
}

function unquote(s: string): string {
  const m = /^"(.*)"$/s.exec(s) ?? /^'(.*)'$/s.exec(s);
  if (!m) return s;
  try {
    return s.startsWith('"') ? (JSON.parse(s) as string) : m[1]!;
  } catch {
    return m[1]!;
  }
}

// ───────────── operators ─────────────

/** Operator labels: symbols and DSL keywords stay as-is; worded operators follow the console language. */
const OP_SYMBOL: Partial<Record<Op, string>> = { eq: '=', ne: '≠', gt: '>', gte: '≥', lt: '<', lte: '≤', in: 'in', not_in: 'not in' };
const OP_WORDED = ['prefix', 'contains', 'exists', 'not_exists'] as const satisfies readonly Op[];

export const OP_LABEL: Readonly<Record<Op, string>> = (() => {
  const o = { ...OP_SYMBOL } as Record<Op, string>;
  for (const op of OP_WORDED) Object.defineProperty(o, op, { enumerable: true, get: () => t(`routing.op.${op}`) });
  return o;
})();

const STRING_OPS: Op[] = ['eq', 'ne', 'prefix', 'contains', 'in', 'not_in', 'exists', 'not_exists'];
const NUMBER_OPS: Op[] = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in', 'exists', 'not_exists'];
const ALL_OPS: Op[] = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in', 'prefix', 'contains', 'exists', 'not_exists'];

/** Operators offered for a field (the current op is always kept so loaded routes stay intact). */
export function opsFor(field: FieldRef | null, type: FieldType): Op[] {
  if (!field) return ALL_OPS;
  if (field.kind === 'partition') return ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in'];
  if (field.kind === 'key' || field.kind === 'header') return STRING_OPS;
  switch (type) {
    case 'number':
      return NUMBER_OPS;
    case 'string':
      return STRING_OPS;
    case 'boolean':
    case 'null':
      return ['eq', 'ne', 'exists', 'not_exists'];
    case 'array':
      return ['contains', 'exists', 'not_exists'];
    case 'object':
      return ['exists', 'not_exists'];
    case 'unknown':
      return ALL_OPS;
  }
}

export const opNeedsValue = (op: Op) => op !== 'exists' && op !== 'not_exists';
export const opIsList = (op: Op) => op === 'in' || op === 'not_in';

// ───────────── values ─────────────

const NUM_RE = /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/;

/** One DSL scalar, as the parser reads it (bare words are strings). */
function scalarFromText(raw: string, kind: FieldRef['kind']): JsonValue {
  const t = raw.trim();
  const quoted = /^".*"$/s.test(t) || /^'.*'$/s.test(t);
  if (kind === 'key' || kind === 'header') return quoted ? unquote(t) : t;
  if (quoted) return unquote(t);
  if (kind === 'partition') return NUM_RE.test(t) ? Number(t) : t;
  if (NUM_RE.test(t)) return Number(t);
  const l = t.toLowerCase();
  if (l === 'true') return true;
  if (l === 'false') return false;
  if (l === 'null') return null;
  return t;
}

/** Split `a, "b, c", 3` on commas outside quotes. */
export function splitList(text: string): string[] {
  const out: string[] = [];
  let cur = '';
  let q: string | null = null;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (q) {
      cur += c;
      if (c === '\\' && i + 1 < text.length) cur += text[++i];
      else if (c === q) q = null;
    } else if (c === '"' || c === "'") {
      q = c;
      cur += c;
    } else if (c === ',') {
      out.push(cur.trim());
      cur = '';
    } else cur += c;
  }
  if (cur.trim() || out.length) out.push(cur.trim());
  return out.filter((s) => s !== '');
}

function scalarToText(v: JsonValue, kind: FieldRef['kind'], inList: boolean): string {
  if (typeof v === 'string') {
    if (kind === 'key' || kind === 'header') return (inList && /[,"']/.test(v)) || /^["']/.test(v) ? JSON.stringify(v) : v;
    // Quote strings that would otherwise read back as another type (or split in a list).
    const l = v.toLowerCase();
    const ambiguous = v === '' || v.trim() !== v || NUM_RE.test(v) || l === 'true' || l === 'false' || l === 'null' || /^["']/.test(v) || (inList && /[,"']/.test(v));
    return ambiguous ? JSON.stringify(v) : v;
  }
  if (v === null) return 'null';
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return JSON.stringify(v);
}

// ───────────── row ⇄ condition ─────────────

export interface RowError {
  field?: string;
  value?: string;
}

export function validateRow(r: Row): RowError | null {
  const f = fieldFromText(r.field);
  const err: RowError = {};
  if (!f) err.field = r.field.trim() ? t('routing.row.incompleteField') : t('routing.row.pickField');
  if (opNeedsValue(r.op)) {
    const items = opIsList(r.op) ? splitList(r.value) : [r.value.trim()];
    if (!items.length || items.some((x) => x === '')) err.value = opIsList(r.op) ? t('routing.row.enterValues') : t('routing.row.enterValue');
    else if (f?.kind === 'partition' && items.some((x) => !/^\d+$/.test(x))) err.value = t('routing.row.partitionInt');
  }
  return err.field || err.value ? err : null;
}

export function rowToCondition(r: Row): Condition | null {
  const field = fieldFromText(r.field);
  if (!field || validateRow(r)) return null;
  if (!opNeedsValue(r.op)) return { field, op: r.op, value: null };
  if (opIsList(r.op)) return { field, op: r.op, value: splitList(r.value).map((x) => scalarFromText(x, field.kind)) };
  return { field, op: r.op, value: scalarFromText(r.value, field.kind) };
}

/** All rows → conditions, or null if any row is invalid. */
export function rowsToConditions(rows: readonly Row[]): Condition[] | null {
  const out: Condition[] = [];
  for (const r of rows) {
    const c = rowToCondition(r);
    if (!c) return null;
    out.push(c);
  }
  return out;
}

export function conditionToRow(c: Condition): Row {
  let value = '';
  if (opNeedsValue(c.op)) {
    const v = c.value ?? null;
    if (Array.isArray(v)) value = v.map((x) => scalarToText(x, c.field.kind, true)).join(', ');
    else value = scalarToText(v, c.field.kind, false);
  }
  return { id: newRowId(), field: fieldToText(c.field), op: c.op, value };
}

export const conditionsToRows = (cs: readonly Condition[]): Row[] => cs.map(conditionToRow);

/** Rows without ids (for comparisons in tests and dirty checks). */
export const rowShape = (rows: readonly Row[]) => rows.map(({ field, op, value }) => ({ field, op, value }));

// ───────────── field suggestions from recent messages ─────────────

export interface FieldSuggestion {
  /** DSL field text, e.g. `shipping.country`, `header.flux-type`. */
  field: string;
  type: FieldType;
  /** Distinct example values (as row value text), most frequent first. */
  examples: string[];
  /** Messages (of the sample) containing the field. */
  seen: number;
}

function typeOf(v: unknown): FieldType {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  switch (typeof v) {
    case 'string':
      return 'string';
    case 'number':
      return 'number';
    case 'boolean':
      return 'boolean';
    case 'object':
      return 'object';
    default:
      return 'unknown';
  }
}

/**
 * Learn payload paths and header names from sampled messages. Arrays contribute their first
 * element's paths as `items.0.sku`. Depth-limited so a deep document doesn't flood the list.
 */
export function suggestFields(messages: readonly { headers: Record<string, string>; payload: { encoding: string; data: string } }[], maxDepth = 4): FieldSuggestion[] {
  const acc = new Map<string, { types: Map<FieldType, number>; values: Map<string, number>; seen: number }>();
  const add = (field: string, v: unknown) => {
    let e = acc.get(field);
    if (!e) acc.set(field, (e = { types: new Map(), values: new Map(), seen: 0 }));
    e.seen++;
    const t = typeOf(v);
    e.types.set(t, (e.types.get(t) ?? 0) + 1);
    if (t === 'string' || t === 'number' || t === 'boolean') {
      const text = field.startsWith('header.') ? String(v) : scalarToText(v as JsonValue, 'payload', false);
      if (text.length <= 64) e.values.set(text, (e.values.get(text) ?? 0) + 1);
    }
  };
  const walk = (v: unknown, path: string, depth: number) => {
    if (path) add(fieldToText({ kind: 'payload', name: path }), v);
    if (depth >= maxDepth || v === null || typeof v !== 'object') return;
    if (Array.isArray(v)) {
      if (v.length) walk(v[0], path ? `${path}.0` : '0', depth + 1);
      return;
    }
    for (const [k, child] of Object.entries(v)) walk(child, path ? `${path}.${k}` : k, depth + 1);
  };
  for (const m of messages) {
    for (const [k, v] of Object.entries(m.headers)) add(fieldToText({ kind: 'header', name: k }), v);
    if (m.payload.encoding !== 'utf8') continue;
    try {
      walk(JSON.parse(m.payload.data) as unknown, '', 0);
    } catch {
      /* not JSON */
    }
  }
  const out: FieldSuggestion[] = [];
  for (const [field, e] of acc) {
    const types = [...e.types.entries()].filter(([t]) => t !== 'null').sort((a, b) => b[1] - a[1]);
    const type: FieldType = types.length === 0 ? 'null' : types.length === 1 ? types[0]![0] : 'unknown';
    const examples = [...e.values.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6).map(([v]) => v);
    out.push({ field, type, examples, seen: e.seen });
  }
  // payload paths first (by frequency, shallow first), then headers
  return out.sort((a, b) => {
    const ha = a.field.startsWith('header.') ? 1 : 0;
    const hb = b.field.startsWith('header.') ? 1 : 0;
    return ha - hb || b.seen - a.seen || a.field.split('.').length - b.field.split('.').length || a.field.localeCompare(b.field);
  });
}

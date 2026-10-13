import { describe, expect, it } from 'vitest';
import { conditionsToRows, emptyRow, fieldFromText, fieldToText, opsFor, rowShape, rowsToConditions, splitList, suggestFields, validateRow, type Row } from './routeRows';

// Text ⇄ rows round trips through the (mock) server's parser/renderer live in mock/dsl.test.ts:
// the console has no DSL implementation of its own.

const row = (field: string, op: Row['op'], value = ''): Row => ({ ...emptyRow(field), op, value });

describe('row model', () => {
  it('parses fields like the DSL', () => {
    expect(fieldFromText('key')).toEqual({ kind: 'key' });
    expect(fieldFromText('Partition')).toEqual({ kind: 'partition' });
    expect(fieldFromText('header.flux-type')).toEqual({ kind: 'header', name: 'flux-type' });
    expect(fieldFromText('payload.key')).toEqual({ kind: 'payload', name: 'key' });
    expect(fieldFromText('shipping.country')).toEqual({ kind: 'payload', name: 'shipping.country' });
    expect(fieldFromText('header.')).toBeNull();
  });

  it('writes fields back so they read the same (payload `key` needs the prefix)', () => {
    const fields = [
      { kind: 'key' },
      { kind: 'partition' },
      { kind: 'header', name: 'flux-type' },
      { kind: 'header', name: 'x y' },
      { kind: 'payload', name: 'shipping.country' },
      { kind: 'payload', name: 'key' },
      { kind: 'payload', name: 'Partition' },
      { kind: 'payload', name: 'header.x' },
      { kind: 'payload', name: 'naïve "quoted", name' },
    ] as const;
    for (const f of fields) expect(fieldFromText(fieldToText(f))).toEqual(f);
    expect(fieldToText({ kind: 'payload', name: 'key' })).toBe('payload.key');
    expect(fieldToText({ kind: 'header', name: 'x y' })).toBe('header."x y"');
  });

  it('turns server conditions into rows and back', () => {
    const conds = rowsToConditions([row('shipping.country', 'in', 'MD, RO, "a, b"'), row('order_id', 'eq', '"8842"'), row('flag', 'ne', 'true'), row('payment', 'exists')])!;
    expect(conds).toEqual([
      { field: { kind: 'payload', name: 'shipping.country' }, op: 'in', value: ['MD', 'RO', 'a, b'] },
      { field: { kind: 'payload', name: 'order_id' }, op: 'eq', value: '8842' },
      { field: { kind: 'payload', name: 'flag' }, op: 'ne', value: true },
      { field: { kind: 'payload', name: 'payment' }, op: 'exists', value: null },
    ]);
    const back = conditionsToRows(conds);
    expect(rowsToConditions(back)).toEqual(conds);
    expect(rowShape(back)[1]).toEqual({ field: 'order_id', op: 'eq', value: '"8842"' });
  });

  it('keeps key/header values as strings and partitions as numbers', () => {
    const c = rowsToConditions([row('key', 'eq', '8842'), row('partition', 'eq', '3'), row('order_id', 'eq', '8842')])!;
    expect(c.map((x) => x.value)).toEqual(['8842', 3, 8842]);
  });

  it('validates rows', () => {
    expect(validateRow(row('', 'eq', 'x'))?.field).toBeTruthy();
    expect(validateRow(row('total', 'gt', ''))?.value).toBeTruthy();
    expect(validateRow(row('partition', 'eq', 'x'))?.value).toMatch(/whole number/);
    expect(validateRow(row('x', 'exists'))).toBeNull();
    expect(rowsToConditions([row('a', 'eq', '1'), row('', 'eq', '')])).toBeNull();
  });

  it('offers operators by value type', () => {
    expect(opsFor({ kind: 'payload', name: 'tags' }, 'array')).toEqual(['contains', 'exists', 'not_exists']);
    expect(opsFor({ kind: 'payload', name: 'total' }, 'number')).toContain('gte');
    expect(opsFor({ kind: 'payload', name: 'total' }, 'number')).not.toContain('prefix');
    expect(opsFor({ kind: 'key' }, 'unknown')).toContain('prefix');
    expect(opsFor({ kind: 'partition' }, 'unknown')).not.toContain('exists');
  });

  it('splits lists outside quotes', () => {
    expect(splitList('a, "b, c" , 3')).toEqual(['a', '"b, c"', '3']);
  });

  it('learns field suggestions from messages', () => {
    const msg = (o: unknown) => ({ headers: { 'flux-type': 'order.created' }, payload: { encoding: 'utf8', data: JSON.stringify(o) } });
    const s = suggestFields([msg({ total: 10, shipping: { country: 'DE' }, items: [{ sku: 'A' }] }), msg({ total: 20, shipping: { country: 'NL' }, items: [] })]);
    const by = Object.fromEntries(s.map((x) => [x.field, x]));
    expect(by['total']?.type).toBe('number');
    expect(by['shipping.country']?.examples.sort()).toEqual(['DE', 'NL']);
    expect(by['items']?.type).toBe('array');
    expect(by['items.0.sku']).toBeDefined();
    expect(by['header.flux-type']?.examples).toEqual(['order.created']);
    expect(s[s.length - 1]!.field.startsWith('header.')).toBe(true);
  });
});

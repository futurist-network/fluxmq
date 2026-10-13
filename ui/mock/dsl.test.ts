/**
 * The mock broker's DSL (port of crates/fluxmq-router/src/filter.rs) and the console's row
 * model against it: rows → conditions → server render → server parse → rows.
 */
import { describe, expect, it } from 'vitest';
import type { Condition } from '../src/api/types';
import { conditionsToRows, emptyRow, rowShape, rowsToConditions, type Row } from '../src/lib/routeRows';
import { FilterSyntaxError, parse, render } from './dsl';

const row = (field: string, op: Row['op'], value = ''): Row => ({ ...emptyRow(field), op, value });
const bytes = (s: string) => new TextEncoder().encode(s).length;

describe('editor rows ⇄ server DSL', () => {
  const cases: Row[][] = [
    [row('header.flux-type', 'eq', 'order.created'), row('total', 'gte', '250')],
    [row('key', 'prefix', 'ord_'), row('partition', 'in', '0, 3, 5')],
    [row('shipping.country', 'in', 'MD, RO, "a, b"'), row('payment', 'exists')],
    [row('order_id', 'eq', '"8842"'), row('flag', 'ne', 'true'), row('note', 'eq', 'null')],
    [row('payload.key', 'eq', 'x'), row('header."x y"', 'contains', 'needle'), row('items.0.sku', 'not_in', 'SKU-1, SKU-2')],
    [row('tags', 'contains', 'vip'), row('amount', 'lt', '-12.5'), row('reason', 'not_exists')],
  ];

  it.each(cases.map((c, i) => [i, c] as const))('case %i: rows → render → parse → rows', (_i, rows) => {
    const conds = rowsToConditions(rows);
    expect(conds).not.toBeNull();
    const text = render(conds!);
    const parsed = parse(text);
    expect(parsed).toEqual(conds);
    expect(render(parsed)).toBe(text);
    const back = conditionsToRows(parsed);
    expect(rowsToConditions(back)).toEqual(conds);
    // Rows are stable in their canonical form after one round trip.
    expect(rowShape(conditionsToRows(rowsToConditions(back)!))).toEqual(rowShape(back));
  });

  it('text typed by a user is normalized and becomes rows', () => {
    const conds = parse('order_id = 8842 AND country = MD AND status in (paid, shipped)');
    expect(render(conds)).toBe('order_id = 8842 AND country = "MD" AND status in ["paid", "shipped"]');
    expect(rowShape(conditionsToRows(conds))).toEqual([
      { field: 'order_id', op: 'eq', value: '8842' },
      { field: 'country', op: 'eq', value: 'MD' },
      { field: 'status', op: 'in', value: 'paid, shipped' },
    ]);
  });
});

describe('mock filter DSL (port of filter.rs)', () => {
  it('matches the Rust canonical rendering', () => {
    const c: Condition[] = parse(`type = "order.created" AND country = MD AND amount >= 100 AND header."x y" exists AND payload.key != 'a"b'`);
    expect(render(c)).toBe('type = "order.created" AND country = "MD" AND amount >= 100 AND header."x y" exists AND payload.key != "a\\"b"');
    expect(parse(render(c))).toEqual(c);
  });

  it('rejects OR with a dedicated error', () => {
    expect(() => parse('a = 1 OR b = 2')).toThrow(/OR is not supported/);
  });

  it('accepts == and <> and keyword case', () => {
    expect(render(parse('a == 1 and b <> 2 AND c NOT IN (1)'))).toBe('a = 1 AND b != 2 AND c not in [1]');
  });

  it('reports syntax errors at a UTF-8 byte offset', () => {
    const text = 'naïve = 1 AND total >= OR';
    try {
      parse(text);
      expect.unreachable();
    } catch (e) {
      expect(e).toBeInstanceOf(FilterSyntaxError);
      expect((e as FilterSyntaxError).position).toBe(bytes('naïve = 1 AND total >= '));
    }
  });
});

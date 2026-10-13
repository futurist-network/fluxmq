import { describe, expect, it } from 'vitest';
import { applyMergePatch, diffMergePatch } from './mergePatch';

describe('applyMergePatch (RFC 7386)', () => {
  // Test vectors from RFC 7386 Appendix A.
  const vectors: [unknown, unknown, unknown][] = [
    [{ a: 'b' }, { a: 'c' }, { a: 'c' }],
    [{ a: 'b' }, { b: 'c' }, { a: 'b', b: 'c' }],
    [{ a: 'b' }, { a: null }, {}],
    [{ a: 'b', b: 'c' }, { a: null }, { b: 'c' }],
    [{ a: ['b'] }, { a: 'c' }, { a: 'c' }],
    [{ a: 'c' }, { a: ['b'] }, { a: ['b'] }],
    [{ a: { b: 'c' } }, { a: { b: 'd', c: null } }, { a: { b: 'd' } }],
    [{ a: [{ b: 'c' }] }, { a: [1] }, { a: [1] }],
    [['a', 'b'], ['c', 'd'], ['c', 'd']],
    [{ a: 'b' }, ['c'], ['c']],
    [{ a: 'foo' }, null, null],
    [{ a: 'foo' }, 'bar', 'bar'],
    [{ e: null }, { a: 1 }, { e: null, a: 1 }],
    [[1, 2], { a: 'b', c: null }, { a: 'b' }],
    [{}, { a: { bb: { ccc: null } } }, { a: { bb: {} } }],
  ];
  it.each(vectors)('%j + %j', (target, patch, expected) => {
    expect(applyMergePatch(target, patch)).toEqual(expected);
  });

  it('does not mutate the target', () => {
    const target = { a: { b: 1 }, c: [1, 2] };
    const snapshot = JSON.parse(JSON.stringify(target));
    applyMergePatch(target, { a: { b: 2 }, c: null });
    expect(target).toEqual(snapshot);
  });

  it('keeps identity of untouched subtrees (structural sharing)', () => {
    const target = { cluster: { ingress_rate: 1, dlq_total: 5 }, channels: { orders: { lag: 1 } } };
    const next = applyMergePatch<typeof target>(target, { cluster: { dlq_total: 6 } });
    expect(next.channels).toBe(target.channels);
    expect(next.cluster).not.toBe(target.cluster);
    expect(next.cluster.ingress_rate).toBe(1);
  });

  it('returns the same object when the patch changes nothing', () => {
    const target = { a: 1, b: { c: 2 } };
    expect(applyMergePatch(target, { a: 1, b: { c: 2 } })).toBe(target);
    expect(applyMergePatch(target, { missing: null })).toBe(target);
  });
});

describe('diffMergePatch', () => {
  it('produces a patch that round-trips', () => {
    const prev = { a: 1, b: { c: 2, d: [1, 2] }, e: 'x', gone: true };
    const next = { a: 1, b: { c: 3, d: [1, 2, 3] }, e: 'x', added: { z: 1 } };
    const patch = diffMergePatch(prev, next);
    expect(patch).toEqual({ b: { c: 3, d: [1, 2, 3] }, gone: null, added: { z: 1 } });
    expect(applyMergePatch(prev, patch)).toEqual(next);
  });
  it('returns undefined for equal values', () => {
    expect(diffMergePatch({ a: [1] }, { a: [1] })).toBeUndefined();
  });
});

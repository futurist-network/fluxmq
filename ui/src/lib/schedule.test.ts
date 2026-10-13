import { describe, expect, it } from 'vitest';
import { MAX_DELAY_MS } from '../api/types';
import { DEFAULT_WHEN, resolveWhen, toLocalInput } from './schedule';

const NOW = Date.UTC(2026, 9, 13, 10, 0, 0);

describe('delivery time for the publish dialog', () => {
  it('now sends no fields', () => {
    expect(resolveWhen(DEFAULT_WHEN, NOW)).toEqual({ ok: true, fields: {}, dueMs: null });
  });

  it('in N units becomes delay_ms, bounded by 30 days', () => {
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'in', amount: '15', unit: 'min' }, NOW)).toEqual({ ok: true, fields: { delay_ms: 900_000 }, dueMs: NOW + 900_000 });
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'in', amount: '1.5', unit: 'hour' }, NOW)).toMatchObject({ ok: true, fields: { delay_ms: 5_400_000 } });
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'in', amount: '31', unit: 'day' }, NOW).ok).toBe(false);
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'in', amount: '0', unit: 'sec' }, NOW).ok).toBe(false);
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'in', amount: 'x', unit: 'sec' }, NOW).ok).toBe(false);
  });

  it('at a local time becomes deliver_at_ms; past or too far is refused', () => {
    const at = NOW + 3_600_000;
    const r = resolveWhen({ ...DEFAULT_WHEN, kind: 'at', at: toLocalInput(at) }, NOW);
    expect(r).toEqual({ ok: true, fields: { deliver_at_ms: at }, dueMs: at });
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'at', at: toLocalInput(NOW - 60_000) }, NOW).ok).toBe(false);
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'at', at: toLocalInput(NOW + MAX_DELAY_MS + 3_600_000) }, NOW).ok).toBe(false);
    expect(resolveWhen({ ...DEFAULT_WHEN, kind: 'at', at: '' }, NOW).ok).toBe(false);
  });
});

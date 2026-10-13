/**
 * "When to deliver" for the publish dialog: now, in N units, or at a local date/time →
 * the publish item's `delay_ms` / `deliver_at_ms` (v0.12 delayed messages). Pure, testable.
 */
import { MAX_DELAY_MS } from '../api/types';
import { t } from '../i18n';

export type WhenKind = 'now' | 'in' | 'at';
export type DelayUnit = 'sec' | 'min' | 'hour' | 'day';
export const DELAY_UNITS: readonly DelayUnit[] = ['sec', 'min', 'hour', 'day'];
const UNIT_MS: Record<DelayUnit, number> = { sec: 1000, min: 60_000, hour: 3_600_000, day: 86_400_000 };

export interface When {
  kind: WhenKind;
  amount: string;
  unit: DelayUnit;
  /** `<input type="datetime-local">` value (local time). */
  at: string;
}

export const DEFAULT_WHEN: When = { kind: 'now', amount: '10', unit: 'min', at: '' };

export type WhenResult = { ok: true; fields: { delay_ms?: number; deliver_at_ms?: number }; dueMs: number | null } | { ok: false; error: string };

export function resolveWhen(w: When, nowMs: number): WhenResult {
  if (w.kind === 'now') return { ok: true, fields: {}, dueMs: null };
  if (w.kind === 'in') {
    const n = Number(w.amount);
    if (!w.amount.trim() || !Number.isFinite(n) || n <= 0) return { ok: false, error: t('scheduled.err.amount') };
    const ms = Math.round(n * UNIT_MS[w.unit]);
    if (ms > MAX_DELAY_MS) return { ok: false, error: t('scheduled.err.tooFar') };
    return { ok: true, fields: { delay_ms: ms }, dueMs: nowMs + ms };
  }
  const at = w.at ? new Date(w.at).getTime() : NaN;
  if (!Number.isFinite(at)) return { ok: false, error: t('scheduled.err.at') };
  if (at <= nowMs) return { ok: false, error: t('scheduled.err.past') };
  if (at - nowMs > MAX_DELAY_MS) return { ok: false, error: t('scheduled.err.tooFar') };
  return { ok: true, fields: { deliver_at_ms: at }, dueMs: at };
}

/** `datetime-local` value for a timestamp (local time, minutes). */
export function toLocalInput(ms: number): string {
  const d = new Date(ms - new Date(ms).getTimezoneOffset() * 60_000);
  return d.toISOString().slice(0, 16);
}

/**
 * Secret fields in forms — the uniform rule of the v0.3 contract (notify.rs module docs):
 * reads return `REDACTED`; updates send `REDACTED` to keep the stored value, `""` to remove it,
 * anything else replaces it. Every custom header value is a secret.
 *
 * A form never sees a stored secret: it shows "••••••••" with Replace / Remove; only when the
 * user replaces it does the field hold a real value.
 */
import { REDACTED } from '../api/types';
import { t } from '../i18n';

export interface SecretField {
  /** The server holds a value (read back as REDACTED). */
  stored: boolean;
  /** The user chose to type a new value (or to remove it when `value` is empty). */
  replacing: boolean;
  value: string;
}

export const newSecret = (value = ''): SecretField => ({ stored: false, replacing: true, value });

/** From a read: `REDACTED` (or any non-empty value) = stored; null/"" = none. */
export function secretFromRead(v: string | null | undefined): SecretField {
  return v != null && v !== '' ? { stored: true, replacing: false, value: '' } : { stored: false, replacing: true, value: '' };
}

/** The field keeps the stored value as is. */
export const keepsStored = (f: SecretField) => f.stored && !f.replacing;

/**
 * Value to send. `optional` fields map "nothing" to null on create and to `""` (remove) when a
 * stored value is cleared; required fields never produce null/"" here (validate first).
 */
export function secretToRequest(f: SecretField, optional: true): string | null;
export function secretToRequest(f: SecretField, optional?: false): string;
export function secretToRequest(f: SecretField, optional = false): string | null {
  if (keepsStored(f)) return REDACTED;
  const v = f.value.trim();
  if (v) return v;
  if (!optional) return f.stored ? REDACTED : '';
  return f.stored ? '' : null;
}

/** Validation for a required secret: stored-and-kept is fine, otherwise a value is needed. */
export function secretMissing(f: SecretField): boolean {
  return !keepsStored(f) && f.value.trim() === '';
}

// ───────────────────────────── header rows ─────────────────────────────

export interface HeaderRow {
  id: string;
  key: string;
  /** Header key as read from the server (null for new rows). */
  originalKey: string | null;
  value: SecretField;
}

let rowSeq = 0;
export function newHeaderRow(key = '', value = ''): HeaderRow {
  return { id: `h${++rowSeq}`, key, originalKey: null, value: newSecret(value) };
}

/** Read headers (values are REDACTED) → rows. */
export function headersFromRead(h: Record<string, string> | null | undefined): HeaderRow[] {
  return Object.entries(h ?? {}).map(([key, v]) => ({ id: `h${++rowSeq}`, key, originalKey: key, value: secretFromRead(v) }));
}

const HEADER_NAME = /^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/;

/** Per-row errors (by row id). A kept value can't move to a renamed key: the server keys
 *  stored values by header name, so the value must be typed again. */
export function validateHeaders(rows: HeaderRow[]): Record<string, string> {
  const errs: Record<string, string> = {};
  const seen = new Set<string>();
  for (const r of rows) {
    const k = r.key.trim();
    if (!k && !r.value.value) continue; // blank row, ignored
    if (!k) errs[r.id] = t('security.header.nameRequired');
    else if (!HEADER_NAME.test(k)) errs[r.id] = t('security.header.invalid', { name: k });
    else if (seen.has(k.toLowerCase())) errs[r.id] = t('security.header.duplicate', { name: k });
    else if (keepsStored(r.value) && r.originalKey !== k) errs[r.id] = t('security.header.renamed');
    else if (secretMissing(r.value)) errs[r.id] = t('security.header.valueRequired');
    seen.add(k.toLowerCase());
  }
  return errs;
}

/** Rows → request map (kept values as REDACTED; removed rows are simply absent). */
export function headersToRequest(rows: HeaderRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const r of rows) {
    const k = r.key.trim();
    if (!k) continue;
    out[k] = keepsStored(r.value) ? REDACTED : r.value.value;
  }
  return out;
}

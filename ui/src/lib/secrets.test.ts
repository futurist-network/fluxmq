import { describe, expect, it } from 'vitest';
import { REDACTED, type Notifier } from '../api/types';
import { headersFromRead, headersToRequest, newHeaderRow, secretFromRead, secretToRequest, validateHeaders } from './secrets';
import { draftFromNotifier, draftToCreate, draftToUpdate, emptyNotifierDraft, validateNotifierDraft } from './notifierForm';

describe('secret fields (uniform rule: REDACTED keeps, "" removes, else replace)', () => {
  it('keeps a stored secret untouched', () => {
    const f = secretFromRead(REDACTED);
    expect(f.stored).toBe(true);
    expect(secretToRequest(f)).toBe(REDACTED);
    expect(secretToRequest(f, true)).toBe(REDACTED);
  });
  it('replaces when the user types a new value', () => {
    expect(secretToRequest({ stored: true, replacing: true, value: ' new ' })).toBe('new');
  });
  it('removes an optional stored secret with "" and sends null when never set', () => {
    expect(secretToRequest({ stored: true, replacing: true, value: '' }, true)).toBe('');
    expect(secretToRequest(secretFromRead(null), true)).toBeNull();
  });
  it('a cleared required secret falls back to keeping the stored value', () => {
    expect(secretToRequest({ stored: true, replacing: true, value: '' })).toBe(REDACTED);
  });
});

describe('header rows (every value is a secret)', () => {
  it('round-trips kept values as REDACTED and new values as typed', () => {
    const rows = headersFromRead({ Authorization: REDACTED, 'X-Tenant': REDACTED });
    rows.push(newHeaderRow('X-New', 'v1'));
    expect(headersToRequest(rows)).toEqual({ Authorization: REDACTED, 'X-Tenant': REDACTED, 'X-New': 'v1' });
  });
  it('rejects renaming a header whose value is kept', () => {
    const rows = headersFromRead({ Authorization: REDACTED });
    rows[0]!.key = 'X-Auth';
    expect(Object.values(validateHeaders(rows))[0]).toMatch(/enter its value again/);
  });
  it('flags duplicates and invalid names, ignores blank rows', () => {
    expect(Object.keys(validateHeaders([newHeaderRow('', '')]))).toHaveLength(0);
    expect(Object.values(validateHeaders([newHeaderRow('a', '1'), newHeaderRow('A', '2')]))[0]).toMatch(/duplicate/);
    expect(Object.values(validateHeaders([newHeaderRow('bad name', '1')]))[0]).toMatch(/not a valid/);
  });
});

describe('notifier form', () => {
  const tg: Notifier = { id: 'n1', name: 'On-call', enabled: true, kind: { type: 'telegram', bot_token: REDACTED, chat_id: '-1001234567890', thread_id: 7 }, created_at_ms: 0, updated_at_ms: 0 };
  it('edits without touching the stored bot token', () => {
    const d = draftFromNotifier(tg);
    expect(validateNotifierDraft(d)).toEqual({});
    expect(draftToUpdate({ ...d, chat_id: '-100999' }).kind).toEqual({ type: 'telegram', bot_token: REDACTED, chat_id: '-100999', thread_id: 7 });
  });
  it('validates a new Telegram notifier', () => {
    const d = { ...emptyNotifierDraft('telegram'), name: 'x', chat_id: 'abc' };
    const e = validateNotifierDraft(d);
    expect(e.bot_token).toBeTruthy();
    expect(e.chat_id).toBeTruthy();
    const ok = { ...d, chat_id: '-100123', bot_token: { stored: false, replacing: true, value: '123456:AAEabcdefghijklmnopqrstuv' } };
    expect(validateNotifierDraft(ok)).toEqual({});
    expect(draftToCreate(ok)).toEqual({ name: 'x', enabled: true, kind: { type: 'telegram', bot_token: '123456:AAEabcdefghijklmnopqrstuv', chat_id: '-100123', thread_id: null } });
  });
  it('uses the "pagerduty" tag and maps email optional fields', () => {
    const pd = { ...emptyNotifierDraft('pagerduty'), name: 'PD', routing_key: { stored: false, replacing: true, value: 'a'.repeat(32) } };
    expect(draftToCreate(pd).kind).toEqual({ type: 'pagerduty', routing_key: 'a'.repeat(32) });
    const em = { ...emptyNotifierDraft('email'), name: 'Mail', smtp_host: 'smtp.x.io', from: 'f@x.io', to: ['a@x.io'] };
    expect(validateNotifierDraft(em)).toEqual({});
    expect(draftToCreate(em).kind).toMatchObject({ type: 'email', smtp_port: 587, username: null, password: null, tls: 'starttls' });
  });
});

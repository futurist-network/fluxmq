import { describe, expect, it } from 'vitest';
import { REDACTED, type WebhookSubscription } from '../api/types';
import { draftToCreateWebhook, draftToUpdateWebhook, emptyWebhookDraft, setFormat, statusTone, validateWebhookDraft, webhookToDraft } from './webhookForm';

const base = { ...emptyWebhookDraft('orders.events'), name: 'crm-sync', url: 'https://crm.example.com/hooks/fluxmq' };

describe('webhook editor validation', () => {
  it('accepts defaults', () => {
    expect(validateWebhookDraft(base, { isNew: true, existingNames: [] })).toEqual({});
  });
  it('raw format forces batch = 1', () => {
    const raw = setFormat({ ...base, batch_max: '50' }, 'raw');
    expect(raw.batch_max).toBe('1');
    expect(validateWebhookDraft({ ...raw, batch_max: '10' }, { isNew: true }).batch_max).toMatch(/batch = 1/);
  });
  it('enforces ranges and name rules', () => {
    const e = validateWebhookDraft({ ...base, name: 'CRM Sync', timeout_ms: '50', max_in_flight: '65', batch_max: '501', batch_linger_ms: '10001' }, { isNew: true });
    expect(Object.keys(e).sort()).toEqual(['batch_linger_ms', 'batch_max', 'max_in_flight', 'name', 'timeout_ms']);
    expect(validateWebhookDraft(base, { isNew: true, existingNames: ['crm-sync'] }).name).toMatch(/already exists/);
    expect(validateWebhookDraft({ ...base, timeout_ms: '30000' }, { isNew: true, ackTimeoutMs: 30000 }).timeout_ms).toMatch(/ack timeout/);
    expect(validateWebhookDraft({ ...base, url: 'ftp://x' }, { isNew: true }).url).toBeTruthy();
  });
  it('name/channel are not validated (immutable) when editing', () => {
    expect(validateWebhookDraft({ ...base, name: '' }, { isNew: false })).toEqual({});
  });
});

describe('webhook requests', () => {
  it('create carries start + filter; update omits immutable fields and keeps the secret', () => {
    const c = draftToCreateWebhook({ ...base, start: 'earliest' }, []);
    expect(c).toMatchObject({ name: 'crm-sync', channel: 'orders.events', start: 'earliest', secret: null, batch_max: 1, timeout_ms: 10000 });
    const w: WebhookSubscription = { ...c, id: 'w1', enabled: true, headers: { Authorization: REDACTED }, secret: REDACTED, filter: [], format: 'envelope', timeout_ms: 10000, max_in_flight: 4, batch_max: 1, batch_linger_ms: 0, retry_on_4xx: false, pause_after_failures: 0, start: 'earliest', created_at_ms: 0, updated_at_ms: 0 };
    const u = draftToUpdateWebhook(webhookToDraft(w), []);
    expect(u).not.toHaveProperty('name');
    expect(u).not.toHaveProperty('channel');
    expect(u).not.toHaveProperty('start');
    expect(u.secret).toBe(REDACTED);
    expect(u.headers).toEqual({ Authorization: REDACTED });
  });
  it('circuit breaker: on (5) for new webhooks, off stays off, 0 when unchecked', () => {
    expect(draftToCreateWebhook(base, []).pause_after_failures).toBe(5);
    expect(draftToCreateWebhook({ ...base, pause_enabled: false }, []).pause_after_failures).toBe(0);
    expect(validateWebhookDraft({ ...base, pause_after: '0' }, { isNew: true }).pause_after).toBeTruthy();
    expect(validateWebhookDraft({ ...base, pause_enabled: false, pause_after: 'x' }, { isNew: true })).toEqual({});
    const stored = { ...draftToCreateWebhook(base, []), id: 'w', enabled: true, headers: {}, secret: null, filter: [], format: 'envelope' as const, timeout_ms: 1, max_in_flight: 1, batch_max: 1, batch_linger_ms: 0, retry_on_4xx: false, start: 'latest' as const, created_at_ms: 0, updated_at_ms: 0 };
    // Before v0.11 the field is absent: the breaker is off.
    const old: WebhookSubscription = { ...stored, pause_after_failures: undefined };
    expect(webhookToDraft(old).pause_enabled).toBe(false);
    expect(draftToUpdateWebhook(webhookToDraft(old), []).pause_after_failures).toBe(0);
    expect(webhookToDraft({ ...stored, pause_after_failures: 12 })).toMatchObject({ pause_enabled: true, pause_after: '12' });
  });
  it('colours only error status codes', () => {
    expect(statusTone(200)).toBeNull();
    expect(statusTone(503)).toBe('critical');
    expect(statusTone(404)).toBe('warning');
  });
});

describe('webhook error text', () => {
  it('shortens transport errors for badges, keeps the full text elsewhere', async () => {
    const { shortError } = await import('./webhookForm');
    expect(shortError('connection refused: error sending request for url (http://x)')).toBe('connection refused');
    expect(shortError('HTTP 503')).toBe('HTTP 503');
  });
});

import { afterEach, describe, expect, it } from 'vitest';
import { setLang } from '../i18n';
import { classifyReason, isNonTransient, reasonText } from './dlqReason';

afterEach(() => setLang('en'));

describe('isNonTransient', () => {
  it.each(['Invalid schema', 'validation failed: amount < 0', 'JSON parse error', 'HTTP 422', 'HTTP 404', 'HTTP 301', 'missing field `id`', 'blocked by policy'])('%s → permanent', (r) => expect(isNonTransient(r)).toBe(true));
  it.each(['HTTP 503', 'HTTP 429', 'HTTP 408', 'timeout', 'connection reset by peer', 'ack timeout', 'consumer disconnected', 'TLS error', 'route target unavailable: orders'])('%s → transient', (r) =>
    expect(isNonTransient(r)).toBe(false),
  );
});

describe('classifyReason', () => {
  it('maps broker, router and webhook codes', () => {
    expect(classifyReason('ack timeout').kind).toBe('ack_timeout');
    expect(classifyReason('consumer disconnected').kind).toBe('disconnected');
    expect(classifyReason('unknown').kind).toBe('no_reason');
    expect(classifyReason('route target unavailable: billing.audit')).toEqual({ kind: 'route_target', target: 'billing.audit' });
    expect(classifyReason('HTTP 503')).toEqual({ kind: 'http_unavailable', status: 503 });
    expect(classifyReason('HTTP 429')).toEqual({ kind: 'http_rate_limit', status: 429 });
    expect(classifyReason('HTTP 401')).toEqual({ kind: 'http_rejected', status: 401 });
    expect(classifyReason('HTTP 302')).toEqual({ kind: 'http_redirect', status: 302 });
    for (const [r, k] of [
      ['timeout', 'timeout'],
      ['connection refused', 'conn_refused'],
      ['connection failed', 'conn_failed'],
      ['TLS error', 'tls'],
      ['DNS error', 'dns'],
      ['blocked by policy', 'blocked'],
      ['request failed', 'request_failed'],
    ] as const)
      expect(classifyReason(r).kind).toBe(k);
  });

  it('anything else is the consumer’s own text', () => {
    for (const r of ['Invalid schema', 'PostgreSQL timeout', 'ack timeout exceeded', 'HTTP 503 from upstream', 'SMTP 421']) expect(classifyReason(r).kind).toBe('consumer');
  });
});

describe('reasonText', () => {
  it('translates known codes and keeps the raw code apart', () => {
    const a = reasonText('ack timeout');
    expect(a).toMatchObject({ kind: 'ack_timeout', title: 'Not acknowledged in time', coded: true, permanent: false });
    expect(a.hint).toMatch(/ack timeout/);
    expect(reasonText('HTTP 503').title).toBe('Endpoint unavailable (HTTP 503)');
    expect(reasonText('route target unavailable: audit').title).toBe('Route target unavailable: audit');
    expect(reasonText('HTTP 400')).toMatchObject({ permanent: true, coded: true });
  });

  it('shows consumer text as is, with a hint matching its permanence', () => {
    expect(reasonText('PostgreSQL timeout')).toMatchObject({ kind: 'consumer', title: 'PostgreSQL timeout', coded: false, permanent: false });
    const s = reasonText('Invalid schema');
    expect(s.permanent).toBe(true);
    expect(s.hint).toMatch(/permanent/);
  });

  it('Russian: every code gets Russian text, params substituted', () => {
    setLang('ru');
    const codes = ['ack timeout', 'consumer disconnected', 'unknown', 'route target unavailable: audit', 'HTTP 429', 'HTTP 503', 'HTTP 404', 'HTTP 301', 'timeout', 'connection refused', 'connection failed', 'TLS error', 'DNS error', 'blocked by policy', 'request failed'];
    for (const r of codes) {
      const x = reasonText(r);
      expect(x.title).toMatch(/[а-яё]/i);
      expect(x.hint).toMatch(/[а-яё]/i);
      expect(x.title + x.hint).not.toMatch(/\{\w+\}/);
    }
    expect(reasonText('HTTP 503').title).toBe('Принимающий сервис недоступен (HTTP 503)');
    expect(reasonText('Invalid schema').title).toBe('Invalid schema');
    expect(reasonText('Invalid schema').hint).toMatch(/nack/);
  });
});

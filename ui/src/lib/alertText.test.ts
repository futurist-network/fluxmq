import { afterEach, describe, expect, it } from 'vitest';
import type { Alert, AlertKind, FluxEvent } from '../api/types';
import { setLang } from '../i18n';
import { alertParams, alertText, collapseEvents, eventText, parseCount, parseDurationMs, parseMs } from './alertText';

afterEach(() => setLang('en'));

const base = (kind: AlertKind, over: Partial<Alert>): Alert => ({
  id: `${kind}:email/email-service`,
  severity: 'warning',
  kind,
  title: '',
  detail: '',
  possible_reason: 'English reason from the server.',
  link: null,
  channel: 'email',
  group: 'email-service',
  since_ms: 0,
  ...over,
});

/** Alerts exactly as brokers ≤ v0.9.0 send them (English, no params). */
const LEGACY: Alert[] = [
  base('lag_growth', { severity: 'critical', title: 'Email queue is growing rapidly', detail: 'Lag increased from 1.2k → 18k in 6 min (email-service)' }),
  base('lag_growth', { title: 'Email queue is growing', detail: 'Lag increased from 1.5k → 2.7k in 60 s (email-service)' }),
  base('no_consumers', { severity: 'critical', title: 'No consumers for Email', detail: 'Group email-service has 950 pending messages and no connected instances' }),
  base('consumer_stalled', { severity: 'critical', title: 'Email consumers stopped acknowledging', detail: 'No acks from email-service for 45 s while 50 messages are pending (2 instances)' }),
  base('high_retry_rate', { title: 'High failure rate on Email', detail: '10.0% of messages handled by email-service were nacked in the last minute (10 of 100)' }),
  base('dlq_growth', { title: 'Messages are being dead-lettered on Email', detail: '35 messages moved to the DLQ in the last 3 min (email-service)' }),
  base('slow_instance', {
    id: 'slow_instance:email/email-service/worker-03#7',
    title: 'Slow consumer worker-03#7',
    detail: 'Ack rate 2.0/s vs group median 14/s; Processing p99 1.4 s vs group 36 ms (email-service on email)',
  }),
  base('storage_nearly_full', { id: 'storage_nearly_full:n1', channel: null, group: null, title: 'Storage nearly full', detail: 'Disk 85% used (85 GB of 100 GB) on n1' }),
  base('high_publish_latency', { id: 'high_publish_latency:node/n1', channel: null, group: null, title: 'Disk latency is slowing down publishes', detail: 'fsync p99 is 80 ms on n1' }),
  base('high_publish_latency', { id: 'high_publish_latency:email', group: null, title: 'High publish latency on Email', detail: 'Publish p99 is 120 ms at 1.2k msg/s' }),
  base('node_down', { id: 'node_down:n3', channel: null, group: null, severity: 'critical', title: 'Node n3 is down', detail: 'No stats from n3 for 12s (ECONNREFUSED)' }),
  base('under_replicated_partitions', { id: 'under_replicated_partitions:cluster', channel: null, group: null, title: '3 under-replicated partitions', detail: '3 partitions have fewer in-sync replicas than the replication factor (3)' }),
  base('offline_partitions', { id: 'offline_partitions:cluster', channel: null, group: null, severity: 'critical', title: '1 partition offline', detail: '1 partition has no leader: publishing to and consuming from it fails' }),
  base('tls_cert_expiring', { id: 'tls_cert_expiring:n1/cluster', channel: null, group: null, title: 'TLS certificate for cluster on n1 expires in 5 days', detail: 'CN=n1 (issuer CN=fluxmq-ca) is valid until 2026-10-16 12:00 UTC' }),
  base('tls_reload_failed', { id: 'tls_reload_failed:n1/client', channel: null, group: null, title: 'TLS certificate reload failed for client on n1', detail: 'EACCES' }),
];

/** Same alerts from a v0.9.1 broker (params, no parsing needed). */
const WITH_PARAMS: Alert[] = [
  base('lag_growth', { title: 'x', detail: 'x', params: { variant: 'high', lag: 140_000, mode: 'queue' } }),
  base('slow_instance', { id: 'slow_instance:email/email-service/n2/worker-03#7', title: 'x', detail: 'x', params: { instance: 'n2/worker-03#7', p99_ms: 1400, group_p99_ms: 36 } }),
  base('dlq_growth', { title: 'x', detail: 'x', params: { added: 1, over_ms: 60_000 } }),
];

/** An English sentence: two plain English words in a row (technical tokens like p99/fsync/DLQ are single words). */
const ENGLISH = /\b(?!fsync\b|issuer\b)[a-z]{3,} (?!fsync\b)[a-z]{3,}\b/i;

describe('alert texts', () => {
  it('parses the legacy number formats', () => {
    expect(parseCount('1.2k')).toBe(1200);
    expect(parseCount('18k')).toBe(18_000);
    expect(parseCount('2.5M')).toBe(2_500_000);
    expect(parseDurationMs('6 min')).toBe(360_000);
    expect(parseDurationMs('45 s')).toBe(45_000);
    expect(parseMs('1.4 s')).toBe(1400);
    expect(parseMs('36 ms')).toBe(36);
  });

  it('extracts params from every known legacy alert', () => {
    for (const a of LEGACY) expect(alertParams(a), a.id).not.toBeNull();
    expect(alertParams(LEGACY[6]!)).toMatchObject({ instance: 'worker-03#7', rate: 2, median_rate: 14, p99_ms: 1400, group_p99_ms: 36 });
  });

  it('no English sentence leaks into Russian for known alert kinds', () => {
    setLang('ru');
    for (const a of [...LEGACY, ...WITH_PARAMS]) {
      const x = alertText(a);
      for (const [field, text] of Object.entries(x)) {
        if (text == null) continue;
        // Raw OS / TLS errors (fixtures use error codes) and certificate DNs are data, not prose.
        const prose = String(text).replace(/CN=[^\s)]+/g, '').replace(/\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC/, '');
        expect(prose, `${a.id} ${field}: ${text}`).not.toMatch(ENGLISH);
        expect(prose, `${a.id} ${field}`).toMatch(/[а-яё]/i);
      }
    }
  });

  it('names the instance and channel like the English text does', () => {
    setLang('ru');
    const x = alertText(LEGACY[6]!);
    expect(x.title).toBe('Медленный получатель worker-03#7');
    expect(x.detail).toContain('группа email-service, канал email');
    expect(x.short).toBe('worker-03#7: p99 в 39× выше, чем у группы');
    expect(alertText(LEGACY[0]!).title).toBe('Отставание в email быстро растёт');
  });

  it('English is built from the same params', () => {
    expect(alertText(LEGACY[5]!).detail).toBe('35 messages moved to the DLQ in the last 3m (email-service)');
    expect(alertText(WITH_PARAMS[0]!).title).toBe('email backlog is high');
  });

  it('unknown kinds keep the server text', () => {
    const a = base('lag_growth', { kind: 'brand_new' as AlertKind, title: 'Server title', detail: 'Server detail' });
    setLang('ru');
    expect(alertText(a).detail).toBe('Server detail');
    setLang('en');
    expect(alertText(a).title).toBe('Server title');
  });
});

describe('events', () => {
  const ev = (id: number, ts: number, subject: string, kind: FluxEvent['kind'] = 'dead_lettered'): FluxEvent => ({ id, ts, kind, title: 'Messages rerouted to DLQ', subject, severity: 'warning' });

  it('localises dead-letter subjects', () => {
    setLang('ru');
    expect(eventText(ev(1, 0, 'payments.events (7 messages)')).subject).toBe('payments.events · 7 сообщений');
  });

  it('collapses repeated dead-letter entries of one channel within 5 minutes', () => {
    const rows = collapseEvents([
      ev(5, 400_000, 'payments.events (7 messages)'),
      ev(4, 340_000, 'payments.events (15 messages)'),
      ev(3, 280_000, 'payments.events (13 messages)'),
      ev(2, 270_000, 'orders.events (1 message)'),
      ev(1, -100_000, 'orders.events (2 messages)'),
    ]);
    expect(rows.map((r) => [r.event.id, r.times, r.count])).toEqual([
      [5, 3, 35],
      [2, 1, 1],
      [1, 1, 2],
    ]);
  });
});

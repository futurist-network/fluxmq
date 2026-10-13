import { afterEach, describe, expect, it } from 'vitest';
import { shallow } from 'zustand/shallow';
import type { AlertsState, ChannelSummary, ClusterOverview, GroupState, InstanceState } from '../api/types';
import { applyMergePatch } from '../api/mergePatch';
import { alertCounts, alertSeverityByKind, flattenGroups, medianProcessingP99, openAlerts, sortedChannels, sortedInstances, worstHealth } from './selectors';
import { commitFrame, resetTelemetryStore, useTelemetry } from './telemetry';
import { SeriesStore, recordFrame } from './series';
import { timelineEnd } from '../lib/timeline';

const ch = (name: string, over: Partial<ChannelSummary> = {}): ChannelSummary => ({
  name,
  mode: 'stream',
  health: 'healthy',
  partitions: 1,
  ingress_rate: 0,
  egress_rate: 0,
  ack_rate: 0,
  lag: 0,
  publish_p99_ms: 1,
  consumer_groups: 0,
  consumers: 0,
  storage_bytes: 0,
  messages: 0,
  dlq: 0,
  ...over,
});

const inst = (id: string, over: Partial<InstanceState> = {}): InstanceState => ({
  id,
  client_name: id,
  remote_addr: '',
  connected_at_ms: 0,
  health: 'healthy',
  egress_rate: 0,
  ack_rate: 0,
  nack_rate: 0,
  in_flight: 0,
  prefetch: 0,
  lag: 0,
  assigned_partitions: [],
  processing_p99_ms: 10,
  retries_total: 0,
  last_error: null,
  last_error_at_ms: null,
  cpu_percent: null,
  memory_bytes: null,
  ...over,
});

const lat = { p50: 0, p95: 0, p99: 0, p999: 0, max: 0 };
const grp = (group: string, over: Partial<GroupState> = {}): GroupState => ({
  group,
  health: 'healthy',
  lag: 0,
  egress_rate: 0,
  ack_rate: 0,
  nack_rate: 0,
  in_flight: 0,
  retry_pending: 0,
  dlq: 0,
  processing_latency: lat,
  instances: {},
  ...over,
});

describe('pure selectors', () => {
  it('sortedChannels: problems first, then traffic; filters by mode and query', () => {
    const s = {
      a: ch('a', { ingress_rate: 10 }),
      b: ch('b', { ingress_rate: 100 }),
      email: ch('email', { health: 'warning', mode: 'queue' }),
      inv: ch('inv', { health: 'critical' }),
    };
    expect(sortedChannels(s).map((c) => c.name)).toEqual(['inv', 'email', 'b', 'a']);
    expect(sortedChannels(s, { mode: 'queue' }).map((c) => c.name)).toEqual(['email']);
    expect(sortedChannels(s, { query: 'EM' }).map((c) => c.name)).toEqual(['email']);
    expect(sortedChannels(undefined)).toEqual([]);
  });

  it('alerts: ordering, counts, severity by kind', () => {
    const alerts: AlertsState = {
      x: { id: 'x', severity: 'warning', kind: 'dlq_growth', title: '', detail: '', possible_reason: null, link: null, channel: null, group: null, since_ms: 5 },
      y: { id: 'y', severity: 'critical', kind: 'consumer_stalled', title: '', detail: '', possible_reason: null, link: null, channel: null, group: null, since_ms: 1 },
      z: { id: 'z', severity: 'warning', kind: 'lag_growth', title: '', detail: '', possible_reason: null, link: null, channel: null, group: null, since_ms: 9 },
    };
    expect(openAlerts(alerts).map((a) => a.id)).toEqual(['y', 'z', 'x']);
    expect(alertCounts(alerts)).toEqual({ total: 3, critical: 1, warning: 2 });
    expect(alertSeverityByKind(alerts)).toEqual({ dlq_growth: 'warning', consumer_stalled: 'critical', lag_growth: 'warning' });
  });

  it('flattenGroups and instances: worst first', () => {
    const groups = flattenGroups({
      orders: { billing: grp('billing'), analytics: grp('analytics', { lag: 5 }) },
      email: { workers: grp('workers', { health: 'warning', lag: 1 }) },
      gone: undefined,
    });
    expect(groups.map((g) => `${g.channel}/${g.group.group}`)).toEqual(['email/workers', 'orders/analytics', 'orders/billing']);
    const g = grp('w', { instances: { a: inst('a', { processing_p99_ms: 10 }), b: inst('b', { health: 'warning', processing_p99_ms: 50 }), c: inst('c', { processing_p99_ms: 12 }) } });
    expect(sortedInstances(g).map((i) => i.id)).toEqual(['b', 'a', 'c']);
    expect(medianProcessingP99(g)).toBe(12);
    expect(worstHealth(['healthy', 'warning', 'healthy'])).toBe('warning');
  });

  it('timelineEnd', () => {
    const a = (attempt: number, kind: 'acked' | 'nacked' | 'in_flight') => ({
      attempt,
      consumer: 'c',
      delivered_at_ms: 0,
      finished_at_ms: 1,
      outcome: kind === 'nacked' ? { kind, reason: 'x' } : { kind },
    });
    expect(timelineEnd([], 5)).toBe('none');
    expect(timelineEnd([a(1, 'nacked'), a(2, 'acked')], 5)).toBe('acked');
    expect(timelineEnd([a(1, 'nacked')], 5)).toBe('retrying');
    expect(timelineEnd([a(1, 'nacked'), a(2, 'nacked')], 2)).toBe('dead');
    expect(timelineEnd([a(1, 'in_flight')], 5)).toBe('in_flight');
  });
});

describe('store: fine-grained selection', () => {
  afterEach(() => resetTelemetryStore());

  const overview = { ingress_rate: 100, dlq_total: 1, publish_latency: lat } as unknown as ClusterOverview;

  function commit(view: Record<string, unknown>, changed: string[]) {
    commitFrame({ view, changed: new Set(changed), errors: {}, now: Date.now() });
  }

  it('a DLQ change does not notify a throughput selector', () => {
    commit({ 'cluster.overview': overview }, ['cluster.overview']);
    let throughputCalls = 0;
    let dlqCalls = 0;
    const sel = (s: ReturnType<typeof useTelemetry.getState>) => (s.topics['cluster.overview'] as ClusterOverview | undefined)?.ingress_rate;
    const unsub1 = useTelemetry.subscribe((s, prev) => {
      if (sel(s) !== sel(prev)) throughputCalls++;
    });
    const unsub2 = useTelemetry.subscribe((s, prev) => {
      const d = (x: typeof s) => (x.topics['cluster.overview'] as ClusterOverview).dlq_total;
      if (d(s) !== d(prev)) dlqCalls++;
    });
    const next = applyMergePatch(overview, { dlq_total: 2 });
    commit({ 'cluster.overview': next }, ['cluster.overview']);
    expect(dlqCalls).toBe(1);
    expect(throughputCalls).toBe(0);
    // Untouched nested objects keep identity, so object selectors stay shallow-equal too.
    const latSel = (s: ReturnType<typeof useTelemetry.getState>) => (s.topics['cluster.overview'] as ClusterOverview).publish_latency;
    expect(latSel(useTelemetry.getState())).toBe(lat);
    expect(shallow({ a: 1 }, { a: 1 })).toBe(true);
    unsub1();
    unsub2();
  });

  it('paused freezes the rendered view and resume publishes the latest frame', () => {
    commit({ 'cluster.overview': overview }, ['cluster.overview']);
    useTelemetry.getState().setPaused(true);
    commit({ 'cluster.overview': { ...overview, ingress_rate: 999 }, alerts: {} }, ['cluster.overview', 'alerts']);
    const s = useTelemetry.getState();
    expect((s.topics['cluster.overview'] as ClusterOverview).ingress_rate).toBe(100); // frozen
    expect(s.topics.alerts).toEqual({}); // newly subscribed topic still appears
    s.setPaused(false);
    expect((useTelemetry.getState().topics['cluster.overview'] as ClusterOverview).ingress_rate).toBe(999);
  });

  it('series recorder buckets samples per second', () => {
    const store = new SeriesStore();
    const t0 = 1_700_000_000_000;
    const view = { 'cluster.overview': { ...overview, retries_rate: 3, total_lag: 7, publish_latency: { ...lat, p99: 2 } } };
    recordFrame({ view, changed: new Set(['cluster.overview']), errors: {}, now: t0 }, store);
    recordFrame({ view: { 'cluster.overview': { ...view['cluster.overview'], ingress_rate: 200 } }, changed: new Set(['cluster.overview']), errors: {}, now: t0 + 250 }, store);
    recordFrame({ view, changed: new Set(['cluster.overview']), errors: {}, now: t0 + 1000 }, store);
    const s = store.get('cluster.ingress');
    expect(s.xs).toEqual([t0 / 1000, t0 / 1000 + 1]);
    expect(s.ys).toEqual([200, 100]);
  });
});

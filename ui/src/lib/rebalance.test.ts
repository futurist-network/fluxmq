import { describe, expect, it } from 'vitest';
import type { FluxEvent, PartitionMove, RebalanceConfig, RebalanceNodeLoad } from '../api/types';
import {
  catchUpProgress,
  configsEqual,
  configToDraft,
  draftToConfig,
  fmtSecsHuman,
  fmtThrottle,
  imbalanceLevel,
  joinLoad,
  latestRebalanceEventId,
  loadScale,
  moveDiff,
  moveElapsedMs,
  peakLagFor,
  planSummary,
  pollIntervalMs,
  sortMoves,
  splitDuration,
  splitThroughput,
  spread,
  trackPeakLag,
  validateRebalanceDraft,
} from './rebalance';

const mv = (p: Partial<PartitionMove> = {}): PartitionMove => ({
  channel: 'orders.events',
  partition: 3,
  reason: 'balance',
  from_replicas: ['n1', 'n2', 'n3'],
  to_replicas: ['n4', 'n2', 'n3'],
  state: 'pending',
  lag: null,
  initial_lag: null,
  started_at_ms: null,
  finished_at_ms: null,
  error: null,
  ...p,
});

const load = (node: string, replicas: number, leaders = 0, preferred_leaders = 0, storage_bytes = 0, status: RebalanceNodeLoad['status'] = 'up'): RebalanceNodeLoad => ({ node, status, replicas, leaders, preferred_leaders, storage_bytes });

const defaults: RebalanceConfig = { mode: 'full', max_concurrent_moves: 2, imbalance_threshold: 1, replace_after_secs: 600, throttle_bytes_per_sec: 0, move_timeout_secs: 1800 };

describe('polling cadence', () => {
  it('polls every 2 s while running or cancelling, 10 s otherwise', () => {
    expect(pollIntervalMs('running')).toBe(2000);
    expect(pollIntervalMs('cancelling')).toBe(2000);
    expect(pollIntervalMs('paused')).toBe(10_000);
    expect(pollIntervalMs('idle')).toBe(10_000);
    expect(pollIntervalMs(undefined)).toBe(10_000);
  });
});

describe('rebalance events', () => {
  const ev = (id: number, kind: FluxEvent['kind']): FluxEvent => ({ id, ts: id, kind, title: '', subject: '', severity: 'healthy' });
  it('finds the newest rebalance-related event id', () => {
    expect(latestRebalanceEventId([ev(9, 'node_up'), ev(7, 'partition_moved'), ev(5, 'rebalance_started')])).toBe(7);
    expect(latestRebalanceEventId([ev(3, 'replica_repair_started')])).toBe(3);
    expect(latestRebalanceEventId([ev(4, 'tls_reload_failed'), ev(2, 'channel_created')])).toBeNull();
    expect(latestRebalanceEventId(undefined)).toBeNull();
  });
});

describe('moveDiff', () => {
  it('splits added / removed / kept and detects a preferred-leader change', () => {
    expect(moveDiff(mv())).toEqual({ added: ['n4'], removed: ['n1'], kept: ['n2', 'n3'], leaderChange: true });
  });
  it('a reorder only moves the preferred leader', () => {
    expect(moveDiff(mv({ from_replicas: ['n2', 'n1'], to_replicas: ['n1', 'n2'] }))).toEqual({ added: [], removed: [], kept: ['n1', 'n2'], leaderChange: true });
  });
  it('RF growth (repair of a capped channel) only adds', () => {
    const d = moveDiff(mv({ from_replicas: ['n1', 'n2'], to_replicas: ['n1', 'n2', 'n3'] }));
    expect(d.added).toEqual(['n3']);
    expect(d.removed).toEqual([]);
    expect(d.leaderChange).toBe(false);
  });
});

describe('catch-up progress', () => {
  it('is relative to the highest lag seen for this attempt', () => {
    const a = mv({ state: 'catching_up', lag: 40_000, started_at_ms: 100 });
    let peaks = trackPeakLag({}, [a]);
    expect(peakLagFor(peaks, a)).toBe(40_000);
    expect(catchUpProgress(a, peakLagFor(peaks, a))).toBe(0);
    const b = { ...a, lag: 10_000 };
    peaks = trackPeakLag(peaks, [b]);
    expect(peakLagFor(peaks, b)).toBe(40_000);
    expect(catchUpProgress(b, peakLagFor(peaks, b))).toBeCloseTo(0.75);
  });
  it('a retried move (new start time) gets a fresh baseline; finished moves are dropped', () => {
    const peaks = trackPeakLag({ 'orders.events/3@100': 90_000 }, [mv({ state: 'catching_up', lag: 5_000, started_at_ms: 200 })]);
    expect(peaks).toEqual({ 'orders.events/3@200': 5_000 });
    expect(trackPeakLag(peaks, [mv({ state: 'done' })])).toEqual({});
  });
  it('uses initial_lag from the server when present', () => {
    expect(catchUpProgress(mv({ state: 'catching_up', lag: 25_000, initial_lag: 100_000 }), 30_000)).toBeCloseTo(0.75);
    // null initial_lag → observed peak
    expect(catchUpProgress(mv({ state: 'catching_up', lag: 25_000, initial_lag: null }), 50_000)).toBeCloseTo(0.5);
  });
  it('maps the other states', () => {
    expect(catchUpProgress(mv({ state: 'cancelled' }), null)).toBeNull();
    expect(catchUpProgress(mv({ state: 'pending' }), null)).toBe(0);
    expect(catchUpProgress(mv({ state: 'finalizing', lag: 0 }), 10)).toBe(1);
    expect(catchUpProgress(mv({ state: 'done' }), null)).toBe(1);
    expect(catchUpProgress(mv({ state: 'rolled_back' }), 10)).toBeNull();
    expect(catchUpProgress(mv({ state: 'catching_up', lag: null }), null)).toBeNull();
    expect(catchUpProgress(mv({ state: 'catching_up', lag: 0 }), null)).toBe(1);
    // Lag grew beyond the recorded peak (leader is busy): never negative.
    expect(catchUpProgress(mv({ state: 'catching_up', lag: 500 }), 100)).toBe(0);
  });
});

describe('plan summary and ordering', () => {
  const moves = [mv({ state: 'done' }), mv({ state: 'cancelled' }), mv({ state: 'catching_up' }), mv({ state: 'pending' }), mv({ state: 'rolled_back' }), mv({ state: 'finalizing' }), mv({ state: 'done' }), mv({ state: 'cancelled' })];
  it('counts states (cancelled counts as settled)', () => {
    expect(planSummary(moves)).toEqual({ total: 8, pending: 1, inFlight: 2, done: 2, rolledBack: 1, cancelled: 2, fraction: 5 / 8 });
    expect(planSummary([]).fraction).toBe(0);
  });
  it('sorts in flight → pending → rolled back → done → cancelled, stable', () => {
    expect(sortMoves(moves).map((m) => m.state)).toEqual(['catching_up', 'finalizing', 'pending', 'rolled_back', 'done', 'done', 'cancelled', 'cancelled']);
  });
  it('elapsed time counts running moves up to now', () => {
    expect(moveElapsedMs({ started_at_ms: 1000, finished_at_ms: null }, 4000)).toBe(3000);
    expect(moveElapsedMs({ started_at_ms: 1000, finished_at_ms: 2500 }, 9000)).toBe(1500);
    expect(moveElapsedMs({ started_at_ms: null, finished_at_ms: null }, 9000)).toBeNull();
  });
});

describe('load', () => {
  const cur = [load('n2', 35, 14, 14, 900), load('n1', 35, 11, 11, 800), load('n10', 0)];
  const next = [load('n1', 26, 8, 8), load('n2', 26), load('n10', 27)];
  it('shares a scale across current and projected load', () => {
    expect(loadScale(cur, next)).toEqual({ replicas: 35, leaders: 14, preferred_leaders: 14, storage_bytes: 900 });
  });
  it('spread = max − min replicas over up nodes only', () => {
    expect(spread(cur)).toBe(35);
    expect(spread(next)).toBe(1);
    expect(spread([])).toBe(0);
    // A down node keeps its replicas until repair moves them, but doesn't count.
    expect(spread([load('n1', 30), load('n2', 31), load('n3', 12, 0, 0, 0, 'down')])).toBe(1);
    expect(spread([load('n1', 30), load('n4', 0, 0, 0, 0, 'joining')])).toBe(0);
  });
  it('joins by node with numeric-aware order', () => {
    expect(joinLoad(cur, next).map((r) => r.node)).toEqual(['n1', 'n2', 'n10']);
    expect(joinLoad(cur, [load('n4', 3)]).find((r) => r.node === 'n4')).toEqual({ node: 'n4', cur: null, next: load('n4', 3) });
  });
  it('imbalance level against the threshold', () => {
    expect(imbalanceLevel(0, 1)).toBe('balanced');
    expect(imbalanceLevel(1, 1)).toBe('within');
    expect(imbalanceLevel(2, 1)).toBe('over');
    expect(imbalanceLevel(1, 0)).toBe('over');
  });
});

describe('config form (human units)', () => {
  it('splits durations and throughput into the largest exact unit', () => {
    expect(splitDuration(600)).toEqual({ value: '10', unit: 'min' });
    expect(splitDuration(1800)).toEqual({ value: '30', unit: 'min' });
    expect(splitDuration(7200)).toEqual({ value: '2', unit: 'h' });
    expect(splitDuration(45)).toEqual({ value: '45', unit: 's' });
    expect(splitThroughput(50 * 1024 * 1024)).toEqual({ value: '50', unit: 'MB/s' });
    expect(splitThroughput(2 * 1024 ** 3)).toEqual({ value: '2', unit: 'GB/s' });
    expect(splitThroughput(1536 * 1024)).toEqual({ value: '1.5', unit: 'MB/s' });
    expect(splitThroughput(512 * 1024)).toEqual({ value: '512', unit: 'KB/s' });
    expect(splitThroughput(0)).toEqual({ value: '', unit: 'MB/s' });
  });
  it('round-trips the defaults and special values', () => {
    expect(draftToConfig(configToDraft(defaults))).toEqual(defaults);
    const odd: RebalanceConfig = { mode: 'repair', max_concurrent_moves: 8, imbalance_threshold: 0, replace_after_secs: 0, throttle_bytes_per_sec: 20 * 1024 * 1024, move_timeout_secs: 5400 };
    const d = configToDraft(odd);
    expect(d.replace_never).toBe(true);
    expect(d.throttle_unlimited).toBe(false);
    expect(d.move_timeout).toBe('90');
    expect(d.move_timeout_unit).toBe('min');
    expect(draftToConfig(d)).toEqual(odd);
    expect(configsEqual(draftToConfig(d), odd)).toBe(true);
  });
  it('converts edited human values to wire units', () => {
    const d = { ...configToDraft(defaults), replace_after: '1.5', replace_after_unit: 'h' as const, throttle_unlimited: false, throttle: '40', throttle_unit: 'MB/s' as const };
    expect(validateRebalanceDraft(d)).toEqual({});
    const c = draftToConfig(d);
    expect(c.replace_after_secs).toBe(5400);
    expect(c.throttle_bytes_per_sec).toBe(40 * 1024 * 1024);
  });
  it('validates', () => {
    const base = configToDraft(defaults);
    expect(validateRebalanceDraft({ ...base, max_concurrent_moves: '0' }).max_concurrent_moves).toBeTruthy();
    expect(validateRebalanceDraft({ ...base, max_concurrent_moves: '2.5' }).max_concurrent_moves).toBeTruthy();
    expect(validateRebalanceDraft({ ...base, imbalance_threshold: '-1' }).imbalance_threshold).toBeTruthy();
    expect(validateRebalanceDraft({ ...base, replace_after: '30', replace_after_unit: 's' }).replace_after).toBeTruthy();
    expect(validateRebalanceDraft({ ...base, replace_never: true, replace_after: 'x' }).replace_after).toBeUndefined();
    expect(validateRebalanceDraft({ ...base, throttle_unlimited: false, throttle: '0' }).throttle).toBeTruthy();
    expect(validateRebalanceDraft({ ...base, move_timeout: '0.5', move_timeout_unit: 'min' }).move_timeout).toBeTruthy();
    expect(validateRebalanceDraft({ ...base, move_timeout: '1.01', move_timeout_unit: 'min' }).move_timeout).toBeTruthy();
  });
  it('formats', () => {
    expect(fmtSecsHuman(600)).toBe('10 min');
    expect(fmtSecsHuman(5400)).toBe('1 h 30 min');
    expect(fmtSecsHuman(0, 'never')).toBe('never');
    expect(fmtThrottle(0)).toBe('unlimited');
    expect(fmtThrottle(50 * 1024 * 1024)).toBe('50 MB/s');
  });
});

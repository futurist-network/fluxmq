/**
 * Pure shaping of the v0.4 rebalancing contract (`RebalanceStatus`, crates/fluxmq-proto/src/cluster.rs)
 * for the Rebalancing panel on the Nodes page: labels, polling cadence, move diffs, catch-up
 * progress, load scales and the config form (human units ⇄ wire units).
 */
import type { EventKind, FluxEvent, MoveReason, MoveState, PartitionMove, RebalanceConfig, RebalanceMode, RebalanceNodeLoad, RebalanceState, RebalanceStatus } from '../api/types';
import { fmtBytes } from './format';
import { t } from '../i18n';
import { localized } from './labels';

// All label maps translate on access (current console language).
const MODES = ['off', 'repair', 'full'] as const satisfies readonly RebalanceMode[];
const STATES = ['idle', 'running', 'paused', 'cancelling'] as const satisfies readonly RebalanceState[];
const MOVE_STATES = ['pending', 'catching_up', 'finalizing', 'done', 'rolled_back', 'cancelled'] as const satisfies readonly MoveState[];
const MOVE_REASONS = ['repair', 'balance', 'manual'] as const satisfies readonly MoveReason[];

export const REBALANCE_MODE_LABEL: Readonly<Record<RebalanceMode, string>> = localized(MODES, 'rebalance.mode.');
export const REBALANCE_MODE_HINT: Readonly<Record<RebalanceMode, string>> = localized(MODES, 'rebalance.modeHint.');
export const REBALANCE_STATE_LABEL: Readonly<Record<RebalanceState, string>> = localized(STATES, 'rebalance.state.');
export const MOVE_STATE_LABEL: Readonly<Record<MoveState, string>> = localized(MOVE_STATES, 'rebalance.moveState.');
export const MOVE_REASON_LABEL: Readonly<Record<MoveReason, string>> = localized(MOVE_REASONS, 'rebalance.reason.');
export const MOVE_REASON_HINT: Readonly<Record<MoveReason, string>> = localized(MOVE_REASONS, 'rebalance.reasonHint.');

/** Fast while a plan is moving data, slow otherwise (events trigger extra refreshes). */
export function pollIntervalMs(state: RebalanceState | null | undefined): number {
  return state === 'running' || state === 'cancelling' ? 2000 : 10_000;
}

const REBALANCE_EVENTS: ReadonlySet<EventKind> = new Set<EventKind>(['rebalance_started', 'rebalance_completed', 'rebalance_failed', 'partition_moved', 'replica_repair_started']);

export function isRebalanceEvent(kind: EventKind): boolean {
  return REBALANCE_EVENTS.has(kind);
}

/** Id of the newest rebalance-related event (the feed is newest first), or null. A change means "refresh now". */
export function latestRebalanceEventId(events: readonly FluxEvent[] | undefined): number | null {
  if (!events) return null;
  let best: number | null = null;
  for (const e of events) if (isRebalanceEvent(e.kind) && (best == null || e.id > best)) best = e.id;
  return best;
}

export function isActiveMove(m: Pick<PartitionMove, 'state'>): boolean {
  return m.state === 'pending' || m.state === 'catching_up' || m.state === 'finalizing';
}

export interface MoveDiff {
  /** In `to` but not `from` — replicas being created (they catch up from the leader). */
  added: string[];
  /** In `from` but not `to` — replicas deleted once the move commits. */
  removed: string[];
  kept: string[];
  /** Preferred leader (replica 0) changes. */
  leaderChange: boolean;
}

export function moveDiff(m: Pick<PartitionMove, 'from_replicas' | 'to_replicas'>): MoveDiff {
  const from = new Set(m.from_replicas);
  const to = new Set(m.to_replicas);
  return {
    added: m.to_replicas.filter((r) => !from.has(r)),
    removed: m.from_replicas.filter((r) => !to.has(r)),
    kept: m.to_replicas.filter((r) => from.has(r)),
    leaderChange: m.from_replicas[0] !== m.to_replicas[0],
  };
}

export const moveKey = (m: Pick<PartitionMove, 'channel' | 'partition'>) => `${m.channel}/${m.partition}`;

/**
 * Fallback baseline for servers that don't report `initial_lag`: the highest lag seen for this
 * attempt (keyed by start time). Entries for moves no longer catching up are dropped.
 */
export function trackPeakLag(prev: Readonly<Record<string, number>>, moves: readonly PartitionMove[]): Record<string, number> {
  const next: Record<string, number> = {};
  for (const m of moves) {
    if (m.state !== 'catching_up' || m.lag == null) continue;
    const k = `${moveKey(m)}@${m.started_at_ms ?? 0}`;
    next[k] = Math.max(prev[k] ?? 0, m.lag);
  }
  return next;
}

export function peakLagFor(peaks: Readonly<Record<string, number>>, m: PartitionMove): number | null {
  return peaks[`${moveKey(m)}@${m.started_at_ms ?? 0}`] ?? null;
}

/**
 * 0..1 catch-up progress = 1 − lag / initial_lag (falling back to the highest observed lag when
 * `initial_lag` is null), or null when there is nothing meaningful to show.
 */
export function catchUpProgress(m: PartitionMove, peak: number | null): number | null {
  switch (m.state) {
    case 'pending':
      return 0;
    case 'finalizing':
    case 'done':
      return 1;
    case 'rolled_back':
    case 'cancelled':
      return null;
    case 'catching_up': {
      if (m.lag == null) return null;
      if (m.lag === 0) return 1;
      const base = Math.max(m.initial_lag ?? peak ?? 0, m.lag);
      return base > 0 ? Math.min(1, Math.max(0, 1 - m.lag / base)) : null;
    }
  }
}

export interface PlanSummary {
  total: number;
  pending: number;
  /** catching_up + finalizing */
  inFlight: number;
  done: number;
  rolledBack: number;
  cancelled: number;
  /** Settled (done, rolled back or cancelled) / total. */
  fraction: number;
}

export function planSummary(moves: readonly PartitionMove[]): PlanSummary {
  const s = { total: moves.length, pending: 0, inFlight: 0, done: 0, rolledBack: 0, cancelled: 0, fraction: 0 };
  for (const m of moves) {
    if (m.state === 'pending') s.pending++;
    else if (m.state === 'done') s.done++;
    else if (m.state === 'rolled_back') s.rolledBack++;
    else if (m.state === 'cancelled') s.cancelled++;
    else s.inFlight++;
  }
  s.fraction = s.total ? (s.done + s.rolledBack + s.cancelled) / s.total : 0;
  return s;
}

/** Elapsed time of a move (running moves count to `now`), or null before it started. */
export function moveElapsedMs(m: Pick<PartitionMove, 'started_at_ms' | 'finished_at_ms'>, now: number): number | null {
  if (m.started_at_ms == null) return null;
  return Math.max(0, (m.finished_at_ms ?? now) - m.started_at_ms);
}

/** Moves sorted for display: in flight, pending, rolled back, done, cancelled (stable within a group). */
export function sortMoves(moves: readonly PartitionMove[]): PartitionMove[] {
  const rank: Record<MoveState, number> = { catching_up: 0, finalizing: 0, pending: 1, rolled_back: 2, done: 3, cancelled: 4 };
  return moves.map((m, i) => ({ m, i })).sort((a, b) => rank[a.m.state] - rank[b.m.state] || a.i - b.i).map((x) => x.m);
}

export type LoadMetric = 'replicas' | 'leaders' | 'preferred_leaders' | 'storage_bytes';
export const LOAD_METRICS: readonly LoadMetric[] = ['replicas', 'leaders', 'preferred_leaders', 'storage_bytes'];
export const LOAD_METRIC_LABEL: Readonly<Record<LoadMetric, string>> = localized(LOAD_METRICS, 'rebalance.metric.');

/** Shared max per metric over current and projected load, so bars of both are comparable. */
export function loadScale(...lists: readonly (readonly RebalanceNodeLoad[])[]): Record<LoadMetric, number> {
  const out: Record<LoadMetric, number> = { replicas: 0, leaders: 0, preferred_leaders: 0, storage_bytes: 0 };
  for (const list of lists) for (const l of list) for (const k of LOAD_METRICS) out[k] = Math.max(out[k], l[k]);
  return out;
}

/** `max - min` of a metric over `up` nodes (the contract's `imbalance` uses replicas). */
export function spread(load: readonly RebalanceNodeLoad[], metric: LoadMetric = 'replicas'): number {
  const up = load.filter((l) => l.status === 'up');
  if (up.length === 0) return 0;
  const v = up.map((l) => l[metric]);
  return Math.max(...v) - Math.min(...v);
}

export function fmtLoadValue(metric: LoadMetric, v: number): string {
  return metric === 'storage_bytes' ? fmtBytes(v) : String(v);
}

/** Current and projected load joined by node (nodes in either list; sorted by id, numeric-aware). */
export function joinLoad(current: readonly RebalanceNodeLoad[], projected: readonly RebalanceNodeLoad[]): { node: string; cur: RebalanceNodeLoad | null; next: RebalanceNodeLoad | null }[] {
  const ids = [...new Set([...current.map((l) => l.node), ...projected.map((l) => l.node)])].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  return ids.map((node) => ({ node, cur: current.find((l) => l.node === node) ?? null, next: projected.find((l) => l.node === node) ?? null }));
}

export type ImbalanceLevel = 'balanced' | 'within' | 'over';

/** `balanced` = 0; `within` = >0 but ≤ threshold; `over` = above the threshold (balancing triggers in full mode). */
export function imbalanceLevel(imbalance: number, threshold: number): ImbalanceLevel {
  if (imbalance <= 0) return 'balanced';
  return imbalance > threshold ? 'over' : 'within';
}

/** True when a plan exists that hasn't finished (running, paused or cancelling). */
export function planInProgress(s: Pick<RebalanceStatus, 'state'>): boolean {
  return s.state !== 'idle';
}

// ───────────────────────────── config form ─────────────────────────────

export type DurationUnit = 's' | 'min' | 'h';
export const DURATION_UNIT_SECS: Record<DurationUnit, number> = { s: 1, min: 60, h: 3600 };
export type ThroughputUnit = 'KB/s' | 'MB/s' | 'GB/s';
export const THROUGHPUT_UNIT_BYTES: Record<ThroughputUnit, number> = { 'KB/s': 1024, 'MB/s': 1024 ** 2, 'GB/s': 1024 ** 3 };

/** Largest unit that divides `secs` evenly (600 → 10 min, 1800 → 30 min, 7200 → 2 h, 45 → 45 s). */
export function splitDuration(secs: number): { value: string; unit: DurationUnit } {
  for (const u of ['h', 'min'] as const) if (secs > 0 && secs % DURATION_UNIT_SECS[u] === 0) return { value: String(secs / DURATION_UNIT_SECS[u]), unit: u };
  return { value: String(secs), unit: 's' };
}

/** Largest unit that represents `bps` exactly (or with ≤ 2 decimals); 0 = unlimited → empty value in MB/s. */
export function splitThroughput(bps: number): { value: string; unit: ThroughputUnit } {
  if (bps <= 0) return { value: '', unit: 'MB/s' };
  for (const u of ['GB/s', 'MB/s'] as const) {
    const v = bps / THROUGHPUT_UNIT_BYTES[u];
    if (v >= 1 && Math.abs(Math.round(v * 100) - v * 100) < 1e-6) return { value: String(Math.round(v * 100) / 100), unit: u };
  }
  return { value: String(Math.round((bps / 1024) * 100) / 100), unit: 'KB/s' };
}

/** Display label of a form duration unit ("s" / "min" / "h"; ru "с" / "мин" / "ч"). */
export function durationUnitLabel(u: DurationUnit): string {
  return t(`rebalance.unit.${u}`);
}

/** Display label of a throughput unit ("MB/s"; ru "МБ/с"). */
export function throughputUnitLabel(u: ThroughputUnit): string {
  return t(u === 'KB/s' ? 'rebalance.unit.kbps' : u === 'MB/s' ? 'rebalance.unit.mbps' : 'rebalance.unit.gbps');
}

/** "10 min", "1 h 30 min", "45 s"; `zero` for 0. */
export function fmtSecsHuman(secs: number, zero?: string): string {
  if (secs <= 0) return zero ?? `0 ${durationUnitLabel('s')}`;
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  const parts: string[] = [];
  if (h) parts.push(`${h} ${durationUnitLabel('h')}`);
  if (m) parts.push(`${m} ${durationUnitLabel('min')}`);
  if (s) parts.push(`${s} ${durationUnitLabel('s')}`);
  return parts.join(' ');
}

export function fmtThrottle(bps: number): string {
  return bps <= 0 ? t('rebalance.unlimited') : `${fmtBytes(bps)}${t('rebalance.unit.perSec')}`;
}

export interface RebalanceDraft {
  mode: RebalanceMode;
  max_concurrent_moves: string;
  imbalance_threshold: string;
  replace_never: boolean;
  replace_after: string;
  replace_after_unit: DurationUnit;
  throttle_unlimited: boolean;
  throttle: string;
  throttle_unit: ThroughputUnit;
  move_timeout: string;
  move_timeout_unit: DurationUnit;
}

export function configToDraft(c: RebalanceConfig): RebalanceDraft {
  const ra = splitDuration(c.replace_after_secs);
  const th = splitThroughput(c.throttle_bytes_per_sec);
  const mt = splitDuration(c.move_timeout_secs);
  return {
    mode: c.mode,
    max_concurrent_moves: String(c.max_concurrent_moves),
    imbalance_threshold: String(c.imbalance_threshold),
    replace_never: c.replace_after_secs === 0,
    replace_after: c.replace_after_secs === 0 ? '10' : ra.value,
    replace_after_unit: c.replace_after_secs === 0 ? 'min' : ra.unit,
    throttle_unlimited: c.throttle_bytes_per_sec === 0,
    throttle: th.value || '50',
    throttle_unit: th.unit,
    move_timeout: mt.value,
    move_timeout_unit: mt.unit,
  };
}

const INT = /^\d+$/;
const NUM = /^\d+(\.\d+)?$/;

export type RebalanceDraftErrors = Partial<Record<'max_concurrent_moves' | 'imbalance_threshold' | 'replace_after' | 'throttle' | 'move_timeout', string>>;

export function validateRebalanceDraft(d: RebalanceDraft): RebalanceDraftErrors {
  const e: RebalanceDraftErrors = {};
  const mc = d.max_concurrent_moves.trim();
  if (!INT.test(mc) || Number(mc) < 1 || Number(mc) > 64) e.max_concurrent_moves = t('rebalance.err.maxMoves');
  const th = d.imbalance_threshold.trim();
  if (!INT.test(th) || Number(th) > 1000) e.imbalance_threshold = t('rebalance.err.threshold');
  if (!d.replace_never) {
    const v = d.replace_after.trim();
    if (!NUM.test(v) || Number(v) * DURATION_UNIT_SECS[d.replace_after_unit] < 60) e.replace_after = t('rebalance.err.replaceAfter');
    else if (!Number.isInteger(Number(v) * DURATION_UNIT_SECS[d.replace_after_unit])) e.replace_after = t('rebalance.err.wholeSeconds');
  }
  if (!d.throttle_unlimited) {
    const v = d.throttle.trim();
    if (!NUM.test(v) || Number(v) <= 0) e.throttle = t('rebalance.err.throttle');
    else if (Number(v) * THROUGHPUT_UNIT_BYTES[d.throttle_unit] < 1024) e.throttle = t('rebalance.err.throttleMin');
  }
  const mt = d.move_timeout.trim();
  if (!NUM.test(mt) || Number(mt) * DURATION_UNIT_SECS[d.move_timeout_unit] < 60) e.move_timeout = t('rebalance.err.moveTimeout');
  else if (!Number.isInteger(Number(mt) * DURATION_UNIT_SECS[d.move_timeout_unit])) e.move_timeout = t('rebalance.err.wholeSeconds');
  return e;
}

/** Wire config from a (valid) draft. */
export function draftToConfig(d: RebalanceDraft): RebalanceConfig {
  return {
    mode: d.mode,
    max_concurrent_moves: Number(d.max_concurrent_moves.trim()),
    imbalance_threshold: Number(d.imbalance_threshold.trim()),
    replace_after_secs: d.replace_never ? 0 : Math.round(Number(d.replace_after.trim()) * DURATION_UNIT_SECS[d.replace_after_unit]),
    throttle_bytes_per_sec: d.throttle_unlimited ? 0 : Math.round(Number(d.throttle.trim()) * THROUGHPUT_UNIT_BYTES[d.throttle_unit]),
    move_timeout_secs: Math.round(Number(d.move_timeout.trim()) * DURATION_UNIT_SECS[d.move_timeout_unit]),
  };
}

export function configsEqual(a: RebalanceConfig, b: RebalanceConfig): boolean {
  return (
    a.mode === b.mode &&
    a.max_concurrent_moves === b.max_concurrent_moves &&
    a.imbalance_threshold === b.imbalance_threshold &&
    a.replace_after_secs === b.replace_after_secs &&
    a.throttle_bytes_per_sec === b.throttle_bytes_per_sec &&
    a.move_timeout_secs === b.move_timeout_secs
  );
}


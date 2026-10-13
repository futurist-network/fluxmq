/**
 * Pure selectors over telemetry topic state. Pure functions so they are unit-testable and
 * can be memoised by callers; components combine them with `useTopicSelect`.
 */
import type {
  Alert,
  AlertsState,
  ChannelMode,
  ChannelSummary,
  ChannelsSummary,
  ConsumersState,
  GroupState,
  Health,
  InstanceState,
} from '../api/types';
import { HEALTH_RANK } from '../api/types';

export function worstHealth(list: Iterable<Health>): Health {
  let worst: Health = 'healthy';
  for (const h of list) if (HEALTH_RANK[h] > HEALTH_RANK[worst]) worst = h;
  return worst;
}

export interface ChannelFilter {
  query?: string;
  mode?: ChannelMode | 'all';
}

/** Problems first (critical → warning), then by traffic. */
export function sortedChannels(summary: ChannelsSummary | undefined, filter: ChannelFilter = {}): ChannelSummary[] {
  if (!summary) return [];
  const q = filter.query?.trim().toLowerCase() ?? '';
  return Object.values(summary)
    .filter((c) => (filter.mode && filter.mode !== 'all' ? c.mode === filter.mode : true))
    .filter((c) => (q ? c.name.toLowerCase().includes(q) : true))
    .sort((a, b) => HEALTH_RANK[b.health] - HEALTH_RANK[a.health] || b.ingress_rate - a.ingress_rate || a.name.localeCompare(b.name));
}

export function channelNames(summary: ChannelsSummary | undefined): string[] {
  return summary ? Object.keys(summary).sort() : [];
}

/** Open alerts: critical first, then newest. */
export function openAlerts(alerts: AlertsState | undefined): Alert[] {
  if (!alerts) return [];
  return Object.values(alerts).sort((a, b) => HEALTH_RANK[b.severity] - HEALTH_RANK[a.severity] || b.since_ms - a.since_ms);
}

export interface AlertCounts {
  total: number;
  critical: number;
  warning: number;
}

export function alertCounts(alerts: AlertsState | undefined): AlertCounts {
  const out = { total: 0, critical: 0, warning: 0 };
  if (!alerts) return out;
  for (const a of Object.values(alerts)) {
    out.total++;
    if (a.severity === 'critical') out.critical++;
    else if (a.severity === 'warning') out.warning++;
  }
  return out;
}

export interface GroupRef {
  channel: string;
  group: GroupState;
  /** Rolled-up status (instances and open alerts included), see `effectiveGroupHealth`. */
  health: Health;
}

/** Flatten `channel → ConsumersState` into a problems-first list of groups. */
export function flattenGroups(byChannel: Record<string, ConsumersState | undefined>, alerts?: AlertsState): GroupRef[] {
  const out: GroupRef[] = [];
  for (const [channel, groups] of Object.entries(byChannel)) {
    if (!groups) continue;
    for (const g of Object.values(groups)) out.push({ channel, group: g, health: effectiveGroupHealth(g, channel, alerts) });
  }
  return out.sort(
    (a, b) =>
      HEALTH_RANK[b.health] - HEALTH_RANK[a.health] ||
      b.group.lag - a.group.lag ||
      `${a.channel}/${a.group.group}`.localeCompare(`${b.channel}/${b.group.group}`),
  );
}

/** Instances sorted problems-first, then by id. */
export function sortedInstances(group: GroupState | undefined): InstanceState[] {
  if (!group) return [];
  return Object.values(group.instances).sort((a, b) => HEALTH_RANK[b.health] - HEALTH_RANK[a.health] || a.id.localeCompare(b.id));
}

/** Median of instance processing p99 — the baseline the slow worker is compared against. */
export function medianProcessingP99(group: GroupState | undefined): number | null {
  const xs = sortedInstances(group)
    .map((i) => i.processing_p99_ms)
    .sort((a, b) => a - b);
  if (!xs.length) return null;
  const mid = Math.floor(xs.length / 2);
  return xs.length % 2 ? xs[mid]! : (xs[mid - 1]! + xs[mid]!) / 2;
}

export function channelCountsByMode(summary: ChannelsSummary | undefined): Record<ChannelMode, number> {
  const out: Record<ChannelMode, number> = { stream: 0, queue: 0, keyed: 0, priority: 0 };
  if (summary) for (const c of Object.values(summary)) out[c.mode]++;
  return out;
}

/** Worst open-alert severity per alert kind (drives KPI colouring: colour only real problems). */
export function alertSeverityByKind(alerts: AlertsState | undefined): Partial<Record<Alert['kind'], Health>> {
  const out: Partial<Record<Alert['kind'], Health>> = {};
  if (!alerts) return out;
  for (const a of Object.values(alerts)) {
    const prev = out[a.kind];
    if (!prev || HEALTH_RANK[a.severity] > HEALTH_RANK[prev]) out[a.kind] = a.severity;
  }
  return out;
}

// ---------------------------------------------------------------------------------------------
// Status roll-up and "why"
//
// One rule (computed by the broker, crates/fluxmq-telemetry/src/health.rs): instance → group →
// channel → cluster takes the worst status, and an open alert on an object makes it at least
// `warning`. The helpers below re-apply the same floor on the client so a console talking to an
// older broker (which didn't roll instances up) still shows and filters the same way; with a
// current broker they are no-ops.
// ---------------------------------------------------------------------------------------------

export interface HealthScope {
  channel?: string;
  group?: string;
  instance?: string;
}

/** Open alerts about an object (or anything inside it), worst first. No scope = the cluster. */
export function alertsFor(alerts: AlertsState | undefined, scope: HealthScope = {}): Alert[] {
  return openAlerts(alerts).filter((a) => {
    if (scope.channel != null && a.channel !== scope.channel) return false;
    if (scope.group != null && a.group !== scope.group) return false;
    if (scope.instance != null) return a.kind === 'slow_instance' && alertInstanceId(a) === scope.instance;
    return true;
  });
}

/** Instance id of a `slow_instance` alert (params, or the id `slow_instance:{ch}/{g}/{instance}`). */
export function alertInstanceId(a: Alert): string | null {
  if (a.kind !== 'slow_instance') return null;
  const p = a.params?.instance;
  if (typeof p === 'string') return p;
  const prefix = `slow_instance:${a.channel ?? ''}/${a.group ?? ''}/`;
  return a.id.startsWith(prefix) ? a.id.slice(prefix.length) : null;
}

const floor = (alerts: Alert[]): Health => worstHealth(alerts.map((a) => (a.severity === 'healthy' ? 'warning' : a.severity)));

export function effectiveInstanceHealth(i: InstanceState, channel: string, group: string, alerts: AlertsState | undefined): Health {
  return worstHealth([i.health, floor(alertsFor(alerts, { channel, group, instance: i.id }))]);
}

export function effectiveGroupHealth(g: GroupState, channel: string, alerts: AlertsState | undefined): Health {
  return worstHealth([g.health, ...Object.values(g.instances).map((i) => i.health), floor(alertsFor(alerts, { channel, group: g.group }))]);
}

export function effectiveChannelHealth(c: Pick<ChannelSummary, 'name' | 'health'>, alerts: AlertsState | undefined): Health {
  return worstHealth([c.health, floor(alertsFor(alerts, { channel: c.name }))]);
}

export function effectiveClusterHealth(h: Health, alerts: AlertsState | undefined): Health {
  return worstHealth([h, floor(openAlerts(alerts))]);
}

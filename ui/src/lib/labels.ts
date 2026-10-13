import type { AlertKind, ChannelMode, EventKind, Health } from '../api/types';
import { t, type TKey } from '../i18n';

/**
 * A read-only map whose values are translated on every access (current console language), so
 * call sites keep the `LABEL[kind]` shape. Components re-render on language change via `useT()`.
 */
export function localized<K extends string>(keys: readonly K[], prefix: string): Readonly<Record<K, string>> {
  const o = {} as Record<K, string>;
  for (const k of keys) Object.defineProperty(o, k, { enumerable: true, get: () => t(`${prefix}${k}` as TKey) });
  return o;
}

const MODES = ['stream', 'queue', 'keyed', 'priority'] as const satisfies readonly ChannelMode[];
const HEALTHS = ['healthy', 'warning', 'critical'] as const satisfies readonly Health[];
const ALERT_KINDS = [
  'lag_growth',
  'consumer_stalled',
  'no_consumers',
  'dlq_growth',
  'high_retry_rate',
  'slow_instance',
  'storage_nearly_full',
  'high_publish_latency',
  'node_down',
  'under_replicated_partitions',
  'offline_partitions',
  'tls_cert_expiring',
  'tls_reload_failed',
] as const satisfies readonly AlertKind[];
const EVENT_KINDS = [
  'channel_created',
  'channel_deleted',
  'channel_updated',
  'consumer_connected',
  'consumer_disconnected',
  'alert_opened',
  'alert_resolved',
  'dead_lettered',
  'dlq_retried',
  'dlq_discarded',
  'messages_replayed',
  'controller_changed',
  'node_down',
  'node_up',
  'rebalance_started',
  'rebalance_completed',
  'rebalance_failed',
  'partition_moved',
  'replica_repair_started',
  'tls_reload_failed',
  'node_decommission_started',
  'node_decommissioned',
  'node_decommission_failed',
  'node_forgotten',
] as const satisfies readonly EventKind[];

export const MODE_HINT: Readonly<Record<ChannelMode, string>> = localized(MODES, 'labels.modeHint.');
export const MODE_LABEL: Readonly<Record<ChannelMode, string>> = localized(MODES, 'labels.mode.');
export const HEALTH_LABEL: Readonly<Record<Health, string>> = localized(HEALTHS, 'labels.health.');
export const ALERT_KIND_LABEL: Readonly<Record<AlertKind, string>> = localized(ALERT_KINDS, 'labels.alert.');
export const EVENT_KIND_LABEL: Readonly<Record<EventKind, string>> = localized(EVENT_KINDS, 'labels.event.');

export const channelPath = (name: string, tab?: string) => `/channels/${encodeURIComponent(name)}${tab ? `/${tab}` : ''}`;

/** Cluster instance ids are `"{node}/{id}"` (v0.2); single-node ids have no node prefix. */
export function splitInstanceId(id: string): { node: string | null; local: string } {
  const i = id.indexOf('/');
  return i > 0 ? { node: id.slice(0, i), local: id.slice(i + 1) } : { node: null, local: id };
}

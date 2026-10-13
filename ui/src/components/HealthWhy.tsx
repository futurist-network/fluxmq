import { TOPICS, type AlertsState, type Health, type InstanceState, type GroupState } from '../api/types';
import { useTopicData } from '../telemetry';
import { alertsFor, type HealthScope } from '../store/selectors';
import { alertText } from '../lib/alertText';
import { HEALTH_LABEL } from '../lib/labels';
import { instanceReason } from './consumers/reason';
import { t } from '../i18n';
import type { Alert } from '../api/types';

const MAX_LINES = 3;

/** One reason line, prefixed with where it is when the tooltip covers a wider scope. */
function line(a: Alert, scope: HealthScope): string {
  const short = alertText(a).short;
  if (a.kind === 'slow_instance' && scope.group != null) return short;
  if (scope.group != null) return short;
  if (scope.channel != null) return a.group && a.kind !== 'slow_instance' ? `${a.group}: ${short}` : short;
  const where = a.channel ? `${a.channel}${a.group && a.kind !== 'slow_instance' ? `/${a.group}` : ''}` : null;
  return where ? `${where} · ${short}` : short;
}

/** Tooltip text explaining a status ("Почему «Внимание»: • worker-03#7: p99 в 39× …"). */
export function healthWhy(health: Health, alerts: AlertsState | undefined, scope: HealthScope, inst?: { i: InstanceState; g: GroupState }): string | undefined {
  if (health === 'healthy') return undefined;
  const list = alertsFor(alerts, scope);
  const lines = list.slice(0, MAX_LINES).map((a) => `• ${line(a, scope)}`);
  if (!lines.length && inst) {
    const r = instanceReason({ ...inst.i, health }, inst.g);
    if (r) lines.push(`• ${r.text}`);
  }
  if (!lines.length) lines.push(`• ${t('alerts.why.pending')}`);
  if (list.length > MAX_LINES) lines.push(t('alerts.why.more', { count: list.length - MAX_LINES }));
  return `${t('alerts.why.title', { health: HEALTH_LABEL[health] })}\n${lines.join('\n')}`;
}

/** Live open alerts (the topic is subscribed app-wide by the sidebar badge). */
export function useAlerts(): AlertsState | undefined {
  return useTopicData<AlertsState>(TOPICS.ALERTS);
}

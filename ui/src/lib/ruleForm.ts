/** Notification rule editor: draft ⇄ `NotificationRule` / requests, presets, summary text. */
import type { AlertKind, CreateRuleRequest, EventKind, Health, NotificationRule, UpdateRuleRequest } from '../api/types';
import { t } from '../i18n';
import { fmtBackoff } from './format';
import { globMatch } from './rbac';

export interface RuleDraft {
  name: string;
  enabled: boolean;
  notifier_ids: string[];
  min_severity: Exclude<Health, 'healthy'>;
  alert_kinds: AlertKind[];
  event_kinds: EventKind[];
  channels: string[];
  group_wait_secs: number;
  repeat_interval_secs: number;
  send_resolved: boolean;
}

export const DEFAULT_GROUP_WAIT = 30;
export const DEFAULT_REPEAT = 14_400;

/** Preset labels are getters so they read in the current language ("10 s" / "10 с"). */
const preset = (value: number, label: () => string) => ({
  value,
  get label() {
    return label();
  },
});

export const GROUP_WAIT_PRESETS: { value: number; label: string }[] = [
  preset(0, () => t('notifications.rule.presetNone')),
  ...[10, 30, 60, 300].map((v) => preset(v, () => fmtBackoff(v * 1000))),
];

export const REPEAT_PRESETS: { value: number; label: string }[] = [
  preset(0, () => t('notifications.rule.presetNever')),
  ...[3_600, 14_400, 43_200, 86_400].map((v) => preset(v, () => fmtBackoff(v * 1000))),
];

export function emptyRuleDraft(notifierIds: string[] = []): RuleDraft {
  return {
    name: '',
    enabled: true,
    notifier_ids: notifierIds,
    min_severity: 'warning',
    alert_kinds: [],
    event_kinds: [],
    channels: [],
    group_wait_secs: DEFAULT_GROUP_WAIT,
    repeat_interval_secs: DEFAULT_REPEAT,
    send_resolved: true,
  };
}

export function ruleToDraft(r: NotificationRule): RuleDraft {
  return {
    name: r.name,
    enabled: r.enabled,
    notifier_ids: [...r.notifier_ids],
    min_severity: r.match.min_severity === 'critical' ? 'critical' : 'warning',
    alert_kinds: [...r.match.alert_kinds],
    event_kinds: [...r.match.event_kinds],
    channels: [...r.match.channels],
    group_wait_secs: r.group_wait_secs,
    repeat_interval_secs: r.repeat_interval_secs,
    send_resolved: r.send_resolved,
  };
}

const matchOf = (d: RuleDraft) => ({ min_severity: d.min_severity, alert_kinds: d.alert_kinds, event_kinds: d.event_kinds, channels: d.channels });

export function draftToCreateRule(d: RuleDraft): CreateRuleRequest {
  return {
    name: d.name.trim(),
    enabled: d.enabled,
    notifier_ids: d.notifier_ids,
    match: matchOf(d),
    group_wait_secs: d.group_wait_secs,
    repeat_interval_secs: d.repeat_interval_secs,
    send_resolved: d.send_resolved,
  };
}

/** Only the fields that differ from `base` (the PATCH stays minimal and readable in the audit log). */
export function draftToUpdateRule(d: RuleDraft, base: NotificationRule): UpdateRuleRequest {
  const out: UpdateRuleRequest = {};
  const b = ruleToDraft(base);
  if (d.name.trim() !== b.name) out.name = d.name.trim();
  if (d.enabled !== b.enabled) out.enabled = d.enabled;
  if (JSON.stringify(d.notifier_ids) !== JSON.stringify(b.notifier_ids)) out.notifier_ids = d.notifier_ids;
  if (JSON.stringify(matchOf(d)) !== JSON.stringify(matchOf(b))) out.match = matchOf(d);
  if (d.group_wait_secs !== b.group_wait_secs) out.group_wait_secs = d.group_wait_secs;
  if (d.repeat_interval_secs !== b.repeat_interval_secs) out.repeat_interval_secs = d.repeat_interval_secs;
  if (d.send_resolved !== b.send_resolved) out.send_resolved = d.send_resolved;
  return out;
}

export type RuleErrors = Partial<Record<'name' | 'notifier_ids' | 'channels' | 'group_wait_secs', string>>;

export function validateRuleDraft(d: RuleDraft, knownNotifiers: string[]): RuleErrors {
  const e: RuleErrors = {};
  if (!d.name.trim()) e.name = t('notifications.rule.errName');
  if (!d.notifier_ids.length) e.notifier_ids = t('notifications.rule.errNotifiers');
  else if (d.notifier_ids.some((id) => !knownNotifiers.includes(id))) e.notifier_ids = t('notifications.rule.errNotifierGone');
  if (!Number.isInteger(d.group_wait_secs) || d.group_wait_secs < 0 || d.group_wait_secs > 3600) e.group_wait_secs = t('notifications.rule.errGroupWait');
  return e;
}

/** Channels a set of globs matches today. */
export function matchedChannels(globs: string[], channels: string[]): string[] {
  if (!globs.length) return channels;
  return channels.filter((c) => globs.some((g) => globMatch(g, c)));
}

export function fmtInterval(secs: number): string {
  if (secs === 0) return t('notifications.rule.never');
  // "45 s", "5 min", "4 h" in the current language ("4 ч").
  return fmtBackoff(secs * 1000);
}

/** "Critical alerts (DLQ growth, Lag growth) + 2 event kinds on orders.*" */
export function ruleSummary(d: Pick<RuleDraft, 'min_severity' | 'alert_kinds' | 'event_kinds' | 'channels'>, labelAlert: (k: AlertKind) => string): string {
  const sev = t(d.min_severity === 'critical' ? 'notifications.rule.sevCritical' : 'notifications.rule.sevWarning');
  const kinds = d.alert_kinds.length ? ` (${d.alert_kinds.slice(0, 2).map(labelAlert).join(', ')}${d.alert_kinds.length > 2 ? ` +${d.alert_kinds.length - 2}` : ''})` : '';
  const ev = d.event_kinds.length ? t('notifications.rule.eventKinds', { count: d.event_kinds.length }) : '';
  const ch = d.channels.length ? t('notifications.rule.on', { channels: d.channels.join(', ') }) : '';
  return `${sev}${kinds}${ev}${ch}`;
}

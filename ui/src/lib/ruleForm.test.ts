import { describe, expect, it } from 'vitest';
import type { NotificationRule } from '../api/types';
import { draftToCreateRule, draftToUpdateRule, emptyRuleDraft, matchedChannels, ruleToDraft, validateRuleDraft } from './ruleForm';

const rule: NotificationRule = {
  id: 'r1',
  name: 'Critical → on-call',
  enabled: true,
  notifier_ids: ['n1'],
  match: { min_severity: 'critical', alert_kinds: ['dlq_growth'], event_kinds: [], channels: ['orders.*'] },
  group_wait_secs: 30,
  repeat_interval_secs: 14400,
  send_resolved: true,
  created_at_ms: 0,
  updated_at_ms: 0,
};

describe('rule editor ⇄ request', () => {
  it('serializes the matcher under `match`', () => {
    const req = draftToCreateRule({ ...emptyRuleDraft(['n1']), name: ' R ' });
    expect(req).toEqual({
      name: 'R',
      enabled: true,
      notifier_ids: ['n1'],
      match: { min_severity: 'warning', alert_kinds: [], event_kinds: [], channels: [] },
      group_wait_secs: 30,
      repeat_interval_secs: 14400,
      send_resolved: true,
    });
    expect(JSON.parse(JSON.stringify(req))).toHaveProperty('match.min_severity', 'warning');
  });
  it('round-trips a stored rule and sends a minimal PATCH', () => {
    const d = ruleToDraft(rule);
    expect(draftToUpdateRule(d, rule)).toEqual({});
    expect(draftToUpdateRule({ ...d, repeat_interval_secs: 0 }, rule)).toEqual({ repeat_interval_secs: 0 });
    expect(draftToUpdateRule({ ...d, event_kinds: ['node_down'] }, rule)).toEqual({ match: { ...rule.match, event_kinds: ['node_down'] } });
  });
  it('validates notifiers and name', () => {
    expect(validateRuleDraft(emptyRuleDraft(), ['n1'])).toMatchObject({ name: expect.any(String), notifier_ids: expect.any(String) });
    expect(validateRuleDraft({ ...emptyRuleDraft(['gone']), name: 'x' }, ['n1']).notifier_ids).toMatch(/no longer exists/);
  });
  it('counts matched channels (empty = all)', () => {
    const chans = ['orders.events', 'orders.priority', 'email'];
    expect(matchedChannels(['orders.*'], chans)).toEqual(['orders.events', 'orders.priority']);
    expect(matchedChannels([], chans)).toHaveLength(3);
  });
});

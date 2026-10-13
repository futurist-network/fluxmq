import { describe, expect, it } from 'vitest';
import { needFor } from './auth';
import { gateReason, roleAllows } from '../src/lib/rbac';

const seg = (path: string) => path.split('/').filter(Boolean);

describe('v0.3 role gating (design §1-3: GET = read, mutations + test = admin)', () => {
  it.each([
    ['GET', '/api/notify/notifiers', 'read'],
    ['GET', '/api/notify/history', 'read'],
    ['GET', '/api/events/feed', 'read'],
    ['GET', '/api/webhooks/stats', 'read'],
    ['GET', '/api/backups/status', 'read'],
    ['POST', '/api/notify/notifiers', 'admin'],
    ['POST', '/api/notify/notifiers/test', 'admin'],
    ['POST', '/api/notify/notifiers/nt_1/test', 'admin'],
    ['PATCH', '/api/notify/rules/r1', 'admin'],
    ['POST', '/api/webhooks/test', 'admin'],
    ['DELETE', '/api/webhooks/w1', 'admin'],
    ['PUT', '/api/backups/config', 'admin'],
    ['POST', '/api/backups/test-target', 'admin'],
    ['POST', '/api/backups/run', 'admin'],
    ['DELETE', '/api/backups/b1', 'admin'],
  ])('%s %s needs %s', (method, path, need) => {
    expect(needFor(method, seg(path))).toBe(need);
  });

  it('viewer and operator see v0.3 screens read-only; admin can mutate', () => {
    expect(roleAllows('viewer', 'admin')).toBe(false);
    expect(roleAllows('operator', 'admin')).toBe(false);
    expect(roleAllows('admin', 'admin')).toBe(true);
    expect(gateReason('operator', 'admin')).toMatch(/admin role/);
  });
});

describe('v0.4 role gating (GET = viewer, rebalance mutations = admin)', () => {
  it.each([
    ['GET', '/api/tls', 'read'],
    ['GET', '/api/cluster/rebalance', 'read'],
    ['POST', '/api/cluster/rebalance', 'admin'],
    ['PUT', '/api/cluster/rebalance/config', 'admin'],
    ['POST', '/api/cluster/rebalance/pause', 'admin'],
    ['POST', '/api/cluster/rebalance/resume', 'admin'],
    ['POST', '/api/cluster/rebalance/cancel', 'admin'],
  ])('%s %s needs %s', (method, path, need) => {
    expect(needFor(method, seg(path))).toBe(need);
  });
});

import { describe, expect, it } from 'vitest';
import { forbiddenText, gateReason, globMatch, roleAllows, validateGlob } from './rbac';
import { effectiveRole } from '../store/auth';

describe('role gating', () => {
  it('is cumulative viewer ⊂ operator ⊂ admin', () => {
    expect(roleAllows('viewer', 'operate')).toBe(false);
    expect(roleAllows('viewer', 'admin')).toBe(false);
    expect(roleAllows('operator', 'operate')).toBe(true);
    expect(roleAllows('operator', 'admin')).toBe(false);
    expect(roleAllows('admin', 'operate')).toBe(true);
    expect(roleAllows('admin', 'admin')).toBe(true);
  });

  it('allows while the role is unknown (the server decides)', () => {
    expect(roleAllows(null, 'admin')).toBe(true);
    expect(gateReason(null, 'admin')).toBeNull();
  });

  it('explains why a control is disabled', () => {
    expect(gateReason('viewer', 'operate')).toBe('Requires operator or admin role — you are signed in as viewer');
    expect(gateReason('operator', 'admin')).toBe('Requires admin role — you are signed in as operator');
    expect(gateReason('admin', 'admin')).toBeNull();
    expect(forbiddenText('viewer')).toBe('Your role (viewer) can’t do this');
  });

  it('derives the effective role from whoami', () => {
    const me = { mode: 'enforced' as const, name: 'grafana', token_id: 'g1', role: 'viewer' as const, scopes: { publish: [], consume: [] } };
    expect(effectiveRole({ me, status: 'ready' })).toBe('viewer');
    expect(effectiveRole({ me: null, status: 'unsupported' })).toBe('admin'); // v0.1 server: single admin token
    expect(effectiveRole({ me: null, status: 'loading' })).toBeNull();
  });

  it('matches scope globs like the broker', () => {
    expect(globMatch('orders.*', 'orders.events')).toBe(true);
    expect(globMatch('orders.*', 'payments.events')).toBe(false);
    expect(globMatch('*', 'anything')).toBe(true);
    expect(globMatch('email?', 'email1')).toBe(true);
    expect(globMatch('a.b', 'axb')).toBe(false);
    expect(validateGlob('orders.*')).toBeNull();
    expect(validateGlob('bad glob')).not.toBeNull();
  });
});

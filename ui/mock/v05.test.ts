import { describe, expect, it } from 'vitest';
import { Auth, needFor } from './auth';
import { ApiErr } from './sim';

const seg = (path: string) => path.split('/').filter(Boolean);

describe('mock v0.5 auth', () => {
  it('cert bindings are admin-only, including GET', () => {
    expect(needFor('GET', seg('/api/auth/cert-bindings'))).toBe('admin');
    expect(needFor('POST', seg('/api/auth/cert-bindings'))).toBe('admin');
    expect(needFor('DELETE', seg('/api/auth/cert-bindings/svc'))).toBe('admin');
    expect(needFor('GET', seg('/api/cluster/nodes/n4/decommission'))).toBe('read');
    expect(needFor('POST', seg('/api/cluster/nodes/n4/decommission'))).toBe('admin');
    expect(needFor('DELETE', seg('/api/cluster/nodes/n5'))).toBe('admin');
  });

  it('refuses revoking the last active admin with 409 last_admin (no bootstrap token)', () => {
    const auth = new Auth(null, 'admin');
    const ops = auth.list().find((t) => t.name === 'ops-admin')!;
    let err: unknown;
    try {
      auth.revoke(ops.id);
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(ApiErr);
    expect((err as ApiErr).status).toBe(409);
    expect((err as ApiErr).code).toBe('last_admin');
    // With a bootstrap token the guard doesn't apply.
    const boot = new Auth('boot', undefined);
    boot.revoke(boot.list().find((t) => t.name === 'ops-admin')!.id);
    expect(boot.sessionEnd(ops.id)).toBe('token revoked');
  });

  it('creates, lists and deletes bindings', () => {
    const auth = new Auth(null, 'admin');
    const actor = { name: 'ops-admin', token_id: 'k7q2m9', role: 'admin' as const, scopes: { publish: [], consume: [] } };
    const before = auth.listBindings().bindings.length;
    auth.createBinding({ cn: 'svc-a', role: 'viewer', scopes: { consume: ['orders.*'] } }, actor);
    expect(auth.listBindings().bindings).toHaveLength(before + 1);
    expect(() => auth.createBinding({ cn: 'svc-a', role: 'viewer' }, actor)).toThrow(/already exists/);
    expect(() => auth.createBinding({ cn: 'a/b', role: 'viewer' }, actor)).toThrow(/'\/'/);
    expect(() => auth.createBinding({ cn: 'x'.repeat(257), role: 'viewer' }, actor)).toThrow(/256 bytes/);
    expect(auth.createBinding({ cn: 'x'.repeat(100), role: 'viewer' }, actor).cn).toHaveLength(100);
    auth.deleteBinding('svc-a');
    expect(() => auth.deleteBinding('svc-a')).toThrow(/no binding/);
  });
});

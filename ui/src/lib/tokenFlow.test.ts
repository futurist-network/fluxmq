import { describe, expect, it, vi } from 'vitest';
import type { CreateTokenRequest, CreateTokenResponse } from '../api/types';
import { draftToRequest, initialDraft, scopeMatches, submitDraft, validateDraft } from './tokenFlow';
import { globMatch } from './rbac';

const resp = (req: CreateTokenRequest, adopt = false): CreateTokenResponse => ({
  token: 'fmq_ab12cd_s3cr3t',
  adopt,
  info: { id: 'ab12cd', name: req.name, role: req.role, scopes: req.scopes, created_at_ms: 1, created_by: 'anonymous', expires_at_ms: null, last_used_ms: null },
});

describe('create-token dialog flow', () => {
  it('open mode: defaults to an admin token and forbids other roles', () => {
    const d = initialDraft('open');
    expect(d.role).toBe('admin');
    expect(validateDraft(d, 'open')).toEqual({});
    expect(validateDraft({ ...d, role: 'viewer' }, 'open').role).toMatch(/first token must be admin/);
  });

  it('validates name, duplicates and scope globs', () => {
    const d = initialDraft('enforced');
    expect(validateDraft(d, 'enforced').name).toBeTruthy();
    expect(validateDraft({ ...d, name: 'grafana' }, 'enforced', ['grafana']).name).toMatch(/already exists/);
    expect(validateDraft({ ...d, name: 'Admin' }, 'enforced').name).toMatch(/reserved/);
    expect(validateDraft({ ...d, name: 'ANONYMOUS' }, 'enforced').name).toMatch(/reserved/);
    expect(validateDraft({ ...d, name: 'x', consume: ['orders events'] }, 'enforced').consume).toMatch(/orders events/);
  });

  it('builds the request with TTL presets and de-duplicated scopes', () => {
    expect(draftToRequest({ name: ' svc ', role: 'viewer', publish: ['a.*', 'a.*'], consume: ['b'], subscribe: [], ttl: '30d' })).toEqual({
      name: 'svc',
      role: 'viewer',
      scopes: { publish: ['a.*'], consume: ['b'] },
      ttl_secs: 30 * 86_400,
    });
    expect(draftToRequest({ name: 's', role: 'admin', publish: [], consume: [], subscribe: [], ttl: 'never' }).ttl_secs).toBeNull();
  });

  it('subscriber: needs a subscribe glob, sends only subscribe scopes, defaults to 1 day', () => {
    const d = { ...initialDraft('enforced', 'subscriber'), name: 'web-user-42' };
    expect(d.ttl).toBe('1d');
    expect(validateDraft(d, 'enforced').subscribe).toMatch(/at least one channel/);
    // Publish/consume left over from another role are dropped (the server refuses them).
    const ok = { ...d, publish: ['x'], subscribe: ['notifications.user42', 'notifications.user42'] };
    expect(validateDraft(ok, 'enforced')).toEqual({});
    expect(draftToRequest(ok)).toEqual({ name: 'web-user-42', role: 'subscriber', scopes: { publish: [], consume: [], subscribe: ['notifications.user42'] }, ttl_secs: 86_400 });
  });

  it('a token the server marks `adopt` becomes the console token, before anything else', async () => {
    const order: string[] = [];
    const create = vi.fn(async (req: CreateTokenRequest) => {
      order.push('create');
      return resp(req, true);
    });
    const adopt = vi.fn(() => order.push('adopt'));
    const r = await submitDraft(initialDraft('open'), { mode: 'open', create, adopt });
    expect(r.adopted).toBe(true);
    expect(adopt).toHaveBeenCalledWith('fmq_ab12cd_s3cr3t');
    expect(order).toEqual(['create', 'adopt']);
    expect(create.mock.calls[0]![0].role).toBe('admin');
  });

  it('adoption follows the server flag, not the (possibly stale) local mode', async () => {
    // The console still thinks the broker is open, but another client created the first token.
    const adopt = vi.fn();
    const r = await submitDraft(initialDraft('open'), { mode: 'open', create: async (req) => resp(req, false), adopt });
    expect(r.adopted).toBe(false);
    expect(adopt).not.toHaveBeenCalled();
    // ...and the other way round: mode unknown (whoami not loaded) but the server adopted it.
    const adopt2 = vi.fn();
    const r2 = await submitDraft({ name: 'first', role: 'admin', publish: [], consume: [], subscribe: [], ttl: 'never' }, { mode: null, create: async (req) => resp(req, true), adopt: adopt2 });
    expect(r2.adopted).toBe(true);
    expect(adopt2).toHaveBeenCalledWith('fmq_ab12cd_s3cr3t');
  });

  it('enforced mode: the secret is returned once and not adopted', async () => {
    const adopt = vi.fn();
    const r = await submitDraft({ name: 'ci', role: 'operator', publish: [], consume: [], subscribe: [], ttl: '7d' }, { mode: 'enforced', create: async (req) => resp(req), adopt });
    expect(r.adopted).toBe(false);
    expect(r.resp.token).toMatch(/^fmq_/);
    expect(adopt).not.toHaveBeenCalled();
  });

  it('invalid drafts never reach the server', async () => {
    const create = vi.fn();
    await expect(submitDraft(initialDraft('enforced'), { mode: 'enforced', create, adopt: vi.fn() })).rejects.toThrow('invalid token draft');
    expect(create).not.toHaveBeenCalled();
  });

  it('reports which channels a scope covers', () => {
    expect(scopeMatches(['orders.*'], ['orders.events', 'email', 'orders.priority'], globMatch)).toEqual(['orders.events', 'orders.priority']);
  });
});

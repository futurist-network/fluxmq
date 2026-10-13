import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, ApiError, onUnauthorized } from '../api/client';
import { ERROR_LAST_ADMIN, WS_CLOSE_TOKEN_REVOKED, type CertBindingList, type DecommissionStatus } from '../api/types';
import { revokeDisconnectNote, revokeErrorView } from './tokenFlow';

describe('v0.5 contract mirror', () => {
  it('constants match auth.rs', () => {
    expect(ERROR_LAST_ADMIN).toBe('last_admin');
    expect(WS_CLOSE_TOKEN_REVOKED).toBe(4401);
  });

  it('serde shapes round-trip', () => {
    const list: CertBindingList = JSON.parse('{"bindings":[{"cn":"svc","role":"viewer","scopes":{"publish":[],"consume":["a.*"]},"created_at_ms":1,"created_by":"admin","last_used_ms":null}],"client_mtls_enabled":false}');
    expect(list.bindings[0]!.scopes.consume).toEqual(['a.*']);
    const d: DecommissionStatus = JSON.parse('{"node":"n4","state":"moving_replicas","replicas_remaining":3,"initial_replicas":9,"started_at_ms":1,"finished_at_ms":null,"error":null}');
    expect(d.state).toBe('moving_replicas');
    expect(d.initial_replicas).toBe(9);
  });
});

describe('revoke errors branch on the code', () => {
  it('last_admin gets a dedicated explanation', () => {
    const v = revokeErrorView(new ApiError(409, ERROR_LAST_ADMIN, 'anything'), 'ops-admin');
    expect(v?.lastAdmin).toBe(true);
    expect(v?.title).toBe('“ops-admin” is the last admin token');
    expect(v?.body).toMatch(/Create another admin token first/);
  });
  it('a 409 conflict with similar text is not treated as last_admin', () => {
    const v = revokeErrorView(new ApiError(409, 'conflict', 'last admin'), 'x');
    expect(v).toEqual({ title: 'Couldn’t revoke the token', body: 'last admin', lastAdmin: false });
    expect(revokeErrorView(null, 'x')).toBeNull();
  });
  it('the confirm text says live clients are disconnected immediately', () => {
    expect(revokeDisconnectNote()).toMatch(/disconnected immediately/);
  });
});

describe('REST paths', () => {
  afterEach(() => vi.unstubAllGlobals());
  const stub = (status = 200, body: unknown = {}) => {
    const fetch = vi.fn(async () => new Response(status === 204 ? null : JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } }));
    vi.stubGlobal('fetch', fetch);
    return fetch;
  };

  it('hit the contract routes', async () => {
    const f = stub(204);
    await api.deleteCertBinding('orders worker/1');
    await api.forgetNode('n5');
    expect(f.mock.calls.map((c) => [(c as unknown[])[0], ((c as unknown[])[1] as RequestInit).method])).toEqual([
      ['/api/auth/cert-bindings/orders%20worker%2F1', 'DELETE'],
      ['/api/cluster/nodes/n5', 'DELETE'],
    ]);
    const g = stub(200, { node: 'n4' });
    await api.decommissionNode('n4');
    await api.decommissionStatus('n4');
    await api.decommissionDryRun('n4');
    await api.listDecommissions();
    expect(g.mock.calls.map((c) => [(c as unknown[])[0], ((c as unknown[])[1] as RequestInit).method])).toEqual([
      ['/api/cluster/nodes/n4/decommission', 'POST'],
      ['/api/cluster/nodes/n4/decommission', 'GET'],
      ['/api/cluster/nodes/n4/decommission?dry_run=true', 'POST'],
      ['/api/cluster/decommissions', 'GET'],
    ]);
  });

  it('checkToken sends the candidate token and does not broadcast its 401', async () => {
    const f = stub(401, { error: 'unauthorized', message: 'invalid token' });
    const heard = vi.fn();
    const off = onUnauthorized(heard);
    await expect(api.checkToken('fmq_new')).rejects.toBeInstanceOf(ApiError);
    off();
    expect(((f.mock.calls[0] as unknown[])[1] as RequestInit).headers).toMatchObject({ Authorization: 'Bearer fmq_new' });
    expect(heard).not.toHaveBeenCalled();
  });
});

describe('last_admin explanation is translated', () => {
  it('Russian', async () => {
    const { setLang } = await import('../i18n');
    setLang('ru');
    try {
      const v = revokeErrorView(new ApiError(409, ERROR_LAST_ADMIN, 'refusing to revoke the last admin token'), 'ops-admin');
      expect(v?.title).toBe('«ops-admin» — последний администраторский токен');
      expect(v?.body).toMatch(/--admin-token/);
      expect(v?.lastAdmin).toBe(true);
      expect(revokeDisconnectNote()).toMatch(/сразу отключены/);
    } finally {
      setLang('en');
    }
  });
});

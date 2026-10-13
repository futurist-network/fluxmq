import { describe, expect, it } from 'vitest';
import type { CertBinding } from '../api/types';
import { certActor, certDraftToRequest, cnWarning, initialCertDraft, sortBindings, validateCertDraft } from './certBindings';

describe('certificate binding drafts', () => {
  it('requires a CN, applies the server rules and rejects duplicates', () => {
    expect(validateCertDraft(initialCertDraft()).cn).toMatch(/subject CN/);
    // Server rules: 1–256 bytes, no '/', no control characters; > 64 characters is only a warning.
    expect(validateCertDraft({ ...initialCertDraft(), cn: 'x'.repeat(65) }).cn).toBeUndefined();
    expect(cnWarning('x'.repeat(65))).toMatch(/usual X\.509 limit of 64/);
    expect(cnWarning('short')).toBeNull();
    expect(validateCertDraft({ ...initialCertDraft(), cn: 'x'.repeat(257) }).cn).toMatch(/256 bytes/);
    expect(validateCertDraft({ ...initialCertDraft(), cn: 'é'.repeat(129) }).cn).toMatch(/256 bytes/);
    expect(validateCertDraft({ ...initialCertDraft(), cn: 'ou=x/cn=y' }).cn).toMatch(/“\/”/);
    expect(validateCertDraft({ ...initialCertDraft(), cn: 'a\u0007b' }).cn).toMatch(/control/);
    expect(validateCertDraft({ ...initialCertDraft(), cn: ' ledger-sync ' }, ['ledger-sync']).cn).toMatch(/already has a binding/);
    expect(validateCertDraft({ ...initialCertDraft(), cn: 'orders-worker.prod' })).toEqual({});
  });

  it('validates scope globs like tokens', () => {
    expect(validateCertDraft({ ...initialCertDraft(), cn: 'a', publish: ['orders/*'] }).publish).toMatch(/orders\/\*/);
  });

  it('builds the exact request shape (trimmed CN, deduped scopes)', () => {
    expect(certDraftToRequest({ cn: ' svc ', role: 'operator', publish: ['a.*', 'a.*'], consume: [] })).toEqual({ cn: 'svc', role: 'operator', scopes: { publish: ['a.*'], consume: [] } });
  });

  it('actor and ordering', () => {
    expect(certActor('svc')).toBe('cert:svc');
    const b = (cn: string, t: number): CertBinding => ({ cn, role: 'viewer', scopes: { publish: [], consume: [] }, created_at_ms: t, created_by: 'x', last_used_ms: null });
    expect(sortBindings([b('a', 1), b('c', 5), b('b', 5)]).map((x) => x.cn)).toEqual(['b', 'c', 'a']);
  });
});

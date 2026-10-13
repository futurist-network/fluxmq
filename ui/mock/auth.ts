/**
 * Mock RBAC — contract of crates/fluxmq-proto/src/auth.rs and the permission table design
 * (docs/design/v0.2.md §2).
 *
 *   (default)              open mode: no tokens, everyone is an anonymous admin;
 *                          the first POST /api/auth/tokens switches to enforced mode
 *   MOCK_ADMIN_TOKEN=…     bootstrap admin token (never listed), enforced mode
 *   MOCK_ROLE=viewer|operator|admin
 *                          enforced mode with seeded tokens; requests without a known token
 *                          act as the seeded token of that role (to try role-aware UI)
 *
 * v0.5: certificate bindings (`/api/auth/cert-bindings`, seeded with the tokens);
 *   `client_mtls_enabled` is false with MOCK_TLS=off or MOCK_CLIENT_MTLS=off (bindings inert).
 *   Last-admin guard: revoking the last active admin token while other tokens exist and there is
 *   no bootstrap token → 409 `last_admin` (with MOCK_ROLE=admin: revoke `ops-admin`).
 *   Revocation / expiry closes WS sessions of that token with 4401 (see server.ts).
 */
import { createHash, randomBytes } from 'node:crypto';
import type { AuthMode, CertBinding, CertBindingList, ClientScopes, CreateTokenRequest, CreateTokenResponse, Role, TokenInfo, WhoAmI } from '../src/api/types';
import { ERROR_LAST_ADMIN, ROLES } from '../src/api/types';
import { ApiErr } from './sim';

export interface Principal {
  name: string;
  token_id: string | null;
  role: Role;
  scopes: ClientScopes;
}

export type Need = 'read' | 'operate' | 'admin';

const RANK: Record<Role, number> = { subscriber: -1, viewer: 0, operator: 1, admin: 2 };
const NEED_RANK: Record<Need, number> = { read: 0, operate: 1, admin: 2 };

interface Stored {
  info: TokenInfo;
  hash: string;
}

const sha = (s: string) => createHash('sha256').update(s).digest('hex');

const MTLS = process.env.MOCK_TLS !== 'off' && process.env.MOCK_CLIENT_MTLS !== 'off';

export class Auth {
  private tokens = new Map<string, Stored>();
  private bindings = new Map<string, CertBinding>();
  /** Token strings whose sessions the mock revoked (MOCK_WS_REVOKE_AFTER): refused from then on. */
  private revokedSecrets = new Set<string>();
  readonly bootstrap: string | null;
  readonly forcedRole: Role | null;

  constructor(bootstrap: string | null, forcedRole: string | undefined) {
    this.bootstrap = bootstrap;
    this.forcedRole = ROLES.includes(forcedRole as Role) ? (forcedRole as Role) : null;
    if (this.forcedRole || bootstrap) this.seed();
  }

  private seed(): void {
    const now = Date.now();
    const H = 3_600_000;
    const D = 24 * H;
    const seed: [string, string, Role, ClientScopes, number, string, number | null, number | null][] = [
      ['k7q2m9', 'ops-admin', 'admin', { publish: [], consume: [] }, now - 40 * D, 'admin', null, now - 20_000],
      ['p3x8v1', 'oncall-operator', 'operator', { publish: [], consume: [] }, now - 25 * D, 'ops-admin#k7q2m9', now + 5 * D, now - 2 * H],
      ['g5n0c4', 'grafana', 'viewer', { publish: [], consume: [] }, now - 18 * D, 'ops-admin#k7q2m9', null, now - 15_000],
      ['b8r6t2', 'billing-service', 'viewer', { publish: ['billing.*'], consume: ['orders.events', 'payments.*'] }, now - 12 * D, 'ops-admin#k7q2m9', now + 80 * D, now - 4_000],
      ['a2d9w7', 'analytics-reader', 'viewer', { publish: [], consume: ['*'] }, now - 3 * D, 'ops-admin#k7q2m9', now + 27 * D, null],
      ['c4e1z5', 'ci-deploy', 'admin', { publish: [], consume: [] }, now - 95 * D, 'admin', now - 5 * D, now - 6 * D],
    ];
    for (const [id, name, role, scopes, created, by, exp, used] of seed) {
      this.tokens.set(id, { info: { id, name, role, scopes, created_at_ms: created, created_by: by, expires_at_ms: exp, last_used_ms: used }, hash: sha(`seed-${id}`) });
    }
    const bseed: [string, Role, ClientScopes, number, string, number | null][] = [
      ['orders-worker.prod-eu-1', 'viewer', { publish: ['orders.*'], consume: ['orders.events', 'payments.*'] }, now - 21 * D, 'ops-admin#k7q2m9', now - 3_000],
      ['ledger-sync', 'operator', { publish: [], consume: [] }, now - 9 * D, 'ops-admin#k7q2m9', now - 40 * 60_000],
      ['edge-gateway-07.dmz', 'viewer', { publish: ['telemetry.*'], consume: [] }, now - 2 * D, 'ops-admin#k7q2m9', null],
    ];
    for (const [cn, role, scopes, created, by, used] of bseed) this.bindings.set(cn, { cn, role, scopes, created_at_ms: created, created_by: by, last_used_ms: used });
  }

  mode(): AuthMode {
    return this.bootstrap || this.tokens.size ? 'enforced' : 'open';
  }

  /** null = unauthenticated (401). */
  authenticate(presented: string | null): Principal | null {
    if (this.mode() === 'open') return { name: 'anonymous', token_id: null, role: 'admin', scopes: { publish: [], consume: [] } };
    if (presented && this.revokedSecrets.has(presented)) return null;
    if (presented && this.bootstrap && presented === this.bootstrap) return { name: 'admin', token_id: null, role: 'admin', scopes: { publish: [], consume: [] } };
    if (presented) {
      const m = /^fmq_([a-z0-9]+)_([A-Za-z0-9]+)$/.exec(presented);
      const t = m ? this.tokens.get(m[1]!) : undefined;
      if (t && t.hash === sha(m![2]!) && (t.info.expires_at_ms == null || t.info.expires_at_ms > Date.now())) {
        t.info.last_used_ms = Date.now();
        return { name: t.info.name, token_id: t.info.id, role: t.info.role, scopes: t.info.scopes };
      }
    }
    if (this.forcedRole) {
      const t = [...this.tokens.values()].find((x) => x.info.role === this.forcedRole && (x.info.expires_at_ms == null || x.info.expires_at_ms > Date.now()));
      if (t) {
        t.info.last_used_ms = Date.now();
        return { name: t.info.name, token_id: t.info.id, role: t.info.role, scopes: t.info.scopes };
      }
    }
    return null;
  }

  authorize(p: Principal, need: Need): void {
    if (RANK[p.role] < NEED_RANK[need]) {
      throw new ApiErr(403, 'forbidden', `forbidden: role ${p.role} cannot perform ${need === 'operate' ? 'operator' : 'admin'} actions`);
    }
  }

  whoami(p: Principal): WhoAmI {
    return { mode: this.mode(), name: p.name, token_id: p.token_id, role: p.role, scopes: p.scopes };
  }

  list(): TokenInfo[] {
    return [...this.tokens.values()].map((t) => t.info).sort((a, b) => a.created_at_ms - b.created_at_ms);
  }

  create(req: CreateTokenRequest, actor: Principal): CreateTokenResponse {
    const name = typeof req.name === 'string' ? req.name.trim() : '';
    if (!name || name.length > 64) throw new ApiErr(400, 'bad_request', "'name' must be 1-64 characters");
    if (['admin', 'anonymous'].includes(name.toLowerCase())) throw new ApiErr(400, 'bad_request', `name '${name}' is reserved (bootstrap / open-mode principal)`);
    if (!ROLES.includes(req.role)) throw new ApiErr(400, 'bad_request', "'role' must be viewer, operator or admin");
    if ([...this.tokens.values()].some((t) => t.info.name === name)) throw new ApiErr(409, 'conflict', `a token named '${name}' already exists`);
    if (this.mode() === 'open' && req.role !== 'admin') throw new ApiErr(400, 'bad_request', 'the first token must have role admin (it enables access control)');
    const scopes: ClientScopes = { publish: req.scopes?.publish ?? [], consume: req.scopes?.consume ?? [] };
    for (const g of [...scopes.publish, ...scopes.consume]) if (!/^[a-zA-Z0-9._\-*?]+$/.test(g)) throw new ApiErr(400, 'bad_request', `invalid scope pattern '${g}'`);
    const id = randomBytes(4).toString('hex').slice(0, 6);
    const secret = randomBytes(18).toString('base64').replace(/[^A-Za-z0-9]/g, '').slice(0, 24);
    const now = Date.now();
    const info: TokenInfo = {
      id,
      name,
      role: req.role,
      scopes,
      created_at_ms: now,
      created_by: actor.token_id ? `${actor.name}#${actor.token_id}` : actor.name,
      expires_at_ms: req.ttl_secs ? now + req.ttl_secs * 1000 : null,
      last_used_ms: null,
    };
    const adopt = this.mode() === 'open';
    this.tokens.set(id, { info, hash: sha(secret) });
    // The first token switches open → enforced mode: the caller must adopt it.
    return { token: `fmq_${id}_${secret}`, info, adopt };
  }

  revoke(id: string): void {
    const t = this.tokens.get(id);
    if (!t) throw new ApiErr(404, 'not_found', `token '${id}' not found`);
    if (this.tokens.size === 1 && !this.bootstrap) throw new ApiErr(400, 'bad_request', 'cannot revoke the last token without a bootstrap --admin-token: nobody could administer the broker');
    // v0.5 last-admin guard.
    const now = Date.now();
    const active = (x: TokenInfo) => x.expires_at_ms == null || x.expires_at_ms > now;
    if (!this.bootstrap && t.info.role === 'admin' && active(t.info)) {
      const otherAdmins = [...this.tokens.values()].filter((x) => x.info.id !== id && x.info.role === 'admin' && active(x.info));
      if (!otherAdmins.length) throw new ApiErr(409, ERROR_LAST_ADMIN, `cannot revoke '${t.info.name}': it is the last active admin token and no bootstrap --admin-token is configured`);
    }
    this.tokens.delete(id);
  }

  /** Why a session authenticated with this token must end, or null while it is valid. */
  sessionEnd(tokenId: string | null): 'token revoked' | 'token expired' | null {
    if (!tokenId) return null;
    const t = this.tokens.get(tokenId);
    if (!t) return 'token revoked';
    if (t.info.expires_at_ms != null && t.info.expires_at_ms <= Date.now()) return 'token expired';
    return null;
  }

  /** MOCK_WS_REVOKE_AFTER: treat this presented token string as revoked from now on. */
  revokeSecret(presented: string): void {
    this.revokedSecrets.add(presented);
  }

  // ── v0.5 certificate bindings ──

  listBindings(): CertBindingList {
    return { bindings: [...this.bindings.values()].sort((a, b) => a.created_at_ms - b.created_at_ms), client_mtls_enabled: MTLS };
  }

  createBinding(req: { cn?: unknown; role?: unknown; scopes?: { publish?: string[]; consume?: string[] } }, actor: Principal): CertBinding {
    const cn = typeof req.cn === 'string' ? req.cn.trim() : '';
    if (!cn || Buffer.byteLength(cn, 'utf8') > 256) throw new ApiErr(400, 'bad_request', "'cn' must be 1-256 bytes");
    // eslint-disable-next-line no-control-regex
    if (cn.includes('/') || /[\u0000-\u001f\u007f]/.test(cn)) throw new ApiErr(400, 'bad_request', "'cn' may not contain '/' or control characters");
    if (!ROLES.includes(req.role as Role)) throw new ApiErr(400, 'bad_request', "'role' must be viewer, operator or admin");
    if (this.bindings.has(cn)) throw new ApiErr(409, 'conflict', `a binding for CN '${cn}' already exists`);
    const scopes: ClientScopes = { publish: req.scopes?.publish ?? [], consume: req.scopes?.consume ?? [] };
    for (const g of [...scopes.publish, ...scopes.consume]) if (!/^[a-zA-Z0-9._\-*?]+$/.test(g)) throw new ApiErr(400, 'bad_request', `invalid scope pattern '${g}'`);
    const b: CertBinding = { cn, role: req.role as Role, scopes, created_at_ms: Date.now(), created_by: actor.token_id ? `${actor.name}#${actor.token_id}` : actor.name, last_used_ms: null };
    this.bindings.set(cn, b);
    return b;
  }

  deleteBinding(cn: string): void {
    if (!this.bindings.delete(cn)) throw new ApiErr(404, 'not_found', `no binding for CN '${cn}'`);
  }
}

/** Central permission table: (method, path) → what it needs. */
export function needFor(method: string, seg: string[]): Need {
  const [, a, b, c] = seg;
  if (method === 'GET') {
    if (a === 'dlq' && b === 'export') return 'operate';
    if (a === 'auth' && (b === 'tokens' || b === 'cert-bindings')) return 'admin';
    return 'read';
  }
  // Reads that use POST bodies.
  if (method === 'POST' && a === 'filter' && (b === 'parse' || b === 'render')) return 'read';
  if (method === 'POST' && a === 'routes' && b === 'test') return 'read';
  // Deleting a consumer group is admin; resetting its offsets is operator.
  if (a === 'channels' && c === 'groups' && method === 'DELETE') return 'admin';
  if (a === 'channels' && (c === 'publish' || c === 'replay' || c === 'groups')) return 'operate';
  if (a === 'dlq') return 'operate';
  return 'admin';
}

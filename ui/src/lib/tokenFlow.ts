/**
 * Create-token dialog logic, kept out of React so it is testable without a DOM:
 * draft validation, request building, and the open-mode rules: the first token must be an
 * admin (the server answers 400 otherwise) and switches the broker to enforced mode — the
 * response then has `adopt = true` and the console stores the token as its own immediately.
 */
import type { AuthMode, CreateTokenRequest, CreateTokenResponse, Role } from '../api/types';
import { validateGlob } from './rbac';
import { t, type TKey } from '../i18n';

export type TtlPreset = '1d' | '7d' | '30d' | '90d' | '1y' | 'never';
/** `labelKey` is translated at render (`t(p.labelKey)`). */
export const TTL_PRESETS: { id: TtlPreset; labelKey: TKey; secs: number | null }[] = [
  { id: '1d', labelKey: 'tokens.ttl.1d', secs: 86_400 },
  { id: '7d', labelKey: 'tokens.ttl.7d', secs: 7 * 86_400 },
  { id: '30d', labelKey: 'tokens.ttl.30d', secs: 30 * 86_400 },
  { id: '90d', labelKey: 'tokens.ttl.90d', secs: 90 * 86_400 },
  { id: '1y', labelKey: 'tokens.ttl.1y', secs: 365 * 86_400 },
  { id: 'never', labelKey: 'tokens.ttl.never', secs: null },
];

export interface TokenDraft {
  name: string;
  role: Role;
  publish: string[];
  consume: string[];
  /** Live subscription channels: only for the `subscriber` role (viewer+ may subscribe anywhere). */
  subscribe: string[];
  ttl: TtlPreset;
}

/** Roles in the order the create dialog offers them: `subscriber` is special-purpose, so last. */
export const DIALOG_ROLES: readonly Role[] = ['viewer', 'operator', 'admin', 'subscriber'];

/** Default lifetime for a role: subscriber tokens end up in browsers, so they live short. */
export const defaultTtl = (role: Role): TtlPreset => (role === 'subscriber' ? '1d' : '90d');

/** Shown next to the (locked) role in open mode. */
export const firstTokenNote = (): string => t('tokens.firstTokenNote');

export function initialDraft(mode: AuthMode | null, role?: Role): TokenDraft {
  // Open mode: the first token must be an admin, or nobody could administer the broker.
  const r = mode === 'open' ? 'admin' : (role ?? 'viewer');
  return { name: mode === 'open' ? 'console-admin' : '', role: r, publish: [], consume: [], subscribe: [], ttl: mode === 'open' ? 'never' : defaultTtl(r) };
}

/** Principal names the broker uses itself (`admin` = bootstrap token, `anonymous` = open mode). */
export const RESERVED_NAMES = ['admin', 'anonymous'];

export type DraftErrors = Partial<Record<'name' | 'role' | 'publish' | 'consume' | 'subscribe', string>>;

export function validateDraft(d: TokenDraft, mode: AuthMode | null, existingNames: readonly string[] = []): DraftErrors {
  const e: DraftErrors = {};
  const name = d.name.trim();
  if (!name) e.name = t('tokens.err.nameRequired');
  else if (name.length > 64) e.name = t('tokens.err.nameLong');
  else if (!/^[a-zA-Z0-9._\- ]+$/.test(name)) e.name = t('tokens.err.nameChars');
  else if (RESERVED_NAMES.includes(name.toLowerCase())) e.name = t('tokens.err.nameReserved', { name });
  else if (existingNames.includes(name)) e.name = t('tokens.err.nameExists');
  if (mode === 'open' && d.role !== 'admin') e.role = firstTokenNote();
  const bad = (list: string[]) => list.map((g) => (validateGlob(g) ? t('tokens.err.glob', { glob: g, error: validateGlob(g) }) : null)).find(Boolean) ?? undefined;
  if (d.role === 'subscriber') {
    // The server's rule: at least one subscribe glob, no publish/consume (those fields are hidden).
    const s = bad(d.subscribe);
    if (s) e.subscribe = s;
    else if (!d.subscribe.length) e.subscribe = t('tokens.err.subscribeRequired');
    return e;
  }
  const p = bad(d.publish);
  const c = bad(d.consume);
  if (p) e.publish = p;
  if (c) e.consume = c;
  return e;
}

export function draftToRequest(d: TokenDraft): CreateTokenRequest {
  return {
    name: d.name.trim(),
    role: d.role,
    scopes: d.role === 'subscriber' ? { publish: [], consume: [], subscribe: [...new Set(d.subscribe)] } : { publish: [...new Set(d.publish)], consume: [...new Set(d.consume)] },
    ttl_secs: TTL_PRESETS.find((t) => t.id === d.ttl)?.secs ?? null,
  };
}

export interface FlowResult {
  resp: CreateTokenResponse;
  /** The new token was stored as this browser's token (`resp.adopt`: it enabled auth). */
  adopted: boolean;
}

/**
 * Submit a draft. Throws on validation errors (as `DraftErrors` in `cause`) or API errors.
 * When the server says `adopt` (this token switched the broker to enforced mode) the secret
 * becomes the console's token before anything else happens, because the very next request
 * without it would get 401. The local `mode` is only used for validation — it may be stale.
 */
export async function submitDraft(
  d: TokenDraft,
  ctx: { mode: AuthMode | null; existingNames?: readonly string[]; create: (req: CreateTokenRequest) => Promise<CreateTokenResponse>; adopt: (token: string) => void },
): Promise<FlowResult> {
  const errors = validateDraft(d, ctx.mode, ctx.existingNames);
  if (Object.keys(errors).length) throw new Error('invalid token draft', { cause: errors });
  const resp = await ctx.create(draftToRequest(d));
  const adopted = resp.adopt === true;
  if (adopted) ctx.adopt(resp.token);
  return { resp, adopted };
}

/** Channels a glob list covers (for the "matches 3 channels" hint). */
export function scopeMatches(globs: readonly string[], channels: readonly string[], match: (g: string, n: string) => boolean): string[] {
  return channels.filter((c) => globs.some((g) => match(g, c)));
}

// ───────────── v0.5: revocation ─────────────

/** What the revoke confirm says happens to live sessions (auth.rs v0.5: within 1 s, every node). */
export const revokeDisconnectNote = (): string => t('tokens.revokeDisconnectNote');

export interface RevokeErrorView {
  /** Short heading for the inline error. */
  title: string;
  /** Explanation (and what to do instead). */
  body: string;
  /** True for the last-admin guard (409 `last_admin`). */
  lastAdmin: boolean;
}

/**
 * Explain a failed revoke. Branches on the machine-readable code only (`ApiError.error`), never on
 * the message: `last_admin` gets a dedicated explanation, everything else shows the server text.
 */
export function revokeErrorView(e: { code?: string; message: string } | null | undefined, tokenName: string): RevokeErrorView | null {
  if (!e) return null;
  if (e.code === 'last_admin') {
    return {
      title: t('tokens.lastAdmin.title', { name: tokenName }),
      body: t('tokens.lastAdmin.body'),
      lastAdmin: true,
    };
  }
  return { title: t('tokens.revokeFailed'), body: e.message, lastAdmin: false };
}

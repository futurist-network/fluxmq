/**
 * Role gating (mirrors the role table in crates/fluxmq-proto/src/auth.rs).
 *
 *   subscriber — (v0.10) live subscriptions (WebSocket/SSE) on `scopes.subscribe` channels only
 *   viewer   — all reads, telemetry, message browsing
 *   operator — + DLQ retry/discard/export, replay, reset offsets, REST publish
 *   admin    — + channel create/update/delete, consumer group delete, routes, tokens, cluster operations
 *
 * The server is the authority (403); the UI only disables what would certainly fail and says why.
 */
import type { Role } from '../api/types';
import { t } from '../i18n';
import { localized } from './labels';

/** What an action needs beyond read access. */
export type Need = 'operate' | 'admin';

export const ROLE_RANK: Record<Role, number> = { subscriber: -1, viewer: 0, operator: 1, admin: 2 };
const NEED_ROLE: Record<Need, Role> = { operate: 'operator', admin: 'admin' };

const ROLES = ['subscriber', 'viewer', 'operator', 'admin'] as const satisfies readonly Role[];
export const ROLE_LABEL: Readonly<Record<Role, string>> = localized(ROLES, 'labels.role.');
export const ROLE_HINT: Readonly<Record<Role, string>> = localized(ROLES, 'labels.roleHint.');

/**
 * `role === null` means "not known yet" (whoami in flight) — allow, the server still decides.
 */
export function roleAllows(role: Role | null, need: Need): boolean {
  if (role == null) return true;
  return ROLE_RANK[role] >= ROLE_RANK[NEED_ROLE[need]];
}

/** Tooltip for a disabled control, or null when allowed. */
export function gateReason(role: Role | null, need: Need): string | null {
  if (roleAllows(role, need)) return null;
  return t(need === 'operate' ? 'labels.gate.operate' : 'labels.gate.admin', { role: role ? ROLE_LABEL[role].toLowerCase() : '' });
}

/** Toast text for a 403 from the server. */
export function forbiddenText(role: Role | null): string {
  return role ? t('labels.forbidden.role', { role: ROLE_LABEL[role].toLowerCase() }) : t('labels.forbidden.unknown');
}

/** Glob match as the broker does it for scopes: `*` any run, `?` one character. */
export function globMatch(glob: string, name: string): boolean {
  const re = new RegExp(`^${glob.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')}$`);
  return re.test(name);
}

export function validateGlob(glob: string): string | null {
  if (!glob) return t('labels.glob.empty');
  if (!/^[a-zA-Z0-9._\-*?]+$/.test(glob)) return t('labels.glob.chars');
  return null;
}

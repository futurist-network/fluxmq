/**
 * Certificate bindings (v0.5, crates/fluxmq-proto/src/auth.rs): a verified client-port mTLS
 * certificate subject CN maps to a principal, so a client can authenticate with its certificate
 * alone. Draft validation and request building, kept out of React for tests.
 */
import type { CertBinding, CreateCertBindingRequest, Role } from '../api/types';
import { validateGlob } from './rbac';
import { t } from '../i18n';

export interface CertDraft {
  cn: string;
  role: Role;
  publish: string[];
  consume: string[];
}

export type CertDraftErrors = Partial<Record<'cn' | 'publish' | 'consume', string>>;

/** Server rule (auth.rs v0.5): a CN is 1–256 bytes (UTF-8), no `/`, no control characters. */
export const CN_MAX_BYTES = 256;
/** RFC 5280 `ub-common-name`: longer CNs are unusual but some CAs issue them — a soft warning only. */
export const CN_SOFT_MAX = 64;

/** The server flag that enables client-port mTLS — without it bindings are inert. */
export const CLIENT_CA_FLAG = '--tls-client-ca';
/** Operator guide section that explains client certificates (docs/GUIDE.ru.md §24). */
export const certGuideRef = (): string => t('security.cb.guideRef');

export function initialCertDraft(): CertDraft {
  return { cn: '', role: 'viewer', publish: [], consume: [] };
}

export function validateCertDraft(d: CertDraft, existingCns: readonly string[] = []): CertDraftErrors {
  const e: CertDraftErrors = {};
  const cn = d.cn.trim();
  if (!cn) e.cn = t('security.cb.err.cnRequired');
  else if (new TextEncoder().encode(cn).length > CN_MAX_BYTES) e.cn = t('security.cb.err.cnBytes', { max: CN_MAX_BYTES });
  else if (cn.includes('/')) e.cn = t('security.cb.err.cnSlash');
  // eslint-disable-next-line no-control-regex
  else if (/[\u0000-\u001f\u007f]/.test(cn)) e.cn = t('security.cb.err.cnControl');
  else if (existingCns.includes(cn)) e.cn = t('security.cb.err.cnExists');
  const bad = (list: string[]) => list.map((g) => (validateGlob(g) ? t('tokens.err.glob', { glob: g, error: validateGlob(g) }) : null)).find(Boolean) ?? undefined;
  const p = bad(d.publish);
  const c = bad(d.consume);
  if (p) e.publish = p;
  if (c) e.consume = c;
  return e;
}

/** Non-blocking hint for an unusually long CN (the server accepts up to 256 bytes). */
export function cnWarning(cn: string): string | null {
  const n = cn.trim().length;
  return n > CN_SOFT_MAX ? t('security.cb.cnWarning', { n, max: CN_SOFT_MAX }) : null;
}

export function certDraftToRequest(d: CertDraft): CreateCertBindingRequest {
  return { cn: d.cn.trim(), role: d.role, scopes: { publish: [...new Set(d.publish)], consume: [...new Set(d.consume)] } };
}

/** How the principal of a binding appears as the actor in the audit log. */
export function certActor(cn: string): string {
  return `cert:${cn}`;
}

/** Newest first, then CN. */
export function sortBindings(list: readonly CertBinding[]): CertBinding[] {
  return [...list].sort((a, b) => b.created_at_ms - a.created_at_ms || a.cn.localeCompare(b.cn));
}

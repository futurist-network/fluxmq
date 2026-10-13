/** Pure shaping of `TlsStatus` (crates/fluxmq-proto/src/tls.rs) for Settings → Security. */
import { t } from '../i18n';
import { localized } from './labels';
import { CERT_EXPIRY_CRITICAL_DAYS, CERT_EXPIRY_WARNING_DAYS, TLS_SURFACES, type TlsEndpointStatus, type TlsStatus, type TlsSurface } from '../api/types';

const DAY_MS = 86_400_000;

export const TLS_SURFACE_LABEL: Readonly<Record<TlsSurface, string>> = localized(TLS_SURFACES, 'security.surface.');

export const TLS_SURFACE_HINT: Readonly<Record<TlsSurface, string>> = localized(TLS_SURFACES, 'security.surfaceHint.');

export type ExpiryLevel = 'ok' | 'warning' | 'critical' | 'expired';

/** Same thresholds as the `tls_cert_expiring` alert: warning under 14 days, critical under 3 days or expired. */
export function expiryLevel(notAfterMs: number, now: number): ExpiryLevel {
  const left = notAfterMs - now;
  if (left <= 0) return 'expired';
  if (left < CERT_EXPIRY_CRITICAL_DAYS * DAY_MS) return 'critical';
  if (left < CERT_EXPIRY_WARNING_DAYS * DAY_MS) return 'warning';
  return 'ok';
}

/** "in 9 days", "in 2 days 4 h", "in 5 h 12 min", "in 40 min", "2 days ago". */
export function fmtExpiryCountdown(notAfterMs: number, now: number): string {
  const diff = notAfterMs - now;
  const a = Math.abs(diff);
  const d = Math.floor(a / DAY_MS);
  const h = Math.floor((a % DAY_MS) / 3_600_000);
  const m = Math.floor((a % 3_600_000) / 60_000);
  let s: string;
  if (d >= 14) s = t('security.countdown.days', { count: d });
  else if (d >= 1) s = h ? t('security.countdown.daysHours', { count: d, h }) : t('security.countdown.days', { count: d });
  else if (h >= 1) s = m ? t('security.countdown.hoursMin', { h, m }) : t('security.countdown.hours', { h });
  else s = t('security.countdown.min', { m: Math.max(1, m) });
  return t(diff > 0 ? 'security.countdown.in' : 'security.countdown.ago', { span: s });
}

/** Fraction of the validity window already used (0..1), for the lifetime bar. */
export function lifetimeUsed(notBeforeMs: number, notAfterMs: number, now: number): number {
  const total = notAfterMs - notBeforeMs;
  if (total <= 0) return 1;
  return Math.min(1, Math.max(0, (now - notBeforeMs) / total));
}

/** "AB:CD:EF:…" — uppercase, colon-separated byte pairs (how openssl and browsers print it). */
export function fmtFingerprint(hex: string): string {
  const clean = hex.replace(/[^0-9a-fA-F]/g, '').toUpperCase();
  return clean.match(/.{1,2}/g)?.join(':') ?? '';
}

/** Common name out of an RFC 4514-ish subject ("CN=n1.fluxmq.internal, O=FluxMQ" → "n1.fluxmq.internal"). */
export function commonName(subject: string): string | null {
  const m = /(?:^|[,/]\s*)CN\s*=\s*([^,/]+)/i.exec(subject);
  return m ? m[1]!.trim() : null;
}

/** The subject without its CN component ("CN=n1, O=Acme, C=DE" → "O=Acme, C=DE"); null when nothing is left or there is no CN. */
export function subjectRest(subject: string): string | null {
  if (!commonName(subject)) return null;
  const rest = subject
    .split(/,\s*/)
    .filter((p) => !/^\s*CN\s*=/i.test(p))
    .join(', ');
  return rest || null;
}

/** Fingerprint split in two halves of 16 byte pairs, for a tidy two-line display. */
export function fingerprintLines(hex: string): [string, string] {
  const pairs = fmtFingerprint(hex).split(':');
  const half = Math.ceil(pairs.length / 2);
  return [pairs.slice(0, half).join(':'), pairs.slice(half).join(':')];
}

export interface TlsOverview {
  /** No surface has TLS. */
  plaintext: boolean;
  enabled: TlsSurface[];
  disabled: TlsSurface[];
  /** Worst expiry level over enabled surfaces with a certificate. */
  worst: ExpiryLevel;
  /** Surfaces whose last reload failed. */
  reloadErrors: TlsSurface[];
  handshakeFailures: number;
}

const LEVEL_RANK: Record<ExpiryLevel, number> = { ok: 0, warning: 1, critical: 2, expired: 3 };

export function tlsOverview(s: TlsStatus, now: number): TlsOverview {
  const enabled = TLS_SURFACES.filter((k) => s[k].enabled);
  let worst: ExpiryLevel = 'ok';
  for (const k of enabled) {
    const c = s[k].certificate;
    if (!c) continue;
    const l = expiryLevel(c.not_after_ms, now);
    if (LEVEL_RANK[l] > LEVEL_RANK[worst]) worst = l;
  }
  return {
    plaintext: enabled.length === 0,
    enabled,
    disabled: TLS_SURFACES.filter((k) => !s[k].enabled),
    worst,
    reloadErrors: TLS_SURFACES.filter((k) => s[k].last_error),
    handshakeFailures: TLS_SURFACES.reduce((n, k) => n + s[k].handshake_failures_total, 0),
  };
}

/** True when two enabled surfaces serve the same leaf (http reuses the client certificate unless overridden). */
export function sameCertificate(a: TlsEndpointStatus, b: TlsEndpointStatus): boolean {
  return !!a.certificate && !!b.certificate && a.certificate.sha256_fingerprint === b.certificate.sha256_fingerprint;
}

export const GEN_DEV_COMMAND = 'fluxmq tls gen-dev --out ./certs --nodes n1,n2,n3 --hosts localhost,127.0.0.1';

/** The operator guide: the "TLS" section of the user guide (tls.rs contract). */
export const TLS_GUIDE_FILE = 'docs/GUIDE.ru.md';
export const TLS_GUIDE_SECTION = 'TLS';

/** Server flags (tls.rs) with the `gen-dev` output names (also contract) — shown for node n1. */
export const ENABLE_TLS_FLAGS = [
  'fluxmq-server --data-dir ./data \\',
  '  --tls-cert ./certs/server.pem --tls-key ./certs/server-key.pem \\',
  '  --tls-client-ca ./certs/ca.pem \\',
  '  --cluster-tls-cert ./certs/node-n1.pem --cluster-tls-key ./certs/node-n1-key.pem \\',
  '  --cluster-tls-ca ./certs/ca.pem',
].join('\n');

/** A client with an mTLS identity (fluxmq-bench / fluxmq-demo flags). */
export const CLIENT_TLS_FLAGS = 'fluxmq-bench --tls-ca ./certs/ca.pem --tls-cert ./certs/client.pem --tls-key ./certs/client-key.pem …';

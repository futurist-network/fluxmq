import { describe, expect, it } from 'vitest';
import type { CertificateInfo, TlsEndpointStatus, TlsStatus } from '../api/types';
import { commonName, expiryLevel, fingerprintLines, subjectRest, fmtExpiryCountdown, fmtFingerprint, lifetimeUsed, sameCertificate, tlsOverview } from './tls';

const DAY = 86_400_000;
const NOW = 1_800_000_000_000;

const cert = (days: number, fp = 'ab'.repeat(32)): CertificateInfo => ({ subject: 'CN=n1, O=FluxMQ', issuer: 'CN=CA', sans: ['n1'], not_before_ms: NOW - 30 * DAY, not_after_ms: NOW + days * DAY, sha256_fingerprint: fp });
const ep = (p: Partial<TlsEndpointStatus> = {}): TlsEndpointStatus => ({ enabled: true, mutual: false, certificate: cert(200), loaded_at_ms: NOW - DAY, last_error: null, handshake_failures_total: 0, ...p });
const off = (): TlsEndpointStatus => ({ enabled: false, mutual: false, certificate: null, loaded_at_ms: null, last_error: null, handshake_failures_total: 0 });

describe('certificate expiry', () => {
  it('uses the alert thresholds: warning < 14 days, critical < 3 days or expired', () => {
    expect(expiryLevel(NOW + 30 * DAY, NOW)).toBe('ok');
    expect(expiryLevel(NOW + 14 * DAY, NOW)).toBe('ok');
    expect(expiryLevel(NOW + 14 * DAY - 1, NOW)).toBe('warning');
    expect(expiryLevel(NOW + 3 * DAY, NOW)).toBe('warning');
    expect(expiryLevel(NOW + 3 * DAY - 1, NOW)).toBe('critical');
    expect(expiryLevel(NOW, NOW)).toBe('expired');
    expect(expiryLevel(NOW - DAY, NOW)).toBe('expired');
  });
  it('formats a countdown', () => {
    expect(fmtExpiryCountdown(NOW + 291 * DAY, NOW)).toBe('in 291 days');
    expect(fmtExpiryCountdown(NOW + 9 * DAY + 6 * 3_600_000, NOW)).toBe('in 9 days 6 h');
    expect(fmtExpiryCountdown(NOW + 1 * DAY, NOW)).toBe('in 1 day');
    expect(fmtExpiryCountdown(NOW + 5 * 3_600_000 + 12 * 60_000, NOW)).toBe('in 5 h 12 min');
    expect(fmtExpiryCountdown(NOW + 20_000, NOW)).toBe('in 1 min');
    expect(fmtExpiryCountdown(NOW - 2 * DAY, NOW)).toBe('2 days ago');
  });
  it('lifetime used', () => {
    expect(lifetimeUsed(0, 100, 25)).toBe(0.25);
    expect(lifetimeUsed(0, 100, 200)).toBe(1);
    expect(lifetimeUsed(50, 100, 0)).toBe(0);
    expect(lifetimeUsed(100, 100, 100)).toBe(1);
  });
});

describe('certificate details', () => {
  it('formats the SHA-256 fingerprint like openssl', () => {
    expect(fmtFingerprint('a1b2c3')).toBe('A1:B2:C3');
    expect(fmtFingerprint('ab'.repeat(32)).split(':')).toHaveLength(32);
    expect(fmtFingerprint('')).toBe('');
  });
  it('extracts the common name', () => {
    expect(commonName('CN=mq.acme.io, O=Acme, C=DE')).toBe('mq.acme.io');
    expect(commonName('O=Acme, CN=n1')).toBe('n1');
    expect(commonName('/C=DE/O=Acme/CN=n2')).toBe('n2');
    expect(commonName('O=Acme')).toBeNull();
  });
  it('strips the CN from the subject', () => {
    expect(subjectRest('CN=mq.acme.io, O=Acme, C=DE')).toBe('O=Acme, C=DE');
    expect(subjectRest('CN=n1')).toBeNull();
    expect(subjectRest('O=Acme')).toBeNull();
  });
  it('splits the fingerprint into two even lines', () => {
    const [a, b] = fingerprintLines('ab'.repeat(32));
    expect(a.split(':')).toHaveLength(16);
    expect(b.split(':')).toHaveLength(16);
  });
  it('detects a shared certificate', () => {
    expect(sameCertificate(ep(), ep())).toBe(true);
    expect(sameCertificate(ep(), ep({ certificate: cert(200, 'cd'.repeat(32)) }))).toBe(false);
    expect(sameCertificate(ep(), off())).toBe(false);
  });
});

describe('overview', () => {
  it('plaintext deployment', () => {
    const s: TlsStatus = { node_id: 'n1', client: off(), http: off(), cluster: off() };
    const o = tlsOverview(s, NOW);
    expect(o.plaintext).toBe(true);
    expect(o.enabled).toEqual([]);
    expect(o.worst).toBe('ok');
  });
  it('worst expiry, reload errors and handshake failures over enabled surfaces', () => {
    const s: TlsStatus = {
      node_id: 'n1',
      client: ep({ handshake_failures_total: 37, mutual: true }),
      http: off(),
      cluster: ep({ certificate: cert(9), last_error: 'reload failed', handshake_failures_total: 2, mutual: true }),
    };
    const o = tlsOverview(s, NOW);
    expect(o.plaintext).toBe(false);
    expect(o.enabled).toEqual(['client', 'cluster']);
    expect(o.disabled).toEqual(['http']);
    expect(o.worst).toBe('warning');
    expect(o.reloadErrors).toEqual(['cluster']);
    expect(o.handshakeFailures).toBe(39);
  });
  it('ignores certificates of disabled surfaces', () => {
    const s: TlsStatus = { node_id: 'n1', client: ep(), http: { ...off(), certificate: cert(-1) }, cluster: off() };
    expect(tlsOverview(s, NOW).worst).toBe('ok');
  });
});

describe('expiry countdown in Russian', () => {
  it('uses Russian plural forms', async () => {
    const { setLang } = await import('../i18n');
    setLang('ru');
    try {
      expect(fmtExpiryCountdown(NOW + 21 * DAY, NOW)).toBe('через 21 день');
      expect(fmtExpiryCountdown(NOW + 2 * DAY + 4 * 3_600_000, NOW)).toBe('через 2 дня 4 ч');
      expect(fmtExpiryCountdown(NOW + 5 * DAY, NOW)).toBe('через 5 дней');
      expect(fmtExpiryCountdown(NOW - 2 * DAY, NOW)).toBe('2 дня назад');
      expect(fmtExpiryCountdown(NOW + 40 * 60_000, NOW)).toBe('через 40 мин');
    } finally {
      setLang('en');
    }
  });
});

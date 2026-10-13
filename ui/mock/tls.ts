/**
 * Mock `GET /api/tls` (v0.4) — contract of crates/fluxmq-proto/src/tls.rs.
 *
 *   default         TLS on every surface: client (mTLS) and http share a public certificate;
 *                   the cluster node certificate expires in 9 days and its last reload failed
 *                   (the rotated key didn't match), so the old certificate is still served.
 *   MOCK_TLS=off    plaintext deployment: every surface disabled.
 *   MOCK_CLIENT_MTLS=off   client port without --tls-client-ca (no mTLS; v0.5 cert bindings inert).
 */
import { createHash } from 'node:crypto';
import type { CertificateInfo, TlsEndpointStatus, TlsStatus } from '../src/api/types';
import type { Sim } from './sim';

const DAY = 86_400_000;
const OFF = process.env.MOCK_TLS === 'off';
const CLIENT_MTLS = process.env.MOCK_CLIENT_MTLS !== 'off';

const fp = (seed: string) => createHash('sha256').update(seed).digest('hex');

export class Tls {
  private readonly clientCert: CertificateInfo;
  private readonly clusterCert: CertificateInfo;
  private readonly loadedAt: number;
  private readonly clusterLoadedAt: number;

  constructor(private sim: Sim) {
    const t = sim.start;
    this.clientCert = {
      subject: 'CN=mq.prod-eu-1.acme.io, O=Acme Payments Ltd, C=DE',
      issuer: 'CN=Acme Issuing CA R3, O=Acme Payments Ltd, C=DE',
      sans: ['mq.prod-eu-1.acme.io', 'console.prod-eu-1.acme.io', '*.mq.prod-eu-1.acme.io', '10.12.0.11', '10.12.0.12', '10.12.0.13'],
      not_before_ms: t - 74 * DAY,
      not_after_ms: t + 291 * DAY + 5 * 3_600_000,
      sha256_fingerprint: fp('client-leaf'),
    };
    this.clusterCert = {
      subject: 'CN=n1, OU=fluxmq-cluster, O=Acme Payments Ltd',
      issuer: 'CN=prod-eu-1 cluster CA, O=Acme Payments Ltd',
      sans: ['n1', 'n1.fluxmq.internal', '10.12.0.11'],
      not_before_ms: t - 81 * DAY,
      not_after_ms: t + 9 * DAY + 6 * 3_600_000,
      sha256_fingerprint: fp('cluster-leaf-n1'),
    };
    this.loadedAt = t - 6 * DAY - 2 * 3_600_000;
    this.clusterLoadedAt = t - 6 * DAY - 2 * 3_600_000;
    if (!OFF) sim.tlsAlerts = () => this.alerts();
    if (!OFF) sim.pushEvent('tls_reload_failed', 'TLS reload failed — previous certificate kept', 'cluster', 'warning', t - 38 * 60_000);
  }

  /** Same thresholds as the server: warning < 14 days, critical < 3 days or expired; plus failed reloads. */
  private alerts() {
    const out: { id: string; kind: 'tls_cert_expiring' | 'tls_reload_failed'; severity: 'warning' | 'critical'; title: string; detail: string; possible_reason: string | null; link: string }[] = [];
    const left = this.clusterCert.not_after_ms - Date.now();
    if (left < 14 * DAY) {
      out.push({ id: 'tls_cert_expiring:n1/cluster', kind: 'tls_cert_expiring', severity: left < 3 * DAY ? 'critical' : 'warning', title: `Cluster certificate on n1 expires in ${Math.max(0, Math.floor(left / DAY))} days`, detail: `${this.clusterCert.subject} · not after ${new Date(this.clusterCert.not_after_ms).toISOString().slice(0, 10)}`, possible_reason: 'Rotate the PEM files; FluxMQ reloads them within 30 s or on SIGHUP.', link: '/settings/security' });
    }
    out.push({ id: 'tls_reload_failed:n1/cluster', kind: 'tls_reload_failed', severity: 'warning', title: 'TLS reload failed on n1 (cluster)', detail: 'private key does not match certificate — the previous certificate is still served', possible_reason: 'The key and certificate files were replaced out of step.', link: '/settings/security' });
    return out;
  }

  private failures(base: number, perHour: number): number {
    return base + Math.floor(((Date.now() - this.sim.start) / 3_600_000) * perHour);
  }

  status(): TlsStatus {
    if (OFF) {
      const off = (): TlsEndpointStatus => ({ enabled: false, mutual: false, certificate: null, loaded_at_ms: null, last_error: null, handshake_failures_total: 0 });
      return { node_id: 'n1', client: off(), http: off(), cluster: off() };
    }
    return {
      node_id: 'n1',
      client: { enabled: true, mutual: CLIENT_MTLS, certificate: { ...this.clientCert, sans: [...this.clientCert.sans] }, loaded_at_ms: this.loadedAt, last_error: null, handshake_failures_total: this.failures(37, 120) },
      http: { enabled: true, mutual: false, certificate: { ...this.clientCert, sans: [...this.clientCert.sans] }, loaded_at_ms: this.loadedAt, last_error: null, handshake_failures_total: this.failures(4, 12) },
      cluster: {
        enabled: true,
        mutual: true,
        certificate: { ...this.clusterCert, sans: [...this.clusterCert.sans] },
        loaded_at_ms: this.clusterLoadedAt,
        last_error: 'reload failed: /etc/fluxmq/tls/n1-key.pem: private key does not match certificate',
        handshake_failures_total: 0,
      },
    };
  }
}

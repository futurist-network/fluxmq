import { AlertTriangle, BookOpen, Lock, LockOpen, ShieldCheck, ShieldOff, Users } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { TLS_SURFACES, type CertificateInfo, type TlsEndpointStatus, type TlsStatus, type TlsSurface } from '../../api/types';
import { fmtDate, fmtDateTime, fmtInt, fmtRelative } from '../../lib/format';
import { useAsync, useInterval, useNow } from '../../lib/hooks';
import { CLIENT_TLS_FLAGS, commonName, ENABLE_TLS_FLAGS, expiryLevel, fingerprintLines, fmtExpiryCountdown, fmtFingerprint, GEN_DEV_COMMAND, lifetimeUsed, sameCertificate, TLS_GUIDE_FILE, TLS_GUIDE_SECTION, TLS_SURFACE_HINT, TLS_SURFACE_LABEL, subjectRest, tlsOverview, type ExpiryLevel } from '../../lib/tls';
import { EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { CodeBlock, CopyButton } from '../../components/forms';
import { SettingsTabs } from '../Settings';
import { localized } from '../../lib/labels';
import { useT } from '../../i18n';
import '../settings.css';
import './security.css';

const LEVEL_TEXT: Record<ExpiryLevel, string> = { ok: '', warning: 'text-warning', critical: 'text-critical', expired: 'text-critical' };
const LEVEL_BADGE: Record<ExpiryLevel, string> = { ok: '', warning: 'badge-warning', critical: 'badge-critical', expired: 'badge-critical' };
const LEVEL_LABEL: Readonly<Record<ExpiryLevel, string>> = localized(['ok', 'warning', 'critical', 'expired'] as const, 'security.level.');

/** Which flags turn a surface on (tls.rs contract). */
const ENABLE_HINT: Readonly<Record<TlsSurface, string>> = localized(TLS_SURFACES, 'security.enableHint.');

/** File and URL names in the plaintext how-to (technical, never translated). */
const TECH = {
  certsDir: './certs',
  files: ['ca.pem', 'server.pem', 'client.pem', 'node-<id>.pem'],
  keySuffix: '-key.pem',
  n2: 'node-n2.pem',
  n3: 'node-n3.pem',
  addr: 'tls://host:7420',
  client: 'client.pem',
  https: 'https://',
  sha: 'SHA-256',
};

function Expiry({ cert, now }: { cert: CertificateInfo; now: number }) {
  const level = expiryLevel(cert.not_after_ms, now);
  const used = lifetimeUsed(cert.not_before_ms, cert.not_after_ms, now);
  const t = useT();
  return (
    <div className={`tls-expiry ${level}`}>
      <span className="hm-label">{level === 'expired' ? t('security.expired') : t('security.expires')}</span>
      <span className={`tls-countdown num ${LEVEL_TEXT[level]}`}>{fmtExpiryCountdown(cert.not_after_ms, now)}</span>
      <div className={`meter tls-life ${level === 'ok' ? '' : level === 'warning' ? 'warning' : 'critical'}`} role="meter" aria-label={t('security.lifetimeUsed')} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(used * 100)}>
        <span style={{ width: `${Math.max(2, used * 100)}%` }} />
      </div>
      <span className="hm-sub">
        {t('security.validRange', { from: fmtDate(cert.not_before_ms), to: fmtDate(cert.not_after_ms) })}
      </span>
      {level !== 'ok' && (
        <span className={`badge tls-level ${LEVEL_BADGE[level]}`}>
          <AlertTriangle size={11} aria-hidden /> {LEVEL_LABEL[level]}
        </span>
      )}
    </div>
  );
}

function SurfaceCard({ surface, ep, sharedWith, now }: { surface: TlsSurface; ep: TlsEndpointStatus; sharedWith: TlsSurface | null; now: number }) {
  const c = ep.certificate;
  const level = c ? expiryLevel(c.not_after_ms, now) : 'ok';
  const t = useT();
  const tone = ep.enabled && (level === 'critical' || level === 'expired') ? 'tone-critical' : ep.enabled && (level === 'warning' || ep.last_error) ? 'tone-warning' : '';
  return (
    <section className={`card tls-card ${tone} ${ep.enabled ? '' : 'off'}`} aria-label={t('security.surfaceAria', { surface: TLS_SURFACE_LABEL[surface] })}>
      <div className="card-head tls-head">
        <span className={`tls-icon ${ep.enabled ? 'on' : ''}`} aria-hidden>
          {ep.enabled ? <Lock size={15} /> : <LockOpen size={15} />}
        </span>
        <div className="tls-titles">
          <h2>{TLS_SURFACE_LABEL[surface]}</h2>
          <span className="card-sub">{TLS_SURFACE_HINT[surface]}</span>
        </div>
        <span className="spacer" />
        {ep.enabled ? (
          <>
            {ep.mutual && (
              <span className="badge" title={surface === 'cluster' ? t('security.mtlsClusterTitle') : t('security.mtlsClientTitle')}>
                <Users size={11} aria-hidden /> mTLS
              </span>
            )}
            <span className="badge badge-success">
              <ShieldCheck size={11} aria-hidden /> TLS
            </span>
          </>
        ) : (
          <span className="badge">
            <ShieldOff size={11} aria-hidden /> {t('security.plaintext')}
          </span>
        )}
      </div>
      <div className="card-body">
        {!ep.enabled ? (
          <p className="faint tls-off-note">{ENABLE_HINT[surface]}</p>
        ) : (
          <div className="tls-grid">
            {c ? <Expiry cert={c} now={now} /> : <div className="tls-expiry faint">{t('security.noCertLoaded')}</div>}
            <dl className="kv tls-kv">
              {c && (
                <>
                  <dt>{t('security.subject')}</dt>
                  <dd className="mono tls-wrap" title={c.subject}>
                    <span className="tls-cn">{commonName(c.subject) ?? c.subject}</span>
                    {subjectRest(c.subject) && <span className="faint"> · {subjectRest(c.subject)}</span>}
                  </dd>
                  {sharedWith ? (
                    <>
                      <dt>{t('security.certificate')}</dt>
                      <dd>
                        {t('security.sameLeaf', { surface: TLS_SURFACE_LABEL[sharedWith] })} <span className="faint mono">· {fmtFingerprint(c.sha256_fingerprint).slice(0, 23)}…</span>
                      </dd>
                    </>
                  ) : (
                    <>
                      <dt>{t('security.sans')}</dt>
                      <dd className="tls-sans">
                        {c.sans.length ? (
                          c.sans.map((s) => (
                            <span key={s} className="chip static mono">
                              {s}
                            </span>
                          ))
                        ) : (
                          <span className="faint">{t('security.none')}</span>
                        )}
                      </dd>
                      <dt>{t('security.issuer')}</dt>
                      <dd className="mono tls-wrap">{c.issuer}</dd>
                      <dt>{TECH.sha}</dt>
                      <dd className="tls-fp">
                        <code className="mono" title={fmtFingerprint(c.sha256_fingerprint)} aria-label={t('security.fingerprintAria', { fp: fmtFingerprint(c.sha256_fingerprint) })}>
                          {fingerprintLines(c.sha256_fingerprint).map((l, i) => (
                            <span key={i}>{l}</span>
                          ))}
                        </code>
                        <CopyButton text={fmtFingerprint(c.sha256_fingerprint)} label={t('common.copy')} />
                      </dd>
                    </>
                  )}
                </>
              )}
              <dt>{t('security.loaded')}</dt>
              <dd>
                {ep.loaded_at_ms ? (
                  <>
                    <span title={fmtDateTime(ep.loaded_at_ms)}>{fmtRelative(ep.loaded_at_ms, now)}</span>
                    <span className="faint">{t('security.reloadNote', { date: fmtDateTime(ep.loaded_at_ms) })}</span>
                  </>
                ) : (
                  <span className="faint">—</span>
                )}
              </dd>
              <dt>{t('security.handshakeFailures')}</dt>
              <dd>
                <span className="num">{fmtInt(ep.handshake_failures_total)}</span>
                <span className="faint">{t('security.handshakeNote')}</span>
              </dd>
            </dl>
          </div>
        )}
        {ep.last_error && (
          <div className="tls-reload-err" role="alert">
            <AlertTriangle size={15} />
            <span>
              <b>{t('security.reloadFailed')}</b> <span className="mono">{ep.last_error}</span>
            </span>
          </div>
        )}
      </div>
    </section>
  );
}

function PlaintextHint() {
  const t = useT();
  const mono = (s: string) => <span className="mono">{s}</span>;
  const files = TECH.files.map((f, i) => (
    <span key={f}>
      {i > 0 && (i === TECH.files.length - 1 ? ` ${t('security.plain.and')} ` : ', ')}
      {mono(f)}
    </span>
  ));
  return (
    <section className="card tls-plain">
      <div className="card-head">
        <span className="tls-icon" aria-hidden>
          <LockOpen size={15} />
        </span>
        <h2>{t('security.plain.title')}</h2>
      </div>
      <div className="card-body form-stack">
        <p className="muted tls-plain-p">
          {t('security.plain.body')}
        </p>
        <ol className="tls-steps">
          <li>
            {t.rich('security.plain.step1', { dir: mono(TECH.certsDir), files: <>{files}</>, key: <span className="mono nowrap">{TECH.keySuffix}</span> })}
            <CodeBlock title={t('security.plain.devCerts')} code={GEN_DEV_COMMAND} />
          </li>
          <li>
            {t.rich('security.plain.step2', { n2: mono(TECH.n2), n3: mono(TECH.n3) })}
            <CodeBlock title={t('security.plain.serverFlags')} code={ENABLE_TLS_FLAGS} />
          </li>
          <li>
            {t.rich('security.plain.step3', { addr: mono(TECH.addr), client: mono(TECH.client), https: mono(TECH.https) })}
            <CodeBlock title={t('security.plain.client')} code={CLIENT_TLS_FLAGS} />
          </li>
        </ol>
        <p className="faint tls-guide">
          <BookOpen size={13} aria-hidden /> <span>
            {t.rich('security.plain.guide', { section: TLS_GUIDE_SECTION, file: mono(TLS_GUIDE_FILE) })}
          </span>
        </p>
      </div>
    </section>
  );
}

function Overview({ s, now, node }: { s: TlsStatus; now: number; node: string | null }) {
  const o = tlsOverview(s, now);
  const t = useT();
  const items: { k: string; v: string; sub: string; tone?: string }[] = [
    { k: t('security.ov.encrypted'), v: `${o.enabled.length}/3`, sub: o.plaintext ? t('security.ov.plaintextEverywhere') : o.disabled.length ? t('security.ov.plaintextList', { list: o.disabled.map((d) => TLS_SURFACE_LABEL[d]).join(', ') }) : t('security.ov.allEncrypted'), tone: o.disabled.length && !o.plaintext ? 'text-warning' : undefined },
    (() => {
      const first = o.enabled
        .filter((k) => s[k].certificate)
        .sort((a, b) => s[a].certificate!.not_after_ms - s[b].certificate!.not_after_ms)[0];
      if (!first) return { k: t('security.ov.nextExpiry'), v: '—', sub: o.plaintext ? t('security.ov.noCerts') : t('security.ov.noCertLoaded') };
      const c = s[first].certificate!;
      const level = expiryLevel(c.not_after_ms, now);
      return { k: t('security.ov.nextExpiry'), v: fmtExpiryCountdown(c.not_after_ms, now), sub: `${TLS_SURFACE_LABEL[first]} · ${fmtDateTime(c.not_after_ms)}`, tone: LEVEL_TEXT[level] || undefined };
    })(),
    { k: t('security.ov.reloadErrors'), v: String(o.reloadErrors.length), sub: o.reloadErrors.length ? o.reloadErrors.map((d) => TLS_SURFACE_LABEL[d]).join(', ') : t('security.none'), tone: o.reloadErrors.length ? 'text-warning' : undefined },
    { k: t('security.ov.handshakeFailures'), v: fmtInt(o.handshakeFailures), sub: node ? t('security.ov.sinceStartNode', { node }) : t('security.ov.sinceStart') },
  ];
  return (
    <section className="card cluster-summary tls-summary" aria-label={t('security.ov.aria')}>
      {items.map((i) => (
        <div key={i.k} className="cs-item">
          <span className="hm-label">{i.k}</span>
          <span className={`cs-value num ${i.tone ?? ''}`}>{i.v}</span>
          <span className="hm-sub truncate" title={i.sub}>
            {i.sub}
          </span>
        </div>
      ))}
    </section>
  );
}

export default function SecurityPage() {
  const tls = useAsync((s) => api.tlsStatus(s), []);
  useInterval(tls.reload, 30_000);
  const now = useNow();
  const notFound = tls.error instanceof ApiError && tls.error.isNotFound;
  const s = tls.data;
  const node = s?.node_id ?? null;
  const t = useT();
  return (
    <div className="page settings-page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('common.settings')}</h1>
          <p>{t('security.pageSub')}</p>
        </div>
      </div>
      <SettingsTabs />
      {notFound ? (
        <section className="card">
          <EmptyState icon={<Lock size={20} />} title={t('security.unavailableTitle')}>
            {t('security.unavailableBody')}
          </EmptyState>
        </section>
      ) : tls.error && !s ? (
        <InlineError onRetry={tls.reload}>{errorMessage(tls.error)}</InlineError>
      ) : !s ? (
        <section className="card">
          <SkeletonRows rows={4} />
        </section>
      ) : (
        <>
          <Overview s={s} now={now} node={node} />
          {tlsOverview(s, now).plaintext && <PlaintextHint />}
          {node && <p className="faint tls-node-note">{t.rich('security.nodeNote', { node: <span className="mono">{node}</span> })}</p>}
          {TLS_SURFACES.map((k) => {
            const shared = k === 'http' && s.client.enabled && s.http.enabled && sameCertificate(s.client, s.http) ? 'client' : null;
            return <SurfaceCard key={k} surface={k} ep={s[k]} sharedWith={shared} now={now} />;
          })}
        </>
      )}
    </div>
  );
}

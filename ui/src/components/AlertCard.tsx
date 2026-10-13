import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { AlertOctagon, AlertTriangle, ArrowRight, Inbox, Lightbulb, RotateCcw, ShieldAlert, Users, Wrench } from 'lucide-react';
import { api } from '../api/client';
import { HEADERS, type Alert, type DeadLetterView } from '../api/types';
import { fmtDateTime, fmtRelative } from '../lib/format';
import { ALERT_KIND_LABEL, channelPath } from '../lib/labels';
import { alertInstance, alertText } from '../lib/alertText';
import { reasonText } from '../lib/dlqReason';
import { useT } from '../i18n';
import { GateButton } from './auth/Gate';
import { ResetOffsetsDialog } from './consumers/ResetOffsetsDialog';
import './alert-card.css';

const LAG_KINDS: ReadonlySet<Alert['kind']> = new Set(['lag_growth', 'consumer_stalled', 'no_consumers']);

/** Deep links built from the alert subject. */
function alertLinks(a: Alert) {
  const ch = a.channel ? encodeURIComponent(a.channel) : '';
  const gr = a.group ? encodeURIComponent(a.group) : '';
  const instance = alertInstance(a);
  return {
    group: a.channel ? `${channelPath(a.channel, 'consumers')}${a.group ? `#${gr}` : ''}` : null,
    dlq: a.channel ? `/dlq?channel=${ch}${a.group ? `&group=${gr}` : ''}` : '/dlq',
    instance: instance && a.channel && a.group ? `/consumers?instance=${encodeURIComponent(instance)}&channel=${ch}&group=${gr}` : null,
  };
}

/** The main next step for an alert (dashboard rows show only this one). */
function primaryLink(a: Alert): { to: string; label: 'alerts.pb.openCause' | 'alerts.pb.openInstance' | 'alerts.pb.openGroup' | 'alerts.inspect' } | null {
  const l = alertLinks(a);
  if (a.kind === 'dlq_growth') return { to: l.dlq, label: 'alerts.pb.openCause' };
  if (a.kind === 'slow_instance' && l.instance) return { to: l.instance, label: 'alerts.pb.openInstance' };
  if (LAG_KINDS.has(a.kind) && l.group) return { to: l.group, label: 'alerts.pb.openGroup' };
  return a.link ? { to: a.link, label: 'alerts.inspect' } : null;
}

/** Compact alert row for the dashboard strip. */
export function AlertRow({ alert: a, now }: { alert: Alert; now: number }) {
  const t = useT();
  const x = alertText(a);
  const next = primaryLink(a);
  return (
    <li className={`alert-row ${a.severity}`}>
      <span className={`sev-bar ${a.severity}`} aria-hidden />
      <div className="ar-main">
        <div className="ar-title">
          <span className="sr-only">{t(a.severity === 'critical' ? 'labels.health.critical' : 'labels.health.warning')}: </span>
          {x.title}
          {a.channel && a.kind !== 'slow_instance' ? (
            <span className="mono faint">
              {' '}
              · {a.channel}
              {a.group ? `/${a.group}` : ''}
            </span>
          ) : null}
        </div>
        <div className="ar-detail">{x.detail}</div>
      </div>
      <span className="ar-since faint">{fmtRelative(a.since_ms, now)}</span>
      {next && (
        <Link to={next.to} className="btn btn-sm">
          {t(next.label)}
        </Link>
      )}
    </li>
  );
}

interface DlqCause {
  reason: string;
  count: number;
  total: number;
  sample: DeadLetterView;
}

/** Top dead-letter reason of a group (from the newest entries of its channel). */
function useDlqCause(channel: string | null, group: string | null): { cause: DlqCause | null; loading: boolean } {
  const [state, setState] = useState<{ key: string; cause: DlqCause | null } | null>(null);
  const key = `${channel}/${group}`;
  useEffect(() => {
    if (!channel) return;
    const ctrl = new AbortController();
    api
      .dlq({ channel, limit: 200 }, ctrl.signal)
      .then((p) => {
        const mine = p.entries.filter((e) => !group || e.group === group);
        const counts = new Map<string, number>();
        for (const e of mine) counts.set(e.reason, (counts.get(e.reason) ?? 0) + 1);
        const top = [...counts.entries()].sort((x, y) => y[1] - x[1])[0];
        const sample = top ? mine.find((e) => e.reason === top[0]) : undefined;
        setState({ key, cause: top && sample ? { reason: top[0], count: top[1], total: mine.length, sample } : null });
      })
      .catch(() => {
        if (!ctrl.signal.aborted) setState({ key, cause: null });
      });
    return () => ctrl.abort();
  }, [channel, group, key]);
  return { cause: state?.key === key ? state.cause : null, loading: state?.key !== key };
}

function DlqPlaybook({ a }: { a: Alert }) {
  const t = useT();
  const { cause, loading } = useDlqCause(a.channel, a.group);
  const base = `/dlq?channel=${encodeURIComponent(a.channel ?? '')}${a.group ? `&group=${encodeURIComponent(a.group)}` : ''}`;
  if (loading) return <p className="pb-line faint">{t('alerts.pb.loading')}</p>;
  if (!cause)
    return (
      <div className="pb-actions">
        <p className="pb-line faint">{t('alerts.pb.noDlq')}</p>
        <Link to={base} className="btn btn-sm">
          <Inbox size={13} /> {t('alerts.pb.openCause')}
        </Link>
      </div>
    );
  const why = reasonText(cause.reason);
  const permanent = why.permanent;
  const reasonLink = `${base}&reason=${encodeURIComponent(cause.reason)}`;
  const s = cause.sample;
  return (
    <>
      <dl className="pb-facts">
        <div>
          <dt>{t('alerts.pb.topReason')}</dt>
          <dd>
            <span className={`text-critical ${why.coded ? '' : 'mono'}`} title={why.coded ? cause.reason : undefined}>
              {why.title}
            </span> <span className="faint">· {t('alerts.pb.reasonCount', { count: cause.count })}</span>
          </dd>
        </div>
        <div>
          <dt>{t('alerts.pb.sample')}</dt>
          <dd className="truncate">
            {s.message.headers[HEADERS.EVENT_TYPE] ?? t('dlq.col.messageFallback')} <span className="faint mono">#{s.id}</span>
            {s.message.key ? <span className="faint mono"> · {s.message.key}</span> : null}
            <span className="faint"> · {s.attempts.length}×</span>
          </dd>
        </div>
      </dl>
      <p className={`pb-hint ${permanent ? 'permanent' : ''}`}>
        {permanent ? <ShieldAlert size={14} /> : <Wrench size={14} />}
        <span>{t(permanent ? 'alerts.pb.nonTransient' : 'alerts.pb.transient')}</span>
      </p>
      <div className="pb-actions">
        <Link to={reasonLink} className="btn btn-sm btn-primary">
          <Inbox size={13} /> {t('alerts.pb.openCause')} <ArrowRight size={13} />
        </Link>
        {/* Secondary: opens the DLQ page's retry confirmation for this reason; nothing runs from here. */}
        <Link to={`${reasonLink}&action=retry`} className="btn btn-sm btn-ghost">
          <RotateCcw size={13} /> {t('alerts.pb.retryAfterFix')}
        </Link>
      </div>
    </>
  );
}

function Playbook({ a }: { a: Alert }) {
  const t = useT();
  const [resetOpen, setResetOpen] = useState(false);
  const l = alertLinks(a);
  if (a.kind === 'dlq_growth' && a.channel) return <DlqPlaybook a={a} />;
  if (a.kind === 'slow_instance' && l.instance)
    return (
      <>
        <p className="pb-hint">
          <Wrench size={14} />
          <span>{t('alerts.pb.slowHint')}</span>
        </p>
        <div className="pb-actions">
          <Link to={l.instance} className="btn btn-sm btn-primary">
            {t('alerts.pb.openInstance')} <ArrowRight size={13} />
          </Link>
          {l.group && (
            <Link to={l.group} className="btn btn-sm btn-ghost">
              <Users size={13} /> {t('alerts.pb.openGroup')}
            </Link>
          )}
        </div>
      </>
    );
  if (LAG_KINDS.has(a.kind) && a.channel && a.group && l.group)
    return (
      <>
        <p className="pb-hint">
          <Wrench size={14} />
          <span>{t('alerts.pb.lagHint')}</span>
        </p>
        <div className="pb-actions">
          <Link to={l.group} className="btn btn-sm btn-primary">
            <Users size={13} /> {t('alerts.pb.openGroup')} <ArrowRight size={13} />
          </Link>
          {/* Opens the existing confirmation dialog for this group; nothing is reset automatically. */}
          <GateButton need="operate" className="btn btn-sm btn-ghost" onClick={() => setResetOpen(true)}>
            <RotateCcw size={13} /> {t('alerts.pb.resetOffsets')}
          </GateButton>
        </div>
        <ResetOffsetsDialog channel={a.channel} group={a.group} open={resetOpen} onClose={() => setResetOpen(false)} />
      </>
    );
  return a.link ? (
    <div className="pb-actions">
      <Link to={a.link} className="btn btn-sm">
        {t('alerts.inspect')} <ArrowRight size={13} />
      </Link>
    </div>
  ) : null;
}

/** Full alert card for the Alerts center: what happened → likely cause → what to do. */
export function AlertCard({ alert: a, now }: { alert: Alert; now: number }) {
  const t = useT();
  const x = alertText(a);
  const sev = a.severity === 'critical' ? t('labels.health.critical') : t('labels.health.warning');
  const Icon = a.severity === 'critical' ? AlertOctagon : AlertTriangle;
  return (
    <article className={`card alert-card tone-${a.severity}`} aria-label={t('alerts.cardAria', { severity: sev, title: x.title })}>
      <div className="ac-head">
        <span className={`ac-icon ${a.severity}`} aria-hidden>
          <Icon size={16} />
        </span>
        <div className="ac-titles">
          <h3>{x.title}</h3>
          <div className="ac-meta">
            <span className={`badge ${a.severity === 'critical' ? 'badge-critical' : 'badge-warning'}`}>{sev}</span>
            <span className="faint">{ALERT_KIND_LABEL[a.kind] ?? a.kind}</span>
            {a.channel && (
              <span className="faint">
                · <span className="mono">{a.channel}</span>
                {a.group ? <span className="mono">/{a.group}</span> : null}
              </span>
            )}
          </div>
        </div>
        <span className="faint ac-since" title={fmtDateTime(a.since_ms)}>
          {t('alerts.since', { when: fmtRelative(a.since_ms, now) })}
        </span>
      </div>
      <p className="ac-detail">{x.detail}</p>
      {x.reason && (
        <div className="ac-reason">
          <Lightbulb size={14} />
          <span>
            <b>{t('alerts.possibleReason')}</b> {x.reason}
          </span>
        </div>
      )}
      <div className="ac-playbook">
        <Playbook a={a} />
      </div>
    </article>
  );
}

import { useState } from 'react';
import { Link, useSearchParams } from 'react-router';
import { BookOpen, Clock, Send, X } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import type { ScheduledMessage } from '../../api/types';
import { useAsync, useInterval, useNow } from '../../lib/hooks';
import { fmtBytes, fmtCount, fmtDateTime, fmtRelative } from '../../lib/format';
import { channelPath } from '../../lib/labels';
import { toast } from '../../store/ui';
import { ConfirmDialog, EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { CodeBlock } from '../../components/forms';
import { GateButton } from '../../components/auth/Gate';
import { useT } from '../../i18n';
import '../routing/routing-page.css';
import '../channel/channel.css';
import './scheduled.css';

const HOW_TO = `// SDK (Rust)
producer.send(message(body).delay(Duration::from_secs(600))).await?;
producer.send(message(body).deliver_at(nine_am)).await?;

# REST
curl -X POST $FLUXMQ/api/channels/orders/publish -H 'content-type: application/json' \\
  -d '{"messages":[{"payload":{"encoding":"utf8","data":"…"},"delay_ms":600000}]}'`;

const idKey = (m: ScheduledMessage) => `${m.id.partition}/${m.id.offset}`;

export default function ScheduledPage() {
  const t = useT();
  const now = useNow();
  const [params, setParams] = useSearchParams();
  const channel = params.get('channel') ?? '';
  const [bump, setBump] = useState(0);
  const data = useAsync((signal) => api.scheduledList({ channel: channel || undefined, limit: 200 }, signal), [channel, bump]);
  useInterval(data.reload, 5000);
  const [confirm, setConfirm] = useState<ScheduledMessage | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const unsupported = data.error instanceof ApiError && data.error.isNotFound;
  const d = data.data;
  const nodes = new Set(d?.items.map((i) => i.node)).size > 1;

  const act = async (m: ScheduledMessage, release: boolean) => {
    setBusy(idKey(m));
    try {
      await (release ? api.releaseScheduled(m.id) : api.cancelScheduled(m.id));
      toast('success', release ? t('scheduled.releasedToast', { channel: m.channel }) : t('scheduled.cancelledToast'));
    } catch (err) {
      toast('error', err instanceof ApiError && err.isNotFound ? t('scheduled.goneToast') : errorMessage(err));
    } finally {
      setBusy(null);
      setBump((b) => b + 1);
    }
  };

  return (
    <div className="page scheduled-page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('scheduled.title')}</h1>
          <p>{t('scheduled.subtitle')}</p>
        </div>
        <div className="page-actions">
          <Link to="/docs/delayed" className="btn">
            <BookOpen size={15} /> {t('scheduled.docs')}
          </Link>
        </div>
      </div>

      {unsupported ? (
        <section className="card">
          <EmptyState icon={<Clock size={20} />} title={t('scheduled.unsupportedTitle')}>
            {t('scheduled.unsupportedBody')}
          </EmptyState>
        </section>
      ) : data.error && !d ? (
        <InlineError onRetry={data.reload}>{t('scheduled.failed')}</InlineError>
      ) : !d ? (
        <section className="card">
          <SkeletonRows rows={4} />
        </section>
      ) : (
        <>
          <section className="card ch-metrics sched-kpis" aria-label={t('scheduled.kpi.aria')}>
            <div className="hm">
              <span className="hm-label">{t('scheduled.kpi.pending')}</span>
              <span className="hm-value">
                <span className="num">{fmtCount(d.total)}</span>
              </span>
              <span className="hm-sub">{t('scheduled.kpi.channels', { count: d.channels.length })}</span>
            </div>
            <div className="hm">
              <span className="hm-label">{t('scheduled.kpi.next')}</span>
              <span className="hm-value">
                <span className="num">{d.items[0] ? fmtRelative(d.items[0].due_ms, now) : '—'}</span>
              </span>
              <span className="hm-sub">{d.items[0] ? fmtDateTime(d.items[0].due_ms) : t('scheduled.kpi.nothing')}</span>
            </div>
            <div className="hm">
              <span className="hm-label">{t('scheduled.kpi.released')}</span>
              <span className="hm-value">
                <span className="num">{fmtCount(d.released)}</span>
              </span>
              <span className="hm-sub">{t('scheduled.kpi.sinceStart')}</span>
            </div>
            <div className="hm">
              <span className="hm-label">{t('scheduled.kpi.cancelled')}</span>
              <span className="hm-value">
                <span className="num">{fmtCount(d.cancelled)}</span>
              </span>
              <span className="hm-sub">{t('scheduled.kpi.sinceStart')}</span>
            </div>
          </section>

          {d.channels.length > 0 && (
            <div className="sched-chips" role="group" aria-label={t('scheduled.filter')}>
              <button type="button" className="tchip" aria-pressed={!channel} onClick={() => setParams({}, { replace: true })}>
                {t('scheduled.allChannels')}
              </button>
              {d.channels.map((c) => (
                <button key={c.channel} type="button" className="tchip" aria-pressed={channel === c.channel} onClick={() => setParams({ channel: c.channel }, { replace: true })}>
                  {c.channel} <span className="faint num">{fmtCount(c.count)}</span>
                </button>
              ))}
            </div>
          )}

          <section className="card">
            <div className="card-head">
              <h2>{t('scheduled.list.title')}</h2>
              <span className="card-sub">{t('scheduled.list.sub')}</span>
            </div>
            {d.items.length === 0 ? (
              <EmptyState compact icon={<Clock size={18} />} title={t('scheduled.emptyTitle')}>
                {t('scheduled.emptyBody')}
              </EmptyState>
            ) : (
              <div className="table-wrap">
                <table className="table sched-table">
                  <thead>
                    <tr>
                      <th>{t('scheduled.col.when')}</th>
                      <th>{t('scheduled.col.channel')}</th>
                      <th>{t('scheduled.col.message')}</th>
                      <th className="r">{t('scheduled.col.size')}</th>
                      <th>{t('scheduled.col.scheduled')}</th>
                      {nodes && <th>{t('scheduled.col.node')}</th>}
                      <th aria-label={t('scheduled.col.actions')} />
                    </tr>
                  </thead>
                  <tbody>
                    {d.items.map((m) => (
                      <tr key={idKey(m)}>
                        <td title={fmtDateTime(m.due_ms)}>
                          <b className={m.due_ms <= now ? 'text-warning' : ''}>{m.due_ms <= now ? t('scheduled.dueNow') : fmtRelative(m.due_ms, now)}</b>
                          <div className="faint sched-sub">{fmtDateTime(m.due_ms)}</div>
                        </td>
                        <td>
                          <Link to={channelPath(m.channel)} className="mono">
                            {m.channel}
                          </Link>
                        </td>
                        <td>
                          {m.event_type ?? <span className="faint">{t('scheduled.noType')}</span>}
                          <div className="faint sched-sub mono">{m.key ? t('scheduled.key', { key: m.key }) : t('scheduled.noKey')}</div>
                        </td>
                        <td className="r num">{fmtBytes(m.size)}</td>
                        <td className="faint" title={fmtDateTime(m.scheduled_at_ms)}>
                          {fmtRelative(m.scheduled_at_ms, now)}
                        </td>
                        {nodes && <td className="mono faint">{m.node}</td>}
                        <td className="r sched-actions">
                          <GateButton need="operate" className="btn btn-sm" onClick={() => void act(m, true)} disabled={busy === idKey(m)}>
                            <Send size={13} /> {t('scheduled.sendNow')}
                          </GateButton>
                          <GateButton need="operate" className="btn btn-sm btn-ghost" onClick={() => setConfirm(m)} disabled={busy === idKey(m)} aria-label={t('scheduled.cancelAria', { channel: m.channel })}>
                            <X size={13} /> {t('scheduled.cancel')}
                          </GateButton>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <section className="card">
            <div className="card-head">
              <h2>{t('scheduled.how.title')}</h2>
            </div>
            <div className="card-body">
              <p className="faint sched-how">{t('scheduled.how.body')}</p>
              <CodeBlock code={HOW_TO} />
            </div>
          </section>
        </>
      )}

      <ConfirmDialog
        open={confirm != null}
        onClose={() => setConfirm(null)}
        title={t('scheduled.confirm.title')}
        confirmLabel={t('scheduled.confirm.ok')}
        cancelLabel={t('scheduled.confirm.keep')}
        danger
        onConfirm={async () => {
          if (confirm) await act(confirm, false);
          setConfirm(null);
        }}
      >
        {confirm && t('scheduled.confirm.body', { channel: confirm.channel, when: fmtDateTime(confirm.due_ms) })}
      </ConfirmDialog>
    </div>
  );
}

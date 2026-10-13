import { useState } from 'react';
import { Link, useNavigate } from 'react-router';
import { ArrowRight, Info, Loader2, PauseCircle, Plus, Search, Send, Webhook } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { webhookGroup, type WebhookSubscription } from '../../api/types';
import { fmtCount, fmtMs, fmtRelative } from '../../lib/format';
import { useNow } from '../../lib/hooks';
import { channelPath } from '../../lib/labels';
import { shortError, statusTone, urlHost } from '../../lib/webhookForm';
import { useWebhooks, useWebhooksPolling, loadWebhookStats } from '../../store/webhooks';
import { toast } from '../../store/ui';
import { EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import '../routing/routing-page.css';
import './webhooks.css';
import { useT } from '../../i18n';

/** Consumer group naming pattern of webhooks (technical, untranslated). */
const GROUP_PATTERN = '$webhook:<name>';

/** Last HTTP status (or short error when no response); the full detail is in the tooltip. */
export function StatusCode({ status, error, detail }: { status: number | null; error?: string | null; detail?: string | null }) {
  const tip = detail || error || undefined;
  if (status == null)
    return error ? (
      <span className="status-code tone-critical" title={tip}>
        {shortError(error)}
      </span>
    ) : (
      <span className="faint">—</span>
    );
  const tone = statusTone(status);
  return (
    <span className={`status-code mono ${tone ? `tone-${tone}` : ''}`} title={tip}>
      {status}
    </span>
  );
}

function Row({ w }: { w: WebhookSubscription }) {
  const t = useT();
  const s = useWebhooks((st) => st.stats[w.id]);
  const navigate = useNavigate();
  const now = useNow();
  const [testing, setTesting] = useState(false);
  const test = async (e: React.MouseEvent) => {
    e.stopPropagation();
    setTesting(true);
    try {
      const r = await api.testWebhook(w.id);
      if (r.ok) toast('success', t('webhooks.testOkNamed', { name: w.name, status: r.status ?? 200, ms: r.latency_ms }));
      else toast('error', t('webhooks.testFailNamed', { name: w.name, error: r.error ? shortError(r.error) : `HTTP ${r.status}`, ms: r.latency_ms }));
      void loadWebhookStats();
    } catch (err) {
      if (!(err instanceof ApiError && err.isForbidden)) toast('error', errorMessage(err));
    } finally {
      setTesting(false);
    }
  };
  const failing = !!s && w.enabled && (s.breaker != null || s.last_error != null || statusTone(s.last_status) != null);
  return (
    <tr className={`clickable ${!w.enabled ? 'row-muted' : failing ? 'row-critical' : ''}`} onClick={() => navigate(`/webhooks/${encodeURIComponent(w.id)}`)}>
      <td>
        <Link to={`/webhooks/${encodeURIComponent(w.id)}`} className="wh-name" onClick={(e) => e.stopPropagation()}>
          {w.name}
        </Link>
        <div className="faint wh-sub">
          {w.enabled ? (w.format === 'raw' ? t('webhooks.rawBody') : t('webhooks.envelope')) : t('webhooks.paused')}
          {w.batch_max > 1 ? ` · ${t('webhooks.batchN', { n: w.batch_max })}` : ''}
          {w.filter.length ? ` · ${t('webhooks.filterConditions', { count: w.filter.length })}` : ''}
        </div>
      </td>
      <td>
        <span className="wh-flow">
          <Link to={channelPath(w.channel)} className="chip static" onClick={(e) => e.stopPropagation()}>
            {w.channel}
          </Link>
          <ArrowRight size={13} className="faint" aria-hidden />
          <span className="mono wh-host truncate" title={w.url}>
            {urlHost(w.url)}
          </span>
        </span>
      </td>
      <td className="r num">{s ? fmtCount(s.delivered) : '—'}</td>
      <td className={`r num ${s && s.failed > 0 ? 'text-critical' : ''}`}>{s ? fmtCount(s.failed) : '—'}</td>
      <td className="r num faint">{s ? fmtCount(s.skipped_by_filter) : '—'}</td>
      <td className={`r num ${s && s.latency_p99_ms > w.timeout_ms * 0.8 ? 'text-warning' : ''}`}>{s && (s.latency_p99_ms > 0 || s.delivered > 0) ? fmtMs(s.latency_p99_ms) : '—'}</td>
      <td>
        {s ? (
          <span className="wh-last">
            {s.breaker && w.enabled && (
              <span className="wh-breaker-chip" title={t('webhooks.breaker.chipHint', { count: s.breaker.failures })}>
                <PauseCircle size={12} /> {t('webhooks.breaker.chip')} ·{' '}
              </span>
            )}
            <StatusCode status={s.last_status} error={s.last_error} detail={s.last_error_detail} />
            {s.last_delivery_ms && <span className="faint"> · {fmtRelative(s.last_delivery_ms, now)}</span>}
          </span>
        ) : (
          '—'
        )}
      </td>
      <td className="r num">
        {s ? (
          <>
            {s.in_flight}
            <span className="faint">/{w.max_in_flight}</span>
          </>
        ) : (
          '—'
        )}
      </td>
      <td className="r">
        <GateButton need="admin" className="btn btn-sm" onClick={test} disabled={testing} aria-label={t('webhooks.sendTestTo', { name: w.name })}>
          {testing ? <Loader2 size={13} className="spin" /> : <Send size={13} />} {t('common.test')}
        </GateButton>
      </td>
    </tr>
  );
}

export default function WebhooksPage() {
  const t = useT();
  useWebhooksPolling(5000);
  const list = useWebhooks((s) => s.list);
  const error = useWebhooks((s) => s.error);
  const unsupported = useWebhooks((s) => s.unsupported);
  const failing = useWebhooks((s) => s.failing);
  const stats = useWebhooks((s) => s.stats);
  const navigate = useNavigate();
  const [q, setQ] = useState('');
  const all = list ?? [];
  const needle = q.trim().toLowerCase();
  const shown = all.filter((w) => !needle || w.name.includes(needle) || w.channel.includes(needle) || w.url.toLowerCase().includes(needle));
  const failingList = all.filter((w) => {
    const s = stats[w.id];
    return w.enabled && s && (s.breaker != null || s.last_error != null || statusTone(s.last_status) != null);
  });

  return (
    <div className="page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('webhooks.title')}</h1>
          <p>{t('webhooks.subtitle')}</p>
        </div>
        <div className="page-actions">
          <GateButton need="admin" className="btn btn-primary" onClick={() => navigate('/webhooks/new')} disabled={unsupported}>
            <Plus size={15} /> {t('webhooks.create')}
          </GateButton>
        </div>
      </div>

      {unsupported ? (
        <section className="card">
          <EmptyState icon={<Webhook size={20} />} title={t('webhooks.unsupportedTitle')}>
            {t('webhooks.unsupportedBody')}
          </EmptyState>
        </section>
      ) : error && !list ? (
        <InlineError>{error}</InlineError>
      ) : !list ? (
        <section className="card">
          <SkeletonRows rows={3} />
        </section>
      ) : all.length === 0 ? (
        <section className="card">
          <EmptyState
            icon={<Webhook size={20} />}
            title={t('webhooks.emptyTitle')}
            actions={
              <GateButton need="admin" className="btn btn-primary" onClick={() => navigate('/webhooks/new')}>
                <Plus size={15} /> {t('webhooks.emptyCreate')}
              </GateButton>
            }
          >
            {t.rich('webhooks.emptyBody', { group: <span className="mono">{GROUP_PATTERN}</span> })}
          </EmptyState>
        </section>
      ) : (
        <>
          <div className="routing-summary wh-summary">
            <span>
              {t.rich('webhooks.summaryCount', { count: all.length, n: <b className="num">{all.length}</b> })} · {t.rich('webhooks.summaryEnabled', { n: <b className="num">{all.filter((w) => w.enabled).length}</b> })}
            </span>
            {failing > 0 && (
              <span className="text-critical">
                {t.rich('webhooks.summaryFailing', { n: <b className="num">{failing}</b> })} ·{' '}
                {failingList.map((w, i) => (
                  <span key={w.id}>
                    {i > 0 && ', '}
                    <Link to={`/dlq?channel=${encodeURIComponent(w.channel)}`} className="btn-link">
                      {t('webhooks.deadLettersIn', { channel: w.channel })}
                    </Link>
                  </span>
                ))}
              </span>
            )}
            <span className="faint routing-note">
              <Info size={13} /> {t('webhooks.countersNote')}
            </span>
          </div>
          <section className="card">
            <div className="card-head routes-toolbar">
              <div className="input-group routes-search">
                <Search size={14} />
                <input className="input" placeholder={t('webhooks.filterPlaceholder')} value={q} onChange={(e) => setQ(e.target.value)} aria-label={t('webhooks.filterAria')} />
              </div>
            </div>
            {shown.length ? (
              <div className="table-wrap">
                <table className="table wh-table">
                  <thead>
                    <tr>
                      <th>{t('webhooks.col.name')}</th>
                      <th>{t('webhooks.col.flow')}</th>
                      <th className="r">{t('webhooks.col.delivered')}</th>
                      <th className="r">{t('webhooks.col.failed')}</th>
                      <th className="r" title={t('webhooks.col.skippedHint')}>
                        {t('webhooks.col.skipped')}
                      </th>
                      <th className="r">p99</th>
                      <th title={t('webhooks.col.lastStatus')}>{t('webhooks.col.lastStatus')}</th>
                      <th className="r" title={t('webhooks.col.inFlight')}>{t('webhooks.col.inFlight')}</th>
                      <th aria-label={t('webhooks.col.actions')} />
                    </tr>
                  </thead>
                  <tbody>
                    {shown.map((w) => (
                      <Row key={w.id} w={w} />
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyState compact icon={<Search size={18} />} title={t('webhooks.noMatch')} />
            )}
          </section>
          <p className="faint wh-foot">
            {t.rich('webhooks.footGroups', {
              groups: (
                <>
                  {all.slice(0, 3).map((w, i) => (
                    <span key={w.id}>
                      {i > 0 && ', '}
                      <Link to={`${channelPath(w.channel, 'consumers')}#${encodeURIComponent(webhookGroup(w.name))}`} className="btn-link mono">
                        {webhookGroup(w.name)}
                      </Link>
                    </span>
                  ))}
                  {all.length > 3 ? ' …' : ''}
                </>
              ),
            })}
          </p>
        </>
      )}
    </div>
  );
}

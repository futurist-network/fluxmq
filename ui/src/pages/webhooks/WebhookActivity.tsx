import { useMemo, useState } from 'react';
import type uPlot from 'uplot';
import { PauseCircle } from 'lucide-react';
import { api } from '../../api/client';
import type { WebhookAttempt, WebhookAttemptOutcome, WebhookBreaker, WebhookHistory, WebhookHistoryRange } from '../../api/types';
import { useAsync, useInterval, useNow } from '../../lib/hooks';
import { fmtClock, fmtCount, fmtDateTime, fmtMs, fmtRelative } from '../../lib/format';
import { shortError } from '../../lib/webhookForm';
import { EmptyState, InlineError, Segmented, Skeleton } from '../../components/ui';
import { UChart, type ChartSource } from '../../components/charts/UChart';
import { StatusCode } from './WebhooksPage';
import { useT, type TKey } from '../../i18n';

/** Shown while the circuit breaker holds delivery (on any node). */
export function BreakerBanner({ b }: { b: WebhookBreaker }) {
  const t = useT();
  const now = useNow();
  const secs = Math.max(0, Math.ceil((b.next_probe_ms - now) / 1000));
  return (
    <div className="wh-breaker" role="status">
      <PauseCircle size={16} />
      <div>
        <b>{t('webhooks.breaker.title')}</b> {t('webhooks.breaker.body', { count: b.failures, since: fmtClock(b.since_ms) })}
        <div className="faint">
          {secs > 0 ? t('webhooks.breaker.nextProbe', { secs }) : t('webhooks.breaker.probing')}
          {b.nodes > 1 ? ` · ${t('webhooks.breaker.nodes', { count: b.nodes })}` : ''}
        </div>
      </div>
    </div>
  );
}

const RANGES: WebhookHistoryRange[] = ['15m', '1h', '24h'];

function staticSource(data: uPlot.AlignedData): ChartSource {
  return { get: () => data, subscribe: () => () => {} };
}

/** Delivered / failed messages per bucket and response latency, from node memory. */
export function ActivityCard({ id }: { id: string }) {
  const t = useT();
  const [range, setRange] = useState<WebhookHistoryRange>('1h');
  const h = useAsync((signal) => api.webhookHistory(id, range, signal), [id, range]);
  useInterval(h.reload, range === '24h' ? 60_000 : 10_000);
  const data = h.data;
  const counts = useMemo(() => (data ? staticSource([data.points.map((p) => p.ts_ms / 1000), data.points.map((p) => p.delivered), data.points.map((p) => p.failed)]) : null), [data]);
  const latency = useMemo(() => (data ? staticSource([data.points.map((p) => p.ts_ms / 1000), data.points.map((p) => (p.latency_max_ms > 0 ? p.latency_avg_ms : null)), data.points.map((p) => (p.latency_max_ms > 0 ? p.latency_max_ms : null))]) : null), [data]);
  const totals = data ? sums(data) : null;
  return (
    <section className="card wh-activity">
      <div className="card-head">
        <h2>{t('webhooks.activity.title')}</h2>
        {totals && (
          <span className="card-sub">
            {t('webhooks.activity.totals', { delivered: fmtCount(totals.delivered), failed: fmtCount(totals.failed) })}
            {totals.avg != null ? ` · ${t('webhooks.activity.avgLatency', { ms: fmtMs(totals.avg) })}` : ''}
          </span>
        )}
        <span className="spacer" />
        <Segmented value={range} onChange={setRange} label={t('webhooks.activity.range')} options={RANGES.map((r) => ({ value: r, label: t(`webhooks.activity.range.${r}` as TKey) }))} />
      </div>
      <div className="card-body">
        {h.error && !data ? (
          <InlineError onRetry={h.reload}>{t('webhooks.activity.failed')}</InlineError>
        ) : !data || !counts || !latency ? (
          <Skeleton h={200} />
        ) : totals && totals.delivered + totals.failed === 0 ? (
          <EmptyState compact icon={<PauseCircle size={18} />} title={t('webhooks.activity.empty')}>
            {t('webhooks.activity.memoryNote')}
          </EmptyState>
        ) : (
          <>
            <UChart
              source={counts}
              height={150}
              seconds={range !== '24h'}
              yFormat={(v) => fmtCount(v)}
              ariaLabel={t('webhooks.activity.messagesAria')}
              series={[
                { label: t('webhooks.activity.delivered'), color: '--series-2', fill: true },
                { label: t('webhooks.activity.failedSeries'), color: '--critical' },
              ]}
            />
            <UChart
              source={latency}
              height={100}
              seconds={range !== '24h'}
              yFormat={(v) => fmtMs(v)}
              ariaLabel={t('webhooks.activity.latencyAria')}
              series={[
                { label: t('webhooks.activity.latencyAvg'), color: '--series-1' },
                { label: t('webhooks.activity.latencyMax'), color: '--series-3', dash: [4, 3] },
              ]}
            />
            <p className="faint wh-activity-note">{t('webhooks.activity.note', { bucket: data.bucket_ms >= 60_000 ? t('webhooks.activity.bucketMin', { n: data.bucket_ms / 60_000 }) : t('webhooks.activity.bucketSec', { n: data.bucket_ms / 1000 }) })}</p>
          </>
        )}
      </div>
    </section>
  );
}

function sums(h: WebhookHistory): { delivered: number; failed: number; avg: number | null } {
  let delivered = 0;
  let failed = 0;
  let lat = 0;
  let n = 0;
  for (const p of h.points) {
    delivered += p.delivered;
    failed += p.failed;
    if (p.latency_max_ms > 0) {
      lat += p.latency_avg_ms;
      n++;
    }
  }
  return { delivered, failed, avg: n ? lat / n : null };
}



const OUTCOME_TONE: Record<WebhookAttemptOutcome, string> = {
  delivered: 'badge-success',
  retry: 'badge-warning',
  dead: 'badge-critical',
  probe_ok: 'badge-success',
  probe_failed: '',
};

const SHOWN = 15;

/** The last requests (newest first), over all nodes. */
export function AttemptsCard({ id }: { id: string }) {
  const t = useT();
  const now = useNow();
  const [all, setAll] = useState(false);
  const a = useAsync((signal) => api.webhookAttempts(id, signal), [id]);
  useInterval(a.reload, 5000);
  const list = a.data ?? [];
  const nodes = new Set(list.map((x) => x.node)).size > 1;
  const shown = all ? list : list.slice(0, SHOWN);
  return (
    <section className="card wh-attempts">
      <div className="card-head">
        <h2>{t('webhooks.attempts.title')}</h2>
        <span className="card-sub">{t('webhooks.attempts.sub')}</span>
      </div>
      {a.error && !a.data ? (
        <div className="card-body">
          <InlineError onRetry={a.reload}>{t('webhooks.attempts.failed')}</InlineError>
        </div>
      ) : !a.data ? (
        <div className="card-body">
          <Skeleton h={120} />
        </div>
      ) : list.length === 0 ? (
        <EmptyState compact icon={<PauseCircle size={18} />} title={t('webhooks.attempts.empty')}>
          {t('webhooks.activity.memoryNote')}
        </EmptyState>
      ) : (
        <>
          <div className="table-wrap">
            <table className="table wh-attempts-table">
              <thead>
                <tr>
                  <th>{t('webhooks.attempts.when')}</th>
                  <th>{t('webhooks.attempts.message')}</th>
                  <th className="r">{t('webhooks.attempts.attempt')}</th>
                  <th>{t('webhooks.attempts.result')}</th>
                  <th className="r">{t('webhooks.attempts.latency')}</th>
                  {nodes && <th>{t('webhooks.attempts.node')}</th>}
                </tr>
              </thead>
              <tbody>
                {shown.map((x, i) => (
                  <AttemptRow key={`${x.node}-${x.at_ms}-${x.delivery_id}-${i}`} x={x} now={now} nodes={nodes} />
                ))}
              </tbody>
            </table>
          </div>
          {list.length > SHOWN && (
            <div className="wh-attempts-foot">
              <button type="button" className="btn-link" onClick={() => setAll(!all)}>
                {all ? t('webhooks.attempts.showLess') : t('webhooks.attempts.showAll', { n: list.length })}
              </button>
            </div>
          )}
        </>
      )}
    </section>
  );
}

function AttemptRow({ x, now, nodes }: { x: WebhookAttempt; now: number; nodes: boolean }) {
  const t = useT();
  const probe = x.outcome === 'probe_ok' || x.outcome === 'probe_failed';
  const tip = x.error_detail ?? x.error ?? undefined;
  return (
    <tr>
      <td className="faint wh-when" title={fmtDateTime(x.at_ms)}>
        {fmtRelative(x.at_ms, now)}
      </td>
      <td>
        {probe ? (
          <span className="faint">{t('webhooks.attempts.probe')}</span>
        ) : (
          <span className="mono wh-delivery">
            {x.delivery_id}
            {x.messages > 1 && <span className="faint"> {t('webhooks.attempts.more', { n: x.messages - 1 })}</span>}
          </span>
        )}
      </td>
      <td className="r num">{probe ? '—' : x.attempt}</td>
      <td>
        <span className="wh-outcome" title={tip}>
          <span className={`badge ${OUTCOME_TONE[x.outcome]}`}>{t(`webhooks.attempts.outcome.${x.outcome}` as TKey)}</span>
          {x.status != null ? <StatusCode status={x.status} detail={tip} /> : x.error ? <span className="status-code tone-critical">{shortError(x.error)}</span> : null}
        </span>
      </td>
      <td className="r num">{fmtMs(x.latency_ms)}</td>
      {nodes && <td className="mono faint">{x.node}</td>}
    </tr>
  );
}

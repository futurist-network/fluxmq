import { useState } from 'react';
import { Link } from 'react-router';
import { BookOpen, KeyRound, Radio, ShieldCheck } from 'lucide-react';
import { api, ApiError } from '../../api/client';
import { TOPICS, type ChannelsSummary, type LiveLimits, type LiveNodeStatus } from '../../api/types';
import { useAsync, useInterval } from '../../lib/hooks';
import { fmtBytes, fmtCount } from '../../lib/format';
import { channelPath } from '../../lib/labels';
import { DEFAULT_LIVE_OPTIONS, LIMIT_ROWS, liveTotals, rejectionLabel } from '../../lib/live';
import { channelNames } from '../../store/selectors';
import { useTopicSelect } from '../../telemetry';
import { useGate } from '../../components/auth/useGate';
import { EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { LiveSnippetCard } from './LiveConnect';
import { useT } from '../../i18n';
import '../routing/routing-page.css';
import '../channel/channel.css';
import './live.css';

function Kpi({ label, value, sub, tone }: { label: string; value: string; sub?: string; tone?: 'warning' | 'critical' }) {
  return (
    <div className="hm">
      <span className="hm-label">{label}</span>
      <span className={`hm-value ${tone ? `text-${tone}` : ''}`}>
        <span className="num">{value}</span>
      </span>
      <span className="hm-sub">{sub ?? ' '}</span>
    </div>
  );
}

function limitValue(t: ReturnType<typeof useT>, l: LiveLimits, row: (typeof LIMIT_ROWS)[number]): string {
  const v = l[row.key];
  if (row.unlimited0 && v === 0) return t('live.limit.unlimited');
  switch (row.unit) {
    case 'bytes':
      return fmtBytes(v);
    case 'bytesPerSec':
      return `${fmtBytes(v)}/s`;
    case 'secs':
      return t('live.limit.secs', { n: v });
    case 'perSec':
      return `${fmtCount(v)}/s`;
    default:
      return fmtCount(v);
  }
}

/** One column per node when the nodes disagree; a single value column otherwise. */
function LimitsCard({ nodes }: { nodes: LiveNodeStatus[] }) {
  const t = useT();
  const same = nodes.every((n) => JSON.stringify(n.limits) === JSON.stringify(nodes[0]!.limits));
  const cols = same ? [nodes[0]!] : nodes;
  const origins = nodes[0]!.limits.allowed_origins;
  return (
    <section className="card">
      <div className="card-head">
        <h2>{t('live.limits.title')}</h2>
        <span className="card-sub">{same ? t('live.limits.sub') : t('live.limits.subDiffer')}</span>
      </div>
      <div className="table-wrap">
        <table className="table live-limits">
          <thead>
            <tr>
              <th>{t('live.limits.limit')}</th>
              <th>{t('live.limits.flag')}</th>
              {cols.map((n) => (
                <th key={n.node} className="r">
                  {same ? t('live.limits.value') : <span className="mono">{n.node}</span>}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {LIMIT_ROWS.map((row) => (
              <tr key={row.key}>
                <td>{t(row.label)}</td>
                <td className="mono faint">{row.flag}</td>
                {cols.map((n) => (
                  <td key={n.node} className="r num">
                    {limitValue(t, n.limits, row)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="card-body live-origins">
        <ShieldCheck size={14} aria-hidden />
        <span>
          {origins.length === 0 ? t('live.limits.originsSame') : origins.includes('*') ? t('live.limits.originsAny') : t('live.limits.originsList', { list: origins.join(', ') })}{' '}
          <span className="faint">{t('live.limits.howToChange')}</span>
        </span>
      </div>
    </section>
  );
}

export default function LivePage() {
  const t = useT();
  const admin = useGate('admin');
  const st = useAsync((signal) => api.liveStatus(signal), []);
  useInterval(st.reload, 5000);
  const names = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames).filter((n) => !n.startsWith('$'));
  const [picked, setPicked] = useState<string | null>(null);
  const unsupported = st.error instanceof ApiError && st.error.isNotFound;
  const nodes = st.data?.nodes ?? null;
  const tot = nodes ? liveTotals(nodes) : null;
  const channel = picked ?? tot?.channels[0]?.channel ?? names[0] ?? 'orders';
  const refused = tot ? tot.rejections.reduce((s, r) => s + r.count, 0) : 0;

  return (
    <div className="page live-page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('live.title')}</h1>
          <p>{t('live.subtitle')}</p>
        </div>
        <div className="page-actions">
          <Link to="/docs/live" className="btn">
            <BookOpen size={15} /> {t('live.docs')}
          </Link>
          {admin.allowed && (
            <Link to="/settings/tokens?create=subscriber" className="btn">
              <KeyRound size={15} /> {t('live.createToken')}
            </Link>
          )}
        </div>
      </div>

      {unsupported ? (
        <section className="card">
          <EmptyState icon={<Radio size={20} />} title={t('live.unsupportedTitle')}>
            {t('live.unsupportedBody')}
          </EmptyState>
        </section>
      ) : st.error && !nodes ? (
        <InlineError onRetry={st.reload}>{t('live.failed')}</InlineError>
      ) : !nodes || !tot ? (
        <section className="card">
          <SkeletonRows rows={3} />
        </section>
      ) : (
        <>
          <section className="card ch-metrics live-kpis" aria-label={t('live.kpi.aria')}>
            <Kpi label={t('live.kpi.connections')} value={fmtCount(tot.connectionsWs + tot.connectionsSse)} sub={t('live.kpi.connectionsSub', { ws: fmtCount(tot.connectionsWs), sse: fmtCount(tot.connectionsSse) })} />
            <Kpi label={t('live.kpi.subscriptions')} value={fmtCount(tot.subscriptionsLive + tot.subscriptionsCatchup)} sub={t('live.kpi.subscriptionsSub', { live: fmtCount(tot.subscriptionsLive), catchup: fmtCount(tot.subscriptionsCatchup) })} />
            <Kpi label={t('live.kpi.channels')} value={fmtCount(tot.channels.length)} sub={tot.nodes > 1 ? t('live.kpi.nodes', { count: tot.nodes }) : t('live.kpi.oneNode')} />
            <Kpi label={t('live.kpi.sent')} value={fmtCount(tot.messagesSent)} sub={t('live.kpi.sentSub', { bytes: fmtBytes(tot.bytesSent) })} />
            <Kpi label={t('live.kpi.refused')} value={fmtCount(refused)} sub={tot.rejections[0] ? rejectionLabel(tot.rejections[0].reason) : t('live.kpi.none')} tone={refused ? 'warning' : undefined} />
            <Kpi label={t('live.kpi.problems')} value={fmtCount(tot.slowDisconnects + tot.gaps)} sub={t('live.kpi.problemsSub', { slow: fmtCount(tot.slowDisconnects), gaps: fmtCount(tot.gaps) })} tone={tot.gaps ? 'warning' : undefined} />
          </section>

          <section className="card">
            <div className="card-head">
              <h2>{t('live.channels.title')}</h2>
              <span className="card-sub">{t('live.channels.sub')}</span>
            </div>
            {tot.channels.length === 0 ? (
              <EmptyState compact icon={<Radio size={18} />} title={t('live.channels.emptyTitle')}>
                {t('live.channels.emptyBody')}
              </EmptyState>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>{t('live.channels.channel')}</th>
                      <th className="r">{t('live.channels.subscribers')}</th>
                      <th className="r">{t('live.channels.ofLimit')}</th>
                      <th aria-label={t('live.channels.actions')} />
                    </tr>
                  </thead>
                  <tbody>
                    {tot.channels.map((c) => {
                      const limit = nodes[0]!.limits.max_connections_per_channel * nodes.length;
                      const share = limit ? c.subscriptions / limit : 0;
                      return (
                        <tr key={c.channel}>
                          <td>
                            <Link to={channelPath(c.channel, 'live')} className="mono">
                              {c.channel}
                            </Link>
                          </td>
                          <td className="r num">{fmtCount(c.subscriptions)}</td>
                          <td className={`r num ${share > 0.8 ? 'text-warning' : 'faint'}`}>{share < 0.001 ? '<0.1 %' : `${(share * 100).toFixed(1)} %`}</td>
                          <td className="r">
                            <Link to={channelPath(c.channel, 'live')} className="btn btn-sm">
                              {t('live.channels.open')}
                            </Link>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <section className="card live-start">
            <div className="card-head">
              <h2>{t('live.start.title')}</h2>
            </div>
            <div className="card-body">
              <ol className="live-steps">
                <li>{t('live.start.step1')}</li>
                <li>{t('live.start.step2')}</li>
                <li>{t('live.start.step3')}</li>
              </ol>
              {names.length > 0 && (
                <div className="field live-pick">
                  <label htmlFor="live-pick">{t('live.start.channel')}</label>
                  <select id="live-pick" className="input" value={channel} onChange={(e) => setPicked(e.target.value)}>
                    {[...new Set([channel, ...names])].map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                  </select>
                </div>
              )}
            </div>
          </section>
          <LiveSnippetCard channel={channel} options={DEFAULT_LIVE_OPTIONS} canCreateToken={admin.allowed} />

          <LimitsCard nodes={nodes} />

          {tot.rejections.length > 0 && (
            <section className="card">
              <div className="card-head">
                <h2>{t('live.refused.title')}</h2>
                <span className="card-sub">{t('live.refused.sub')}</span>
              </div>
              <div className="table-wrap">
                <table className="table">
                  <tbody>
                    {tot.rejections.map((r) => (
                      <tr key={r.reason}>
                        <td>{rejectionLabel(r.reason)}</td>
                        <td className="mono faint">{r.reason}</td>
                        <td className="r num">{fmtCount(r.count)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}
        </>
      )}
    </div>
  );
}

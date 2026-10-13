import { useState } from 'react';
import { Link } from 'react-router';
import { ArrowRight } from 'lucide-react';
import { TOPICS, type ChannelDetail, type ConsumersState, type HistoryRange, type LatencySummary } from '../../api/types';
import { useTopicData, useTopicSelect } from '../../telemetry';
import { SK } from '../../store/series';
import { fmtBackoff, fmtBytes, fmtCount, fmtMs, fmtRate, fmtRetention } from '../../lib/format';
import { channelPath, HEALTH_LABEL } from '../../lib/labels';
import { flattenGroups } from '../../store/selectors';
import { TimeChart } from '../../components/charts/TimeChart';
import { EmptyState, HealthDot, Segmented, SkeletonRows } from '../../components/ui';
import { healthWhy, useAlerts } from '../../components/HealthWhy';
import { Users } from 'lucide-react';
import { useT, type TFn } from '../../i18n';
import { rangeOptions } from '../../components/charts/ranges';


function LatencyTable({ rows, t }: { rows: [string, LatencySummary][]; t: TFn }) {
  return (
    <table className="table lat-table">
      <thead>
        <tr>
          <th />
          <th className="r">p50</th>
          <th className="r">p95</th>
          <th className="r">p99</th>
          <th className="r">p99.9</th>
          <th className="r">{t('channels.overview.max')}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(([label, l]) => (
          <tr key={label}>
            <td className="muted">{label}</td>
            <td className="r num">{fmtMs(l.p50)}</td>
            <td className="r num">{fmtMs(l.p95)}</td>
            <td className="r num">{fmtMs(l.p99)}</td>
            <td className="r num">{fmtMs(l.p999)}</td>
            <td className="r num muted">{fmtMs(l.max)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function GroupsSummary({ name }: { name: string }) {
  const t = useT();
  const groups = useTopicData<ConsumersState>(TOPICS.consumers(name));
  const alerts = useAlerts();
  if (!groups) return <SkeletonRows rows={2} />;
  const list = flattenGroups({ [name]: groups }, alerts);
  if (!list.length)
    return (
      <EmptyState compact icon={<Users size={18} />} title={t('channels.overview.noGroups')}>
        {t('channels.overview.noGroupsBody')}
      </EmptyState>
    );
  return (
    <table className="table groups-mini">
      <thead>
        <tr>
          <th>{t('common.group')}</th>
          <th className="r" title={t('channels.overview.instances')}>
            {t('channels.overview.instShort')}
          </th>
          <th className="r" title={t('channels.overview.acksPerSec')}>
            {t('common.rate')}
          </th>
          <th className="r">{t('common.lag')}</th>
          <th className="c" aria-label={t('common.status')} />
        </tr>
      </thead>
      <tbody>
        {list.map(({ group: g, health }) => (
          <tr key={g.group} className={health !== 'healthy' ? `row-${health}` : ''}>
            <td className="truncate" style={{ maxWidth: 140 }} title={g.group}>
              <Link className="ch-name mono" to={`${channelPath(name, 'consumers')}#${encodeURIComponent(g.group)}`}>
                {g.group}
              </Link>
            </td>
            <td className="r num">{Object.keys(g.instances).length}</td>
            <td className="r num">{fmtRate(g.ack_rate, false)}</td>
            <td className="r num">{fmtCount(g.lag)}</td>
            <td className="c">
              {/* Narrow card: the dot carries the status, the tooltip says why (text for screen readers). */}
              <HealthDot health={health} title={healthWhy(health, alerts, { channel: name, group: g.group }) ?? HEALTH_LABEL[health]} />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function OverviewTab({ name }: { name: string }) {
  const t = useT();
  const [range, setRange] = useState<HistoryRange>('15m');
  const d = useTopicSelect<ChannelDetail, ChannelDetail | undefined>(TOPICS.channel(name), (c) => c);
  const ranges = rangeOptions(t);
  return (
    <div className="overview-grid">
      <section className="card chart-card span-2">
        <div className="card-head">
          <h2>{t('dashboard.kpi.traffic')}</h2>
          <span className="spacer" />
          <Segmented value={range} options={ranges} onChange={setRange} label={t('dashboard.timeRange')} />
        </div>
        <div className="card-body">
          <TimeChart
            metrics={[`channel.${name}.ingress_rate`, `channel.${name}.ack_rate`, `channel.${name}.egress_rate`]}
            liveKeys={[SK.chIngress(name), SK.chAcked(name), SK.chEgress(name)]}
            range={range}
            series={[
              { label: t('dashboard.stat.published'), color: '--series-1', fill: true },
              { label: t('dashboard.traffic.consumedAcked'), color: '--series-2' },
              { label: t('channels.overview.deliveredAll'), color: '--series-3' },
            ]}
            height={220}
            yFormat={(v) => fmtRate(v, false)}
            unitLabel={t('dashboard.unit.msgRate')}
          />
        </div>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>{t('channels.overview.configuration')}</h2>
          <span className="spacer" />
          <Link to={channelPath(name, 'settings')} className="btn-link">
            {t('common.edit')}
          </Link>
        </div>
        <div className="card-body">
          {!d ? (
            <SkeletonRows rows={5} h={28} />
          ) : (
            <>
              <p className="muted explain" style={{ fontSize: 13, marginBottom: 14 }}>
                {t(`channels.semantics.${d.config.mode}`)}
              </p>
              <dl className="kv">
                <dt>{t('channels.partitionsFirst')}</dt>
                <dd className="num">{d.config.partitions}</dd>
                <dt>{t('channels.orderingKey')}</dt>
                <dd className="mono">{d.config.ordering_key ?? <span className="faint">{t('channels.metric.none')}</span>}</dd>
                <dt>{t('common.retention')}</dt>
                <dd>
                  {fmtRetention(d.config.retention.max_age_secs)}
                  {d.config.retention.max_bytes != null && ` · ${t('channels.overview.maxBytes', { bytes: fmtBytes(d.config.retention.max_bytes) })}`}
                </dd>
                <dt>{t('channels.overview.retries')}</dt>
                <dd>
                  {t('channels.overview.attempts', { count: d.config.retry.max_attempts })} · {d.config.retry.backoff_ms.map((ms) => fmtBackoff(ms)).join(' → ')}
                </dd>
                <dt>{t('channels.retry.ackTimeout')}</dt>
                <dd>{fmtBackoff(d.config.retry.ack_timeout_ms)}</dd>
                <dt>{t('channels.metric.deadLetters')}</dt>
                <dd>{d.config.dlq_enabled ? t('common.enabled') : <span className="text-warning">{t('channels.overview.dlqDisabled')}</span>}</dd>
              </dl>
            </>
          )}
        </div>
      </section>

      <section className="card span-2">
        <div className="card-head">
          <h2>{t('channels.overview.latency')}</h2>
          {d && (
            <span className="card-sub">
              {t('channels.overview.latencySub', { retries: fmtRate(d.retries_rate), dlq: fmtRate(d.dlq_rate) })}
            </span>
          )}
        </div>
        <div className="card-body flush">
          {!d ? (
            <SkeletonRows rows={2} />
          ) : (
            <LatencyTable
              t={t}
              rows={[
                [t('channels.overview.publishDurable'), d.publish_latency],
                [t('channels.overview.appendDelivery'), d.delivery_latency],
              ]}
            />
          )}
        </div>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>{t('channels.consumerGroups')}</h2>
          <span className="spacer" />
          <Link to={channelPath(name, 'consumers')} className="btn-link">
            {t('common.details')} <ArrowRight size={13} style={{ verticalAlign: -2 }} />
          </Link>
        </div>
        <div className="card-body flush">
          <GroupsSummary name={name} />
        </div>
      </section>
    </div>
  );
}

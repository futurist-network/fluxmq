import { memo } from 'react';
import { useNavigate } from 'react-router';
import { TOPICS, isSystemChannel, type ChannelMode, type ChannelSummary, type ChannelsSummary } from '../api/types';
import { useTopicSelect } from '../telemetry';
import { SK } from '../store/series';
import { fmtBytes, fmtCount, fmtMs, fmtRate } from '../lib/format';
import { channelPath, MODE_HINT } from '../lib/labels';
import { HealthText, ModeBadge, SkeletonRows } from './ui';
import { LiveSparkline } from './charts/Sparkline';
import { useT } from '../i18n';

type RowData = Pick<ChannelSummary, 'name' | 'mode' | 'health' | 'ingress_rate' | 'egress_rate' | 'lag' | 'publish_p99_ms' | 'consumers' | 'consumer_groups' | 'storage_bytes' | 'dlq'> | null;

/** One row subscribes to its own channel's slice: a change in `email` doesn't re-render `orders`. */
const ChannelRow = memo(function ChannelRow({ name, compact }: { name: string; compact?: boolean }) {
  const t = useT();
  const navigate = useNavigate();
  const c = useTopicSelect<ChannelsSummary, RowData>(TOPICS.CHANNELS, (s) => {
    const x = s?.[name];
    return x
      ? {
          name: x.name,
          mode: x.mode,
          health: x.health,
          ingress_rate: x.ingress_rate,
          egress_rate: x.egress_rate,
          lag: x.lag,
          publish_p99_ms: x.publish_p99_ms,
          consumers: x.consumers,
          consumer_groups: x.consumer_groups,
          storage_bytes: x.storage_bytes,
          dlq: x.dlq,
        }
      : null;
  });
  if (!c) return null;
  const open = () => navigate(channelPath(name));
  const problem = c.health !== 'healthy';
  return (
    <tr className={`clickable ${problem ? `row-${c.health}` : ''}`} onClick={open}>
      <td>
        <a
          href={channelPath(name)}
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            open();
          }}
          className="ch-name"
        >
          {name}
        </a>
        {isSystemChannel(name) && (
          <span className="system-tag ch-system" title={t('channels.systemHint')}>
            {t('channels.system')}
          </span>
        )}
      </td>
      <td>
        <ModeBadge mode={c.mode} />
      </td>
      <td className="r num">{fmtRate(c.ingress_rate, false)}</td>
      <td className="spark-cell" aria-hidden>
        <LiveSparkline seriesKey={SK.chIngress(name)} label={t('channels.rateAria', { name })} color={problem ? (c.health === 'critical' ? '--critical' : '--warning') : '--series-1'} />
      </td>
      <td className={`r num ${problem && c.lag > 1000 ? (c.health === 'critical' ? 'text-critical' : 'text-warning') : ''}`}>{fmtCount(c.lag)}</td>
      <td className="r num">{fmtMs(c.publish_p99_ms)}</td>
      {!compact && <td className="r num muted">{c.consumers}</td>}
      {!compact && <td className="r num muted">{fmtBytes(c.storage_bytes)}</td>}
      {!compact && <td className={`r num ${c.dlq > 0 ? '' : 'faint'}`}>{fmtCount(c.dlq)}</td>}
      <td>
        <HealthText health={c.health} />
      </td>
    </tr>
  );
});

export function ChannelsTable({ names, loading, limit, compact }: { names: string[]; loading?: boolean; limit?: number; compact?: boolean }) {
  const t = useT();
  if (loading) return <SkeletonRows rows={5} />;
  const shown = limit ? names.slice(0, limit) : names;
  return (
    <div className="table-wrap">
      <table className="table channels-table">
        <thead>
          <tr>
            <th>{t('common.channel')}</th>
            <th>{t('common.mode')}</th>
            <th className="r">{t('channels.col.rate')}</th>
            <th aria-hidden />
            <th className="r">{t('common.lag')}</th>
            <th className="r">p99</th>
            {!compact && <th className="r">{t('common.consumers')}</th>}
            {!compact && <th className="r">{t('channels.col.stored')}</th>}
            {!compact && <th className="r">DLQ</th>}
            <th>{t('common.status')}</th>
          </tr>
        </thead>
        <tbody>
          {shown.map((n) => (
            <ChannelRow key={n} name={n} compact={compact} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** "Modes: stream ≈ a Kafka topic · queue ≈ a RabbitMQ queue · keyed = strict order per key" (mode ids stay untranslated). */
export function ModesLegend({ className }: { className?: string }) {
  const t = useT();
  const id = (m: ChannelMode) => (
    <b className="mono" title={MODE_HINT[m]}>
      {m}
    </b>
  );
  return <p className={className}>{t.rich('channels.modesLegend', { stream: id('stream'), queue: id('queue'), keyed: id('keyed') })}</p>;
}

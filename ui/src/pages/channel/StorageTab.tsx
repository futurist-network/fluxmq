import { HardDrive } from 'lucide-react';
import { TOPICS, type ChannelDetail } from '../../api/types';
import { useTopicData } from '../../telemetry';
import { fmtBytes, fmtCount, fmtDateTime, fmtDuration, fmtInt, fmtRate, fmtRetention } from '../../lib/format';
import { useT } from '../../i18n';
import { useNow } from '../../lib/hooks';
import { EmptyState, SkeletonRows, Stat } from '../../components/ui';

export function StorageTab({ name }: { name: string }) {
  const t = useT();
  const d = useTopicData<ChannelDetail>(TOPICS.channel(name));
  const now = useNow();
  if (!d) return <section className="card"><SkeletonRows rows={6} /></section>;
  const parts = d.partitions;
  const maxBytes = Math.max(1, ...parts.map((p) => p.storage_bytes));
  const totalSegments = parts.reduce((a, p) => a + p.segments, 0);
  const totalMsgs = parts.reduce((a, p) => a + (p.high_offset - p.low_offset), 0);
  const ret = d.config.retention;
  const usedPct = ret.max_bytes ? (d.summary.storage_bytes / ret.max_bytes) * 100 : null;
  return (
    <div className="stack">
      <section className="card storage-stats">
        <Stat label={t('channels.col.stored')} value={fmtBytes(d.summary.storage_bytes)} />
        <Stat label={t('channels.storage.retainedMessages')} value={fmtCount(totalMsgs)} />
        <Stat label={t('channels.storage.segments')} value={fmtInt(totalSegments)} />
        <Stat
          label={t('channels.storage.oldestMessage')}
          value={d.oldest_timestamp_ms ? fmtDuration(Math.max(0, (now - d.oldest_timestamp_ms) / 1000)) : '—'}
          unit={d.oldest_timestamp_ms ? t('channels.storage.old') : undefined}
        />
        <div className="stat">
          <span className="stat-label">{t('common.retention')}</span>
          <span className="stat-value" style={{ fontSize: 16 }}>
            {fmtRetention(ret.max_age_secs)}
            {ret.max_bytes != null && <span className="unit">{t('channels.storage.orBytes', { bytes: fmtBytes(ret.max_bytes) })}</span>}
          </span>
          {usedPct != null && (
            <div className={`meter ${usedPct > 90 ? 'critical' : usedPct > 80 ? 'warning' : ''}`} style={{ marginTop: 6 }}>
              <span style={{ width: `${Math.min(100, usedPct)}%` }} />
            </div>
          )}
        </div>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>{t('channels.partitionsFirst')}</h2>
          <span className="card-sub">{parts.length}</span>
          <span className="spacer" />
          {d.oldest_timestamp_ms && <span className="card-sub">{t('channels.storage.oldestRetained', { time: fmtDateTime(d.oldest_timestamp_ms) })}</span>}
        </div>
        <div className="card-body flush">
          {parts.length === 0 ? (
            <EmptyState compact icon={<HardDrive size={18} />} title={t('channels.storage.noPartitionData')} />
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>{t('common.partition')}</th>
                    <th className="r">{t('channels.storage.lowOffset')}</th>
                    <th className="r">{t('channels.storage.highOffset')}</th>
                    <th className="r">{t('channels.tab.messages')}</th>
                    <th className="r">{t('channels.storage.segments')}</th>
                    <th className="r">{t('common.rate')}</th>
                    <th style={{ width: '28%' }}>{t('common.size')}</th>
                  </tr>
                </thead>
                <tbody>
                  {parts.map((p) => (
                    <tr key={p.partition}>
                      <td className="mono">p{p.partition}</td>
                      <td className="r num muted">{fmtInt(p.low_offset)}</td>
                      <td className="r num">{fmtInt(p.high_offset)}</td>
                      <td className="r num">{fmtCount(p.high_offset - p.low_offset)}</td>
                      <td className="r num muted">{p.segments}</td>
                      <td className="r num">{fmtRate(p.ingress_rate)}</td>
                      <td>
                        <div className="size-cell">
                          <div className="meter" style={{ flex: 1 }}>
                            <span style={{ width: `${(p.storage_bytes / maxBytes) * 100}%` }} />
                          </div>
                          <span className="num">{fmtBytes(p.storage_bytes)}</span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}

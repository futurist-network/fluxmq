import { Link } from 'react-router';
import { AlertTriangle, Inbox, MessageSquare, RotateCcw, Users } from 'lucide-react';
import { TOPICS, type ConsumersState } from '../../api/types';
import { GateButton } from '../auth/Gate';
import { channelPath } from '../../lib/labels';
import { effectiveInstanceHealth } from '../../store/selectors';
import { healthWhy, useAlerts } from '../HealthWhy';
import { useTelemetry } from '../../store/telemetry';
import { fmtBytes, fmtCount, fmtDateTime, fmtDuration, fmtMs, fmtPercent, fmtRate, fmtRelative } from '../../lib/format';
import { useNow } from '../../lib/hooks';
import { Drawer, HealthText, Stat } from '../ui';
import { instanceReason } from './reason';
import { useT } from '../../i18n';

export function InstanceDrawer({
  channel,
  group,
  instanceId,
  onClose,
  onResetOffsets,
}: {
  channel: string;
  group: string;
  instanceId: string | null;
  onClose: () => void;
  /** Opens the group's reset-offsets dialog (nothing runs without its confirmation). */
  onResetOffsets?: () => void;
}) {
  const g = useTelemetry((s) => (s.topics[TOPICS.consumers(channel)] as ConsumersState | undefined)?.[group]);
  const alerts = useAlerts();
  const now = useNow();
  const t = useT();
  const i = instanceId ? g?.instances[instanceId] : undefined;
  const open = !!instanceId;
  return (
    <Drawer
      open={open}
      onClose={onClose}
      title={<span className="mono">{instanceId}</span>}
      subtitle={
        <>
          {channel} / {group}
        </>
      }
    >
      {!i || !g ? (
        <p className="muted">{t('consumers.drawer.disconnected')}</p>
      ) : (
        <>
          {(() => {
            const health = effectiveInstanceHealth(i, channel, group, alerts);
            const reason = instanceReason({ ...i, health }, g);
            return (
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                <HealthText health={health} why={healthWhy(health, alerts, { channel, group, instance: i.id }, { i, g })} />
                {reason && <span className="muted" style={{ fontSize: 13 }}>— {reason.text}</span>}
              </div>
            );
          })()}
          <div className="drawer-actions">
            <Link className="btn btn-sm" to={`${channelPath(channel, 'consumers')}#${encodeURIComponent(group)}`} onClick={onClose}>
              <Users size={13} /> {t('consumers.drawer.openGroup')}
            </Link>
            <Link className="btn btn-sm" to={`/dlq?channel=${encodeURIComponent(channel)}&group=${encodeURIComponent(group)}`} onClick={onClose}>
              <Inbox size={13} /> {t('consumers.group.openDlq')}
            </Link>
            <Link className="btn btn-sm" to={channelPath(channel, 'messages')} onClick={onClose}>
              <MessageSquare size={13} /> {t('consumers.drawer.openMessages')}
            </Link>
            {onResetOffsets && (
              <GateButton need="operate" className="btn btn-sm btn-ghost text-critical" onClick={onResetOffsets}>
                <RotateCcw size={13} /> {t('alerts.pb.resetOffsets')}
              </GateButton>
            )}
          </div>
          <div className="drawer-stats">
            <Stat label="CPU" value={i.cpu_percent == null ? '—' : fmtPercent(i.cpu_percent)} />
            <Stat label={t('consumers.drawer.memory')} value={i.memory_bytes == null ? '—' : fmtBytes(i.memory_bytes)} />
            <Stat label={t('consumers.stat.processingP99')} value={fmtMs(i.processing_p99_ms)} tone={i.processing_p99_ms > 3 * g.processing_latency.p99 ? 'warning' : undefined} />
            <Stat label={t('consumers.stat.ackRate')} value={fmtRate(i.ack_rate, false)} unit={t('consumers.unit.msgPerSec')} />
            <Stat label={t('consumers.drawer.retriesTotal')} value={fmtCount(i.retries_total)} />
            <Stat label={t('common.lag')} value={fmtCount(i.lag)} />
          </div>
          {(i.cpu_percent == null || i.memory_bytes == null) && (
            <p className="faint" style={{ fontSize: 12, marginTop: -8 }}>
              {t('consumers.drawer.noCpu')}
            </p>
          )}

          <section>
            <h3 className="drawer-section">{t('consumers.drawer.inFlightTitle')}</h3>
            <div className="inflight">
              <div className="inflight-bar" role="meter" aria-valuemin={0} aria-valuemax={i.prefetch || 1} aria-valuenow={i.in_flight} aria-label={t('consumers.drawer.inFlightPrefetch')}>
                <span style={{ width: `${Math.min(100, (i.in_flight / Math.max(1, i.prefetch)) * 100)}%` }} />
              </div>
              <span className="num">
                {i.in_flight} / {i.prefetch}
              </span>
            </div>
            <p className="faint" style={{ fontSize: 12, marginTop: 6 }}>
              {t('consumers.drawer.inFlightHint')}
            </p>
          </section>

          <section>
            <h3 className="drawer-section">{t('consumers.drawer.lastError')}</h3>
            <p className="muted" style={{ fontSize: 12.5, margin: '0 0 8px' }}>
              {t('consumers.drawer.errorRates', { nacks: fmtRate(i.nack_rate), retries: fmtCount(i.retries_total) })}
            </p>
            {i.last_error ? (
              <div className="last-error">
                <AlertTriangle size={14} />
                <div>
                  <div className="mono">{i.last_error}</div>
                  {i.last_error_at_ms != null && (
                    <div className="faint" style={{ fontSize: 12, marginTop: 2 }}>
                      {fmtRelative(i.last_error_at_ms, now)} · {fmtDateTime(i.last_error_at_ms)}
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <p className="muted" style={{ fontSize: 13 }}>
                {t('consumers.drawer.noErrors')}
              </p>
            )}
          </section>

          <section>
            <h3 className="drawer-section">{t('consumers.drawer.connection')}</h3>
            <dl className="kv">
              <dt>{t('consumers.drawer.client')}</dt>
              <dd className="mono">{i.client_name}</dd>
              <dt>{t('consumers.drawer.remoteAddr')}</dt>
              <dd className="mono">{i.remote_addr}</dd>
              <dt>{t('consumers.drawer.connected')}</dt>
              <dd>
                {t('consumers.drawer.ago', { duration: fmtDuration((now - i.connected_at_ms) / 1000) })} <span className="faint">· {fmtDateTime(i.connected_at_ms)}</span>
              </dd>
              <dt>{t('consumers.drawer.assigned')}</dt>
              <dd className="mono">{i.assigned_partitions.length ? i.assigned_partitions.map((p) => `p${p}`).join(', ') : t('consumers.drawer.competing')}</dd>
              <dt>{t('consumers.drawer.inFlightPrefetch')}</dt>
              <dd className="num">
                {i.in_flight} / {i.prefetch}
              </dd>
              <dt>{t('common.delivered')}</dt>
              <dd>{fmtRate(i.egress_rate)}</dd>
              <dt>{t('consumers.stat.nacks')}</dt>
              <dd>{fmtRate(i.nack_rate)}</dd>
            </dl>
          </section>
        </>
      )}
    </Drawer>
  );
}

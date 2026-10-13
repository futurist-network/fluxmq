import { memo, useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router';
import { AlertTriangle, ChevronDown, ChevronRight, Inbox, RotateCcw, Trash2, Users } from 'lucide-react';
import { ROUTER_GROUP, TOPICS, WEBHOOK_GROUP_PREFIX, type AlertsState, type ConsumersState, type GroupState } from '../../api/types';
import { useTelemetry } from '../../store/telemetry';
import { loadWebhooks, useWebhooks } from '../../store/webhooks';
import { effectiveGroupHealth, effectiveInstanceHealth, sortedInstances } from '../../store/selectors';
import { healthWhy, useAlerts } from '../HealthWhy';
import { OverflowMenu, type MenuItem } from '../OverflowMenu';
import { instanceReason } from './reason';
import { fmtCount, fmtMs, fmtRate } from '../../lib/format';
import { channelPath, HEALTH_LABEL, splitInstanceId } from '../../lib/labels';
import { EmptyState, HealthText } from '../ui';
import { InstanceDrawer } from './InstanceDrawer';
import { ResetOffsetsDialog } from './ResetOffsetsDialog';
import { DeleteGroupDialog } from './DeleteGroupDialog';
import './consumers.css';
import { useT, type TKey } from '../../i18n';

function GroupStats({ g }: { g: GroupState }) {
  const t = useT();
  const n = Object.keys(g.instances).length;
  const items: [TKey, string, ('warning' | 'critical')?][] = [
    ['consumers.stat.instances', String(n), n === 0 && g.lag > 0 ? 'critical' : undefined],
    ['common.lag', fmtCount(g.lag), g.health !== 'healthy' && g.lag > 1000 ? g.health : undefined],
    ['consumers.stat.ackRate', fmtRate(g.ack_rate)],
    ['consumers.stat.processingP99', fmtMs(g.processing_latency.p99)],
    ['common.inFlight', fmtCount(g.in_flight)],
    ['consumers.stat.retryPending', fmtCount(g.retry_pending)],
    ['consumers.stat.nacks', fmtRate(g.nack_rate), g.ack_rate > 0 && g.nack_rate / g.ack_rate > 0.05 ? 'warning' : undefined],
    ['consumers.stat.dlq', fmtCount(g.dlq)],
  ];
  return (
    <dl className="group-stats">
      {items.map(([k, v, tone]) => (
        <div key={k}>
          <dt title={t(k)}>{t(k)}</dt>
          <dd className={`num ${tone ? `text-${tone}` : ''}`}>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

function InstanceTable({ g, channel, alerts, onInspect }: { g: GroupState; channel: string; alerts: AlertsState | undefined; onInspect: (id: string) => void }) {
  const t = useT();
  const list = sortedInstances(g);
  const clustered = list.some((i) => splitInstanceId(i.id).node != null);
  if (!list.length)
    return (
      <EmptyState compact icon={<Users size={18} />} title={t('consumers.instances.emptyTitle')}>
        {g.lag > 0 ? t('consumers.instances.waiting', { count: g.lag, n: fmtCount(g.lag), group: g.group }) : t('consumers.instances.startHint')}
      </EmptyState>
    );
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>{t('consumers.col.instance')}</th>
            {clustered && <th className="hide-sm">{t('common.node')}</th>}
            <th className="r">{t('common.rate')}</th>
            <th className="r hide-sm">{t('common.lag')}</th>
            <th className="r hide-sm">{t('common.inFlight')}</th>
            <th className="r">{t('consumers.stat.processingP99')}</th>
            <th className="hide-sm">{t('common.partitions')}</th>
            <th>{t('consumers.col.health')}</th>
            <th className="hide-sm" aria-label={t('common.actions')} />
          </tr>
        </thead>
        <tbody>
          {list.map((i) => {
            const health = effectiveInstanceHealth(i, channel, g.group, alerts);
            const reason = instanceReason({ ...i, health }, g);
            const why = healthWhy(health, alerts, { channel, group: g.group, instance: i.id }, { i, g });
            return (
              <tr key={i.id} className={`clickable ${health !== 'healthy' ? `row-${health}` : ''}`} onClick={() => onInspect(i.id)}>
                <td>
                  <span className="mono inst-id">{splitInstanceId(i.id).local}</span>
                </td>
                {clustered && (
                  <td className="hide-sm">
                    <span className="chip static node-chip" title={t('consumers.instances.nodeHint')}>
                      {splitInstanceId(i.id).node ?? '—'}
                    </span>
                  </td>
                )}
                <td className="r num">{fmtRate(i.ack_rate, false)}</td>
                <td className="r num hide-sm">{fmtCount(i.lag)}</td>
                <td className="r num hide-sm">
                  {i.in_flight}
                  <span className="faint">/{i.prefetch}</span>
                </td>
                <td className={`r num ${reason?.kind === 'processing' ? 'text-warning' : ''}`}>{fmtMs(i.processing_p99_ms)}</td>
                <td className="muted mono parts-cell hide-sm">{i.assigned_partitions.length ? i.assigned_partitions.map((p) => `p${p}`).join(' ') : '—'}</td>
                <td className="inst-health">
                  {health === 'healthy' ? (
                    <HealthText health="healthy" />
                  ) : (
                    <span className={`health-text has-why ${health}`} title={why}>
                      <AlertTriangle size={13} />
                      <span className="truncate inst-reason">{reason?.text ?? HEALTH_LABEL[health]}</span>
                    </span>
                  )}
                </td>
                <td className="r hide-sm">
                  <button
                    className="btn btn-sm"
                    onClick={(e) => {
                      e.stopPropagation();
                      onInspect(i.id);
                    }}
                  >
                    {t('consumers.instances.inspect')}
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** Link to the webhook behind a `$webhook:<name>` group (by name, from the shared list). */
function WebhookLink({ name }: { name: string }) {
  const t = useT();
  const id = useWebhooks((s) => s.list?.find((w) => w.name === name)?.id);
  useEffect(() => {
    if (!useWebhooks.getState().list) void loadWebhooks();
  }, []);
  return (
    <Link to={id ? `/webhooks/${encodeURIComponent(id)}` : '/webhooks'} className="group-channel">
      {t('consumers.group.viewWebhook')}
    </Link>
  );
}

/** One consumer group, live. Selects its own slice so sibling groups don't re-render it. */
export const GroupCard = memo(function GroupCard({ channel, group, showChannel, defaultOpen = true }: { channel: string; group: string; showChannel?: boolean; defaultOpen?: boolean }) {
  const g = useTelemetry((s) => (s.topics[TOPICS.consumers(channel)] as ConsumersState | undefined)?.[group]);
  const alerts = useAlerts();
  const [open, setOpen] = useState(defaultOpen || (g ? effectiveGroupHealth(g, channel, alerts) !== 'healthy' : false));
  const [inspect, setInspect] = useState<string | null>(null);
  const [resetOpen, setResetOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const navigate = useNavigate();
  const t = useT();
  if (!g) return null;
  const n = Object.keys(g.instances).length;
  const health = effectiveGroupHealth(g, channel, alerts);
  const menu: MenuItem[] = [
    { key: 'dlq', label: t('consumers.group.openDlq'), icon: <Inbox size={14} />, onSelect: () => navigate(`/dlq?channel=${encodeURIComponent(channel)}&group=${encodeURIComponent(group)}`) },
    { key: 'reset', label: t('alerts.pb.resetOffsets'), icon: <RotateCcw size={14} />, danger: true, need: 'operate', onSelect: () => setResetOpen(true) },
  ];
  if (n === 0) menu.push({ key: 'delete', label: t('consumers.delete.confirm'), icon: <Trash2 size={14} />, danger: true, need: 'admin', hint: t('consumers.group.deleteHint'), onSelect: () => setDeleteOpen(true) });
  return (
    <section id={encodeURIComponent(group)} className={`card group-card ${health !== 'healthy' ? `tone-${health}` : ''}`} aria-label={t('consumers.group.aria', { group })}>
      <div className="group-head">
        <button className="btn btn-ghost btn-icon btn-sm" onClick={() => setOpen(!open)} aria-expanded={open} aria-label={open ? t('consumers.group.collapse') : t('consumers.group.expand')}>
          {open ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
        </button>
        <div className="group-titles">
          {group === ROUTER_GROUP ? (
            <>
              <h3>{t('consumers.group.systemRouter')}</h3>
              <span className="badge system-badge mono" title={t('consumers.group.routerHint')}>
                {ROUTER_GROUP}
              </span>
              <Link to={channelPath(channel, 'routing')} className="group-channel">
                {t('consumers.group.viewRoutes')}
              </Link>
            </>
          ) : group.startsWith(WEBHOOK_GROUP_PREFIX) ? (
            <>
              <h3>{t('consumers.group.systemWebhook')}</h3>
              <span className="badge system-badge mono" title={t('consumers.group.webhookHint')}>
                {group}
              </span>
              <WebhookLink name={group.slice(WEBHOOK_GROUP_PREFIX.length)} />
            </>
          ) : (
            <h3 className="mono">{group}</h3>
          )}
          {showChannel && (
            <Link to={channelPath(channel, 'consumers')} className="group-channel">
              {channel}
            </Link>
          )}
          <span className="faint" style={{ fontSize: 12 }}>
            {t('common.instancesCount', { count: n })}
          </span>
        </div>
        <HealthText health={health} why={healthWhy(health, alerts, { channel, group })} />
        <span className="spacer" />
        <OverflowMenu items={menu} label={t('consumers.group.menu', { group })} />
      </div>
      <GroupStats g={g} />
      {open && (
        <div className="group-instances">
          <InstanceTable g={g} channel={channel} alerts={alerts} onInspect={setInspect} />
        </div>
      )}
      <InstanceDrawer channel={channel} group={group} instanceId={inspect} onClose={() => setInspect(null)} onResetOffsets={() => setResetOpen(true)} />
      <ResetOffsetsDialog channel={channel} group={group} open={resetOpen} onClose={() => setResetOpen(false)} />
      {(n === 0 || deleteOpen) && <DeleteGroupDialog channel={channel} group={group} open={deleteOpen} onClose={() => setDeleteOpen(false)} />}
    </section>
  );
});

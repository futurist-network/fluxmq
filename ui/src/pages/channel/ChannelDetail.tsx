import { useLayoutEffect, useRef, useState } from 'react';
import { Link, NavLink, useNavigate, useParams } from 'react-router';
import { ChevronRight, Search, Send, Waypoints } from 'lucide-react';
import { TOPICS, isSystemChannel, type ChannelDetail as ChannelDetailT } from '../../api/types';
import { useTopicError, useTopicSelect } from '../../telemetry';
import { fmtBytes, fmtCount, fmtMs, fmtRate, splitUnit } from '../../lib/format';
import { channelPath } from '../../lib/labels';
import { EmptyState, HealthText, ModeBadge, Skeleton } from '../../components/ui';
import { OverviewTab } from './OverviewTab';
import { StorageTab } from './StorageTab';
import { SettingsTab } from './SettingsTab';
import { MessagesTab } from './MessagesTab';
import { ChannelConsumersTab } from './ConsumersTab';
import { RoutingTab } from './RoutingTab';
import { PublishDialog } from './PublishDialog';
import { GateButton } from '../../components/auth/Gate';
import { LiveTab } from './LiveTab';
import './channel.css';
import { useT, type TKey } from '../../i18n';

const TABS: readonly { id: string; label: TKey }[] = [
  { id: '', label: 'channels.tab.overview' },
  { id: 'messages', label: 'channels.tab.messages' },
  { id: 'consumers', label: 'channels.tab.consumers' },
  { id: 'routing', label: 'channels.tab.routing' },
  { id: 'live', label: 'channels.tab.live' },
  { id: 'storage', label: 'channels.tab.storage' },
  { id: 'settings', label: 'channels.tab.settings' },
];

type HeaderData = {
  mode?: ChannelDetailT['summary']['mode'];
  health?: ChannelDetailT['summary']['health'];
  ingress?: number;
  acked?: number;
  p99?: number;
  consumers?: number;
  groups?: number;
  lag?: number;
  bytes?: number;
  messages?: number;
  dlq?: number;
  partitions?: number;
};

function HeaderMetric({ label, value, sub, tone }: { label: string; value?: string; sub?: string; tone?: 'warning' | 'critical' }) {
  const [n, u] = value ? splitUnit(value) : ['', ''];
  return (
    <div className="hm">
      <span className="hm-label">{label}</span>
      {value == null ? (
        <Skeleton w={70} h={22} />
      ) : (
        <span className={`hm-value ${tone ? `text-${tone}` : ''}`}>
          <span className="num">{n}</span>
          {u && <span className="unit">{u}</span>}
        </span>
      )}
      <span className="hm-sub">{sub ?? ' '}</span>
    </div>
  );
}

/** How long a tab switch keeps the panel at least as tall as the previous tab (while the new one loads). */
const HOLD_MS = 700;

/**
 * Keeps the tab panel from collapsing to a loading skeleton for a moment on a tab switch: the
 * page below would shrink and the browser would clamp the scroll position (a visible jump).
 */
function useHeldHeight(key: string) {
  const ref = useRef<HTMLDivElement>(null);
  const last = useRef(0);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    // Runs after the new tab rendered but before the observer saw it: `last` is the old tab's height.
    if (last.current > 0) el.style.minHeight = `${last.current}px`;
    const timer = window.setTimeout(() => {
      el.style.minHeight = '';
      last.current = el.offsetHeight;
    }, HOLD_MS);
    const ro = new ResizeObserver(() => {
      if (!el.style.minHeight) last.current = el.offsetHeight;
    });
    ro.observe(el);
    return () => {
      window.clearTimeout(timer);
      ro.disconnect();
      el.style.minHeight = '';
    };
  }, [key]);
  return ref;
}

export default function ChannelDetailPage() {
  const { name = '', tab = '' } = useParams();
  const t = useT();
  const navigate = useNavigate();
  const topic = TOPICS.channel(name);
  const d = useTopicSelect<ChannelDetailT, HeaderData>(topic, (c) =>
    c
      ? {
          mode: c.summary.mode,
          health: c.summary.health,
          ingress: c.summary.ingress_rate,
          acked: c.summary.ack_rate,
          p99: c.publish_latency.p99,
          consumers: c.summary.consumers,
          groups: c.summary.consumer_groups,
          lag: c.summary.lag,
          bytes: c.summary.storage_bytes,
          messages: c.summary.messages,
          dlq: c.summary.dlq,
          partitions: c.summary.partitions,
        }
      : {},
  );
  const error = useTopicError(topic);
  const panelRef = useHeldHeight(tab);
  const [publishOpen, setPublishOpen] = useState(false);

  if (error && d.mode == null) {
    return (
      <div className="page">
        <EmptyState
          icon={<Waypoints size={20} />}
          title={t('channels.detail.notFound', { name })}
          actions={
            <Link to="/channels" className="btn">
              {t('channels.detail.backToChannels')}
            </Link>
          }
        >
          {t('channels.detail.notFoundBody', { error })}
        </EmptyState>
      </div>
    );
  }

  const lagTone = d.health && d.health !== 'healthy' && (d.lag ?? 0) > 1000 ? d.health : undefined;
  const known = TABS.some((x) => x.id === tab);

  return (
    <div className="page channel-page">
      <div>
        <nav className="breadcrumbs" aria-label={t('channels.detail.breadcrumb')}>
          <Link to="/channels">{t('channels.title')}</Link>
          <ChevronRight size={12} />
          <span aria-current="page">{name}</span>
        </nav>
        <div className="page-head">
          <div className="titles ch-titles">
            <h1 className="mono-title">{name}</h1>
            {d.mode && <ModeBadge mode={d.mode} />}
            {isSystemChannel(name) && (
              <span className="system-tag" title={t('channels.systemHint')}>
                {t('channels.systemReadOnly')}
              </span>
            )}
            {d.health && <HealthText health={d.health} />}
          </div>
          <div className="page-actions">
            {!isSystemChannel(name) && (
              <GateButton need="operate" className="btn" onClick={() => setPublishOpen(true)}>
                <Send size={14} /> {t('scheduled.publish.open')}
              </GateButton>
            )}
            <button className="btn" onClick={() => navigate(channelPath(name, 'messages'))}>
              <Search size={14} /> {t('channels.detail.searchMessages')}
            </button>
          </div>
        </div>
      </div>

      <section className="card ch-metrics" aria-label={t('channels.detail.metricsAria')}>
        <HeaderMetric label={t('channels.metric.publishRate')} value={d.ingress != null ? fmtRate(d.ingress) : undefined} sub={d.acked != null ? t('channels.metric.consumedSub', { rate: fmtRate(d.acked) }) : undefined} />
        <HeaderMetric label={t('dashboard.stat.publishP99')} value={d.p99 != null ? fmtMs(d.p99) : undefined} sub={t('channels.metric.publishDurable')} />
        <HeaderMetric label={t('common.consumers')} value={d.consumers != null ? fmtCount(d.consumers) : undefined} sub={d.groups != null ? t('common.groupsCount', { count: d.groups }) : undefined} />
        <HeaderMetric label={t('common.lag')} value={d.lag != null ? `${fmtCount(d.lag)} ${t('dashboard.kpi.msgsUnit')}` : undefined} sub={t('channels.metric.lagSub')} tone={lagTone} />
        <HeaderMetric label={t('channels.col.stored')} value={d.bytes != null ? fmtBytes(d.bytes) : undefined} sub={d.messages != null ? t('channels.metric.messagesSub', { count: fmtCount(d.messages) }) : undefined} />
        <HeaderMetric label={t('channels.metric.deadLetters')} value={d.dlq != null ? fmtCount(d.dlq) : undefined} sub={d.dlq ? t('channels.metric.needsReview') : t('channels.metric.none')} />
      </section>

      <nav className="tabs" aria-label={t('channels.detail.sectionsAria')}>
        {TABS.map((x) => (
          <NavLink key={x.id} to={channelPath(name, x.id || undefined)} end className={() => (x.id === tab ? 'active' : '')} aria-current={x.id === tab ? 'page' : undefined}>
            {t(x.label)}
          </NavLink>
        ))}
      </nav>

      {publishOpen && <PublishDialog channel={name} open onClose={() => setPublishOpen(false)} levels={d.mode === 'priority' ? d.partitions : undefined} />}
      <div className="tab-panel" ref={panelRef}>
        {tab === '' && <OverviewTab name={name} />}
        {tab === 'messages' && <MessagesTab name={name} />}
        {tab === 'consumers' && <ChannelConsumersTab name={name} />}
        {tab === 'routing' && <RoutingTab name={name} />}
        {tab === 'live' && <LiveTab name={name} />}
        {tab === 'storage' && <StorageTab name={name} />}
        {tab === 'settings' && <SettingsTab name={name} />}
        {!known && (
          <EmptyState icon={<Waypoints size={20} />} title={t('channels.detail.unknownSection')}>
            <Link to={channelPath(name)} className="btn-link">
              {t('channels.detail.goOverview')}
            </Link>
          </EmptyState>
        )}
      </div>
    </div>
  );
}

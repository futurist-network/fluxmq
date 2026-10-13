import { Link } from 'react-router';
import { Activity, AlertTriangle, ArrowRightLeft, CheckCircle2, Crown, Inbox, PlugZap, Plus, LogOut, Eraser, RotateCcw, Scale, Server, ServerCog, ServerCrash, ServerOff, CircleMinus, ShieldAlert, Trash2, Unplug, Waypoints, CircleX } from 'lucide-react';
import { TOPICS, type EventKind, type EventsFeed, type FluxEvent } from '../api/types';
import { useTopicData } from '../telemetry';
import { fmtDuration, fmtRelative } from '../lib/format';
import { collapseEvents, EVENT_COLLAPSE_MS, eventParams, eventText } from '../lib/alertText';
import { useNow } from '../lib/hooks';
import { channelPath } from '../lib/labels';
import { useT } from '../i18n';
import { EmptyState, SkeletonRows } from './ui';

const ICON: Record<EventKind, typeof Activity> = {
  channel_created: Plus,
  channel_deleted: Trash2,
  channel_updated: Waypoints,
  consumer_connected: PlugZap,
  consumer_disconnected: Unplug,
  alert_opened: AlertTriangle,
  alert_resolved: CheckCircle2,
  dead_lettered: Inbox,
  dlq_retried: RotateCcw,
  dlq_discarded: Trash2,
  messages_replayed: RotateCcw,
  controller_changed: Crown,
  node_down: ServerOff,
  node_up: Server,
  rebalance_started: Scale,
  rebalance_completed: CheckCircle2,
  rebalance_failed: CircleX,
  partition_moved: ArrowRightLeft,
  replica_repair_started: ServerCog,
  tls_reload_failed: ShieldAlert,
  node_decommission_started: LogOut,
  node_decommissioned: CircleMinus,
  node_decommission_failed: ServerCrash,
  node_forgotten: Eraser,
};

function subjectLink(e: FluxEvent): string | null {
  if (e.kind.startsWith('channel_') && e.kind !== 'channel_deleted') return channelPath(e.subject);
  if (e.kind === 'dead_lettered') {
    const ch = eventParams(e)?.channel;
    return typeof ch === 'string' ? `/dlq?channel=${encodeURIComponent(ch)}` : '/dlq';
  }
  if (e.kind.startsWith('dlq_')) return '/dlq';
  if (e.kind.startsWith('alert_')) return '/alerts';
  if (e.kind.startsWith('node_') || e.kind === 'controller_changed') return '/nodes';
  if (e.kind.startsWith('rebalance_') || e.kind === 'partition_moved' || e.kind === 'replica_repair_started') return '/nodes#rebalancing';
  if (e.kind === 'tls_reload_failed') return '/settings/security';
  return null;
}

export function EventsFeedList({ limit = 10 }: { limit?: number }) {
  const feed = useTopicData<EventsFeed>(TOPICS.EVENTS);
  const now = useNow();
  const t = useT();
  if (!feed) return <SkeletonRows rows={5} h={48} />;
  if (!feed.events.length) return <EmptyState compact icon={<Activity size={18} />} title={t('alerts.events.emptyTitle')}>{t('alerts.events.emptyBody')}</EmptyState>;
  return (
    <ul className="events">
      {collapseEvents(feed.events)
        .slice(0, limit)
        .map(({ event: e, times, count }) => {
        const Icon = ICON[e.kind] ?? Activity;
        const link = subjectLink(e);
        const x = eventText(e);
        const ch = times > 1 ? eventParams(e)?.channel : undefined;
        const subject = times > 1 && typeof ch === 'string' && count != null ? t('alerts.ev.channelCount', { channel: ch, count }) : x.subject;
        const inner = (
          <>
            <span className={`ev-icon ${e.severity}`} aria-hidden>
              <Icon size={13} />
            </span>
            <span className="ev-text">
              <span className="ev-title">
                {x.title}
                {times > 1 && <span className="ev-times">{t('alerts.ev.collapsed', { n: times, window: fmtDuration(EVENT_COLLAPSE_MS / 1000) })}</span>}
              </span>
              <span className="ev-subject">{subject}</span>
            </span>
            <time className="ev-time" dateTime={new Date(e.ts).toISOString()}>
              {fmtRelative(e.ts, now)}
            </time>
          </>
        );
        return <li key={e.id}>{link ? <Link to={link}>{inner}</Link> : <div>{inner}</div>}</li>;
      })}
    </ul>
  );
}

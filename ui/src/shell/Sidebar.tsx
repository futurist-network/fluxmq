import { NavLink } from 'react-router';
import { Link } from 'react-router';
import { BookOpen, Bell, Clock, Inbox, LayoutDashboard, Radio, Route as RouteIcon, Server, Settings, ShieldOff, UserRound, Users, Waypoints, Webhook } from 'lucide-react';
import { useAuth } from '../store/auth';
import { useWebhooks, useWebhooksPolling } from '../store/webhooks';
import { RoleBadge } from '../components/auth/Gate';
import { TOPICS, type AlertsState, type ClusterOverview } from '../api/types';
import { alertCounts } from '../store/selectors';
import { useTopicSelect } from '../telemetry';
import { useTelemetry } from '../store/telemetry';
import { useUi } from '../store/ui';
import { fmtCount } from '../lib/format';
import { useT, type TKey } from '../i18n';
import { markViewTransition, viewTransitionsOn } from '../lib/motion';

export function Logo() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" aria-hidden>
      <defs>
        <linearGradient id="lg" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor="#8EA4FF" />
          <stop offset="1" stopColor="#4766F0" />
        </linearGradient>
      </defs>
      <path d="M4 6.5C4 5.1 5.1 4 6.5 4H18a2 2 0 0 1 0 4H8v3h7a2 2 0 0 1 0 4H8v3a2 2 0 0 1-4 0V6.5Z" fill="url(#lg)" />
      <circle cx="19" cy="17" r="2" fill="#5B7CFA" opacity="0.6" />
    </svg>
  );
}

function AlertBadge() {
  const t = useT();
  const c = useTopicSelect<AlertsState, ReturnType<typeof alertCounts>>(TOPICS.ALERTS, alertCounts);
  if (!c.total) return null;
  return (
    <span className={`count-pill ${c.critical ? '' : 'warning'}`} aria-label={t('shell.badge.alerts', { count: c.total })}>
      {c.total}
    </span>
  );
}

function DlqBadge() {
  const t = useT();
  const n = useTopicSelect<ClusterOverview, number>(TOPICS.CLUSTER, (o) => o?.dlq_total ?? 0);
  if (!n) return null;
  return (
    <span className="count-pill muted" aria-label={t('shell.badge.dlq', { count: n })}>
      {fmtCount(n)}
    </span>
  );
}

/** Failing webhooks (last delivery an error), from the shared stats poll. */
function WebhookBadge() {
  const t = useT();
  useWebhooksPolling(30_000);
  const n = useWebhooks((s) => s.failing);
  if (!n) return null;
  return (
    <span className="count-pill" aria-label={t('shell.badge.webhooks', { count: n })}>
      {n}
    </span>
  );
}

const NAV: { to: string; label: TKey; hint?: TKey; icon: typeof Bell; end?: boolean; badge?: React.ReactNode }[] = [
  { to: '/', label: 'shell.nav.dashboard', icon: LayoutDashboard, end: true },
  { to: '/channels', label: 'shell.nav.channels', hint: 'shell.nav.channelsHint', icon: Waypoints },
  { to: '/consumers', label: 'shell.nav.consumers', icon: Users },
  { to: '/routing', label: 'shell.nav.routing', icon: RouteIcon },
  { to: '/webhooks', label: 'shell.nav.webhooks', icon: Webhook, badge: <WebhookBadge /> },
  { to: '/scheduled', label: 'shell.nav.scheduled', hint: 'shell.nav.scheduledHint', icon: Clock },
  { to: '/live', label: 'shell.nav.live', hint: 'shell.nav.liveHint', icon: Radio },
  { to: '/dlq', label: 'shell.nav.dlq', icon: Inbox, badge: <DlqBadge /> },
  { to: '/alerts', label: 'shell.nav.alerts', icon: Bell, badge: <AlertBadge /> },
  { to: '/nodes', label: 'shell.nav.nodes', icon: Server },
];

/** Who am I: token name + role, links to the token settings. */
function Identity() {
  const t = useT();
  const me = useAuth((s) => s.me);
  const status = useAuth((s) => s.status);
  if (status === 'unsupported') return null;
  if (!me) {
    return status === 'unauthenticated' ? (
      <div className="identity muted">
        <UserRound size={14} />
        <span className="identity-name">{t('shell.identity.notSignedIn')}</span>
      </div>
    ) : null;
  }
  const open = me.mode === 'open';
  return (
    <Link to="/settings/tokens" className="identity" title={open ? t('shell.identity.openHint') : t('shell.identity.tokenHint', { name: me.name })}>
      {open ? <ShieldOff size={14} className="text-warning" /> : <UserRound size={14} />}
      <span className="identity-name truncate">{open ? t('shell.identity.anonymous') : me.name}</span>
      <RoleBadge role={me.role} small />
    </Link>
  );
}

/**
 * The console's primary navigation. `rail` (docs mode): on desktop it shrinks to an icon rail so
 * the documentation's own navigation is the one prominent column; labels stay for screen
 * readers and show as tooltips. The mobile drawer is unchanged.
 */
export function Sidebar({ rail = false }: { rail?: boolean }) {
  const t = useT();
  const navOpen = useUi((s) => s.navOpen);
  const setNavOpen = useUi((s) => s.setNavOpen);
  const conn = useTelemetry((s) => s.conn);
  const version = useTopicSelect<ClusterOverview, string | undefined>(TOPICS.CLUSTER, (o) => o?.version);
  const vt = viewTransitionsOn();
  return (
    <>
      {navOpen && <div className="scrim nav-scrim" onClick={() => setNavOpen(false)} />}
      <nav className={`sidebar ${navOpen ? 'open' : ''} ${rail ? 'rail' : ''}`} aria-label={t('shell.nav.primary')}>
        <div className="brand">
          <Logo />
          <span>FluxMQ</span>
        </div>
        <ul className="nav">
          {NAV.map((n) => (
            <li key={n.to}>
              <NavLink
                to={n.to}
                end={n.end}
                viewTransition={vt}
                onClick={() => {
                  markViewTransition();
                  setNavOpen(false);
                }}
                title={rail ? t(n.label) : n.hint ? t(n.hint) : undefined}
              >
                <n.icon size={17} strokeWidth={1.75} />
                <span className="nav-label">{t(n.label)}</span>
                {n.badge}
              </NavLink>
            </li>
          ))}
        </ul>
        <div className="nav-spacer" />
        <ul className="nav">
          <li>
            <NavLink
              to="/docs"
              title={rail ? t('shell.nav.docs') : undefined}
              viewTransition={vt}
              onClick={() => {
                markViewTransition();
                setNavOpen(false);
              }}
            >
              <BookOpen size={17} strokeWidth={1.75} />
              <span className="nav-label">{t('shell.nav.docs')}</span>
            </NavLink>
          </li>
          <li>
            <NavLink
              to="/settings"
              title={rail ? t('shell.nav.settings') : undefined}
              viewTransition={vt}
              onClick={() => {
                markViewTransition();
                setNavOpen(false);
              }}
            >
              <Settings size={17} strokeWidth={1.75} />
              <span className="nav-label">{t('shell.nav.settings')}</span>
            </NavLink>
          </li>
        </ul>
        <Identity />
        <div className="sidebar-foot">
          <span>
            {version ? `v${version}` : 'FluxMQ'} · {conn.status === 'open' ? (conn.rttMs != null ? t('shell.foot.rtt', { ms: conn.rttMs }) : t('shell.foot.connected')) : conn.revoked ? t('shell.foot.revoked') : t(`shell.conn.${conn.status}` as TKey)}
          </span>
        </div>
      </nav>
    </>
  );
}

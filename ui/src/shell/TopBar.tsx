import { Link, useLocation } from 'react-router';
import { docsHrefForPath } from '../docs/helpMap';
import { markViewTransition, viewTransitionsOn } from '../lib/motion';
import { Bell, CircleHelp, Languages, Menu, Moon, Pause, Play, Plus, Search, Sun } from 'lucide-react';
import { TOPICS, type AlertsState, type ClusterOverview, type Health } from '../api/types';
import { alertCounts } from '../store/selectors';
import { useTelemetry } from '../store/telemetry';
import { useUi } from '../store/ui';
import { useTopicSelect } from '../telemetry';
import { HealthDot } from '../components/ui';
import { HEALTH_LABEL } from '../lib/labels';
import { GateButton } from '../components/auth/Gate';
import { useCluster, useClusterPolling } from '../store/cluster';
import { LANG_NAME, LANGS, useLang, useT, type Lang } from '../i18n';

const isMac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform);

function ClusterPill() {
  const t = useT();
  useClusterPolling(10_000);
  // "N/M nodes up" is status-based (`nodes_up`: reachable members); the colour is the
  // overview's `health`, which also covers node health (disk, fsync) and partitions.
  const o = useTopicSelect<ClusterOverview, { name?: string; health?: Health; up?: number; nt?: number; partial?: boolean }>(TOPICS.CLUSTER, (o) => ({
    name: o?.cluster_name,
    health: o?.health,
    // A v0.1 server has no `nodes_up` (single node: healthy ⇔ up).
    up: o ? (o.nodes_up ?? o.nodes_healthy) : undefined,
    nt: o?.nodes_total,
    partial: o?.partial,
  }));
  // Partition problems from the cluster status poll (detail only — never the count or colour).
  const status = useCluster((s) => s.status);
  const name = o.name ?? status?.metadata.cluster_name;
  const health = o.health;
  let sub = o.nt != null && o.up != null ? t('shell.cluster.nodesUp', { up: o.up, total: o.nt }) : '—';
  if (status?.offline_partitions) sub += ` · ${t('shell.cluster.offline', { count: status.offline_partitions })}`;
  else if (status?.under_replicated_partitions) sub += ` · ${t('shell.cluster.underReplicated', { count: status.under_replicated_partitions })}`;
  else if (health && health !== 'healthy') sub += ` · ${HEALTH_LABEL[health]}`;
  // Telemetry merged from reachable peers only: say so, or the totals look like a traffic drop.
  const unreachable = (o.nt ?? 0) - (o.up ?? 0);
  const partialHint = o.partial ? (unreachable > 0 ? t('shell.cluster.partialHintNodes', { count: unreachable }) : t('shell.cluster.partialHint')) : undefined;
  return (
    <Link to="/nodes" title={partialHint} className="cluster-pill" aria-label={name ? t('shell.cluster.ariaNamed', { name, sub }) : t('shell.cluster.cluster')}>
      {health ? <HealthDot health={health} pulse /> : <span className="dot muted" aria-hidden />}
      <span className="cluster-pill-text">
        <b>{name ?? t('shell.cluster.connecting')}</b>
        <small>
          {sub}
          {o.partial && <span className="partial-hint"> · {t('shell.cluster.partial')}</span>}
        </small>
      </span>
    </Link>
  );
}

function LiveToggle() {
  const t = useT();
  const paused = useTelemetry((s) => s.paused);
  const setPaused = useTelemetry((s) => s.setPaused);
  return (
    <button
      className={`live-toggle ${paused ? 'paused' : ''}`}
      onClick={() => setPaused(!paused)}
      aria-pressed={!paused}
      title={paused ? t('shell.top.resumeHint') : t('shell.top.freezeHint')}
    >
      {paused ? <Play size={13} /> : <span className="live-dot" aria-hidden />}
      <span>{paused ? t('shell.top.paused') : t('shell.top.live')}</span>
      {!paused && <Pause size={12} className="live-pause-ico" />}
    </button>
  );
}

function Bellish() {
  const t = useT();
  const c = useTopicSelect<AlertsState, ReturnType<typeof alertCounts>>(TOPICS.ALERTS, alertCounts);
  return (
    <Link to="/alerts" className="btn btn-ghost btn-icon bell" aria-label={c.total ? t('shell.top.alertsOpen', { count: c.total }) : t('shell.top.alerts')}>
      <Bell size={17} />
      {c.total > 0 && <span className={`bell-dot ${c.critical ? '' : 'warning'}`}>{c.total}</span>}
    </Link>
  );
}

/** Light ⇄ dark in one click (an explicit choice; Settings also offers "follow the system"). */
export function ThemeSwitch() {
  const t = useT();
  const theme = useUi((s) => s.theme);
  const setTheme = useUi((s) => s.setTheme);
  const next = theme === 'dark' ? 'light' : 'dark';
  const label = next === 'light' ? t('shell.top.themeLight') : t('shell.top.themeDark');
  return (
    <button className="btn btn-ghost btn-icon" onClick={() => setTheme(next)} title={label} aria-label={label}>
      {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
    </button>
  );
}

/** Compact EN/RU switch, reachable from every screen (the full setting lives in Settings → General). */
export function LangSwitch() {
  const t = useT();
  const lang = useLang((s) => s.lang);
  const setLang = useLang((s) => s.setLang);
  const next: Lang = LANGS[(LANGS.indexOf(lang) + 1) % LANGS.length]!;
  return (
    <button className="btn btn-ghost lang-switch" onClick={() => setLang(next)} title={`${t('shell.top.language')}: ${LANG_NAME[lang]} → ${LANG_NAME[next]}`} aria-label={`${t('shell.top.language')}: ${LANG_NAME[lang]}`}>
      <Languages size={15} />
      <span className="lang-code">{lang.toUpperCase()}</span>
    </button>
  );
}

/** Contextual help: the docs article (and anchor) for the screen being viewed. */
function HelpLink() {
  const t = useT();
  const { pathname } = useLocation();
  if (pathname.startsWith('/docs')) return null;
  return (
    <Link to={docsHrefForPath(pathname)} viewTransition={viewTransitionsOn()} onClick={markViewTransition} className="btn btn-ghost btn-icon" aria-label={t('shell.help')} title={t('shell.help')}>
      <CircleHelp size={16} />
    </Link>
  );
}

export function TopBar() {
  const t = useT();
  const setPaletteOpen = useUi((s) => s.setPaletteOpen);
  const setCreateOpen = useUi((s) => s.setCreateOpen);
  const setNavOpen = useUi((s) => s.setNavOpen);
  return (
    <header className="topbar">
      <button className="btn btn-ghost btn-icon nav-burger" aria-label={t('shell.top.openNav')} onClick={() => setNavOpen(true)}>
        <Menu size={18} />
      </button>
      <ClusterPill />
      <button className="search-trigger" onClick={() => setPaletteOpen(true)} aria-label={t('shell.top.search')} aria-keyshortcuts={isMac ? 'Meta+K' : 'Control+K'}>
        <Search size={15} />
        <span>{t('shell.top.searchPlaceholder')}</span>
        <span className="search-kbd">
          <kbd>{isMac ? '⌘' : 'Ctrl'}</kbd>
          <kbd>K</kbd>
        </span>
      </button>
      <div className="topbar-spacer" />
      <LiveToggle />
      <LangSwitch />
      <ThemeSwitch />
      <HelpLink />
      <Bellish />
      <GateButton need="admin" className="btn btn-primary create-btn" onClick={() => setCreateOpen(true)}>
        <Plus size={15} />
        <span>{t('shell.top.createChannel')}</span>
      </GateButton>
    </header>
  );
}

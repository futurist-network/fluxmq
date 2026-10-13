import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useLocation, useNavigate } from 'react-router';
import { Command } from 'cmdk';
import { Bell, BellRing, Clock, DatabaseBackup, Filter, Inbox, KeyRound, Send, Webhook, LayoutDashboard, Moon, Pause, Play, Plus, Radio, Route as RouteIcon, Scale, Search, Server, Settings, ShieldCheck, Users, Waypoints, type LucideIcon } from 'lucide-react';
import { useRoutes, loadRoutes } from '../store/routes';
import { useWebhooks, loadWebhooks } from '../store/webhooks';
import { runBackupNow } from '../lib/backupActions';
import { useRole } from '../store/auth';
import { roleAllows } from '../lib/rbac';
import { api } from '../api/client';
import { TOPICS, type AlertsState, type ChannelsSummary, type SearchResult } from '../api/types';
import { useTopicSelect } from '../telemetry';
import { channelNames, openAlerts } from '../store/selectors';
import { useTelemetry } from '../store/telemetry';
import { useUi } from '../store/ui';
import { channelPath, HEALTH_LABEL } from '../lib/labels';
import { LANG_NAME, useLang, useT } from '../i18n';
import { BookOpen, Languages } from 'lucide-react';
import { markViewTransition, viewTransitionsOn } from '../lib/motion';
import './palette.css';

interface Item {
  id: string;
  label: string;
  hint?: string;
  icon: LucideIcon;
  keywords?: string[];
  /** Always shown (bypasses cmdk's fuzzy filter, e.g. for "where …" queries with operators). */
  force?: boolean;
  run: () => void;
}

function Row({ item, onRun }: { item: Item; onRun: (i: Item) => void }) {
  return (
    <Command.Item value={item.id} keywords={[item.label, ...(item.keywords ?? [])]} forceMount={item.force} onSelect={() => onRun(item)}>
      <item.icon size={15} />
      <span className="pal-label">{item.label}</span>
      {item.hint && <span className="pal-hint">{item.hint}</span>}
    </Command.Item>
  );
}

export function CommandPalette() {
  const open = useUi((s) => s.paletteOpen);
  // Mounted only while open: query and results reset on every open.
  return open ? <Palette /> : null;
}

function Palette() {
  const t = useT();
  const lang = useLang((s) => s.lang);
  const setLang = useLang((s) => s.setLang);
  const setOpen = useUi((s) => s.setPaletteOpen);
  const setCreateOpen = useUi((s) => s.setCreateOpen);
  const theme = useUi((s) => s.theme);
  const setTheme = useUi((s) => s.setTheme);
  const paused = useTelemetry((s) => s.paused);
  const setPaused = useTelemetry((s) => s.setPaused);
  const navigate = useNavigate();
  const names = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const alerts = useTopicSelect<AlertsState, ReturnType<typeof openAlerts>>(TOPICS.ALERTS, openAlerts);
  const [query, setQuery] = useState('');
  const [remote, setRemote] = useState<SearchResult[]>([]);
  const routes = useRoutes((s) => s.routes);
  const role = useRole();
  const { pathname } = useLocation();
  const webhooks = useWebhooks((s) => s.list);
  useEffect(() => {
    if (!useRoutes.getState().routes) void loadRoutes();
    if (!useWebhooks.getState().list) void loadWebhooks();
  }, []);

  useEffect(() => {
    const q = query.trim();
    if (q.length < 2) return;
    const ctrl = new AbortController();
    const timer = setTimeout(() => {
      api
        .search(q, ctrl.signal)
        .then(setRemote)
        .catch(() => setRemote([]));
    }, 120);
    return () => {
      clearTimeout(timer);
      ctrl.abort();
    };
  }, [query]);

  // cmdk keeps the list's scroll offset while results are re-filtered/re-ranked (and server
  // results arrive late), which can leave the best match scrolled out of view. Results are
  // ranked, so a new query always starts at the top.
  const listRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const id = requestAnimationFrame(() => {
      if (listRef.current) listRef.current.scrollTop = 0;
    });
    return () => cancelAnimationFrame(id);
  }, [query, remote]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [setOpen]);

  const go = (to: string) => () => {
    const vt = viewTransitionsOn();
    if (vt) markViewTransition();
    void navigate(to, { viewTransition: vt });
  };
  const run = (i: Item) => {
    setOpen(false);
    i.run();
  };

  // "Search messages where …": the channel being viewed, else the first one.
  const here = /^\/channels\/([^/]+)/.exec(pathname);
  const currentChannel = here ? decodeURIComponent(here[1]!) : null;
  const filterLike = /(!=|>=|<=|[=<>])|\s(in|not in|prefix|contains|exists)\b/i.test(query);
  const filterTargets = currentChannel ? [currentChannel, ...names.filter((n) => n !== currentChannel)] : names;
  const isAdmin = roleAllows(role, 'admin');
  const filterItems: Item[] = filterLike
    ? filterTargets.slice(0, 6).map((n) => ({
        id: `flt:${n}`,
        label: t('shell.pal.searchIn', { channel: n, query: query.trim() }),
        hint: t('shell.pal.hint.filter'),
        icon: Filter,
        force: true,
        keywords: [query, n],
        run: go(`${channelPath(n, 'messages')}?filter=${encodeURIComponent(query.trim())}`),
      }))
    : [];
  const routeItems: Item[] = query.trim()
    ? (routes ?? []).map((r) => ({ id: `rt:${r.id}`, label: t('shell.pal.route', { name: r.name }), hint: `${r.source} → ${r.targets.join(', ')}`, icon: RouteIcon, keywords: [r.name, r.source, ...r.targets, 'route'], run: go(`/routing/${encodeURIComponent(r.id)}`) }))
    : [];

  const webhookItems: Item[] = query.trim()
    ? (webhooks ?? []).map((w) => ({ id: `wh:${w.id}`, label: t('shell.pal.webhook', { name: w.name }), hint: `${w.channel} → ${w.url.replace(/^https?:\/\//, '').split('/')[0]}`, icon: Webhook, keywords: [w.name, w.channel, 'webhook'], run: go(`/webhooks/${encodeURIComponent(w.id)}`) }))
    : [];

  const actions: Item[] = [
    ...(isAdmin ? [{ id: 'act:create', label: t('shell.pal.act.createChannel'), icon: Plus, keywords: ['new', 'add'], run: () => setCreateOpen(true) }] : []),
    ...(isAdmin ? [{ id: 'act:route', label: t('shell.pal.act.createRoute'), icon: RouteIcon, keywords: ['new', 'routing', 'rule', 'when', 'then'], run: go(currentChannel ? `/routing/new?source=${encodeURIComponent(currentChannel)}` : '/routing/new') }] : []),
    ...(filterTargets.length && !filterLike
      ? [{ id: 'act:filter', label: t('shell.pal.act.filter'), hint: filterTargets[0], icon: Filter, keywords: ['filter', 'where', 'find', 'query', 'payload'], run: go(`${channelPath(filterTargets[0]!, 'messages')}?focus=filter`) }]
      : []),
    ...(isAdmin ? [{ id: 'act:webhook', label: t('shell.pal.act.createWebhook'), icon: Webhook, keywords: ['new', 'http', 'push', 'endpoint', 'subscription'], run: go(currentChannel ? `/webhooks/new?channel=${encodeURIComponent(currentChannel)}` : '/webhooks/new') }] : []),
    ...(isAdmin ? [{ id: 'act:telegram', label: t('shell.pal.act.telegram'), icon: Send, keywords: ['notify', 'notification', 'alert', 'telegram', 'bot', 'chat'], run: go('/settings/notifications?add=telegram') }] : []),
    ...(isAdmin ? [{ id: 'act:notifier', label: t('shell.pal.act.notifier'), icon: BellRing, keywords: ['notify', 'slack', 'discord', 'email', 'pagerduty', 'webhook', 'alert'], run: go('/settings/notifications?add=slack') }] : []),
    ...(isAdmin
      ? [
          {
            id: 'act:backup',
            label: t('shell.pal.act.backup'),
            icon: DatabaseBackup,
            keywords: ['backup', 'snapshot', 's3', 'restore'],
            run: () => {
              void runBackupNow();
              navigate('/settings/backups');
            },
          },
        ]
      : []),
    ...(isAdmin ? [{ id: 'act:token', label: t('shell.pal.act.token'), icon: KeyRound, keywords: ['token', 'auth', 'rbac', 'key'], run: go('/settings/tokens?create=1') }] : []),
    { id: 'act:dlq', label: t('shell.pal.act.dlq'), icon: Inbox, keywords: ['dlq', 'errors', 'failed'], run: go('/dlq') },
    { id: 'act:live', label: paused ? t('shell.pal.act.resume') : t('shell.pal.act.pause'), icon: paused ? Play : Pause, keywords: ['freeze', 'live'], run: () => setPaused(!paused) },
    { id: 'act:theme', label: theme === 'dark' ? t('shell.pal.act.themeLight') : t('shell.pal.act.themeDark'), icon: Moon, keywords: ['theme', 'appearance', ...t('shell.pal.kw.settings').split(' ')], run: () => setTheme(theme === 'dark' ? 'light' : 'dark') },
    {
      id: 'act:lang',
      label: lang === 'en' ? t('shell.pal.act.langRu') : t('shell.pal.act.langEn'),
      hint: LANG_NAME[lang === 'en' ? 'ru' : 'en'],
      icon: Languages,
      keywords: ['language', 'lang', 'locale', 'russian', 'english', 'язык', 'русский', 'английский'],
      run: () => setLang(lang === 'en' ? 'ru' : 'en'),
    },
  ];
  const pages: Item[] = [
    { id: 'go:dashboard', label: t('shell.pal.go.dashboard'), icon: LayoutDashboard, keywords: ['home', 'overview'], run: go('/') },
    { id: 'go:channels', label: t('shell.pal.go.channels'), icon: Waypoints, keywords: ['topics', 'queues', ...t('shell.pal.kw.channels').split(' ')], run: go('/channels') },
    { id: 'go:consumers', label: t('shell.pal.go.consumers'), icon: Users, keywords: ['groups', 'workers'], run: go('/consumers') },
    { id: 'go:routing', label: t('shell.pal.go.routing'), icon: RouteIcon, keywords: ['routes', 'rules', 'forward'], run: go('/routing') },
    { id: 'go:webhooks', label: t('shell.pal.go.webhooks'), icon: Webhook, keywords: ['http', 'push', 'endpoint'], run: go('/webhooks') },
    { id: 'go:scheduled', label: t('shell.pal.go.scheduled'), icon: Clock, keywords: ['delay', 'delayed', 'schedule', 'later', 'timer', 'cron'], run: go('/scheduled') },
    { id: 'go:live', label: t('shell.pal.go.live'), icon: Radio, keywords: ['websocket', 'sse', 'live', 'stream', 'browser', 'realtime'], run: go('/live') },
    { id: 'go:alerts', label: t('shell.pal.go.alerts'), icon: Bell, keywords: ['problems', 'issues'], run: go('/alerts') },
    { id: 'go:nodes', label: t('shell.pal.go.nodes'), icon: Server, keywords: ['cluster', 'disk'], run: go('/nodes') },
    { id: 'go:settings', label: t('shell.pal.go.settings'), icon: Settings, keywords: ['theme', 'language', ...t('shell.pal.kw.settings').split(' ')], run: go('/settings') },
    { id: 'go:notifications', label: t('shell.pal.go.notifications'), icon: BellRing, keywords: ['telegram', 'slack', 'email', 'pagerduty', 'alerts', 'rules', 'feed'], run: go('/settings/notifications') },
    { id: 'go:backups', label: t('shell.pal.go.backups'), icon: DatabaseBackup, keywords: ['backup', 'restore', 's3', 'r2', 'minio'], run: go('/settings/backups') },
    { id: 'go:tokens', label: t('shell.pal.go.tokens'), icon: KeyRound, keywords: ['token', 'auth', 'rbac', 'roles'], run: go('/settings/tokens') },
    { id: 'go:security', label: t('shell.pal.go.security'), icon: ShieldCheck, keywords: ['tls', 'ssl', 'https', 'certificate', 'mtls', 'expiry'], run: go('/settings/security') },
    { id: 'go:docs', label: t('shell.pal.go.docs'), icon: BookOpen, keywords: ['docs', 'help', 'guide', 'manual', 'документация', 'справка', 'помощь'], run: go('/docs') },
    { id: 'go:rebalance', label: t('shell.pal.go.rebalance'), icon: Scale, keywords: ['rebalance', 'balance', 'move', 'replicas', 'partitions', 'cluster'], run: go('/nodes#rebalancing') },
  ];
  const typed = query.trim().length > 0;
  const channels: Item[] = names.flatMap((n) => [
    { id: `ch:open:${n}`, label: t('shell.pal.ch.open', { channel: n }), hint: t('shell.pal.hint.channel'), icon: Waypoints, keywords: [n], run: go(channelPath(n)) },
    ...(typed
      ? [
          { id: `ch:msg:${n}`, label: t('shell.pal.ch.search', { channel: n }), hint: t('shell.pal.hint.messages'), icon: Search, keywords: [n, 'find', 'key', 'offset'], run: go(channelPath(n, 'messages')) },
          { id: `ch:tail:${n}`, label: t('shell.pal.ch.tail', { channel: n }), hint: t('shell.pal.hint.messages'), icon: Radio, keywords: [n, 'stream', 'watch'], run: go(`${channelPath(n, 'messages')}?mode=tail`) },
          { id: `ch:cons:${n}`, label: t('shell.pal.ch.consumers', { channel: n }), hint: t('shell.pal.hint.consumers'), icon: Users, keywords: [n, 'groups', 'lag'], run: go(channelPath(n, 'consumers')) },
        ]
      : []),
  ]);
  const local = new Set(channels.map((c) => c.id));
  const server: Item[] = (query.trim().length >= 2 ? remote : [])
    .filter((r) => !(r.kind === 'channel' && local.has(`ch:open:${r.label}`)))
    .map((r) => ({ id: `srv:${r.kind}:${r.link}`, label: r.label, hint: r.kind === 'group' ? t('shell.pal.hint.group') : r.kind === 'channel' ? t('shell.pal.hint.channel') : r.kind, icon: r.kind === 'group' ? Users : Waypoints, keywords: [query], run: go(r.link) }));
  const alertItems: Item[] = alerts.map((a) => ({ id: `al:${a.id}`, label: a.title, hint: HEALTH_LABEL[a.severity], icon: Bell, keywords: ['alert', a.channel ?? '', a.group ?? ''], run: go(a.link ?? '/alerts') }));

  return createPortal(
    <>
      <div className="scrim" onClick={() => setOpen(false)} />
      <div className="palette" role="dialog" aria-modal="true" aria-label={t('shell.pal.label')}>
        <Command label={t('shell.pal.label')} loop>
          <div className="pal-input">
            <Search size={16} />
            <Command.Input autoFocus value={query} onValueChange={setQuery} placeholder={t('shell.pal.placeholder')} />
            <kbd>esc</kbd>
          </div>
          <Command.List ref={listRef}>
            {!filterItems.length && <Command.Empty>{t('shell.pal.empty', { query })}</Command.Empty>}
            {filterItems.length > 0 && (
              <Command.Group heading={t('shell.pal.g.filter')} forceMount>
                {filterItems.map((i) => (
                  <Row key={i.id} item={i} onRun={run} />
                ))}
              </Command.Group>
            )}
            {channels.length > 0 && (
              <Command.Group heading={t('shell.pal.g.channels')}>
                {channels.map((i) => (
                  <Row key={i.id} item={i} onRun={run} />
                ))}
              </Command.Group>
            )}
            {server.length > 0 && (
              <Command.Group heading={t('shell.pal.g.server')}>
                {server.map((i) => (
                  <Row key={i.id} item={i} onRun={run} />
                ))}
              </Command.Group>
            )}
            {routeItems.length > 0 && (
              <Command.Group heading={t('shell.pal.g.routes')}>
                {routeItems.map((i) => (
                  <Row key={i.id} item={i} onRun={run} />
                ))}
              </Command.Group>
            )}
            {webhookItems.length > 0 && (
              <Command.Group heading={t('shell.pal.g.webhooks')}>
                {webhookItems.map((i) => (
                  <Row key={i.id} item={i} onRun={run} />
                ))}
              </Command.Group>
            )}
            {alertItems.length > 0 && (
              <Command.Group heading={t('shell.pal.g.alerts')}>
                {alertItems.map((i) => (
                  <Row key={i.id} item={i} onRun={run} />
                ))}
              </Command.Group>
            )}
            <Command.Group heading={t('shell.pal.g.actions')}>
              {actions.map((i) => (
                <Row key={i.id} item={i} onRun={run} />
              ))}
            </Command.Group>
            <Command.Group heading={t('shell.pal.g.nav')}>
              {pages.map((i) => (
                <Row key={i.id} item={i} onRun={run} />
              ))}
            </Command.Group>
          </Command.List>
          <div className="pal-foot">
            <span>
              <kbd>↑</kbd>
              <kbd>↓</kbd> {t('shell.pal.foot.navigate')}
            </span>
            <span>
              <kbd>↵</kbd> {t('shell.pal.foot.open')}
            </span>
            <span>
              <kbd>esc</kbd> {t('shell.pal.foot.close')}
            </span>
          </div>
        </Command>
      </div>
    </>,
    document.body,
  );
}

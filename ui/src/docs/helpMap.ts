/**
 * Contextual help: which docs article explains the console screen at a given path.
 * Used by the shell's "?" button: `<Link to={docsHrefForPath(pathname)}>`.
 * Anchors are explicit `{#id}` heading ids, identical in both languages.
 */

interface HelpTarget {
  slug: string;
  anchor?: string;
}

const CHANNEL_TABS: Record<string, string> = {
  messages: 'messages',
  consumers: 'consumers-tab',
  routing: 'routing-tab',
  live: 'live-tab',
  storage: 'storage',
  settings: 'settings-tab',
};

/** Ordered: first match wins. */
const RULES: [RegExp, (m: RegExpExecArray) => HelpTarget][] = [
  [/^\/channels\/[^/]+\/([^/]+)\/?$/, (m) => ({ slug: 'channel-detail', anchor: CHANNEL_TABS[m[1]!] })],
  [/^\/channels\/[^/]+\/?$/, () => ({ slug: 'channel-detail' })],
  [/^\/channels\/?$/, () => ({ slug: 'channels' })],
  [/^\/consumers\/?$/, () => ({ slug: 'consumers' })],
  [/^\/routing\/.+$/, () => ({ slug: 'routing', anchor: 'route-editor' })],
  [/^\/routing\/?$/, () => ({ slug: 'routing' })],
  [/^\/webhooks\/.+$/, () => ({ slug: 'webhooks', anchor: 'webhook-editor' })],
  [/^\/webhooks\/?$/, () => ({ slug: 'webhooks' })],
  [/^\/live\/?$/, () => ({ slug: 'live' })],
  [/^\/scheduled\/?$/, () => ({ slug: 'delayed' })],
  [/^\/dlq\/?$/, () => ({ slug: 'dlq' })],
  [/^\/alerts\/?$/, () => ({ slug: 'alerts' })],
  [/^\/nodes\/?$/, () => ({ slug: 'nodes' })],
  [/^\/settings\/tokens\/?$/, () => ({ slug: 'access-tokens' })],
  [/^\/settings\/notifications\/?$/, () => ({ slug: 'notifications' })],
  [/^\/settings\/backups\/?$/, () => ({ slug: 'backups' })],
  [/^\/settings\/security\/?$/, () => ({ slug: 'security' })],
  [/^\/settings\/?$/, () => ({ slug: 'settings' })],
  [/^\/?$/, () => ({ slug: 'dashboard' })],
];

/** The article for a console path; `overview` when nothing more specific applies. */
export function docForPath(pathname: string): HelpTarget {
  for (const [re, target] of RULES) {
    const m = re.exec(pathname);
    if (m) {
      const t = target(m);
      return t.anchor ? t : { slug: t.slug };
    }
  }
  return { slug: 'overview' };
}

/** `/docs/<slug>[#anchor]` for a console path. */
export function docsHrefForPath(pathname: string): string {
  const t = docForPath(pathname);
  return `/docs/${t.slug}${t.anchor ? `#${t.anchor}` : ''}`;
}

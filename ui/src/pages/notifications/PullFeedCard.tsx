import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { Bell, CircleCheck, Rss, Zap } from 'lucide-react';
import { api, ApiError } from '../../api/client';
import { SYSTEM_EVENTS_CHANNEL, type FeedItem } from '../../api/types';
import { fmtRelative } from '../../lib/format';
import { useNow } from '../../lib/hooks';
import { channelPath } from '../../lib/labels';
import { CodeBlock } from '../../components/forms';
import { HealthDot } from '../../components/ui';
import { t as tr, useT } from '../../i18n';

const PREVIEW = 6;

function curlExample(origin: string): string {
  return `# Long-poll: returns as soon as something new exists (max 25 s).
# The cursor file lets you resume after a restart.
URL="${origin}/api/events/feed"
# Seed the newest 20 once: curl "$URL?tail=20", then use its next_cursor
cursor=$(cat .fluxmq-cursor 2>/dev/null || echo latest)
while true; do
  page=$(curl -s -H "Authorization: Bearer $FLUXMQ_TOKEN" \\
    "$URL?after=$cursor&limit=100&wait_ms=25000")
  echo "$page" | jq -c '.items[]'   # one item per line
  next=$(echo "$page" | jq -r '.next_cursor // empty')
  cursor=\${next:-$cursor}
  echo "$cursor" > .fluxmq-cursor
done`;
}

function itemText(i: FeedItem): { title: string; sub: string; tone: 'healthy' | 'warning' | 'critical' } {
  if (i.alert) {
    const resolved = i.kind === 'alert_resolved';
    return { title: resolved ? tr('notifications.feed.resolved', { title: i.alert.title }) : i.alert.title, sub: [i.kind, i.alert.channel, i.alert.group].filter(Boolean).join(' · '), tone: resolved ? 'healthy' : i.alert.severity };
  }
  if (i.event) return { title: i.event.title, sub: `${i.event.kind} · ${i.event.subject}`, tone: i.event.severity };
  return { title: i.kind, sub: '', tone: 'healthy' };
}

/** Live tail of the pull feed (long-poll loop while mounted). */
function FeedPreview() {
  const [items, setItems] = useState<FeedItem[] | null>(null);
  const t = useT();
  const [error, setError] = useState<string | null>(null);
  const now = useNow();
  useEffect(() => {
    const ctrl = new AbortController();
    let stop = false;
    (async () => {
      // Seed with the newest items (`tail`), then long-poll on from the returned cursor.
      let cursor: string | undefined;
      try {
        const seed = await api.eventsFeed({ tail: PREVIEW }, ctrl.signal);
        setItems(seed.items);
        cursor = seed.next_cursor ?? 'latest';
        while (!stop) {
          const p = await api.eventsFeed({ after: cursor ?? 'latest', limit: 100, wait_ms: 25_000 }, ctrl.signal);
          if (p.items.length) setItems((cur) => [...(cur ?? []), ...p.items].slice(-PREVIEW));
          cursor = p.next_cursor ?? cursor;
        }
      } catch (e) {
        if (ctrl.signal.aborted) return;
        setError(e instanceof ApiError && e.isNotFound ? tr('notifications.feed.noFeed') : e instanceof Error ? e.message : String(e));
      }
    })();
    return () => {
      stop = true;
      ctrl.abort();
    };
  }, []);
  if (error) return <p className="muted feed-empty">{error}</p>;
  if (!items) return <p className="faint feed-empty">{t('notifications.feed.loading')}</p>;
  if (!items.length) return <p className="faint feed-empty">{t('notifications.feed.empty')}</p>;
  return (
    <ul className="feed-list" aria-live="polite">
      {[...items].reverse().map((i) => {
        const it = itemText(i);
        return (
          <li key={i.cursor}>
            <HealthDot health={it.tone} />
            <span className="feed-title truncate">{it.title}</span>
            <span className="feed-sub faint mono truncate">{it.sub}</span>
            <span className="feed-meta faint mono">
              #{i.cursor} · {fmtRelative(i.ts, now)}
            </span>
          </li>
        );
      })}
    </ul>
  );
}

export function PullFeedCard() {
  const t = useT();
  const origin = typeof location === 'undefined' ? 'http://localhost:7421' : location.origin;
  return (
    <section className="card" aria-label={t('notifications.feed.aria')}>
      <div className="card-head">
        <h2>
          <Rss size={15} className="faint" style={{ verticalAlign: -2, marginRight: 6 }} />
          {t('notifications.feed.title')}
        </h2>
        <span className="card-sub">{t('notifications.feed.sub')}</span>
      </div>
      <div className="card-body feed-grid">
        <div className="feed-explain explain">
          <p>
            {t.rich('notifications.feed.explain', {
              channel: (
                <Link to={channelPath(SYSTEM_EVENTS_CHANNEL, 'messages')} className="mono btn-link">
                  {SYSTEM_EVENTS_CHANNEL}
                </Link>
              ),
            })}
          </p>
          <ul className="feed-points">
            <li>
              <Zap size={13} /> <b>{t('notifications.feed.longPollLabel')}</b> {t.rich('notifications.feed.longPoll', { waitMs: <span className="mono">{'wait_ms'}</span> })}
            </li>
            <li>
              <CircleCheck size={13} /> <b>{t('notifications.feed.cursorLabel')}</b>{' '}
              {t.rich('notifications.feed.cursor', {
                next: <span className="mono">{'next_cursor'}</span>,
                after: <span className="mono">{'after'}</span>,
                latest: <span className="mono">{'after=latest'}</span>,
                gap: <span className="mono">{'gap: true'}</span>,
              })}
            </li>
            <li>
              <Bell size={13} /> <b>{t('notifications.feed.tokenLabel')}</b>{' '}
              {t.rich('notifications.feed.token', {
                viewer: <b>{t('notifications.feed.viewer')}</b>,
                channel: <span className="mono">{SYSTEM_EVENTS_CHANNEL}</span>,
                link: (
                  <Link to="/settings/tokens?create=1" className="btn-link">
                    {t('notifications.feed.newToken')}
                  </Link>
                ),
              })}
            </li>
          </ul>
          <CodeBlock title="curl" code={curlExample(origin)} />
        </div>
        <div className="feed-preview">
          <div className="feed-preview-head">
            <span className="live-dot" aria-hidden /> {t('notifications.feed.latest')} <span className="faint">{t('notifications.feed.latestSub')}</span>
          </div>
          <FeedPreview />
        </div>
      </div>
    </section>
  );
}

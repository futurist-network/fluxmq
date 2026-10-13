import { useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';
import { useShallow } from 'zustand/react/shallow';
import { Search, Users } from 'lucide-react';
import { TOPICS, type ChannelsSummary, type ConsumersState } from '../api/types';
import { useTopic, useTopicSelect } from '../telemetry';
import { useTelemetry } from '../store/telemetry';
import { channelNames, flattenGroups } from '../store/selectors';
import { fmtCount, fmtRate } from '../lib/format';
import { GroupCard } from '../components/consumers/GroupCard';
import { InstanceDrawer } from '../components/consumers/InstanceDrawer';
import { ResetOffsetsDialog } from '../components/consumers/ResetOffsetsDialog';
import { useAlerts } from '../components/HealthWhy';
import { EmptyState, Segmented, SkeletonRows, Stat } from '../components/ui';
import { useT } from '../i18n';
import '../components/consumers/consumers.css';

/** Subscribes to one channel's consumers topic while mounted (renders nothing). */
function ConsumersSubscription({ channel }: { channel: string }) {
  useTopic(TOPICS.consumers(channel));
  return null;
}

/**
 * Global consumers view. The contract has no cluster-wide consumers topic, so this page
 * subscribes to `channel.{name}.consumers` for every channel and aggregates client-side.
 */
export default function ConsumersPage() {
  const names = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const loadedChannels = useTelemetry((s) => TOPICS.CHANNELS in s.topics);
  const [filter, setFilter] = useState<'all' | 'problems'>('all');
  const [q, setQ] = useState('');
  const t = useT();

  const states = useTelemetry(useShallow((s) => names.map((n) => s.topics[TOPICS.consumers(n)] as ConsumersState | undefined)));
  const alerts = useAlerts();
  const groups = useMemo(() => flattenGroups(Object.fromEntries(names.map((n, i) => [n, states[i]])), alerts), [names, states, alerts]);
  // Deep link from an alert: /consumers?instance=…[&channel=…&group=…] opens that instance directly.
  const [params, setParams] = useSearchParams();
  const linkInstance = params.get('instance');
  const linked = useMemo(() => {
    if (!linkInstance) return null;
    const ch = params.get('channel');
    const gr = params.get('group');
    const hit = groups.find((r) => (!ch || r.channel === ch) && (!gr || r.group.group === gr) && linkInstance in r.group.instances);
    if (hit) return { channel: hit.channel, group: hit.group.group };
    return ch && gr ? { channel: ch, group: gr } : null;
  }, [linkInstance, params, groups]);
  const [linkReset, setLinkReset] = useState(false);
  const closeLink = () => {
    const next = new URLSearchParams(params);
    for (const k of ['instance', 'channel', 'group']) next.delete(k);
    setParams(next, { replace: true });
  };
  const loading = !loadedChannels || (names.length > 0 && states.every((s) => s === undefined));

  const totals = useMemo(() => {
    let inst = 0;
    let lag = 0;
    let rate = 0;
    let bad = 0;
    for (const { group: g, health } of groups) {
      inst += Object.keys(g.instances).length;
      lag += g.lag;
      rate += g.ack_rate;
      if (health !== 'healthy') bad++;
    }
    return { inst, lag, rate, bad };
  }, [groups]);

  const query = q.trim().toLowerCase();
  const shown = groups.filter(
    (r) => (filter === 'all' || r.health !== 'healthy') && (!query || r.group.group.toLowerCase().includes(query) || r.channel.toLowerCase().includes(query)),
  );

  return (
    <div className="page">
      {names.map((n) => (
        <ConsumersSubscription key={n} channel={n} />
      ))}
      <div className="page-head">
        <div className="titles">
          <h1>{t('consumers.title')}</h1>
          <p>{t('consumers.subtitle')}</p>
        </div>
      </div>

      <section className="card consumers-summary" aria-label={t('consumers.totals')}>
        <Stat label={t('consumers.stat.groups')} value={loading ? '—' : fmtCount(groups.length)} />
        <Stat label={t('consumers.stat.instances')} value={loading ? '—' : fmtCount(totals.inst)} />
        <Stat label={t('consumers.stat.ackRate')} value={loading ? '—' : fmtRate(totals.rate, false)} unit={t('consumers.unit.msgPerSec')} />
        <Stat label={t('consumers.stat.totalLag')} value={loading ? '—' : fmtCount(totals.lag)} unit={totals.bad ? t('consumers.unhealthyCount', { count: totals.bad }) : undefined} tone={totals.bad ? 'warning' : undefined} />
      </section>

      <div className="toolbar">
        <div className="input-group" style={{ width: 280, maxWidth: '100%' }}>
          <Search size={14} />
          <input className="input" placeholder={t('consumers.filter.placeholder')} value={q} onChange={(e) => setQ(e.target.value)} aria-label={t('consumers.filter.aria')} />
        </div>
        <Segmented
          value={filter}
          onChange={setFilter}
          label={t('consumers.filter.show')}
          options={[
            { value: 'all', label: t('consumers.filter.all') },
            { value: 'problems', label: `${t('consumers.filter.problems')}${totals.bad ? ` ${totals.bad}` : ''}` },
          ]}
        />
      </div>

      {loading ? (
        <section className="card">
          <SkeletonRows rows={5} />
        </section>
      ) : groups.length === 0 ? (
        <section className="card">
          <EmptyState icon={<Users size={20} />} title={t('consumers.empty.title')}>
            {t.rich('consumers.empty.body', { group: <b>{t('consumers.empty.group')}</b> })}
          </EmptyState>
        </section>
      ) : shown.length === 0 ? (
        <section className="card">
          <EmptyState compact icon={<Users size={18} />} title={filter === 'problems' ? t('consumers.empty.allHealthy') : t('consumers.empty.noMatch')} />
        </section>
      ) : (
        <div className="stack">
          {shown.map(({ channel, group, health }) => (
            <GroupCard key={`${channel}/${group.group}`} channel={channel} group={group.group} showChannel defaultOpen={health !== 'healthy'} />
          ))}
        </div>
      )}
      {linked && (
        <>
          <InstanceDrawer channel={linked.channel} group={linked.group} instanceId={linkInstance} onClose={closeLink} onResetOffsets={() => setLinkReset(true)} />
          <ResetOffsetsDialog channel={linked.channel} group={linked.group} open={linkReset} onClose={() => setLinkReset(false)} />
        </>
      )}
    </div>
  );
}

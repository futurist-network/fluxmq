import { useMemo } from 'react';
import { useSearchParams } from 'react-router';
import { Plus, Search, Waypoints } from 'lucide-react';
import { CHANNEL_MODES, TOPICS, type ChannelMode, type ChannelsSummary } from '../api/types';
import { useTopicSelect } from '../telemetry';
import { channelCountsByMode, sortedChannels } from '../store/selectors';
import { useUi } from '../store/ui';
import { MODE_LABEL } from '../lib/labels';
import { ChannelsTable, ModesLegend } from '../components/ChannelsTable';
import { EmptyState, Segmented } from '../components/ui';
import { GateButton } from '../components/auth/Gate';
import { useT } from '../i18n';

/** Demo traffic generator (a shell command — not translated). */
const DEMO_CMD = 'cargo run -p fluxmq-client --bin fluxmq-demo';

export default function ChannelsPage() {
  const t = useT();
  const [params, setParams] = useSearchParams();
  const query = params.get('q') ?? '';
  const mode = (params.get('mode') as ChannelMode | null) ?? 'all';
  const setCreateOpen = useUi((s) => s.setCreateOpen);

  // Select only the ordered name list + counts; rows subscribe to their own slices.
  const key = useTopicSelect<ChannelsSummary, string>(TOPICS.CHANNELS, (c) =>
    c ? sortedChannels(c, { query, mode }).map((x) => x.name).join('\n') : '\u0000',
  );
  const counts = useTopicSelect<ChannelsSummary, Record<ChannelMode, number> & { total: number; loaded: boolean }>(TOPICS.CHANNELS, (c) => ({
    ...channelCountsByMode(c),
    total: c ? Object.keys(c).length : 0,
    loaded: !!c,
  }));
  const loading = key === '\u0000';
  const names = useMemo(() => (loading || !key ? [] : key.split('\n')), [key, loading]);

  const set = (k: string, v: string) => {
    const next = new URLSearchParams(params);
    if (!v || v === 'all') next.delete(k);
    else next.set(k, v);
    setParams(next, { replace: true });
  };

  return (
    <div className="page">
      <div className="page-head">
        <div className="titles">
          <h1 title={t('channels.titleHint')}>{t('channels.title')}</h1>
          <p>{t('channels.subtitle')}</p>
          <ModesLegend className="modes-legend faint" />
        </div>
        <div className="page-actions">
          <GateButton need="admin" className="btn btn-primary" onClick={() => setCreateOpen(true)}>
            <Plus size={15} /> {t('channels.create')}
          </GateButton>
        </div>
      </div>

      <section className="card">
        <div className="card-head" style={{ paddingBottom: 4, flexWrap: 'wrap' }}>
          <div className="input-group" style={{ width: 280, maxWidth: '100%' }}>
            <Search size={14} />
            <input className="input" placeholder={t('channels.filterPlaceholder')} value={query} onChange={(e) => set('q', e.target.value)} aria-label={t('channels.filterAria')} />
          </div>
          <Segmented<ChannelMode | 'all'>
            value={mode}
            onChange={(v) => set('mode', v)}
            label={t('common.mode')}
            options={[
              { value: 'all', label: `${t('common.all')} ${counts.loaded ? counts.total : ''}`.trim() },
              ...CHANNEL_MODES.map((m) => ({ value: m, label: `${MODE_LABEL[m]} ${counts.loaded ? counts[m] : ''}`.trim() })),
            ]}
          />
        </div>
        <div className="card-body flush">
          {!loading && counts.total === 0 ? (
            <EmptyState
              icon={<Waypoints size={20} />}
              title={t('dashboard.empty.title')}
              actions={
                <GateButton need="admin" className="btn btn-primary" onClick={() => setCreateOpen(true)}>
                  <Plus size={15} /> {t('dashboard.onboarding.createFirst')}
                </GateButton>
              }
            >
              {t.rich('channels.empty.body', {
                stream: <b>{MODE_LABEL.stream}</b>,
                queue: <b>{MODE_LABEL.queue}</b>,
                keyed: <b>{MODE_LABEL.keyed}</b>,
                cmd: <code>{DEMO_CMD}</code>,
              })}
            </EmptyState>
          ) : !loading && names.length === 0 ? (
            <EmptyState compact icon={<Search size={18} />} title={t('channels.noMatch.title')}>
              {mode !== 'all' ? t('channels.noMatch.inMode', { query, mode: MODE_LABEL[mode] }) : t('channels.noMatch.body', { query })}
            </EmptyState>
          ) : (
            <ChannelsTable names={names} loading={loading} />
          )}
        </div>
      </section>
    </div>
  );
}

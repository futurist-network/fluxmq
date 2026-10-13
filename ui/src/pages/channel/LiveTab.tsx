import { useState } from 'react';
import { Link } from 'react-router';
import { Radio } from 'lucide-react';
import { api } from '../../api/client';
import { isSystemChannel } from '../../api/types';
import { useAsync, useInterval } from '../../lib/hooks';
import { fmtCount } from '../../lib/format';
import { channelSubscriptions, DEFAULT_LIVE_OPTIONS, type LiveOptions } from '../../lib/live';
import { useGate } from '../../components/auth/useGate';
import { LiveOptionsForm, LiveSnippetCard, LiveTryCard } from '../live/LiveConnect';
import { useT } from '../../i18n';
import '../routing/routing-page.css';
import '../live/live.css';

/** Channel → Live: who is subscribed right now, a try-it subscription and client code. */
export function LiveTab({ name }: { name: string }) {
  const t = useT();
  const admin = useGate('admin');
  const [options, setOptions] = useState<LiveOptions>(DEFAULT_LIVE_OPTIONS);
  const st = useAsync((signal) => api.liveStatus(signal), []);
  useInterval(st.reload, 5000);
  const nodes = st.data?.nodes;
  const subs = nodes ? channelSubscriptions(nodes, name) : null;
  const perChannel = nodes?.[0]?.limits.max_connections_per_channel;
  return (
    <div className="live-tab">
      <div className="routing-summary live-summary">
        <span>
          <Radio size={14} aria-hidden />{' '}
          {subs == null ? t('live.tab.loading') : t.rich('live.tab.subscribers', { count: subs, n: <b className="num">{fmtCount(subs)}</b> })}
          {nodes && nodes.length > 1 ? ` · ${t('live.tab.nodes', { count: nodes.length })}` : ''}
          {perChannel != null ? ` · ${t('live.tab.limit', { n: fmtCount(perChannel) })}` : ''}
        </span>
        <span className="faint routing-note">
          {t('live.tab.what')}{' '}
          <Link to="/live" className="btn-link">
            {t('live.tab.allChannels')}
          </Link>
        </span>
      </div>
      {isSystemChannel(name) && <p className="faint">{t('live.tab.system')}</p>}
      <section className="card">
        <div className="card-head">
          <h2>{t('live.opt.title')}</h2>
          <span className="card-sub">{t('live.opt.sub')}</span>
        </div>
        <div className="card-body">
          <LiveOptionsForm value={options} onChange={setOptions} />
        </div>
      </section>
      <div className="live-grid">
        <LiveTryCard channel={name} options={options} />
        <LiveSnippetCard channel={name} options={options} canCreateToken={admin.allowed} />
      </div>
    </div>
  );
}

import { Link, useNavigate } from 'react-router';
import { ArrowRight } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import type { Route } from '../../api/types';
import { useConditionSummary } from '../../lib/useFilterRender';
import { fmtCount, fmtNumber, fmtRate, fmtRelative } from '../../lib/format';
import { useNow } from '../../lib/hooks';
import { channelPath } from '../../lib/labels';
import { upsertRoute, useRoutes } from '../../store/routes';
import { toast } from '../../store/ui';
import { Switch } from '../ui';
import { useGate } from '../auth/useGate';
import { useT } from '../../i18n';
import './routing.css';

/** DSL keyword, shown verbatim. */
const DSL_WHEN = 'WHEN';

/** `source → a → b` with channel links. */
export function RouteFlow({ source, targets, highlight }: { source: string; targets: string[]; highlight?: string }) {
  const tr = useT();
  return (
    <span className="route-flow">
      <Link to={channelPath(source, 'routing')} className={`chip static flow-src ${highlight === source ? 'hl' : ''}`} onClick={(e) => e.stopPropagation()}>
        {source}
      </Link>
      {targets.map((t) => (
        <span key={t} className="flow-step">
          <ArrowRight size={12} className="faint" aria-label={tr('routing.flowTo')} />
          <Link to={channelPath(t, 'routing')} className={`chip static ${highlight === t ? 'hl' : ''}`} onClick={(e) => e.stopPropagation()}>
            {t}
          </Link>
        </span>
      ))}
    </span>
  );
}

/** WHEN line: the server-rendered canonical filter text. */
function RouteWhen({ r }: { r: Route }) {
  const summary = useConditionSummary(r.conditions);
  return (
    <div className="route-when mono" title={summary}>
      <span className="when-k">{DSL_WHEN}</span> {summary}
    </div>
  );
}

function EnabledToggle({ r }: { r: Route }) {
  const t = useT();
  const g = useGate('admin');
  const toggle = async (enabled: boolean) => {
    upsertRoute({ ...r, enabled });
    try {
      upsertRoute(await api.updateRoute(r.id, { enabled }));
      toast('success', t(enabled ? 'routing.toastEnabled' : 'routing.toastPaused', { name: r.name }));
    } catch (e) {
      upsertRoute(r);
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    }
  };
  return (
    <span onClick={(e) => e.stopPropagation()}>
      <Switch checked={r.enabled} onChange={toggle} label={t(r.enabled ? 'routing.pauseRoute' : 'routing.enableRoute', { name: r.name })} disabled={!g.allowed} title={g.reason ?? t(r.enabled ? 'routing.enabledClickPause' : 'routing.pausedClickEnable')} />
    </span>
  );
}

export function RoutesTable({ routes, highlight, showSource = true }: { routes: Route[]; highlight?: string; showSource?: boolean }) {
  const t = useT();
  const stats = useRoutes((s) => s.stats);
  const rates = useRoutes((s) => s.rates);
  const now = useNow();
  const navigate = useNavigate();
  return (
    <div className="table-wrap">
      <table className="table routes-table">
        <thead>
          <tr>
            <th>{t('routing.col.route')}</th>
            <th>{showSource ? t('routing.col.sourceTargets') : t('routing.col.targets')}</th>
            <th className="r">{t('routing.col.matched')}</th>
            <th className="r" title={t('routing.col.forwardedHint')}>
              {t('routing.col.forwarded')}
            </th>
            <th className="r">{t('routing.col.failed')}</th>
            <th title={t('routing.col.lastMatch')}>{t('routing.col.lastMatch')}</th>
            <th>{t('routing.col.enabled')}</th>
          </tr>
        </thead>
        <tbody>
          {routes.map((r) => {
            const s = stats[r.id];
            const rate = rates[r.id];
            return (
              <tr key={r.id} className={`clickable ${r.enabled ? '' : 'route-off'}`} onClick={() => navigate(`/routing/${encodeURIComponent(r.id)}`)}>
                <td className="route-name-cell">
                  <Link to={`/routing/${encodeURIComponent(r.id)}`} className="route-name" onClick={(e) => e.stopPropagation()}>
                    {r.name}
                  </Link>
                  <RouteWhen r={r} />
                </td>
                <td>
                  <RouteFlow source={r.source} targets={r.targets} highlight={highlight} />
                </td>
                <td className="r num">
                  {s ? fmtCount(s.matched) : '—'}
                  {s && rate != null && r.enabled && <div className="faint cell-sub">{fmtRate(rate)}</div>}
                  {s && s.evaluated > 0 && rate == null && <div className="faint cell-sub">{t('routing.ofEvaluated', { pct: fmtNumber((s.matched / s.evaluated) * 100, s.matched / s.evaluated < 0.01 ? 2 : 1) })}</div>}
                </td>
                <td className="r num">{s ? fmtCount(s.forwarded) : '—'}</td>
                <td className={`r num ${s && s.failed > 0 ? 'text-critical' : 'faint'}`}>{s ? fmtCount(s.failed) : '—'}</td>
                <td className={s?.last_matched_ms ? '' : 'faint'}>{s?.last_matched_ms ? fmtRelative(s.last_matched_ms, now) : r.enabled ? t('routing.noMatchYet') : t('routing.paused')}</td>
                <td>
                  <EnabledToggle r={r} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

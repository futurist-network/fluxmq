import { Link, useNavigate, useSearchParams } from 'react-router';
import { Info, Plus, Route as RouteIcon, Search, X } from 'lucide-react';
import { ROUTER_GROUP } from '../../api/types';
import { useRoutes, useRoutesPolling } from '../../store/routes';
import { fmtCount } from '../../lib/format';
import { channelPath } from '../../lib/labels';
import { EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import { RoutesTable } from '../../components/routing/RoutesTable';
import { conditionsKey, useRenderCache } from '../../lib/useFilterRender';
import { useT } from '../../i18n';
import './routing-page.css';

/** A route in DSL form (technical, never translated). */
const ROUTE_EXAMPLE = 'WHEN type = order.created AND country = MD THEN → billing → analytics';

export default function RoutingPage() {
  const t = useT();
  useRoutesPolling(5000);
  const routes = useRoutes((s) => s.routes);
  const stats = useRoutes((s) => s.stats);
  const error = useRoutes((s) => s.error);
  const unsupported = useRoutes((s) => s.unsupported);
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const q = params.get('q') ?? '';
  const source = params.get('source');

  const setParam = (k: string, v: string | null) => {
    const next = new URLSearchParams(params);
    if (v) next.set(k, v);
    else next.delete(k);
    setParams(next, { replace: true });
  };

  const rendered = useRenderCache((s) => s.results);
  const whenText = (conditions: Parameters<typeof conditionsKey>[0]) => {
    const r = rendered[conditionsKey(conditions)];
    return r?.status === 'ok' ? r.rendered.toLowerCase() : '';
  };
  const needle = q.trim().toLowerCase();
  const list = (routes ?? []).filter(
    (r) =>
      (!source || r.source === source || r.targets.includes(source)) &&
      (!needle || r.name.toLowerCase().includes(needle) || r.source.includes(needle) || r.targets.some((t) => t.includes(needle)) || whenText(r.conditions).includes(needle)),
  );
  const all = routes ?? [];
  const enabled = all.filter((r) => r.enabled).length;
  const failed = all.reduce((s, r) => s + (stats[r.id]?.failed ?? 0), 0);
  const sources = [...new Set(all.filter((r) => r.enabled).map((r) => r.source))];

  return (
    <div className="page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('routing.title')}</h1>
          <p>{t('routing.subtitle')}</p>
        </div>
        <div className="page-actions">
          <GateButton need="admin" className="btn btn-primary" onClick={() => navigate(source ? `/routing/new?source=${encodeURIComponent(source)}` : '/routing/new')} disabled={unsupported}>
            <Plus size={15} /> {t('routing.newRoute')}
          </GateButton>
        </div>
      </div>

      {unsupported ? (
        <section className="card">
          <EmptyState icon={<RouteIcon size={20} />} title={t('routing.unsupportedTitle')}>
            {t('routing.unsupportedBody')}
          </EmptyState>
        </section>
      ) : error && !routes ? (
        <InlineError>{error}</InlineError>
      ) : !routes ? (
        <section className="card">
          <SkeletonRows rows={4} />
        </section>
      ) : all.length === 0 ? (
        <section className="card">
          <EmptyState
            icon={<RouteIcon size={20} />}
            title={t('routing.emptyTitle')}
            actions={
              <GateButton need="admin" className="btn btn-primary" onClick={() => navigate('/routing/new')}>
                <Plus size={15} /> {t('routing.emptyCreate')}
              </GateButton>
            }
          >
            {t.rich('routing.emptyBody', {
              example: <span className="mono">{ROUTE_EXAMPLE}</span>,
              group: <span className="mono">{ROUTER_GROUP}</span>,
            })}
          </EmptyState>
        </section>
      ) : (
        <>
          <div className="routing-summary">
            <span>
              {t.rich('routing.summaryRoutes', { count: all.length, n: <b className="num">{all.length}</b> })} · {t.rich('routing.summaryEnabled', { count: <b className="num">{enabled}</b> })}
            </span>
            {failed > 0 && (
              <span className="text-critical">
                {t.rich('routing.failedForwards', { count: <b className="num">{fmtCount(failed)}</b>, group: <span className="mono">{ROUTER_GROUP}</span> })} ·{' '}
                <Link to={`/dlq`} className="btn-link">
                  {t('routing.deadLetters')}
                </Link>
              </span>
            )}
            <span className="faint routing-note">
              <Info size={13} />{' '}
              {t.rich('routing.runsIn', {
                group: <span className="mono">{ROUTER_GROUP}</span>,
                sources: sources.length
                  ? sources.map((s, i) => (
                      <span key={s}>
                        {i > 0 && ', '}
                        <Link to={`${channelPath(s, 'consumers')}#${encodeURIComponent(ROUTER_GROUP)}`} className="btn-link">
                          {s}
                        </Link>
                      </span>
                    ))
                  : t('routing.eachSource'),
              })}
            </span>
          </div>

          <section className="card">
            <div className="card-head routes-toolbar">
              <div className="input-group routes-search">
                <Search size={14} />
                <input className="input" placeholder={t('routing.filterPlaceholder')} value={q} onChange={(e) => setParam('q', e.target.value || null)} aria-label={t('routing.filterAria')} />
              </div>
              {source && (
                <span className="chip">
                  {t('routing.channelChip', { name: source })}
                  <button type="button" aria-label={t('routing.clearChannelFilter')} onClick={() => setParam('source', null)}>
                    <X size={12} />
                  </button>
                </span>
              )}
              <span className="spacer" />
              <span className="card-sub">{t('routing.countersNote')}</span>
            </div>
            {list.length ? (
              <RoutesTable routes={list} highlight={source ?? undefined} />
            ) : (
              <EmptyState compact icon={<Search size={18} />} title={t('routing.noMatchTitle')}>
                {t('routing.noMatchBody', { count: all.length })}
              </EmptyState>
            )}
          </section>
        </>
      )}
    </div>
  );
}

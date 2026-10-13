import { Link, useNavigate } from 'react-router';
import { ArrowDownToLine, ArrowUpFromLine, Plus, Route as RouteIcon } from 'lucide-react';
import { ROUTER_GROUP } from '../../api/types';
import { useRoutes, useRoutesPolling } from '../../store/routes';
import { channelPath } from '../../lib/labels';
import { EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import { RoutesTable } from '../../components/routing/RoutesTable';
import { useT } from '../../i18n';

/** Example route in the routing DSL (code — not translated). */
const EXAMPLE_ROUTE = 'WHEN type = order.created AND total >= 500 THEN → orders.priority';

export function RoutingTab({ name }: { name: string }) {
  const t = useT();
  useRoutesPolling(5000);
  const routes = useRoutes((s) => s.routes);
  const error = useRoutes((s) => s.error);
  const unsupported = useRoutes((s) => s.unsupported);
  const navigate = useNavigate();
  if (unsupported) {
    return (
      <section className="card">
        <EmptyState icon={<RouteIcon size={20} />} title={t('channels.routing.unavailable')}>
          {t('channels.routing.unavailableBody')}
        </EmptyState>
      </section>
    );
  }
  if (!routes) return error ? <InlineError>{error}</InlineError> : <section className="card"><SkeletonRows rows={3} /></section>;
  const outgoing = routes.filter((r) => r.source === name);
  const incoming = routes.filter((r) => r.targets.includes(name));
  const newRoute = () => navigate(`/routing/new?source=${encodeURIComponent(name)}`);
  return (
    <div className="routing-tab" style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <section className="card">
        <div className="card-head">
          <ArrowUpFromLine size={15} className="faint" />
          <h2>{t('channels.routing.from', { name })}</h2>
          <span className="card-sub">
            {outgoing.length
              ? <>
                  {t('channels.routing.evaluatedBy')}{' '}
                  <Link to={`${channelPath(name, 'consumers')}#${encodeURIComponent(ROUTER_GROUP)}`} className="btn-link mono">
                    {ROUTER_GROUP}
                  </Link>
                </>
              : t('channels.routing.noneReading')}
          </span>
          <span className="spacer" />
          <GateButton need="admin" className="btn btn-sm btn-primary" onClick={newRoute}>
            <Plus size={14} /> {t('channels.routing.newFrom', { name })}
          </GateButton>
        </div>
        {outgoing.length ? (
          <RoutesTable routes={outgoing} highlight={name} />
        ) : (
          <EmptyState compact icon={<RouteIcon size={18} />} title={t('channels.routing.noneFrom')}>
            {t.rich('channels.routing.noneFromBody', { example: <span className="mono">{EXAMPLE_ROUTE}</span> })}
          </EmptyState>
        )}
      </section>
      <section className="card">
        <div className="card-head">
          <ArrowDownToLine size={15} className="faint" />
          <h2>{t('channels.routing.hereFrom')}</h2>
          <span className="card-sub">{incoming.length ? t('channels.routing.incomingHeader') : t('channels.routing.nothingIncoming')}</span>
        </div>
        {incoming.length ? (
          <RoutesTable routes={incoming} highlight={name} />
        ) : (
          <EmptyState compact icon={<ArrowDownToLine size={18} />} title={t('channels.routing.noIncoming')}>
            {t.rich('channels.routing.noIncomingBody', { name: <span className="mono">{name}</span> })}
          </EmptyState>
        )}
      </section>
    </div>
  );
}

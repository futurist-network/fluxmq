import type { NodeStatus, RebalanceNodeLoad } from '../../api/types';
import { NODE_STATUS_HINT, NODE_STATUS_LABEL, statusTone } from '../../lib/cluster';
import { fmtLoadValue, joinLoad, LOAD_METRIC_LABEL, LOAD_METRICS, loadScale } from '../../lib/rebalance';
import { localized } from '../../lib/labels';
import { useT } from '../../i18n';

const METRIC_HINT = localized(LOAD_METRICS, 'rebalance.metricHint.');

function Bar({ value, max, kind }: { value: number; max: number; kind: 'cur' | 'next' }) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div className={`lb-bar ${kind}`} aria-hidden>
      <span style={{ width: `${value > 0 ? Math.max(2, pct) : 0}%` }} />
    </div>
  );
}

/**
 * Per-node load as bars, one column per metric. With `projected`, each cell also shows the load
 * expected after the plan (a second, primary-coloured bar and "→ n"), on a shared scale.
 */
export function LoadBars({ current, projected, slots, label, compact }: { current: RebalanceNodeLoad[]; projected?: RebalanceNodeLoad[] | null; slots: Record<string, number>; label?: string; compact?: boolean }) {
  const t = useT();
  const rows = joinLoad(current, projected ?? []);
  const scale = loadScale(current, projected ?? []);
  const showNext = !!projected && projected.length > 0;
  return (
    <div className={`lb ${showNext ? 'with-next' : ''} ${compact ? 'compact' : ''}`} role="table" aria-label={label ?? t('rebalance.loadPerNode')}>
      <div className="lb-row lb-headrow" role="row">
        <span role="columnheader" className="lb-node-h">
          {t('common.node')}
        </span>
        {LOAD_METRICS.map((m) => (
          <span key={m} role="columnheader" className="lb-h" title={METRIC_HINT[m]}>
            {LOAD_METRIC_LABEL[m]}
          </span>
        ))}
      </div>
      {rows.map(({ node, cur, next }) => {
        const status: NodeStatus = cur?.status ?? next?.status ?? 'up';
        return (
        <div key={node} className={`lb-row ${status !== 'up' ? `st-${status}` : ''}`} role="row">
          <span role="rowheader" className="lb-node mono">
            <span className={`lg-sw slot-${slots[node] ?? 0}`} aria-hidden />
            <span className="lb-node-id">
              {node}
              {status !== 'up' && (
                <span className={`lb-status ${status === 'joining' ? '' : statusTone(status)}`} title={NODE_STATUS_HINT[status]}>
                  {NODE_STATUS_LABEL[status]}
                </span>
              )}
            </span>
          </span>
          {LOAD_METRICS.map((m) => {
            const a = cur?.[m] ?? 0;
            const b = next?.[m] ?? a;
            const changed = showNext && b !== a;
            return (
              <div key={m} role="cell" className="lb-cell" aria-label={`${node} ${LOAD_METRIC_LABEL[m]}: ${fmtLoadValue(m, a)}${changed ? `, ${t('rebalance.afterPlanValue', { value: fmtLoadValue(m, b) })}` : ''}`}>
                <div className="lb-vals">
                  <span className="num lb-val">{fmtLoadValue(m, a)}</span>
                  {changed && (
                    <span className={`num lb-next ${b > a ? 'up' : 'down'}`}>
                      → {fmtLoadValue(m, b)}
                    </span>
                  )}
                </div>
                <Bar value={a} max={scale[m]} kind="cur" />
                {showNext && <Bar value={b} max={scale[m]} kind="next" />}
              </div>
            );
          })}
        </div>
        );
      })}
      {showNext && (
        <div className="lb-legend faint">
          <span>
            <i className="lb-key cur" /> {t('rebalance.legendNow')}
          </span>
          <span>
            <i className="lb-key next" /> {t('rebalance.afterPlan')}
          </span>
        </div>
      )}
    </div>
  );
}

import { useState } from 'react';
import { Link } from 'react-router';
import { ArrowRight, Crown, Hand, Loader2, Scale, Wrench } from 'lucide-react';
import type { MoveReason, MoveState, PartitionMove } from '../../api/types';
import { fmtBackoff, fmtClock, fmtCount, fmtDateTime, fmtInt, fmtRelative } from '../../lib/format';
import { channelPath } from '../../lib/labels';
import { useT } from '../../i18n';
import { catchUpProgress, moveDiff, moveElapsedMs, MOVE_REASON_HINT, MOVE_REASON_LABEL, MOVE_STATE_LABEL, peakLagFor, sortMoves } from '../../lib/rebalance';

const STATE_BADGE: Record<MoveState, string> = { pending: '', catching_up: 'badge-primary', finalizing: 'badge-primary', done: 'badge-success', rolled_back: 'badge-warning', cancelled: 'rb-cancelled' };
const REASON_ICON: Record<MoveReason, typeof Scale> = { repair: Wrench, balance: Scale, manual: Hand };

export function ReasonChip({ reason }: { reason: MoveReason }) {
  const Icon = REASON_ICON[reason];
  return (
    <span className={`badge rb-reason reason-${reason}`} title={MOVE_REASON_HINT[reason]}>
      <Icon size={11} aria-hidden />
      {MOVE_REASON_LABEL[reason]}
    </span>
  );
}

export function MoveStateChip({ state }: { state: MoveState }) {
  return (
    <span className={`badge rb-state ${STATE_BADGE[state]}`}>
      {(state === 'catching_up' || state === 'finalizing') && <Loader2 size={11} className="spin" aria-hidden />}
      {MOVE_STATE_LABEL[state]}
    </span>
  );
}

/** from → to replica lists; removed replicas struck through, added ones highlighted, preferred leader crowned. */
export function ReplicaDiff({ m }: { m: PartitionMove }) {
  const t = useT();
  const d = moveDiff(m);
  return (
    <span
      className="rb-diff"
      aria-label={`${t('rebalance.diff.aria', { from: m.from_replicas.join(', '), to: m.to_replicas.join(', ') })}${d.added.length ? `; ${t('rebalance.diff.adds', { list: d.added.join(', ') })}` : ''}${d.removed.length ? `; ${t('rebalance.diff.removes', { list: d.removed.join(', ') })}` : ''}`}
    >
      <span className="rb-reps">
        {m.from_replicas.map((r, i) => (
          <span key={r} className={`rb-rep ${d.removed.includes(r) ? 'removed' : ''}`} title={d.removed.includes(r) ? t('rebalance.diff.removedHint', { node: r }) : i === 0 ? t('rebalance.diff.preferredHint', { node: r }) : r}>
            {i === 0 && <Crown size={9} aria-hidden />}
            {r}
          </span>
        ))}
      </span>
      <ArrowRight size={12} className="rb-arrow" aria-hidden />
      <span className="rb-reps">
        {m.to_replicas.map((r, i) => (
          <span key={r} className={`rb-rep ${d.added.includes(r) ? 'added' : ''}`} title={d.added.includes(r) ? t('rebalance.diff.addedHint', { node: r }) : i === 0 ? t('rebalance.diff.preferredHint', { node: r }) : r}>
            {i === 0 && <Crown size={9} aria-hidden />}
            {r}
          </span>
        ))}
      </span>
    </span>
  );
}

function CatchUp({ m, peak }: { m: PartitionMove; peak: number | null }) {
  const t = useT();
  const p = catchUpProgress(m, peak);
  if (m.state === 'rolled_back') return <span className="faint">{t('rebalance.catch.restored')}</span>;
  if (m.state === 'cancelled') return <span className="faint">{t('rebalance.catch.neverStarted')}</span>;
  if (m.state === 'done') return <span className="faint">{t('rebalance.catch.inSync')}</span>;
  if (m.state === 'pending') return <span className="faint">{t('rebalance.catch.waiting')}</span>;
  if (m.state === 'finalizing') return <span className="faint">{t('rebalance.catch.committing')}</span>;
  return (
    <div className="rb-catch">
      <div className="rb-catch-bar" role="progressbar" aria-label={t('rebalance.catch.aria', { channel: m.channel, partition: m.partition })} aria-valuemin={0} aria-valuemax={100} aria-valuenow={p == null ? undefined : Math.round(p * 100)}>
        <span className={p == null ? 'indeterminate' : ''} style={p == null ? undefined : { width: `${Math.max(3, p * 100)}%` }} />
      </div>
      <span className="rb-catch-meta num" title={m.lag != null ? t('rebalance.catch.behindHint', { count: m.lag, n: fmtInt(m.lag) }) : undefined}>
        {m.lag != null ? t('rebalance.catch.behind', { n: fmtCount(m.lag) }) : t('rebalance.catch.starting')}
      </span>
    </div>
  );
}

function Timing({ m, now }: { m: PartitionMove; now: number }) {
  const t = useT();
  const el = moveElapsedMs(m, now);
  if (m.started_at_ms == null || el == null) return <span className="faint">—</span>;
  const dur = fmtBackoff(Math.max(1000, Math.round(el / 1000) * 1000));
  return (
    <span className="rb-time" title={`${t('nodes.decom.startedAt', { when: fmtDateTime(m.started_at_ms) })}${m.finished_at_ms ? ` · ${t('nodes.decom.finishedAt', { when: fmtDateTime(m.finished_at_ms) })}` : ''}`}>
      {m.finished_at_ms ? (
        <>
          <span>{fmtRelative(m.finished_at_ms, now)}</span>
          <small className="faint num">{t('rebalance.time.took', { dur })}</small>
        </>
      ) : (
        <>
          <span className="num">{dur}</span>
          <small className="faint num">{t('rebalance.time.since', { time: fmtClock(m.started_at_ms, true) })}</small>
        </>
      )}
    </span>
  );
}

/**
 * The plan as a table. `preview` drops the progress / timing columns (dry run). With `limit`,
 * only the first rows (in flight, pending, rolled back first) show until expanded.
 */
export function PlanTable({ moves, peaks, now, preview, limit }: { moves: PartitionMove[]; peaks?: Record<string, number>; now: number; preview?: boolean; limit?: number }) {
  const [all, setAll] = useState(false);
  const t = useT();
  const sorted = preview ? moves : sortMoves(moves);
  const rows = limit && !all ? sorted.slice(0, limit) : sorted;
  const hidden = sorted.length - rows.length;
  const hiddenDone = sorted.slice(rows.length).filter((m) => m.state === 'done').length;
  return (
    <div className="table-wrap">
      <table className={`table rb-table ${preview ? 'preview' : ''}`}>
        <thead>
          <tr>
            <th>{t('common.partition')}</th>
            <th>{t('common.reason')}</th>
            <th>{t('common.replicas')}</th>
            {!preview && (
              <>
                <th>{t('common.state')}</th>
                <th>{t('rebalance.col.catchUp')}</th>
                <th>{t('common.time')}</th>
              </>
            )}
          </tr>
        </thead>
        <tbody>
          {rows.map((m) => (
            <tr key={`${m.channel}/${m.partition}`} className={preview ? '' : m.state === 'rolled_back' ? 'row-warning' : m.state === 'done' || m.state === 'cancelled' ? 'row-muted' : ''}>
              <td className="rb-part">
                <Link to={channelPath(m.channel, 'storage')} className="mono rb-ch truncate" title={m.channel}>
                  {m.channel}
                </Link>
                <span className="faint mono rb-p">p{m.partition}</span>
              </td>
              <td>
                <ReasonChip reason={m.reason} />
              </td>
              <td>
                <ReplicaDiff m={m} />
              </td>
              {!preview && (
                <>
                  <td className="rb-state-cell">
                    <MoveStateChip state={m.state} />
                    {m.error && (
                      <div className={`rb-err ${m.state === 'rolled_back' ? 'text-warning' : 'text-critical'}`} title={m.error}>
                        {m.error}
                      </div>
                    )}
                  </td>
                  <td className="rb-catch-cell">
                    <CatchUp m={m} peak={peaks ? peakLagFor(peaks, m) : null} />
                  </td>
                  <td className="nowrap">
                    <Timing m={m} now={now} />
                  </td>
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
      {(hidden > 0 || (all && limit && sorted.length > limit)) && (
        <div className="rb-more">
          <button type="button" className="btn-link" onClick={() => setAll(!all)}>
            {all ? t('rebalance.showFewer') : t('rebalance.showMore', { count: hidden })}
          </button>
          {!all && hiddenDone > 0 && <span className="faint"> · {hiddenDone === hidden ? t('rebalance.allDone') : t('rebalance.nDone', { n: hiddenDone })}</span>}
        </div>
      )}
    </div>
  );
}

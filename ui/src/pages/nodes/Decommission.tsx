import { useState } from 'react';
import { AlertTriangle, Ban, CircleCheck, Crown, Loader2, PackageCheck, RotateCcw, ServerOff, X, XCircle } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import type { ClusterStatus, DecommissionStatus } from '../../api/types';
import { fmtDateTime, fmtInt, fmtRelative } from '../../lib/format';
import { t as tr, useT } from '../../i18n';
import { useAsync, useNow } from '../../lib/hooks';
import { canCancel, conflictView, decommissionChip, decommissionPreview, decommissionProgress, previewBlocked, type ConflictView } from '../../lib/decommission';
import { refreshCluster } from '../../store/cluster';
import { refreshRebalance, setRebalanceStatus, useRebalance } from '../../store/rebalance';
import { REBALANCE_STATE_LABEL } from '../../lib/rebalance';
import { dismissDecommission, refreshDecommissions, setDecommissionStatus } from '../../store/decommission';
import { toast } from '../../store/ui';
import { ConfirmDialog } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import { LoadBars } from '../rebalance/LoadBars';
import { PlanTable } from '../rebalance/PlanTable';
import './decommission.css';

/** Scroll the Rebalancing panel into view (the decommission's moves run there). */
function PlanLink() {
  const t = useT();
  return (
    <a
      href="#rebalancing"
      className="btn-link"
      onClick={(e) => {
        e.preventDefault();
        document.getElementById('rebalancing')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }}
    >
      {t('nodes.decom.viewPlan')}
    </a>
  );
}

/** Progress block inside a node card: chip, bar, detail and actions for the node's decommission. */
export function DecommissionProgress({ id, s, error, onRetry }: { id: string; s: DecommissionStatus | undefined; error?: string | null; onRetry?: () => void }) {
  const now = useNow();
  const t = useT();
  const [cancelOpen, setCancelOpen] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  if (!s) {
    return (
      <div className="decom tone-primary" aria-label={t('nodes.decom.aria', { id })}>
        <div className="decom-top">
          <span className="decom-chip primary">
            <Loader2 size={12} className="spin" aria-hidden /> {t('nodes.status.decommissioning')}
          </span>
        </div>
        <div className="decom-detail faint">{error ? t('nodes.decom.progressUnavailable', { error }) : t('nodes.decom.loadingProgress')}</div>
      </div>
    );
  }
  const chip = decommissionChip(s);
  const pct = decommissionProgress(s);
  const doCancel = async () => {
    setCancelling(true);
    try {
      setRebalanceStatus(await api.cancelRebalance());
      toast('success', tr('nodes.decom.cancellingToast', { id }));
      setCancelOpen(false);
      void refreshDecommissions();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setCancelling(false);
    }
  };
  return (
    <div className={`decom tone-${chip.tone}`} aria-label={t('nodes.decom.aria', { id })}>
      <div className="decom-top">
        <span className={`decom-chip ${chip.tone}`}>
          {chip.active ? <Loader2 size={12} className="spin" aria-hidden /> : chip.tone === 'success' ? <CircleCheck size={12} aria-hidden /> : <XCircle size={12} aria-hidden />}
          {chip.label}
        </span>
        {s.state === 'moving_replicas' && s.initial_replicas > 0 && (
          <span className="num decom-count">
            {t('nodes.decom.moved', { done: fmtInt(s.initial_replicas - s.replicas_remaining), total: fmtInt(s.initial_replicas) })}
          </span>
        )}
        <span className="spacer" />
        <span className="faint decom-when" title={`${t('nodes.decom.startedAt', { when: fmtDateTime(s.started_at_ms) })}${s.finished_at_ms ? ` · ${t('nodes.decom.finishedAt', { when: fmtDateTime(s.finished_at_ms) })}` : ''}`}>
          {s.finished_at_ms
            ? t(s.state === 'failed' ? 'nodes.decom.endedRel' : 'nodes.decom.finishedRel', { when: fmtRelative(s.finished_at_ms, now) })
            : t('nodes.decom.startedRel', { when: fmtRelative(s.started_at_ms, now) })}
        </span>
      </div>
      {chip.active && (
        <div className={`meter decom-meter ${pct == null ? 'indet' : ''}`} role="progressbar" aria-label={t('nodes.decom.replicasMoved')} aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct == null ? undefined : Math.round(pct * 100)}>
          <span style={{ width: `${pct == null ? 30 : Math.max(3, pct * 100)}%` }} />
        </div>
      )}
      <div className={`decom-detail ${chip.tone === 'critical' ? 'text-critical' : ''}`}>
        {chip.detail}
        {s.state === 'moving_replicas' && (
          <>
            {' · '}
            <PlanLink />
          </>
        )}
      </div>
      {s.state === 'removed' && <div className="decom-detail faint">{t('nodes.decom.restartNote')}</div>}
      {error && <div className="decom-detail faint">{t('nodes.decom.refreshFailed', { error })}</div>}
      {canCancel(s) && (
        <div className="decom-actions">
          <GateButton need="admin" className="btn btn-sm" onClick={() => setCancelOpen(true)}>
            <Ban size={13} /> {t('nodes.decom.cancel')}
          </GateButton>
        </div>
      )}
      {s.state === 'failed' && (
        <div className="decom-actions">
          <button className="btn btn-sm btn-ghost" onClick={() => dismissDecommission(s)}>
            {t('nodes.decom.dismiss')}
          </button>
          {onRetry && (
            <GateButton need="admin" className="btn btn-sm" onClick={onRetry}>
              <RotateCcw size={13} /> {chip.cancelled ? t('nodes.decom.again') : t('nodes.decom.retry')}
            </GateButton>
          )}
        </div>
      )}
      <ConfirmDialog
        open={cancelOpen}
        title={t('nodes.decom.cancelTitle', { id })}
        description={t('nodes.decom.cancelBody', { id })}
        confirmLabel={t('nodes.decom.cancel')}
        cancelLabel={t('nodes.decom.keepGoing')}
        danger
        busy={cancelling}
        onConfirm={doCancel}
        onClose={() => setCancelOpen(false)}
      >
        <p className="faint" style={{ fontSize: 12.5, margin: 0 }}>
          {s.initial_replicas
            ? t('nodes.decom.stillOnOf', { count: s.replicas_remaining, id, total: fmtInt(s.initial_replicas) })
            : t('nodes.decom.stillOn', { count: s.replicas_remaining, id })}
        </p>
      </ConfirmDialog>
    </div>
  );
}

/** A node that finished decommissioning and left the registry: kept until dismissed. */
export function RemovedNodeCard({ s }: { s: DecommissionStatus }) {
  const now = useNow();
  const t = useT();
  const id = s.node;
  const failed = s.state === 'failed';
  return (
    <article className={`card node-card removed-card ${failed ? 'tone-critical' : ''}`} aria-label={t('nodes.removed.aria', { id })}>
      <div className="node-head">
        <span className="node-icon" aria-hidden>
          {failed ? <ServerOff size={16} /> : <PackageCheck size={16} />}
        </span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div className="node-title">
            <h3 className="mono">{id}</h3>
            <span className="node-role faint">{t('nodes.removed.notMember')}</span>
          </div>
          <div className="faint node-addr">{s.finished_at_ms ? t('nodes.removed.leftRel', { when: fmtRelative(s.finished_at_ms, now) }) : t('nodes.removed.left')}</div>
        </div>
        <button className="btn btn-ghost btn-icon btn-sm" onClick={() => dismissDecommission(s)} aria-label={t('nodes.removed.dismissAria', { id })} title={t('nodes.decom.dismiss')}>
          <X size={14} />
        </button>
      </div>
      <DecommissionProgress id={id} s={s} />
      {!failed && (
        <div className="removed-steps">
          <div className="faint">{t('nodes.removed.next')}</div>
          <ol>
            <li>{t('nodes.removed.step1')}</li>
            <li>{t('nodes.removed.step2')}</li>
          </ol>
        </div>
      )}
    </article>
  );
}

/** Confirm dialog: what will happen, what we can predict from the current status, and 409 explanations. */
export function DecommissionDialog({ id, status, slots, onClose }: { id: string; status: ClusterStatus; slots: Record<string, number>; onClose: () => void }) {
  const rebalance = useRebalance((s) => s.status);
  const p = decommissionPreview(status, id, rebalance);
  const blocked = previewBlocked(p);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ConflictView | null>(null);
  const now = useNow();
  const t = useT();
  // Optional server feature (cluster.rs): `?dry_run=true` → the planned moves. 400 / 404 = not
  // supported: fall back to the projection from cluster status.
  const dry = useAsync(
    (signal) =>
      api.decommissionDryRun(id, signal).catch((e: unknown) => {
        if (e instanceof ApiError && (e.status === 400 || e.status === 404)) return null;
        throw e;
      }),
    [id],
  );
  const plan = dry.data && dry.data.moves.length > 0 ? dry.data : null;
  const dryConflict = dry.error instanceof ApiError && dry.error.isConflict ? dry.error.message : null;

  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      const r = await api.decommissionNode(id);
      setDecommissionStatus(r);
      toast('success', r.replicas_remaining ? tr('nodes.decom.startedToastMoving', { id, count: r.replicas_remaining }) : tr('nodes.decom.startedToast', { id }));
      void refreshCluster();
      void refreshRebalance();
      onClose();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setError(conflictView(e instanceof ApiError ? e : { message: errorMessage(e) }, 'decommission', p));
    } finally {
      setBusy(false);
    }
  };

  return (
    <ConfirmDialog
      open
      title={t('nodes.decom.dialogTitle', { id })}
      description={t('nodes.decom.dialogBody', { id })}
      confirmLabel={error ? t('nodes.tryAgain') : t('nodes.decom.confirm', { id })}
      danger
      busy={busy}
      onConfirm={submit}
      onClose={onClose}
      wide={plan != null}
    >
      <div className="decom-dialog">
        <div className="rb-sub-h">{t('nodes.decom.projected')}</div>
        <dl className="decom-facts">
          <div>
            <dt>{t('nodes.decom.replicasToMove')}</dt>
            <dd className="num">{fmtInt(p.replicas)}</dd>
          </div>
          <div>
            <dt>{t('nodes.decom.leadersDrained')}</dt>
            <dd className="num">{fmtInt(p.leaders)}</dd>
          </div>
          <div>
            <dt>{t('nodes.decom.channelsAffected')}</dt>
            <dd className="num">{fmtInt(p.channels)}</dd>
          </div>
          <div>
            <dt>{t('nodes.decom.canTake')}</dt>
            <dd className="decom-targets">
              {p.targets.length ? (
                p.targets.map((nid) => (
                  <span key={nid} className="lg-item mono">
                    <span className={`lg-sw slot-${slots[nid] ?? 0}`} aria-hidden />
                    {nid}
                  </span>
                ))
              ) : (
                <span className="text-critical">{t('nodes.decom.noTarget')}</span>
              )}
            </dd>
          </div>
        </dl>
        {plan ? (
          <div className="decom-plan">
            <div className="rb-sub-h">
              {t('nodes.decom.plannedMoves')} <span className="faint rb-sub-note">{t('nodes.decom.dryRunMoves', { count: plan.moves.length })}</span>
            </div>
            <LoadBars current={plan.load} projected={plan.projected_load} slots={slots} compact label={t('nodes.decom.loadLabel')} />
            <PlanTable moves={plan.moves} now={now} preview limit={6} />
            <p className="faint decom-plan-note">{t('nodes.decom.replanNote')}</p>
          </div>
        ) : (
          <p className="faint decom-plan-note">
            {dry.loading ? `${t('nodes.decom.asking')} ` : ''}
            {t.rich('nodes.decom.planNote', {
              reason: <b>{t('rebalance.reason.repair').toLowerCase()}</b>,
              max: rebalance ? String(rebalance.config.max_concurrent_moves) : t('nodes.decom.configuredNumber'),
            })}
            {p.replicas === 0 ? ` ${t('nodes.decom.noReplicas', { id })}` : ''}
          </p>
        )}
        {dryConflict && !error && (
          <div className="decom-warn" role="note">
            <AlertTriangle size={16} aria-hidden />
            <div>
              <b>{t('nodes.decom.dryRefused')}</b>
              <p className="mono decom-error-msg" style={{ margin: '4px 0 0' }}>
                {dryConflict}
              </p>
            </div>
          </div>
        )}
        {p.isController && (
          <div className="decom-note">
            <Crown size={14} aria-hidden />
            <span>{t('nodes.decom.isController', { id })}</span>
          </div>
        )}
        {blocked && !error && !dryConflict && (
          <div className="decom-warn" role="note">
            <AlertTriangle size={16} aria-hidden />
            <div>
              <b>{t('nodes.decom.likelyRefused')}</b>
              <ul>
                {p.planBusy && <li>{t('nodes.decom.planBusy', { state: (rebalance ? REBALANCE_STATE_LABEL[rebalance.state] : REBALANCE_STATE_LABEL.running).toLowerCase() })}</li>}
                {p.rfBlocked.slice(0, 4).map((b) => (
                  <li key={b.channel}>
                    {t.rich('nodes.decom.rfNeeds', { channel: <span className="mono">{b.channel}</span>, rf: b.rf, count: p.targets.length })}
                  </li>
                ))}
                {p.rfBlocked.length > 4 && <li>{t('nodes.decom.moreChannels', { count: p.rfBlocked.length - 4 })}</li>}
                {p.quorumRisk && <li>{p.quorumRisk}</li>}
              </ul>
            </div>
          </div>
        )}
        {error && (
          <div className="decom-error" role="alert">
            <XCircle size={16} aria-hidden />
            <div>
              <b>{error.title}</b>
              <p className="mono decom-error-msg">{error.message}</p>
              {error.hint && <p>{error.hint}</p>}
            </div>
          </div>
        )}
      </div>
    </ConfirmDialog>
  );
}

export function ForgetDialog({ id, onClose }: { id: string; onClose: () => void }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ConflictView | null>(null);
  const t = useT();
  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.forgetNode(id);
      toast('success', tr('nodes.forget.done', { id }));
      void refreshCluster();
      void refreshRebalance();
      onClose();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) setError(conflictView(e instanceof ApiError ? e : { message: errorMessage(e) }, 'forget'));
    } finally {
      setBusy(false);
    }
  };
  return (
    <ConfirmDialog
      open
      title={t('nodes.forget.title', { id })}
      description={t('nodes.forget.body', { id })}
      confirmLabel={error ? t('nodes.tryAgain') : t('nodes.forget.confirm', { id })}
      danger
      busy={busy}
      onConfirm={submit}
      onClose={onClose}
    >
      <div className="decom-dialog">
        <div className="decom-note">
          <CircleCheck size={14} aria-hidden />
          <span>{t('nodes.forget.safe', { id })}</span>
        </div>
        {error && (
          <div className="decom-error" role="alert">
            <XCircle size={16} aria-hidden />
            <div>
              <b>{error.title}</b>
              <p className="mono decom-error-msg">{error.message}</p>
              {error.hint && <p>{error.hint}</p>}
            </div>
          </div>
        )}
      </div>
    </ConfirmDialog>
  );
}

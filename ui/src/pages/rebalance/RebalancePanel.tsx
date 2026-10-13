import { useEffect, useRef, useState } from 'react';
import { useLocation } from 'react-router';
import { Ban, CircleCheck, Loader2, Pause, Play, Scale, Settings2, Shuffle } from 'lucide-react';
import { api, ApiError, errorMessage, isApiErrorCode } from '../../api/client';
import { ERROR_UNSUPPORTED_SINGLE_NODE, TOPICS, type EventsFeed, type RebalanceStatus } from '../../api/types';
import { fmtDateTime, fmtInt, fmtRelative } from '../../lib/format';
import { t as tr, useT, type TKey } from '../../i18n';
import { useNow } from '../../lib/hooks';
import {
  fmtSecsHuman,
  fmtThrottle,
  imbalanceLevel,
  latestRebalanceEventId,
  planInProgress,
  planSummary,
  REBALANCE_MODE_HINT,
  REBALANCE_MODE_LABEL,
  REBALANCE_STATE_LABEL,
} from '../../lib/rebalance';
import { useTopicSelect } from '../../telemetry';
import { refreshRebalance, setRebalanceStatus, useRebalance, useRebalancePolling } from '../../store/rebalance';
import { toast } from '../../store/ui';
import { ConfirmDialog, EmptyState, InlineError, SkeletonRows } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import { LoadBars } from './LoadBars';
import { PlanTable } from './PlanTable';
import { RebalanceConfigDialog, RebalanceNowDialog } from './RebalanceDialogs';
import './rebalance.css';

/** Path shown verbatim (not translated). */
const CLUSTER_DOC = 'docs/CLUSTER.md';

type Action = 'pause' | 'resume' | 'cancel';

/** Dictionary keys per action (translated at render). */
const ACTION_COPY: Record<Action, { title: TKey; description: TKey; confirm: TKey; done: TKey; danger?: boolean; dismiss?: TKey }> = {
  pause: { title: 'rebalance.action.pauseTitle', description: 'rebalance.action.pauseBody', confirm: 'rebalance.action.pause', done: 'rebalance.action.pauseDone' },
  resume: { title: 'rebalance.action.resumeTitle', description: 'rebalance.action.resumeBody', confirm: 'rebalance.action.resume', done: 'rebalance.action.resumeDone' },
  cancel: {
    title: 'rebalance.action.cancelTitle',
    description: 'rebalance.action.cancelBody',
    confirm: 'rebalance.action.cancelPlan',
    dismiss: 'rebalance.action.keepRunning',
    done: 'rebalance.action.cancelDone',
    danger: true,
  },
};

const STATE_BADGE: Record<RebalanceStatus['state'], string> = { idle: '', running: 'badge-primary', paused: 'badge-warning', cancelling: 'badge-warning' };

function SingleNodeState() {
  const t = useT();
  return (
    <section className="card rb-card" id="rebalancing" aria-label={t('rebalance.title')}>
      <div className="card-head">
        <h2>{t('rebalance.title')}</h2>
      </div>
      <EmptyState icon={<Scale size={20} />} title={t('rebalance.single.title')}>
        {t.rich('rebalance.single.body', { doc: <span className="mono">{CLUSTER_DOC}</span> })}
      </EmptyState>
    </section>
  );
}

function Facts({ s, now }: { s: RebalanceStatus; now: number }) {
  const level = imbalanceLevel(s.imbalance, s.config.imbalance_threshold);
  const sum = planSummary(s.moves);
  const active = planInProgress(s);
  const autoBalances = s.config.mode === 'full';
  const t = useT();
  const parts = [
    sum.inFlight ? t('rebalance.sum.inFlight', { n: sum.inFlight }) : null,
    sum.pending ? t('rebalance.sum.pending', { n: sum.pending }) : null,
    sum.rolledBack ? t('rebalance.sum.rolledBack', { n: sum.rolledBack }) : null,
    sum.cancelled ? t('rebalance.sum.cancelled', { n: sum.cancelled }) : null,
  ].filter(Boolean);
  return (
    <div className="rb-facts">
      <div className="cs-item">
        <span className="hm-label">{t('rebalance.facts.imbalance')}</span>
        <span className="rb-fact-v">
          <span className={`cs-value num ${level === 'over' ? 'text-warning' : ''}`}>{fmtInt(s.imbalance)}</span>
          <span className={`badge rb-imb ${level === 'over' ? 'badge-warning' : level === 'balanced' ? 'badge-success' : ''}`} title={t('rebalance.facts.imbalanceHint')}>
            {level === 'over'
              ? t('rebalance.facts.over', { n: s.config.imbalance_threshold })
              : level === 'balanced'
                ? t('rebalance.facts.balanced')
                : t('rebalance.facts.within', { n: s.config.imbalance_threshold })}
          </span>
        </span>
        {(() => {
          const sub = level === 'over' && !autoBalances && !active ? t('rebalance.facts.autoOff', { mode: REBALANCE_MODE_LABEL[s.config.mode].toLowerCase() }) : t('rebalance.facts.maxMin');
          return (
            <span className="hm-sub" title={sub}>
              {sub}
            </span>
          );
        })()}
      </div>
      <div className="cs-item">
        <span className="hm-label" title={t('nodes.summary.under')}>
          {t('nodes.summary.under')}
        </span>
        <span className={`cs-value num ${s.under_replicated ? 'text-warning' : ''}`}>{fmtInt(s.under_replicated)}</span>
        <span className="hm-sub" title={s.under_replicated ? t('rebalance.facts.underYes') : t('rebalance.facts.underNo')}>
          {s.under_replicated ? t('rebalance.facts.underYes') : t('rebalance.facts.underNo')}
        </span>
      </div>
      <div className="cs-item">
        <span className="hm-label">{active ? t('rebalance.plan') : t('rebalance.lastPlan')}</span>
        <span className="cs-value num">{sum.total ? `${sum.done + sum.rolledBack + sum.cancelled}/${sum.total}` : '—'}</span>
        <span className={`hm-sub ${sum.rolledBack ? 'text-warning' : ''}`}>
          {!sum.total ? t('rebalance.facts.noMoves') : parts.join(' · ') || t('rebalance.facts.allDone')}
        </span>
      </div>
      <div className="cs-item rb-trigger">
        <span className="hm-label">{t('rebalance.facts.trigger')}</span>
        <span className="rb-trigger-v truncate" title={s.trigger ?? undefined}>
          {s.trigger ?? <span className="faint">—</span>}
        </span>
        <span className="hm-sub" title={
            s.started_at_ms
              ? `${t('nodes.decom.startedAt', { when: fmtDateTime(s.started_at_ms) })}${s.finished_at_ms ? ` · ${t('nodes.decom.finishedAt', { when: fmtDateTime(s.finished_at_ms) })}` : ''}`
              : undefined
          }
        >
          {s.finished_at_ms && !active
            ? t('nodes.decom.finishedRel', { when: fmtRelative(s.finished_at_ms, now) })
            : s.started_at_ms
              ? t('nodes.decom.startedRel', { when: fmtRelative(s.started_at_ms, now) })
              : t('rebalance.facts.noPlanYet')}
        </span>
      </div>
    </div>
  );
}

function PlanProgress({ s }: { s: RebalanceStatus }) {
  const sum = planSummary(s.moves);
  const t = useT();
  if (!planInProgress(s) || !sum.total) return null;
  const pct = sum.fraction * 100;
  return (
    <div className="rb-progress">
      <div className={`rb-pbar ${s.state}`} role="progressbar" aria-label={t('rebalance.progressAria')} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(pct)}>
        <span className="done" style={{ width: `${(sum.done / sum.total) * 100}%` }} />
        <span className="rolled" style={{ width: `${(sum.rolledBack / sum.total) * 100}%` }} />
        <span className="cancelled" style={{ width: `${(sum.cancelled / sum.total) * 100}%` }} />
        <span className="flight" style={{ width: `${(sum.inFlight / sum.total) * 100}%` }} />
      </div>
      <span className="faint rb-progress-meta num">
        {Math.floor(pct)}% · {s.state === 'paused' ? t('rebalance.progress.paused') : s.state === 'cancelling' ? t('rebalance.progress.cancelling') : t('rebalance.progress.upTo', { n: s.config.max_concurrent_moves })}
      </span>
    </div>
  );
}

export function RebalancePanel({ singleNode, slots, channels }: { singleNode: boolean; slots: Record<string, number>; channels: string[] }) {
  const [refusedSingle, setRefusedSingle] = useState(false);
  const single = singleNode || refusedSingle;
  useRebalancePolling(!single);
  const status = useRebalance((s) => s.status);
  const loaded = useRebalance((s) => s.loaded);
  const unsupported = useRebalance((s) => s.unsupported);
  const error = useRebalance((s) => s.error);
  const peaks = useRebalance((s) => s.peaks);
  const now = useNow();
  const [action, setAction] = useState<Action | null>(null);
  const [busy, setBusy] = useState(false);
  const [showNow, setShowNow] = useState(false);
  const [showConfig, setShowConfig] = useState(false);
  const t = useT();

  // Rebalance events (started / completed / failed / partition moved / repair) → refresh at once.
  const evId = useTopicSelect<EventsFeed, number | null>(single ? null : TOPICS.EVENTS, (d) => latestRebalanceEventId(d?.events));
  useEffect(() => {
    if (evId != null) void refreshRebalance();
  }, [evId]);

  // `/nodes#rebalancing` (event links) scrolls here once the panel has content.
  const location = useLocation();
  const ref = useRef<HTMLElement>(null);
  const scrolled = useRef(false);
  useEffect(() => {
    if (location.hash !== '#rebalancing' || scrolled.current || !ref.current || !status) return;
    scrolled.current = true;
    ref.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, [location.hash, status]);

  if (single) return <SingleNodeState />;
  if (unsupported) return null; // pre-v0.4 server: no panel at all

  const doAction = async () => {
    if (!action) return;
    setBusy(true);
    try {
      const r = action === 'pause' ? await api.pauseRebalance() : action === 'resume' ? await api.resumeRebalance() : await api.cancelRebalance();
      setRebalanceStatus(r);
      toast('success', tr(ACTION_COPY[action].done));
      setAction(null);
    } catch (e) {
      if (isApiErrorCode(e, ERROR_UNSUPPORTED_SINGLE_NODE)) {
        setRefusedSingle(true);
        setAction(null);
      } else if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  const s = status;
  const active = s ? planInProgress(s) : false;
  const sum = s ? planSummary(s.moves) : null;
  return (
    <section ref={ref} className="card rb-card" id="rebalancing" aria-label={t('rebalance.title')}>
      <div className="card-head rb-head">
        <h2>{t('rebalance.title')}</h2>
        {s && (
          <>
            <span className={`badge rb-run ${STATE_BADGE[s.state]}`}>
              {s.state === 'running' || s.state === 'cancelling' ? <Loader2 size={11} className="spin" aria-hidden /> : s.state === 'paused' ? <Pause size={11} aria-hidden /> : null}
              {REBALANCE_STATE_LABEL[s.state]}
            </span>
            <span className="card-sub" title={REBALANCE_MODE_HINT[s.config.mode]}>
              {t('rebalance.auto', { mode: REBALANCE_MODE_LABEL[s.config.mode].toLowerCase() })}
            </span>
          </>
        )}
        <span className="spacer" />
        {s && (
          <div className="rb-actions">
            {s.state === 'running' && (
              <GateButton need="admin" className="btn btn-sm" onClick={() => setAction('pause')}>
                <Pause size={13} /> {t('rebalance.action.pause')}
              </GateButton>
            )}
            {s.state === 'paused' && (
              <GateButton need="admin" className="btn btn-sm" onClick={() => setAction('resume')}>
                <Play size={13} /> {t('rebalance.action.resume')}
              </GateButton>
            )}
            {(s.state === 'running' || s.state === 'paused') && (
              <GateButton need="admin" className="btn btn-sm" onClick={() => setAction('cancel')}>
                <Ban size={13} /> {t('common.cancel')}
              </GateButton>
            )}
            <GateButton need="admin" className="btn btn-sm" onClick={() => setShowConfig(true)}>
              <Settings2 size={13} /> {t('rebalance.configure')}
            </GateButton>
            <GateButton need="admin" className="btn btn-sm btn-primary" onClick={() => setShowNow(true)} disabled={active} title={active ? t('rebalance.nowBusy') : t('rebalance.nowHint')}>
              <Shuffle size={13} /> {t('rebalance.now.button')}
            </GateButton>
          </div>
        )}
      </div>

      {!s ? (
        loaded && error ? (
          <div className="card-body">
            <InlineError onRetry={() => void refreshRebalance()}>{t('rebalance.unavailable', { error })}</InlineError>
          </div>
        ) : (
          <SkeletonRows rows={4} />
        )
      ) : (
        <div className="card-body rb-body">
          {error && <InlineError onRetry={() => void refreshRebalance()}>{t('rebalance.refreshFailed', { error })}</InlineError>}
          <Facts s={s} now={now} />
          <PlanProgress s={s} />

          <div className="rb-section">
            <div className="rb-sub-h">
              {t('rebalance.loadPerNode')}
              {active && s.projected_load.length > 0 && <span className="faint rb-sub-note">{t('rebalance.withProjected')}</span>}
            </div>
            {s.load.length ? <LoadBars current={s.load} projected={active ? s.projected_load : null} slots={slots} /> : <span className="faint">{t('rebalance.noNodes')}</span>}
          </div>

          <div className="rb-section">
            <div className="rb-sub-h">
              {active ? t('rebalance.currentPlan') : s.moves.length ? t('rebalance.lastPlan') : t('rebalance.plan')}
              {sum && sum.total > 0 && (
                <span className="faint rb-sub-note">
                  {t('rebalance.movesCount', { count: sum.total })}
                  {s.trigger ? ` · ${s.trigger}` : ''}
                </span>
              )}
            </div>
            {s.moves.length ? (
              <PlanTable moves={s.moves} peaks={peaks} now={now} limit={10} />
            ) : (
              <EmptyState compact icon={s.imbalance > s.config.imbalance_threshold ? <Scale size={18} /> : <CircleCheck size={18} />} title={s.imbalance > s.config.imbalance_threshold ? t('rebalance.empty.noPlan') : t('rebalance.empty.balanced')}
              >
                {s.imbalance > s.config.imbalance_threshold
                  ? s.config.mode === 'full'
                    ? t('rebalance.empty.fullBody')
                    : t('rebalance.empty.offBody', { mode: REBALANCE_MODE_LABEL[s.config.mode].toLowerCase() })
                  : t('rebalance.empty.balancedBody')}
              </EmptyState>
            )}
          </div>

          <div className="rb-config-line faint">
            <span>{t.rich('rebalance.line.upTo', { n: <b>{s.config.max_concurrent_moves}</b> })}</span>
            <span>{t.rich('rebalance.line.threshold', { n: <b>{s.config.imbalance_threshold}</b> })}</span>
            <span>{t.rich('rebalance.line.replaceAfter', { v: <b>{fmtSecsHuman(s.config.replace_after_secs, t('common.never'))}</b> })}</span>
            <span>{t.rich('rebalance.line.throttle', { v: <b>{fmtThrottle(s.config.throttle_bytes_per_sec)}</b> })}</span>
            <span>{t.rich('rebalance.line.timeout', { v: <b>{fmtSecsHuman(s.config.move_timeout_secs)}</b> })}</span>
          </div>
        </div>
      )}

      {action && (
        <ConfirmDialog
          open
          title={t(ACTION_COPY[action].title)}
          description={t(ACTION_COPY[action].description)}
          confirmLabel={t(ACTION_COPY[action].confirm)}
          cancelLabel={ACTION_COPY[action].dismiss ? t(ACTION_COPY[action].dismiss) : undefined}
          danger={ACTION_COPY[action].danger} busy={busy} onConfirm={doAction} onClose={() => setAction(null)}>
          {sum && sum.total > 0 && (
            <p className="faint rb-confirm-note">
              {[
                t('rebalance.sum.inFlight', { n: sum.inFlight }),
                t('rebalance.sum.pending', { n: sum.pending }),
                t('rebalance.nDone', { n: sum.done }),
                sum.rolledBack ? t('rebalance.sum.rolledBack', { n: sum.rolledBack }) : null,
                sum.cancelled ? t('rebalance.sum.cancelled', { n: sum.cancelled }) : null,
              ]
                .filter(Boolean)
                .join(' · ')}
            </p>
          )}
        </ConfirmDialog>
      )}
      {showNow && <RebalanceNowDialog onClose={() => setShowNow(false)} channels={channels} slots={slots} onSingleNode={() => setRefusedSingle(true)} />}
      {showConfig && s && <RebalanceConfigDialog open onClose={() => setShowConfig(false)} config={s.config} onSingleNode={() => setRefusedSingle(true)} />}
    </section>
  );
}

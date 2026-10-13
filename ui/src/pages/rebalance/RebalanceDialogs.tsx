import { useEffect, useRef, useState } from 'react';
import { CircleCheck, Loader2, Play, RefreshCw, Save } from 'lucide-react';
import { api, ApiError, errorMessage, isApiErrorCode } from '../../api/client';
import { ERROR_UNSUPPORTED_SINGLE_NODE, REBALANCE_MODES, type RebalanceConfig } from '../../api/types';
import {
  configsEqual,
  configToDraft,
  draftToConfig,
  fmtSecsHuman,
  fmtThrottle,
  moveDiff,
  REBALANCE_MODE_HINT,
  REBALANCE_MODE_LABEL,
  spread,
  validateRebalanceDraft,
  type DurationUnit,
  type RebalanceDraft,
  type ThroughputUnit,
  durationUnitLabel,
  throughputUnitLabel,
} from '../../lib/rebalance';
import { t as tr, useT } from '../../i18n';
import { useAsync, useNow } from '../../lib/hooks';
import { toast } from '../../store/ui';
import { setRebalanceStatus } from '../../store/rebalance';
import { EmptyState, InlineError, Modal } from '../../components/ui';
import { Field, ReadOnlyNote } from '../../components/forms';
import { ChipsInput } from '../../components/ChipsInput';
import { GateButton } from '../../components/auth/Gate';
import { useGate } from '../../components/auth/useGate';
import { LoadBars } from './LoadBars';
import { PlanTable } from './PlanTable';

// ───────────────────────────── Rebalance now: dry run → confirm → execute ─────────────────────────────

/** Mount only while open: every opening starts from a fresh dry run over all channels. */
export function RebalanceNowDialog({ onClose, channels, slots, onSingleNode }: { onClose: () => void; channels: string[]; slots: Record<string, number>; onSingleNode: () => void }) {
  const now = useNow();
  const t = useT();
  const [filter, setFilter] = useState<string[]>([]);
  /** Channels of the last requested preview. */
  const [requested, setRequested] = useState<string[]>([]);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<string | null>(null);
  const dry = useAsync((signal) => api.rebalance({ dry_run: true, channels: requested }).then((r) => (signal.aborted ? null : r)), [requested.join(',')]);
  const preview = dry.data ?? null;
  const previewFor = preview ? requested : null;
  const loading = dry.loading;
  const singleRef = useRef(onSingleNode);
  const closeRef = useRef(onClose);
  useEffect(() => {
    singleRef.current = onSingleNode;
    closeRef.current = onClose;
  });
  useEffect(() => {
    if (isApiErrorCode(dry.error, ERROR_UNSUPPORTED_SINGLE_NODE)) {
      singleRef.current();
      closeRef.current();
    }
  }, [dry.error]);
  const error = startError ?? (dry.error && !(dry.error instanceof ApiError && (dry.error.isForbidden || dry.error.code === ERROR_UNSUPPORTED_SINGLE_NODE)) ? errorMessage(dry.error) : null);
  const runPreview = (chs: string[]) => {
    setStartError(null);
    if (chs.join(',') === requested.join(',')) dry.reload();
    else setRequested(chs);
  };

  const stale = previewFor != null && previewFor.join(',') !== filter.join(',');
  const moves = preview?.moves ?? [];

  const start = async () => {
    setStarting(true);
    setStartError(null);
    try {
      const r = await api.rebalance({ dry_run: false, channels: filter });
      setRebalanceStatus(r);
      toast('success', r.moves.length ? tr('rebalance.now.started', { count: r.moves.length }) : tr('rebalance.now.nothingToast'));
      onClose();
    } catch (e) {
      if (isApiErrorCode(e, ERROR_UNSUPPORTED_SINGLE_NODE)) {
        onSingleNode();
        onClose();
      } else if (!(e instanceof ApiError && e.isForbidden)) setStartError(errorMessage(e));
    } finally {
      setStarting(false);
    }
  };

  const added = new Set<string>();
  for (const m of moves) for (const r of moveDiff(m).added) added.add(r);
  const curSpread = preview ? spread(preview.load) : 0;
  const nextSpread = preview ? spread(preview.projected_load) : 0;

  return (
    <Modal
      open
      onClose={onClose}
      wide
      title={t('rebalance.now.button')}
      description={t('rebalance.now.description')}
      footer={
        <>
          <span className="faint rb-foot-note">{preview && moves.length > 0 ? t('rebalance.now.replan') : ''}</span>
          <button className="btn" onClick={onClose}>
            {t('common.cancel')}
          </button>
          <GateButton need="admin" className="btn btn-primary" onClick={start} disabled={starting || loading || stale || !preview || moves.length === 0} title={stale ? t('rebalance.now.staleHint') : undefined} data-autofocus>
            {starting ? <Loader2 size={14} className="spin" /> : <Play size={14} />}{' '}
            {starting ? t('rebalance.now.starting') : moves.length ? t('rebalance.now.startN', { count: moves.length }) : t('rebalance.now.start')}
          </GateButton>
        </>
      }
    >
      <div className="form-stack">
        <div className="rb-filter">
          <Field label={t('common.channels')} hint={t('rebalance.now.channelsHint')}>
            <ChipsInput label={t('common.channels')} value={filter} onChange={setFilter} suggestions={channels} placeholder={t('rebalance.now.channelsPlaceholder')} validate={(s) => (channels.includes(s) ? null : t('rebalance.now.noSuchChannel'))} />
          </Field>
          <button type="button" className="btn btn-sm rb-preview-btn" onClick={() => runPreview(filter)} disabled={loading}>
            {loading ? <Loader2 size={13} className="spin" /> : <RefreshCw size={13} />} {stale ? t('rebalance.now.previewAgain') : t('rebalance.now.refreshPreview')}
          </button>
        </div>
        {error && <InlineError onRetry={() => runPreview(filter)}>{error}</InlineError>}
        {!preview && loading && (
          <div className="rb-loading faint">
            <Loader2 size={14} className="spin" /> {t('rebalance.now.computing')}
          </div>
        )}
        {preview && (
          <div className={`form-stack ${loading ? 'rb-dim' : ''}`}>
            {moves.length === 0 ? (
              <EmptyState compact icon={<CircleCheck size={18} />} title={t('rebalance.now.nothingTitle')}>
                {t(previewFor && previewFor.length ? 'rebalance.now.nothingBodyChannels' : 'rebalance.now.nothingBody', { imbalance: preview.imbalance, threshold: preview.config.imbalance_threshold })}
              </EmptyState>
            ) : (
              <>
                <div className="rb-preview-sum">
                  <b>{t('rebalance.now.partitionMoves', { count: moves.length })}</b>{' '}
                  <span className="faint">
                    · {t('rebalance.now.imbalanceChange', { from: curSpread, to: nextSpread })}
                    {added.size > 0 && <> · {t('rebalance.now.newReplicasOn', { nodes: [...added].sort().join(', ') })}</>} ·{' '}
                    {t('rebalance.now.limits', { n: preview.config.max_concurrent_moves, throttle: fmtThrottle(preview.config.throttle_bytes_per_sec) })}
                  </span>
                </div>
                <div className="rb-sub-h">{t('rebalance.now.currentVsProjected')}</div>
                <LoadBars current={preview.load} projected={preview.projected_load} slots={slots} label={t('rebalance.now.loadLabel')} compact />
                <div className="rb-sub-h">{t('rebalance.now.moves')}</div>
                <div className="rb-preview-table">
                  <PlanTable moves={moves} now={now} preview />
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </Modal>
  );
}

// ───────────────────────────── config ─────────────────────────────

const DURATION_UNITS: DurationUnit[] = ['s', 'min', 'h'];
const THROUGHPUT_UNITS: ThroughputUnit[] = ['KB/s', 'MB/s', 'GB/s'];

function UnitSelect<T extends string>({ value, options, onChange, label, disabled, format }: { value: T; options: T[]; onChange: (v: T) => void; label: string; disabled?: boolean; format: (v: T) => string }) {
  return (
    <select className="select rb-unit" value={value} onChange={(e) => onChange(e.target.value as T)} aria-label={label} disabled={disabled}>
      {options.map((o) => (
        <option key={o} value={o}>
          {format(o)}
        </option>
      ))}
    </select>
  );
}

export function RebalanceConfigDialog({ open, onClose, config, onSingleNode }: { open: boolean; onClose: () => void; config: RebalanceConfig; onSingleNode: () => void }) {
  const gate = useGate('admin');
  const t = useT();
  const readOnly = !gate.allowed;
  const [d, setD] = useState<RebalanceDraft>(() => configToDraft(config));
  const [showErrors, setShowErrors] = useState(false);
  const [saving, setSaving] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const errs = validateRebalanceDraft(d);
  const shown = showErrors ? errs : {};
  const valid = Object.keys(errs).length === 0;
  const set = (p: Partial<RebalanceDraft>) => setD({ ...d, ...p });
  const next = valid ? draftToConfig(d) : null;
  const dirty = next ? !configsEqual(next, config) : true;

  const save = async () => {
    setShowErrors(true);
    if (!next) return;
    setSaving(true);
    setApiError(null);
    try {
      setRebalanceStatus(await api.putRebalanceConfig(next));
      toast('success', tr('rebalance.config.saved'));
      onClose();
    } catch (e) {
      if (isApiErrorCode(e, ERROR_UNSUPPORTED_SINGLE_NODE)) {
        onSingleNode();
        onClose();
      } else if (!(e instanceof ApiError && e.isForbidden)) setApiError(errorMessage(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={onClose}
      wide
      title={t('rebalance.config.title')}
      description={t('rebalance.config.description')}
      footer={
        <>
          <button className="btn" onClick={onClose}>
            {readOnly ? t('common.close') : t('common.cancel')}
          </button>
          <GateButton need="admin" className="btn btn-primary" onClick={save} disabled={saving || !dirty}>
            <Save size={14} /> {saving ? t('common.saving') : t('rebalance.config.save')}
          </GateButton>
        </>
      }
    >
      <ReadOnlyNote what={t('rebalance.config.readOnlyWhat')} />
      <fieldset className="form-stack fs-plain" disabled={readOnly}>
        <div className="field">
          <span className="field-label">{t('rebalance.config.auto')}</span>
          <div className="rb-modes" role="radiogroup" aria-label={t('rebalance.config.autoAria')}>
            {REBALANCE_MODES.map((m) => (
              <label key={m} className={`rb-mode ${d.mode === m ? 'selected' : ''}`}>
                <input type="radio" name="rb-mode" value={m} checked={d.mode === m} onChange={() => set({ mode: m })} />
                <span className="ro-title">{REBALANCE_MODE_LABEL[m]}</span>
                <span className="ro-hint">{REBALANCE_MODE_HINT[m]}</span>
              </label>
            ))}
          </div>
        </div>
        <div className="form-grid">
          <Field label={t('rebalance.config.maxMoves')} htmlFor="rb-conc" error={shown.max_concurrent_moves} hint={t('rebalance.config.maxMovesHint')}>
            <div className="rb-input-unit">
              <input id="rb-conc" className={`input mono ${shown.max_concurrent_moves ? 'invalid' : ''}`} inputMode="numeric" value={d.max_concurrent_moves} onChange={(e) => set({ max_concurrent_moves: e.target.value })} />
              <span className="faint">{t('rebalance.config.atATime')}</span>
            </div>
          </Field>
          <Field label={t('rebalance.config.threshold')} htmlFor="rb-thr" error={shown.imbalance_threshold} hint={t('rebalance.config.thresholdHint')}>
            <div className="rb-input-unit">
              <input id="rb-thr" className={`input mono ${shown.imbalance_threshold ? 'invalid' : ''}`} inputMode="numeric" value={d.imbalance_threshold} onChange={(e) => set({ imbalance_threshold: e.target.value })} />
              <span className="faint">{t('rebalance.config.replicasUnit')}</span>
            </div>
          </Field>
          <Field
            label={t('rebalance.config.replaceAfter')}
            error={shown.replace_after}
            hint={d.replace_never ? t('rebalance.config.replaceNeverHint') : t('rebalance.config.replaceHint')}
          >
            <div className="rb-input-unit">
              <input className={`input mono ${shown.replace_after ? 'invalid' : ''}`} inputMode="decimal" aria-label={t('rebalance.config.replaceAfter')} value={d.replace_after} disabled={d.replace_never || readOnly} onChange={(e) => set({ replace_after: e.target.value })} />
              <UnitSelect value={d.replace_after_unit} options={DURATION_UNITS} format={durationUnitLabel} onChange={(replace_after_unit) => set({ replace_after_unit })} label={t('rebalance.config.unit')} disabled={d.replace_never || readOnly} />
              <label className="check-row rb-inline-check">
                <input type="checkbox" className="checkbox" checked={d.replace_never} onChange={(e) => set({ replace_never: e.target.checked })} />
                {t('rebalance.config.never')}
              </label>
            </div>
          </Field>
          <Field label={t('rebalance.config.throttle')} error={shown.throttle} hint={d.throttle_unlimited ? t('rebalance.config.throttleUnlimitedHint') : t('rebalance.config.throttleHint')}>
            <div className="rb-input-unit">
              <input className={`input mono ${shown.throttle ? 'invalid' : ''}`} inputMode="decimal" aria-label={t('rebalance.config.throttle')} value={d.throttle_unlimited ? '' : d.throttle} placeholder={d.throttle_unlimited ? t('rebalance.unlimited') : undefined} disabled={d.throttle_unlimited || readOnly} onChange={(e) => set({ throttle: e.target.value })} />
              <UnitSelect value={d.throttle_unit} options={THROUGHPUT_UNITS} format={throughputUnitLabel} onChange={(throttle_unit) => set({ throttle_unit })} label={t('rebalance.config.unit')} disabled={d.throttle_unlimited || readOnly} />
              <label className="check-row rb-inline-check">
                <input type="checkbox" className="checkbox" checked={d.throttle_unlimited} onChange={(e) => set({ throttle_unlimited: e.target.checked })} />
                {t('rebalance.config.unlimited')}
              </label>
            </div>
          </Field>
          <Field label={t('rebalance.config.moveTimeout')} error={shown.move_timeout} hint={t('rebalance.config.moveTimeoutHint')}>
            <div className="rb-input-unit">
              <input className={`input mono ${shown.move_timeout ? 'invalid' : ''}`} inputMode="decimal" aria-label={t('rebalance.config.moveTimeout')} value={d.move_timeout} onChange={(e) => set({ move_timeout: e.target.value })} />
              <UnitSelect value={d.move_timeout_unit} options={DURATION_UNITS} format={durationUnitLabel} onChange={(move_timeout_unit) => set({ move_timeout_unit })} label={t('rebalance.config.unit')} disabled={readOnly} />
            </div>
          </Field>
        </div>
      </fieldset>
      {next && dirty && !readOnly && (
        <p className="faint rb-diffnote">
          {t('rebalance.config.saving', {
            mode: REBALANCE_MODE_LABEL[next.mode].toLowerCase(),
            n: next.max_concurrent_moves,
            threshold: next.imbalance_threshold,
            replace: fmtSecsHuman(next.replace_after_secs, t('common.never')),
            throttle: fmtThrottle(next.throttle_bytes_per_sec),
            timeout: fmtSecsHuman(next.move_timeout_secs),
          })}
        </p>
      )}
      {apiError && <InlineError>{apiError}</InlineError>}
    </Modal>
  );
}

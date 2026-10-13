import { useEffect, useRef, useState } from 'react';
import { AlertTriangle, CircleCheck, CircleX, Cloud, DatabaseBackup, Dices, HardDrive, Loader2, Play, Plug, RotateCcw, Trash2 } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { REDACTED, type BackupConfig, type BackupInfo, type BackupManifest, type BackupOverview, type TestResult } from '../../api/types';
import {
  backupDuration,
  configToDraft,
  draftToConfig,
  draftToTarget,
  generateKey,
  INTERVAL_PRESETS,
  listCommand,
  progress,
  restoreCommand,
  S3_PRESETS,
  savings,
  validateBackupDraft,
  validKey,
  type BackupDraft,
  type S3Preset,
} from '../../lib/backup';
import { keepsStored } from '../../lib/secrets';
import { fmtBytes, fmtDateTime, fmtDuration, fmtInt, fmtPercent, fmtRelative } from '../../lib/format';
import { useAsync, useInterval, useNow } from '../../lib/hooks';
import { toast } from '../../store/ui';
import { ConfirmDialog, EmptyState, InlineError, Segmented, SkeletonRows, Switch } from '../../components/ui';
import { CodeBlock, CopyButton, Field, SecretInput } from '../../components/forms';
import { GateButton } from '../../components/auth/Gate';
import { ReadOnlyNote } from '../../components/forms';
import { useGate } from '../../components/auth/useGate';
import { isClusterRefusal, runBackupNow } from '../../lib/backupActions';
import { SettingsTabs } from '../Settings';
import { useT, type TKey } from '../../i18n';
import '../settings.css';
import './backups.css';

const STATUS_BADGE: Record<BackupInfo['status'], string> = { running: 'badge-primary', succeeded: 'badge-success', failed: 'badge-critical' };
const STATUS_KEY: Record<BackupInfo['status'], TKey> = { running: 'backups.status.running', succeeded: 'backups.status.succeeded', failed: 'backups.status.failed' };
const TRIGGER_KEY: Record<string, TKey> = { scheduled: 'backups.trigger.scheduled', manual: 'backups.trigger.manual' };

function fmtSecs(ms: number): string {
  return ms < 60_000 ? fmtDuration(Math.max(1, Math.round(ms / 1000))) : fmtDuration(Math.round(ms / 1000));
}

/** Known trigger values are translated; anything else (future server values) is shown as is. */
function useTrigger() {
  const t = useT();
  return (trigger: string) => (TRIGGER_KEY[trigger] ? t(TRIGGER_KEY[trigger]) : trigger);
}

function TestResultLine({ r }: { r: TestResult | null }) {
  const t = useT();
  if (!r) return null;
  return r.ok ? (
    <span className="test-result ok" role="status">
      <CircleCheck size={14} /> {t('backups.test.ok', { ms: r.latency_ms })}
    </span>
  ) : (
    <span className="test-result fail" role="status" title={r.error_detail ?? undefined}>
      <CircleX size={14} /> {r.error ?? t('backups.test.failed')}
    </span>
  );
}

// ───────────────────────────── status ─────────────────────────────

function ProgressBar({ b }: { b: BackupInfo }) {
  const t = useT();
  const p = progress(b);
  return (
    <div className="bk-progress">
      <div className="bk-bar" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={p == null ? undefined : Math.round(p * 100)} aria-label={t('backups.progress.aria')}>
        <span className={p == null ? 'indeterminate' : ''} style={p == null ? undefined : { width: `${Math.max(1.5, p * 100)}%` }} />
      </div>
      <div className="bk-progress-meta">
        <b className="num">{p == null ? t('backups.progress.preparing') : fmtPercent(Math.floor(p * 100))}</b>
        <span className="faint">{t('backups.progress.meta', { done: fmtBytes(b.progress_bytes), total: fmtBytes(b.total_bytes), reused: fmtInt(b.segments_reused), uploaded: fmtInt(b.segments_uploaded) })}</span>
      </div>
    </div>
  );
}

function StatusCard({ ov, onRun, running }: { ov: BackupOverview; onRun: () => void; running: boolean }) {
  const t = useT();
  const trig = useTrigger();
  const now = useNow();
  const r = ov.running;
  return (
    <section className="card bk-status">
      <div className="card-head">
        <h2>{t('common.status')}</h2>
        <span className={`badge ${!ov.configured ? '' : ov.enabled ? 'badge-success' : 'badge-warning'}`}>{t(!ov.configured ? 'backups.statusCard.notConfigured' : ov.enabled ? 'backups.statusCard.scheduled' : 'backups.statusCard.paused')}</span>
        <span className="spacer" />
        <GateButton need="admin" className="btn btn-primary btn-sm" onClick={onRun} disabled={!ov.configured || !!r || running} title={!ov.configured ? t('backups.statusCard.configureFirst') : r ? t('backups.statusCard.isRunning') : undefined}>
          {running ? <Loader2 size={14} className="spin" /> : <Play size={14} />} {t('backups.statusCard.runNow')}
        </GateButton>
      </div>
      <div className="card-body">
        {r && (
          <div className="bk-running">
            <div className="bk-running-head">
              <Loader2 size={14} className="spin" /> {t.rich('backups.statusCard.backingUp', { id: <span className="mono">{r.id}</span> })}
              <span className="faint">
                {t('backups.statusCard.runningMeta', {
                  trigger: trig(r.trigger),
                  when: fmtRelative(r.started_at_ms, now),
                  channels: t('common.channelsCount', { count: r.channels }),
                  partitions: t('common.partitionsCount', { count: r.partitions }),
                })}
              </span>
            </div>
            <ProgressBar b={r} />
          </div>
        )}
        <dl className="bk-facts">
          <div>
            <dt>{t('backups.statusCard.lastSuccess')}</dt>
            <dd>
              {ov.last_success ? (
                <>
                  <span title={fmtDateTime(ov.last_success.finished_at_ms)}>{fmtRelative(ov.last_success.finished_at_ms ?? ov.last_success.started_at_ms, now)}</span>
                  <small className="faint">
                    {fmtBytes(ov.last_success.total_bytes)} · {fmtSecs(backupDuration(ov.last_success, now))}
                  </small>
                </>
              ) : (
                <span className="faint">{t('common.never')}</span>
              )}
            </dd>
          </div>
          <div>
            <dt>{t('backups.statusCard.lastFailure')}</dt>
            <dd>
              {ov.last_failure ? (
                <>
                  <span className="text-critical" title={fmtDateTime(ov.last_failure.started_at_ms)}>
                    {fmtRelative(ov.last_failure.started_at_ms, now)}
                  </span>
                  <small className="faint truncate" title={ov.last_failure.error ?? undefined}>
                    {ov.last_failure.error ?? t('backups.statusCard.failed')}
                  </small>
                </>
              ) : (
                <span className="faint">{t('backups.statusCard.none')}</span>
              )}
            </dd>
          </div>
          <div>
            <dt>{t('backups.statusCard.nextRun')}</dt>
            <dd>
              {ov.next_run_ms ? (
                <>
                  <span title={fmtDateTime(ov.next_run_ms)}>{ov.next_run_ms > now ? t('backups.statusCard.inTime', { time: fmtSecs(ov.next_run_ms - now) }) : t('backups.statusCard.dueNow')}</span>
                  <small className="faint">{fmtDateTime(ov.next_run_ms)}</small>
                </>
              ) : (
                <span className="faint">{r ? t('backups.statusCard.afterCurrent') : ov.configured && !ov.enabled ? t('backups.statusCard.schedulePaused') : '—'}</span>
              )}
            </dd>
          </div>
        </dl>
      </div>
    </section>
  );
}

// ───────────────────────────── config form ─────────────────────────────

function ConfigCard({ config, onSaved, onRefused }: { config: BackupConfig | null; onSaved: (c: BackupConfig) => void; onRefused: (m: string) => void }) {
  const t = useT();
  const gate = useGate('admin');
  const readOnly = !gate.allowed;
  const [d, setD] = useState<BackupDraft>(() => configToDraft(config));
  const [showErrors, setShowErrors] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [test, setTest] = useState<TestResult | null>(null);
  const [apiError, setApiError] = useState<string | null>(null);
  const [keyAck, setKeyAck] = useState(false);
  const errs = validateBackupDraft(d);
  const shown = showErrors ? errs : {};
  const set = (p: Partial<BackupDraft>) => setD({ ...d, ...p });
  const setTarget = (p: Partial<BackupDraft>) => {
    setD({ ...d, ...p });
    setTest(null);
  };
  const newKey = d.encrypt && !keepsStored(d.encryption_key) && d.encryption_key.value.trim() !== '';
  const dirty = !config || JSON.stringify(draftToConfig(d)) !== JSON.stringify(draftToConfig(configToDraft(config)));
  const targetErrs = Object.keys(errs).some((k) => ['endpoint', 'region', 'bucket', 'access_key_id', 'secret_access_key', 'path'].includes(k));

  const applyPreset = (p: S3Preset) => setTarget({ endpoint: S3_PRESETS[p].endpoint, region: S3_PRESETS[p].region, path_style: S3_PRESETS[p].path_style });
  const preset: S3Preset | null = d.endpoint === '' ? 'aws' : d.endpoint.includes('r2.cloudflarestorage.com') ? 'r2' : d.path_style ? 'minio' : null;

  const runTest = async () => {
    setShowErrors(true);
    if (targetErrs) return;
    setTesting(true);
    setTest(null);
    try {
      const r = await api.testBackupTarget(draftToTarget(d));
      setTest(r);
      if (r.ok) toast('success', t('backups.toast.targetOk', { ms: r.latency_ms }));
      else toast('error', r.error ?? t('backups.toast.targetFailed'));
    } catch (e) {
      if (isClusterRefusal(e)) onRefused(errorMessage(e));
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setTesting(false);
    }
  };

  const save = async () => {
    setShowErrors(true);
    if (Object.keys(errs).length || (newKey && !keyAck)) return;
    setSaving(true);
    setApiError(null);
    try {
      const c = await api.putBackupConfig(draftToConfig(d));
      toast('success', t(config ? 'backups.toast.saved' : 'backups.toast.configured'));
      setD(configToDraft(c));
      setKeyAck(false);
      onSaved(c);
    } catch (e) {
      if (isClusterRefusal(e)) onRefused(errorMessage(e));
      if (!(e instanceof ApiError && e.isForbidden)) setApiError(errorMessage(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="card">
      <div className="card-head">
        <h2>{t('backups.config.title')}</h2>
        <span className="spacer" />
        <span className="re-enabled">
          <Switch checked={d.enabled} onChange={(enabled) => set({ enabled })} label={t('backups.config.enabledLabel')} disabled={readOnly} title={readOnly ? (gate.reason ?? undefined) : undefined} />
          <span className="muted">{t(d.enabled ? 'backups.statusCard.scheduled' : 'backups.config.manualOnly')}</span>
        </span>
      </div>
      <div className="card-body form-stack">
        <fieldset className="form-stack fs-plain" disabled={readOnly}>
        <div className="bk-kind">
          <Segmented
            value={d.kind}
            onChange={(kind) => setTarget({ kind })}
            label={t('backups.config.targetType')}
            options={[
              { value: 's3', label: t('backups.config.s3') },
              { value: 'filesystem', label: t('backups.config.filesystem') },
            ]}
          />
          {d.kind === 's3' && (
            <div className="bk-presets" role="group" aria-label={t('backups.config.presets')}>
              <span className="faint">{t('backups.config.preset')}</span>
              {(Object.keys(S3_PRESETS) as S3Preset[]).map((p) => (
                <button key={p} type="button" className="tchip" aria-pressed={preset === p} disabled={readOnly} onClick={() => applyPreset(p)}>
                  {S3_PRESETS[p].label}
                </button>
              ))}
            </div>
          )}
        </div>
        {d.kind === 's3' ? (
          <div className="form-grid">
            <Field label={t('backups.config.endpoint')} htmlFor="bk-endpoint" className="span-2" error={shown.endpoint} hint={preset ? S3_PRESETS[preset].hint : t('backups.config.endpointCustomHint')}>
              <input id="bk-endpoint" className={`input mono ${shown.endpoint ? 'invalid' : ''}`} value={d.endpoint} disabled={readOnly} onChange={(e) => setTarget({ endpoint: e.target.value })} placeholder={t('backups.config.endpointPh')} />
            </Field>
            <Field label={t('backups.config.bucket')} htmlFor="bk-bucket" error={shown.bucket}>
              <input id="bk-bucket" className={`input mono ${shown.bucket ? 'invalid' : ''}`} value={d.bucket} disabled={readOnly} onChange={(e) => setTarget({ bucket: e.target.value })} placeholder="fluxmq-backups" />
            </Field>
            <Field label={t('backups.config.region')} htmlFor="bk-region" error={shown.region}>
              <input id="bk-region" className={`input mono ${shown.region ? 'invalid' : ''}`} value={d.region} disabled={readOnly} onChange={(e) => setTarget({ region: e.target.value })} />
            </Field>
            <Field label={t('backups.config.prefix')} htmlFor="bk-prefix" hint={t('backups.config.prefixHint')}>
              <input id="bk-prefix" className="input mono" value={d.prefix} disabled={readOnly} onChange={(e) => setTarget({ prefix: e.target.value })} placeholder="prod-eu-1/" />
            </Field>
            <Field label={t('backups.config.addressing')}>
              <label className="check-row">
                <input type="checkbox" className="checkbox" checked={d.path_style} disabled={readOnly} onChange={(e) => setTarget({ path_style: e.target.checked })} />
                {t('backups.config.pathStyle')}
              </label>
            </Field>
            <Field label={t('backups.config.accessKeyId')} htmlFor="bk-akid" error={shown.access_key_id}>
              <input id="bk-akid" className={`input mono ${shown.access_key_id ? 'invalid' : ''}`} value={d.access_key_id} disabled={readOnly} onChange={(e) => setTarget({ access_key_id: e.target.value })} autoComplete="off" />
            </Field>
            <Field label={t('backups.config.secretAccessKey')} error={shown.secret_access_key}>
              <SecretInput value={d.secret_access_key} onChange={(secret_access_key) => setTarget({ secret_access_key })} label={t('backups.config.secretAccessKey')} invalid={!!shown.secret_access_key} disabled={readOnly} />
            </Field>
          </div>
        ) : (
          <Field label={t('backups.config.path')} htmlFor="bk-path" error={shown.path} hint={t('backups.config.pathHint')}>
            <input id="bk-path" className={`input mono ${shown.path ? 'invalid' : ''}`} value={d.path} disabled={readOnly} onChange={(e) => setTarget({ path: e.target.value })} />
          </Field>
        )}
        <div className="bk-test">
          <GateButton need="admin" className="btn btn-sm" onClick={runTest} disabled={testing}>
            {testing ? <Loader2 size={13} className="spin" /> : <Plug size={13} />} {t('backups.config.testConnection')}
          </GateButton>
          <TestResultLine r={test} />
        </div>

        <div className="bk-divider" />

        <div className="form-grid">
          <Field label={t('backups.config.every')} error={shown.interval_secs}>
            <Segmented value={String(d.interval_secs)} onChange={(v) => set({ interval_secs: Number(v) })} label={t('backups.config.interval')} options={INTERVAL_PRESETS.map((p) => ({ value: String(p.value), label: p.label }))} />
          </Field>
          <Field label={t('backups.config.contents')} hint={t(d.include_messages ? 'backups.config.contentsIncremental' : 'backups.config.contentsMetadata')}>
            <label className="check-row">
              <input type="checkbox" className="checkbox" checked={d.include_messages} disabled={readOnly} onChange={(e) => set({ include_messages: e.target.checked })} />
              {t('backups.config.includeMessages')}
            </label>
          </Field>
          <Field label={t('backups.config.keepLast')} htmlFor="bk-keep" error={shown.keep_last} hint={t('backups.config.keepLastHint')}>
            <input id="bk-keep" className={`input mono ${shown.keep_last ? 'invalid' : ''}`} value={d.keep_last} disabled={readOnly} onChange={(e) => set({ keep_last: e.target.value })} inputMode="numeric" />
          </Field>
          <Field label={t('backups.config.keepDays')} htmlFor="bk-days" error={shown.keep_days} hint={t('backups.config.keepDaysHint')}>
            <input id="bk-days" className={`input mono ${shown.keep_days ? 'invalid' : ''}`} value={d.keep_days} disabled={readOnly} onChange={(e) => set({ keep_days: e.target.value })} inputMode="numeric" />
          </Field>
        </div>

        <div className="bk-divider" />

        <div className="form-stack bk-enc">
          <label className="check-row">
            <input type="checkbox" className="checkbox" checked={d.encrypt} disabled={readOnly} onChange={(e) => set({ encrypt: e.target.checked })} />
            {t('backups.config.encrypt')}
          </label>
          {d.encrypt && (
            <>
              <Field label={t('backups.config.encryptionKey')} error={shown.encryption_key} hint={t('backups.config.encryptionKeyHint')}>
                <div className="bk-key">
                  <SecretInput value={d.encryption_key} onChange={(encryption_key) => set({ encryption_key })} label={t('backups.config.encryptionKey')} invalid={!!shown.encryption_key} disabled={readOnly} placeholder={t('backups.config.encryptionKeyPh')} />
                  {!readOnly && !keepsStored(d.encryption_key) && (
                    <button type="button" className="btn btn-sm" onClick={() => set({ encryption_key: { stored: d.encryption_key.stored, replacing: true, value: generateKey() } })}>
                      <Dices size={13} /> {t('backups.config.generate')}
                    </button>
                  )}
                  {newKey && <CopyButton text={d.encryption_key.value.trim()} label={t('backups.config.copyKey')} />}
                </div>
              </Field>
              {newKey && validKey(d.encryption_key.value) && (
                <div className="secret-box bk-keybox">
                  <code className="mono" aria-label={t('backups.config.newKeyAria')}>
                    {d.encryption_key.value.trim()}
                  </code>
                </div>
              )}
              <div className="secret-warn bk-warn" role="alert">
                <AlertTriangle size={16} />
                <span>
                  <b>{t('backups.config.keyWarnTitle')}</b> {t('backups.config.keyWarnBody')}
                  {d.encryption_key.stored && !keepsStored(d.encryption_key) && t('backups.config.keyReplace')}
                </span>
              </div>
              {newKey && (
                <label className="check-row bk-ack">
                  <input type="checkbox" className="checkbox" checked={keyAck} onChange={(e) => setKeyAck(e.target.checked)} />
                  {t('backups.config.keyAck')}
                </label>
              )}
            </>
          )}
          {!d.encrypt && d.encryption_key.stored && <p className="text-warning bk-note">{t('backups.config.encryptOff')}</p>}
        </div>
        </fieldset>
        {showErrors && newKey && !keyAck && <InlineError>{t('backups.config.confirmKey')}</InlineError>}
        {apiError && <InlineError>{apiError}</InlineError>}
        <div className="bk-save">
          <span className="faint">{t(config ? 'backups.config.secretsNote' : 'backups.config.nothingYet')}</span>
          <span className="spacer" />
          <GateButton need="admin" className="btn btn-primary" onClick={save} disabled={saving || !dirty}>
            {saving ? t('common.saving') : t(config ? 'backups.config.saveSettings' : 'backups.config.saveEnable')}
          </GateButton>
        </div>
      </div>
    </section>
  );
}

// ───────────────────────────── history + restore ─────────────────────────────

function SavingsBar({ b }: { b: BackupInfo }) {
  const t = useT();
  const s = savings(b);
  const segs = b.segments_uploaded + b.segments_reused;
  if (!segs) return <span className="faint">—</span>;
  return (
    <div className="bk-savings" title={t('backups.savings.title', { uploaded: fmtInt(b.segments_uploaded), reused: fmtInt(b.segments_reused), saved: fmtBytes(s.bytesSaved) })}>
      <div className="bk-sbar" aria-hidden>
        <span className="up" style={{ width: `${Math.max(1.5, (1 - s.reusedShare) * 100)}%` }} />
      </div>
      <span className="bk-slabel">{b.status === 'failed' ? t('backups.savings.incomplete') : s.reusedShare > 0 ? t('backups.savings.reused', { pct: Math.round(s.reusedShare * 100) }) : t('backups.savings.full')}</span>
    </div>
  );
}

function HistoryCard({ list, config, selected, onSelect, onDeleted }: { list: BackupInfo[]; config: BackupConfig | null; selected: string | null; onSelect: (id: string) => void; onDeleted: () => void }) {
  const t = useT();
  const trig = useTrigger();
  const now = useNow();
  const [del, setDel] = useState<BackupInfo | null>(null);
  const [busy, setBusy] = useState(false);
  const doDelete = async () => {
    if (!del) return;
    setBusy(true);
    try {
      await api.deleteBackup(del.id);
      toast('success', t('backups.toast.deleted', { id: del.id }));
      setDel(null);
      onDeleted();
    } catch (e) {
      if (!(e instanceof ApiError && e.isForbidden)) toast('error', errorMessage(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <section className="card">
      <div className="card-head">
        <h2>{t('common.history')}</h2>
        <span className="card-sub">
          {list.length
            ? `${t('backups.history.count', { count: list.length })}${config ? `${t('backups.history.keeping', { n: config.keep_last })}${config.keep_days ? t('backups.history.keepingDays', { days: config.keep_days }) : ''}` : ''}`
            : ''}
        </span>
      </div>
      {list.length === 0 ? (
        <EmptyState compact icon={<DatabaseBackup size={18} />} title={t('backups.history.emptyTitle')}>
          {t(config ? 'backups.history.emptyConfigured' : 'backups.history.emptyNoConfig')}
        </EmptyState>
      ) : (
        <div className="table-wrap">
          <table className="table bk-table">
            <thead>
              <tr>
                <th>{t('backups.history.colBackup')}</th>
                <th>{t('common.status')}</th>
                <th>{t('common.started')}</th>
                <th className="r">{t('backups.history.colDuration')}</th>
                <th className="r">{t('common.size')}</th>
                <th>{t('backups.history.colSavings')}</th>
                <th aria-label={t('common.actions')} />
              </tr>
            </thead>
            <tbody>
              {list.map((b) => (
                <tr key={b.id} className={selected === b.id ? 'selected' : ''}>
                  <td>
                    <span className="mono bk-id">{b.id}</span>
                    <div className="faint bk-trigger">{trig(b.trigger)}</div>
                  </td>
                  <td>
                    <span className={`badge status-badge ${STATUS_BADGE[b.status]}`}>{b.status === 'running' ? fmtPercent(Math.floor((progress(b) ?? 0) * 100)) : t(STATUS_KEY[b.status])}</span>
                    {b.error && (
                      <div className="text-critical bk-err truncate" title={b.error}>
                        {b.error}
                      </div>
                    )}
                  </td>
                  <td className="nowrap" title={fmtDateTime(b.started_at_ms)}>
                    {fmtRelative(b.started_at_ms, now)}
                  </td>
                  <td className="r num">{fmtSecs(backupDuration(b, now))}</td>
                  <td className="r num">
                    {fmtBytes(b.total_bytes)}
                    <div className="faint bk-up">↑ {fmtBytes(b.bytes_uploaded)}</div>
                  </td>
                  <td>
                    <SavingsBar b={b} />
                  </td>
                  <td className="r nowrap">
                    {b.status === 'succeeded' && (
                      <button type="button" className={`btn btn-sm ${selected === b.id ? 'btn-primary' : ''}`} onClick={() => onSelect(b.id)} aria-pressed={selected === b.id}>
                        <RotateCcw size={13} /> {t('backups.history.restore')}
                      </button>
                    )}
                    {b.status !== 'running' && (
                      <GateButton need="admin" className="btn btn-sm btn-ghost btn-icon" onClick={() => setDel(b)} aria-label={t('backups.history.deleteAria', { id: b.id })}>
                        <Trash2 size={13} />
                      </GateButton>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <ConfirmDialog
        open={del != null}
        title={t('backups.history.deleteTitle', { id: del?.id ?? '' })}
        description={t('backups.history.deleteBody')}
        confirmLabel={t('backups.history.deleteConfirm')}
        danger
        busy={busy}
        onConfirm={doDelete}
        onClose={() => setDel(null)}
      />
    </section>
  );
}

function ManifestDetails({ m }: { m: BackupManifest }) {
  const t = useT();
  const rows = m.channels;
  return (
    <div className="bk-manifest">
      <dl className="kv">
        <dt>{t('backups.manifest.takenOn')}</dt>
        <dd>{t.rich('backups.manifest.takenOnValue', { node: <span className="mono">{m.node_id}</span>, cluster: m.cluster_name, version: m.fluxmq_version })}</dd>
        {m.nodes.length > 1 && (
          <>
            <dt>{t('backups.manifest.dataFrom')}</dt>
            <dd>
              {m.nodes.map((n, i) => (
                <span key={n}>
                  {i > 0 && ', '}
                  <span className="mono">{n}</span>
                </span>
              ))}
            </dd>
          </>
        )}
        <dt>{t('backups.manifest.contents')}</dt>
        <dd>
          {t('common.channelsCount', { count: m.info.channels })}, {t('common.partitionsCount', { count: m.info.partitions })} · {m.include_messages ? t('backups.manifest.messages', { n: fmtInt(m.info.messages) }) : t('backups.manifest.metadataOnly')}
        </dd>
        <dt>{t('backups.manifest.encryption')}</dt>
        <dd>{m.encryption ? t.rich('backups.manifest.encrypted', { fp: <span className="mono">{m.encryption}</span> }) : <span className="faint">{t('backups.manifest.notEncrypted')}</span>}</dd>
      </dl>
      {rows.length > 0 && (
        <details className="bk-more">
          <summary>{t('backups.manifest.channelsIn', { count: rows.length })}</summary>
          <div className="table-wrap">
            <table className="table bk-ch-table">
              <thead>
                <tr>
                  <th>{t('common.channel')}</th>
                  <th className="r">{t('common.partitions')}</th>
                  <th className="r">{t('backups.manifest.colMessages')}</th>
                  <th className="r">{t('common.size')}</th>
                  <th className="r">{t('backups.manifest.colSegments')}</th>
                  <th className="r">{t('backups.manifest.colGroups')}</th>
                  <th className="r">{'DLQ'}</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.name}>
                    <td className="mono">{r.name}</td>
                    <td className="r num">{r.partitions}</td>
                    <td className="r num">{fmtInt(r.messages)}</td>
                    <td className="r num">{fmtBytes(r.bytes)}</td>
                    <td className="r num">{fmtInt(r.segments)}</td>
                    <td className="r num">{fmtInt(r.groups)}</td>
                    <td className="r num">{fmtInt(r.dlq_entries)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}
    </div>
  );
}

function RestorePanel({ config, backup }: { config: BackupConfig; backup: BackupInfo | null }) {
  const t = useT();
  const [dataDir, setDataDir] = useState('/var/lib/fluxmq-restored');
  const id = backup?.id ?? 'latest';
  const manifest = useAsync((signal) => api.getBackup(id, signal), [id]);
  // Tolerate servers that return a bare BackupInfo here (pre-manifest builds).
  const m = manifest.data && Array.isArray(manifest.data.channels) && manifest.data.info ? manifest.data : undefined;
  // The manifest knows whether *this* backup is encrypted (the config may have changed since).
  const encrypted = m ? m.encryption != null : config.encryption_key != null && config.encryption_key !== '';
  const cmdConfig: BackupConfig = { ...config, encryption_key: encrypted ? REDACTED : null };
  const cmdTitle = `fluxmq restore · ${id}`; // CLI command: not translated
  return (
    <section className="card" id="restore">
      <div className="card-head">
        <h2>{t('backups.restore.title')}</h2>
        <span className="card-sub">{backup ? t('backups.restore.backupSub', { id: backup.id, when: fmtDateTime(backup.finished_at_ms) }) : t('backups.restore.latestSub')}</span>
      </div>
      <div className="card-body form-stack">
        {m ? <ManifestDetails m={m} /> : manifest.error ? <InlineError>{errorMessage(manifest.error)}</InlineError> : null}
        <ol className="bk-steps">
          <li>{t.rich('backups.restore.step1', { single: <b>{t('backups.restore.single')}</b> })}</li>
          <li>
            {t.rich('backups.restore.step2', { empty: <b>{t('backups.restore.empty')}</b> })}
            <input className="input mono bk-dir" value={dataDir} onChange={(e) => setDataDir(e.target.value || '/var/lib/fluxmq-restored')} aria-label={t('backups.restore.dirAria')} />
          </li>
          <li>{t.rich(encrypted ? 'backups.restore.step3Encrypted' : 'backups.restore.step3', { flag: <span className="mono">{`--data-dir ${dataDir}`}</span> })}</li>
        </ol>
        <CodeBlock title={cmdTitle} code={restoreCommand(cmdConfig, id, dataDir)} />
        <details className="bk-more">
          <summary>{t('backups.restore.listCli')}</summary>
          <CodeBlock code={listCommand(config)} />
        </details>
      </div>
    </section>
  );
}

// ───────────────────────────── page ─────────────────────────────

export default function BackupsPage() {
  const t = useT();
  const config = useAsync((s) => api.backupConfig(s), []);
  const status = useAsync((s) => api.backupStatus(s), []);
  const list = useAsync((s) => api.listBackups(s), []);
  const meta = useAsync((s) => api.clusterMetadata(s).catch(() => null), []);
  const clustered = (meta.data?.nodes.length ?? 0) > 1;
  const isRunning = !!status.data?.running;
  useInterval(status.reload, isRunning ? 1500 : 10_000);
  useInterval(list.reload, isRunning ? 3000 : 30_000);
  const [selected, setSelected] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const [refusal, setRefusal] = useState<string | null>(null);
  const restoreRef = useRef<HTMLDivElement>(null);

  // When a run finishes, refresh the history once.
  const runningId = status.data?.running?.id ?? null;
  const prevRunning = useRef<string | null>(null);
  useEffect(() => {
    if (prevRunning.current && !runningId) list.reload();
    prevRunning.current = runningId;
  }, [runningId, list]);

  const run = async () => {
    setStarting(true);
    if (await runBackupNow(setRefusal)) {
      status.reload();
      list.reload();
    }
    setStarting(false);
  };

  const notFound = [config.error, status.error].some((e) => e instanceof ApiError && e.isNotFound);
  const backups = list.data ?? [];
  const latestOk = backups.find((b) => b.status === 'succeeded') ?? null;
  const sel = backups.find((b) => b.id === selected) ?? latestOk;
  const loaded = config.data !== undefined && status.data !== undefined;

  return (
    <div className="page settings-page">
      <div className="page-head">
        <div className="titles">
          <h1>{t('common.settings')}</h1>
          <p>{t('backups.subtitle')}</p>
        </div>
      </div>
      <SettingsTabs />
      <ReadOnlyNote what={t('backups.readOnlyWhat')} />
      {notFound ? (
        <section className="card">
          <EmptyState icon={<DatabaseBackup size={20} />} title={t('backups.unavailable.title')}>
            {t('backups.unavailable.body')}
          </EmptyState>
        </section>
      ) : (config.error || status.error) && !loaded ? (
        <InlineError
          onRetry={() => {
            config.reload();
            status.reload();
          }}
        >
          {errorMessage(config.error ?? status.error)}
        </InlineError>
      ) : !loaded ? (
        <section className="card">
          <SkeletonRows rows={4} />
        </section>
      ) : (
        <>
          {!status.data!.configured && !config.data && (
            <section className="card bk-intro explain">
              <div className="bk-intro-icons" aria-hidden>
                <Cloud size={18} />
                <HardDrive size={18} />
              </div>
              <div>
                <h2>{t('backups.intro.title')}</h2>
                <p className="muted">{t('backups.intro.body')}</p>
              </div>
            </section>
          )}
          {(refusal || [config.error, status.error, list.error].some(isClusterRefusal)) && (
            <div className="open-callout bk-cluster" role="status">
              <AlertTriangle size={16} />
              <span>
                <b>{t('backups.clusterRefusal.title')}</b> {t('backups.clusterRefusal.body')}{' '}
                <span className="faint">{refusal ?? errorMessage([config.error, status.error, list.error].find(isClusterRefusal))}</span>
              </span>
            </div>
          )}
          {clustered && (
            <p className="bk-cluster-note muted">{t('backups.clusterNote')}</p>
          )}
          <StatusCard ov={status.data!} onRun={run} running={starting} />
          <ConfigCard
            key={config.data ? 'cfg' : 'none'}
            config={config.data ?? null}
            onRefused={setRefusal}
            onSaved={() => {
              config.reload();
              status.reload();
            }}
          />
          <HistoryCard
            list={backups}
            config={config.data ?? null}
            selected={sel?.id ?? null}
            onSelect={(id) => {
              setSelected(id);
              restoreRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }}
            onDeleted={() => {
              list.reload();
              status.reload();
            }}
          />
          <div ref={restoreRef}>{config.data && <RestorePanel config={config.data} backup={sel} />}</div>
        </>
      )}
    </div>
  );
}

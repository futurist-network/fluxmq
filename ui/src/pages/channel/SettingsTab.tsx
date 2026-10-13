import { useState } from 'react';
import { useNavigate } from 'react-router';
import { Lock, Trash2 } from 'lucide-react';
import { api, errorMessage } from '../../api/client';
import { isSystemChannel, type ChannelConfig } from '../../api/types';
import { useAsync } from '../../lib/hooks';
import { fmtDateTime } from '../../lib/format';
import { MODE_LABEL } from '../../lib/labels';
import { formatBackoffList, MAX_BYTES_PRESETS, parseBackoffList, RETENTION_PRESETS } from '../../lib/presets';
import { toast } from '../../store/ui';
import { ConfirmDialog, InlineError, SkeletonRows, Switch } from '../../components/ui';
import { GateButton } from '../../components/auth/Gate';
import { useGate } from '../../components/auth/useGate';
import { useT } from '../../i18n';

function SettingsForm({ cfg, onSaved }: { cfg: ChannelConfig; onSaved: (c: ChannelConfig) => void }) {
  const t = useT();
  const navigate = useNavigate();
  const [age, setAge] = useState<string>(cfg.retention.max_age_secs == null ? 'null' : String(cfg.retention.max_age_secs));
  const [bytes, setBytes] = useState<string>(cfg.retention.max_bytes == null ? 'null' : String(cfg.retention.max_bytes));
  const [attempts, setAttempts] = useState(String(cfg.retry.max_attempts));
  const [backoff, setBackoff] = useState(formatBackoffList(cfg.retry.backoff_ms));
  const [ackTimeout, setAckTimeout] = useState(String(cfg.retry.ack_timeout_ms / 1000));
  const [dlq, setDlq] = useState(cfg.dlq_enabled);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [confirmName, setConfirmName] = useState('');
  const [deleting, setDeleting] = useState(false);

  const backoffMs = parseBackoffList(backoff);
  const attemptsN = Number(attempts);
  const ackN = Number(ackTimeout);
  const invalid = !backoffMs || !Number.isInteger(attemptsN) || attemptsN < 1 || !(ackN > 0);

  const ageOptions = RETENTION_PRESETS.some((p) => String(p.secs) === age) ? RETENTION_PRESETS : [...RETENTION_PRESETS, { label: t('channels.settings.customAge', { secs: age }), secs: Number(age) }];
  const bytesOptions = MAX_BYTES_PRESETS.some((p) => String(p.bytes) === bytes) ? MAX_BYTES_PRESETS : [...MAX_BYTES_PRESETS, { label: t('channels.settings.customBytes', { bytes }), bytes: Number(bytes) }];

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    if (invalid || !backoffMs) return;
    setSaving(true);
    setError(null);
    try {
      const next = await api.updateChannel(cfg.name, {
        retention: { max_age_secs: age === 'null' ? null : Number(age), max_bytes: bytes === 'null' ? null : Number(bytes) },
        retry: { max_attempts: attemptsN, backoff_ms: backoffMs, ack_timeout_ms: Math.round(ackN * 1000) },
        dlq_enabled: dlq,
      });
      onSaved(next);
      toast('success', t('channels.settings.saved'));
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setSaving(false);
    }
  };

  const doDelete = async () => {
    setDeleting(true);
    try {
      await api.deleteChannel(cfg.name);
      toast('success', t('channels.settings.deleted', { name: cfg.name }));
      navigate('/channels');
    } catch (err) {
      toast('error', errorMessage(err));
      setDeleting(false);
    }
  };

  const gate = useGate('admin');
  const system = isSystemChannel(cfg.name);
  const canEdit = gate.allowed && !system;
  const editReason = system ? t('channels.settings.systemReadOnly') : gate.reason;
  return (
    <div className="stack settings-stack">
      <form className="card" onSubmit={save}>
        <div className="card-head">
          <h2>{t('channels.settings.retentionDelivery')}</h2>
          <span className="card-sub">{t('channels.settings.appliesNow')}</span>
        </div>
        <fieldset className="card-body form-grid" disabled={!canEdit}>
          <div className="field">
            <label htmlFor="s-age">{t('channels.create.keepFor')}</label>
            <select id="s-age" className="select" value={age} onChange={(e) => setAge(e.target.value)}>
              {ageOptions.map((p) => (
                <option key={String(p.secs)} value={String(p.secs)}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="s-bytes">{t('channels.sizeLimit')}</label>
            <select id="s-bytes" className="select" value={bytes} onChange={(e) => setBytes(e.target.value)}>
              {bytesOptions.map((p) => (
                <option key={String(p.bytes)} value={String(p.bytes)}>
                  {p.label}
                </option>
              ))}
            </select>
            <span className="field-hint">{t('channels.settings.oldestDropped')}</span>
          </div>
          <div className="field">
            <label htmlFor="s-attempts">{t('channels.retry.maxAttempts')}</label>
            <input id="s-attempts" className={`input ${!Number.isInteger(attemptsN) || attemptsN < 1 ? 'invalid' : ''}`} inputMode="numeric" value={attempts} onChange={(e) => setAttempts(e.target.value)} />
          </div>
          <div className="field">
            <label htmlFor="s-backoff">{t('channels.settings.backoffBetween')}</label>
            <input id="s-backoff" className={`input mono ${!backoffMs ? 'invalid' : ''}`} value={backoff} onChange={(e) => setBackoff(e.target.value)} />
            <span className={backoffMs ? 'field-hint' : 'field-error'}>{backoffMs ? t('channels.settings.backoffHint') : t('channels.settings.backoffError')}</span>
          </div>
          <div className="field">
            <label htmlFor="s-ack">{t('channels.retry.ackTimeoutSecs')}</label>
            <input id="s-ack" className={`input ${!(ackN > 0) ? 'invalid' : ''}`} inputMode="decimal" value={ackTimeout} onChange={(e) => setAckTimeout(e.target.value)} />
            <span className="field-hint">{t('channels.settings.ackHint')}</span>
          </div>
          <div className="field">
            <span className="field-label">{t('channels.dlqFull')}</span>
            <div className="switch-row">
              <Switch checked={dlq} onChange={setDlq} label={t('channels.dlqFull')} />
              <span className="muted">{dlq ? t('channels.settings.dlqOn') : t('channels.settings.dlqOff')}</span>
            </div>
          </div>
        </fieldset>
        {!canEdit && <p className="faint readonly-note">{editReason}.</p>}
        {error && (
          <div style={{ padding: '0 20px 12px' }}>
            <InlineError>{error}</InlineError>
          </div>
        )}
        <div className="modal-foot">
          <GateButton need="admin" className="btn btn-primary" type="submit" disabled={saving || invalid || system}>
            {saving ? t('common.saving') : t('channels.settings.saveChanges')}
          </GateButton>
        </div>
      </form>

      <section className="card">
        <div className="card-head">
          <h2>{t('channels.settings.fixed')}</h2>
          <Lock size={13} className="faint" />
        </div>
        <div className="card-body">
          <dl className="kv">
            <dt>{t('common.mode')}</dt>
            <dd>{MODE_LABEL[cfg.mode]}</dd>
            <dt>{t('channels.partitionsFirst')}</dt>
            <dd className="num">{cfg.partitions}</dd>
            <dt>{t('channels.orderingKey')}</dt>
            <dd className="mono">{cfg.ordering_key ?? '—'}</dd>
            <dt>{t('common.created')}</dt>
            <dd>{fmtDateTime(cfg.created_at_ms)}</dd>
          </dl>
        </div>
      </section>

      {!system && (
      <section className="card tone-critical">
        <div className="card-head">
          <h2>{t('channels.settings.deleteTitle')}</h2>
        </div>
        <div className="card-body danger-row">
          <p className="muted">{t('channels.settings.deleteBody')}</p>
          <GateButton need="admin" className="btn btn-danger" onClick={() => setDeleteOpen(true)}>
            <Trash2 size={14} /> {t('channels.settings.deleteEllipsis')}
          </GateButton>
        </div>
      </section>
      )}

      <ConfirmDialog
        open={deleteOpen}
        title={t('channels.settings.deleteConfirmTitle', { name: cfg.name })}
        description={t('channels.settings.cannotUndo')}
        confirmLabel={t('channels.settings.deleteTitle')}
        danger
        busy={deleting || confirmName !== cfg.name}
        onConfirm={doDelete}
        onClose={() => {
          setDeleteOpen(false);
          setConfirmName('');
        }}
      >
        <div className="field">
          <label htmlFor="del-confirm">
            {t.rich('channels.settings.typeToConfirm', { name: <b className="mono">{cfg.name}</b> })}
          </label>
          <input id="del-confirm" className="input mono" value={confirmName} onChange={(e) => setConfirmName(e.target.value)} autoComplete="off" />
        </div>
      </ConfirmDialog>
    </div>
  );
}

export function SettingsTab({ name }: { name: string }) {
  const t = useT();
  const { data, error, loading, reload } = useAsync((signal) => (signal.aborted ? Promise.reject(new Error('aborted')) : api.getChannel(name)), [name]);
  const [saved, setSaved] = useState<ChannelConfig | null>(null);
  if (loading && !data) return <section className="card"><SkeletonRows rows={6} /></section>;
  if (error && !data) return <InlineError onRetry={reload}>{t('channels.settings.loadError', { error: errorMessage(error) })}</InlineError>;
  const cfg = saved ?? data!;
  return <SettingsForm key={JSON.stringify(cfg)} cfg={cfg} onSaved={setSaved} />;
}

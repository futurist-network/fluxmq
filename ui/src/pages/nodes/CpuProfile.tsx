import { useEffect, useRef, useState } from 'react';
import { Activity, Download, ExternalLink } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { PROFILE_DURATIONS, profileFileName, type ProfileDuration as Duration, type ProfileFormat as Format } from '../../lib/profile';
import { InlineError, Modal, Segmented } from '../../components/ui';
import { useT } from '../../i18n';

type Phase = { kind: 'idle' } | { kind: 'running'; startedAt: number; seconds: number } | { kind: 'done'; url: string; file: string; format: Format };

/** Records a CPU profile of the node serving the console (sampling, the broker keeps running). */
export function CpuProfileDialog({ node, onClose }: { node: string; onClose: () => void }) {
  const t = useT();
  const [duration, setDuration] = useState<Duration>('30');
  const [format, setFormat] = useState<Format>('svg');
  const [phase, setPhase] = useState<Phase>({ kind: 'idle' });
  const [error, setError] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const abort = useRef<AbortController | null>(null);

  // Abort a running request and free the result when the dialog goes away.
  const doneUrl = phase.kind === 'done' ? phase.url : null;
  useEffect(() => () => abort.current?.abort(), []);
  useEffect(() => () => (doneUrl ? URL.revokeObjectURL(doneUrl) : undefined), [doneUrl]);
  useEffect(() => {
    if (phase.kind !== 'running') return;
    const id = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(id);
  }, [phase.kind]);

  const start = async () => {
    const seconds = Number(duration);
    const ctl = new AbortController();
    abort.current = ctl;
    setError(null);
    setNow(Date.now());
    setPhase({ kind: 'running', startedAt: Date.now(), seconds });
    try {
      const body = await api.cpuProfile(seconds, format, ctl.signal);
      const blob = new Blob([body], { type: format === 'svg' ? 'image/svg+xml' : 'text/plain' });
      setPhase({ kind: 'done', url: URL.createObjectURL(blob), file: profileFileName(node, format, new Date()), format });
    } catch (e) {
      if (e instanceof DOMException && e.name === 'AbortError') return;
      setPhase({ kind: 'idle' });
      setError(e instanceof ApiError && e.status === 409 ? t('nodes.profile.busy') : errorMessage(e));
    } finally {
      if (abort.current === ctl) abort.current = null;
    }
  };

  const download = (url: string, file: string) => {
    const a = document.createElement('a');
    a.href = url;
    a.download = file;
    a.click();
  };

  const running = phase.kind === 'running';
  const elapsed = running ? Math.min(phase.seconds, (now - phase.startedAt) / 1000) : 0;
  return (
    <Modal
      open
      onClose={onClose}
      title={t('nodes.profile.title', { id: node })}
      description={t('nodes.profile.body')}
      footer={
        phase.kind === 'done' ? (
          <>
            <button className="btn" onClick={() => setPhase({ kind: 'idle' })}>
              {t('nodes.profile.again')}
            </button>
            {phase.format === 'svg' && (
              <button className="btn" onClick={() => window.open(phase.url, '_blank', 'noopener')}>
                <ExternalLink size={14} /> {t('nodes.profile.open')}
              </button>
            )}
            <button className="btn btn-primary" onClick={() => download(phase.url, phase.file)}>
              <Download size={14} /> {t('nodes.profile.download')}
            </button>
          </>
        ) : (
          <>
            <button className="btn" onClick={onClose}>
              {running ? t('nodes.profile.cancel') : t('common.cancel')}
            </button>
            <button className="btn btn-primary" onClick={start} disabled={running}>
              <Activity size={14} /> {running ? t('nodes.profile.running') : t('nodes.profile.start')}
            </button>
          </>
        )
      }
    >
      {phase.kind === 'done' ? (
        <p className="field-hint">{t(phase.format === 'svg' ? 'nodes.profile.doneSvg' : 'nodes.profile.doneFolded', { file: phase.file })}</p>
      ) : (
        <div className="profile-form">
          <div className="field">
            <span className="field-label">{t('nodes.profile.duration')}</span>
            <Segmented value={duration} onChange={(v) => !running && setDuration(v)} label={t('nodes.profile.duration')} options={PROFILE_DURATIONS.map((d) => ({ value: d, label: t('nodes.profile.seconds', { n: d }) }))} />
          </div>
          <div className="field">
            <span className="field-label">{t('nodes.profile.format')}</span>
            <Segmented
              value={format}
              onChange={(v) => !running && setFormat(v)}
              label={t('nodes.profile.format')}
              options={[
                { value: 'svg', label: t('nodes.profile.svg') },
                { value: 'folded', label: t('nodes.profile.folded') },
              ]}
            />
            <span className="field-hint">{t(format === 'svg' ? 'nodes.profile.svgHint' : 'nodes.profile.foldedHint')}</span>
          </div>
          {running && (
            <div className="profile-progress">
              <div className="meter" role="progressbar" aria-label={t('nodes.profile.running')} aria-valuenow={Math.round(elapsed)} aria-valuemin={0} aria-valuemax={phase.seconds}>
                <span style={{ width: `${(elapsed / phase.seconds) * 100}%` }} />
              </div>
              <span className="field-hint num">{t('nodes.profile.progress', { done: Math.floor(elapsed), total: phase.seconds })}</span>
            </div>
          )}
        </div>
      )}
      {error && <InlineError>{error}</InlineError>}
    </Modal>
  );
}

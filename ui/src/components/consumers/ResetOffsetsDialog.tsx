import { useState } from 'react';
import { TOPICS, type ConsumersState } from '../../api/types';
import { useTelemetry } from '../../store/telemetry';
import { api, errorMessage } from '../../api/client';
import type { StartFrom } from '../../api/types';
import { toast } from '../../store/ui';
import { ConfirmDialog } from '../ui';
import { t as tr, useT, type TKey } from '../../i18n';
import { fmtClock, fmtCount, fmtDateTime } from '../../lib/format';

type Choice = 'earliest' | 'latest' | 'timestamp';

function toLocalInput(ms: number): string {
  const d = new Date(ms - new Date().getTimezoneOffset() * 60_000);
  return d.toISOString().slice(0, 16);
}

/** Lag when the dialog opened (mounted with the dialog body), plus the live value if it moved. */
function GroupSnapshot({ channel, group }: { channel: string; group: string }) {
  const t = useT();
  const lag = useTelemetry((s) => (s.topics[TOPICS.consumers(channel)] as ConsumersState | undefined)?.[group]?.lag);
  const [snap] = useState(() => ({ at: Date.now(), lag }));
  if (snap.lag == null) return null;
  return (
    <p className="confirm-snapshot">
      {t('consumers.reset.snapshot', { time: fmtClock(snap.at, true), lag: fmtCount(snap.lag) })}
      {lag != null && lag !== snap.lag ? ` ${t('consumers.reset.snapshotNow', { lag: fmtCount(lag) })}` : ''}
    </p>
  );
}

export function ResetOffsetsDialog({ channel, group, open, onClose }: { channel: string; group: string; open: boolean; onClose: () => void }) {
  const [choice, setChoice] = useState<Choice>('latest');
  const [ts, setTs] = useState(() => toLocalInput(Date.now() - 3_600_000));
  const [busy, setBusy] = useState(false);
  const t = useT();
  const tsMs = new Date(ts).getTime();

  const submit = async () => {
    const to: StartFrom = choice === 'timestamp' ? { timestamp_ms: tsMs } : choice;
    setBusy(true);
    try {
      await api.resetOffsets(channel, group, to);
      toast('success', tr('consumers.reset.done', { group, to: choice === 'timestamp' ? fmtDateTime(tsMs) : tr(`consumers.reset.${choice}` as TKey).toLowerCase() }));
      onClose();
    } catch (e) {
      toast('error', tr('consumers.reset.failed', { error: errorMessage(e) }));
    } finally {
      setBusy(false);
    }
  };

  const options: { v: Choice; title: string; desc: string }[] = [
    { v: 'latest', title: t('consumers.reset.latest'), desc: t('consumers.reset.latestDesc') },
    { v: 'earliest', title: t('consumers.reset.earliest'), desc: t('consumers.reset.earliestDesc') },
    { v: 'timestamp', title: t('consumers.reset.timestamp'), desc: t('consumers.reset.timestampDesc') },
  ];

  return (
    <ConfirmDialog
      open={open}
      title={t('consumers.reset.title', { group })}
      description={
        <>{t.rich('consumers.reset.body', { group: <b className="mono">{group}</b>, channel: <b className="mono">{channel}</b> })}</>
      }
      confirmLabel={t('consumers.reset.confirm')}
      danger
      busy={busy || (choice === 'timestamp' && !Number.isFinite(tsMs))}
      onConfirm={submit}
      onClose={onClose}
    >
      <GroupSnapshot channel={channel} group={group} />
      <div className="radio-cards" role="radiogroup" aria-label={t('consumers.reset.to')}>
        {options.map((o) => (
          <label key={o.v} className={`radio-card ${choice === o.v ? 'selected' : ''}`}>
            <input type="radio" name="reset" value={o.v} checked={choice === o.v} onChange={() => setChoice(o.v)} />
            <span>
              <b>{o.title}</b>
              <span className="muted">{o.desc}</span>
              {o.v === 'timestamp' && choice === 'timestamp' && (
                <input type="datetime-local" className="input" style={{ marginTop: 8, maxWidth: 240 }} value={ts} onChange={(e) => setTs(e.target.value)} aria-label={t('consumers.reset.tsAria')} />
              )}
            </span>
          </label>
        ))}
      </div>
    </ConfirmDialog>
  );
}

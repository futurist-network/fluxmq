import { Check, Clock, Inbox, Loader, Send, X } from 'lucide-react';
import type { DeliveryAttempt } from '../../api/types';
import { timelineEnd } from '../../lib/timeline';
import { fmtBackoff, fmtMs, fmtTimeMs } from '../../lib/format';
import { t as tr, useT } from '../../i18n';

function failReason(a: DeliveryAttempt): string | null {
  if (a.outcome.kind === 'nacked') return a.outcome.reason || tr('messages.timeline.nacked');
  if (a.outcome.kind === 'timed_out') return tr('messages.timeline.ackTimeout');
  return null;
}

function GroupTimeline({ group, attempts, publishedAt, maxAttempts, deadLettered }: { group: string; attempts: DeliveryAttempt[]; publishedAt: number; maxAttempts: number | null; deadLettered?: boolean }) {
  const t = useT();
  const sorted = [...attempts].sort((a, b) => a.attempt - b.attempt);
  const end = timelineEnd(sorted, maxAttempts, deadLettered);
  const last = sorted[sorted.length - 1];
  const totalMs = last ? (last.finished_at_ms ?? last.delivered_at_ms) - publishedAt : 0;
  const summary =
    end === 'acked'
      ? `${sorted.length > 1 ? t('messages.timeline.ackedAfter', { count: sorted.length }) : t('messages.timeline.ackedFirst')} · ${fmtMs(totalMs)}`
      : end === 'dead'
        ? t('messages.timeline.deadAfter', { count: sorted.length })
        : end === 'in_flight'
          ? t('messages.timeline.inFlightAttempt', { attempt: last?.attempt ?? '' })
          : end === 'retrying'
            ? t('messages.timeline.retryScheduled', { used: `${sorted.length}${maxAttempts ? `/${maxAttempts}` : ''}` })
            : t('messages.timeline.notDelivered');
  return (
    <section className="tl-group" aria-label={t('messages.timeline.groupAria', { group })}>
      <div className="tl-group-head">
        <span className="mono tl-group-name">{group}</span>
        <span className={`badge ${end === 'acked' ? '' : end === 'dead' ? 'badge-critical' : end === 'retrying' ? 'badge-warning' : 'badge-primary'}`}>{summary}</span>
      </div>
      <ol className="tl">
        <li className="tl-step">
          <span className="tl-node neutral">
            <Send size={11} />
          </span>
          <div className="tl-body">
            <span className="tl-title">{t('messages.timeline.published')}</span>
            <span className="tl-time">{fmtTimeMs(publishedAt)}</span>
          </div>
        </li>
        {sorted.map((a, idx) => {
          const prev = sorted[idx - 1];
          const backoff = prev ? a.delivered_at_ms - (prev.finished_at_ms ?? prev.delivered_at_ms) : a.delivered_at_ms - publishedAt;
          const reason = failReason(a);
          const dur = a.finished_at_ms != null ? a.finished_at_ms - a.delivered_at_ms : null;
          const kind = a.outcome.kind;
          return (
            <li key={a.attempt} className="tl-step-wrap">
              {prev && backoff > 0 && (
                <div className="tl-gap">
                  <Clock size={11} /> {t('messages.timeline.backoff', { delay: fmtBackoff(backoff) })}
                </div>
              )}
              <div className="tl-step">
                <span className={`tl-node ${kind === 'acked' ? 'ok' : kind === 'in_flight' ? 'pending' : 'fail'}`}>
                  {kind === 'acked' ? <Check size={11} /> : kind === 'in_flight' ? <Loader size={11} /> : <X size={11} />}
                </span>
                <div className="tl-body">
                  <span className="tl-title">
                    {t('messages.timeline.attempt', { attempt: a.attempt })}
                    {reason && <span className="tl-reason"> ✕ {reason}</span>}
                    {kind === 'acked' && <span className="tl-ok"> {t('messages.timeline.acked')}</span>}
                    {kind === 'in_flight' && <span className="faint"> {t('messages.timeline.inFlight')}</span>}
                  </span>
                  <span className="tl-time">
                    {fmtTimeMs(a.delivered_at_ms)}
                    {dur != null && dur >= 0 && ` · ${fmtMs(dur)}`} · <span className="mono">{a.consumer}</span>
                  </span>
                </div>
              </div>
            </li>
          );
        })}
        {end === 'dead' && (
          <li className="tl-step">
            <span className="tl-node fail">
              <Inbox size={11} />
            </span>
            <div className="tl-body">
              <span className="tl-title text-critical">{t('messages.timeline.movedDead')}</span>
            </div>
          </li>
        )}
      </ol>
    </section>
  );
}

/** Per-consumer-group retry timeline from `MessageDetail.deliveries` / `DeadLetterView.attempts`. */
export function RetryTimeline({
  deliveries,
  publishedAt,
  maxAttempts,
  deadGroup,
}: {
  deliveries: Record<string, DeliveryAttempt[]>;
  publishedAt: number;
  maxAttempts: number | null;
  /** Group known to have dead-lettered this message (DLQ view). */
  deadGroup?: string;
}) {
  const t = useT();
  const groups = Object.keys(deliveries).sort();
  if (!groups.length) return <p className="muted" style={{ fontSize: 13 }}>{t('messages.timeline.none')}</p>;
  return (
    <div className="tl-groups">
      {groups.map((g) => (
        <GroupTimeline key={g} group={g} attempts={deliveries[g]!} publishedAt={publishedAt} maxAttempts={maxAttempts} deadLettered={g === deadGroup} />
      ))}
    </div>
  );
}

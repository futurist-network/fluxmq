import { useState } from 'react';
import { Copy, RotateCcw, X } from 'lucide-react';
import { api, errorMessage } from '../../api/client';
import { HEADERS, TOPICS, type ChannelsSummary, type DeliveryAttempt, type JsonPayload } from '../../api/types';
import { copyText } from '../../lib/hooks';
import { decodePayload } from '../../lib/payload';
import { contentTypeOf } from '../../lib/payloadFormat';
import { fmtBytes, fmtInt, fmtTimestampFull } from '../../lib/format';
import { toast } from '../../store/ui';
import { useTopicSelect } from '../../telemetry';
import { channelNames } from '../../store/selectors';
import { Modal, Skeleton } from '../ui';
import { PayloadView } from './PayloadView';
import { RetryTimeline } from './RetryTimeline';
import { GateButton } from '../auth/Gate';
import { useT } from '../../i18n';

export interface InspectorMessage {
  partition: number;
  offset: number;
  key: string | null;
  headers: Record<string, string>;
  timestamp_ms: number;
  size: number;
  payload: JsonPayload | string;
  truncated?: boolean;
}

function ReplayDialog({ channel, ids, open, onClose }: { channel: string; ids: { partition: number; offset: number }[]; open: boolean; onClose: () => void }) {
  const t = useT();
  const names = useTopicSelect<ChannelsSummary, string[]>(TOPICS.CHANNELS, channelNames);
  const [target, setTarget] = useState(channel);
  const [busy, setBusy] = useState(false);
  const submit = async () => {
    setBusy(true);
    try {
      const res = await api.replay(channel, { ids, target_channel: target === channel ? null : target });
      const first = res.ids[0];
      toast('success', t('messages.replay.done', { count: res.ids.length, target, pos: first ? ` (p${first.partition}:${first.offset})` : '' }));
      onClose();
    } catch (e) {
      toast('error', t('messages.replay.failed', { error: errorMessage(e) }));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal
      open={open}
      onClose={onClose}
      title={t('messages.replay.title')}
      description={t('messages.replay.desc')}
      footer={
        <>
          <button className="btn" onClick={onClose}>
            {t('common.cancel')}
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={busy}>
            <RotateCcw size={14} /> {busy ? t('messages.replay.busy') : t('messages.replay.to', { target })}
          </button>
        </>
      }
    >
      <div className="field">
        <label htmlFor="replay-target">{t('messages.replay.target')}</label>
        <select id="replay-target" className="select" value={target} onChange={(e) => setTarget(e.target.value)} data-autofocus>
          {[channel, ...names.filter((n) => n !== channel)].map((n) => (
            <option key={n} value={n}>
              {n === channel ? t('messages.replay.same', { name: n }) : n}
            </option>
          ))}
        </select>
      </div>
    </Modal>
  );
}

function Meta({ label, children, mono }: { label: string; children: React.ReactNode; mono?: boolean }) {
  return (
    <div className="meta-item">
      <dt>{label}</dt>
      <dd className={mono ? 'mono' : 'num'}>{children ?? <span className="faint">—</span>}</dd>
    </div>
  );
}

/**
 * DevTools-style message inspector: title from `flux-type`, metadata, payload, headers,
 * actions and per-group retry timeline.
 */
export function MessageInspector({
  channel,
  message,
  deliveries,
  deliveriesLoading,
  maxAttempts,
  deadGroup,
  onClose,
  extraActions,
  replay = true,
}: {
  channel: string;
  message: InspectorMessage | null;
  deliveries: Record<string, DeliveryAttempt[]> | null;
  deliveriesLoading?: boolean;
  maxAttempts: number | null;
  deadGroup?: string;
  onClose?: () => void;
  extraActions?: React.ReactNode;
  replay?: boolean;
}) {
  const t = useT();
  const [replayOpen, setReplayOpen] = useState(false);
  if (!message) return null;
  const m = message;
  const type = m.headers[HEADERS.EVENT_TYPE];
  const producer = m.headers[HEADERS.PRODUCER];
  const trace = m.headers[HEADERS.TRACE_ID];
  const otherHeaders = Object.entries(m.headers).filter(([k]) => ![HEADERS.EVENT_TYPE, HEADERS.PRODUCER, HEADERS.TRACE_ID].includes(k as never));

  const copyPayload = async () => {
    const d = decodePayload(m.payload);
    const text = d.isJson ? JSON.stringify(d.json, null, 2) : (d.text ?? (typeof m.payload === 'string' ? m.payload : m.payload.data));
    toast((await copyText(text)) ? 'success' : 'error', t('messages.inspector.payloadCopied'));
  };
  const copyJson = async () => {
    const d = decodePayload(m.payload);
    const obj = { channel, partition: m.partition, offset: m.offset, key: m.key, timestamp_ms: m.timestamp_ms, headers: m.headers, payload: d.isJson ? d.json : (d.text ?? m.payload) };
    toast((await copyText(JSON.stringify(obj, null, 2))) ? 'success' : 'error', t('messages.inspector.jsonCopied'));
  };

  return (
    <div className="inspector" aria-label={t('messages.inspector.aria')}>
      <div className="insp-head">
        <div style={{ minWidth: 0, flex: 1 }}>
          <div className="insp-title truncate">{type ?? <span className="muted">{t('messages.list.message')}</span>}</div>
          <div className="insp-sub">
            <span className="mono">
              p{m.partition}:{fmtInt(m.offset)}
            </span>
            {m.key && (
              <>
                <span className="faint">·</span>
                <span className="mono truncate">{t('messages.inspector.keyLabel', { key: m.key })}</span>
              </>
            )}
          </div>
        </div>
        {onClose && (
          <button className="btn btn-ghost btn-icon btn-sm" onClick={onClose} aria-label={t('messages.inspector.close')}>
            <X size={16} />
          </button>
        )}
      </div>

      <div className="insp-actions">
        <button className="btn btn-sm" onClick={copyPayload}>
          <Copy size={13} /> {t('messages.inspector.copyPayload')}
        </button>
        <button className="btn btn-sm" onClick={copyJson}>
          <Copy size={13} /> {t('messages.inspector.copyJson')}
        </button>
        {replay && (
          <GateButton need="operate" className="btn btn-sm" onClick={() => setReplayOpen(true)}>
            <RotateCcw size={13} /> {t('messages.inspector.replay')}
          </GateButton>
        )}
        {extraActions}
      </div>

      <div className="insp-scroll">
        <dl className="meta-grid">
          <Meta label={t('messages.inspector.timestamp')}>{fmtTimestampFull(m.timestamp_ms)}</Meta>
          <Meta label={t('common.size')}>{fmtBytes(m.size)}</Meta>
          <Meta label={t('common.partition')}>{m.partition}</Meta>
          <Meta label={t('messages.inspector.offset')}>{fmtInt(m.offset)}</Meta>
          <Meta label={t('messages.inspector.producer')} mono>
            {producer}
          </Meta>
          <Meta label={t('messages.inspector.trace')} mono>
            {trace ? (
              <button className="trace-btn mono" title={t('messages.inspector.copyTrace')} onClick={() => copyText(trace).then(() => toast('success', t('messages.inspector.traceCopied')))}>
                {trace}
              </button>
            ) : null}
          </Meta>
        </dl>

        <section className="insp-section">
          <h3 className="drawer-section">{t('messages.inspector.payload')}</h3>
          <PayloadView key={`${m.partition}:${m.offset}`} payload={m.payload} truncated={m.truncated} contentType={contentTypeOf(m.headers)} />
        </section>

        {otherHeaders.length > 0 && (
          <section className="insp-section">
            <h3 className="drawer-section">{t('messages.inspector.headers')}</h3>
            <dl className="kv headers-kv">
              {otherHeaders.map(([k, v]) => (
                <div key={k} style={{ display: 'contents' }}>
                  <dt className="mono">{k}</dt>
                  <dd className="mono">{v}</dd>
                </div>
              ))}
            </dl>
          </section>
        )}

        <section className="insp-section">
          <h3 className="drawer-section">{t('messages.inspector.timeline')}</h3>
          {deliveriesLoading && !deliveries ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <Skeleton h={18} w="60%" />
              <Skeleton h={18} w="80%" />
              <Skeleton h={18} w="50%" />
            </div>
          ) : deliveries ? (
            <RetryTimeline deliveries={deliveries} publishedAt={m.timestamp_ms} maxAttempts={maxAttempts} deadGroup={deadGroup} />
          ) : (
            <p className="faint" style={{ fontSize: 13 }}>
              {t('messages.inspector.unavailable')}
            </p>
          )}
        </section>
      </div>
      {replay && <ReplayDialog channel={channel} ids={[{ partition: m.partition, offset: m.offset }]} open={replayOpen} onClose={() => setReplayOpen(false)} />}
    </div>
  );
}

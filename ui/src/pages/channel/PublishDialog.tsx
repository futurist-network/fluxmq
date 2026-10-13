import { useState } from 'react';
import { Clock, Send } from 'lucide-react';
import { api, errorMessage } from '../../api/client';
import { HEADERS, type PublishItem } from '../../api/types';
import { fmtDateTime } from '../../lib/format';
import { DEFAULT_WHEN, DELAY_UNITS, resolveWhen, toLocalInput, type When, type WhenKind } from '../../lib/schedule';
import { toast } from '../../store/ui';
import { InlineError, Modal, Segmented } from '../../components/ui';
import { useT, type TKey } from '../../i18n';

type Format = 'json' | 'text' | 'xml';
/** Header carrying a message's level on a priority queue (technical, not translated). */
const PRIORITY_HEADER = 'flux-priority';
const CONTENT_TYPE: Record<Format, string> = { json: 'application/json', text: 'text/plain; charset=utf-8', xml: 'application/xml' };
/** Placeholder: an event type is a technical identifier (not translated). */
const TYPE_EXAMPLE = 'PaymentReminder';
const SAMPLE: Record<Format, string> = { json: '{\n  "order_id": 8842,\n  "status": "reminder"\n}', text: 'Your order 8842 is waiting for payment', xml: '<reminder><order>8842</order></reminder>' };

/** Operator: publish one message to the channel now or later (v0.12 delayed delivery). */
/** `levels`: priority levels when the channel is a priority queue (adds a priority picker). */
export function PublishDialog({ channel, open, onClose, levels }: { channel: string; open: boolean; onClose: () => void; levels?: number }) {
  const t = useT();
  const [key, setKey] = useState('');
  const [type, setType] = useState('');
  const [priority, setPriority] = useState(0);
  const [format, setFormat] = useState<Format>('json');
  const [payload, setPayload] = useState(SAMPLE.json);
  const [when, setWhen] = useState<When>(() => ({ ...DEFAULT_WHEN, at: toLocalInput(Date.now() + 3_600_000) }));
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const set = (w: Partial<When>) => setWhen((cur) => ({ ...cur, ...w }));

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    if (format === 'json') {
      try {
        JSON.parse(payload);
      } catch (err) {
        setError(t('scheduled.publish.err.json', { error: err instanceof Error ? err.message : String(err) }));
        return;
      }
    }
    const w = resolveWhen(when, Date.now());
    if (!w.ok) {
      setError(w.error);
      return;
    }
    const headers: Record<string, string> = { [HEADERS.CONTENT_TYPE]: CONTENT_TYPE[format] };
    if (type.trim()) headers[HEADERS.EVENT_TYPE] = type.trim();
    if (levels) headers[PRIORITY_HEADER] = String(priority);
    const item: PublishItem = { key: key.trim() || null, headers, payload: { encoding: 'utf8', data: payload }, ...w.fields };
    setBusy(true);
    try {
      await api.publish(channel, { messages: [item] });
      toast('success', w.dueMs ? t('scheduled.publish.scheduledToast', { channel, when: fmtDateTime(w.dueMs) }) : t('scheduled.publish.sentToast', { channel }));
      onClose();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={onClose}
      wide
      title={t('scheduled.publish.title', { channel })}
      description={t('scheduled.publish.desc')}
      footer={
        <>
          <button className="btn" type="button" onClick={onClose}>
            {t('common.cancel')}
          </button>
          <button className="btn btn-primary" type="submit" form="publish-form" disabled={busy}>
            {when.kind === 'now' ? <Send size={14} /> : <Clock size={14} />} {when.kind === 'now' ? t('scheduled.publish.send') : t('scheduled.publish.schedule')}
          </button>
        </>
      }
    >
      <form id="publish-form" className="publish-form" onSubmit={submit}>
        <div className="publish-row">
          <div className="field">
            <label htmlFor="pub-key">{t('scheduled.publish.key')}</label>
            <input id="pub-key" className="input mono" value={key} onChange={(e) => setKey(e.target.value)} placeholder="ord-8842" autoComplete="off" />
          </div>
          <div className="field">
            <label htmlFor="pub-type">{t('scheduled.publish.type')}</label>
            <input id="pub-type" className="input" value={type} onChange={(e) => setType(e.target.value)} placeholder={TYPE_EXAMPLE} autoComplete="off" />
          </div>
        </div>
        {levels != null && (
          <div className="field">
            <label htmlFor="pub-prio">{t('scheduled.publish.priority')}</label>
            <select id="pub-prio" className="input publish-prio" value={priority} onChange={(e) => setPriority(Number(e.target.value))}>
              {Array.from({ length: levels }, (_, i) => levels - 1 - i).map((l) => (
                <option key={l} value={l}>
                  {l === levels - 1 ? t('scheduled.publish.priorityTop', { n: l }) : l === 0 ? t('scheduled.publish.priorityLow', { n: l }) : String(l)}
                </option>
              ))}
            </select>
          </div>
        )}
        <div className="field">
          <span className="field-label">{t('scheduled.publish.payload')}</span>
          <Segmented
            value={format}
            onChange={(f) => {
              if (payload === SAMPLE[format]) setPayload(SAMPLE[f]);
              setFormat(f);
            }}
            label={t('scheduled.publish.format')}
            options={[
              { value: 'json', label: 'JSON' },
              { value: 'text', label: t('scheduled.publish.formatText') },
              { value: 'xml', label: 'XML' },
            ]}
          />
          <textarea className="input mono publish-payload" value={payload} onChange={(e) => setPayload(e.target.value)} rows={7} spellCheck={false} aria-label={t('scheduled.publish.payload')} />
        </div>
        <div className="field">
          <span className="field-label">{t('scheduled.publish.when')}</span>
          <Segmented
            value={when.kind}
            onChange={(kind: WhenKind) => set({ kind })}
            label={t('scheduled.publish.when')}
            options={[
              { value: 'now', label: t('scheduled.publish.whenNow') },
              { value: 'in', label: t('scheduled.publish.whenIn') },
              { value: 'at', label: t('scheduled.publish.whenAt') },
            ]}
          />
          {when.kind === 'in' && (
            <div className="publish-delay">
              <input className="input num" type="number" min={1} value={when.amount} onChange={(e) => set({ amount: e.target.value })} aria-label={t('scheduled.publish.amount')} />
              <select className="input" value={when.unit} onChange={(e) => set({ unit: e.target.value as When['unit'] })} aria-label={t('scheduled.publish.unit')}>
                {DELAY_UNITS.map((u) => (
                  <option key={u} value={u}>
                    {t(`scheduled.publish.unit.${u}` as TKey)}
                  </option>
                ))}
              </select>
            </div>
          )}
          {when.kind === 'at' && <input className="input publish-at" type="datetime-local" value={when.at} onChange={(e) => set({ at: e.target.value })} aria-label={t('scheduled.publish.whenAt')} />}
          <span className="field-hint">{when.kind === 'now' ? t('scheduled.publish.hintNow') : t('scheduled.publish.hintLater')}</span>
        </div>
        {error && <InlineError>{error}</InlineError>}
      </form>
    </Modal>
  );
}

import { useCallback, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router';
import { KeyRound, Play, Radio, Square } from 'lucide-react';
import type { LiveEnvelope } from '../../api/types';
import { getToken } from '../../lib/settings';
import { fmtBytes, fmtClock, fmtCount } from '../../lib/format';
import { closeReason, liveQuery, livePath, liveSnippet, liveSubprotocols, parseLiveEvent, payloadPreview, SNIPPET_KINDS, ssePath, wsOrigin, type LiveOptions, type SnippetKind } from '../../lib/live';
import { CodeBlock } from '../../components/forms';
import { FormatChip } from '../../components/messages/FormatChip';
import { payloadFormat } from '../../lib/payloadFormat';
import { EmptyState, Segmented } from '../../components/ui';
import { t as tr, useT, type TKey } from '../../i18n';
import './live.css';

/** Messages kept on screen; older ones scroll out (the counter keeps counting). */
const KEPT = 100;
/** Incoming messages are rendered at most this often (a busy channel sends hundreds per second). */
const FLUSH_MS = 250;

type Row = { id: number; at: number } & ({ kind: 'msg'; m: LiveEnvelope } | { kind: 'note'; text: string; tone?: 'warning' | 'critical' });

type Status = 'idle' | 'connecting' | 'open' | 'closed';

/** Why a WebSocket handshake failed: browsers hide the HTTP status, so ask the SSE endpoint (same checks). */
async function diagnose(channel: string, o: LiveOptions): Promise<string> {
  const ctrl = new AbortController();
  const token = getToken();
  try {
    const res = await fetch(`${ssePath(channel)}${liveQuery(o)}`, { headers: token ? { Authorization: `Bearer ${token}` } : {}, signal: ctrl.signal });
    if (res.ok) return tr('live.try.failedUnknown');
    let message = `HTTP ${res.status}`;
    try {
      const b = (await res.json()) as { message?: string };
      if (b.message) message = `HTTP ${res.status}: ${b.message}`;
    } catch {
      /* not JSON */
    }
    return message;
  } catch {
    return tr('live.try.failedUnknown');
  } finally {
    ctrl.abort();
  }
}

/** A live subscription opened by the console itself (WebSocket, the console's own token). */
function useLiveSocket(channel: string) {
  const [status, setStatus] = useState<Status>('idle');
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<Row[]>([]);
  const [count, setCount] = useState({ messages: 0, bytes: 0 });
  const ws = useRef<WebSocket | null>(null);
  const pending = useRef<{ rows: Row[]; messages: number; bytes: number }>({ rows: [], messages: 0, bytes: 0 });
  const seq = useRef(0);

  const flush = useCallback(() => {
    const p = pending.current;
    if (!p.rows.length && !p.messages) return;
    pending.current = { rows: [], messages: 0, bytes: 0 };
    setRows((cur) => [...p.rows.reverse(), ...cur].slice(0, KEPT));
    setCount((c) => ({ messages: c.messages + p.messages, bytes: c.bytes + p.bytes }));
  }, []);

  useEffect(() => {
    if (status !== 'open') return;
    const id = window.setInterval(flush, FLUSH_MS);
    return () => window.clearInterval(id);
  }, [status, flush]);

  const stop = useCallback(() => {
    const s = ws.current;
    ws.current = null;
    if (s && s.readyState <= WebSocket.OPEN) s.close(1000);
    flush();
    setStatus((st) => (st === 'idle' ? st : 'closed'));
  }, [flush]);

  const start = useCallback(
    (o: LiveOptions) => {
      stop();
      setRows([]);
      setCount({ messages: 0, bytes: 0 });
      setError(null);
      setStatus('connecting');
      const note = (text: string, tone?: 'warning' | 'critical') => pending.current.rows.push({ id: ++seq.current, at: Date.now(), kind: 'note', text, tone });
      let opened = false;
      const s = new WebSocket(`${wsOrigin(location.origin)}${livePath(channel)}${liveQuery(o)}`, liveSubprotocols(getToken()));
      ws.current = s;
      s.onmessage = (e) => {
        if (ws.current !== s || typeof e.data !== 'string') return;
        const ev = parseLiveEvent(e.data);
        if (!ev) return;
        if (ev.type === 'hello') {
          opened = true;
          setStatus('open');
          note(tr(o.from === 'earliest' ? 'live.try.helloEarliest' : 'live.try.helloLatest', { channel }));
        } else if (ev.type === 'messages' && ev.messages) {
          for (const m of ev.messages) {
            pending.current.rows.push({ id: ++seq.current, at: Date.now(), kind: 'msg', m });
            pending.current.bytes += m.size;
          }
          pending.current.messages += ev.messages.length;
          if (pending.current.rows.length > KEPT) pending.current.rows.splice(0, pending.current.rows.length - KEPT);
        } else if (ev.type === 'gap') {
          note(tr('live.try.gap', { partition: ev.partition ?? 0, lost: ev.lost ?? 0 }), 'warning');
        } else if (ev.type === 'reset') {
          note(tr('live.try.reset', { partition: ev.partition ?? 0 }), 'warning');
        } else if (ev.type === 'unavailable') {
          note(tr('live.try.unavailable'), 'warning');
        } else if (ev.type === 'error') {
          setError(ev.message ?? ev.code ?? tr('live.try.failedUnknown'));
        }
      };
      s.onclose = (e) => {
        if (ws.current !== s) return; // stopped by the user
        ws.current = null;
        flush();
        setStatus('closed');
        if (!opened) {
          void diagnose(channel, o).then((m) => setError(tr('live.try.failed', { reason: m })));
          return;
        }
        if (e.code !== 1000) setError(closeReason(e.code) ?? tr('live.try.closedCode', { code: e.code }));
      };
    },
    [channel, flush, stop],
  );

  useEffect(
    () => () => {
      const s = ws.current;
      ws.current = null;
      s?.close(1000);
    },
    [],
  );
  useEffect(() => stop, [channel, stop]);

  return { status, error, rows, count, start, stop };
}

/** Subscription options shared by the try-it panel and the code snippets. */
export function LiveOptionsForm({ value, onChange, disabled }: { value: LiveOptions; onChange: (o: LiveOptions) => void; disabled?: boolean }) {
  const t = useT();
  return (
    <div className="live-options">
      <div className="field">
        <span className="field-label">{t('live.opt.from')}</span>
        <Segmented
          value={value.from}
          onChange={(from) => onChange({ ...value, from })}
          label={t('live.opt.from')}
          options={[
            { value: 'latest', label: t('live.opt.fromLatest') },
            { value: 'earliest', label: t('live.opt.fromEarliest') },
          ]}
        />
      </div>
      <div className="field">
        <label htmlFor="live-key">{t('live.opt.key')}</label>
        <input id="live-key" className="input mono" value={value.key} disabled={disabled} onChange={(e) => onChange({ ...value, key: e.target.value })} placeholder="user_42, user_43" autoComplete="off" />
      </div>
      <div className="field live-filter">
        <label htmlFor="live-filter">{t('live.opt.filter')}</label>
        <input id="live-filter" className="input mono" value={value.filter} disabled={disabled} onChange={(e) => onChange({ ...value, filter: e.target.value })} placeholder="amount >= 100" autoComplete="off" />
      </div>
    </div>
  );
}

/** Opens a real subscription from the console and shows what arrives. */
export function LiveTryCard({ channel, options }: { channel: string; options: LiveOptions }) {
  const t = useT();
  const s = useLiveSocket(channel);
  const running = s.status === 'connecting' || s.status === 'open';
  return (
    <section className="card live-try">
      <div className="card-head">
        <h2>{t('live.try.title')}</h2>
        <span className={`live-status live-status-${s.status}`}>
          <Radio size={13} aria-hidden /> {t(`live.try.status.${s.status}` as TKey)}
        </span>
        <span className="spacer" />
        {running ? (
          <button type="button" className="btn btn-sm" onClick={s.stop}>
            <Square size={13} /> {t('live.try.stop')}
          </button>
        ) : (
          <button type="button" className="btn btn-sm btn-primary" onClick={() => s.start(options)}>
            <Play size={13} /> {s.status === 'idle' ? t('live.try.start') : t('live.try.restart')}
          </button>
        )}
      </div>
      <div className="card-body">
        <p className="faint live-try-note">{t('live.try.note')}</p>
        {s.error && (
          <div className="live-error" role="alert">
            {s.error}
          </div>
        )}
        {s.status !== 'idle' && (
          <div className="live-count faint">
            {t('live.try.received', { count: s.count.messages, n: fmtCount(s.count.messages), bytes: fmtBytes(s.count.bytes) })}
            {s.count.messages > KEPT ? ` · ${t('live.try.keptNote', { n: KEPT })}` : ''}
          </div>
        )}
        {s.status === 'idle' ? (
          <EmptyState compact icon={<Radio size={18} />} title={t('live.try.idleTitle')}>
            {t('live.try.idleBody')}
          </EmptyState>
        ) : s.rows.length === 0 ? (
          s.status === 'open' && <p className="faint live-waiting">{t('live.try.waiting')}</p>
        ) : (
          <ol className="live-rows" aria-live="off">
            {s.rows.map((r) =>
              r.kind === 'note' ? (
                <li key={r.id} className={`live-note ${r.tone ? `text-${r.tone}` : 'faint'}`}>
                  <span className="live-at mono">{fmtClock(r.at, true)}</span> {r.text}
                </li>
              ) : (
                <li key={r.id} className="live-msg">
                  <span className="live-at mono" title={new Date(r.m.timestamp).toISOString()}>
                    {fmtClock(r.m.timestamp, true)}
                  </span>
                  <span className="live-pos mono">
                    p{r.m.partition}:{r.m.offset}
                  </span>
                  <FormatChip format={payloadFormat(r.m.encoding === 'base64' ? { encoding: 'base64', data: r.m.payload } : r.m.payload, r.m.headers)} />
                  {r.m.key != null && <span className="chip static live-key">{r.m.key}</span>}
                  <span className="live-payload mono truncate">{payloadPreview(r.m)}</span>
                  {r.m.truncated && <span className="badge badge-warning">{t('live.try.truncated', { size: fmtBytes(r.m.size) })}</span>}
                </li>
              ),
            )}
          </ol>
        )}
      </div>
    </section>
  );
}

const SNIPPET_LABEL: Record<SnippetKind, TKey> = { sse: 'live.snippet.sse', ws: 'live.snippet.ws', curl: 'live.snippet.curl' };

/** Ready-to-paste client code for this channel and options. */
export function LiveSnippetCard({ channel, options, canCreateToken }: { channel: string; options: LiveOptions; canCreateToken: boolean }) {
  const t = useT();
  const [kind, setKind] = useState<SnippetKind>('sse');
  const code = liveSnippet(kind, location.origin, channel, options);
  return (
    <section className="card live-code">
      <div className="card-head">
        <h2>{t('live.snippet.title')}</h2>
        <span className="spacer" />
        <Segmented value={kind} onChange={setKind} label={t('live.snippet.title')} options={SNIPPET_KINDS.map((k) => ({ value: k, label: t(SNIPPET_LABEL[k]) }))} />
      </div>
      <div className="card-body">
        <CodeBlock code={code} />
        <p className="faint live-code-note">{t(`live.snippet.note.${kind}` as TKey)}</p>
        <div className="live-token">
          <KeyRound size={14} aria-hidden />
          <span>{t('live.snippet.token', { channel })}</span>
          {canCreateToken && (
            <Link className="btn btn-sm" to={`/settings/tokens?create=subscriber&channel=${encodeURIComponent(channel)}`}>
              {t('live.snippet.createToken')}
            </Link>
          )}
        </div>
      </div>
    </section>
  );
}

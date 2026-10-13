import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { FormatChip } from '../../components/messages/FormatChip';
import { payloadFormat } from '../../lib/payloadFormat';
import type { PayloadFormat } from '../../lib/payloadFormat';
import { useNavigate, useSearchParams } from 'react-router';
import { useVirtualizer } from '@tanstack/react-virtual';
import { Pause, Play, Radio, Route as RouteIcon, Search, Trash2, X } from 'lucide-react';
import { api, ApiError, errorMessage } from '../../api/client';
import { HEADERS, TOPICS, type ChannelDetail, type MessageDetail, type MessagePage, type MessageQuery, type MessageView, type TailMessage, type TailState } from '../../api/types';
import { useTopicData, useTopicSelect } from '../../telemetry';
import { fmtBytes, fmtCount, fmtInt, fmtNumber, fmtRate, fmtTimeMs } from '../../lib/format';
import { useMediaQuery } from '../../lib/hooks';
import { EmptyState, InlineError, Segmented, Skeleton } from '../../components/ui';
import { MessageInspector, type InspectorMessage } from '../../components/messages/MessageInspector';
import { FilterInput } from '../../components/messages/FilterInput';
import { useFilterParse, type FilterCheck } from '../../lib/useFilterParse';
import { filterErrorInfo, type FilterErrorInfo } from '../../lib/filterError';
import { suggestFields } from '../../lib/routeRows';
import { GateButton } from '../../components/auth/Gate';
import './messages.css';
import { useT } from '../../i18n';

type RangeKey = 'latest' | '15m' | '1h' | '24h' | 'custom';
const RANGE_MS: Record<'15m' | '1h' | '24h', number> = { '15m': 15 * 60_000, '1h': 3_600_000, '24h': 86_400_000 };
const PAGE = 200;

interface Row {
  id: string;
  partition: number;
  offset: number;
  key: string | null;
  timestamp_ms: number;
  size: number;
  type: string | undefined;
  format: PayloadFormat;
  msg: InspectorMessage;
}

function rowFromView(m: MessageView): Row {
  return { id: `${m.partition}:${m.offset}`, partition: m.partition, offset: m.offset, key: m.key, timestamp_ms: m.timestamp_ms, size: m.size, type: m.headers[HEADERS.EVENT_TYPE], format: payloadFormat(m.payload, m.headers), msg: m };
}

function rowFromTail(m: TailMessage): Row {
  return {
    id: `${m.partition}:${m.offset}`,
    partition: m.partition,
    offset: m.offset,
    key: m.key,
    timestamp_ms: m.timestamp_ms,
    size: m.size,
    type: m.headers[HEADERS.EVENT_TYPE],
    format: payloadFormat(m.payload_preview, m.headers),
    msg: { ...m, payload: m.payload_preview, truncated: m.truncated },
  };
}

function toLocalInput(ms: number): string {
  return new Date(ms - new Date().getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
}

/** Virtualized, keyboard-navigable message list. */
function MessageList({ rows, selected, onSelect, footer, live }: { rows: Row[]; selected: string | null; onSelect: (r: Row) => void; footer?: React.ReactNode; live?: boolean }) {
  const t = useT();
  const parentRef = useRef<HTMLDivElement>(null);
  // TanStack Virtual returns unstable functions; the React compiler skips this component.
  // eslint-disable-next-line react-hooks/incompatible-library
  const v = useVirtualizer({ count: rows.length, getScrollElement: () => parentRef.current, estimateSize: () => 54, overscan: 12 });
  const selIdx = selected ? rows.findIndex((r) => r.id === selected) : -1;

  const onKey = (e: React.KeyboardEvent) => {
    if (!rows.length) return;
    const moves: Record<string, number> = {
      ArrowDown: Math.min(rows.length - 1, selIdx + 1),
      j: Math.min(rows.length - 1, selIdx + 1),
      ArrowUp: Math.max(0, selIdx - 1),
      k: Math.max(0, selIdx - 1),
      Home: 0,
      End: rows.length - 1,
    };
    const next = moves[e.key];
    if (next == null) return;
    e.preventDefault();
    const r = rows[next];
    if (r) {
      onSelect(r);
      v.scrollToIndex(next, { align: 'auto' });
    }
  };

  return (
    <div
      ref={parentRef}
      className="msg-list"
      role="listbox"
      tabIndex={0}
      aria-label={live ? t('messages.list.liveAria') : t('messages.list.aria')}
      aria-activedescendant={selected ? `msg-${selected}` : undefined}
      onKeyDown={onKey}
    >
      <div style={{ height: v.getTotalSize(), position: 'relative' }}>
        {v.getVirtualItems().map((it) => {
          const r = rows[it.index]!;
          const isSel = r.id === selected;
          return (
            <div
              key={r.id}
              id={`msg-${r.id}`}
              role="option"
              aria-selected={isSel}
              className={`msg-row ${isSel ? 'selected' : ''}`}
              style={{ transform: `translateY(${it.start}px)`, height: it.size }}
              onClick={() => onSelect(r)}
            >
              <span className="msg-time">{fmtTimeMs(r.timestamp_ms)}</span>
              <span className="msg-main">
                <span className="msg-type">{r.type ?? <span className="faint">{t('messages.list.message')}</span>}</span>
                <span className="msg-key">{r.key ? t.rich('messages.list.key', { key: <span className="mono">{r.key}</span> }) : <span className="faint">{t('messages.list.noKey')}</span>}</span>
              </span>
              <span className="msg-pos">
                <span className="mono">
                  <FormatChip format={r.format} /> p{r.partition}:{fmtInt(r.offset)}
                </span>
                <span className="faint">{fmtBytes(r.size)}</span>
              </span>
            </div>
          );
        })}
      </div>
      {footer}
    </div>
  );
}

function useMaxAttempts(name: string): number | null {
  return useTopicSelect<ChannelDetail, number | null>(TOPICS.channel(name), (d) => d?.config.retry.max_attempts ?? null);
}

/** Inspector pane: fetches `MessageDetail` (full payload + deliveries) for the selected row. */
function InspectorPane({ name, row, onClose, asDrawer }: { name: string; row: Row | null; onClose: () => void; asDrawer: boolean }) {
  const [detail, setDetail] = useState<{ id: string; d: MessageDetail | null; error?: string } | null>(null);
  const t = useT();
  const maxAttempts = useMaxAttempts(name);
  const id = row?.id ?? null;

  useEffect(() => {
    if (!row) return;
    const ctrl = new AbortController();
    const timer = setTimeout(() => {
      api
        .message(name, row.partition, row.offset, ctrl.signal)
        .then((d) => setDetail({ id: row.id, d }))
        .catch((e: unknown) => {
          if (!ctrl.signal.aborted) setDetail({ id: row.id, d: null, error: errorMessage(e) });
        });
    }, 60); // debounce fast keyboard scrubbing
    return () => {
      clearTimeout(timer);
      ctrl.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name, id]);

  if (!row) {
    return (
      <aside className="msg-inspector empty-insp" aria-label={t('messages.inspector.aria')}>
        <EmptyState compact icon={<Search size={18} />} title={t('messages.inspector.select')}>
          {t('messages.inspector.selectBody')}
        </EmptyState>
      </aside>
    );
  }
  const d = detail?.id === row.id ? detail : null;
  const msg: InspectorMessage = d?.d ? d.d.message : row.msg;
  return (
    <aside className={`msg-inspector ${asDrawer ? 'as-drawer' : ''}`}>
      {d?.error && (
        <div style={{ padding: '12px 16px 0' }}>
          <InlineError>{t('messages.inspector.fullUnavailable', { error: d.error })}</InlineError>
        </div>
      )}
      <MessageInspector channel={name} message={msg} deliveries={d?.d?.deliveries ?? null} deliveriesLoading={!d} maxAttempts={maxAttempts} onClose={onClose} />
    </aside>
  );
}

/** Filter examples: a few generic ones plus fields seen in the current results. */
function filterExamples(rows: Row[]): string[] {
  const views = rows.slice(0, 100).map((r) => r.msg).filter((m): m is MessageView => typeof m.payload === 'object');
  const sug = suggestFields(views);
  const out: string[] = [];
  const typeH = sug.find((s) => s.field === `header.${HEADERS.EVENT_TYPE}`);
  if (typeH?.examples[0]) out.push(`header.${HEADERS.EVENT_TYPE} = "${typeH.examples[0]}"`);
  const num = sug.find((s) => s.type === 'number' && !s.field.startsWith('header.') && !s.field.includes('.0.'));
  const str = sug.filter((s) => s.type === 'string' && !s.field.startsWith('header.') && s.examples.length > 1 && s.examples.length < 6 && !s.examples.some((x) => /\d{4}-\d\d-\d\dT/.test(x)));
  if (num?.examples[0]) out.push(`${num.field} >= ${num.examples[0]}`);
  if (str[0]?.examples[0]) out.push(`${str[0].field} = "${str[0].examples[0]}"`);
  if (str[1] && str[1].examples.length >= 2) out.push(`${str[1].field} in ["${str[1].examples[0]}", "${str[1].examples[1]}"]`);
  if (num && str[0]?.examples[0]) out.push(`${str[0].field} = "${str[0].examples[0]}" AND ${num.field} > ${num.examples[0]}`);
  const nested = sug.find((s) => s.field.includes('.') && !s.field.startsWith('header.') && s.type === 'string' && s.examples[0]);
  if (nested) out.push(`${nested.field} prefix "${nested.examples[0]!.replace(/^"|"$/g, '').slice(0, 3)}"`);
  out.push('key prefix "ord_"', 'header.flux-trace-id exists', 'order_id = 8842 AND country = "MD"');
  return [...new Set(out)].slice(0, 8);
}

function SearchMode({ name }: { name: string }) {
  const t = useT();
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const filter = params.get('filter') ?? '';
  const asDrawer = useMediaQuery('(max-width: 1100px)');
  const key = params.get('key') ?? '';
  const partition = params.get('p') ?? '';
  const offset = params.get('o') ?? '';
  const range = (params.get('range') as RangeKey | null) ?? 'latest';
  const from = params.get('from') ?? '';
  const to = params.get('to') ?? '';
  const sel = params.get('sel');
  const partitions = useTopicSelect<ChannelDetail, number>(TOPICS.channel(name), (d) => d?.config.partitions ?? 0);

  // Draft inputs (applied on submit) so typing doesn't fire a scan per keystroke.
  const [draftKey, setDraftKey] = useState(key);
  const [draftFilter, setDraftFilter] = useState(filter);
  const filterCheck = useFilterParse(draftFilter);
  const [draftP, setDraftP] = useState(partition);
  const [draftO, setDraftO] = useState(offset);
  const [draftFrom, setDraftFrom] = useState(() => from || toLocalInput(Date.now() - 3_600_000));
  const [draftTo, setDraftTo] = useState(to);

  const [nonce, setNonce] = useState(0);
  const queryKey = `${name}|${key}|${partition}|${offset}|${range}|${from}|${to}|${filter}|${nonce}`;
  // Results are tagged with the query they answer; `loading` = no answer for the current query yet.
  const [result, setResult] = useState<{ key: string; page: { rows: Row[]; scanned: number; hasMore: boolean } | null; error: string | null; filterError?: { text: string; info: FilterErrorInfo } } | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const current = result?.key === queryKey ? result : null;
  const loading = current == null;
  const page = current?.page ?? null;
  const error = current?.error ?? null;
  const setPage = (p: { rows: Row[]; scanned: number; hasMore: boolean }) => setResult({ key: queryKey, page: p, error: null });
  const setError = (e: string) => setResult((r) => ({ key: queryKey, page: r?.key === queryKey ? r.page : null, error: e }));

  const buildQuery = useCallback((): MessageQuery => {
    const q: MessageQuery = { limit: PAGE };
    if (key) q.key = key;
    if (filter.trim()) q.filter = filter.trim();
    if (partition !== '') q.partition = Number(partition);
    if (offset !== '') {
      q.offset = Number(offset);
      if (q.partition == null) q.partition = 0;
    } else if (range === 'custom') {
      if (from) q.since_ms = new Date(from).getTime();
      if (to) q.until_ms = new Date(to).getTime();
    } else if (range !== 'latest') {
      q.since_ms = Date.now() - RANGE_MS[range];
    }
    // "Latest" and the relative presets show the newest matches first (server returns the
    // newest `limit` in the window, sorted ascending; we present them descending).
    if (offset === '' && range !== 'custom') q.newest_first = true;
    return q;
  }, [key, partition, offset, range, from, to, filter]);

  const newestFirst = offset === '' && range !== 'custom';

  useEffect(() => {
    const ctrl = new AbortController();
    const k = queryKey;
    api
      .messages(name, buildQuery(), ctrl.signal)
      .then((p: MessagePage) => {
        const rows = p.messages.map(rowFromView);
        setResult({ key: k, page: { rows: newestFirst ? rows.reverse() : rows, scanned: p.scanned, hasMore: p.has_more }, error: null });
      })
      .catch((e: unknown) => {
        if (ctrl.signal.aborted) return;
        // A syntax error in `filter=` carries ApiError.position: point at it in the filter box.
        const text = filter.trim();
        const filterError = e instanceof ApiError && e.position != null && text ? { text, info: filterErrorInfo(text, e) } : undefined;
        setResult({ key: k, page: null, error: errorMessage(e), filterError });
      });
    return () => ctrl.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryKey]);

  const loadMore = async () => {
    if (!page || !page.rows.length) return;
    setLoadingMore(true);
    try {
      const q = buildQuery();
      const last = page.rows[page.rows.length - 1]!;
      if (q.offset != null) q.offset = last.offset + 1;
      else if (newestFirst) {
        // Page backwards: newest matches at or before the oldest row shown. `until_ms` is
        // inclusive, so widen the limit by the rows already seen at that millisecond.
        const atBoundary = page.rows.filter((r) => r.timestamp_ms === last.timestamp_ms).length;
        q.until_ms = last.timestamp_ms;
        q.limit = Math.min(1000, PAGE + atBoundary);
      } else q.since_ms = last.timestamp_ms;
      const p = await api.messages(name, q);
      const seen = new Set(page.rows.map((r) => r.id));
      const fresh = p.messages.map(rowFromView).filter((r) => !seen.has(r.id));
      const more = newestFirst ? fresh.reverse() : fresh;
      setPage({ rows: [...page.rows, ...more], scanned: page.scanned + p.scanned, hasMore: p.has_more && more.length > 0 });
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setLoadingMore(false);
    }
  };

  const update = (patch: Record<string, string | null>) => {
    const next = new URLSearchParams(params);
    for (const [k, v] of Object.entries(patch)) {
      if (v == null || v === '') next.delete(k);
      else next.set(k, v);
    }
    setParams(next, { replace: true });
  };

  const submit = (e?: React.FormEvent) => {
    e?.preventDefault();
    if (filterCheck.status === 'error') return;
    update({
      filter: draftFilter.trim() || null,
      key: draftKey.trim() || null,
      p: draftP,
      o: draftO.trim() || null,
      from: range === 'custom' ? draftFrom : null,
      to: range === 'custom' ? draftTo : null,
      sel: null,
    });
    setNonce((n) => n + 1);
  };

  const clear = () => {
    setDraftKey('');
    setDraftP('');
    setDraftO('');
    setDraftFilter('');
    update({ filter: null, key: null, p: null, o: null, range: null, from: null, to: null, sel: null });
  };

  // The parse check is authoritative while typing; the search's own syntax error (e.g. when the
  // parse endpoint isn't available) is shown in the box when it is about the text in it.
  const searchFilterError = current?.filterError;
  const shownCheck: FilterCheck =
    searchFilterError && (filterCheck.status === 'unknown' || filterCheck.status === 'ok') && draftFilter.trim() === searchFilterError.text
      ? { status: 'error', ...searchFilterError.info, index: searchFilterError.info.index == null ? null : searchFilterError.info.index + (draftFilter.length - draftFilter.trimStart().length) }
      : filterCheck;

  const selectedRow = page?.rows.find((r) => r.id === sel) ?? null;
  const filtersActive = !!(key || partition || offset || filter || range !== 'latest');
  const examples = useMemo(() => filterExamples(page?.rows ?? []), [page?.rows]);

  return (
    <>
      <form className="card query-bar" onSubmit={submit} role="search" aria-label={t('messages.search.aria')}>
        <div className="qb-field qb-filter">
          <div className="qb-filter-head">
            <label htmlFor="q-filter">{t('common.filter')}</label>
            <span className="faint">{t('messages.search.filterHint')}</span>
          </div>
          <FilterInput id="q-filter" value={draftFilter} onChange={setDraftFilter} check={shownCheck} examples={examples} autoFocus={params.get('focus') === 'filter'} />
        </div>
        <div className="qb-field qb-key">
          <label htmlFor="q-key">{t('common.key')}</label>
          <div className="input-group">
            <Search size={14} />
            <input id="q-key" className="input mono" placeholder={t('messages.search.keyPlaceholder')} value={draftKey} onChange={(e) => setDraftKey(e.target.value)} />
          </div>
        </div>
        <div className="qb-field">
          <label htmlFor="q-p">{t('messages.search.partitionFirst')}</label>
          <select id="q-p" className="select" value={draftP} onChange={(e) => setDraftP(e.target.value)}>
            <option value="">{t('common.all')}</option>
            {Array.from({ length: partitions }, (_, i) => (
              <option key={i} value={i}>
                p{i}
              </option>
            ))}
          </select>
        </div>
        <div className="qb-field qb-offset">
          <label htmlFor="q-o">{t('messages.search.fromOffset')}</label>
          <input id="q-o" className="input mono" inputMode="numeric" placeholder="—" value={draftO} onChange={(e) => setDraftO(e.target.value.replace(/\D/g, ''))} />
        </div>
        <div className="qb-field">
          <span className="field-label">{t('common.time')}</span>
          <Segmented<RangeKey>
            value={range}
            label={t('dashboard.timeRange')}
            onChange={(v) => update({ range: v === 'latest' ? null : v, sel: null, ...(v === 'custom' ? { from: draftFrom, to: draftTo } : { from: null, to: null }) })}
            options={[
              { value: 'latest', label: t('messages.search.latest') },
              { value: '15m', label: t('messages.search.last15') },
              { value: '1h', label: t('dashboard.rangeShort.1h') },
              { value: '24h', label: t('dashboard.rangeShort.24h') },
              { value: 'custom', label: t('messages.search.custom') },
            ]}
          />
        </div>
        {range === 'custom' && (
          <div className="qb-field qb-custom">
            <span className="field-label">{t('messages.search.fromTo')}</span>
            <div style={{ display: 'flex', gap: 6 }}>
              <input type="datetime-local" className="input" value={draftFrom} onChange={(e) => setDraftFrom(e.target.value)} aria-label={t('messages.search.from')} />
              <input type="datetime-local" className="input" value={draftTo} onChange={(e) => setDraftTo(e.target.value)} aria-label={t('messages.search.to')} />
            </div>
          </div>
        )}
        <div className="qb-actions">
          {filtersActive && (
            <button type="button" className="btn btn-ghost" onClick={clear}>
              <X size={14} /> {t('common.clear')}
            </button>
          )}
          <button type="submit" className="btn btn-primary" disabled={filterCheck.status === 'error'} title={filterCheck.status === 'error' ? t('messages.search.fixFilter') : undefined}>
            <Search size={14} /> {t('common.search')}
          </button>
        </div>
      </form>

      <div className="msg-layout">
        <section className="card msg-results" aria-label={t('messages.search.results')}>
          <div className="results-head">
            {loading ? (
              <Skeleton w={220} h={14} />
            ) : error ? (
              <span className="text-critical">{t('messages.search.failed')}</span>
            ) : (
              <span className="muted">
                <b className="num" style={{ color: 'var(--text)' }}>
                  {fmtInt(page?.rows.length ?? 0)}
                </b>{' '}
                {t('messages.search.summary', { scanned: fmtCount(page?.scanned ?? 0), order: newestFirst ? t('messages.search.newestFirst') : t('messages.search.oldestFirst') })}
                {filter && page?.hasMore && page.rows.length < PAGE && <span className="cost-hint"> · {t('messages.search.scanLimit')}</span>}
              </span>
            )}
            {filter && !loading && !error && (
              <GateButton need="admin" className="btn btn-sm btn-ghost results-route-btn" onClick={() => navigate(`/routing/new?source=${encodeURIComponent(name)}&filter=${encodeURIComponent(filter)}`)} title={t('messages.search.routeHint')}>
                <RouteIcon size={13} /> {t('messages.search.routeFromFilter')}
              </GateButton>
            )}
          </div>
          {loading ? (
            <div style={{ padding: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
              {Array.from({ length: 10 }, (_, i) => (
                <Skeleton key={i} h={38} />
              ))}
            </div>
          ) : error ? (
            <div style={{ padding: 16 }}>
              <InlineError onRetry={() => setNonce((n) => n + 1)}>{error}</InlineError>
            </div>
          ) : !page?.rows.length ? (
            <EmptyState icon={<Search size={18} />} title={t('messages.search.noMatch')}>
              {filter
                ? t.rich('messages.search.noMatchFilter', { filter: <code>{filter}</code> })
                : key
                  ? t.rich('messages.search.noMatchKey', { key: <code>{key}</code> })
                  : t('messages.search.noMatchRange')}
            </EmptyState>
          ) : (
            <MessageList
              rows={page.rows}
              selected={sel}
              onSelect={(r) => update({ sel: r.id })}
              footer={
                page.hasMore && (
                  <div className="list-foot">
                    <button className="btn btn-sm" onClick={loadMore} disabled={loadingMore}>
                      {loadingMore ? t('common.loadingEllipsis') : newestFirst ? t('messages.search.loadOlder') : t('messages.search.loadMore')}
                    </button>
                  </div>
                )
              }
            />
          )}
        </section>
        {(!asDrawer || selectedRow) && <InspectorPane name={name} row={selectedRow} onClose={() => update({ sel: null })} asDrawer={asDrawer} />}
      </div>
    </>
  );
}

function TailMode({ name }: { name: string }) {
  const t = useT();
  const tail = useTopicData<TailState>(TOPICS.tail(name));
  const asDrawer = useMediaQuery('(max-width: 1100px)');
  const [paused, setPaused] = useState(false);
  const [frozen, setFrozen] = useState<TailState | null>(null);
  const [clearedAt, setClearedAt] = useState(0);
  const [selected, setSelected] = useState<Row | null>(null);

  const view = paused ? frozen : tail;
  const rows = useMemo(() => {
    const msgs = view?.messages ?? [];
    const out: Row[] = [];
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i]!;
      if (m.timestamp_ms < clearedAt) break;
      out.push(rowFromTail(m));
    }
    return out;
  }, [view?.messages, clearedAt]);

  const togglePause = () => {
    if (!paused) setFrozen(tail ?? null);
    setPaused(!paused);
  };

  return (
    <>
      <div className="card tail-bar" role="status" aria-live="off">
        <span className={`tail-indicator ${paused ? 'paused' : ''}`} aria-hidden />
        {tail ? (
          <span>
            {t.rich('messages.tail.showing', { shown: <b className="num">{fmtRate(tail.shown_rate, false)}</b>, total: <b className="num">{fmtRate(tail.total_rate, false)}</b> })}
            <span className="faint">
              {' · '}
              {t('messages.tail.sampled', { pct: fmtNumber(tail.sample_ratio * 100, tail.sample_ratio < 0.01 ? 2 : 1), kept: fmtInt(rows.length) })}
            </span>
          </span>
        ) : (
          <Skeleton w={320} h={14} />
        )}
        <span className="spacer" />
        <button className="btn btn-sm" onClick={() => setClearedAt(Date.now())}>
          <Trash2 size={13} /> {t('common.clear')}
        </button>
        <button className={`btn btn-sm ${paused ? 'btn-primary' : ''}`} onClick={togglePause} aria-pressed={paused}>
          {paused ? <Play size={13} /> : <Pause size={13} />} {paused ? t('messages.tail.resume') : t('messages.tail.pause')}
        </button>
      </div>
      <div className="msg-layout">
        <section className="card msg-results" aria-label={t('messages.tail.live')}>
          <div className="results-head">
            <span className="muted">{t('messages.tail.hint')}</span>
          </div>
          {!tail ? (
            <div style={{ padding: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
              {Array.from({ length: 10 }, (_, i) => (
                <Skeleton key={i} h={38} />
              ))}
            </div>
          ) : rows.length === 0 ? (
            <EmptyState icon={<Radio size={18} />} title={t('messages.tail.waiting')}>
              {t('messages.tail.waitingBody')}
            </EmptyState>
          ) : (
            <MessageList rows={rows} selected={selected?.id ?? null} onSelect={setSelected} live />
          )}
        </section>
        {(!asDrawer || selected) && <InspectorPane name={name} row={selected} onClose={() => setSelected(null)} asDrawer={asDrawer} />}
      </div>
    </>
  );
}

export function MessagesTab({ name }: { name: string }) {
  const t = useT();
  const [params, setParams] = useSearchParams();
  const mode = params.get('mode') === 'tail' ? 'tail' : 'search';
  return (
    <div className="messages-tab">
      <div className="msg-modebar">
        <Segmented
          value={mode}
          label={t('common.mode')}
          onChange={(v) => setParams(v === 'tail' ? { mode: 'tail' } : {}, { replace: true })}
          options={[
            { value: 'search', label: t('common.search') },
            { value: 'tail', label: t('messages.tail.live') },
          ]}
        />
        <span className="faint" style={{ fontSize: 12 }}>
          {mode === 'search' ? t('messages.modeSearchHint') : t('messages.modeTailHint')}
        </span>
      </div>
      {mode === 'search' ? <SearchMode key={`${name}|${params.get('filter') ?? ''}`} name={name} /> : <TailMode key={name} name={name} />}
    </div>
  );
}

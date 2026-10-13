/** State of the shared filter builder (see FilterBuilder.tsx): rows ⇄ DSL text via the server. */
import { useEffect, useMemo, useState } from 'react';
import { api, ApiError } from '../../api/client';
import type { Condition } from '../../api/types';
import { filterErrorInfo } from '../../lib/filterError';
import { renderConditions, useFilterRender } from '../../lib/useFilterRender';
import { conditionsToRows, emptyRow, rowsToConditions, suggestFields, validateRow, type FieldSuggestion, type Row } from '../../lib/routeRows';
import { useAsync } from '../../lib/hooks';
import { toast } from '../../store/ui';
import { t as tr } from '../../i18n';

const BASE_FIELDS: FieldSuggestion[] = [
  { field: 'key', type: 'string', examples: [], seen: 0 },
  { field: 'partition', type: 'number', examples: [], seen: 0 },
];

/** Server parse of the text (`index` = JS string index of `ApiError.position`, a byte offset). */
export type ParseState = { text: string; status: 'ok'; conditions: Condition[]; rendered: string } | { text: string; status: 'error'; message: string; detail: string; index: number | null } | { text: string; status: 'forbidden' };

// ───────────────────────────── state hook ─────────────────────────────

export interface FilterBuilderState {
  mode: 'builder' | 'text';
  rows: Row[];
  setRows: (r: Row[]) => void;
  text: string;
  setText: (t: string) => void;
  parse: ParseState | null;
  /** Current conditions, or null while incomplete / unparsed. */
  conditions: Condition[] | null;
  /** Why `conditions` is null (for previews), else null. */
  pendingReason: string | null;
  /** The user typed something that isn't valid yet (counts as a change). */
  hasDraft: boolean;
  suggestions: FieldSuggestion[];
  sampleCount: number | null;
  builderRender: ReturnType<typeof useFilterRender>;
  switchMode: (m: 'builder' | 'text') => Promise<void>;
  switching: boolean;
  focusId: string | null;
  addRow: () => void;
  /** Replace everything with these conditions (builder mode). */
  reset: (c: Condition[]) => void;
}

export function useFilterBuilder({ source, initial, presetFilter, startEmpty }: { source: string; initial: Condition[] | null; presetFilter?: string | null; startEmpty?: boolean }): FilterBuilderState {
  const [rows, setRows] = useState<Row[]>(() => (initial ? conditionsToRows(initial) : presetFilter || startEmpty ? [] : [emptyRow()]));
  const [mode, setMode] = useState<'builder' | 'text'>(presetFilter ? 'text' : 'builder');
  const [text, setText] = useState(presetFilter ?? '');
  const [parse, setParse] = useState<ParseState | null>(null);
  const [focusId, setFocusId] = useState<string | null>(null);

  // Validate the text through the server (debounced). Results are tagged with their text.
  useEffect(() => {
    if (mode !== 'text') return;
    const t = text;
    if (t.trim() === '') return;
    const ctrl = new AbortController();
    const timer = setTimeout(() => {
      api
        .parseFilter(t, ctrl.signal)
        .then((r) => setParse({ text: t, status: 'ok', conditions: r.conditions, rendered: r.rendered }))
        .catch((e: unknown) => {
          if (ctrl.signal.aborted) return;
          if (e instanceof ApiError && e.isForbidden) setParse({ text: t, status: 'forbidden' });
          else setParse({ text: t, status: 'error', ...filterErrorInfo(t, e) });
        });
    }, 300);
    return () => {
      clearTimeout(timer);
      ctrl.abort();
    };
  }, [text, mode]);

  const textParse: ParseState | null = text.trim() === '' ? { text, status: 'ok', conditions: [], rendered: '' } : parse?.text === text ? parse : null;
  const builderConds = useMemo(() => rowsToConditions(rows), [rows]);
  const conditions: Condition[] | null = mode === 'builder' ? builderConds : textParse?.status === 'ok' ? textParse.conditions : null;
  const pendingReason = mode === 'text' ? (textParse == null ? tr('routing.fb.pendingChecking') : textParse.status === 'error' ? tr('routing.fb.pendingSyntax') : null) : rows.some((r) => validateRow(r)) ? tr('routing.fb.pendingIncomplete') : null;

  // Field autocomplete: JSON keys and headers of the source's recent messages.
  const sample = useAsync((signal) => (source ? api.messages(source, { limit: 100, newest_first: true }, signal) : Promise.resolve(null)), [source]);
  const suggestions = useMemo(() => {
    const learned = sample.data ? suggestFields(sample.data.messages) : [];
    return [...BASE_FIELDS, ...learned];
  }, [sample.data]);

  // Builder → text goes through the server renderer (debounced while editing, so the switch
  // is usually instant).
  const builderRender = useFilterRender(mode === 'builder' ? builderConds : null);
  const [switching, setSwitching] = useState(false);

  const switchMode = async (m: 'builder' | 'text') => {
    if (m === mode || switching) return;
    if (m === 'text') {
      if (!builderConds) return;
      setSwitching(true);
      const r = builderRender.status === 'ok' ? builderRender : await renderConditions(builderConds);
      setSwitching(false);
      if (r.status === 'ok') {
        setText(r.rendered);
        setMode('text');
      } else toast('error', r.status === 'error' ? tr('routing.fb.cantConvertMsg', { message: r.message }) : tr('routing.fb.cantConvert'));
    } else if (textParse?.status === 'ok') {
      setRows(textParse.conditions.length ? conditionsToRows(textParse.conditions) : [emptyRow()]);
      setMode('builder');
    }
  };

  return {
    mode,
    rows,
    setRows,
    text,
    setText,
    parse,
    conditions,
    pendingReason,
    hasDraft: conditions == null && (mode === 'text' || rows.some((r) => r.field || r.value)),
    suggestions,
    sampleCount: sample.data ? sample.data.messages.length : null,
    builderRender,
    switchMode,
    switching,
    focusId,
    addRow: () => {
      const r = emptyRow();
      setFocusId(r.id);
      setRows([...rows, r]);
    },
    reset: (c) => {
      setRows(conditionsToRows(c));
      setMode('builder');
      setText('');
    },
  };
}


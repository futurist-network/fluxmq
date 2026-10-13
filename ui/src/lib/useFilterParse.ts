import { useEffect, useState } from 'react';
import { api, ApiError } from '../api/client';
import type { Condition } from '../api/types';
import { filterErrorInfo } from './filterError';

export type FilterCheck =
  | { status: 'empty' }
  | { status: 'checking' }
  | { status: 'ok'; conditions: Condition[]; rendered: string }
  /** `index`: JS string index of the error (from `ApiError.position`, a byte offset). */
  | { status: 'error'; message: string; detail: string; index: number | null }
  /** The server refused to validate (role, or a server without the endpoint) — let the search itself report problems. */
  | { status: 'unknown' };

type Tagged = { text: string; check: FilterCheck };

/** Debounced `POST /api/filter/parse` of `text` — the server is the only DSL parser. */
export function useFilterParse(text: string, delay = 300): FilterCheck {
  const [res, setRes] = useState<Tagged | null>(null);
  useEffect(() => {
    if (!text.trim()) return;
    const ctrl = new AbortController();
    const t = setTimeout(() => {
      api
        .parseFilter(text, ctrl.signal)
        .then((r) => setRes({ text, check: { status: 'ok', conditions: r.conditions, rendered: r.rendered } }))
        .catch((e: unknown) => {
          if (ctrl.signal.aborted) return;
          if (e instanceof ApiError && (e.isForbidden || e.isNotFound)) {
            setRes({ text, check: { status: 'unknown' } });
            return;
          }
          setRes({ text, check: { status: 'error', ...filterErrorInfo(text, e) } });
        });
    }, delay);
    return () => {
      clearTimeout(t);
      ctrl.abort();
    };
  }, [text, delay]);
  if (!text.trim()) return { status: 'empty' };
  return res?.text === text ? res.check : { status: 'checking' };
}

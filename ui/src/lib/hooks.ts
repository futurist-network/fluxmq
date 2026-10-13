import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { useTelemetry } from '../store/telemetry';

export interface AsyncState<T> {
  data: T | undefined;
  error: unknown;
  loading: boolean;
  reload: () => void;
}

/**
 * Minimal data-fetching hook with abort-on-change. `fn` receives an AbortSignal.
 * Keeps the previous data while reloading (no flash to skeleton on refresh).
 */
export function useAsync<T>(fn: (signal: AbortSignal) => Promise<T>, deps: readonly unknown[]): AsyncState<T> {
  const [nonce, setNonce] = useState(0);
  // Deps are primitives in practice; the key tells a finished request from the current one.
  const key = JSON.stringify([...deps, nonce]);
  const [state, setState] = useState<{ data: T | undefined; error: unknown; key: string | null }>({ data: undefined, error: undefined, key: null });
  const fnRef = useRef(fn);
  useEffect(() => {
    fnRef.current = fn;
  });

  useEffect(() => {
    const ctrl = new AbortController();
    fnRef.current(ctrl.signal).then(
      (data) => {
        if (!ctrl.signal.aborted) setState({ data, error: undefined, key });
      },
      (error: unknown) => {
        if (!ctrl.signal.aborted) setState((s) => ({ data: s.data, error, key }));
      },
    );
    return () => ctrl.abort();
  }, [key]);

  const reload = useCallback(() => setNonce((n) => n + 1), []);
  return { data: state.data, error: state.error, loading: state.key !== key, reload };
}

// One shared 1 s clock for relative timestamps, so 50 "4m ago" labels don't each own a timer.
let nowValue = Date.now();
const nowListeners = new Set<() => void>();
let nowTimer: ReturnType<typeof setInterval> | null = null;

function subscribeNow(l: () => void): () => void {
  nowListeners.add(l);
  if (!nowTimer) {
    nowTimer = setInterval(() => {
      nowValue = Date.now();
      nowListeners.forEach((fn) => fn());
    }, 1000);
  }
  return () => {
    nowListeners.delete(l);
    if (!nowListeners.size && nowTimer) {
      clearInterval(nowTimer);
      nowTimer = null;
    }
  };
}

/** Wall clock that ticks every second (frozen while the console is PAUSED). */
export function useNow(): number {
  const now = useSyncExternalStore(subscribeNow, () => nowValue);
  const paused = useTelemetry((s) => s.paused);
  const frameAt = useTelemetry((s) => s.frameAt);
  return paused && frameAt ? frameAt : now;
}

/** Re-run `fn` every `ms` while mounted and the document is visible. */
export function useInterval(fn: () => void, ms: number | null): void {
  const ref = useRef(fn);
  useEffect(() => {
    ref.current = fn;
  });
  useEffect(() => {
    if (ms == null) return;
    const id = setInterval(() => {
      if (!document.hidden) ref.current();
    }, ms);
    return () => clearInterval(id);
  }, [ms]);
}

export function useMediaQuery(query: string): boolean {
  return useSyncExternalStore(
    (l) => {
      const m = window.matchMedia(query);
      m.addEventListener('change', l);
      return () => m.removeEventListener('change', l);
    },
    () => window.matchMedia(query).matches,
  );
}

/** Copy to clipboard with a fallback for non-secure contexts. */
export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    return ok;
  }
}

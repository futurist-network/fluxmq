import { createElement, lazy, type ComponentType } from 'react';

/**
 * `React.lazy` with `preload()`: once the chunk has loaded, the page renders directly instead of
 * suspending for a frame (React.lazy always suspends on first render, even for a loaded module),
 * so switching to a preloaded screen never flashes the Suspense fallback.
 */
export function lazyPage<P extends object>(factory: () => Promise<{ default: ComponentType<P> }>) {
  let loaded: ComponentType<P> | null = null;
  let pending: Promise<{ default: ComponentType<P> }> | null = null;
  const load = () =>
    (pending ??= factory().then(
      (m) => {
        loaded = m.default;
        return m;
      },
      (e: unknown) => {
        pending = null; // a failed chunk fetch can be retried later
        throw e;
      },
    ));
  const Lazy = lazy(load);
  function Page(props: P) {
    return createElement((loaded ?? Lazy) as ComponentType<P>, props);
  }
  Page.preload = () => {
    load().catch(() => {});
  };
  return Page;
}

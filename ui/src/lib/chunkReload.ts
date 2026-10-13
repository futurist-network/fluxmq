/**
 * After a broker upgrade (or a console rebuild) an open tab still references the old build's
 * lazily loaded chunks, which no longer exist: loading one fails. The fix is a reload, which
 * fetches the new index.html. Reload at most once per 10 s so a real outage can't loop.
 */
const KEY = 'fluxmq.chunkReloadAt';
const GUARD_MS = 10_000;

/** A failed dynamic import / module preload (Chrome, Firefox and Safari wordings). */
export function isChunkLoadError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : typeof err === 'string' ? err : '';
  return /Failed to fetch dynamically imported module|error loading dynamically imported module|Importing a module script failed|Unable to preload CSS/i.test(msg);
}

/** Reloads the page unless it already did so in the last 10 s; `true` = reloading. */
export function reloadForNewBuild(now = Date.now(), storage: Pick<Storage, 'getItem' | 'setItem'> | null = safeSession()): boolean {
  const last = Number(storage?.getItem(KEY) ?? 0);
  if (now - last < GUARD_MS) return false;
  storage?.setItem(KEY, String(now));
  window.location.reload();
  return true;
}

function safeSession(): Storage | null {
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

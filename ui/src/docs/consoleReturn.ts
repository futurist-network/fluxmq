/**
 * Where "Back to console" in the docs leads: the last console screen the reader was on in this
 * tab (so opening the docs from a channel and going back lands on that channel), else the
 * dashboard. In-memory only — a fresh load of /docs simply returns to `/`.
 */
let last = '/';

export function isDocsPath(pathname: string): boolean {
  return pathname === '/docs' || pathname.startsWith('/docs/');
}

/** Called by the shell on every navigation. */
export function rememberConsolePath(pathname: string, search = ''): void {
  if (!isDocsPath(pathname)) last = pathname + search;
}

export function consoleReturnPath(): string {
  return last;
}

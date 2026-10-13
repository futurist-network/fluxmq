/**
 * Screen transitions. Route changes animate only the content area (the shell stays put):
 *
 *  - shell navigation (sidebar, settings tabs, command palette) uses the View Transitions API
 *    through react-router's `viewTransition` — the browser cross-fades the `page` snapshot;
 *  - every other route change (in-page links, back/forward, and browsers without the API) gets
 *    the same short fade + rise via the Web Animations API on the content wrapper.
 *
 * Neither ever delays navigation or data: the new screen renders immediately and is only faded
 * in (≤ 200 ms). Live numbers/charts are never re-animated — this runs once per route change.
 *
 * Disabled entirely for `prefers-reduced-motion: reduce`, for automation (`navigator.webdriver`,
 * i.e. Playwright screenshots stay deterministic, unless `localStorage['fluxmq.motion'] = 'on'`) and
 * when `localStorage['fluxmq.motion'] = 'off'`.
 */

function readMotionPref(): string | null {
  try {
    return window.localStorage.getItem('fluxmq.motion');
  } catch {
    return null;
  }
}

export function motionEnabled(): boolean {
  if (typeof window === 'undefined' || typeof document === 'undefined') return false;
  if (window.matchMedia?.('(prefers-reduced-motion: reduce)').matches) return false;
  const pref = readMotionPref();
  if (pref === 'off') return false;
  // Automation is deterministic unless a test explicitly opts in ('on') to record transitions.
  if (navigator.webdriver) return pref === 'on';
  return true;
}

/** True when shell links should request a view transition. */
export function viewTransitionsOn(): boolean {
  return motionEnabled() && typeof document.startViewTransition === 'function';
}

let vtPending = false;

/** Called by a link that navigates with `viewTransition`: the content fallback must not double-animate. */
export function markViewTransition(): void {
  if (viewTransitionsOn()) vtPending = true;
}

/** Read-and-clear: did the navigation that just happened run as a view transition? */
export function consumeViewTransition(): boolean {
  const p = vtPending;
  vtPending = false;
  return p;
}

/**
 * The screen a path belongs to: tabs of one screen share it (`/channels/orders/messages` →
 * `/channels/orders`, `/settings/tokens` → `/settings`), so switching tabs is not a screen change.
 */
export function routeSection(pathname: string): string {
  const m = /^\/channels\/([^/]+)/.exec(pathname);
  if (m) return `/channels/${m[1]}`;
  if (pathname === '/settings' || pathname.startsWith('/settings/')) return '/settings';
  return pathname;
}

export const PAGE_ENTER_MS = 180;

/** Fade + slight rise of the content wrapper (Web Animations; no class toggling, no remount). */
export function animatePageEnter(el: HTMLElement | null): void {
  if (!el || !motionEnabled() || typeof el.animate !== 'function') return;
  el.animate(
    [
      { opacity: 0, transform: 'translateY(6px)' },
      { opacity: 1, transform: 'none' },
    ],
    { duration: PAGE_ENTER_MS, easing: 'cubic-bezier(0.2, 0, 0, 1)' },
  );
}

/** `<html data-motion="off">` lets CSS switch off its own (mount) animations in the same cases. */
export function applyMotionAttr(): void {
  if (typeof document === 'undefined') return;
  if (!motionEnabled()) document.documentElement.dataset.motion = 'off';
  else delete document.documentElement.dataset.motion;
}

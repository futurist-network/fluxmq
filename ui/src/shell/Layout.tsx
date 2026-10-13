import { Suspense, useEffect, useLayoutEffect, useRef } from 'react';
import { Outlet, useLocation } from 'react-router';
import { Sidebar } from './Sidebar';
import { TopBar } from './TopBar';
import { AuthPrompt, ConnectionBanner, OpenModeBanner, SessionRevoked, Toasts } from './Banners';
import { CommandPalette } from './CommandPalette';
import { CreateChannelWizard } from '../pages/CreateChannel';
import { toast, useUi } from '../store/ui';
import { onForbidden, onUnauthorized } from '../api/client';
import { effectiveRole, useAuth } from '../store/auth';
import { forbiddenText } from '../lib/rbac';
import { useT } from '../i18n';
import { animatePageEnter, consumeViewTransition, routeSection } from '../lib/motion';
import { isDocsPath, rememberConsolePath } from '../docs/consoleReturn';
import './layout.css';

export function Layout() {
  const t = useT();
  const setPaletteOpen = useUi((s) => s.setPaletteOpen);
  const setAuthRequired = useUi((s) => s.setAuthRequired);
  const { pathname, search } = useLocation();
  const docsMode = isDocsPath(pathname);

  useEffect(() => rememberConsolePath(pathname, search), [pathname, search]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        setPaletteOpen(!useUi.getState().paletteOpen);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [setPaletteOpen]);

  useEffect(() => onUnauthorized(() => setAuthRequired(true)), [setAuthRequired]);

  // 403: the token is valid but its role is too small. Explain once, and re-read the identity
  // (an admin may have changed it, or the server switched from open to enforced mode).
  useEffect(
    () =>
      onForbidden(() => {
        toast('error', forbiddenText(effectiveRole(useAuth.getState())));
        void useAuth.getState().load();
      }),
    [],
  );

  // A tab switch inside one screen (channel tabs, settings tabs) keeps the scroll position and the
  // header in place; only a move to another screen scrolls to the top and fades the content in.
  const section = routeSection(pathname);
  useEffect(() => {
    window.scrollTo({ top: 0 });
  }, [section]);

  // Screen change: fade the content in (the shell stays static). Skipped on first paint and when
  // the browser already ran a view transition for this navigation (shell links).
  const hostRef = useRef<HTMLDivElement>(null);
  const first = useRef(true);
  useLayoutEffect(() => {
    if (first.current) {
      first.current = false;
      return;
    }
    if (!consumeViewTransition()) animatePageEnter(hostRef.current);
  }, [section]);

  return (
    <div className={`app ${docsMode ? 'docs-mode' : ''}`}>
      <a className="skip-link" href="#main">
        {t('shell.skip')}
      </a>
      <Sidebar rail={docsMode} />
      <div className="app-main">
        <TopBar />
        <ConnectionBanner />
        <SessionRevoked />
        <OpenModeBanner />
        <main id="main" className="content" tabIndex={-1}>
          <div className="page-host" ref={hostRef}>
            <Suspense fallback={null}>
              <Outlet />
            </Suspense>
          </div>
        </main>
      </div>
      <CommandPalette />
      <CreateChannelWizard />
      <AuthPrompt />
      <Toasts />
    </div>
  );
}

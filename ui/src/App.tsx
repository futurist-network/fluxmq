import { createBrowserRouter, isRouteErrorResponse, Link, RouterProvider, useRouteError } from 'react-router';
import { AlertTriangle, Compass } from 'lucide-react';
import { isChunkLoadError, reloadForNewBuild } from './lib/chunkReload';
import { Layout } from './shell/Layout';
import Dashboard from './pages/dashboard/Dashboard';
import ChannelsPage from './pages/Channels';
import ChannelDetailPage from './pages/channel/ChannelDetail';
import ConsumersPage from './pages/Consumers';
import DeadLettersPage from './pages/DeadLetters';
import AlertsPage from './pages/Alerts';
import NodesPage from './pages/Nodes';
import SettingsPage from './pages/Settings';
import AccessTokensPage from './pages/AccessTokens';
import RoutingPage from './pages/routing/RoutingPage';
import RouteEditorPage from './pages/routing/RouteEditor';
import { Suspense, type ReactNode } from 'react';
import { BackupsPage, DocsPage, LivePage, ScheduledPage, NotificationsPage, SecurityPage, WebhookEditorPage, WebhooksPage } from './pages/lazyPages';
import { EmptyState, Skeleton } from './components/ui';
import { useT } from './i18n';

function Lazy({ children }: { children: ReactNode }) {
  return (
    <Suspense
      fallback={
        <div className="page">
          <Skeleton w={240} h={28} />
          <Skeleton h={320} style={{ marginTop: 20 }} />
        </div>
      }
    >
      {children}
    </Suspense>
  );
}

function NotFound() {
  const t = useT();
  return (
    <div className="page">
      <EmptyState
        icon={<Compass size={20} />}
        title={t('shell.notFound.title')}
        actions={
          <Link to="/" className="btn">
            {t('shell.notFound.back')}
          </Link>
        }
      >
        {t.rich('shell.notFound.body', {
          keys: (
            <>
              <kbd>⌘</kbd> <kbd>K</kbd>
            </>
          ),
        })}
      </EmptyState>
    </div>
  );
}

/** Route errors: a stale lazy chunk reloads the page (new build); anything else gets a reload button. */
function RouteError() {
  const t = useT();
  const err = useRouteError();
  const stale = isChunkLoadError(err);
  if (stale && reloadForNewBuild()) return null;
  const detail = isRouteErrorResponse(err) ? `${err.status} ${err.statusText}` : err instanceof Error ? err.message : String(err);
  return (
    <div className="page">
      <EmptyState
        icon={<AlertTriangle size={20} />}
        title={stale ? t('shell.error.staleTitle') : t('shell.error.title')}
        actions={
          <>
            <button className="btn btn-primary" onClick={() => window.location.reload()}>
              {t('shell.error.reload')}
            </button>
            <a href="/" className="btn">
              {t('shell.notFound.back')}
            </a>
          </>
        }
      >
        {stale ? t('shell.error.staleBody') : t('shell.error.body')}
        <span className="mono faint" style={{ display: 'block', marginTop: 8, fontSize: 12, overflowWrap: 'anywhere' }}>
          {detail}
        </span>
      </EmptyState>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    errorElement: <RouteError />,
    children: [
      {
        // Pages fail inside the shell: sidebar and top bar stay usable.
        errorElement: <RouteError />,
        children: [
          { index: true, element: <Dashboard /> },
          { path: 'channels', element: <ChannelsPage /> },
          { path: 'channels/:name', element: <ChannelDetailPage /> },
          { path: 'channels/:name/:tab', element: <ChannelDetailPage /> },
          { path: 'consumers', element: <ConsumersPage /> },
          { path: 'dlq', element: <DeadLettersPage /> },
          { path: 'alerts', element: <AlertsPage /> },
          { path: 'nodes', element: <NodesPage /> },
          { path: 'settings', element: <SettingsPage /> },
          { path: 'settings/tokens', element: <AccessTokensPage /> },
          { path: 'settings/notifications', element: <Lazy><NotificationsPage /></Lazy> },
          { path: 'settings/backups', element: <Lazy><BackupsPage /></Lazy> },
          { path: 'settings/security', element: <Lazy><SecurityPage /></Lazy> },
          { path: 'webhooks', element: <Lazy><WebhooksPage /></Lazy> },
          { path: 'webhooks/new', element: <Lazy><WebhookEditorPage /></Lazy> },
          { path: 'webhooks/:id', element: <Lazy><WebhookEditorPage /></Lazy> },
          { path: 'scheduled', element: <Lazy><ScheduledPage /></Lazy> },
          { path: 'live', element: <Lazy><LivePage /></Lazy> },
          { path: 'docs', element: <Lazy><DocsPage /></Lazy> },
          { path: 'docs/:slug', element: <Lazy><DocsPage /></Lazy> },
          { path: 'routing', element: <RoutingPage /> },
          { path: 'routing/new', element: <RouteEditorPage /> },
          { path: 'routing/:id', element: <RouteEditorPage /> },
          { path: '*', element: <NotFound /> },
        ],
      },
    ],
  },
]);

export function App() {
  return <RouterProvider router={router} />;
}

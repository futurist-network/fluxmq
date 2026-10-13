import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import '@fontsource-variable/inter';
import '@fontsource-variable/jetbrains-mono';
import './styles/tokens.css';
import './styles/base.css';
import './styles/ui.css';
import { App } from './App';
import { getHints, getTheme, onTokenChange } from './lib/settings';
import { applyMotionAttr } from './lib/motion';
import { reloadForNewBuild } from './lib/chunkReload';
import { telemetry } from './telemetry';
import { useAuth } from './store/auth';

document.documentElement.dataset.theme = getTheme();
document.documentElement.dataset.hints = getHints() ? 'on' : 'off';
// Vite fires this when a chunk's preload fails (stale tab after an upgrade): reload once.
window.addEventListener('vite:preloadError', (e) => {
  if (reloadForNewBuild()) e.preventDefault();
});
applyMotionAttr();

telemetry.start();
void useAuth.getState().load();
onTokenChange(() => {
  telemetry.restart();
  void useAuth.getState().load();
});

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import '@fontsource-variable/inter';
import '@fontsource-variable/jetbrains-mono';
import './styles/tokens.css';
import './styles/base.css';
import './styles/ui.css';
import { App } from './App';
import { getTheme, onTokenChange } from './lib/settings';
import { applyMotionAttr } from './lib/motion';
import { telemetry } from './telemetry';
import { useAuth } from './store/auth';

document.documentElement.dataset.theme = getTheme();
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

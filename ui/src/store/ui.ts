/** UI-only state: overlays, auth prompt, toasts. Kept apart from telemetry on purpose. */
import { create } from 'zustand';
import { getThemePref, resolveTheme, setTheme as persistTheme, systemTheme, type Theme, type ThemePref } from '../lib/settings';

export interface Toast {
  id: number;
  kind: 'success' | 'error' | 'info';
  text: string;
}

interface UiStore {
  paletteOpen: boolean;
  createOpen: boolean;
  authRequired: boolean;
  navOpen: boolean;
  /** The theme in effect (resolved from `themePref`). */
  theme: Theme;
  themePref: ThemePref;
  setTheme: (t: ThemePref) => void;
  toasts: Toast[];
  setPaletteOpen: (v: boolean) => void;
  setCreateOpen: (v: boolean) => void;
  setAuthRequired: (v: boolean) => void;
  setNavOpen: (v: boolean) => void;
  toast: (kind: Toast['kind'], text: string) => void;
  dismissToast: (id: number) => void;
}

let toastId = 0;

export const useUi = create<UiStore>()((set, get) => ({
  paletteOpen: false,
  createOpen: false,
  authRequired: false,
  navOpen: false,
  theme: typeof window === 'undefined' ? 'dark' : resolveTheme(getThemePref()),
  themePref: typeof window === 'undefined' ? 'system' : getThemePref(),
  setTheme: (themePref) => {
    persistTheme(themePref);
    set({ themePref, theme: resolveTheme(themePref) });
  },
  toasts: [],
  setPaletteOpen: (paletteOpen) => set({ paletteOpen }),
  setCreateOpen: (createOpen) => set({ createOpen }),
  setAuthRequired: (authRequired) => set({ authRequired }),
  setNavOpen: (navOpen) => set({ navOpen }),
  toast: (kind, text) => {
    const id = ++toastId;
    set({ toasts: [...get().toasts.slice(-3), { id, kind, text }] });
    setTimeout(() => get().dismissToast(id), kind === 'error' ? 6000 : 3500);
  },
  dismissToast: (id) => set({ toasts: get().toasts.filter((t) => t.id !== id) }),
}));

export const toast = (kind: Toast['kind'], text: string) => useUi.getState().toast(kind, text);

// Following the system: switch live when the OS theme changes (e.g. at sunset).
if (typeof window !== 'undefined' && window.matchMedia) {
  window.matchMedia('(prefers-color-scheme: light)').addEventListener?.('change', () => {
    const { themePref } = useUi.getState();
    if (themePref !== 'system') return;
    const theme = systemTheme();
    document.documentElement.dataset.theme = theme;
    useUi.setState({ theme });
  });
}

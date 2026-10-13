/**
 * Current identity (`GET /api/auth/me`). Loaded on startup and whenever the stored token
 * changes. A v0.1 server (404) has no roles: the single admin token can do everything.
 */
import { create } from 'zustand';
import { api, ApiError } from '../api/client';
import type { Role, WhoAmI } from '../api/types';

export type AuthStatus = 'loading' | 'ready' | 'unsupported' | 'unauthenticated' | 'error';

interface AuthStore {
  me: WhoAmI | null;
  status: AuthStatus;
  load: () => Promise<void>;
}

let inflight: AbortController | null = null;

export const useAuth = create<AuthStore>()((set) => ({
  me: null,
  status: 'loading',
  load: async () => {
    inflight?.abort();
    const ctrl = new AbortController();
    inflight = ctrl;
    try {
      const me = await api.whoami(ctrl.signal);
      if (!ctrl.signal.aborted) set({ me, status: 'ready' });
    } catch (e) {
      if (ctrl.signal.aborted) return;
      if (e instanceof ApiError && e.isNotFound) set({ me: null, status: 'unsupported' });
      else if (e instanceof ApiError && e.isUnauthorized) set({ me: null, status: 'unauthenticated' });
      else set((s) => ({ me: s.me, status: s.me ? 'ready' : 'error' }));
    }
  },
}));

/** Effective role; null while unknown (UI allows, the server decides). */
export function effectiveRole(s: Pick<AuthStore, 'me' | 'status'>): Role | null {
  if (s.me) return s.me.role;
  if (s.status === 'unsupported') return 'admin';
  return null;
}

export const useRole = (): Role | null => useAuth((s) => effectiveRole(s));

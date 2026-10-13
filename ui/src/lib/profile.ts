// CPU profile of the node serving the console (`GET /api/debug/pprof/profile`, `profiling` builds).
import { useEffect, useState } from 'react';
import { api } from '../api/client';
import type { HealthResponse } from '../api/types';

/** The node serving the console and its build features (`/api/health`); null until known. */
export function useServingNode(): HealthResponse | null {
  const [h, setH] = useState<HealthResponse | null>(null);
  useEffect(() => {
    let live = true;
    api
      .health()
      .then((r) => live && setH(r))
      .catch(() => {});
    return () => {
      live = false;
    };
  }, []);
  return h;
}

export const PROFILE_DURATIONS = ['10', '30', '60'] as const;
export type ProfileDuration = (typeof PROFILE_DURATIONS)[number];
export type ProfileFormat = 'svg' | 'folded';

export function profileFileName(node: string, format: ProfileFormat, at: Date): string {
  const ts = at.toISOString().slice(0, 19).replace(/:/g, '');
  return `fluxmq-cpu-${node.replace(/[^a-z0-9._-]+/gi, '-')}-${ts}.${format === 'svg' ? 'svg' : 'folded.txt'}`;
}

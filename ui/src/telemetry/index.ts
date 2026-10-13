/** The tab-wide telemetry client instance and React bindings. */
import { useEffect } from 'react';
import { useShallow } from 'zustand/react/shallow';
import { ApiError, api } from '../api/client';
import { getToken } from '../lib/settings';
import { recordFrame } from '../store/series';
import { commitFrame, setConn, useTelemetry } from '../store/telemetry';
import { useUi } from '../store/ui';
import { TelemetryClient } from './client';

export function wsUrl(): string {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const token = getToken();
  return `${proto}//${location.host}/ws${token ? `?token=${encodeURIComponent(token)}` : ''}`;
}

export const telemetry = new TelemetryClient({
  url: wsUrl,
  onCommit: (c) => {
    recordFrame(c);
    commitFrame(c);
  },
  onConn: setConn,
  onConnectFailed: (attempt) => {
    // Browsers hide the HTTP status of a failed WS upgrade; probe REST to tell
    // "server down" from "needs a token". Only every few attempts to stay quiet.
    if (attempt > 1 && attempt % 3 !== 0) return;
    api.checkAuth().catch((e: unknown) => {
      if (e instanceof ApiError && e.isUnauthorized) useUi.getState().setAuthRequired(true);
    });
  },
});

/** Subscribe the calling component to a topic for as long as it is mounted. */
export function useTopic(topic: string | null | undefined): void {
  useEffect(() => {
    if (!topic) return;
    return telemetry.subscribe(topic);
  }, [topic]);
}

/** Subscribe + select the whole topic state. Prefer `useTopicSelect` for narrow reads. */
export function useTopicData<T>(topic: string | null | undefined): T | undefined {
  useTopic(topic);
  return useTelemetry((s) => (topic ? (s.topics[topic] as T | undefined) : undefined));
}

/**
 * Subscribe + fine-grained selection: re-renders only when the selected value changes
 * (shallow compare, so selecting a small object/array of primitives is fine).
 */
export function useTopicSelect<T, R>(topic: string | null | undefined, select: (data: T | undefined) => R): R {
  useTopic(topic);
  return useTelemetry(useShallow((s) => select(topic ? (s.topics[topic] as T | undefined) : undefined)));
}

export function useTopicError(topic: string | null | undefined): string | undefined {
  return useTelemetry((s) => (topic ? s.errors[topic] : undefined));
}

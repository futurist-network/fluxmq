/**
 * Telemetry store (zustand). Holds the per-topic view committed by the TelemetryClient
 * once per animation frame. Components select narrow slices (`useTelemetry(s => ...)`),
 * and because merge patches share structure, unchanged slices keep identity and do
 * not re-render.
 *
 * LIVE/PAUSED: the client keeps applying frames (it must, to stay consistent with the
 * delta stream); while paused we only stop publishing them to React. Resume publishes
 * the latest frame.
 */
import { create } from 'zustand';
import type { ConnState, FrameCommit } from '../telemetry/client';

export interface TelemetryStore {
  topics: Readonly<Record<string, unknown>>;
  errors: Readonly<Record<string, string>>;
  conn: ConnState;
  paused: boolean;
  /** Wall clock of the frame currently shown (frozen while paused). */
  frameAt: number;
  setPaused: (paused: boolean) => void;
}

let latest: FrameCommit | null = null;

export const initialConn: ConnState = {
  status: 'idle',
  attempt: 0,
  retryAt: null,
  everOpened: false,
  brokerVersion: null,
  nodeId: null,
  contractMismatch: null,
  rttMs: null,
  gaps: 0,
  revoked: null,
};

export const useTelemetry = create<TelemetryStore>()((set, get) => ({
  topics: {},
  errors: {},
  conn: initialConn,
  paused: false,
  frameAt: 0,
  setPaused: (paused) => {
    if (get().paused === paused) return;
    if (!paused && latest) {
      set({ paused, topics: latest.view, errors: latest.errors, frameAt: latest.now });
    } else {
      set({ paused });
    }
  },
}));

/** Sink for TelemetryClient.onCommit. */
export function commitFrame(c: FrameCommit): void {
  latest = c;
  const s = useTelemetry.getState();
  if (s.paused) {
    // Subscriptions/unsubscriptions must still be visible while paused, otherwise a
    // newly opened page would show a skeleton until resume. Publish topics that were
    // absent from the frozen view (first data), keep the rest frozen.
    let patched: Record<string, unknown> | null = null;
    for (const t of c.changed) {
      const inShown = t in s.topics;
      const inNew = t in c.view;
      if (inShown !== inNew) {
        patched ??= { ...s.topics };
        if (inNew) patched[t] = c.view[t];
        else delete patched[t];
      }
    }
    if (patched) useTelemetry.setState({ topics: patched, errors: c.errors });
    return;
  }
  useTelemetry.setState({ topics: c.view, errors: c.errors, frameAt: c.now });
}

export function setConn(conn: ConnState): void {
  useTelemetry.setState({ conn });
}

/** Test helper. */
export function resetTelemetryStore(): void {
  latest = null;
  useTelemetry.setState({ topics: {}, errors: {}, conn: initialConn, paused: false, frameAt: 0 });
}

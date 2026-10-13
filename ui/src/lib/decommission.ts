/**
 * Node decommission / forget (v0.5, the decommission block of crates/fluxmq-proto/src/cluster.rs),
 * shaped for the Nodes page: progress chips, what the confirm dialog can predict from the current
 * cluster status, forget eligibility, and 409 explanations. Pure, so it is testable without a DOM.
 *
 * The server is the authority on every refusal; predictions here only warn before the request.
 */
import type { ClusterStatus, DecommissionState, DecommissionStatus, NodeStatus, RebalanceStatus } from '../api/types';
import { t } from '../i18n';
import { localized } from './labels';

const DECOMMISSION_STATES = ['moving_replicas', 'leaving_membership', 'removed', 'failed'] as const satisfies readonly DecommissionState[];

/** Translated on access (current console language). */
export const DECOMMISSION_STATE_LABEL: Readonly<Record<DecommissionState, string>> = localized(DECOMMISSION_STATES, 'nodes.decomState.');

export type ChipTone = 'primary' | 'success' | 'warning' | 'critical';

/** `DecommissionStatus.error` of a decommission ended by `POST /api/cluster/rebalance/cancel`. */
export const DECOMMISSION_CANCELLED = 'cancelled';

export interface DecommissionChip {
  label: string;
  tone: ChipTone;
  /** One line under the chip. */
  detail: string;
  /** Still running (poll fast, show a spinner). */
  active: boolean;
  /** Ended by an operator cancel (`failed` with error "cancelled"). */
  cancelled: boolean;
}

export function isTerminal(state: DecommissionState): boolean {
  return state === 'removed' || state === 'failed';
}

export function decommissionChip(s: DecommissionStatus): DecommissionChip {
  switch (s.state) {
    case 'moving_replicas':
      return {
        label: DECOMMISSION_STATE_LABEL.moving_replicas,
        tone: 'primary',
        detail: s.replicas_remaining > 0 ? t('nodes.decom.remaining', { count: s.replicas_remaining }) : t('nodes.decom.committingLast'),
        active: true,
        cancelled: false,
      };
    case 'leaving_membership':
      return { label: DECOMMISSION_STATE_LABEL.leaving_membership, tone: 'primary', detail: t('nodes.decom.leavingDetail'), active: true, cancelled: false };
    case 'removed':
      return { label: DECOMMISSION_STATE_LABEL.removed, tone: 'success', detail: t('nodes.decom.removedDetail'), active: false, cancelled: false };
    case 'failed':
      // The contract's cancel outcome: `failed` with error "cancelled" (cluster.rs). The node is back up.
      if (s.error === DECOMMISSION_CANCELLED) {
        return { label: t('nodes.decom.cancelledLabel'), tone: 'warning', detail: t('nodes.decom.cancelledDetail'), active: false, cancelled: true };
      }
      return { label: DECOMMISSION_STATE_LABEL.failed, tone: 'critical', detail: t('nodes.decom.failedDetail', { error: s.error ?? t('nodes.decom.noReason') }), active: false, cancelled: false };
  }
}

/** Progress 0..1 of the replica moves, against `initial_replicas`. Null when there is no baseline. */
export function decommissionProgress(s: DecommissionStatus): number | null {
  if (s.state === 'leaving_membership' || s.state === 'removed') return 1;
  if (!s.initial_replicas || s.initial_replicas <= 0) return null;
  return Math.min(1, Math.max(0, 1 - s.replicas_remaining / s.initial_replicas));
}

// ───────────── confirm-dialog projection ─────────────

export interface DecommissionPreview {
  /** Partition replicas on the node now — all of them move away. */
  replicas: number;
  /** Partitions it currently leads (leadership is drained first). */
  leaders: number;
  /** Channels with at least one replica on the node. */
  channels: number;
  /** `up` nodes that would remain and can take replicas. */
  targets: string[];
  /** Channels whose replication factor can't be held without the node (→ 409 expected). */
  rfBlocked: { channel: string; rf: number }[];
  /** A rebalance plan is running / paused / cancelling (→ 409 expected). */
  planBusy: boolean;
  /** Raft: removing this voter would leave no voter, or fewer live voters than a quorum (→ 409 expected). */
  quorumRisk: string | null;
  isController: boolean;
  isVoter: boolean;
}

/** Read the dry facts of a decommission off `ClusterStatus` (+ the rebalance status, when loaded). */
export function decommissionPreview(status: ClusterStatus, nodeId: string, rebalance: Pick<RebalanceStatus, 'state'> | null | undefined): DecommissionPreview {
  const nodes = status.metadata.nodes;
  const self = nodes.find((n) => n.id === nodeId);
  const targets = nodes.filter((n) => n.id !== nodeId && n.status === 'up').map((n) => n.id);
  let replicas = 0;
  let leaders = 0;
  let channels = 0;
  const rfBlocked: { channel: string; rf: number }[] = [];
  for (const [channel, parts] of Object.entries(status.metadata.channels)) {
    let here = false;
    let rf = 0;
    for (const p of parts) {
      rf = Math.max(rf, p.replicas.length);
      if (p.replicas.includes(nodeId)) {
        replicas++;
        here = true;
      }
      if (p.leader === nodeId) leaders++;
    }
    if (here) channels++;
    if (here && rf > targets.length) rfBlocked.push({ channel, rf });
  }
  const voters = status.raft.voters;
  const isVoter = self?.is_voter ?? voters.includes(nodeId);
  let quorumRisk: string | null = null;
  if (isVoter) {
    const remaining = voters.filter((v) => v !== nodeId);
    const live = remaining.filter((v) => nodes.find((n) => n.id === v)?.status !== 'down').length;
    const quorum = Math.floor(remaining.length / 2) + 1;
    if (remaining.length === 0) quorumRisk = t('nodes.decom.onlyVoter');
    else if (live < quorum) quorumRisk = t('nodes.decom.belowQuorum', { live, remaining: remaining.length, quorum });
  }
  const busy = rebalance?.state;
  return {
    replicas,
    leaders,
    channels,
    targets,
    rfBlocked,
    planBusy: busy === 'running' || busy === 'paused' || busy === 'cancelling',
    quorumRisk,
    isController: self?.is_controller ?? status.metadata.controller === nodeId,
    isVoter,
  };
}

/** True when the preview already predicts a 409 (the button still lets the server decide). */
export function previewBlocked(p: DecommissionPreview): boolean {
  return p.rfBlocked.length > 0 || p.planBusy || p.quorumRisk != null;
}

// ───────────── forget ─────────────

export interface ForgetGate {
  /** Show the button at all (only for `down` nodes). */
  visible: boolean;
  enabled: boolean;
  /** Tooltip when disabled. */
  reason: string | null;
}

/** Forget applies only to a `down` node holding no replicas (cluster.rs: 409 otherwise). */
export function forgetGate(status: NodeStatus, replicas: number | null | undefined): ForgetGate {
  if (status !== 'down') return { visible: false, enabled: false, reason: null };
  if (replicas == null) return { visible: true, enabled: false, reason: t('nodes.forget.unknown') };
  if (replicas > 0) return { visible: true, enabled: false, reason: t('nodes.forget.stillHolds', { count: replicas }) };
  return { visible: true, enabled: true, reason: null };
}

// ───────────── 409 explanations ─────────────

export interface ConflictView {
  title: string;
  /** The server's reason, verbatim. */
  message: string;
  /** What to do about it (our hint), or null. */
  hint: string | null;
}

/**
 * Explain a refused decommission / forget. Branches on the HTTP status / error code; the server's
 * message is shown verbatim (it names the channel or quorum problem). The hint uses what the UI
 * knows locally (preview), never the message text.
 */
export function conflictView(e: { status?: number; code?: string; message: string }, action: 'decommission' | 'forget', preview?: DecommissionPreview | null): ConflictView {
  const isConflict = e.status === 409 || e.code === 'conflict';
  if (!isConflict) return { title: action === 'forget' ? t('nodes.conflict.forgetFailed') : t('nodes.conflict.decomFailed'), message: e.message, hint: null };
  if (action === 'forget') {
    return { title: t('nodes.conflict.forgetRefused'), message: e.message, hint: t('nodes.conflict.forgetHint') };
  }
  let hint: string | null = null;
  if (preview?.planBusy) hint = t('nodes.conflict.planBusy');
  else if (preview && preview.rfBlocked.length)
    hint = t('nodes.conflict.rfBlocked', { channels: `${preview.rfBlocked.map((b) => b.channel).slice(0, 3).join(', ')}${preview.rfBlocked.length > 3 ? '…' : ''}` });
  else if (preview?.quorumRisk) hint = t('nodes.conflict.quorum');
  return { title: t('nodes.conflict.decomRefused'), message: e.message, hint };
}

// ───────────── tracking (GET /api/cluster/decommissions) ─────────────

/** Identity of one decommission run (a node may be decommissioned again after it rejoins). */
export function decomKey(s: Pick<DecommissionStatus, 'node' | 'started_at_ms'>): string {
  return `${s.node}@${s.started_at_ms}`;
}

/** Newest run per node. */
export function latestByNode(list: readonly DecommissionStatus[]): Record<string, DecommissionStatus> {
  const out: Record<string, DecommissionStatus> = {};
  for (const s of list) if (!out[s.node] || out[s.node]!.started_at_ms < s.started_at_ms) out[s.node] = s;
  return out;
}

/** Fast while something runs (or the registry says a node is decommissioning), slow otherwise. */
export function decommissionPollMs(byNode: Readonly<Record<string, DecommissionStatus>>, nodes: readonly { status: NodeStatus }[]): number {
  const running = Object.values(byNode).some((s) => !isTerminal(s.state)) || nodes.some((n) => n.status === 'decommissioning');
  return running ? 2000 : 10_000;
}

/**
 * The status a present node's card shows: running ones always; a finished one until dismissed —
 * except `removed`, which on a present node means it rejoined fresh afterwards (old news).
 */
export function cardStatus(s: DecommissionStatus | undefined, dismissed: ReadonlySet<string>): DecommissionStatus | undefined {
  if (!s) return undefined;
  if (!isTerminal(s.state)) return s;
  if (s.state === 'removed' || dismissed.has(decomKey(s))) return undefined;
  return s;
}

/** Finished decommissions whose node left the registry (shown as "removed" cards until dismissed). */
export function departedNodes(nodes: readonly { id: string }[], byNode: Readonly<Record<string, DecommissionStatus>>, dismissed: ReadonlySet<string>): DecommissionStatus[] {
  const present = new Set(nodes.map((n) => n.id));
  return Object.values(byNode)
    .filter((s) => !present.has(s.node) && !dismissed.has(decomKey(s)))
    .sort((a, b) => a.node.localeCompare(b.node, undefined, { numeric: true }));
}

/** Cancel (= rebalance cancel) only applies while replicas are moving. */
export function canCancel(s: DecommissionStatus | undefined): boolean {
  return s?.state === 'moving_replicas';
}

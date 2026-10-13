/** Pure shaping of `ClusterStatus` (crates/fluxmq-proto/src/cluster.rs) for the Nodes screen. */
import type { ClusterStatus, NodeInfo, NodeStatus, PartitionAssignment } from '../api/types';
import { localized } from './labels';

const NODE_STATUSES = ['up', 'down', 'joining', 'leaving', 'decommissioning'] as const satisfies readonly NodeStatus[];

/** Translated on access (current console language). */
export const NODE_STATUS_LABEL: Readonly<Record<NodeStatus, string>> = localized(NODE_STATUSES, 'nodes.status.');

/** Hover text for a non-up status (node cards, load bars, matrix legend). */
export const NODE_STATUS_HINT: Readonly<Record<NodeStatus, string>> = localized(NODE_STATUSES, 'nodes.statusHint.');

/** Colour of a status chip: problems red/amber, an operator-driven removal in the primary colour. */
export function statusTone(status: NodeStatus): '' | 'critical' | 'warning' | 'primary' {
  return status === 'down' ? 'critical' : status === 'decommissioning' ? 'primary' : status === 'up' ? '' : 'warning';
}

/** Stable colour slot per node (sorted by id), used for "leader" fills. 6 slots, then repeat. */
export function nodeSlots(nodes: readonly Pick<NodeInfo, 'id'>[]): Record<string, number> {
  const out: Record<string, number> = {};
  [...nodes]
    .map((n) => n.id)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
    .forEach((id, i) => (out[id] = i % 6));
  return out;
}

export interface MatrixCell {
  partition: number;
  leader: string | null;
  replicas: string[];
  isr: string[];
  epoch: number;
  /** ISR smaller than the replica set. */
  underReplicated: boolean;
  /** No live leader (no ISR member alive) — unavailable for publish and consume. */
  offline: boolean;
  /** Max follower lag (messages) from `partitions`, when reported. */
  maxLag: number | null;
  /** Leader is not the preferred (first) replica — e.g. after a failover. */
  notPreferred: boolean;
}

export interface MatrixRow {
  channel: string;
  cells: MatrixCell[];
  underReplicated: number;
  offline: number;
  /** Replication factor of this channel (max replica count). */
  rf: number;
}

/**
 * Channels × partitions, coloured by leader. A partition is offline when it has no leader or
 * its leader is not up; problem rows sort first.
 */
export function partitionMatrix(status: ClusterStatus): MatrixRow[] {
  const up = new Set(status.metadata.nodes.filter((n) => n.status !== 'down').map((n) => n.id));
  const lag = new Map<string, number>();
  for (const p of status.partitions) {
    const vals = Object.values(p.follower_lag);
    lag.set(`${p.channel}/${p.partition}`, vals.length ? Math.max(...vals) : 0);
  }
  const rows: MatrixRow[] = [];
  for (const [channel, parts] of Object.entries(status.metadata.channels)) {
    const cells = [...parts]
      .sort((a, b) => a.partition - b.partition)
      .map((a: PartitionAssignment): MatrixCell => {
        const offline = a.leader == null || !up.has(a.leader);
        return {
          partition: a.partition,
          leader: a.leader,
          replicas: a.replicas,
          isr: a.isr,
          epoch: a.epoch,
          underReplicated: !offline && a.isr.length < a.replicas.length,
          offline,
          maxLag: lag.get(`${channel}/${a.partition}`) ?? null,
          notPreferred: a.leader != null && a.replicas[0] !== a.leader,
        };
      });
    rows.push({
      channel,
      cells,
      underReplicated: cells.filter((c) => c.underReplicated).length,
      offline: cells.filter((c) => c.offline).length,
      rf: Math.max(0, ...cells.map((c) => c.replicas.length)),
    });
  }
  return rows.sort((a, b) => b.offline - a.offline || b.underReplicated - a.underReplicated || a.channel.localeCompare(b.channel));
}

export interface NodeLoad {
  leaders: number;
  replicas: number;
}

/** Leaderships and replicas hosted per node. */
export function nodeLoad(status: ClusterStatus): Record<string, NodeLoad> {
  const out: Record<string, NodeLoad> = {};
  for (const n of status.metadata.nodes) out[n.id] = { leaders: 0, replicas: 0 };
  for (const parts of Object.values(status.metadata.channels)) {
    for (const a of parts) {
      if (a.leader) (out[a.leader] ??= { leaders: 0, replicas: 0 }).leaders++;
      for (const r of a.replicas) (out[r] ??= { leaders: 0, replicas: 0 }).replicas++;
    }
  }
  return out;
}

/** Status-based, like `ClusterOverview.nodes_up`: every member that isn't `down` (a node that is
 * `leaving` after a drain, or `joining`, is still running and reachable). */
export function nodesUp(status: ClusterStatus): { up: number; total: number } {
  const nodes = status.metadata.nodes;
  return { up: nodes.filter((n) => n.status !== 'down').length, total: nodes.length };
}

export function totalPartitions(status: ClusterStatus): number {
  return Object.values(status.metadata.channels).reduce((s, p) => s + p.length, 0);
}

/** Node ordering for cards: controller first, then problems, then id. */
export function sortNodes<T extends Pick<NodeInfo, 'id' | 'status' | 'is_controller'>>(nodes: readonly T[]): T[] {
  const rank: Record<NodeStatus, number> = { down: 0, decommissioning: 1, leaving: 2, joining: 3, up: 4 };
  return [...nodes].sort((a, b) => Number(b.is_controller) - Number(a.is_controller) || rank[a.status] - rank[b.status] || a.id.localeCompare(b.id, undefined, { numeric: true }));
}

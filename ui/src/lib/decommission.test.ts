import { describe, expect, it } from 'vitest';
import type { ClusterStatus, DecommissionStatus, NodeInfo, NodeStatus } from '../api/types';
import { NODE_STATUS_LABEL, sortNodes, statusTone } from './cluster';
import { canCancel, cardStatus, conflictView, decomKey, decommissionChip, decommissionPollMs, decommissionPreview, decommissionProgress, departedNodes, forgetGate, isTerminal, latestByNode, previewBlocked } from './decommission';

const node = (id: string, status: NodeStatus = 'up', extra: Partial<NodeInfo> = {}): NodeInfo => ({ id, client_addr: `${id}:7420`, http_addr: `${id}:7421`, status, is_controller: false, is_voter: true, version: '0.5.0', ...extra });

/** 4 nodes, RF 3 over n1–n4 (n4 holds 3 of 4 partitions, leads 1). */
function cluster(nodes: NodeInfo[] = [node('n1', 'up', { is_controller: true }), node('n2'), node('n3'), node('n4')]): ClusterStatus {
  const a = (partition: number, replicas: string[]) => ({ partition, leader: replicas[0]!, replicas, isr: replicas, epoch: 1 });
  return {
    metadata: {
      cluster_name: 't',
      epoch: 1,
      controller: 'n1',
      nodes,
      channels: {
        orders: [a(0, ['n1', 'n2', 'n4']), a(1, ['n4', 'n3', 'n1'])],
        email: [a(0, ['n2', 'n3', 'n4']), a(1, ['n1', 'n2', 'n3'])],
      },
    },
    replication_factor: 3,
    under_replicated_partitions: 0,
    offline_partitions: 0,
    raft: { term: 1, leader: 'n1', last_log_index: 1, committed_index: 1, voters: nodes.filter((n) => n.is_voter).map((n) => n.id), learners: nodes.filter((n) => !n.is_voter).map((n) => n.id) },
    partitions: [],
  };
}

const st = (state: DecommissionStatus['state'], remaining = 0, extra: Partial<DecommissionStatus> = {}): DecommissionStatus => ({ node: 'n4', state, replicas_remaining: remaining, initial_replicas: 24, started_at_ms: 1000, finished_at_ms: null, error: null, ...extra });

describe('decommissioning status', () => {
  it('has a label, primary tone and sorts right after down nodes', () => {
    expect(NODE_STATUS_LABEL.decommissioning).toBe('Decommissioning');
    expect(statusTone('decommissioning')).toBe('primary');
    expect(statusTone('down')).toBe('critical');
    expect(statusTone('leaving')).toBe('warning');
    expect(statusTone('up')).toBe('');
    const sorted = sortNodes([node('n2'), node('n3', 'decommissioning'), node('n4', 'down'), node('n1', 'up', { is_controller: true })]);
    expect(sorted.map((n) => n.id)).toEqual(['n1', 'n4', 'n3', 'n2']);
  });
});

describe('progress chip', () => {
  it('maps every state', () => {
    expect(decommissionChip(st('moving_replicas', 7))).toMatchObject({ tone: 'primary', active: true, detail: '7 replicas remaining on this node' });
    expect(decommissionChip(st('moving_replicas', 1)).detail).toBe('1 replica remaining on this node');
    expect(decommissionChip(st('leaving_membership'))).toMatchObject({ label: 'Leaving membership', active: true });
    expect(decommissionChip(st('removed'))).toMatchObject({ tone: 'success', active: false, detail: 'Removed from the cluster — safe to stop the process' });
    expect(decommissionChip(st('failed', 3, { error: 'move orders/1 rolled back' }))).toMatchObject({ tone: 'critical', detail: 'move orders/1 rolled back — the node is back up', cancelled: false });
    expect(decommissionChip(st('failed')).detail).toBe('The server reported no reason — the node is back up');
    // The contract's cancel outcome.
    expect(decommissionChip(st('failed', 5, { error: 'cancelled' }))).toMatchObject({ label: 'Decommission cancelled', tone: 'warning', cancelled: true });
    expect(isTerminal('removed') && isTerminal('failed') && !isTerminal('moving_replicas')).toBe(true);
  });

  it('progress uses initial_replicas as the baseline', () => {
    expect(decommissionProgress(st('moving_replicas', 6))).toBeCloseTo(0.75);
    expect(decommissionProgress(st('moving_replicas', 0, { initial_replicas: 0 }))).toBeNull();
    expect(decommissionProgress(st('leaving_membership'))).toBe(1);
    expect(decommissionProgress(st('removed'))).toBe(1);
  });

  it('cancel only while replicas move', () => {
    expect(canCancel(st('moving_replicas', 3))).toBe(true);
    expect(canCancel(st('leaving_membership'))).toBe(false);
    expect(canCancel(undefined)).toBe(false);
  });
});

describe('confirm-dialog projection', () => {
  it('counts replicas, leaders, channels and targets', () => {
    const p = decommissionPreview(cluster(), 'n4', { state: 'idle' });
    expect(p).toMatchObject({ replicas: 3, leaders: 1, channels: 2, targets: ['n1', 'n2', 'n3'], rfBlocked: [], planBusy: false, quorumRisk: null, isController: false });
    expect(previewBlocked(p)).toBe(false);
  });

  it('predicts the 409s: RF cannot be held, plan running, quorum', () => {
    const three = cluster([node('n1', 'up', { is_controller: true }), node('n2'), node('n4'), node('n3', 'down')]);
    const p = decommissionPreview(three, 'n4', { state: 'running' });
    expect(p.targets).toEqual(['n1', 'n2']);
    expect(p.rfBlocked).toEqual([
      { channel: 'orders', rf: 3 },
      { channel: 'email', rf: 3 },
    ]);
    expect(p.planBusy).toBe(true);
    // 4 voters → 3 remain (n1, n2 up, n3 down): 2 live ≥ quorum 2 → fine.
    expect(p.quorumRisk).toBeNull();
    const twoDown = cluster([node('n1', 'up', { is_controller: true }), node('n2', 'down'), node('n3', 'down'), node('n4')]);
    expect(decommissionPreview(twoDown, 'n4', null).quorumRisk).toMatch(/below a Raft quorum of 2/);
    expect(previewBlocked(p)).toBe(true);
    // A learner never threatens the quorum.
    expect(decommissionPreview(cluster([node('n1'), node('n2', 'down'), node('n3', 'down'), node('n4', 'up', { is_voter: false })]), 'n4', null).quorumRisk).toBeNull();
  });

  it('knows the controller', () => {
    expect(decommissionPreview(cluster(), 'n1', null).isController).toBe(true);
  });
});

describe('forget gate', () => {
  it('only for down nodes with no replicas', () => {
    expect(forgetGate('up', 0).visible).toBe(false);
    expect(forgetGate('decommissioning', 0).visible).toBe(false);
    expect(forgetGate('down', 0)).toEqual({ visible: true, enabled: true, reason: null });
    expect(forgetGate('down', 4)).toMatchObject({ visible: true, enabled: false });
    expect(forgetGate('down', 4).reason).toMatch(/Still holds 4 replicas/);
    expect(forgetGate('down', null).enabled).toBe(false);
  });
});

describe('409 explanations', () => {
  it('shows the server reason verbatim and hints from local facts, not the text', () => {
    const busy = decommissionPreview(cluster(), 'n4', { state: 'running' });
    const v = conflictView({ status: 409, code: 'conflict', message: 'a rebalance plan is running' }, 'decommission', busy);
    expect(v.message).toBe('a rebalance plan is running');
    expect(v.hint).toMatch(/Rebalancing panel/);
    const rf = decommissionPreview(cluster([node('n1'), node('n2'), node('n4')]), 'n4', { state: 'idle' });
    expect(conflictView({ status: 409, code: 'conflict', message: 'x' }, 'decommission', rf).hint).toMatch(/Add a node.*orders, email/);
    expect(conflictView({ status: 409, code: 'conflict', message: 'node n3 still holds 2 replicas' }, 'forget').hint).toMatch(/down and holds no replicas/);
    expect(conflictView({ status: 500, code: 'internal', message: 'boom' }, 'forget')).toEqual({ title: 'Couldn’t forget the node', message: 'boom', hint: null });
  });
});

describe('tracking (GET /api/cluster/decommissions)', () => {
  const n = (node: string, state: DecommissionStatus['state'], started = 1000) => st(state, 0, { node, started_at_ms: started });

  it('keeps the newest run per node', () => {
    const by = latestByNode([n('n4', 'removed', 1000), n('n4', 'moving_replicas', 5000), n('n6', 'failed')]);
    expect(by.n4!.state).toBe('moving_replicas');
    expect(Object.keys(by).sort()).toEqual(['n4', 'n6']);
  });

  it('polls fast only while something runs', () => {
    expect(decommissionPollMs({ n4: n('n4', 'moving_replicas') }, [])).toBe(2000);
    expect(decommissionPollMs({ n4: n('n4', 'removed') }, [{ status: 'up' }])).toBe(10_000);
    expect(decommissionPollMs({}, [{ status: 'decommissioning' }])).toBe(2000);
  });

  it('cards: running always, finished until dismissed, removed never on a present (rejoined) node', () => {
    const failed = n('n4', 'failed');
    expect(cardStatus(n('n4', 'moving_replicas'), new Set())).toBeDefined();
    expect(cardStatus(failed, new Set())).toBe(failed);
    expect(cardStatus(failed, new Set([decomKey(failed)]))).toBeUndefined();
    expect(cardStatus(n('n4', 'removed'), new Set())).toBeUndefined();
  });

  it('departed: listed nodes no longer in the registry, minus dismissed runs', () => {
    const by = latestByNode([n('n4', 'removed'), n('n7', 'removed', 2000), n('n1', 'failed')]);
    const nodes = [{ id: 'n1' }];
    expect(departedNodes(nodes, by, new Set()).map((d) => d.node)).toEqual(['n4', 'n7']);
    expect(departedNodes(nodes, by, new Set(['n7@2000'])).map((d) => d.node)).toEqual(['n4']);
  });
});

import { describe, expect, it } from 'vitest';
import type { ClusterStatus, NodeInfo } from '../api/types';
import { nodeLoad, nodeSlots, nodesUp, partitionMatrix, sortNodes, totalPartitions } from './cluster';

const node = (id: string, status: NodeInfo['status'] = 'up', ctl = false): NodeInfo => ({ id, client_addr: `${id}:7420`, http_addr: `${id}:7421`, status, is_controller: ctl, is_voter: true, version: '0.2.0' });

const status = (): ClusterStatus => ({
  metadata: {
    cluster_name: 'c',
    epoch: 9,
    controller: 'n1',
    nodes: [node('n1', 'up', true), node('n2'), node('n3', 'down')],
    channels: {
      'orders.events': [
        { partition: 1, leader: 'n2', replicas: ['n3', 'n2', 'n1'], isr: ['n2', 'n1'], epoch: 5 },
        { partition: 0, leader: 'n1', replicas: ['n1', 'n2', 'n3'], isr: ['n1', 'n2', 'n3'], epoch: 3 },
      ],
      audit: [{ partition: 0, leader: 'n3', replicas: ['n3'], isr: ['n3'], epoch: 1 }],
      zeta: [{ partition: 0, leader: 'n1', replicas: ['n1', 'n2'], isr: ['n1', 'n2'], epoch: 1 }],
    },
  },
  replication_factor: 3,
  under_replicated_partitions: 1,
  offline_partitions: 1,
  raft: { term: 3, leader: 'n1', last_log_index: 10, committed_index: 10, voters: ['n1', 'n2', 'n3'], learners: [] },
  partitions: [
    { channel: 'orders.events', partition: 1, leader: 'n2', epoch: 5, high_watermark: 100, log_end_offset: 101, follower_lag: { n1: 4, n3: 900 }, isr: ['n2', 'n1'] },
  ],
});

describe('partition matrix shaping', () => {
  it('orders cells by partition and flags health', () => {
    const rows = partitionMatrix(status());
    const orders = rows.find((r) => r.channel === 'orders.events')!;
    expect(orders.cells.map((c) => c.partition)).toEqual([0, 1]);
    const p1 = orders.cells[1]!;
    expect(p1.underReplicated).toBe(true);
    expect(p1.offline).toBe(false);
    expect(p1.notPreferred).toBe(true); // preferred n3 is down, n2 leads
    expect(p1.maxLag).toBe(900);
    expect(orders.cells[0]!.underReplicated).toBe(false);
    expect(orders.rf).toBe(3);
    expect(orders.underReplicated).toBe(1);
  });

  it('marks partitions led by a down node as offline, not under-replicated', () => {
    const audit = partitionMatrix(status()).find((r) => r.channel === 'audit')!;
    expect(audit.cells[0]!.offline).toBe(true);
    expect(audit.cells[0]!.underReplicated).toBe(false);
  });

  it('sorts problem rows first', () => {
    expect(partitionMatrix(status()).map((r) => r.channel)).toEqual(['audit', 'orders.events', 'zeta']);
  });

  it('counts leaders and replicas per node', () => {
    expect(nodeLoad(status())).toEqual({ n1: { leaders: 2, replicas: 3 }, n2: { leaders: 1, replicas: 3 }, n3: { leaders: 1, replicas: 3 } });
    expect(totalPartitions(status())).toBe(4);
    expect(nodesUp(status())).toEqual({ up: 2, total: 3 });
    // A drained (`leaving`) node is still running: it counts as up, like `nodes_up`.
    const s = status();
    const leaving = { ...s, metadata: { ...s.metadata, nodes: s.metadata.nodes.map((n) => ({ ...n, status: n.status === 'down' ? n.status : ('leaving' as const) })) } };
    expect(nodesUp(leaving)).toEqual({ up: 2, total: 3 });
  });

  it('assigns stable colour slots and orders node cards', () => {
    expect(nodeSlots([{ id: 'n10' }, { id: 'n2' }, { id: 'n1' }])).toEqual({ n1: 0, n2: 1, n10: 2 });
    expect(sortNodes(status().metadata.nodes).map((n) => n.id)).toEqual(['n1', 'n3', 'n2']);
  });
});

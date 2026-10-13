import { describe, expect, it } from 'vitest';
import { profileFileName } from './profile';

describe('profileFileName', () => {
  const at = new Date('2026-10-12T09:08:07.123Z');
  it('names a flame graph after the node and the time', () => {
    expect(profileFileName('node-1', 'svg', at)).toBe('fluxmq-cpu-node-1-2026-10-12T090807.svg');
  });
  it('keeps folded stacks a text file and sanitizes the node id', () => {
    expect(profileFileName('n1/a b', 'folded', at)).toBe('fluxmq-cpu-n1-a-b-2026-10-12T090807.folded.txt');
  });
});

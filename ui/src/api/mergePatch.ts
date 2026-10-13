/**
 * RFC 7386 JSON Merge Patch.
 *
 * Objects merge recursively, `null` deletes the key, arrays and scalars replace.
 * Pure and structural-sharing: untouched subtrees keep their identity, so zustand
 * selectors on unchanged parts of a topic don't re-render. The target is never mutated.
 */
export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

export function applyMergePatch<T = unknown>(target: unknown, patch: unknown): T {
  if (!isPlainObject(patch)) return patch as T;
  const base: Record<string, unknown> = isPlainObject(target) ? target : {};
  let out: Record<string, unknown> | null = null;
  for (const key of Object.keys(patch)) {
    const pv = patch[key];
    if (pv === null) {
      if (key in base) {
        out ??= { ...base };
        delete out[key];
      }
      continue;
    }
    const prev = base[key];
    const next = applyMergePatch(prev, pv);
    if (next !== prev) {
      out ??= { ...base };
      out[key] = next;
    }
  }
  if (out) return out as T;
  // Nothing changed; if target wasn't an object, the result is still an (empty-merged) object.
  return (isPlainObject(target) ? target : base) as T;
}

/**
 * Build a merge patch that turns `prev` into `next` (used by the mock server and tests).
 * Returns `undefined` when there is no difference.
 */
export function diffMergePatch(prev: unknown, next: unknown): unknown {
  if (isPlainObject(prev) && isPlainObject(next)) {
    const patch: Record<string, unknown> = {};
    let changed = false;
    for (const key of Object.keys(prev)) {
      if (!(key in next) || next[key] === undefined) {
        patch[key] = null;
        changed = true;
      }
    }
    for (const key of Object.keys(next)) {
      const nv = next[key];
      if (nv === undefined) continue;
      if (!(key in prev)) {
        patch[key] = nv;
        changed = true;
        continue;
      }
      // A null value cannot be expressed in a merge patch except as deletion; callers
      // should avoid null leaves or accept that semantics (RFC 7386 limitation).
      const d = diffMergePatch(prev[key], nv);
      if (d !== undefined) {
        patch[key] = d;
        changed = true;
      }
    }
    return changed ? patch : undefined;
  }
  if (Array.isArray(prev) && Array.isArray(next)) {
    return JSON.stringify(prev) === JSON.stringify(next) ? undefined : next;
  }
  return Object.is(prev, next) ? undefined : next;
}

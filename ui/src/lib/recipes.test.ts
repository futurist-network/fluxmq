import { describe, expect, it } from 'vitest';
import { MAX_BYTES_PRESETS, parseBackoffList, RETENTION_PRESETS, validateCreateForm } from './presets';
import { RECIPES, recipeForm } from './recipes';
import { en } from '../i18n/en';
import ruMd from '../docs/content/ru/recipes.md?raw';
import enMd from '../docs/content/en/recipes.md?raw';

const DOCS = { ru: ruMd, en: enMd };

/** The body of the `## … {#id}` section, up to the next `## ` heading. */
function section(doc: string, id: string): string | null {
  const start = doc.search(new RegExp(`^## .*\\{#${id}\\}\\s*$`, 'm'));
  if (start < 0) return null;
  const rest = doc.slice(start + 3);
  const end = rest.search(/^## /m);
  return end < 0 ? rest : rest.slice(0, end);
}

describe('recipe templates', () => {
  it('has the 8 documented recipes with unique ids and names', () => {
    expect(RECIPES.map((r) => r.id)).toEqual(['orders-keyed', 'jobs-queue', 'event-log', 'fanout', 'content-routing', 'webhook-push', 'retry-poison', 'telemetry-ingest']);
    expect(new Set(RECIPES.map((r) => r.suggestedName)).size).toBe(RECIPES.length);
  });

  for (const r of RECIPES) {
    describe(r.id, () => {
      it('passes the wizard validation', () => {
        const f = recipeForm(r);
        const v = validateCreateForm(f);
        expect(v.name).toBeNull();
        expect(v.partitions).toBe(false);
        expect(v.advanced).toBeNull();
        // What the wizard submits equals the recipe (ack timeout: seconds in the form, ms in the API).
        expect(v.backoffMs).toEqual(r.backoffMs);
        expect(Math.round(Number(f.ackTimeout) * 1000)).toBe(r.ackTimeoutMs);
        expect(Number(f.attempts)).toBe(r.attempts);
        expect(Number(f.partitions)).toBe(r.partitions);
        expect(parseBackoffList(f.backoff)).toEqual(r.backoffMs);
      });

      it('uses retention and size values the wizard offers', () => {
        expect(RETENTION_PRESETS.map((p) => p.secs)).toContain(r.maxAgeSecs);
        expect(MAX_BYTES_PRESETS.map((p) => p.bytes)).toContain(r.maxBytes);
      });

      it('keyed recipes set an ordering key', () => {
        if (r.mode === 'keyed') expect(r.orderingKey).toBeTruthy();
      });

      it('has a docs anchor in ru and en, matching the documented config when one is shown', () => {
        for (const lang of ['ru', 'en'] as const) {
          const body = section(DOCS[lang], r.id);
          expect(body, `${lang}/recipes.md has no {#${r.id}}`).not.toBeNull();
          const m = new RegExp(`"name": "${r.suggestedName.replace('.', '\\.')}", "mode": "(\\w+)", "partitions": (\\d+),\\s*"retention": (\\{[^}]*\\}),\\s*"retry": (\\{[^}]*\\})`).exec(body!);
          // fan-out / routing reuse the event-log config and show no curl body of their own
          if (!body!.includes(`"name": "${r.suggestedName}", "mode"`)) continue;
          expect(m, `${lang}: curl body of ${r.id} not parsed`).not.toBeNull();
          expect(m![1]).toBe(r.mode);
          expect(Number(m![2])).toBe(r.partitions);
          expect(JSON.parse(m![3]!)).toEqual({ max_age_secs: r.maxAgeSecs, max_bytes: r.maxBytes });
          expect(JSON.parse(m![4]!)).toEqual({ max_attempts: r.attempts, backoff_ms: r.backoffMs, ack_timeout_ms: r.ackTimeoutMs });
        }
      });

      it('has title, summary and next steps in the dictionary', () => {
        for (const part of ['title', 'summary', 'next']) expect(en).toHaveProperty([`recipes.${r.id}.${part}`]);
      });
    });
  }

  it('checks the documented config for at least the six recipes that show one', () => {
    const shown = RECIPES.filter((r) => section(DOCS.ru, r.id)?.includes(`"name": "${r.suggestedName}", "mode"`));
    expect(shown.length).toBeGreaterThanOrEqual(6);
  });
});

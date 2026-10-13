/**
 * "Ready-made setups" for the Create channel wizard — the single source of the values that
 * docs/content/{ru,en}/recipes.md documents (each `id` is the anchor of its section there,
 * `/docs/recipes#<id>`). recipes.test.ts checks that every recipe passes the wizard's
 * validation, uses retention / size values the wizard offers, and has an anchor in both languages.
 */
import type { ChannelMode } from '../api/types';
import type { TKey } from '../i18n';
import { channelPath } from './labels';
import { formatBackoffList, type CreateForm } from './presets';

export type RecipeId = 'orders-keyed' | 'jobs-queue' | 'event-log' | 'fanout' | 'content-routing' | 'webhook-push' | 'retry-poison' | 'telemetry-ingest';

/** Where a "next step" link leads after the channel is created. */
export type NextLink = 'consumers' | 'route' | 'webhook' | 'dlq' | 'newChannel';

export interface Recipe {
  id: RecipeId;
  mode: ChannelMode;
  partitions: number;
  /** Seconds; null = keep forever. Must be one of RETENTION_PRESETS. */
  maxAgeSecs: number | null;
  /** Bytes; null = unlimited. Must be one of MAX_BYTES_PRESETS. */
  maxBytes: number | null;
  attempts: number;
  backoffMs: number[];
  ackTimeoutMs: number;
  dlq: boolean;
  orderingKey: string | null;
  suggestedName: string;
  /** Links shown with the next steps (besides the full recipe in the docs). */
  next: NextLink[];
}

const DAY = 86_400;
const GB100 = 100 * 1024 ** 3;
const STD_RETRY = { attempts: 5, backoffMs: [1_000, 10_000, 60_000, 300_000], ackTimeoutMs: 30_000 };

export const RECIPES: readonly Recipe[] = [
  { id: 'orders-keyed', mode: 'keyed', partitions: 16, maxAgeSecs: 7 * DAY, maxBytes: null, attempts: 5, backoffMs: [1_000, 5_000, 30_000, 120_000], ackTimeoutMs: 30_000, dlq: true, orderingKey: 'order_id', suggestedName: 'orders.commands', next: ['consumers'] },
  { id: 'jobs-queue', mode: 'queue', partitions: 1, maxAgeSecs: DAY, maxBytes: null, attempts: 8, backoffMs: [10_000, 60_000, 300_000, 900_000, 1_800_000], ackTimeoutMs: 60_000, dlq: true, orderingKey: null, suggestedName: 'email.send', next: ['consumers', 'dlq'] },
  { id: 'event-log', mode: 'stream', partitions: 8, maxAgeSecs: 30 * DAY, maxBytes: GB100, ...STD_RETRY, dlq: true, orderingKey: null, suggestedName: 'audit.events', next: ['consumers'] },
  { id: 'fanout', mode: 'stream', partitions: 8, maxAgeSecs: 7 * DAY, maxBytes: null, ...STD_RETRY, dlq: true, orderingKey: null, suggestedName: 'user.events', next: ['consumers'] },
  { id: 'content-routing', mode: 'stream', partitions: 8, maxAgeSecs: 7 * DAY, maxBytes: null, ...STD_RETRY, dlq: true, orderingKey: null, suggestedName: 'orders.events', next: ['newChannel', 'route'] },
  { id: 'webhook-push', mode: 'stream', partitions: 8, maxAgeSecs: 3 * DAY, maxBytes: null, attempts: 10, backoffMs: [5_000, 30_000, 120_000, 600_000, 1_800_000], ackTimeoutMs: 30_000, dlq: true, orderingKey: null, suggestedName: 'crm.events', next: ['webhook'] },
  { id: 'retry-poison', mode: 'queue', partitions: 1, maxAgeSecs: 3 * DAY, maxBytes: null, attempts: 6, backoffMs: [30_000, 120_000, 600_000, 1_800_000, 3_600_000], ackTimeoutMs: 60_000, dlq: true, orderingKey: null, suggestedName: 'partner.sync', next: ['consumers', 'dlq'] },
  { id: 'telemetry-ingest', mode: 'stream', partitions: 32, maxAgeSecs: DAY, maxBytes: GB100, attempts: 3, backoffMs: [1_000, 10_000], ackTimeoutMs: 30_000, dlq: true, orderingKey: null, suggestedName: 'telemetry.metrics', next: ['consumers'] },
];

export const recipeById = (id: string | null | undefined): Recipe | undefined => RECIPES.find((r) => r.id === id);

/** The wizard's text inputs as a recipe fills them (ack timeout in seconds, backoff as "1s, 5s"). */
export function recipeForm(r: Recipe): CreateForm {
  return {
    name: r.suggestedName,
    partitions: String(r.partitions),
    attempts: String(r.attempts),
    backoff: formatBackoffList(r.backoffMs),
    ackTimeout: String(r.ackTimeoutMs / 1000),
  };
}

export const recipeDocsHref = (id: RecipeId) => `/docs/recipes#${id}`;

/** i18n keys of a recipe (dictionary namespace `recipes`). */
export const recipeKey = (id: RecipeId, part: 'title' | 'summary' | 'next') => `recipes.${id}.${part}` as TKey;

export function nextLinkHref(link: NextLink, channel: string): string | null {
  switch (link) {
    case 'consumers':
      return channelPath(channel, 'consumers');
    case 'route':
      return `/routing/new?source=${encodeURIComponent(channel)}`;
    case 'webhook':
      return `/webhooks/new?channel=${encodeURIComponent(channel)}`;
    case 'dlq':
      return `/dlq?channel=${encodeURIComponent(channel)}`;
    case 'newChannel':
      return null; // reopens the wizard
  }
}

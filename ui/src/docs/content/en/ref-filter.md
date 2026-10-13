---
title: Filter language
group: reference
order: 5
summary: Conditions for message search, routes and webhooks — fields, operators, comparison rules and examples.
---

The same language is used in message search (a channel's Messages tab, the `filter` parameter over REST), in [route](/docs/routing) WHEN conditions and in [webhook](/docs/webhooks) filters. Conditions are joined with `AND`; there is no `OR` — for "either", create several routes. Keywords are case-insensitive.

## Fields {#fields}

| Field | What it is |
|---|---|
| `key` | the message key |
| `partition` | the partition number |
| `header.<name>` | a header, e.g. `header.flux-type` |
| `a.b.0.c` | a path into the JSON payload; numbers are array indexes |
| `payload.<path>` | an explicit payload path. **Required** for fields named `key` and `partition`: without the prefix they mean the message key and partition, while `payload.key` is the JSON field `key` |

## Operators {#operators}

| Operator | Example |
|---|---|
| `=`, `!=` | `country = MD`, `status != cancelled` |
| `>`, `>=`, `<`, `<=` | `amount >= 100`, `created_at >= "2026-10-01"` |
| `in [..]`, `not in [..]` | `country in [MD, RO]` |
| `prefix` | `header.flux-type prefix payment.` |
| `contains` | `tags contains vip` (a substring of a string or an element of an array) |
| `exists`, `not exists` | `header.x-trace-id exists` |

## Comparison rules {#rules}

- numbers compare as numbers, strings lexicographically;
- a number equals a string holding that number: `order_id = 8842` finds both `8842` and `"8842"`;
- strings without spaces need no quotes (`country = MD`);
- a missing field matches no operator except `!=`, `not in` and `not exists`;
- payload conditions never match a non-JSON payload.

## Examples {#examples}

```text
order_id = 8842
type = "order.created" AND country = "MD" AND amount >= 100
country in [MD, RO] AND header.flux-type prefix payment.
customer.tier != gold AND items.0.sku = "A-1"
tags contains vip
header.x-trace-id exists
key prefix "user-" AND partition = 3
created_at >= "2026-10-01"
```

## Errors {#errors}

A syntax error comes back with a byte position — the console underlines that spot:

```bash
curl -s -X POST localhost:7421/api/filter/parse -H 'content-type: application/json' -d '{"filter":"country ~ MD"}'
# {"error":"bad_request","message":"filter syntax error at 8: unexpected character '~'","position":8}
```

`POST /api/filter/parse` turns text into conditions (what the route editor does), `POST /api/filter/render` turns conditions back into text.

## Performance {#performance}

There is no payload index: search scans messages until it has enough matches, at most 200,000 per query (around 2 million messages per second, i.e. ~0.1 s). The answer's `scanned` is how many were looked at; `has_more: true` means the scan stopped before the end of the range — narrow the time, partition or key. A `key` search without a partition only scans that key's partition.

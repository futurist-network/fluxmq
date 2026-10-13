---
title: Error codes
group: reference
order: 4
summary: Error codes of the REST API, the client protocol and the SDK, and session close reasons — what each means and what to do.
---

## REST API {#http}

An error is a non-2xx answer with `{"error": "<code>", "message": "<text>"}`. Branch on `error`, never on `message`. Filter syntax errors also carry `position` — the byte offset of the error.

```json
{"error":"bad_request","message":"filter syntax error at 8: unexpected character '~'","position":8}
```

| HTTP | `error` | Meaning | What to do |
|---|---|---|---|
| 400 | `bad_request` | invalid request: name, parameters, filter syntax, a webhook address blocked by the SSRF policy | fix the request as `message` says |
| 400 | `unsupported_in_cluster` | the operation can't run in this cluster (e.g. a backup with a node older than 0.3.0) | upgrade the nodes |
| 400 | `unsupported_single_node` | a cluster operation on a single node (rebalancing, decommissioning) | cluster only |
| 401 | `unauthorized` | the token is missing, invalid or expired | send a valid `Authorization: Bearer …` |
| 403 | `forbidden` | the token is valid but its role or scopes aren't enough | a higher role is needed — see [roles](/docs/access-tokens#roles) |
| 404 | `not_found` | no such channel, group, route, node… | check the name or id |
| 409 | `conflict` | a state conflict: the channel exists, the group has consumers, another plan or backup is running, a concurrent change | wait or fix what `message` names |
| 409 | `last_admin` | revoking the last admin token | issue another admin token first |
| 503 | `unavailable` | temporarily unavailable (e.g. notification history while a partition has no leader) | retry later |
| 500 / 503 | `internal` | an internal error or a transient unavailability (leader change, no metadata quorum) | retry; a 503 "partitions … are changing leaders; retry" clears within seconds |

## Client protocol {#wire}

Broker response errors (`ErrorCode`) surface in the SDK as `ClientError::Server`:

| Code | Meaning | SDK retries? |
|---|---|---|
| `BadRequest` | an invalid request (including `nack` in cumulative mode, an unknown protocol version on an old broker) | no |
| `Unauthorized` | missing or invalid token on connect | no |
| `Forbidden` | role or scopes aren't enough; the connection stays open | no |
| `NotFound` | no such channel (in a cluster retried while the metadata still lists it) | no |
| `AlreadyExists` | the channel already exists | no |
| `NotLeader` | this node doesn't lead the partition (`message` holds the leader id) | yes, after refreshing metadata |
| `Overloaded` | the broker is overloaded or the ISR is below `--min-isr` ("not enough replicas") | yes |
| `ProtocolViolation` | a protocol violation, e.g. plaintext on a TLS port: `this port requires TLS` | no |
| `Internal` | an internal broker error | — |

SDK errors (`ClientError`): `Io` (network, including `TLS handshake with …`), `Protocol` (e.g. `TLS configuration: …` for wrong certificate paths), `Server(…)` (the codes above), `Timeout` (`request_timeout` elapsed), `Closed` (the client was closed or gave up reconnecting). Publishes are retried on connection loss, timeouts, `NotLeader` and `Overloaded` until `publish_timeout`.

## Session closes {#sessions}

| Where | Code / reason | Meaning |
|---|---|---|
| Client protocol | `Goodbye` "token revoked" / "token expired" | the token was revoked or expired |
| Client protocol | `Goodbye` "certificate binding revoked" | the certificate binding was deleted |
| Client protocol | `Goodbye` on broker shutdown | a graceful stop; the SDK reconnects |
| Console WebSocket | code **4401** | the session token was revoked or expired — the console asks for a new one |

## Delivery errors (DLQ, webhooks, notifications) {#delivery}

Short reason classes: `HTTP <code>`, `timeout`, `connection refused`, `connection failed`, `DNS error`, `TLS error`, `blocked by policy` (SSRF), `SMTP <code>`, `route target unavailable: <channel>`. The full text is in `error_detail` / `last_error_detail` and in the console's tooltips.

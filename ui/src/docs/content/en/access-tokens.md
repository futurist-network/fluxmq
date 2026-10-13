---
title: Access tokens and roles
group: console
order: 11
summary: Named tokens with viewer / operator / admin roles and per-channel scopes, live-session revocation, and client certificate bindings.
console: /settings/tokens
---

**Settings → Access tokens** controls who can do what with the broker. Tokens authenticate the console, REST and WebSocket clients and SDK connections.

## Open and enforced mode {#modes}

- **Open mode** — while `--admin-token` is not set and no token exists. Everyone is an anonymous admin, CORS is open, and the console shows an "Auth disabled" warning. Local development only.
- **Enforced** — as soon as `--admin-token` is set or the first token is created. Without a token only `GET /api/health` is reachable.

**Create admin token** in open mode creates the first token: admin role only (anything else gets 400), the names `admin` and `anonymous` are reserved. The broker switches to enforced mode at once and the console keeps the new token in this browser so you don't lock yourself out. Copy the secret anyway — it is shown once.

## Roles {#roles}

| Role | Allowed |
|---|---|
| Viewer (`viewer`) | all reads: screens, messages, consumers, REST `GET`, WebSocket, `/metrics` |
| Operator (`operator`) | + DLQ retry, discard and export, replay, offset resets, REST publish; on the client protocol, publish to and consume from any channel |
| Admin (`admin`) | + create, change and delete channels, delete groups, routes, webhooks, notifications, backups, tokens, cluster operations |

Every `GET` is open to viewers except three: the DLQ export (`GET /api/dlq/export`, operator — a bulk payload dump), the token list and the certificate binding list (admin).

## Scopes {#scopes}

For services, a `viewer` token gets **scopes** — channel globs it may **publish** to (`publish`) and **consume** from (`consume`) over the client protocol. In globs `*` matches any run of characters (dots included) and `?` one character. Example: publish to `orders.*`, consume from `orders.eu`. Without globs a viewer token can neither publish nor consume on the client protocol. Operators and admins don't need scopes — every channel is open to them.

## The token list {#list}

At the top, "Signed in as": the token name and role (or the bootstrap `--admin-token`) and **Sign out**. The table: name, role, scopes, created when and by whom, last used, expiry.

**New token**: name (up to 64 characters), role, "Publish to" and "Consume from" globs, lifetime — 7, 30, 90 days, 1 year or never. The secret `fmq_<id>_<secret>` is shown **once** with a Copy button; the broker stores only a SHA-256, so a lost secret can't be recovered — issue a new token and revoke the old one.

**Revoke** asks for confirmation. Revoking the token you're signed in with signs you out immediately.

## Revocation closes live sessions {#revocation}

When a token is revoked or expires, the broker closes every connection using it **on every node** within ~1 second: a client-protocol connection gets `Goodbye` with `token revoked` / `token expired`, the console WebSocket closes with code **4401**. Reconnecting with that token gets `Unauthorized`. Bootstrap-token and open-mode sessions are not affected.

> [!WARNING]
> **The last admin.** Without `--admin-token` you can't revoke the last active admin token while other tokens exist (`409 last_admin`): issue another admin token first. Revoking the very last token is refused too (400) — it would reopen the broker.

## Certificate bindings {#cert-bindings}

At the bottom of the tab, **Certificate bindings** map a verified client certificate's CN to a role and scopes, like a token. A service with that certificate connects to the client port **without a token**.

- They work only on the client port and only with mTLS (`--tls-client-ca`). Without it bindings are stored but inert — the console shows a warning.
- If the client sends a token, the token decides (an invalid token is not rescued by the certificate). With no token, the binding's role applies. With neither — `Unauthorized`.
- The CN is matched exactly, case-sensitively; a second binding for the same CN is `409` (to change the role, delete and recreate it).
- Deleting a binding closes live connections with that certificate on every node (`Goodbye` with `certificate binding revoked`). CRL and OCSP aren't checked — revoke a compromised certificate by deleting its binding.
- HTTP, REST and the console still require a token. In the audit log such a client shows as `cert:<cn>`.

Issuing certificates and enabling mTLS: [Secure the broker](/docs/howto-secure#mtls) and [Security (TLS)](/docs/security).

## Audit log {#audit}

Every mutating action (channels, DLQ, replay, offset resets, tokens, routes…) is recorded as the token's `name#id` (the bootstrap token as `admin`, a binding as `cert:<cn>`). Read it with `GET /api/audit` or the file `<data-dir>/audit.log`; in a cluster each node keeps its own.

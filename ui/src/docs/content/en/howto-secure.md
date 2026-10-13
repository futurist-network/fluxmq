---
title: Secure the broker
group: howto
order: 7
summary: A checklist before going on a network — the admin token, service tokens with roles and scopes, TLS, mTLS and certificate bindings.
---

By default FluxMQ runs in **open mode**: no auth and no encryption. Great on a laptop, dangerous anywhere else.

> [!WARNING]
> Don't expose ports 7420 and 7421 without a token and TLS — built-in or a TLS proxy (nginx, Caddy) in front. Keep the cluster ports 7422/7423 on a private network.

## 1. Turn auth on {#admin-token}

Option A — a bootstrap token at start (never stored by the broker, handy for automation):

```bash
./target/release/fluxmq --admin-token "$(openssl rand -hex 32)"     # or FLUXMQ_ADMIN_TOKEN=…
```

Option B — from the console: Settings → Access tokens → **Create admin token**. The broker switches to enforced mode at once and the console keeps the token. Copy the secret — it is shown once.

With auth on, REST requires `Authorization: Bearer <token>` (except `/api/health`), the WebSocket a header or `?token=`, SDK clients `auth_token`, the demo and bench `--token` or `FLUXMQ_TOKEN`.

## 2. Give everyone their own token {#tokens}

| Who | Role | Scopes |
|---|---|---|
| A service writing `orders.*` and reading `payments.events` | `viewer` | publish `orders.*`, consume `payments.events` |
| Prometheus | `viewer` | — (reads `/metrics`) |
| The on-call engineer | `operator` | — (DLQ, replay, offset resets) |
| Platform admin | `admin` | — |

```bash
curl -s -X POST localhost:7421/api/auth/tokens \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"name":"orders-svc","role":"viewer",
       "scopes":{"publish":["orders.*"],"consume":["payments.events"]},
       "ttl_secs":2592000}'
# {"token":"fmq_k3x9…_9f2c…","info":{…}}  — the secret is shown once
```

An expired or revoked token cuts its live sessions on every node within ~1 s. Details: [Access tokens and roles](/docs/access-tokens).

## 3. Turn TLS on {#tls}

| Flag | Variable | What it does |
|---|---|---|
| `--tls-cert`, `--tls-key` | `FLUXMQ_TLS_CERT`, `FLUXMQ_TLS_KEY` | TLS on the client port and (by default) HTTPS on the HTTP port |
| `--tls-client-ca` | `FLUXMQ_TLS_CLIENT_CA` | require client certificates signed by this CA (mTLS) |
| `--http-tls-cert`, `--http-tls-key` | `FLUXMQ_HTTP_TLS_CERT`, `FLUXMQ_HTTP_TLS_KEY` | a separate certificate for the console and REST |
| `--http-tls on\|off` | `FLUXMQ_HTTP_TLS` | `off` keeps HTTP plaintext (e.g. behind a TLS proxy) |
| `--cluster-tls-cert`, `--cluster-tls-key`, `--cluster-tls-ca` | `FLUXMQ_CLUSTER_TLS_CERT`, `_KEY`, `_CA` | mutual TLS between nodes; all three required, only with `--peers` |

```bash
./target/release/fluxmq --data-dir ./data \
  --tls-cert ./certs/server.pem --tls-key ./certs/server-key.pem
curl --cacert ./certs/ca.pem https://localhost:7421/api/health
```

Incomplete combinations (a certificate without a key, `--tls-client-ca` without `--tls-cert`) and broken PEM files fail the start instead of silently falling back to plaintext. Development certificates: `fluxmq tls gen-dev` (see [Cluster with TLS](/docs/howto-cluster#certs)). Clients connect to `tls://host:7420`. A plaintext client on a TLS port gets a clear `this port requires TLS` error.

## 4. mTLS and certificate bindings {#mtls}

With `--tls-client-ca` a client must present a certificate signed by that CA, otherwise the handshake fails. The certificate CN shows in the console next to the instance address (`tls:orders-svc`).

mTLS alone answers "who connected"; permissions come from the token. To let a service connect **with its certificate only**, create a **binding**: Settings → Access tokens → Certificate bindings → **New binding** — CN, role and scopes.

```bash
curl -s --cacert ./certs/ca.pem -X POST https://localhost:7421/api/auth/cert-bindings \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"cn":"billing-svc","role":"viewer","scopes":{"publish":["billing.*"],"consume":["billing.events"]}}'
```

The SDK then needs only `TlsOptions::identity_files(…)`, no `auth_token`. Bindings apply to the client port only; the console and REST still require a token.

## 5. The rest {#rest}

- **Webhooks**: with auth enforced, private addresses are blocked (SSRF protection); allow them with `--webhooks-allow-private`.
- **Secrets** of notifiers, webhooks and backups are never returned by the server (`********`) and never reach the audit log or logs.
- **Audit log** — `GET /api/audit` and `<data-dir>/audit.log`: who changed what.
- **Certificate rotation** without a restart — replace the files or send `SIGHUP`, see [Security (TLS)](/docs/security#rotation).

## Checklist {#checklist}

- [ ] `--admin-token` is set or an admin token exists; no "Auth disabled" banner;
- [ ] every service has its own token with the smallest role, scopes and an expiry;
- [ ] TLS on the client and HTTP ports (or a TLS proxy), certificates from your CA;
- [ ] in a cluster — cluster TLS and a shared `--cluster-secret`, Raft/replication ports on a private network;
- [ ] notifications on `tls_cert_expiring`;
- [ ] a second admin token in case the first is lost.

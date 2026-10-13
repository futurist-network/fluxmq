---
title: Security (TLS)
group: console
order: 14
summary: Which ports are encrypted, who must present a certificate, when certificates expire and how they reload without a restart.
console: /settings/security
---

**Settings → Security** shows the transport security of the node serving the console: which ports use TLS, whether a client certificate is required and when certificates expire. TLS itself is configured with flags when the broker starts — see [Secure the broker](/docs/howto-secure#tls).

## Three surfaces {#surfaces}

| Surface | Port | Protects |
|---|---|---|
| Client protocol | 7420 | the SDK's binary protocol; optionally client certificates (mTLS) |
| HTTP · console | 7421 | REST, WebSocket, the web console (HTTPS) |
| Cluster | 7422, 7423, internal HTTP | Raft, replication and `/internal/*` between nodes — always mutual TLS |

It uses rustls, TLS 1.2 and 1.3. Without TLS flags everything is plaintext; plaintext and TLS never mix on one port.

## Overview {#overview}

- **Encrypted surfaces** — which of the three use TLS ("clients, HTTP and cluster" or the list of plaintext ones).
- **Next expiry** — which certificate expires first and when.
- **Handshake failures** — since this node started: plaintext on a TLS port, a missing or foreign client certificate, protocol mismatch, timeout.

## Surface cards {#cards}

Whether TLS is on and mutual; the certificate — subject, issuer, SANs, validity with a "lifetime used" bar, SHA-256 fingerprint; when it was loaded; the last reload error ("Last reload failed — still serving the previous certificate"); handshake failures. When the HTTP port serves the same certificate as the client port, the card says so.

Without TLS the tab shows "This deployment runs without TLS" with hints: the `fluxmq tls gen-dev` command for development certificates, the server flags and the client settings.

## What's normal and what's a problem {#normal}

| Observation | Means |
|---|---|
| A few handshake failures when opening the console with `gen-dev` certificates | normal: the browser doesn't trust the development CA and drops connections |
| Failures while cluster nodes start | normal: peers knock before the node is ready |
| Failures growing steadily | check: something connects in plaintext to a TLS port or with a foreign certificate |
| < 14 days to expiry | `tls_cert_expiring` alert (warning), < 3 days critical; replace the files |
| A reload error | the new PEM is broken or the key doesn't match; the old certificate still serves — fix the files |

## Rotation without a restart {#rotation}

The broker re-reads certificate and key PEM files **every 30 seconds** (when the file's mtime, size or inode changed) and **immediately on `SIGHUP`** (`kill -HUP $(pgrep -x fluxmq)`). New handshakes use the new certificate; open connections are not dropped. Replace files atomically (write a temp file, then `mv`). A failed reload keeps the old certificate and opens a `tls_reload_failed` alert.

> [!NOTE]
> CA files (`--tls-client-ca`, `--cluster-tls-ca`) are read only at start: rotate a CA by restarting nodes one at a time, with both the old and the new CA in the file during the transition. CRL and OCSP are not checked.

## In a cluster {#cluster}

The cards show the certificates of the node answering the console. Expiry and reload failures on **any** node appear as alerts (e.g. `tls_cert_expiring:n2/cluster`), and the controller sends the notification once. Metrics: `fluxmq_tls_handshake_failures_total{surface}` and `fluxmq_tls_cert_expiry_seconds{surface}` — see [Metrics](/docs/ref-metrics).

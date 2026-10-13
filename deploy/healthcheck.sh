#!/bin/sh
# Container healthcheck: GET /api/health on the local HTTP port, HTTPS first (self-signed /
# gen-dev certificates are fine, hence -k), then plain HTTP. A plain request to an HTTPS port
# gets 400 and a TLS handshake to a plaintext port fails, so exactly one of them can succeed.
port="${FLUXMQ_HTTP_ADDR##*:}"
port="${port:-7421}"
curl -fsSk --max-time 2 "https://127.0.0.1:${port}/api/health" >/dev/null 2>&1 \
  || curl -fsS --max-time 2 "http://127.0.0.1:${port}/api/health" >/dev/null

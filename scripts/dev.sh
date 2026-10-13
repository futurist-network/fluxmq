#!/usr/bin/env bash
# Local development loop: broker (debug build) + demo traffic + UI dev server.
#
#   scripts/dev.sh              # everything
#   NO_DEMO=1 scripts/dev.sh    # skip the demo traffic generator
#   NO_UI=1 scripts/dev.sh      # skip the Vite dev server
#
# Ctrl-C stops all of them. Data lives in ./data-dev (delete it for a clean slate).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATA_DIR="${FLUXMQ_DATA_DIR:-$ROOT/data-dev}"
CLIENT_ADDR="${FLUXMQ_CLIENT_ADDR:-127.0.0.1:7420}"
HTTP_ADDR="${FLUXMQ_HTTP_ADDR:-127.0.0.1:7421}"
export RUST_LOG="${RUST_LOG:-info}"

PIDS=()
cleanup() {
  trap - INT TERM EXIT
  echo
  echo "dev.sh: stopping..."
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill -TERM "$pid" 2>/dev/null || true
  done
  # Give the broker time to flush, then force.
  for _ in $(seq 1 50); do
    alive=0
    for pid in "${PIDS[@]:-}"; do
      [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && alive=1
    done
    [[ $alive -eq 0 ]] && break
    sleep 0.1
  done
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill -KILL "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap 'cleanup; exit 0' INT TERM
trap cleanup EXIT

echo "dev.sh: building..."
cargo build -p fluxmq-server
HAVE_DEMO=0
if [[ -z "${NO_DEMO:-}" && -f crates/fluxmq-client/src/bin/fluxmq-demo.rs ]]; then
  cargo build -p fluxmq-client --bin fluxmq-demo && HAVE_DEMO=1
fi

mkdir -p "$DATA_DIR"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
"$TARGET_DIR/debug/fluxmq" \
  --data-dir "$DATA_DIR" \
  --client-addr "$CLIENT_ADDR" \
  --http-addr "$HTTP_ADDR" \
  --ui-dir "$ROOT/ui/dist" &
PIDS+=($!)

echo "dev.sh: waiting for broker on http://$HTTP_ADDR ..."
ready=0
for _ in $(seq 1 100); do
  if curl -fsS "http://$HTTP_ADDR/api/health" >/dev/null 2>&1; then ready=1; break; fi
  kill -0 "${PIDS[0]}" 2>/dev/null || { echo "dev.sh: broker exited during startup" >&2; exit 1; }
  sleep 0.2
done
[[ $ready -eq 1 ]] || { echo "dev.sh: broker did not become ready" >&2; exit 1; }

if [[ $HAVE_DEMO -eq 1 ]]; then
  if [[ -n "${FLUXMQ_ADMIN_TOKEN:-}" ]]; then export FLUXMQ_TOKEN="$FLUXMQ_ADMIN_TOKEN"; fi
  FLUXMQ_ADDR="$CLIENT_ADDR" "$TARGET_DIR/debug/fluxmq-demo" &
  PIDS+=($!)
else
  echo "dev.sh: demo traffic generator not available (skipped)"
fi

if [[ -z "${NO_UI:-}" && -f ui/package.json ]]; then
  if command -v pnpm >/dev/null 2>&1; then
    [[ -d ui/node_modules ]] || pnpm --dir ui install
    pnpm --dir ui dev &
    PIDS+=($!)
  else
    echo "dev.sh: pnpm not found; UI dev server skipped (npm i -g pnpm)" >&2
  fi
else
  echo "dev.sh: ui/ not present or NO_UI set; UI dev server skipped"
fi

echo "dev.sh: running. API http://$HTTP_ADDR/api/health — Ctrl-C to stop."
# Exit (and clean up the rest) as soon as any child dies.
while true; do
  for pid in "${PIDS[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "dev.sh: process $pid exited" >&2
      exit 1
    fi
  done
  sleep 1
done

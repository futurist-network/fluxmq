#!/usr/bin/env bash
# Build the FluxMQ image (and optionally the bench harness image).
#
#   deploy/build.sh                         # working tree        -> fluxmq:dev
#   deploy/build.sh --ref HEAD              # committed snapshot  -> fluxmq:dev
#   deploy/build.sh --ref 3a6bb81 --tag v0.4-baseline
#   deploy/build.sh --harness               # also fluxmq-harness:dev (bench/harness)
#
# With --ref, the sources (Cargo.toml, Cargo.lock, crates/, ui/) come from `git archive <ref>`
# and the packaging (Dockerfile, .dockerignore, deploy/*.sh, bench/harness) from the working
# tree, so any commit can be packaged the same way (used for the v0.4 baseline image in the
# batched-delivery comparison, and when the working tree is mid-edit).
set -euo pipefail
cd "$(dirname "$0")/.."

ref="" tag="dev" harness=0 harness_only=0
while [ $# -gt 0 ]; do
  case "$1" in
    --ref) ref="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --harness) harness=1; shift ;;
    --harness-only) harness=1; harness_only=1; shift ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

ctx="."
cleanup() { :; }
if [ -n "$ref" ]; then
  ctx="$(mktemp -d "${TMPDIR:-/tmp}/fluxmq-ctx.XXXXXX")"
  cleanup() { rm -rf "$ctx"; }
  trap cleanup EXIT
  git archive --format=tar "$ref" -- Cargo.toml Cargo.lock crates ui \
    | tar -x -C "$ctx" --exclude 'ui/screenshots'
  cp Dockerfile .dockerignore "$ctx/"
  mkdir -p "$ctx/deploy" "$ctx/bench/harness"
  cp deploy/docker-entrypoint.sh deploy/healthcheck.sh "$ctx/deploy/"
  cp -R bench/harness/Cargo.toml bench/harness/Dockerfile bench/harness/src "$ctx/bench/harness/"
  [ -f bench/harness/Cargo.lock ] && cp bench/harness/Cargo.lock "$ctx/bench/harness/"
  echo "build context: $ref -> $ctx"
fi
sha="$(git rev-parse --short "${ref:-HEAD}")"
[ -z "$ref" ] && ! git diff --quiet HEAD -- crates ui Cargo.toml Cargo.lock && sha="$sha-dirty"

export DOCKER_BUILDKIT=1
if [ "$harness_only" = 0 ]; then
  start=$(date +%s)
  docker build -f "$ctx/Dockerfile" -t "fluxmq:$tag" \
    --label "org.opencontainers.image.revision=$sha" \
    --label "org.opencontainers.image.source=fluxmq" "$ctx"
  echo "fluxmq:$tag ($sha) built in $(( $(date +%s) - start ))s"
fi
if [ "$harness" = 1 ]; then
  start=$(date +%s)
  docker build -f "$ctx/bench/harness/Dockerfile" -t "fluxmq-harness:$tag" \
    --label "org.opencontainers.image.revision=$sha" "$ctx"
  echo "fluxmq-harness:$tag ($sha) built in $(( $(date +%s) - start ))s"
fi

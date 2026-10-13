#!/usr/bin/env bash
# Builds the crash-test images (bench/crash/README.md):
#   fluxmq-crash:dev        FROM $FLUXMQ_IMAGE (default fluxmq:dev) + crashctl
#   kafka-crash:dev         FROM $KAFKA_IMAGE (default apache/kafka:3.7.0) + crashctl
#   fluxmq-crash-client:dev mq-crash (FluxMQ SDK from the working tree, or --ref <git ref>)
#
#   bench/crash/build.sh                 # all three
#   bench/crash/build.sh --brokers-only  # only the two broker images (seconds)
#   bench/crash/build.sh --ref HEAD      # client from a committed snapshot (tree mid-edit)
set -euo pipefail
cd "$(dirname "$0")/../.."

ref="" brokers_only=0
while [ $# -gt 0 ]; do
  case "$1" in
    --ref) ref="$2"; shift 2 ;;
    --brokers-only) brokers_only=1; shift ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
export DOCKER_BUILDKIT=1
docker build -f bench/crash/Dockerfile.broker --build-arg "BASE=${FLUXMQ_IMAGE:-fluxmq:dev}" \
  -t fluxmq-crash:dev bench/crash
docker build -f bench/crash/Dockerfile.broker --build-arg "BASE=${KAFKA_IMAGE:-apache/kafka:3.7.0}" \
  -t kafka-crash:dev bench/crash
[ "$brokers_only" = 1 ] && exit 0

ctx="."
if [ -n "$ref" ]; then
  ctx="$(mktemp -d "${TMPDIR:-/tmp}/fluxmq-crash-ctx.XXXXXX")"
  trap 'rm -rf "$ctx"' EXIT
  git archive --format=tar "$ref" -- Cargo.toml Cargo.lock crates | tar -x -C "$ctx"
  mkdir -p "$ctx/bench/crash"
  cp -R bench/crash/Cargo.toml bench/crash/Cargo.lock bench/crash/src "$ctx/bench/crash/"
  echo "client build context: $ref -> $ctx"
fi
docker build -f bench/crash/Dockerfile.client -t fluxmq-crash-client:dev \
  --label "org.opencontainers.image.revision=$(git rev-parse --short "${ref:-HEAD}")" "$ctx"

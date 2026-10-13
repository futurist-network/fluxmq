#!/usr/bin/env bash
# FluxMQ vs Kafka vs RabbitMQ benchmarks in Docker. See bench/README.md.
#
#   bench/run.sh                                     # everything (~1.5 h), report at the end
#   bench/run.sh --smoke                             # a few seconds per scenario (~15 min)
#   bench/run.sh --systems fluxmq,kafka --scenarios saturation
#   bench/run.sh --help
#
# Results: bench/results/<timestamp>/ (raw JSON + docker stats), report: bench/report/index.html
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 bench/runner.py "$@"

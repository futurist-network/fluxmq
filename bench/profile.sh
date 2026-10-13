#!/usr/bin/env bash
# CPU profile of a single FluxMQ broker under fluxmq-bench load (Docker, bench resources).
#
#   docker build --build-arg FLUXMQ_FEATURES=profiling -t fluxmq:profiling .   # symbols kept
#   bench/profile.sh OUT_DIR saturation|catchup [tls]
#
# saturation: 4 producers + 4 consumers, 1 KiB, closed loop; profiles 20 s mid-run.
# catchup:    prefill (no consumers), then 4 consumers drain; profiles the drain.
# PROF_COMPOSE_EXTRA=file.yml adds a compose override (e.g. broker env); PROF_LABEL tags the output.
# Writes OUT_DIR/<name>.folded (+ .svg, bench output); summarize with scripts/pprof-top.py.
set -euo pipefail

out=${1:?out dir}; scenario=${2:?scenario}; tls=${3:-}
image=${FLUXMQ_IMAGE:-fluxmq:profiling}
size=${SIZE:-1024}
project=fluxmq-prof
# With TLS on, the broker serves its HTTP API over HTTPS too (self-signed dev CA).
scheme=http; [ -n "$tls" ] && scheme=https
http=$scheme://127.0.0.1:${PROF_HTTP_PORT:-37621}
name="$scenario-${size}B${tls:+-tls}${PROF_LABEL:+-$PROF_LABEL}"
mkdir -p "$out"
root=$(cd "$(dirname "$0")/.." && pwd)

compose() {
    FLUXMQ_IMAGE=$image FLUXMQ_SINGLE_TLS=${tls:+on} FLUXMQ_CLIENT_PORT=${PROF_CLIENT_PORT:-37620} \
        FLUXMQ_HTTP_PORT=${PROF_HTTP_PORT:-37621} \
        docker compose -p $project -f "$root/deploy/docker-compose.yml" \
        ${PROF_COMPOSE_EXTRA:+-f "$PROF_COMPOSE_EXTRA"} --profile single "$@"
}
profile() { # seconds file
    curl -sfk -o "$2.folded" "$http/api/debug/pprof/profile?seconds=$1&hz=199&format=folded"
}
bench() { # args...
    local t=()
    [ -n "$tls" ] && t=(--tls-ca /certs/ca.pem)
    docker run --rm --network ${project}_default --cpus "${BENCH_CPUS:-2}" -v ${project}_certs:/certs:ro \
        "$image" fluxmq-bench --addr fluxmq:7420 "${t[@]}" --size "$size" "$@"
}

compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d fluxmq >/dev/null
trap 'compose down -v --remove-orphans >/dev/null 2>&1 || true' EXIT
for _ in $(seq 60); do curl -sfk "$http/api/health" >/dev/null && break; sleep 1; done

case $scenario in
saturation)
    bench --producers 4 --consumers 4 --inflight 4 --batch 100 --duration 30 >"$out/$name.bench.txt" 2>&1 &
    sleep 8
    profile 20 "$out/$name"
    wait
    ;;
catchup)
    n=${PRELOAD:-2000000}
    bench --producers 4 --consumers 4 --inflight 4 --batch 100 --drain "$n" \
        >"$out/$name.bench.txt" 2>&1 &
    until grep -q prefilled "$out/$name.bench.txt" 2>/dev/null; do
        kill -0 $! 2>/dev/null || { cat "$out/$name.bench.txt"; exit 1; }
        sleep 0.2
    done
    sleep 0.5
    profile "${CATCHUP_PROFILE_S:-4}" "$out/$name"
    wait
    ;;
*) echo "unknown scenario $scenario" >&2; exit 2 ;;
esac
docker stats --no-stream --format '{{.MemUsage}}' ${project}-fluxmq-1 >"$out/$name.mem.txt" 2>/dev/null || true
python3 "$root/scripts/pprof-top.py" "$out/$name.folded" -n 30 >"$out/$name.top.txt"
echo "$out/$name.top.txt"

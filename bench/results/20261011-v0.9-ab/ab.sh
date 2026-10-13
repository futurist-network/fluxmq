#!/bin/bash
# v0.9 A/B: fluxmq:v080 (f87a865) vs fluxmq:v09a (working tree), same harness image (v080).
cd "$(git rev-parse --show-toplevel)"
S=${S:-/tmp/fluxmq-bench}
O=$S/v09/ab; mkdir -p $O; L="$S/benchlock.sh v09"
C=(--build never --retention-mb 512 --no-report --min-free-gb 15 --bench-cpus 4 --harness-image fluxmq-harness:v080)
run() { img=$1; tag=$2; shift 2; $L python3 bench/runner.py "${C[@]}" --fluxmq-image fluxmq:$img --out $O/$img-$tag "$@" >> $O.log 2>&1 || echo "fail $img $tag"; }
for rep in 1 2 3; do for img in v09a v080; do
  run $img lat$rep --systems fluxmq-cluster --scenarios latency --rates 1000,10000,50000
done; done
for rep in 1 2; do for img in v09a v080; do
  run $img sat$rep --systems fluxmq-single,fluxmq-cluster --scenarios saturation --sizes 1024 --clients 1x1,4x4
done; done
echo DONE

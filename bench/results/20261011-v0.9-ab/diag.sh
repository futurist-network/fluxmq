#!/bin/bash
cd "$(git rev-parse --show-toplevel)"
S=${S:-/tmp/fluxmq-bench}
O=$S/v09/diag; mkdir -p $O; L="$S/benchlock.sh v09"
C=(--build never --retention-mb 512 --no-report --min-free-gb 15 --bench-cpus 4 --harness-image fluxmq-harness:v080)
rm -f $O/stop; python3 $S/v09/sampler.py $O/samples.jsonl $O/stop 2 nodu & sp=$!
for rep in 1 2 3; do
  for v in "v09a fluxmq-cluster on" "v09a fluxmq-cluster-nopipe off" "v080 fluxmq-cluster base"; do set -- $v
    echo "$(date +%s) START $3 r$rep" >> $O/marks.txt
    $L python3 bench/runner.py "${C[@]}" --fluxmq-image fluxmq:$1 --out $O/$3-r$rep --systems $2 --scenarios latency --rates 50000 >> $O.log 2>&1 || echo "fail $3 $rep"
    echo "$(date +%s) END $3 r$rep" >> $O/marks.txt
  done
done
touch $O/stop; wait $sp; echo DONE

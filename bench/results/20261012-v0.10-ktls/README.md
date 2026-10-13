# v0.10 kernel TLS A/B (2026-10-12)

Not a full result set (no top-level `env.json`, so the report generator skips it). Raw runs behind
`docs/design/v0.10-ktls.md` §5. Host: WSL2 kernel 6.18 (`tls` module loaded), 4 vCPUs, Docker Desktop.

At the time of these runs kernel TLS was the default, so the labels are: `fluxmq-*-tls` = **kernel TLS**,
`fluxmq-*-tls-user` = the userspace record layer (`FLUXMQ_TLS_IMPL=userspace`). Since then the userspace layer is
the default and kernel TLS is `FLUXMQ_TLS_IMPL=ktls` (bench systems `fluxmq-*-tls-ktls`).

- `ab/`: `bench/runner.py --systems ktls --scenarios idle,saturation,latency,catchup,overload --retention-mb 512
  --bench-cpus 4` (1 node and 3 nodes, each without TLS, kernel TLS, userspace TLS).
- `repeat/`: 1 node, 1 KiB, 4x4 saturation ×3 and catch-up, both TLS paths.
- `repeat-catchup-{2,3}/`: 1 node, 1 KiB catch-up, both TLS paths.

0 failures, every run drained, no acknowledged message missing.

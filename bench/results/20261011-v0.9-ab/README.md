# v0.9 commit-path A/B (2026-10-11)

Not a full result set (no top-level `env.json`, so the report generator skips it). Raw runs behind
`docs/design/v0.9-commit-path.md` §7:

- `ab-<image>-lat<N>` / `ab-<image>-sat<N>`: `fluxmq:v080` (f87a865) vs `fluxmq:v09a` (v0.9 working tree), same
  harness image, 3-node latency 1k/10k/50k ×3 and 1 KiB saturation ×2 (`ab.sh`).
- `diag-{on,off,base}-r<N>`: 3-node 50k latency, v0.9 pipeline on vs `FLUXMQ_COMMIT_PIPELINE=off` vs the v0.8.0 image,
  3 interleaved rounds (`diag.sh`); `samples.jsonl` = broker `/metrics` every 2 s (`sampler.py`), `marks.txt` = run
  boundaries.

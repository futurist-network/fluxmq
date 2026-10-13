# v0.11 CPU profiles (2026-10-12)

Not a full result set (no top-level `env.json`, so the report generator skips it). Broker CPU profiles taken with the
`profiling` build (`GET /api/debug/pprof/profile`, pprof-rs, 199 Hz) and `bench/profile.sh`. Host: WSL2 kernel 6.18,
Docker Desktop, broker 2 CPUs / 2 GiB, `fluxmq-bench` client 2 CPUs, 1 KiB, 4 producers × 4 consumers, fsync before ack.
`*.folded` = raw stacks, `*.top.txt` = `scripts/pprof-top.py` summary, `*.bench.txt` = client output.

## Findings

- **Saturation is disk-bound on this host, not CPU-bound.** The broker uses ~0.5–0.8 of its 2 CPUs (samples / (199 Hz ×
  20 s)), throughput plateaus at ~120–200k msg/s whatever the client window (inflight 4 / 16 / 64: 182k / 195k / 143k,
  only latency grows), and `fdatasync` + `write`/`writev` dominate. Raw `dd oflag=dsync` on a Docker volume: ~1.5 ms per
  64 KiB, ~2.3 ms per 1 MiB. Userspace code (CRC, append, groups) is < 15 % of the broker's CPU.
- **No regression since v0.8.** Images v0.8 / v0.9 / v0.10 / this branch, same client, interleaved: 99–149k / 116–117k /
  114–117k / 113–117k msg/s. The v0.8 report's 547k were measured on a different host (macOS, faster fsync).
- **Catch-up was CPU-bound in reads** (`catchup-*`): `pread` 37–40 %, `writev` 18–19 %, and ~16 % zeroing 4 MiB read
  buffers before `pread` overwrote them (`vec![0; len]`; memset samples land in the callers `SegmentScan::fill` /
  `read_entries_in`). Fixed in `bad1b18` (`nozero/`): 689k / 738k / 726k → 817k / 863k / 845k msg/s, ~12 % fewer samples.
- **jemalloc is slower here** (`alloc/`): catch-up 644k / 648k / 622k vs glibc 721k / 749k / 731k. It purges freed pages
  (`madvise` 7 %), so every 4 MiB read faults them in again (`pread` 58 %). Kept as an opt-in feature only.
- **TLS** (`tls/`): throughput equal to plain on this disk-bound host (112–114k vs 112–120k); broker CPU +7–12 %:
  AES-GCM (`_aesni_ctr32_ghash_6x`, AES-NI/AVX) 8.6 %, copying plaintext into the record buffer 2.5 %. Little left to
  gain in code; the cipher itself is the cost.
- **Next:** the remaining catch-up cost is the two copies (page cache → user, user → socket); see the sendfile analysis
  (removing the read copy would require reworking redelivery/DLQ, which keep message bytes in memory). Throughput and p99
  comparisons with Kafka need a bare-metal Linux host.

## Commit path (fsync) on this host

Saturation 20 s, broker `/metrics` after the run (`fluxmq_node_fsync_latency_seconds`, `..._commit_latency_...`):

| setting | msg/s | fsyncs/s | avg fsync | KiB per fsync | avg commit | fsyncs in flight |
|---|---:|---:|---:|---:|---:|---:|
| pipeline depth 1 (default) | 167k | 1250 | 3.2 ms | 134 | 6.2 ms | 4.0 |
| pipeline depth 2 | 112k | 1050 | 3.8 ms | 107 | 12.5 ms | 4.0 |
| pipeline depth 4 | 149k | 830 | 4.7 ms | 180 | 9.3 ms | 3.9 |
| fsync threads 4 (default here: 2 × 2 CPUs) | 165k / 112k | 1240 / 840 | 3.2 / 4.7 ms | 134 | 6.3 / 9.3 ms | 4.0 |
| fsync threads 8 | 162k / 151k | 1320 / 1240 | 5.5 / 5.8 ms | 123 | 5.7 / 6.1 ms | 7.2 |
| fsync threads 16 | 144k / 129k | 1180 / 1050 | 6.2 / 7.0 ms | 123 | 6.4 / 7.3 ms | 7.3 |

The WSL2 virtual disk completes ~1 250 flushes/s however many run in parallel (more threads only make each fsync
slower), so throughput ≈ flushes/s × bytes per flush (8 partitions → 8 fsyncs per commit, ~130 KiB each ≈ 160 MB/s).
Run-to-run noise of one setting (112–165k) exceeds every tuning effect, so no default changes on this evidence. The
structural lever would be fewer flushes per commit (one shared write-ahead log instead of a flush per partition file), a
large design change to evaluate on a bare-metal Linux host first.

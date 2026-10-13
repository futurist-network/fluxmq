"""v0.8 report sections: goals met / not met (authored), retention at saturation (data-dir peaks
sampled during the runs), disk health (fsync latency from the broker's /metrics during the runs),
same-host A/B against the v0.7 image, TLS overhead v0.7 -> v0.8, the v0.8.0 fast-TLS A/B, and reused rows. Imported by
generate.py; Python 3.9+ stdlib only. Every section returns None when its data is absent, so
earlier result sets render unchanged.

Data sources inside a result set directory:
* goals.html                 authored goals table (numbers from this set)
* samples-summary.json       per harness run: peak data-dir bytes per broker, fsync stats in the
                             measured window (bench sampler, see notes.html)
* ab/runs/*.json             same-host A/B runs of the previous release's image (runner --out <set>/ab)
* tls-ab/<group>-<part>/runs  v0.8.0 fast-TLS A/B (v08p1 = before, v08-tls2 = v0.8.0, lat3-* = latency
                             repeats per TLS implementation, *kafka* = Kafka plain/TLS)
* runs/*.json "reused_from"  rows copied from an earlier set (not re-measured), marked in titles
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from pathlib import Path

from sections_v07 import _fmt, _ms, _rate, _sat_cell, _table, esc


def reused_systems(runs):
    """{system: source set} for systems whose rows were all copied from an earlier set."""
    seen, fresh = {}, set()
    for r in runs:
        s = r.get("system_name")
        if not s or r.get("kind") not in ("harness", "idle"):
            continue
        if r.get("reused_from"):
            seen[s] = r["reused_from"]
        else:
            fresh.add(s)
    # Only systems with no row of their own are marked in their title; mixed systems are marked per row.
    return {s: src for s, src in seen.items() if s not in fresh}


def goals_section(results: Path):
    p = results / "goals.html"
    if not p.exists():
        return None
    return ("goals", "Цели v0.8: выполнено / не выполнено · v0.8 goals: met / not met", p.read_text())


def _summary(results: Path):
    p = results / "samples-summary.json"
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return []


def retention_section(results: Path, title_of):
    rows = []
    data = [r for r in _summary(results) if r.get("scenario") in ("saturation", "overload")
            and r.get("peak_data_bytes") and r.get("retention_bytes_per_partition")]
    if not data:
        return None
    for r in data:
        parts = r.get("partitions") or 8
        cap = r["retention_bytes_per_partition"] * parts
        seg = r.get("segment_bytes") or 16 * 2**20
        bound = cap + 2 * seg * parts
        peak = max(r["peak_data_bytes"].values())
        fam = "fluxmq" if str(r["system"]).startswith("fluxmq") else "other"
        ok = peak <= bound
        rows.append((r["system"], r["run"], r.get("published_bytes") or 0, peak, cap, bound, ok, fam,
                     len(r["peak_data_bytes"])))
    rows.sort(key=lambda x: (x[7] != "fluxmq", x[0], x[1]))
    trows = []
    for s, run, pub, peak, cap, bound, ok, fam, nodes in rows:
        trows.append([esc(title_of(s)), esc(run.replace(s + "-", "")), f"{pub / 2**30:,.1f} GiB",
                      f"{peak / 2**20:,.0f} MiB" + (f" (max of {nodes} nodes)" if nodes > 1 else ""),
                      f"{cap / 2**20:,.0f} MiB", f"{bound / 2**20:,.0f} MiB",
                      ('<b class="ok">yes</b>' if ok else
                       ('<b class="bad">no</b>' if fam == "fluxmq" else "no (Kafka's own retention pace)"))])
    flux = [x for x in rows if x[7] == "fluxmq"]
    within = sum(1 for x in flux if x[6])
    body = ("<p>The bench sampler ran <code>du</code> on every broker's data directory every ~2 s during each run. "
            "Cap = retention per partition × partitions (512 MiB × 8 = 4 GiB per broker). Bound = cap + 2 segments "
            "per partition (the v0.8 paced-retention guarantee: a segment is removed within about two 50 ms ticks of "
            "becoming due; Kafka's rule keeps the active segment plus one). Ingest = bytes published during the "
            "run, i.e. how much had to be removed. v0.7 at 1 KiB 1×1 overshot the same cap by ~4 GiB (data dir "
            "4.7 GB for a 512 MiB cap in the v0.8 TLS/retention A/B).</p>"
            f"<p><b>FluxMQ: {within} of {len(flux)} saturation/overload runs stayed within the bound.</b></p>"
            + _table(["System", "Run", "Ingest", "Peak data dir", "Cap", "Bound", "Within bound"], trows))
    return ("retention", "Retention at saturation: data dir vs cap", body)


def disk_section(results: Path, title_of):
    data = [r for r in _summary(results) if r.get("fsync")]
    notes = results / "host-health.html"
    if not data and not notes.exists():
        return None
    trows = []
    for r in data:
        for n, f in sorted(r["fsync"].items()):
            trows.append([esc(title_of(r["system"])), esc(r["run"].replace(r["system"] + "-", "")), esc(n),
                          _fmt(f.get("per_s")), f"{f.get('mean_ms', 0):.2f} ms",
                          ("≤ " + _ms(f["p99_le_ms"])) if f.get("p99_le_ms") else "–",
                          _ms(f.get("worst_2s_mean_ms"))])
    body = notes.read_text() if notes.exists() else ""
    if trows:
        body += ("<details><summary>fsync latency per run and broker (FluxMQ <code>/metrics</code>, measured window)"
                 "</summary><p>From <code>fluxmq_node_fsync_latency_seconds</code>, scraped every ~2 s. p99 is the "
                 "upper edge of the histogram bucket that holds the 99th percentile; \"worst 2 s\" is the highest "
                 "mean over one scrape interval.</p>"
                 + _table(["System", "Run", "Node", "fsyncs/s", "Mean", "p99", "Worst 2 s mean"], trows)
                 + "</details>")
    return ("host", "Host health: disk and fsync during the runs", body)


def ab_section(results: Path, sat_by, lat_by, title_of, load, aggregate):
    d = results / "ab"
    if not (d / "runs").exists():
        return None
    _, runs, _ = load(d)
    groups = defaultdict(list)
    lat = defaultdict(list)
    for r in runs:
        if r.get("kind") != "harness" or r.get("excluded"):
            continue
        pr = r["params"]
        key = (r["system_name"], r.get("ab_label") or "?")
        if r.get("scenario") == "saturation":
            groups[key + (pr["size"], pr["producers"], pr["consumers"])].append(r)
        elif r.get("scenario") == "latency" and r.get("e2e_ms"):
            lat[key + (pr["rate"],)].append(r)
    rows = []
    for k in sorted(groups):
        a = aggregate(groups[k])
        cur = sat_by.get((k[0],) + k[2:])
        va, vb = a.get("consume_rate"), (cur or {}).get("consume_rate")
        rows.append([esc(title_of(k[0])), f"saturation {k[2]} B {k[3]}×{k[4]}", esc(k[1]), _sat_cell(a),
                     _sat_cell(cur), (f"{(vb - va) / va * 100:+.0f} %" if va and vb else "–")])
    for k in sorted(lat):
        rs = lat[k]
        cur = lat_by.get((k[0], k[2]))
        ea = "; ".join(f"{_ms(r['e2e_ms']['p50'])} / {_ms(r['e2e_ms']['p99'])}" for r in rs)
        eb = cur.get("e2e_ms") if cur else None
        rows.append([esc(title_of(k[0])), f"latency {k[2]:,}/s p50 / p99", esc(k[1]), ea,
                     f"{_ms(eb['p50'])} / {_ms(eb['p99'])}" if eb else "–", ""])
    if not rows:
        return None
    note = results / "ab" / "note.html"
    body = ((note.read_text() if note.exists() else
             "<p>The previous release's image measured on the same host in the same session, same harness and "
             "settings; the v0.8 column is this set's matrix row.</p>")
            + _table(["System", "Measurement", "Previous image", "Previous", "v0.8 (matrix)", "Δ"], rows))
    return ("ab", "Same-host A/B: v0.7 image vs v0.8", body)


def tls_history_section(results: Path, sat_by, title_of, load, aggregate):
    """TLS overhead (consume rate, TLS vs plaintext) in the previous release's set vs this one."""
    # Only for sets newer than the v0.7 set (set names start with the date).
    prev = sorted(p for p in results.parent.glob("*-v0.7") if p.is_dir() and p.name < results.name)
    if not prev:
        return None
    _, pruns, _ = load(prev[-1])
    pg = defaultdict(list)
    for r in pruns:
        if r.get("kind") == "harness" and r.get("scenario") == "saturation" and not r.get("excluded"):
            pr = r["params"]
            pg[(r["system_name"], pr["size"], pr["producers"], pr["consumers"])].append(r)
    pby = {k: aggregate(v) for k, v in pg.items()}
    rows = []
    for plain, tls in (("fluxmq-single", "fluxmq-single-tls"), ("fluxmq-cluster", "fluxmq-cluster-tls")):
        for cfg in sorted({k[1:] for k in sat_by if k[0] == tls}, key=lambda c: (-c[0], c[1])):
            def ov(by):
                a, b = by.get((plain,) + cfg), by.get((tls,) + cfg)
                if not a or not b or not a.get("consume_rate") or b.get("consume_rate") is None:
                    return None, a, b
                return (b["consume_rate"] - a["consume_rate"]) / a["consume_rate"] * 100, a, b
            o7, a7, b7 = ov(pby)
            o8, a8, b8 = ov(sat_by)
            rows.append([esc(title_of(plain).replace("FluxMQ · ", "")), f"{cfg[0]} B {cfg[1]}×{cfg[2]}",
                         (f"{_rate(b7['consume_rate'])} vs {_rate(a7['consume_rate'])}: <b>{o7:+.0f} %</b>"
                          if o7 is not None else "–"),
                         (f"{_rate(b8['consume_rate'])} vs {_rate(a8['consume_rate'])}: <b>{o8:+.0f} %</b>"
                          if o8 is not None else "–"),
                         (f"{_rate(b8['consume_rate'] - b7['consume_rate'])}" if b7 and b8 and
                          b7.get("consume_rate") and b8.get("consume_rate") else "–")])
    if not rows:
        return None
    return ("tls-history", "TLS cost: v0.7 → v0.8",
            "<p>Consume msg/s with TLS vs plaintext in the same set (median of the repeats). Target for v0.8: TLS "
            "costs ≤ 15 % at saturation. This set ran 95107b0, before the v0.8.0 TLS rewrite (see the next section for the v0.8.0 A/B); up to that commit no TLS code changed in v0.8 (see <code>docs/design/v0.8-tls-retention.md</code>: "
            "rustls' buffered API copies every byte twice on receive and once on send, and the Docker Desktop kernel "
            "has no kTLS). The plaintext path got much cheaper (zero-copy), so the relative TLS share grows. "
            "\"TLS Δ\" = change of the TLS row's absolute rate, v0.7 → v0.8.</p>"
            + _table(["Topology", "Saturation", f"v0.7 ({esc(prev[-1].name)}) TLS vs plain",
                      "v0.8 (this set) TLS vs plain", "TLS Δ msg/s"], rows))



def _tls_ab_runs(results: Path):
    """{(group, system, scenario, size, P, C, rate): [run]} from tls-ab/<group>-<part>/runs/*.json."""
    out = defaultdict(list)
    for f in sorted((results / "tls-ab").glob("*/runs/*.json")):
        try:
            r = json.loads(f.read_text())
        except json.JSONDecodeError:
            continue
        if r.get("kind") != "harness" or r.get("error") or r.get("excluded"):
            continue
        d = f.parent.parent.name
        group = ("lat3-" + d.split("-")[1]) if d.startswith("lat3-") and not d.startswith("lat3-kafka") \
            else ("kafka" if "kafka" in d else d.split("-r")[0].split("-lat")[0])
        p = r["params"]
        out[(group, r["system_name"], r["scenario"], p.get("size"), p.get("producers"), p.get("consumers"),
             p.get("rate") if r["scenario"] == "latency" else None)].append(r)
    return out


def tls_ab_section(results: Path, title_of):
    """v0.8.0 fast TLS record layer: same-host A/B against the pre-fast-TLS image, Kafka with TLS."""
    g = _tls_ab_runs(results)
    if not g:
        return None

    def med(key, field="consume_rate"):
        v = [r.get(field) for r in g.get(key, []) if r.get(field) is not None]
        return statistics.median(v) if v else None

    def p99(key):
        v = [r["e2e_ms"]["p99"] for r in g.get(key, []) if (r.get("e2e_ms") or {}).get("p99") is not None]
        return (statistics.median(v), len(v)) if v else (None, 0)

    def cell(img, plain, tls, cfg):
        a, b = med((img, plain, "saturation") + cfg), med((img, tls, "saturation") + cfg)
        if a is None or b is None:
            return "–", None
        o = (b - a) / a * 100
        cls = "ok" if o >= -15 else "bad"
        return f"{_rate(b)} vs {_rate(a)}: <b class=\"{cls}\">{o:+.0f} %</b>", b

    cfgs = [("fluxmq-single", 1024, 1, 1), ("fluxmq-single", 1024, 4, 4), ("fluxmq-single", 100, 1, 1),
            ("fluxmq-single", 100, 4, 4), ("fluxmq-cluster", 1024, 4, 4)]
    rows = []
    for plain, size, pp, cc in cfgs:
        cfg = (size, pp, cc, None)
        c0, b0 = cell("v08p1", plain, plain + "-tls", cfg)
        c1, b1 = cell("v08-tls2", plain, plain + "-tls", cfg)
        rows.append([esc(title_of(plain).replace("FluxMQ · ", "")),
                     f"{'1 KiB' if size == 1024 else str(size) + ' B'} {pp}×{cc}", c0, c1,
                     f"{(b1 - b0) / b0 * 100:+.0f} %" if b0 and b1 else "–"])
    body = ("<p><b>v0.8.0</b> replaces rustls' buffered stream (tokio-rustls) with FluxMQ's own record layer on "
            "the same rustls handshake and AEAD: decrypt in place (64 KiB reads, no plaintext copy), independent "
            "read/write halves, AES-128-GCM preferred among TLS 1.3 suites (TLS 1.2 stays on rustls' record "
            "layer). <code>FLUXMQ_TLS_IMPL=classic</code> keeps the old path for one release. Design and analysis: "
            "<code>docs/design/v0.8-tls.md</code>.</p>"
            "<p>Same-host A/B after the main set: image <code>fluxmq:v08p1</code> (95107b0, the commit measured above, "
            "tokio-rustls, AES-256 first) vs <code>fluxmq:v08-tls2</code> (v0.8.0 TLS), interleaved, 2 repeats, same "
            "flags (bench CPUs 4, retention 512 MiB). Consumed msg/s, median; overhead is TLS vs plaintext of the "
            "<i>same image in the same session</i>. Green = within the ≤ 15 % goal. Plaintext rows are unchanged "
            "between the images (no regression). Raw runs: <code>tls-ab/</code> in this result set.</p>"
            + _table(["Topology", "Saturation", "Before (v08p1) TLS vs plain", "v0.8.0 TLS vs plain",
                      "TLS rate Δ"], rows))

    # Kafka with TLS on the same host (harness rdkafka built with OpenSSL).
    krows = []
    for pp in (1, 4):
        kp = med(("kafka", "kafka-single", "saturation", 1024, pp, pp, None))
        kt = med(("kafka", "kafka-single-tls", "saturation", 1024, pp, pp, None))
        ft = med(("v08-tls2", "fluxmq-single-tls", "saturation", 1024, pp, pp, None))
        if kt is None:
            continue
        krows.append([f"1 KiB {pp}×{pp}", _rate(kp), _rate(kt) + (f" ({(kt - kp) / kp * 100:+.0f} %)" if kp else ""),
                      _rate(ft), f"<b class=\"ok\">{ft / kt:.1f}×</b>" if ft else "–"])
    if krows:
        body += ("<h3>FluxMQ with TLS vs Kafka with TLS (1 node)</h3>"
                 "<p>Kafka 9095 SSL listener, client rdkafka with OpenSSL, defaults otherwise (as in the matrix). "
                 "At 4×4 Kafka's consumers fall behind its producers under TLS.</p>"
                 + _table(["Saturation", "Kafka plain", "Kafka TLS", "FluxMQ v0.8.0 TLS", "FluxMQ / Kafka (TLS)"],
                          krows))

    # Latency on 3 nodes with TLS on clients and replication: 3 interleaved repeats per implementation.
    lrows = []
    for rate in (1000, 10000):
        cells = []
        for grp, sysn in (("lat3-fast", "fluxmq-cluster-tls"), ("lat3-classic", "fluxmq-cluster-tls"),
                          ("lat3-base", "fluxmq-cluster-tls"), ("kafka", "kafka-cluster-tls")):
            v, n = p99((grp, sysn, "latency", 1024, 4, 4, rate))
            cells.append(f"{_ms(v)}" + (f" <span class=\"muted-inline\">(n={n})</span>" if n > 1 else ""))
        lrows.append([f"{rate:,} msg/s"] + cells)
    body += ("<h3>Latency, 3 nodes, TLS everywhere: p99 (median of repeats)</h3>"
             "<p>Early runs showed 0.3–1.3 s p99 outliers with the fast path. A dedicated investigation (16 runs "
             "crossing broker/harness implementations, cgroup CPU sampling, an in-process 3-node TLS cluster on "
             "Linux) found such outliers in every combination, Kafka included, and tied them to virtual-disk fsync "
             "stalls plus the 100 ms fetch long-poll, not to the record layer. On Linux the fast path has the "
             "better tail (p99/max 8–12 / 36–52 ms vs classic 11–14 / 194–200 ms). In Docker the three FluxMQ "
             "variants are inseparable at this noise level; FluxMQ with TLS stays below Kafka with TLS. Regression "
             "test for the stall class: <code>lock_step_request_response_never_stalls</code>.</p>"
             + _table(["Rate", "v0.8.0 fast", "v0.8.0 classic", "Before (v08p1)", "Kafka TLS"], lrows))
    return ("tls-fast", "v0.8.0: fast TLS record layer — A/B and Kafka with TLS", body)

CSS = r"""
b.ok,.goal .met{color:#1a8a4a}b.bad,.goal .miss{color:#c0392b}.goal .part{color:#b26a00}
.goal td:first-child{font-weight:600}.goal td:nth-child(2){white-space:nowrap}
@media (max-width:640px){.goal table,.goal tbody,.goal tr,.goal td{display:block;width:auto}.goal thead{display:none}
.goal tr{padding:10px 0;border-bottom:1px solid var(--border)}.goal td{border:0;padding:2px 0}
.goal table{min-width:0}}
.badge{display:inline-block;padding:1px 8px;border-radius:999px;font-size:12px;font-weight:600;white-space:nowrap;
border:1px solid currentColor}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]) b.ok,:root:not([data-theme="light"]) .goal .met{color:#4cc27f}
:root:not([data-theme="light"]) b.bad,:root:not([data-theme="light"]) .goal .miss{color:#f07a6c}
:root:not([data-theme="light"]) .goal .part{color:#f0b35a}}
:root[data-theme="dark"] b.ok,:root[data-theme="dark"] .goal .met{color:#4cc27f}
:root[data-theme="dark"] b.bad,:root[data-theme="dark"] .goal .miss{color:#f07a6c}
:root[data-theme="dark"] .goal .part{color:#f0b35a}
"""

#!/usr/bin/env python3
"""Renders a bench results directory into ONE self-contained HTML page.

    python3 bench/report/generate.py bench/results/<timestamp> [-o bench/report/index.html]

Python 3.9+ stdlib only. Charts are inline SVG (no external scripts), colors are CSS custom
properties so the page follows the OS light/dark setting and a toggle.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

import sys as _sys

_sys.path.insert(0, str(Path(__file__).resolve().parent))
import sections_v07 as v07  # noqa: E402
import sections_v08 as v08  # noqa: E402

FAMILY_TITLE = {"fluxmq": "FluxMQ", "kafka": "Kafka", "rabbitmq": "RabbitMQ"}
ORDER = ["fluxmq-single", "fluxmq-single-tls", "fluxmq-single-os", "fluxmq-single-lz4",
         "fluxmq-single-cumulative", "fluxmq-single-pipeline",
         "fluxmq-cluster", "fluxmq-cluster-tls", "fluxmq-cluster-os", "fluxmq-cluster-lz4",
         "fluxmq-cluster-cumulative",
         "kafka-single", "kafka-single-fsync", "kafka-cluster", "kafka-cluster-fsync",
         "rabbitmq-single", "rabbitmq-cluster"]
# FluxMQ with FLUXMQ_FSYNC=os: acks after write(2), no fsync — the same durability point as
# Kafka's defaults (page cache + replication). Shown as its own family (lighter colour) and in
# its own section; the headline comparisons and conclusions stay on group commit.
OS_VARIANTS = {"fluxmq-single-os": "fluxmq-single", "fluxmq-cluster-os": "fluxmq-cluster"}
OS_LABEL = "durability parity with Kafka defaults"
TITLES = {
    "fluxmq-single": "FluxMQ · 1 node",
    "fluxmq-single-tls": "FluxMQ · 1 node · TLS",
    "fluxmq-cluster": "FluxMQ · 3 nodes RF=3",
    "fluxmq-cluster-tls": "FluxMQ · 3 nodes RF=3 · TLS",
    "fluxmq-single-os": "FluxMQ · 1 node · fsync=os ‡",
    "fluxmq-cluster-os": "FluxMQ · 3 nodes RF=3 · fsync=os ‡",
    "fluxmq-single-pipeline": "FluxMQ · 1 node · commit pipeline",
    "fluxmq-single-lz4": "FluxMQ · 1 node · lz4",
    "fluxmq-cluster-lz4": "FluxMQ · 3 nodes RF=3 · lz4",
    "fluxmq-single-cumulative": "FluxMQ · 1 node · cumulative ack",
    "fluxmq-cluster-cumulative": "FluxMQ · 3 nodes RF=3 · cumulative ack",
    "kafka-single": "Kafka · 1 node",
    "kafka-cluster": "Kafka · 3 nodes RF=3",
    "kafka-single-fsync": "Kafka · 1 node · fsync per message",
    "kafka-cluster-fsync": "Kafka · 3 nodes RF=3 · fsync per message",
    "rabbitmq-single": "RabbitMQ · 1 node classic",
    "rabbitmq-cluster": "RabbitMQ · 3 nodes quorum",
}
BROKER_SERVICES = {"fluxmq", "n1", "n2", "n3", "kafka", "kafka1", "kafka2", "kafka3",
                   "rabbitmq", "rmq1", "rmq2", "rmq3"}
CLIENT_SERVICES = {"bench", "fluxmq-bench"}

esc = html.escape


def family(name):
    """CSS family of a system: fluxmq | fluxmqos | kafka | kafkafsync | rabbitmq."""
    if name in OS_VARIANTS:
        return "fluxmqos"
    if name and name.startswith("kafka") and name.endswith("-fsync"):
        return "kafkafsync"
    return name.split("-")[0] if name else ""


def drained_cell(r):
    """'yes', or 'no' with the late/missing split of the harness's id check (schema 2)."""
    if r.get("drained"):
        return "yes"
    misses = [x.get("drain_miss") for x in r.get("reps", [r]) if x.get("drain_miss")]
    if not misses:
        return "<b>no</b>"
    late = sum(m.get("late") or 0 for m in misses)
    dropped = sum((m.get("classification") or {}).get("retention_dropped") or 0 for m in misses)
    missing = sum(miss_true(m) for m in misses)
    parts = [f"{late:,} late"]
    stored = sum((m.get("classification") or {}).get("stored_undelivered") or 0 for m in misses)
    if dropped:
        parts.append(f"{dropped:,} dropped by retention (backlog &gt; cap)")
    if stored:
        parts.append(f"{stored:,} stored, not yet delivered")
    if missing or not (dropped or stored):
        parts.append(f"<b>{missing:,} missing</b>" if missing else "0 missing")
    return "<b>no</b> (" + ", ".join(parts) + ")"


def miss_true(m):
    """Missing messages not explained by size retention (true loss unless shown otherwise)."""
    c = m.get("classification")
    if c is not None:
        return c.get("true_missing") or 0
    return m.get("missing") or 0


def fmt_num(v, digits=0):
    if v is None:
        return "–"
    if isinstance(v, float) and digits:
        return f"{v:,.{digits}f}"
    return f"{v:,.0f}"


def fmt_rate(v):
    if v is None:
        return "–"
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}k"
    return f"{v:.0f}"


def fmt_ms(v):
    if v is None:
        return "–"
    if v >= 100:
        return f"{v:,.0f} ms"
    if v >= 10:
        return f"{v:.1f} ms"
    return f"{v:.2f} ms"


def fmt_mb(b):
    return "–" if b is None else f"{b / 2**20:,.0f} MiB"


# ----------------------------------------------------------------------------------------
# Loading


def load(results):
    env = json.loads((results / "env.json").read_text()) if (results / "env.json").exists() else {}
    runs = []
    for p in sorted((results / "runs").glob("*.json")):
        try:
            d = json.loads(p.read_text())
        except json.JSONDecodeError:
            continue
        d["_file"] = p.name
        if d.get("kind") == "harness":
            d.setdefault("system_name", d.get("label"))
        runs.append(d)
    stats = []
    sp = results / "stats.jsonl"
    if sp.exists():
        for line in sp.read_text().splitlines():
            try:
                stats.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return env, runs, stats


def window_usage(stats, system, start_ms, end_ms):
    """Per-service CPU (cores) and memory in [start, end] for one system's samples."""
    if not start_ms or not end_ms:
        return None
    per = defaultdict(list)
    # Short windows (smoke runs) may fall between two `docker stats` passes on multi-node
    # stacks: widen by up to 3 s on each side before giving up.
    # A `docker stats` sample is stamped when its pass starts and its numbers can already
    # include the next run (seen: an idle window's last sample at 150 % CPU / 329 MiB from the
    # saturation run starting right after it). Drop the last 2.5 s of windows longer than 10 s.
    if end_ms - start_ms > 10_000:
        end_ms -= 2_500
    for pad in (0, 1.5, 3):
        s, e = start_ms / 1000 - pad, end_ms / 1000 + pad
        for r in stats:
            if r.get("system") == system and s <= r["ts"] <= e:
                per[r["service"]].append(r)
        if per and any(k in BROKER_SERVICES for k in per):
            break
        per.clear()
    if not per:
        return None
    out = {}
    for svc, rows in per.items():
        cpu = [r["cpu_pct"] / 100 for r in rows]
        mem = [r["mem_bytes"] for r in rows]
        out[svc] = {"cpu_avg": statistics.mean(cpu), "cpu_max": max(cpu),
                    "mem_avg": statistics.mean(mem), "mem_max": max(mem), "n": len(rows)}
    brokers = {k: v for k, v in out.items() if k in BROKER_SERVICES}
    clients = {k: v for k, v in out.items() if k in CLIENT_SERVICES}
    return {
        "services": out,
        "broker_cpu": sum(v["cpu_avg"] for v in brokers.values()) if brokers else None,
        "broker_cpu_peak": sum(v["cpu_max"] for v in brokers.values()) if brokers else None,
        "broker_mem": sum(v["mem_max"] for v in brokers.values()) if brokers else None,
        "client_cpu": sum(v["cpu_avg"] for v in clients.values()) if clients else None,
        "samples": min(v["n"] for v in out.values()),
    }


def aggregate(runs):
    """Repeats of one configuration -> one record: medians, range, the median run (for
    latency / resources), drained only if every repeat drained."""
    runs = [r for r in runs if r]
    if not runs:
        return None
    ok = sorted(runs, key=lambda r: r.get("consume_rate") or 0)
    rep = ok[(len(ok) - 1) // 2]
    cons = [r.get("consume_rate") or 0 for r in ok]
    pubs = sorted(r.get("publish_rate") or 0 for r in ok)
    out = dict(rep)
    out["consume_rate"] = statistics.median(cons)
    out["publish_rate"] = statistics.median(pubs)
    out["publish_mb_s"] = statistics.median(sorted(r.get("publish_mb_s") or 0 for r in ok))
    out["range"] = (min(cons), max(cons)) if len(ok) > 1 else None
    out["n"] = len(ok)
    out["drained"] = all(r.get("drained") for r in ok)
    out["reps"] = ok
    out["errors"] = sum(r.get("errors") or 0 for r in ok)
    out["rep"] = rep
    return out


def fmt_range(a):
    if not a or not a.get("range"):
        return "–"
    lo, hi = a["range"]
    return f"{fmt_rate(lo)}–{fmt_rate(hi)}"


def bench_cpus_of(runs, system, default):
    # Rows measured in this set first; rows reused from an earlier set (reused_from) last.
    for r in sorted(runs, key=lambda r: bool(r.get("reused_from"))):
        if r.get("system_name") == system and (r.get("limits") or {}).get("bench_cpus"):
            try:
                return float(r["limits"]["bench_cpus"])
            except (TypeError, ValueError):
                pass
    return default


# ----------------------------------------------------------------------------------------
# SVG charts


def tip_attr(text):
    return f' data-tip="{esc(text, quote=True)}" tabindex="0"'


def hbar(rows, unit_fmt, title, note=None, max_value=None):
    """rows: [(label, value|None, family, tooltip, flag)] -> horizontal bar chart."""
    rows = [r for r in rows]
    if not rows:
        return ""
    W, label_w, row_h, top, right = 760, 230, 28, 8, 90
    H = top + row_h * len(rows) + 26
    vmax = max_value or max([r[1] or 0 for r in rows] + [r[5][1] for r in rows if len(r) > 5 and r[5]] + [1e-9])
    plot_w = W - label_w - right
    nice = nice_max(vmax)
    parts = [f'<svg class="chart" viewBox="0 0 {W} {H}" role="img" aria-label="{esc(title)}">']
    # grid
    for i in range(5):
        x = label_w + plot_w * i / 4
        parts.append(f'<line class="grid" x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{H - 22}"/>')
        parts.append(f'<text class="axis" x="{x:.1f}" y="{H - 6}" text-anchor="middle">{esc(unit_fmt(nice * i / 4))}</text>')
    for i, row in enumerate(rows):
        label, value, fam, tip, flag = row[:5]
        rng = row[5] if len(row) > 5 else None
        y = top + i * row_h
        parts.append(f'<text class="label" x="{label_w - 10}" y="{y + row_h / 2 + 4}" text-anchor="end">{esc(label)}</text>')
        if value is None:
            parts.append(f'<text class="muted" x="{label_w + 6}" y="{y + row_h / 2 + 4}">{esc(flag or "no data")}</text>')
            continue
        w = max(2.0, plot_w * value / nice)
        h = 14
        by = y + (row_h - h) / 2
        parts.append(
            f'<g class="hit"{tip_attr(tip)}><rect x="{label_w}" y="{y + 1}" width="{plot_w + right}" height="{row_h - 2}" fill="transparent"/>'
            f'<path class="bar fam-{fam}" d="{bar_path(label_w, by, w, h)}"/></g>')
        tx = label_w + w + 6
        if rng:
            x1 = label_w + max(0.0, plot_w * rng[0] / nice)
            x2 = label_w + plot_w * rng[1] / nice
            cy = y + row_h / 2
            parts.append(f'<path class="whisker" d="M{x1:.1f},{cy:.1f} H{x2:.1f} M{x1:.1f},{cy - 5:.1f} V{cy + 5:.1f} '
                         f'M{x2:.1f},{cy - 5:.1f} V{cy + 5:.1f}"/>')
            tx = max(tx, x2 + 6)
        txt = unit_fmt(value) + (f"  {flag}" if flag else "")
        parts.append(f'<text class="value" x="{tx:.1f}" y="{y + row_h / 2 + 4}">{esc(txt)}</text>')
    parts.append("</svg>")
    head = f'<figure class="fig"><figcaption>{esc(title)}</figcaption>'
    tail = f'<p class="note">{note}</p>' if note else ""
    return head + "".join(parts) + tail + "</figure>"


def bar_path(x, y, w, h, r=4):
    r = min(r, w / 2, h / 2)
    return (f"M{x:.1f},{y:.1f} H{x + w - r:.1f} Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f} "
            f"V{y + h - r:.1f} Q{x + w:.1f},{y + h:.1f} {x + w - r:.1f},{y + h:.1f} H{x:.1f} Z")


def nice_max(v):
    if v <= 0:
        return 1
    exp = 10 ** math.floor(math.log10(v))
    for m in (1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if v <= m * exp:
            return m * exp
    return 10 * exp


def latency_plot(rows, title):
    """rows: [(label, family, {p50,p99,p999}|None, flag)] -> dot plot on a log axis."""
    vals = [v for _, _, p, _ in rows if p for v in (p.get("p50"), p.get("p99"), p.get("p999")) if v]
    if not vals:
        return ""
    lo = 10 ** math.floor(math.log10(max(min(vals), 0.01)))
    hi = 10 ** math.ceil(math.log10(max(vals)))
    if hi <= lo:
        hi = lo * 10
    W, label_w, row_h, top, right = 760, 230, 30, 8, 30
    H = top + row_h * len(rows) + 26
    plot_w = W - label_w - right

    def X(v):
        return label_w + plot_w * (math.log10(max(v, lo)) - math.log10(lo)) / (math.log10(hi) - math.log10(lo))

    parts = [f'<svg class="chart" viewBox="0 0 {W} {H}" role="img" aria-label="{esc(title)}">']
    d = lo
    while d <= hi * 1.0001:
        x = X(d)
        parts.append(f'<line class="grid" x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{H - 22}"/>')
        parts.append(f'<text class="axis" x="{x:.1f}" y="{H - 6}" text-anchor="middle">{esc(fmt_ms(d))}</text>')
        d *= 10
    for i, (label, fam, p, flag) in enumerate(rows):
        y = top + i * row_h + row_h / 2
        parts.append(f'<text class="label" x="{label_w - 10}" y="{y + 4}" text-anchor="end">{esc(label)}</text>')
        if not p:
            parts.append(f'<text class="muted" x="{label_w + 6}" y="{y + 4}">{esc(flag or "no data")}</text>')
            continue
        x50, x99, x999 = X(p["p50"]), X(p["p99"]), X(p["p999"])
        tip = (f"{label}: p50 {fmt_ms(p['p50'])}, p99 {fmt_ms(p['p99'])}, p99.9 {fmt_ms(p['p999'])}"
               + (f" ({flag})" if flag else ""))
        parts.append(f'<g class="hit"{tip_attr(tip)}>'
                     f'<rect x="{label_w}" y="{y - row_h / 2 + 1}" width="{plot_w + right}" height="{row_h - 2}" fill="transparent"/>'
                     f'<line class="span fam-{fam}" x1="{x50:.1f}" y1="{y}" x2="{x999:.1f}" y2="{y}"/>'
                     f'<circle class="dot fam-{fam}" cx="{x50:.1f}" cy="{y}" r="5"/>'
                     f'<rect class="dot fam-{fam}" x="{x99 - 5:.1f}" y="{y - 5}" width="10" height="10" rx="1.5"/>'
                     f'<path class="dot hollow fam-{fam}" d="M{x999:.1f},{y - 6} L{x999 + 6:.1f},{y} L{x999:.1f},{y + 6} L{x999 - 6:.1f},{y} Z"/>'
                     "</g>")
        if flag:
            parts.append(f'<text class="muted" x="{min(x999 + 12, W - 4):.1f}" y="{y + 4}" text-anchor="{"end" if x999 + 12 > W - 60 else "start"}">{esc(flag)}</text>')
    parts.append("</svg>")
    legend = ('<div class="legend"><span><svg width="14" height="14"><circle class="dot legend-ink" cx="7" cy="7" r="5"/></svg> p50</span>'
              '<span><svg width="14" height="14"><rect class="dot legend-ink" x="2" y="2" width="10" height="10" rx="1.5"/></svg> p99</span>'
              '<span><svg width="14" height="14"><path class="dot hollow legend-ink" d="M7,1 L13,7 L7,13 L1,7 Z"/></svg> p99.9</span>'
              '<span class="muted">log scale</span></div>')
    return f'<figure class="fig"><figcaption>{esc(title)}</figcaption>{legend}{"".join(parts)}</figure>'


def table(headers, rows, cls=""):
    h = "".join(f"<th>{esc(x)}</th>" for x in headers)
    b = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="tablewrap"><table class="{cls}"><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table></div>'


def details(summary, body):
    return f'<details><summary>{esc(summary)}</summary>{body}</details>'


# ----------------------------------------------------------------------------------------
# Report


def build(results, env, runs, stats, extra_sections=()):
    harness = [r for r in runs if r.get("kind") == "harness"]
    excluded = [r for r in harness if r.get("excluded")]
    harness = [r for r in harness if not r.get("excluded")]
    sat = [r for r in harness if r.get("scenario") == "saturation"]
    lat = [r for r in harness if r.get("scenario") == "latency"]
    skipped = [r for r in runs if r.get("kind") == "skipped" and r.get("scenario") == "latency"]
    failed = [r for r in runs if r.get("kind") in ("failed", "system-failed")]
    invalid = [r for r in runs if r.get("kind") == "invalid"]
    idle = {r["system_name"]: r for r in runs if r.get("kind") == "idle"}
    sysinfo = {r["system_name"]: r for r in runs if r.get("kind") == "system"}
    batch = [r for r in runs if r.get("kind") == "fluxmq-bench"]
    present = [s for s in ORDER if any(r.get("system_name") == s for r in runs)]
    smoke = bool(env.get("smoke"))
    sections = []

    reused = v08.reused_systems(runs)

    def title_of(s):
        return TITLES.get(s, s) + (f" ◦ {reused[s].split('-')[-1]} data" if s in reused else "")

    # ---------------- saturation
    configs = sorted({(r["params"]["size"], r["params"]["producers"], r["params"]["consumers"]) for r in sat},
                     key=lambda c: (-c[0], c[1]))
    groups = defaultdict(list)
    for r in sat:
        groups[(r["system_name"], r["params"]["size"], r["params"]["producers"], r["params"]["consumers"])].append(r)
    sat_by = {k: aggregate(v) for k, v in groups.items()}
    repeated = any(a["n"] > 1 for a in sat_by.values())
    sat_html = []
    for (size, pr, co) in configs:
        rows = []
        trows = []
        for s in present:
            r = sat_by.get((s, size, pr, co))
            if not r:
                continue
            v = r.get("consume_rate")
            flag = "" if r.get("drained") else "not drained"
            misses = [x.get("drain_miss") or {} for x in r.get("reps", [])]
            missing = sum(miss_true(m) for m in misses)
            dropped = sum((m.get("classification") or {}).get("retention_dropped") or 0 for m in misses)
            stored = sum((m.get("classification") or {}).get("stored_undelivered") or 0 for m in misses)
            if dropped or stored:
                flag = "consumers behind producers" + (": backlog > retention cap" if dropped else "")
            if missing:
                flag = f"{missing:,} MISSING"
            if r.get("errors"):
                flag = (flag + f" {r['errors']} err").strip()
            if r.get("reused_from"):
                flag = (flag + f" · {r['reused_from'].split('-')[-1]} run, reused").strip(" ·")
            tip = (f"{title_of(s)}: publish {fmt_num(r.get('publish_rate'))} msg/s "
                   f"({r.get('publish_mb_s'):.1f} MB/s), consume {fmt_num(v)} msg/s"
                   + (f" (median of {r['n']}, range {fmt_range(r)})" if r.get("n", 1) > 1 else "")
                   + f", e2e p99 {fmt_ms((r.get('e2e_ms') or {}).get('p99'))}")
            rows.append((title_of(s), v, family(s), tip, flag, r.get("range")))
            e2e = r.get("e2e_ms") or {}
            trows.append([esc(title_of(s)) + v07._rmark(r), fmt_num(r.get("publish_rate")), f"{r.get('publish_mb_s', 0):.1f}",
                          fmt_num(v), fmt_range(r), str(r.get("n", 1)), fmt_ms(e2e.get("p50")), fmt_ms(e2e.get("p99")),
                          drained_cell(r), fmt_num(r.get("errors")),
                          esc(str((r.get("limits") or {}).get("bench_cpus", "?")))])
        sat_html.append(hbar(rows, lambda x: fmt_rate(x) + "/s",
                             f"Consume throughput, {size} B messages, {pr} producer(s) × {co} consumer(s) (msg/s, higher is better)",
                             note=("Bar = median of the repeats, whisker = min–max." if repeated else None)))
        sat_html.append(details("Table: " + f"{size} B, {pr}×{co}", table(
            ["System", "Publish msg/s", "Publish MB/s", "Consume msg/s", "Range", "Runs", "E2E p50 (median run)",
             "E2E p99 (median run)", "Drained", "Errors", "Client CPUs"], trows)))
    if sat_html:
        sections.append(("throughput", "Throughput (saturation)",
                         "<p>Closed loop: every producer keeps up to the window of unacknowledged "
                         "publishes in flight; consumers ack every message. The bar is the consume rate in "
                         "the measured window (publish and consume match when the system keeps up). "
                         "End-to-end latency under saturation mostly measures queueing and is shown in the "
                         "tables only.</p>" + "".join(sat_html)))

    # ---------------- latency
    rates = sorted({r["params"]["rate"] for r in lat} | {r["params"]["rate"] for r in skipped})
    lat_by = {(r["system_name"], r["params"]["rate"]): r for r in lat}
    skip_by = {(r["system_name"], r["params"]["rate"]): r for r in skipped}
    lat_html = []
    for rate in rates:
        rows, trows = [], []
        for s in present:
            r = lat_by.get((s, rate))
            if r:
                e2e = r.get("e2e_ms")
                flag = "" if r.get("sustained") else f"not sustained ({fmt_rate(r.get('publish_rate'))}/s)"
                if r.get("reused_from"):
                    flag = (flag + f" · {r['reused_from'].split('-')[-1]} run, reused").strip(" ·")
                rows.append((title_of(s), family(s), e2e, flag))
                e = e2e or {}
                trows.append([esc(title_of(s)) + v07._rmark(r), fmt_num(r.get("publish_rate")), fmt_num(r.get("consume_rate")),
                              fmt_ms(e.get("p50")), fmt_ms(e.get("p99")), fmt_ms(e.get("p999")),
                              fmt_ms(e.get("max")), "yes" if r.get("sustained") else "<b>no</b>"])
            elif (s, rate) in skip_by:
                reason = skip_by[(s, rate)].get("reason", "skipped")
                rows.append((title_of(s), family(s), None, "skipped: " + reason))
                trows.append([esc(title_of(s)), "–", "–", "–", "–", "–", "–", esc("skipped: " + reason)])
        if rows:
            lat_html.append(latency_plot(rows, f"End-to-end latency at {rate:,} msg/s, 1 KiB (lower is better)"))
            lat_html.append(details(f"Table: {rate:,} msg/s", table(
                ["System", "Publish msg/s", "Consume msg/s", "p50", "p99", "p99.9", "max", "Sustained"], trows)))
    if lat_html:
        sections.append(("latency", "Latency at fixed rates",
                         "<p>Open loop: sends are scheduled at a fixed rate and latency is measured from "
                         "the <i>scheduled</i> send time to receipt by a consumer (coordinated-omission "
                         "corrected). A rate counts as sustained when publish and consume reach 95 % of the "
                         "target and the window drained; higher rates are skipped once a system fails a "
                         "lower one or its saturation throughput is below the rate.</p>" + "".join(lat_html)))

    # ---------------- resources
    res_rows_idle, res_rows_load, rtab = [], [], []
    try:
        bench_cpus = float(env.get("limits", {}).get("bench_cpus", 2))
    except (TypeError, ValueError):
        bench_cpus = 2.0
    for s in present:
        iu = window_usage(stats, s, idle.get(s, {}).get("measure_start_ms"), idle.get(s, {}).get("measure_end_ms"))
        # under load = the 1 KiB saturation run with the most clients
        cands = [a for (sn, size, _, _), a in sat_by.items() if sn == s and size == 1024]
        cands.sort(key=lambda a: a["params"]["producers"] * a["params"]["consumers"])
        lr = cands[-1]["rep"] if cands else None
        try:
            sys_bench_cpus = float(((lr or {}).get("limits") or {}).get("bench_cpus"))
        except (TypeError, ValueError):
            sys_bench_cpus = bench_cpus_of(runs, s, bench_cpus)
        lu = window_usage(stats, s, lr.get("measure_start_ms"), lr.get("measure_end_ms")) if lr else None
        fam = family(s)
        if iu:
            res_rows_idle.append((title_of(s), iu["broker_mem"] / 2**20, fam,
                                  f"{title_of(s)} idle: {fmt_mb(iu['broker_mem'])}, CPU {iu['broker_cpu']:.2f} cores", ""))
        if lu:
            res_rows_load.append((title_of(s), lu["broker_cpu"], fam,
                                  f"{title_of(s)} under load: CPU avg {lu['broker_cpu']:.2f} cores, "
                                  f"peak {lu['broker_cpu_peak']:.2f}, memory {fmt_mb(lu['broker_mem'])}", ""))
        if iu or lu:
            client_cpu = (lu or {}).get("client_cpu")
            rtab.append([esc(title_of(s)) + v07._rmark(lr),
                         f"{iu['broker_cpu']:.2f}" if iu else "–", fmt_mb(iu["broker_mem"]) if iu else "–",
                         f"{lu['broker_cpu']:.2f}" if lu else "–", f"{lu['broker_cpu_peak']:.2f}" if lu else "–",
                         fmt_mb(lu["broker_mem"]) if lu else "–",
                         (("<b>" + f"{client_cpu:.2f} client-bound" + "</b>")
                          if client_cpu is not None and client_cpu >= 0.9 * sys_bench_cpus
                          else (f"{client_cpu:.2f}" if client_cpu is not None else "–")),
                         f"{sys_bench_cpus:g}",
                         (f"{fmt_rate(lr['consume_rate'])}/s" if lr else "–"),
                         (f"{lr['consume_rate'] / lu['broker_cpu'] / 1000:.1f}k" if lr and lu and lu["broker_cpu"] else "–"),
                         str(min((iu or {}).get("samples", 99), (lu or {}).get("samples", 99)))])
    if rtab:
        lim = env.get("limits", {})
        body = (f"<p>From <code>docker stats</code>, sampled continuously (one sample every 1–2 s). CPU in cores "
                f"(1.0 = one core), summed over the system's broker containers; memory = cgroup usage "
                f"(excluding page cache that the kernel can drop), summed. Each broker container is limited to "
                f"{esc(str(lim.get('broker_cpus', '?')))} CPUs and {esc(str(lim.get('broker_mem', '?')))}; "
                f"the client container to the CPUs in the \"Client limit\" column. Kafka's JVM reserves its "
                f"heap (-Xms1g) up front, so its idle memory is mostly heap, not live data.</p>")
        body += hbar(res_rows_idle, lambda x: f"{x:,.0f} MiB", "Idle memory, all broker containers (lower is better)")
        body += hbar(res_rows_load, lambda x: f"{x:.2f} cores",
                     "Broker CPU during the 1 KiB saturation run with the most clients (average, cores)")
        body += ("<p class=\"note\">Under load = the median repeat of the 1 KiB saturation configuration with "
                 "the most clients.</p>")
        body += table(["System", "Idle CPU", "Idle memory", "Load CPU avg", "Load CPU peak", "Load memory",
                       "Client CPU", "Client limit", "Consume rate", "msg per CPU·s", "Samples (min)"], rtab)
        body += ('<p class="note">"Client CPU" near the client container\'s CPU limit means the harness, not the '
                 'broker, may be the bottleneck for that run. "msg per CPU·s" = consume rate ÷ broker CPU.</p>')
        sections.append(("resources", "Resource usage", body))

    # ---------------- TLS vs plaintext
    tls_rows = []
    for plain, tls in (("fluxmq-single", "fluxmq-single-tls"), ("fluxmq-cluster", "fluxmq-cluster-tls")):
        for (size, pr, co) in configs:
            a, b = sat_by.get((plain, size, pr, co)), sat_by.get((tls, size, pr, co))
            if a and b and a.get("consume_rate"):
                d = (b["consume_rate"] - a["consume_rate"]) / a["consume_rate"] * 100
                tls_rows.append([esc(title_of(plain).replace("FluxMQ · ", "")), f"saturation {size} B {pr}×{co}",
                                 fmt_num(a["consume_rate"]) + " msg/s", fmt_num(b["consume_rate"]) + " msg/s",
                                 f"{d:+.1f} %"])
        for rate in rates:
            a, b = lat_by.get((plain, rate)), lat_by.get((tls, rate))
            if a and b and a.get("e2e_ms") and b.get("e2e_ms"):
                tls_rows.append([esc(title_of(plain).replace("FluxMQ · ", "")), f"latency {rate:,}/s p50 / p99",
                                 f"{fmt_ms(a['e2e_ms']['p50'])} / {fmt_ms(a['e2e_ms']['p99'])}",
                                 f"{fmt_ms(b['e2e_ms']['p50'])} / {fmt_ms(b['e2e_ms']['p99'])}",
                                 f"{(b['e2e_ms']['p99'] - a['e2e_ms']['p99']):+.2f} ms p99"])
    if tls_rows:
        sections.append(("tls", "FluxMQ: TLS vs plaintext",
                         "<p>Same broker image and settings; TLS = client port TLS 1.3 (rustls/aws-lc-rs, gen-dev "
                         "ECDSA P-256 certificates) and, in the cluster, HTTPS plus node-to-node mTLS for Raft, "
                         "replication and internal HTTP. Kafka and RabbitMQ were measured plaintext only.</p>"
                         + table(["Topology", "Measurement", "Plaintext", "TLS", "Δ"], tls_rows)))

    # ---------------- fsync=os vs group commit
    os_rows = []
    for osv, base in OS_VARIANTS.items():
        for (size, pr, co) in configs:
            a, b = sat_by.get((base, size, pr, co)), sat_by.get((osv, size, pr, co))
            if a and b and a.get("consume_rate"):
                d = (b["consume_rate"] - a["consume_rate"]) / a["consume_rate"] * 100
                os_rows.append([esc(title_of(base).replace("FluxMQ · ", "")), f"saturation {size} B {pr}×{co}",
                                fmt_num(a["consume_rate"]) + " msg/s", fmt_num(b["consume_rate"]) + " msg/s",
                                f"{d:+.1f} %"])
        for rate in rates:
            a, b = lat_by.get((base, rate)), lat_by.get((osv, rate))
            if a and b and a.get("e2e_ms") and b.get("e2e_ms"):
                os_rows.append([esc(title_of(base).replace("FluxMQ · ", "")), f"latency {rate:,}/s p50 / p99",
                                f"{fmt_ms(a['e2e_ms']['p50'])} / {fmt_ms(a['e2e_ms']['p99'])}",
                                f"{fmt_ms(b['e2e_ms']['p50'])} / {fmt_ms(b['e2e_ms']['p99'])}",
                                f"{(b['e2e_ms']['p99'] - a['e2e_ms']['p99']):+.2f} ms p99"])
    if any(s in OS_VARIANTS for s in present):
        sections.append(("fsync-os", "FluxMQ: fsync=os (" + OS_LABEL + ")",
                         "<p><span class=\"syslegend\"><span><i style=\"background:var(--fam-fluxmqos)\"></i>"
                         "‡ FluxMQ with <code>FLUXMQ_FSYNC=os</code></span></span> Same image and settings as the "
                         "group-commit rows, but a publish is acknowledged after <code>write(2)</code> (in a cluster: "
                         "after every ISR member has written it), with no fsync; the OS flushes later. That is the "
                         "durability point of Kafka's defaults (page cache + replication, no fsync per message), so "
                         "these rows are the like-for-like comparison with Kafka. A power failure can lose "
                         "acknowledged messages (on one node; in a cluster unless another replica survives). "
                         "<b>The headline numbers and conclusions use group commit</b> (fsync before every ack), "
                         "FluxMQ's default.</p>"
                         + (table(["Topology", "Measurement", "Group commit", "fsync=os", "Δ"], os_rows)
                            if os_rows else "<p>No group-commit counterpart in this result set.</p>")))

    # ---------------- v0.7: comparisons, catch-up / overload, reliability, failover
    sections.extend(v07.comparison_sections(sat_by, lat_by, configs, rates, title_of))
    sections.extend(v07.catchup_overload_sections(runs, present, title_of))
    for sec in (v07.reliability_section(results), v07.failover_section(results),
                v07.quickfix_section(results, sat_by, lat_by, title_of, load, aggregate),
                v07.improvements_section(results),
                v08.tls_history_section(results, sat_by, title_of, load, aggregate),
                v08.tls_ab_section(results, title_of),
                v08.retention_section(results, title_of),
                v08.ab_section(results, sat_by, lat_by, title_of, load, aggregate),
                v08.disk_section(results, title_of)):
        if sec:
            sections.append(sec)

    # ---------------- batching
    if batch:
        grp = defaultdict(list)
        revs = {}
        for r in batch:
            grp[(r["run"], r["label"])].append(r)
            revs[r["label"]] = r.get("revision") or r.get("image")
        same = len(set(revs.values())) == 1

        def med(rs, f):
            v = [f(r) for r in rs if f(r) is not None]
            return (statistics.median(v), min(v), max(v), len(v)) if v else None

        def cell(m, fmt):
            if not m:
                return "–"
            v, lo, hi, n = m
            return fmt(v) + (f" <span class=\"muted-inline\">[{fmt(lo)}–{fmt(hi)}, n={n}]</span>" if n > 1 else "")

        brows = []
        spec = [
            ("drain-1KiB", "drain 1 KiB (1 M prefilled): consume msg/s", "rate"),
            ("drain-100B", "drain 100 B (3 M prefilled): consume msg/s", "rate"),
            ("closed-1KiB", "closed loop 1 KiB: consume msg/s", "rate"),
            ("closed-100B", "closed loop 100 B: consume msg/s", "rate"),
            ("light-1k", "1k msg/s, 1 msg per request: e2e p50", "p50"),
            ("light-1k", "1k msg/s, 1 msg per request: e2e p99", "p99"),
        ]
        for rn, label, what in spec:
            a, b = grp.get((rn, "baseline")), grp.get((rn, "current"))
            if not a or not b:
                continue
            if what == "rate":
                f = (lambda r: r.get("consume_rate"))
                ma, mb = med(a, f), med(b, f)
                fmt = fmt_num
                delta = f"{(mb[0] - ma[0]) / ma[0] * 100:+.1f} %" if ma and mb and ma[0] else "–"
            else:
                f = (lambda r, w=what: (r.get("e2e_ms") or {}).get(w))
                ma, mb = med(a, f), med(b, f)
                fmt = fmt_ms
                delta = f"{(mb[0] - ma[0]):+.2f} ms" if ma and mb else "–"
            def drain_failed(rs, m):
                if m or not rs or not all(r.get("exit") for r in rs):
                    return None
                return (f'<b>failed</b> <span class="muted-inline">(bench exited {rs[0].get("exit")} in all {len(rs)} '
                        f'rounds: the v0.5 SDK does not connect to a v0.4 broker)</span>')
            brows.append([esc(label), drain_failed(a, ma) or cell(ma, fmt), drain_failed(b, mb) or cell(mb, fmt), delta])
        note = ("<p class=\"warn\">This run compared an image with itself (the v0.4 baseline image was not "
                "built), so it only proves the measurement path; the numbers say nothing about batching.</p>"
                if same else "")
        rounds = max((r.get("repeat") or 1) for r in batch)
        sections.append(("batching", "FluxMQ v0.5 batched delivery vs v0.4",
                         f"<p>Single broker, plaintext, same container limits; baseline = v0.4 "
                         f"(<code>{esc(str(revs.get('baseline')))}</code>), current = v0.5 "
                         f"(<code>{esc(str(revs.get('current')))}</code>); {rounds} round(s), baseline and current "
                         f"interleaved, a fresh broker per round; median with [min–max]. <b>Drain</b> rows use the "
                         f"<i>current</i> <code>fluxmq-bench --drain</code> against both brokers (the v0.4 bench has no "
                         f"drain mode): N messages are prefilled, then 4 consumers (prefetch 2000) read them from the "
                         f"earliest offset; this isolates the consumer path (one <code>Deliver</code> frame per message "
                         f"vs <code>DeliverBatch</code>). <b>Closed-loop</b> and <b>1k msg/s</b> rows run each image's own "
                         f"bench (old SDK + old broker vs new SDK + new broker) with publishing and consuming at the "
                         f"same time.</p>"
                         + note + table(["Measurement", "v0.4 baseline", "v0.5 current", "Δ"], brows)))

    # ---------------- summary + conclusions
    concl = conclusions(present, sat_by, lat_by, skip_by, configs, rates, title_of,
                        max([a["n"] for a in sat_by.values()] or [1]))
    summary_cards = []
    for (size, pr, co) in configs[:1]:
        best = sorted([(sat_by[(s, size, pr, co)].get("consume_rate") or 0, s) for s in present
                       if (s, size, pr, co) in sat_by], reverse=True)
        if best:
            summary_cards.append(("Highest throughput", f"{fmt_rate(best[0][0])} msg/s",
                                  f"{title_of(best[0][1])} · {size} B {pr}×{co}"))
    if rates:
        r0 = rates[0]
        cand = [(lat_by[(s, r0)]["e2e_ms"]["p99"], s) for s in present
                if (s, r0) in lat_by and lat_by[(s, r0)].get("e2e_ms") and lat_by[(s, r0)].get("sustained")]
        if cand:
            cand.sort()
            summary_cards.append((f"Lowest p99 at {r0:,}/s", fmt_ms(cand[0][0]), title_of(cand[0][1])))
    summary_cards.append(("Systems measured", str(len(present)), f"{len(harness)} runs, {len(failed)} failed"))
    cards = "".join(f'<div class="card"><div class="k">{esc(k)}</div><div class="v">{esc(v)}</div>'
                    f'<div class="s">{esc(s)}</div></div>' for k, v, s in summary_cards)
    banner = ('<p class="warn"><b>Smoke run.</b> Every scenario ran for a few seconds only to prove the harness '
              'works end to end. These numbers are not benchmarks; do not quote them.</p>' if smoke else "")
    fail_html = ""
    if failed:
        fail_html = "<p class=\"warn\">Failed runs: " + ", ".join(
            esc(f"{r.get('run_id')}: {r.get('error', '')[:160]}") for r in failed) + "</p>"
    notes_path = results / "notes.html"
    notes = notes_path.read_text() if notes_path.exists() else ""
    # Later findings about a frozen set live next to the generator, not in the set itself.
    errata_path = Path(__file__).resolve().parent / "errata" / f"{results.name}.html"
    if errata_path.exists():
        notes = ('<div class="warn"><b>Erratum (added after the run; measurements unchanged).</b> '
                 + errata_path.read_text() + "</div>" + notes)
    if excluded:
        fail_html += ('<p class="warn">Excluded from medians (kept on record): '
                      + "; ".join(esc(f"{r.get('run_id')}: {r['excluded']}") for r in excluded) + "</p>")
    if invalid:
        fail_html += ('<p class="warn">Excluded runs (kept on record, not used in any chart or verdict): '
                      + "; ".join(esc(f"{r.get('run_id')}: {r.get('invalid_reason', '')}"
                                      + (f" (publish {fmt_num(r.get('publish_rate'))} msg/s, {r.get('errors')} errors)"
                                         if r.get('publish_rate') else "")
                                      + (" [reconstructed from the runner log]" if r.get('reconstructed') else ""))
                                  for r in invalid) + "</p>")
    sections.insert(0, ("summary", "Summary", banner + f'<div class="cards">{cards}</div>' + fail_html
                        + (notes if notes else "")
                        + "<h3>Computed verdicts</h3>" + concl))

    for sec in reversed(list(extra_sections)):
        sections.insert(1, sec)
    goals = v08.goals_section(results)
    if goals:
        sections.insert(0, goals)
    ans = v07.answers_section(results)
    if ans:
        sections.insert(0, ans)
    integrity = env.get("integrity_note")
    if integrity:
        sections[0] = (sections[0][0], sections[0][1],
                       f'<p class="warn"><b>Partial result set.</b> {esc(integrity)}</p>' + sections[0][2])

    # ---------------- methodology / durability / environment / caveats
    sections.append(("methodology", "Methodology", METHODOLOGY.format(
        partitions=esc(str(env.get("plan", {}).get("partitions", 8))),
        window=esc(str(env.get("plan", {}).get("window", 2000))),
        linger=esc(str(env.get("plan", {}).get("linger_ms", 5))),
        lat_linger=esc(str(env.get("plan", {}).get("latency_linger_ms", 1))),
        warmup=esc(str(env.get("plan", {}).get("warmup", "?"))),
        duration=esc(str(env.get("plan", {}).get("duration", "?"))),
        lat_duration=esc(str(env.get("plan", {}).get("lat_duration", "?"))),
        retention=esc(str(env.get("plan", {}).get("retention_mb_per_partition", 128))),
        segments=(f" Segment size {esc(str(env['plan']['segment_mb']))} MiB for FluxMQ (broker "
                  f"<code>FLUXMQ_SEGMENT_BYTES</code>) and Kafka (topic <code>segment.bytes</code>); RabbitMQ "
                  f"has no equivalent setting." if env.get("plan", {}).get("segment_mb") else ""),
        inflight=(f" (FluxMQ: up to {esc(str(env['plan']['max_in_flight']))} <code>Publish</code> requests "
                  f"pipelined per producer)" if env.get("plan", {}).get("max_in_flight") else ""),
        repeats=repeats_note(harness),
        nostamp=(" FluxMQ producers run with <code>stamp_producer=false</code>: no <code>flux-producer</code> "
                 "header per message, like the Kafka and RabbitMQ clients, which send no header either."
                 if any("stamp_producer=false" in str(r.get("client") or "") for r in harness) else ""),
        idcheck=(" Checked by message id (producer + sequence number in the payload): every message the "
                 "broker acknowledged must arrive at least once. On a miss the harness keeps waiting "
                 f"{esc(str(env['plan']['late_drain']))} s more and reports each missed message as late "
                 "or missing." if env.get("plan", {}).get("late_drain") else ""),
    )))
    dur = {}
    clients = {}
    for r in harness:
        dur.setdefault(r["system_name"], r.get("durability"))
        clients.setdefault(r["system_name"], r.get("client"))
    drows = [[esc(title_of(s)), esc(sysinfo.get(s, {}).get("broker_version") or "–"), esc(dur.get(s) or "–"),
              esc(clients.get(s) or "–")] for s in present]
    sections.append(("durability", "Durability settings per system",
                     DURABILITY + table(["System", "Broker", "Durability as configured", "Client"], drows)))
    sections.append(("environment", "Environment", env_html(env, sysinfo, present, title_of, runs)))
    reps = max([a["n"] for a in sat_by.values()] or [1])
    repeat_note = (f"Each saturation configuration ran {reps}× (interleaved); bars show the median, whiskers the range. "
                   "Latency, idle and resource figures come from one run each (resources: the median saturation repeat)."
                   if reps > 1 else "Each configuration ran once; run-to-run variance was not measured.")
    sections.append(("caveats", "Caveats", CAVEATS.format(repeat_note=repeat_note)))

    nav = "".join(f'<a href="#{sid}">{esc(t)}</a>' for sid, t, _ in sections)
    body = "".join(f'<section id="{sid}"><h2>{esc(t)}</h2>{content}</section>' for sid, t, content in sections)
    when = env.get("captured_at", "")
    return PAGE.format(title="FluxMQ Benchmarks", css=CSS + v07.ANSWERS_CSS + v08.CSS, js=JS + v07.ANSWERS_JS, nav=nav, body=body,
                       subtitle=esc(f"FluxMQ vs Apache Kafka vs RabbitMQ · {when} · results: {results.name}"
                                    + (" · SMOKE" if smoke else "")))


def repeats_note(harness):
    """' 1-node saturation = median of 3 repeats; 3-node = median of 2.' from the data."""
    reps = defaultdict(int)
    for r in harness:
        if r.get("scenario") == "saturation":
            topo = "3-node" if any(x in r.get("system_name", "") for x in ("cluster",)) else "1-node"
            reps[topo] = max(reps[topo], r.get("repeat") or 1)
    if len(set(reps.values())) <= 1:
        return ""
    return " " + "; ".join(f"{t} saturation = median of {n} repeats" for t, n in sorted(reps.items())) + "."


def conclusions(present, sat_by, lat_by, skip_by, configs, rates, title_of, reps=1):
    items = []
    pairs = [("single", ["fluxmq-single", "kafka-single", "rabbitmq-single"]),
             ("3-node", ["fluxmq-cluster", "kafka-cluster", "rabbitmq-cluster"])]
    for topo, systems in pairs:
        flux = systems[0]
        for (size, pr, co) in configs:
            vals = {s: sat_by[(s, size, pr, co)].get("consume_rate") for s in systems if (s, size, pr, co) in sat_by}
            if flux not in vals or len(vals) < 2:
                continue
            ranked = sorted(vals.items(), key=lambda kv: -(kv[1] or 0))
            pos = [s for s, _ in ranked].index(flux) + 1
            others = ", ".join(f"{title_of(s)} {fmt_rate(v)}/s" for s, v in ranked if s != flux)
            verdict = "leads" if pos == 1 else ("loses" if pos == len(ranked) else "is in the middle")
            items.append(f"<li><b>Throughput, {topo}, {size} B {pr}×{co}:</b> FluxMQ {verdict} "
                         f"({fmt_rate(vals[flux])}/s vs {esc(others)}).</li>")
        for rate in rates:
            vals = {}
            for s in systems:
                r = lat_by.get((s, rate))
                if r and r.get("e2e_ms") and r.get("sustained"):
                    vals[s] = r["e2e_ms"]["p99"]
            notes = [title_of(s) for s in systems if (s, rate) in skip_by
                     or ((s, rate) in lat_by and not lat_by[(s, rate)].get("sustained"))]
            if flux in vals and len(vals) >= 2:
                ranked = sorted(vals.items(), key=lambda kv: kv[1])
                pos = [s for s, _ in ranked].index(flux) + 1
                verdict = "lowest" if pos == 1 else ("highest" if pos == len(ranked) else "middle")
                others = ", ".join(f"{title_of(s)} {fmt_ms(v)}" for s, v in ranked if s != flux)
                items.append(f"<li><b>p99 at {rate:,}/s, {topo}:</b> FluxMQ {fmt_ms(vals[flux])} is the {verdict} "
                             f"(vs {esc(others)}).</li>")
            if notes:
                items.append(f"<li><b>{rate:,}/s, {topo}:</b> not sustained or skipped by "
                             f"{esc(', '.join(notes))}.</li>")
    if not items:
        return "<p>Not enough comparable runs for conclusions.</p>"
    items.append(f"<li>These verdicts are computed from this run's data ({'median of ' + str(reps) + ' runs' if reps > 1 else 'one run'} "
                 "per saturation configuration, one run per latency rate) on one laptop. "
                 "Read them together with the durability table: the systems do not pay the same durability cost "
                 "per acknowledged message (FluxMQ and RabbitMQ fsync before confirming; Kafka relies on "
                 "replication and the page cache by default).</li>")
    return "<ul class=\"concl\">" + "".join(items) + "</ul>"


def env_html(env, sysinfo, present, title_of, runs=()):
    per = defaultdict(list)
    for s in present:
        lim_s = next(((r.get("limits") or {}) for r in runs if r.get("system_name") == s and r.get("limits")), {})
        per[f"{lim_s.get('bench_cpus', env.get('limits', {}).get('bench_cpus'))} CPUs / "
            f"{lim_s.get('bench_mem', env.get('limits', {}).get('bench_mem'))}"].append(title_of(s))
    client_limits = "; ".join(f"{k} for {', '.join(v)}" for k, v in per.items()) or "?"
    h = env.get("host", {})
    d = env.get("docker", {})
    lim = env.get("limits", {})
    rows = [
        ["Host", esc(f"{h.get('model', '')} · {h.get('cpu', '')} · {h.get('cores', '?')} cores "
                     f"({h.get('perf_cores', '?')}P+{h.get('eff_cores', '?')}E) · {h.get('ram_gb', '?')} GB RAM")],
        ["OS", esc(str(h.get("os", "")))],
        ["Docker", esc(f"{d.get('server_version', '')} ({d.get('os', '')}), kernel {d.get('kernel', '')}, "
                       f"{d.get('arch', '')}, cgroup v{d.get('cgroup', '')}, storage {d.get('storage_driver', '')}")],
        ["Docker VM", esc(f"{d.get('vm_cpus', '?')} CPUs, {d.get('vm_mem_gb', '?')} GB RAM")],
        ["Limits", esc(f"each broker container {lim.get('broker_cpus')} CPUs / {lim.get('broker_mem')}; "
                       f"client container: {client_limits}")],
        ["Host disk free at start", esc(f"{h.get('disk_free_gb', '?')} GB")],
        ["Git", esc(f"{(env.get('git') or {}).get('sha', '')}{' (dirty working tree)' if (env.get('git') or {}).get('dirty') else ''}")],
        (["Commands", "<br>".join(f"<code>{esc(c)}</code>" for c in env["commands"])] if env.get("commands")
         else ["Command", esc("bench/run.sh " + " ".join(env.get("argv", [])))]),
        ["Total time", esc(f"{env.get('total_s', '?')} s")],
    ]
    for ref, i in (env.get("images") or {}).items():
        rows.append([esc(f"Image {ref}"), esc(f"{i.get('id')} · created {i.get('created')} · {i.get('size_mb')} MB"
                                              + (f" · rev {i['revision']}" if i.get("revision") else ""))])
    for s in present:
        si = sysinfo.get(s)
        if si:
            rows.append([esc(title_of(s)), esc(f"{si.get('broker_version') or '?'} · ready in {si.get('startup_s')} s")])
    return table(["", ""], rows, "kv")


METHODOLOGY = """
<p><b>One harness for every system.</b> <code>bench/harness</code> (<code>mq-harness</code>, Rust) drives FluxMQ
through its SDK, Kafka through librdkafka (rdkafka crate, built from source) and RabbitMQ through lapin (AMQP 0-9-1),
with one engine: the same producer loop, the same consumer accounting and the same HDR histograms. The first 8 bytes
of every payload carry the send time on the harness's monotonic clock; producers and consumers run in one process, so
end-to-end latency has no clock skew. The official perf tools (<code>kafka-producer-perf-test</code>,
RabbitMQ <code>perf-test</code>) were not used for the comparison because they measure latency differently
(Kafka's tool reports produce latency only, perf-test uses its own timestamping and a JVM client), which would mix
tool differences into broker differences.</p>
<ul>
<li><b>Topology per run:</b> a fresh topic / channel / set of queues with {partitions} partitions (RabbitMQ: {partitions}
queues used like partitions, producers round-robin over them, each consumer owns a share). Deleted after the run;
retention capped at {retention} MiB per partition / queue to bound disk use.{segments}</li>
<li><b>Producers:</b> one connection each, messages sent one by one through the library's async API with at most
{window} unacknowledged per producer{inflight}.{nostamp} Client-side batching is the library's own: FluxMQ <code>BatchingProducer</code>
and Kafka <code>linger.ms</code> both linger {linger} ms in saturation and {lat_linger} ms in the latency runs;
AMQP has no producer batching (frames are pipelined, confirms are asynchronous).</li>
<li><b>Consumers:</b> one connection each, one group (Kafka group / FluxMQ consumer group / queue share), starting at
the beginning of the fresh topic; every message is acknowledged (FluxMQ ack, AMQP basic.ack; Kafka commits offsets
automatically every 5 s, its normal mode).</li>
<li><b>Saturation:</b> closed loop, {warmup} s warm-up discarded, {duration} s measured, then up to 15 s for consumers to
receive everything sent in the window ("drained").{idcheck}</li>
<li><b>Fixed-rate latency:</b> open loop, sends scheduled evenly across producers; latency from the scheduled send
time ({lat_duration} s measured). Not sustained = publish or consume below 95 % of the target, or not drained.</li>
<li><b>Idle footprint:</b> broker containers after start-up settle, no clients; <b>under load:</b> the same
<code>docker stats</code> samples during the saturation window.</li>
<li><b>Fairness:</b> every broker container has the same CPU and memory limit; clients run in their own container with
their own limit on the same Docker network. Each system's stack is created fresh (empty volumes) and destroyed after
its runs, so systems never run at the same time.</li>
<li><b>Repeats:</b> every saturation configuration ran several times, interleaved (repeat 1 of every configuration,
then repeat 2, …) so slow drift spreads evenly; the report shows the median and the min–max range.{repeats}</li>
<li><b>Client resources:</b> the single-node systems were driven by a 4-CPU client container, the 3-node systems by a
2-CPU one (3 × 2 broker CPUs + 2 = the VM's 8); every system within one comparison had the same client.</li>
<li><b>Batched delivery (FluxMQ only):</b> <code>fluxmq-bench</code> of the v0.4 baseline image and of the current
image, each against its own single broker, plus the current bench's drain mode against both brokers; rounds
interleaved, a fresh broker per round.</li>
</ul>
<p>Reproduce: <code>bench/run.sh</code> (everything) or e.g.
<code>bench/run.sh --systems fluxmq,kafka --scenarios saturation</code>; raw JSON per run plus
<code>stats.jsonl</code> and <code>env.json</code> are in the results directory.</p>
"""

DURABILITY = """
<p>What "acknowledged" means differs per system; that difference is part of the result.</p>
<ul>
<li><b>FluxMQ:</b> <code>--fsync group-commit</code> (default): a publish is acked after its batch is fsynced
(commit window 2 ms). In the cluster: RF=3, <code>--min-isr 2</code>, acked once every ISR member has the batch
(<code>acks=all</code>); followers also group-commit.</li>
<li><b>Kafka:</b> producer <code>acks=all</code>, <code>enable.idempotence=true</code>. Brokers keep the default flush
policy: no fsync per message; data reaches disk via the page cache, durability comes from replication.
Cluster: RF=3, <code>min.insync.replicas=2</code>.</li>
<li><b>RabbitMQ 1 node:</b> durable classic queues, persistent messages, publisher confirms (sent once the message
is persisted, fsync batched). <b>3 nodes:</b> quorum queues (Raft, 3 members), confirm after a majority has the
entry fsynced to its write-ahead log.</li>
</ul>
"""

CAVEATS = """
<ul>
<li><b>macOS Docker VM.</b> All containers share one Linux VM (Docker Desktop, Apple Virtualization) on one laptop.
Disk I/O goes through the VM's virtual disk; fsync cost there is neither macOS <code>F_FULLFSYNC</code> nor a
server NVMe's. Networking is the VM's bridge, not a real network: no RTT, no bandwidth limit.</li>
<li><b>Single laptop, noisy neighbours.</b> Brokers, clients and the host OS (and anything else running on the
laptop, including builds) compete for the same cores, memory bandwidth and SSD. Thermal state matters. {repeat_note}</li>
<li><b>Not a production cluster.</b> "3 nodes" are 3 containers on one VM and one disk: replication costs CPU and
disk bandwidth that real separate machines would not share, and a node failure is not independent.</li>
<li><b>Client limits.</b> One client container with a CPU limit drives every system (4 CPUs for the single-node
systems, 2 for the 3-node systems so brokers + client fit the VM's 8 CPUs; the same within each comparison); check
"Client CPU" in the resource table before attributing a ceiling to a broker.</li>
<li><b>Defaults, not tuned.</b> Each broker runs close to its defaults (Kafka heap 1 GiB, RabbitMQ defaults, FluxMQ
defaults) and each client library with mostly default settings. Expert tuning would change every number.</li>
<li><b>Different durability semantics</b> (see the table above): faster acknowledgement can mean weaker guarantees.</li>
<li><b>Short runs.</b> 30 s windows do not show segment rolling, retention, compaction, GC pauses over hours, or
long-run steady state. TLS was measured for FluxMQ only, not for Kafka or RabbitMQ.</li>
</ul>
"""

CSS = r"""
:root{color-scheme:light;--bg:#f6f6f4;--surface:#fcfcfb;--border:#e3e2dd;--grid:#ebeae6;--text:#0b0b0b;
--text2:#52514e;--muted:#8a8984;--accent:#2a78d6;--warn-bg:#fff4e0;--warn-text:#6b4400;
--fam-fluxmq:#2a78d6;--fam-fluxmqos:#86b6ee;--fam-kafka:#eb6834;--fam-kafkafsync:#9a3d12;--fam-rabbitmq:#1baf7a}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--bg:#121211;--surface:#1a1a19;
--border:#2e2e2b;--grid:#2a2a27;--text:#ffffff;--text2:#c3c2b7;--muted:#8e8d86;--accent:#3987e5;--warn-bg:#3a2a0a;
--warn-text:#ffd58a;--fam-fluxmq:#3987e5;--fam-fluxmqos:#9cc4f2;--fam-kafka:#d95926;--fam-kafkafsync:#f2a07a;--fam-rabbitmq:#199e70}}
:root[data-theme="dark"]{color-scheme:dark;--bg:#121211;--surface:#1a1a19;--border:#2e2e2b;--grid:#2a2a27;
--text:#ffffff;--text2:#c3c2b7;--muted:#8e8d86;--accent:#3987e5;--warn-bg:#3a2a0a;--warn-text:#ffd58a;
--fam-fluxmq:#3987e5;--fam-fluxmqos:#9cc4f2;--fam-kafka:#d95926;--fam-kafkafsync:#f2a07a;--fam-rabbitmq:#199e70}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,sans-serif}
header{padding:28px 16px 12px;max-width:1040px;margin:0 auto}
header h1{margin:0 0 4px;font-size:26px;letter-spacing:-.01em}
header p{margin:0;color:var(--text2)}
nav{position:sticky;top:0;z-index:5;background:var(--bg);border-bottom:1px solid var(--border);overflow-x:auto;white-space:nowrap}
nav .in{max-width:1040px;margin:0 auto;padding:8px 16px;display:flex;gap:16px;align-items:center}
nav a{color:var(--text2);text-decoration:none;font-size:13px}
nav a:hover{color:var(--text)}
nav button{margin-left:auto;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:4px 10px;font-size:13px;cursor:pointer}
main{max-width:1040px;margin:0 auto;padding:8px 16px 64px}
section{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px 20px;margin:18px 0}
h2{margin:0 0 10px;font-size:19px}
h3{font-size:15px;margin:18px 0 6px}
p,li{color:var(--text2)}
b,code{color:var(--text)}
code{font:13px ui-monospace,SFMono-Regular,Menlo,monospace}
.fig{margin:18px 0 6px}
figcaption{font-weight:600;font-size:14px;margin-bottom:6px}
.chart{width:100%;height:auto;display:block;overflow:visible}
.chart text{font-size:12px;fill:var(--text2)}
.chart .label{fill:var(--text)}
.chart .value{fill:var(--text);font-variant-numeric:tabular-nums}
.chart .axis{fill:var(--muted);font-size:11px}
.chart .muted{fill:var(--muted);font-style:italic}
.chart .grid{stroke:var(--grid);stroke-width:1}
.bar{stroke:var(--surface);stroke-width:2}
.span{stroke-width:2;opacity:.55}
.dot{stroke:var(--surface);stroke-width:2}
.dot.hollow{fill:var(--surface)!important;stroke-width:2}
.fam-fluxmq{fill:var(--fam-fluxmq);stroke:var(--fam-fluxmq)}
.fam-fluxmqos{fill:var(--fam-fluxmqos);stroke:var(--fam-fluxmqos)}
.fam-kafka{fill:var(--fam-kafka);stroke:var(--fam-kafka)}
.fam-kafkafsync{fill:var(--fam-kafkafsync);stroke:var(--fam-kafkafsync)}
.fam-rabbitmq{fill:var(--fam-rabbitmq);stroke:var(--fam-rabbitmq)}
.bar.fam-fluxmq,.bar.fam-fluxmqos,.bar.fam-kafka,.bar.fam-kafkafsync,.bar.fam-rabbitmq{stroke:var(--surface)}
circle.dot.fam-fluxmq,rect.dot.fam-fluxmq,circle.dot.fam-fluxmqos,rect.dot.fam-fluxmqos,circle.dot.fam-kafka,rect.dot.fam-kafka,circle.dot.fam-kafkafsync,rect.dot.fam-kafkafsync,circle.dot.fam-rabbitmq,rect.dot.fam-rabbitmq{stroke:var(--surface)}
.legend-ink{fill:var(--text2);stroke:var(--text2)}
.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:12px;color:var(--text2);margin:2px 0 6px}
.legend span{display:inline-flex;gap:6px;align-items:center}
.hit{cursor:default;outline:none}
.hit:hover .bar,.hit:focus .bar{opacity:.85}
.syslegend{display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:var(--text2)}
.syslegend i{display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:6px;vertical-align:-1px}
.note{font-size:13px;color:var(--muted)}
.warn{background:var(--warn-bg);color:var(--warn-text);padding:10px 12px;border-radius:8px}
.warn b{color:inherit}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin:12px 0}
.card{border:1px solid var(--border);border-radius:8px;padding:12px 14px}
.card .k{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.card .v{font-size:24px;font-weight:650;margin:2px 0;font-variant-numeric:tabular-nums}
.card .s{font-size:13px;color:var(--text2)}
.tablewrap{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:13px;margin:8px 0}
th,td{text-align:left;padding:6px 10px;border-bottom:1px solid var(--border);vertical-align:top}
th{color:var(--muted);font-weight:600;white-space:nowrap}
td{color:var(--text);font-variant-numeric:tabular-nums}
table.kv td:first-child{color:var(--muted);white-space:nowrap;width:1%}
details{margin:4px 0 10px}
summary{cursor:pointer;color:var(--text2);font-size:13px}
#tip{position:fixed;pointer-events:none;background:var(--text);color:var(--surface);font-size:12px;padding:6px 9px;
border-radius:6px;max-width:320px;opacity:0;transition:opacity .08s;z-index:10}
ul.concl li{margin:4px 0}
.whisker{stroke:var(--text2);stroke-width:1.5;fill:none}
.muted-inline{color:var(--muted);font-size:12px;white-space:nowrap}
.headline td:first-child{white-space:nowrap}
@media (max-width:640px){section{padding:14px 14px}.card .v{font-size:20px}}
"""

JS = r"""
(function(){
  var root=document.documentElement;
  try{var t=localStorage.getItem('bench-theme');if(t)root.setAttribute('data-theme',t);}catch(e){}
  var b=document.getElementById('theme');
  function cur(){var t=root.getAttribute('data-theme');if(t)return t;
    return matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light';}
  function label(){b.textContent=cur()==='dark'?'Light theme':'Dark theme';}
  label();
  b.addEventListener('click',function(){var n=cur()==='dark'?'light':'dark';root.setAttribute('data-theme',n);
    try{localStorage.setItem('bench-theme',n);}catch(e){} label();});
  var tip=document.getElementById('tip');
  function show(el,x,y){tip.textContent=el.getAttribute('data-tip');tip.style.opacity=1;
    var w=tip.offsetWidth,h=tip.offsetHeight;var left=Math.min(x+14,innerWidth-w-8);var top=y-h-12;if(top<8)top=y+16;
    tip.style.left=left+'px';tip.style.top=top+'px';}
  document.querySelectorAll('[data-tip]').forEach(function(el){
    el.addEventListener('mousemove',function(e){show(el,e.clientX,e.clientY);});
    el.addEventListener('mouseleave',function(){tip.style.opacity=0;});
    el.addEventListener('focus',function(){var r=el.getBoundingClientRect();show(el,r.left+r.width/2,r.top);});
    el.addEventListener('blur',function(){tip.style.opacity=0;});
  });
})();
"""

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{css}</style>
</head>
<body>
<header><h1>FluxMQ benchmarks</h1><p>{subtitle}</p>
<div class="syslegend" style="margin-top:10px"><span><i style="background:var(--fam-fluxmq)"></i>FluxMQ</span>
<span><i style="background:var(--fam-kafka)"></i>Apache Kafka</span><span><i style="background:var(--fam-rabbitmq)"></i>RabbitMQ</span></div>
</header>
<nav><div class="in">{nav}<button id="theme" type="button">Theme</button></div></nav>
<main>{body}</main>
<div id="tip" role="tooltip"></div>
<script>{js}</script>
</body>
</html>
"""


def set_kind(env):
    if env.get("smoke"):
        return "smoke"
    if env.get("status") == "partial" or env.get("failures"):
        return "partial"
    return "full"


def fluxmq_summary(runs):
    """Per FluxMQ system: saturation medians per config and latency p50/p99 per rate."""
    sat = defaultdict(list)
    lat = {}
    for r in runs:
        if r.get("kind") != "harness" or r.get("excluded") or not str(r.get("system_name", "")).startswith("fluxmq"):
            continue
        pr = r["params"]
        if r.get("scenario") == "saturation":
            sat[(r["system_name"], pr["size"], pr["producers"], pr["consumers"])].append(r)
        elif r.get("scenario") == "latency" and r.get("e2e_ms"):
            lat[(r["system_name"], pr["rate"])] = r
    return {k: aggregate(v) for k, v in sat.items()}, lat


def batching_summary(runs):
    out = defaultdict(list)
    for r in runs:
        if r.get("kind") == "fluxmq-bench" and r.get("consume_rate"):
            out[(r["run"], r["label"])].append(r["consume_rate"])
    return {k: statistics.median(v) for k, v in out.items()}


def history_section(sets, current, link=True):
    """sets: [(name, env, runs)] oldest first. Lists every result set and compares FluxMQ."""
    rows = []
    sets_runs = {name: runs for name, _, runs in sets}
    for name, env, runs in sets:
        kind = set_kind(env)
        n = sum(1 for r in runs if r.get("kind") == "harness")
        systems = sorted({r.get("system_name") for r in runs if r.get("kind") == "harness"} - {None})
        label = f'<a href="{esc(name)}.html">{esc(name)}</a>' if link else esc(name)
        if name == current:
            label += " <b>(this report)</b>"
        note = {"smoke": "smoke run: a few seconds per scenario, proves the harness only; not a benchmark",
                "partial": env.get("integrity_note") or "incomplete (some systems failed)",
                "full": "complete comparison"}[kind]
        rev = next((i.get("revision") for ref, i in (env.get("images") or {}).items()
                    if ref.startswith("fluxmq:") and "baseline" not in ref), None)
        rows.append([label, esc(kind), esc(str(env.get("captured_at", ""))[:16]), str(n),
                     esc(", ".join(TITLES.get(x, x) for x in systems)), esc(str(rev or "?")), esc(note)])
    body = ("<p>Every result set in <code>bench/results/</code>, oldest first. Each has its own page; result sets are "
            "never rewritten (the first full measurement is frozen as published). Compare like with like: smoke runs "
            "are seconds long, and partial sets miss systems.</p>"
            + table(["Result set", "Kind", "Captured", "Runs", "Systems", "FluxMQ image", "Note"], rows))
    # FluxMQ before -> after (non-smoke sets with FluxMQ data)
    usable = [(n, e, fluxmq_summary(r), batching_summary(r)) for n, e, r in sets if set_kind(e) != "smoke"]
    usable = [u for u in usable if u[2][0] or u[2][1]]
    if len(usable) >= 2:
        last = usable[-1]
        # Before = the latest complete set before the newest one (e.g. v0.5 -> v0.6), else the oldest.
        prev_full = [u for u in usable[:-1] if set_kind(u[1]) == "full"]
        first = prev_full[-1] if prev_full else usable[0]
        keys = sorted(set(first[2][0]) | set(last[2][0]), key=lambda k: (ORDER.index(k[0]) if k[0] in ORDER else 99, -k[1], k[2]))
        trows = []
        for k in keys:
            a, b = first[2][0].get(k), last[2][0].get(k)
            if not a and not b:
                continue
            va, vb = (a or {}).get("consume_rate"), (b or {}).get("consume_rate")
            trows.append([esc(TITLES.get(k[0], k[0])), f"saturation {k[1]} B {k[2]}×{k[3]}",
                          (fmt_num(va) + (f" <span class=\"muted-inline\">[{fmt_range(a)}]</span>" if a and a.get("range") else "")) if a else "–",
                          (fmt_num(vb) + (f" <span class=\"muted-inline\">[{fmt_range(b)}]</span>" if b and b.get("range") else "")) if b else "–",
                          f"{(vb - va) / va * 100:+.1f} %" if va and vb else "–"])
        lkeys = sorted(set(first[2][1]) | set(last[2][1]), key=lambda k: (ORDER.index(k[0]) if k[0] in ORDER else 99, k[1]))
        for k in lkeys:
            a, b = first[2][1].get(k), last[2][1].get(k)
            fa = f"{fmt_ms(a['e2e_ms']['p50'])} / {fmt_ms(a['e2e_ms']['p99'])}" if a else "–"
            fb = f"{fmt_ms(b['e2e_ms']['p50'])} / {fmt_ms(b['e2e_ms']['p99'])}" if b else "–"
            d = f"{b['e2e_ms']['p99'] - a['e2e_ms']['p99']:+.1f} ms p99" if a and b else "–"
            trows.append([esc(TITLES.get(k[0], k[0])), f"latency {k[1]:,}/s p50 / p99", fa, fb, d])
        def ver(u):
            return v07.set_version(u[0], sets_runs.get(u[0], []))
        va_name, vb_name = ver(first), ver(last)
        pa, pb = first[1].get("plan", {}), last[1].get("plan", {})
        harness_diff = []
        for key, label in (("window", "window"), ("max_in_flight", "FluxMQ max_in_flight"),
                           ("segment_mb", "segment MiB"), ("retention_mb_per_partition", "retention MiB/partition")):
            if pa.get(key) != pb.get(key):
                harness_diff.append(f"{label} {pa.get(key, 'default')} → {pb.get(key, 'default')}")
        # Paired bars: before (light) and after (dark) per FluxMQ saturation configuration.
        prows = []
        for k in keys:
            a, b = first[2][0].get(k), last[2][0].get(k)
            if not (a and b) or k[0] in OS_VARIANTS:
                continue
            short = TITLES.get(k[0], k[0]).replace("FluxMQ · ", "")
            cfg = f"{'1 KiB' if k[1] == 1024 else str(k[1]) + ' B'} {k[2]}×{k[3]}"
            for u, x, fam in ((va_name, a, "fluxmqos"), (vb_name, b, "fluxmq")):
                prows.append((f"{short} · {cfg} · {u}", x.get("consume_rate"), fam,
                              f"{short}, {cfg}, {u}: {fmt_num(x.get('consume_rate'))} msg/s"
                              + (f" (range {fmt_range(x)})" if x.get("range") else ""), "", x.get("range")))
        chart = hbar(prows, lambda x: fmt_rate(x) + "/s",
                     f"FluxMQ consume throughput, {va_name} (light) vs {vb_name} (dark), median and range") if prows else ""
        body += (f"<h3>FluxMQ before → after: {esc(va_name)} ({esc(first[0])}) → {esc(vb_name)} ({esc(last[0])})</h3>"
                 "<p>Same machine, container limits and scenarios (single node: 4 client CPUs; 3 nodes: 2 up to v0.7, 4 from v0.8). Saturation = "
                 "median consume rate with [range]; differences within the ranges are noise. Latency rows are one run each, "
                 "so p99 differences of a few ms at 1k–10k msg/s are run-to-run noise unless they repeat."
                 + (" <b>The harness settings changed between the two sets</b> (" + esc("; ".join(harness_diff))
                    + "; FluxMQ producers no longer stamp the <code>flux-producer</code> header), so the throughput Δ mixes "
                    "broker/SDK work with the fairer client settings (the latest set's notes give a broker-only check). "
                    "Large drops at 100 B 4×4 are consumer starvation under producer overload: with the larger window "
                    "the producers are no longer throttled and publish far more than the consumers receive." if harness_diff else "")
                 + "</p>" + chart
                 + table(["System", "Measurement", esc(f"{va_name} ({first[0]})"), esc(f"{vb_name} ({last[0]})"), "Δ"], trows))
        body += v07.history_three([(n, e, r) for n, e, r in sets if set_kind(e) != "smoke"], fluxmq_summary)
        between = [u for u in usable if u[0] not in (first[0], last[0])]
        if between:
            mid = ", ".join(esc(u[0]) for u in between)
            body += f'<p class="note">Intermediate sets not shown in the table: {mid} (see their pages).</p>'
    return ("history", "History: result sets, before → after", body)


def load_set(path):
    env, runs, stats = load(path)
    return env, runs, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results", type=Path, nargs="?")
    ap.add_argument("-o", "--out", type=Path, default=None)
    ap.add_argument("--all", action="store_true",
                    help="render every result set in bench/results to bench/report/<set>.html and "
                         "index.html = the latest complete set + history")
    a = ap.parse_args()
    here = Path(__file__).resolve().parent
    if not a.all:
        if not a.results:
            ap.error("results dir required (or --all)")
        env, runs, stats = load(a.results)
        page = build(a.results.resolve(), env, runs, stats)
        out = a.out or here / "index.html"
        out.write_text(page)
        print(f"report: {out} ({len(page) // 1024} KiB, {len(runs)} runs, {len(stats)} stats samples)")
        return
    root = a.results or here.parent / "results"
    dirs = sorted([d for d in root.iterdir() if (d / "env.json").exists()],
                  key=lambda d: json.loads((d / "env.json").read_text()).get("captured_at", d.name))
    sets = [(d.name, *load_set(d)) for d in dirs]
    hist_sets = [(n, e, r) for n, e, r, _ in sets]
    for (name, env, runs, stats), d in zip(sets, dirs):
        page = build(d.resolve(), env, runs, stats, [history_section(hist_sets, name)])
        (here / f"{name}.html").write_text(page)
        print(f"report: {here / (name + '.html')} ({len(page) // 1024} KiB, {len(runs)} runs)")
    # Latest measured set: a complete one, or a partial one marked "index": true in env.json
    # (e.g. v0.6, stopped early but newer than the last complete set).
    full = [(n, e, r, st) for n, e, r, st in sets
            if set_kind(e) == "full" or (set_kind(e) == "partial" and e.get("index"))]
    latest = (full or sets)[-1]
    d = root / latest[0]
    page = build(d.resolve(), latest[1], latest[2], latest[3], [history_section(hist_sets, latest[0])])
    (here / "index.html").write_text(page)
    print(f"index: {here / 'index.html'} = {latest[0]} + history of {len(sets)} sets")


if __name__ == "__main__":
    main()

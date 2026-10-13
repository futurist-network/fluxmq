"""v0.7 report sections: key answers, reliability (crash matrix), failover, catch-up, overload,
equal-durability / default-vs-default comparisons, feature costs, scaling, and the three-way
FluxMQ history. Imported by generate.py; Python 3.9+ stdlib only.

Data sources inside a result set directory:
* runs/*.json                      harness runs (catchup / overload / saturation / latency)
* crash/summary.json, crash/runs/  bench/crash/crash.py --out <set>/crash
* failover/runs/*/run.json         crash.py --modes kill9 --targets leader --live-consumer
                                   --down 30 --out <set>/failover
* answers.ru.html, answers.en.html authored "key questions" answers (numbers from this set)
"""

from __future__ import annotations

import html
import json
import statistics
from collections import defaultdict
from pathlib import Path

esc = html.escape


def _fmt(v, d=0):
    if v is None:
        return "–"
    return f"{v:,.{d}f}"


def _rate(v):
    if v is None:
        return "–"
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}k"
    return f"{v:.0f}"


def _ms(v):
    if v is None:
        return "–"
    if v >= 1000:
        return f"{v / 1000:.1f} s"
    if v >= 100:
        return f"{v:,.0f} ms"
    return f"{v:.1f} ms"


def _table(headers, rows, cls=""):
    th = "".join(f"<th>{h}</th>" for h in headers)
    tr = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="tablewrap"><table class="{cls}"><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table></div>'


def _delta(a, b, higher_better=True):
    if not a or b is None:
        return "–"
    d = (b - a) / a * 100
    return f"{d:+.0f} %"


def _ratio(a, b):
    """'1.8× faster' style for b relative to a (consume rates)."""
    if not a or not b:
        return "–"
    r = b / a
    return f"{r:.2f}×"


# ----------------------------------------------------------------------------------------
# Key answers


def answers_section(results: Path):
    ru = results / "answers.ru.html"
    en = results / "answers.en.html"
    if not ru.exists() and not en.exists():
        return None
    body = ('<div class="langtabs" role="tablist">'
            '<button type="button" class="langtab on" data-lang="ru" aria-pressed="true">Русский</button>'
            '<button type="button" class="langtab" data-lang="en" aria-pressed="false">English</button></div>')
    if ru.exists():
        body += f'<div class="answers" lang="ru" data-lang="ru">{ru.read_text()}</div>'
    if en.exists():
        body += f'<div class="answers" lang="en" data-lang="en" hidden>{en.read_text()}</div>'
    return ("answers", "Ключевые вопросы — ответы / Key questions — answers", body)


ANSWERS_CSS = r"""
.langtabs{display:flex;gap:6px;margin:4px 0 12px}
.langtab{font:inherit;font-size:13px;padding:4px 12px;border-radius:999px;border:1px solid var(--border);
background:var(--surface);color:var(--text2);cursor:pointer}
.langtab.on{background:var(--accent);border-color:var(--accent);color:#fff}
.answers a,.limits a{color:var(--accent)}
.answers ol{padding-left:1.3em}.answers li{margin:0 0 10px}
.answers .verdict{font-weight:600}
.answers .better{color:#1a8a4a}.answers .worse{color:#c0392b}.answers .par{color:var(--text2)}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]) .answers .better{color:#4cc27f}
:root:not([data-theme="light"]) .answers .worse{color:#f07a6c}}
:root[data-theme="dark"] .answers .better{color:#4cc27f}:root[data-theme="dark"] .answers .worse{color:#f07a6c}
.limits{border-left:3px solid var(--border);padding:6px 12px;color:var(--text2);margin:12px 0}
"""

ANSWERS_JS = r"""
document.querySelectorAll('.langtab').forEach(function(b){b.addEventListener('click',function(){
var l=b.getAttribute('data-lang');
document.querySelectorAll('.langtab').forEach(function(x){var on=x===b;x.classList.toggle('on',on);x.setAttribute('aria-pressed',on?'true':'false');});
document.querySelectorAll('.answers').forEach(function(d){d.hidden=d.getAttribute('data-lang')!==l;});
try{localStorage.setItem('bench-lang',l);}catch(e){}
});});
try{var _l=localStorage.getItem('bench-lang');if(_l){var _b=document.querySelector('.langtab[data-lang="'+_l+'"]');if(_b)_b.click();}}catch(e){}
"""


# ----------------------------------------------------------------------------------------
# Reliability (crash matrix)


def _crash_runs(d: Path):
    out = []
    for p in sorted((d / "runs").glob("*/run.json")):
        try:
            out.append(json.loads(p.read_text()))
        except (OSError, json.JSONDecodeError):
            pass
    errors = []
    sp = d / "summary.json"
    if sp.exists():
        try:
            errors = [x for x in json.loads(sp.read_text()) if "error" in x]
        except json.JSONDecodeError:
            pass
    return out, errors


CRASH_ORDER = ["fluxmq-single", "fluxmq-single-os", "fluxmq-cluster", "fluxmq-cluster-os",
               "kafka-single", "kafka-single-fsync", "kafka-cluster", "kafka-cluster-fsync"]
CRASH_TITLE = {
    "fluxmq-single": "FluxMQ 1 node, group commit (default)",
    "fluxmq-single-os": "FluxMQ 1 node, fsync=os ‡",
    "fluxmq-cluster": "FluxMQ 3 nodes RF=3, group commit (default)",
    "fluxmq-cluster-os": "FluxMQ 3 nodes RF=3, fsync=os ‡",
    "kafka-single": "Kafka 1 node, default flush",
    "kafka-single-fsync": "Kafka 1 node, fsync per message",
    "kafka-cluster": "Kafka 3 nodes RF=3, default flush",
    "kafka-cluster-fsync": "Kafka 3 nodes RF=3, fsync per message",
}
TARGET_TITLE = {"node": "the node", "leader": "leader of p0", "all": "all 3 nodes"}


def crash_table(runs):
    groups = defaultdict(list)
    for r in runs:
        groups[(r["system"], r["mode"], r.get("target", "node"))].append(r)
    rows = []
    keys = sorted(groups, key=lambda k: (CRASH_ORDER.index(k[0]) if k[0] in CRASH_ORDER else 99,
                                         ["kill9", "powercut"].index(k[1]) if k[1] in ("kill9", "powercut") else 9,
                                         ["node", "leader", "all"].index(k[2]) if k[2] in ("node", "leader", "all") else 9))
    for k in keys:
        rs = groups[k]
        acked = sum(r.get("acked") or 0 for r in rs)
        lost = sum(r.get("lost") or 0 for r in rs)
        lost_runs = sum(1 for r in rs if r.get("lost"))
        valid = sum(1 for r in rs if r.get("method_valid"))
        before = sum(((r.get("verify") or {}).get("lost_acked_before_crash") or 0) for r in rs)
        rec = [r.get("recovery_s") for r in rs if r.get("recovery_s") is not None]
        verdict = ('<b class="ok">0 lost</b>' if lost == 0 else
                   f'<b class="bad">{lost:,} lost</b> in {lost_runs}/{len(rs)} runs')
        src = next((r.get("reused_from") for r in rs if r.get("reused_from")), None)
        rows.append([esc(CRASH_TITLE.get(k[0], k[0])) + (f' <span class="muted-inline">◦ reused from {esc(src)}</span>' if src else ""),
                     esc(k[1]), esc(TARGET_TITLE.get(k[2], k[2])),
                     str(len(rs)), _fmt(acked), verdict,
                     (_fmt(before) if lost else "–"),
                     f"{valid}/{len(rs)}" + ("" if valid == len(rs) else " <b>INVALID</b>"),
                     (f"{statistics.median(rec):.1f} s" if rec else "–")])
    return rows


def reliability_section(results: Path):
    d = results / "crash"
    if not (d / "runs").exists():
        return None
    runs, errors = _crash_runs(d)
    if not runs:
        return None
    env = {}
    if (d / "env.json").exists():
        env = json.loads((d / "env.json").read_text())
    p = runs[0].get("params", {})
    body = ("<p>Every acknowledged message id is recorded by the writer; after the crash a fresh consumer re-reads "
            "the whole topic and every acked id that is not there counts as <b>acked-but-lost</b> (checked twice, "
            "10 s apart). Writer: "
            f"{_fmt(p.get('rate'))} msg/s open loop, {p.get('producers')} producers, {p.get('size')} B, "
            f"{p.get('partitions')} partitions; crash after a random 8–12 s; victims down {p.get('down_s')} s; "
            f"publishing continues {p.get('after_s')} s after recovery. FluxMQ: <code>BatchingProducer</code>; Kafka: "
            "<code>acks=all</code>, idempotent producer.</p>")
    body += _table(["System", "Crash", "Victims", "Runs", "Acked (total)", "Acked-but-lost",
                    "of which acked before the crash", "Canary valid", "Recovery (median)"], crash_table(runs))
    if errors:
        body += ('<p class="warn">Runs that failed to complete (not counted above): '
                 + "; ".join(esc(f"{e.get('run')}: {str(e.get('error'))[:200]}") for e in errors) + "</p>")
    m = env.get("methods") or {}
    body += ("<details><summary>Method: what each crash simulates, and its limits</summary>"
             + "".join(f"<p><b>{esc(k)}</b>: {esc(v)}</p>" for k, v in m.items())
             + "<p><b>Canary.</b> Before each crash the tool writes a file that is never fsynced and checks after "
               "the restart: after <code>kill9</code> it must survive (page cache intact), after "
               "<code>powercut</code> it must be gone while a file synced before the workload must be there. A run "
               "whose canary disagrees is marked invalid.</p>"
               "<p><b>Limits.</b> The device below the cut is trusted (the Docker VM does not crash); kernel "
               "writeback that happened before the cut survives, which favours page-cache systems (Kafka defaults, "
               "FluxMQ fsync=os), never fsync-before-ack systems; torn sectors inside one write are not emulated "
               "(covered by the storage engine's torn-tail tests); one laptop, one SSD, three \"nodes\" on one VM; "
               "recovery time is until the readiness probe passes (Kafka's probe starts a JVM, adding seconds). "
               "Full description: <code>bench/crash/README.md</code>.</p></details>")
    body += (f'<p class="note">Raw evidence per run (acked ids, verify output, broker logs around the crash): '
             f'<code>{esc(results.name)}/crash/runs/</code>. Git {esc(str(env.get("git", ""))[:10])}, '
             f'Docker {esc(str(env.get("docker", "")))}.</p>')
    return ("reliability", "Reliability: acked-but-lost after kill -9 and power cut", body)


# ----------------------------------------------------------------------------------------
# Failover


def failover_section(results: Path):
    d = results / "failover"
    if not (d / "runs").exists():
        return None
    runs, errors = _crash_runs(d)
    runs = [r for r in runs if r.get("failover")]
    if not runs:
        return None
    groups = defaultdict(list)
    for r in runs:
        groups[r["system"]].append(r)
    rows = []

    def med(rs, f):
        v = [f(r) for r in rs if f(r) is not None]
        if not v:
            return None, None, None
        return statistics.median(v), min(v), max(v)

    def cell(t):
        m, lo, hi = t
        if m is None:
            return "–"
        return _ms(m) + (f' <span class="muted-inline">[{_ms(lo)}–{_ms(hi)}]</span>' if lo != hi else "")

    for s in sorted(groups, key=lambda k: CRASH_ORDER.index(k) if k in CRASH_ORDER else 99):
        rs = groups[s]
        f = lambda r, k: (r.get("failover") or {}).get(k)  # noqa: E731
        src = next((r.get("reused_from") for r in rs if r.get("reused_from")), None)
        rows.append([esc(CRASH_TITLE.get(s, s)) + (f' <span class="muted-inline">◦ reused from {esc(src)}</span>' if src else ""),
                     str(len(rs)),
                     cell(med(rs, lambda r: f(r, "max_partition_gap_ms"))),
                     cell(med(rs, lambda r: f(r, "pub_gap_ms"))),
                     cell(med(rs, lambda r: f(r, "max_ack_latency_ms"))),
                     cell(med(rs, lambda r: f(r, "con_gap_ms"))),
                     _fmt(sum(f(r, "errors") or 0 for r in rs)),
                     _fmt(sum(r.get("lost") or 0 for r in rs)),
                     cell(med(rs, lambda r: f(r, "max_partition_gap_ms_after_restart"))),
                     cell(med(rs, lambda r: (r.get("recovery_s") or 0) * 1000 if r.get("recovery_s") is not None else None))])
    p = runs[0].get("params", {})
    body = ("<p>3-node clusters under a steady open-loop load "
            f"({_fmt(p.get('rate'))} msg/s, {p.get('size')} B, {p.get('partitions')} partitions, RF=3, min ISR 2) "
            "with a live consumer group reading at the same time. The broker leading partition 0 is killed with "
            f"<code>kill -9</code> and stays down {p.get('down_s')} s, so the gap measures failover to the surviving "
            "replicas, not the restart. Gaps come from the writer's ack timestamps and the consumer's receive "
            "timestamps (10 ms buckets); all cells: median of the runs [min–max].</p>")
    body += _table(["System", "Runs", "Victim's partitions: no acks for", "All partitions: no acks for",
                    "Worst publish→ack", "Consumer: nothing received for", "Client errors", "Acked lost",
                    "Gap when the node returns", "Restart: time to ready"], rows)
    body += ('<p class="note">"Victim\'s partitions" = the longest stretch after the kill in which some partition '
             'got no ack (its leader died). "All partitions" = the longest stretch with no ack at all. "Worst '
             'publish→ack" counts from the scheduled send time, so messages queued in the client while a partition '
             'was leaderless are included. "Client errors" = publishes that failed or timed out (never counted as '
             'acked; FluxMQ and librdkafka retry internally first). RabbitMQ was not measured here: the crash tool '
             'has no AMQP client.</p>')
    if errors:
        body += ('<p class="warn">Runs that failed: ' + "; ".join(esc(f"{e.get('run')}: {str(e.get('error'))[:200]}")
                                                         for e in errors) + "</p>")
    return ("failover", "Fault tolerance: a broker dies under load (3 nodes)", body)


# ----------------------------------------------------------------------------------------
# Catch-up and overload (harness scenarios)


def catchup_overload_sections(runs, present, title_of):
    cat = [r for r in runs if r.get("kind") == "harness" and r.get("scenario") == "catchup" and not r.get("excluded")]
    ovl = [r for r in runs if r.get("kind") == "harness" and r.get("scenario") == "overload" and not r.get("excluded")]
    out = []
    if cat:
        rows = []
        for size in sorted({r["params"]["size"] for r in cat}, reverse=True):
            for s in present:
                for r in [x for x in cat if x["system_name"] == s and x["params"]["size"] == size]:
                    pre = r.get("preload") or {}
                    mb = (pre.get("bytes") or 0) / 2**20
                    rows.append([esc(title_of(s)) + _rmark(r), f"{size} B", f"{_fmt(pre.get('messages'))} ({mb:,.0f} MiB)",
                                 _fmt(pre.get("publish_rate")), f"<b>{_fmt(r.get('consume_rate'))}</b>",
                                 _fmt(pre.get("consume_rate_after_first")),
                                 f"{r.get('consume_mb_s', 0):,.0f}",
                                 (f"{pre.get('catchup_s'):.1f} s" if pre.get("catchup_s") is not None else "–"),
                                 "yes" if r.get("drained") else "<b>no</b>"])
        out.append(("catchup", "Catch-up: reading a large backlog",
                    "<p>The backlog is published first with no consumer reading; then 4 consumers start and read "
                    "until every preloaded message arrived (checked by id). Consume rate counts from the consumers' "
                    "start (including subscribe / group join); \"after first\" counts from the first receipt.</p>"
                    + _table(["System", "Size", "Backlog", "Preload publish msg/s", "Consume msg/s",
                              "Consume msg/s after first", "Consume MB/s", "Catch-up time", "All arrived"], rows)))
    if ovl:
        rows = []
        for size in sorted({r["params"]["size"] for r in ovl}, reverse=True):
            for s in present:
                for r in [x for x in ovl if x["system_name"] == s and x["params"]["size"] == size]:
                    pub, con = r.get("publish_rate"), r.get("consume_rate")
                    share = f"{con / pub * 100:.0f} %" if pub and con is not None else "–"
                    e2e = r.get("e2e_ms") or {}
                    rows.append([esc(title_of(s)) + _rmark(r), f"{size} B {r['params']['producers']}×{r['params']['consumers']}",
                                 _fmt(pub), f"<b>{_fmt(con)}</b>", share, _fmt(r.get("lag_growth_per_s")),
                                 _fmt(r.get("lag_end")), _ms(e2e.get("p99")), _fmt(r.get("errors"))])
        out.append(("overload", "Overload: producers faster than consumers",
                    "<p>Producers unthrottled (closed loop, window 20 000) for 60 s; consumers ack every message. "
                    "What matters: how much the consumers still get (consume msg/s, share of published) and how fast "
                    "the backlog grows. No drain check (a backlog is the point); size-retention drops are counted by "
                    "the brokers and reported separately, never as loss.</p>"
                    + _table(["System", "Load", "Published msg/s", "Consumed msg/s", "Consumed / published",
                              "Lag growth msg/s", "Lag at end", "E2E p99", "Errors"], rows)))
    return out


# ----------------------------------------------------------------------------------------
# Comparisons


def _rmark(r):
    """Marker for a row copied from an earlier result set (not re-measured)."""
    src = (r or {}).get("reused_from")
    return f' <span class="muted-inline">◦ {esc(src.split("-")[-1])} run</span>' if src else ""


def _sat_cell(a):
    if not a:
        return "–"
    v = a.get("consume_rate")
    rng = a.get("range")
    s = _fmt(v)
    if rng and a.get("n", 1) > 1:
        s += f' <span class="muted-inline">[{_rate(rng[0])}–{_rate(rng[1])}]</span>'
    return s + _rmark(a)


def comparison_sections(sat_by, lat_by, configs, rates, title_of):
    out = []

    def cmp_rows(pairs):
        rows = []
        for a_name, b_name in pairs:
            for (size, pr, co) in configs:
                a, b = sat_by.get((a_name, size, pr, co)), sat_by.get((b_name, size, pr, co))
                if not a or not b:
                    continue
                va, vb = a.get("consume_rate"), b.get("consume_rate")
                rows.append([esc(title_of(a_name)), esc(title_of(b_name)), f"saturation {size} B {pr}×{co}",
                             _sat_cell(a), _sat_cell(b), _ratio(vb, va)])
            for rate in rates:
                a, b = lat_by.get((a_name, rate)), lat_by.get((b_name, rate))
                if not a or not b or not a.get("e2e_ms") or not b.get("e2e_ms"):
                    continue
                ea, eb = a["e2e_ms"], b["e2e_ms"]
                rows.append([esc(title_of(a_name)), esc(title_of(b_name)), f"latency {rate:,}/s p50 / p99 / p99.9",
                             f"{_ms(ea.get('p50'))} / {_ms(ea.get('p99'))} / {_ms(ea.get('p999'))}"
                             + ("" if a.get("sustained") else " (not sustained)") + _rmark(a),
                             f"{_ms(eb.get('p50'))} / {_ms(eb.get('p99'))} / {_ms(eb.get('p999'))}"
                             + ("" if b.get("sustained") else " (not sustained)") + _rmark(b),
                             (f"p99 {eb['p99'] / ea['p99']:.1f}× of FluxMQ" if ea.get("p99") and eb.get("p99") else "–")])
        return rows

    eq = cmp_rows([("fluxmq-single", "kafka-single-fsync"), ("fluxmq-cluster", "kafka-cluster-fsync")])
    if eq:
        out.append(("equal-durability", "Equal durability: FluxMQ group commit vs Kafka with fsync per message",
                    "<p>Both acknowledge a publish only after it is on disk (fsync) — in a cluster on every in-sync "
                    "replica. Kafka: <code>log.flush.interval.messages=1</code> (fsync of the partition log on every "
                    "append before the answer). Ratio = FluxMQ consume rate ÷ Kafka's (above 1 = FluxMQ faster).</p>"
                    + _table(["FluxMQ", "Kafka", "Measurement", "FluxMQ", "Kafka", "FluxMQ ÷ Kafka"], eq)))
    dd = cmp_rows([("fluxmq-single", "kafka-single"), ("fluxmq-cluster", "kafka-cluster")])
    if dd:
        out.append(("default-vs-default", "Default vs default: FluxMQ group commit vs Kafka defaults",
                    "<p>Each system as shipped: FluxMQ fsyncs before every ack (group commit); Kafka acknowledges "
                    "from the page cache (<code>acks=all</code>, default flush), so a power cut can lose acknowledged "
                    "messages (see Reliability). The like-for-like durability rows are the fsync=os ‡ section and the "
                    "equal-durability table above.</p>"
                    + _table(["FluxMQ", "Kafka", "Measurement", "FluxMQ", "Kafka", "FluxMQ ÷ Kafka"], dd)))
    feat = []
    for base, var, what in (("fluxmq-single", "fluxmq-single-lz4", "lz4"), ("fluxmq-cluster", "fluxmq-cluster-lz4", "lz4"),
                            ("fluxmq-single", "fluxmq-single-cumulative", "cumulative ack"),
                            ("fluxmq-cluster", "fluxmq-cluster-cumulative", "cumulative ack")):
        for (size, pr, co) in configs:
            a, b = sat_by.get((base, size, pr, co)), sat_by.get((var, size, pr, co))
            if a and b:
                feat.append([esc(title_of(base).replace("FluxMQ · ", "")), esc(what), f"saturation {size} B {pr}×{co}",
                             _sat_cell(a), _sat_cell(b), _delta(a.get("consume_rate"), b.get("consume_rate"))])
        for rate in rates:
            a, b = lat_by.get((base, rate)), lat_by.get((var, rate))
            if a and b and a.get("e2e_ms") and b.get("e2e_ms"):
                feat.append([esc(title_of(base).replace("FluxMQ · ", "")), esc(what), f"latency {rate:,}/s p50 / p99",
                             f"{_ms(a['e2e_ms']['p50'])} / {_ms(a['e2e_ms']['p99'])}",
                             f"{_ms(b['e2e_ms']['p50'])} / {_ms(b['e2e_ms']['p99'])}",
                             f"{b['e2e_ms']['p99'] - a['e2e_ms']['p99']:+.1f} ms p99"])
    if feat:
        out.append(("features", "FluxMQ: cost or benefit of lz4 and cumulative acks",
                    "<p>Same broker and settings as the headline rows; only the client option changes: producer "
                    "batches compressed with lz4, or consumers in <code>AckMode::Cumulative</code> (commit a position "
                    "per partition instead of acking each message). The bench payload is the harness's (timestamp + "
                    "sequence + filler), which compresses very well; real payloads vary.</p>"
                    + _table(["Topology", "Option", "Measurement", "Default", "With option", "Δ"], feat)))
    sc = []
    for one, three in (("fluxmq-single", "fluxmq-cluster"), ("kafka-single", "kafka-cluster"),
                       ("kafka-single-fsync", "kafka-cluster-fsync"), ("rabbitmq-single", "rabbitmq-cluster")):
        for (size, pr, co) in configs:
            a, b = sat_by.get((one, size, pr, co)), sat_by.get((three, size, pr, co))
            if a and b:
                sc.append([esc(title_of(one).split(" · ")[0]), f"saturation {size} B {pr}×{co}", _sat_cell(a), _sat_cell(b),
                           _delta(a.get("consume_rate"), b.get("consume_rate"))])
        for rate in rates:
            a, b = lat_by.get((one, rate)), lat_by.get((three, rate))
            if a and b and a.get("e2e_ms") and b.get("e2e_ms"):
                sc.append([esc(title_of(one).split(" · ")[0]), f"latency {rate:,}/s p50 / p99",
                           f"{_ms(a['e2e_ms']['p50'])} / {_ms(a['e2e_ms']['p99'])}" + _rmark(a),
                           f"{_ms(b['e2e_ms']['p50'])} / {_ms(b['e2e_ms']['p99'])}" + _rmark(b),
                           f"{b['e2e_ms']['p99'] - a['e2e_ms']['p99']:+.1f} ms p99"])
    if sc:
        out.append(("scaling", "Scaling: 1 node → 3 nodes (RF=3)",
                    "<p>Three nodes here add replication (RF=3, min ISR 2, acks from every in-sync replica), not "
                    "capacity: all three \"nodes\" share the same VM, SSD and its flush rate. The 1-node runs had "
                    "4 client CPUs; the 3-node runs had 2 up to v0.7 and 4 from v0.8 (the VM has 8; rows reused "
                    "from an earlier set keep their own limit, see each table's \"Client CPUs\"). So this shows the "
                    "cost of replication on one machine, not horizontal scaling.</p>"
                    + _table(["System", "Measurement", "1 node", "3 nodes RF=3", "Δ"], sc)))
    return out


# ----------------------------------------------------------------------------------------
# History: v0.5 -> v0.6 -> v0.7


def set_version(name, runs):
    """'v0.8' for a result set: from the set name (…-v0.8) when it carries one, else from the
    broker version reported by the FluxMQ system run. (The v0.8 broker still reports 0.7.0:
    the crate version is bumped at release.)"""
    import re
    m = re.search(r"v(\d+\.\d+)", name)
    if m:
        return "v" + m.group(1)
    ver = next((r.get("broker_version") for r in runs if r.get("kind") == "system"
                and str(r.get("system_name", "")).startswith("fluxmq") and r.get("broker_version")), None)
    return (ver or name).replace("FluxMQ ", "v")


def history_three(sets, summarize):
    """sets: [(name, env, runs)] (non-smoke, oldest first); summarize(runs) -> (sat, lat) as in
    generate.fluxmq_summary. Shows the headline FluxMQ rows for the last three versions."""
    picked = []
    for name, env, runs in sets:
        sat, lat = summarize(runs)
        if not sat and not lat:
            continue
        picked.append((name, set_version(name, runs), sat, lat))
    # one set per version: the last set measured for it
    by_ver = {}
    for p in picked:
        by_ver[p[1]] = p
    seq = sorted(by_ver.values(), key=lambda p: [i for i, x in enumerate(picked) if x[0] == p[0]][0])[-4:]
    if len(seq) < 2:
        return ""
    keys = [("fluxmq-single", 1024, 1, 1), ("fluxmq-single", 1024, 4, 4), ("fluxmq-single", 100, 1, 1),
            ("fluxmq-single", 100, 4, 4), ("fluxmq-cluster", 1024, 1, 1), ("fluxmq-cluster", 1024, 4, 4),
            ("fluxmq-cluster", 100, 1, 1), ("fluxmq-cluster", 100, 4, 4)]
    rows = []
    for k in keys:
        vals = [p[2].get(k) for p in seq]
        if not any(vals):
            continue
        cells = [_sat_cell(v) for v in vals]
        first = next((v.get("consume_rate") for v in vals if v), None)
        last = vals[-1].get("consume_rate") if vals[-1] else None
        rows.append([("1 node" if k[0] == "fluxmq-single" else "3 nodes RF=3") + f", {'1 KiB' if k[1] == 1024 else '100 B'} {k[2]}×{k[3]}"]
                    + cells + [_delta(first, last)])
    for sysname in ("fluxmq-single", "fluxmq-cluster"):
        for rate in (1000, 10000, 50000):
            vals = [p[3].get((sysname, rate)) for p in seq]
            if not any(vals):
                continue
            cells = []
            for v in vals:
                e = (v or {}).get("e2e_ms") or {}
                cells.append(f"{_ms(e.get('p50'))} / {_ms(e.get('p99'))}" if e else "–")
            rows.append([("1 node" if sysname == "fluxmq-single" else "3 nodes RF=3") + f", p50 / p99 at {rate:,}/s"] + cells + [""])
    head = ["FluxMQ, group commit"] + [esc(f"{p[1]} ({p[0]})") for p in seq] + [esc(f"{seq[0][1]} → {seq[-1][1]}")]
    return ("<h3>FluxMQ " + " → ".join(esc(p[1]) for p in seq) + " (headline rows)</h3>"
            "<p>Consume msg/s, median [range]; latency one run each. v0.5 used window 2000 (not directly comparable); "
            "v0.6 and later use the same harness settings (window 20 000, 512 MiB/partition retention, 16 MiB "
            "segments). v0.6 is a partial set (no 3-node Kafka/RabbitMQ). From v0.8 the 3-node runs give the "
            "client 4 CPUs (v0.7: 2), because Kafka's 3-node runs were client-bound with 2.</p>" + _table(head, rows))


# ----------------------------------------------------------------------------------------
# Improvements (authored) and the "after quick fixes" subset


def improvements_section(results: Path):
    p = results / "improvements.html"
    if not p.exists():
        return None
    return ("improvements", "Improvements: where FluxMQ loses, why, and what to do", p.read_text())


def quickfix_section(results: Path, sat_by, lat_by, title_of, load, aggregate):
    d = results / "after-quick-fixes"
    if not (d / "runs").exists():
        return None
    _, runs, _ = load(d)
    groups = defaultdict(list)
    lat = {}
    for r in runs:
        if r.get("kind") != "harness" or r.get("excluded"):
            continue
        pr = r["params"]
        if r.get("scenario") == "saturation":
            groups[(r["system_name"], pr["size"], pr["producers"], pr["consumers"])].append(r)
        elif r.get("scenario") == "latency" and r.get("e2e_ms"):
            lat[(r["system_name"], pr["rate"])] = r
    rows = []
    for k in sorted(groups, key=lambda k: (k[0], -k[1], k[2])):
        a, b = sat_by.get(k), aggregate(groups[k])
        rows.append([esc(title_of(k[0])), f"saturation {k[1]} B {k[2]}×{k[3]}", _sat_cell(a), _sat_cell(b),
                     _delta((a or {}).get("consume_rate"), b.get("consume_rate"))])
    for k in sorted(lat):
        a, b = lat_by.get(k), lat[k]
        ea, eb = (a or {}).get("e2e_ms") or {}, b["e2e_ms"]
        rows.append([esc(title_of(k[0])), f"latency {k[1]:,}/s p50 / p99",
                     f"{_ms(ea.get('p50'))} / {_ms(ea.get('p99'))}" if ea else "–",
                     f"{_ms(eb.get('p50'))} / {_ms(eb.get('p99'))}", ""])
    if not rows:
        return None
    body = ("<p>Measured after the main matrix with one small, proven fix applied on top of the published build "
            "(same harness, limits and settings; 2 repeats; its own result directory "
            f"<code>{esc(results.name)}/after-quick-fixes</code>). The headline tables above stay on the build "
            "that was measured in the matrix. Fix: a raw in-flight run that became mostly acknowledged was "
            "compacted in the middle of an ack batch, decoding and CRC-checking every delivered envelope a second "
            "time (~10 % of broker CPU at 1 KiB, ~23 % at 100 B in the profile); the decision now waits for the "
            "end of the batch. No semantic or durability change.</p>"
            + _table(["System", "Measurement", "Matrix build", "After quick fix", "Δ"], rows))
    return ("quickfix", "After quick fixes (subset)", body)

#!/usr/bin/env python3
"""Crash-durability test: acked-but-lost messages after kill -9 / simulated power cut.

FluxMQ (group commit, fsync=os) vs Apache Kafka (default flush, flush per message), one node
and three nodes, in Docker. See bench/crash/README.md for the method and its limits.

    bench/crash/build.sh                       # images: fluxmq-crash, kafka-crash, client
    bench/crash/crash.py --systems fluxmq-single --modes kill9,powercut --iterations 3
    bench/crash/crash.py --systems all --iterations 5
    bench/crash/crash.py --systems fluxmq-cluster,kafka-cluster --targets leader,all

Per run: fresh topic; the writer publishes at --rate with acks and records every acked id;
crash after a random 8-12 s of publishing; restart after --down s; the writer keeps going
for --after s; then a fresh consumer re-reads everything and reports acked ids that are gone.
Python 3.9+ stdlib only. Project `fluxmq-crash` (never touches other compose projects).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
BASE_COMPOSE = ROOT / "deploy" / "docker-compose.yml"
CRASH_COMPOSE = ROOT / "bench" / "crash" / "docker-compose.crash.yml"
PROJECT = "fluxmq-crash"
GC_IMAGE = "fluxmq-crash:dev"


class Sys:
    def __init__(self, name, family, profile, nodes, brokers, env, write_args, title):
        self.name, self.family, self.profile = name, family, profile
        self.nodes, self.brokers, self.env = nodes, brokers, env
        self.write_args, self.title = write_args, title

    @property
    def cluster(self):
        return len(self.nodes) > 1


FLUX3 = ["n1", "n2", "n3"]
KAFKA3 = ["kafka1", "kafka2", "kafka3"]
SYSTEMS = {s.name: s for s in [
    Sys("fluxmq-single", "fluxmq", "single", ["fluxmq"], ["fluxmq:7420"],
        {"FLUXMQ_SINGLE_TLS": "off", "FLUXMQ_FSYNC": "group-commit"}, [],
        "FluxMQ 1 node, group commit (fsync before ack)"),
    Sys("fluxmq-single-os", "fluxmq", "single", ["fluxmq"], ["fluxmq:7420"],
        {"FLUXMQ_SINGLE_TLS": "off", "FLUXMQ_FSYNC": "os"}, [],
        "FluxMQ 1 node, fsync=os (ack after write(2))"),
    Sys("fluxmq-cluster", "fluxmq", "cluster", FLUX3, [f"{n}:7420" for n in FLUX3],
        {"FLUXMQ_CLUSTER_TLS": "off", "FLUXMQ_FSYNC": "group-commit"}, [],
        "FluxMQ 3 nodes RF=3 min ISR 2, group commit on every replica"),
    Sys("fluxmq-cluster-os", "fluxmq", "cluster", FLUX3, [f"{n}:7420" for n in FLUX3],
        {"FLUXMQ_CLUSTER_TLS": "off", "FLUXMQ_FSYNC": "os"}, [],
        "FluxMQ 3 nodes RF=3 min ISR 2, fsync=os"),
    Sys("kafka-single", "kafka", "kafka", ["kafka"], ["kafka:9092"], {},
        ["--replication", "1"], "Kafka 1 node, default flush (page cache)"),
    Sys("kafka-single-fsync", "kafka", "kafka", ["kafka"], ["kafka:9092"],
        {"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES": "1"}, ["--replication", "1"],
        "Kafka 1 node, log.flush.interval.messages=1 (fsync before ack)"),
    Sys("kafka-cluster", "kafka", "kafka-cluster", KAFKA3, [f"{n}:9092" for n in KAFKA3], {},
        ["--replication", "3", "--min-insync", "2"],
        "Kafka 3 nodes RF=3 min.insync 2 acks=all, default flush"),
    Sys("kafka-cluster-fsync", "kafka", "kafka-cluster", KAFKA3, [f"{n}:9092" for n in KAFKA3],
        {"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES": "1"}, ["--replication", "3", "--min-insync", "2"],
        "Kafka 3 nodes RF=3 min.insync 2 acks=all, log.flush.interval.messages=1"),
]}
ALIASES = {
    "all": list(SYSTEMS),
    "single": [s for s in SYSTEMS if "single" in s],
    "cluster": [s for s in SYSTEMS if "cluster" in s],
    "fluxmq": [s for s in SYSTEMS if s.startswith("fluxmq")],
    "kafka": [s for s in SYSTEMS if s.startswith("kafka")],
}
METHOD = {
    "kill9": (
        "SIGKILL of the broker's whole process group inside its container (crashctl kill9). "
        "The kernel and its page cache survive: everything the broker had written with write(2) "
        "is still there after the restart, fsynced or not. Simulates a broker process crash "
        "(OOM kill, panic, kill -9), not a machine crash."),
    "powercut": (
        "Simulated power loss of the broker's machine (crashctl powercut). The data directory is "
        "an ext4 filesystem on a device-mapper device over a loop device backed by a sparse file "
        "in a Docker volume. At the cut the dm device is suspended without flushing and switched "
        "to the dm `error` target, then the broker gets SIGKILL (before the device resumes, so "
        "it never sees the I/O errors). The filesystem is unmounted: dirty pages (written but "
        "not yet written back or fsynced) fail against the error target and the device's cache "
        "is dropped. Then the linear mapping is restored and ext4 is mounted again (journal "
        "replay), exactly the bytes that had reached the device at the cut. Only data that was "
        "fsynced (or written back by the kernel's periodic writeback before the cut) survives. "
        "A per-crash canary proves it: a file created just before the cut must be gone, a "
        "file synced before the workload must be there."),
}

# ---------------------------------------------------------------------------------------


def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}", flush=True)


def now_ms():
    return int(time.time() * 1000)


class Compose:
    def __init__(self, env):
        self.env = dict(os.environ)
        # Never inherit settings from the caller's shell; the systems set what they need.
        for k in list(self.env):
            if k.startswith(("FLUXMQ_", "KAFKA_", "CRASH_")):
                del self.env[k]
        self.env.update(env)

    def cmd(self, *args, profiles=("*",)):
        c = ["docker", "compose", "-p", PROJECT, "-f", str(BASE_COMPOSE), "-f", str(CRASH_COMPOSE)]
        for p in profiles:
            c += ["--profile", p]
        return c + list(args)

    def run(self, args, extra_env=None, profiles=("*",), check=True, capture=True, timeout=600,
            quiet=False):
        e = dict(self.env)
        e.update(extra_env or {})
        c = self.cmd(*args, profiles=profiles)
        if not quiet:
            log("$ " + " ".join(args))
        r = subprocess.run(c, env=e, text=True, timeout=timeout,
                           stdout=subprocess.PIPE if capture else None,
                           stderr=subprocess.STDOUT if capture else None)
        if check and r.returncode != 0:
            raise RuntimeError(f"failed ({r.returncode}): {' '.join(args)}\n{(r.stdout or '')[-3000:]}")
        return r

    def exec(self, svc, *args, extra_env=None, timeout=120, check=False, quiet=True):
        r = self.run(["exec", "-T", svc, *args], extra_env=extra_env, check=check,
                     timeout=timeout, quiet=quiet)
        return r.stdout.strip() if r.returncode == 0 else None

    def crashctl(self, svc, *args, extra_env=None, timeout=180):
        r = self.run(["exec", "-T", svc, "crashctl", *args], extra_env=extra_env, check=True,
                     timeout=timeout, quiet=True)
        return r.stdout.strip()


def gc():
    """Removes leaked crash devices (dm `fxcrash-*`, loops over /crashdisk/disk.img). They are
    global in the Docker VM kernel and outlive a container that died without `crashctl
    teardown`. Only runs while no `fluxmq-crash` container exists."""
    ps = subprocess.run(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"],
                        capture_output=True, text=True).stdout.strip()
    if ps:
        return
    # The crash broker image already has dmsetup/losetup: no package install at run time
    # (an `apk add` that hung on the network once killed a whole matrix here).
    script = ("for d in $(dmsetup ls 2>/dev/null | awk '/^fxcrash-/{print $1}'); do "
              "dmsetup remove --force $d && echo removed dm $d; done; "
              "for l in $(losetup -a | grep /crashdisk/disk.img | cut -d: -f1); do "
              "losetup -d $l && echo detached $l; done")
    try:
        r = subprocess.run(["docker", "run", "--rm", "--privileged", "--user", "0",
                            "--entrypoint", "sh", GC_IMAGE, "-c", script],
                           capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        log("gc: device cleanup timed out (continuing; leftover devices are removed next time)")
        return
    if r.stdout.strip():
        log("gc: " + r.stdout.strip().replace("\n", "; "))


def parallel(fn, items):
    """Runs fn(item) in threads; returns {item: result}; re-raises the first error."""
    res, errs = {}, []

    def one(i):
        try:
            res[i] = fn(i)
        except Exception as e:  # noqa: BLE001
            errs.append(e)
    ts = [threading.Thread(target=one, args=(i,)) for i in items]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    if errs:
        raise errs[0]
    return res


# ---------------------------------------------------------------------------------------


class Runner:
    def __init__(self, args, outdir):
        self.args, self.outdir = args, outdir
        self.compose = None
        self.summary = []

    def compose_for(self, system):
        env = {
            "BROKER_CPUS": str(self.args.broker_cpus), "BROKER_MEM": self.args.broker_mem,
            "BENCH_CPUS": str(self.args.bench_cpus), "BENCH_MEM": self.args.bench_mem,
            "FLUXMQ_IMAGE": self.args.fluxmq_base_image,
            "FLUXMQ_CRASH_IMAGE": self.args.fluxmq_image,
            "KAFKA_CRASH_IMAGE": self.args.kafka_image,
            "CRASH_CLIENT_IMAGE": self.args.client_image,
            "CRASH_RESULTS_DIR": str(self.outdir),
            "CRASH_DISK_SIZE": self.args.disk_size,
            "FLUXMQ_SEGMENT_BYTES": str(16 * 1024 * 1024),
            "FLUXMQ_FSYNC": "group-commit",
        }
        env.update(system.env)
        return Compose(env)

    def check_disk(self):
        free = shutil.disk_usage("/").free / 1e9
        if free < self.args.min_free_gb:
            raise SystemExit(f"host free disk {free:.1f} GB < --min-free-gb {self.args.min_free_gb}")

    # -- stack
    def up(self, system):
        self.down(system)
        self.compose = self.compose_for(system)
        t0 = time.time()
        self.compose.run(["up", "-d", "--wait", "--wait-timeout", "300"],
                         profiles=[system.profile], timeout=400)
        self.wait_ready(system)
        log(f"{system.name} up in {time.time() - t0:.0f}s")

    def down(self, system):
        c = self.compose or self.compose_for(system)
        # Release the loop/dm devices (kernel-global in the Docker VM) before removing.
        ps = c.run(["ps", "--format", "{{.Service}}"], check=False, quiet=True).stdout or ""
        for svc in ps.split():
            if svc in sum((s.nodes for s in SYSTEMS.values()), []):
                c.run(["exec", "-T", svc, "crashctl", "teardown"], check=False, quiet=True,
                      timeout=120)
        c.run(["down", "-v", "--remove-orphans", "--timeout", "20"], check=False, timeout=300)
        self.compose = None
        gc()

    def wait_ready(self, system, nodes=None, timeout=300):
        nodes = nodes or system.nodes
        deadline = time.time() + timeout
        c = self.compose
        while time.time() < deadline:
            if system.family == "fluxmq":
                ok = True
                for n in nodes:
                    if c.exec(n, "fluxmq-healthcheck", timeout=20) is None:
                        ok = False
                if ok and system.cluster:
                    raw = c.exec(nodes[0], "curl", "-fsk",
                                 "http://127.0.0.1:7421/api/cluster/metadata", timeout=20)
                    try:
                        ns = json.loads(raw or "{}").get("nodes", [])
                        ok = len(ns) == 3 and all(x.get("status") == "up" for x in ns)
                    except json.JSONDecodeError:
                        ok = False
                if ok:
                    return
            else:
                ok = all(c.exec(n, "/opt/kafka/bin/kafka-broker-api-versions.sh",
                                "--bootstrap-server", "localhost:9092", timeout=60) is not None
                         for n in nodes)
                if ok and system.cluster:
                    raw = c.exec(system.nodes[0], "/opt/kafka/bin/kafka-metadata-quorum.sh",
                                 "--bootstrap-server", "localhost:9092", "describe",
                                 "--replication", timeout=60)
                    ok = bool(raw) and len([l for l in raw.splitlines()
                                            if re.search(r"\bLeader\b|\bFollower\b", l)]) >= 3
                if ok:
                    return
            time.sleep(1)
        raise RuntimeError(f"{system.name}: {nodes} not ready after {timeout}s")

    def leader_of(self, system, topic, partition=0):
        c = self.compose
        if system.family == "fluxmq":
            raw = c.exec(system.nodes[0], "curl", "-fsk", "http://127.0.0.1:7421/api/cluster/metadata")
            try:
                parts = json.loads(raw or "{}").get("channels", {}).get(topic) or []
                lead = parts[partition].get("leader")
                if lead in system.nodes:
                    return lead
            except (json.JSONDecodeError, IndexError, AttributeError):
                pass
        else:
            raw = c.exec(system.nodes[0], "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                         "localhost:9092", "--describe", "--topic", topic, timeout=90) or ""
            m = re.search(rf"Partition:\s*{partition}\s+Leader:\s*(\d+)", raw)
            if m:
                return f"kafka{m.group(1)}"
        raise RuntimeError(f"leader of {topic}/{partition} not found")

    def delete_topic(self, system, topic):
        c = self.compose
        if system.family == "fluxmq":
            c.exec(system.nodes[0], "curl", "-fsk", "-X", "DELETE",
                   f"http://127.0.0.1:7421/api/channels/{topic}")
        else:
            c.exec(system.nodes[0], "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                   "localhost:9092", "--delete", "--topic", topic, timeout=90)

    def client(self, *args, timeout=900, background=False):
        cmd = self.compose.cmd("run", "--rm", "-T", "crashclient", *args, profiles=["crashclient"])
        if background:
            return subprocess.Popen(cmd, env=self.compose.env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return subprocess.run(cmd, env=self.compose.env, text=True, timeout=timeout,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # -- one crash run
    def run_one(self, system, mode, target, it):
        a = self.args
        self.check_disk()
        name = f"{system.name}-{mode}-{target}-{it}"
        rdir = self.outdir / "runs" / name
        rdir.mkdir(parents=True, exist_ok=True)
        os.chmod(rdir, 0o777)
        rel = f"/results/runs/{name}"
        topic = f"crash-{mode}-{it}-{now_ms() % 10_000_000}"
        doc = {"schema": 1, "run": name, "system": system.name, "title": system.title,
               "family": system.family, "mode": mode, "target": target, "iteration": it,
               "topic": topic, "method": METHOD[mode],
               "params": {"rate": a.rate, "size": a.size, "producers": a.producers,
                          "partitions": a.partitions, "down_s": a.down, "after_s": a.after}}
        log(f"=== {name}: {system.title}")
        writer_log = open(rdir / "writer.log", "w")
        writer = self.client(
            "write", "--system", system.family, "--brokers", ",".join(system.brokers),
            "--topic", topic, "--partitions", str(a.partitions), "--rate", str(a.rate),
            "--size", str(a.size), "--producers", str(a.producers),
            "--ready-file", f"{rel}/ready", "--go-file", f"{rel}/go", "--stop-file", f"{rel}/stop",
            "--out", f"{rel}/acked.jsonl", "--summary", f"{rel}/write.json",
            *(["--live-consume"] if a.live_consumer else []),
            *system.write_args, background=True)
        pump = threading.Thread(target=lambda: [writer_log.write(l) or writer_log.flush()
                                                for l in writer.stdout], daemon=True)
        pump.start()
        try:
            t = time.time()
            while not (rdir / "ready").exists():
                if writer.poll() is not None or time.time() - t > 240:
                    raise RuntimeError(f"writer not ready (exit {writer.poll()}), see writer.log")
                time.sleep(0.2)
            # Durable starting point (as on a long-running broker), then go.
            parallel(lambda n: self.compose.crashctl(n, "prepare"), system.nodes)
            (rdir / "go").write_text("go")
            go = time.time()
            crash_after = random.uniform(a.crash_min, a.crash_max)
            if system.cluster and target == "leader":
                victims = [self.leader_of(system, topic)]
            else:
                victims = list(system.nodes)
            doc["victims"] = victims
            time.sleep(max(0.0, crash_after - (time.time() - go)))
            # The crash.
            parallel(lambda n: self.compose.crashctl(n, "canary"), victims)
            crash_ms = now_ms()
            if mode == "kill9":
                parallel(lambda n: self.compose.crashctl(n, "kill9"), victims)
            else:
                if len(victims) > 1:
                    # Freeze every victim first so the cuts are as simultaneous as possible.
                    parallel(lambda n: self.compose.crashctl(n, "stop"), victims)
                parallel(lambda n: self.compose.crashctl(n, "powercut"), victims)
            crash_done = now_ms()
            canary = parallel(lambda n: json.loads(self.compose.crashctl(n, "check-canary")),
                              victims)
            doc.update({"crash_ms": crash_ms, "crash_took_ms": crash_done - crash_ms,
                        "crash_after_s": round(crash_after, 2), "canary": canary})
            want_unsynced = mode == "kill9"
            doc["method_valid"] = all(c["synced_survived"] and c["unsynced_survived"] == want_unsynced
                                      for c in canary.values())
            log(f"{name}: crashed {victims} at +{crash_after:.1f}s ({mode}, {crash_done - crash_ms} ms); "
                f"canary {canary}")
            time.sleep(a.down)
            parallel(lambda n: self.compose.crashctl(n, "start"), victims)
            t_restart = time.time()
            doc["restart_ms"] = now_ms()
            self.wait_ready(system)
            doc["recovery_s"] = round(time.time() - t_restart, 1)
            log(f"{name}: recovered in {doc['recovery_s']}s; publishing {a.after}s more")
            time.sleep(a.after)
            (rdir / "stop").write_text("stop")
            writer.wait(timeout=300)
        finally:
            if writer.poll() is None:
                writer.kill()
            pump.join(timeout=10)
            writer_log.close()
        if writer.returncode != 0 or not (rdir / "write.json").exists():
            raise RuntimeError(f"writer failed (exit {writer.returncode}), see {rdir}/writer.log")
        wj = json.loads((rdir / "write.json").read_text())
        doc["write"] = {k: v for k, v in wj.items() if k not in ("timeline", "consumer")}
        doc["failover"] = failover_metrics(rdir / "acked.jsonl", wj, doc, a)
        log(f"{name}: failover {json.dumps(doc['failover'])}")
        # Verify (twice when something is missing: real loss is persistent).
        passes = []
        for p in (1, 2):
            out = f"{rel}/verify{p}.json"
            r = self.client("verify", "--system", system.family, "--brokers", ",".join(system.brokers),
                            "--topic", topic, "--acked", f"{rel}/acked.jsonl",
                            "--crash-ms", str(crash_done), "--out", out, timeout=900)
            (rdir / f"verify{p}.log").write_text(r.stdout or "")
            if r.returncode != 0:
                raise RuntimeError(f"verify failed: {(r.stdout or '')[-2000:]}")
            v = json.loads((rdir / f"verify{p}.json").read_text())
            passes.append(v)
            if v["lost"] == 0 or p == 2:
                break
            log(f"{name}: verify pass 1 reports {v['lost']} lost; re-checking in 10 s")
            time.sleep(10)
        v = passes[-1]
        doc["verify"] = {k: v[k] for k in v if k not in ("lost_sample",)}
        doc["verify"]["lost_sample"] = v.get("lost_sample", [])[:20]
        doc["verify_passes"] = len(passes)
        doc["acked"] = v["acked_unique"]
        doc["lost"] = v["lost"]
        # Broker logs around the crash (evidence), then drop the topic (bounded disk).
        for n in system.nodes:
            r = self.compose.run(["logs", "--no-color", "--since",
                                  dt.datetime.fromtimestamp(crash_ms / 1000 - 5).isoformat(), n],
                                 check=False, quiet=True, timeout=120)
            (rdir / f"{n}.log").write_text((r.stdout or "")[-2_000_000:])
        self.delete_topic(system, topic)
        (rdir / "run.json").write_text(json.dumps(doc, indent=2))
        verdict = "OK" if doc["lost"] == 0 else "ACKED-BUT-LOST"
        if not doc["method_valid"]:
            verdict += " (method check FAILED: canary)"
        log(f"{name}: acked {doc['acked']:,}, lost {doc['lost']:,} -> {verdict}")
        self.summary.append({k: doc.get(k) for k in
                             ("run", "system", "mode", "target", "iteration", "victims", "acked",
                              "lost", "method_valid", "recovery_s", "crash_after_s")}
                            | {"lost_acked_before_crash": v.get("lost_acked_before_crash"),
                               "failed_publishes": doc["write"].get("failed"),
                               "failover": doc.get("failover"),
                               "unacked_at_end": doc["write"].get("unacked_at_end")})
        (self.outdir / "summary.json").write_text(json.dumps(self.summary, indent=2))
        return doc


def _longest_gap(times, lo, hi):
    """Longest interval (ms) inside [lo, hi] without an event; times sorted."""
    prev, best, at = lo, 0, None
    for t in times:
        if t < lo:
            continue
        if t > hi:
            break
        if t - prev > best:
            best, at = t - prev, prev
        prev = t
    if hi - prev > best:
        best, at = hi - prev, prev
    return best, at


def failover_metrics(acked_path, wj, doc, a):
    """Availability around the crash, from the writer's acks (and live consumer, if any).

    * pub_gap_ms: longest time after the crash with no ack at all (every partition stalled).
    * partition_gap_ms: per partition, the longest time after the crash with no ack for it;
      the max over partitions is the unavailability of the worst partition (the victim's).
    * max_ack_latency_ms: worst publish -> ack time (from the scheduled open-loop send time, so
      a stalled client queue counts), and how many acks took > 1 s.
    * con_gap_ms: longest time after the crash in which the live consumer received nothing.
    The *_after_restart variants measure the same from the victims' restart to the end
    (leader coming back / rebalancing)."""
    try:
        recs = [json.loads(l) for l in open(acked_path) if l.strip()]
    except OSError:
        return None
    if not recs:
        return {"acked": 0}
    crash, restart = doc.get("crash_ms"), doc.get("restart_ms")
    go, stop = wj.get("go_ms"), wj.get("stop_ms")
    producers = max(1, int(wj.get("producers") or 1))
    per_rate = max(1.0, float(wj.get("rate") or 1) / producers)
    acks = sorted(r[4] for r in recs)
    by_part = {}
    for r in recs:
        by_part.setdefault(r[2], []).append(r[4])
    end = min(stop or acks[-1], acks[-1])
    out = {"crash_ms": crash, "restart_ms": restart}
    if crash:
        hi = restart if (restart and restart < end) else end
        out["pub_gap_ms"], _ = _longest_gap(acks, crash, end)
        parts = {}
        for p, ts in by_part.items():
            ts.sort()
            parts[str(p)] = _longest_gap(ts, crash, end)[0]
        out["partition_gap_ms"] = parts
        out["max_partition_gap_ms"] = max(parts.values()) if parts else None
        out["pub_gap_ms_while_down"] = _longest_gap(acks, crash, hi)[0]
    if restart and restart < end:
        out["pub_gap_ms_after_restart"] = _longest_gap(acks, restart, end)[0]
        out["max_partition_gap_ms_after_restart"] = max(
            _longest_gap(sorted(ts), restart, end)[0] for ts in by_part.values())
    lat = []
    for pr, seq, _part, _off, ack in recs:
        due = go + 1000.0 * (seq + pr / producers) / per_rate
        lat.append(ack - due)
    lat.sort()
    out["max_ack_latency_ms"] = round(lat[-1])
    out["p99_ack_latency_ms"] = round(lat[int(0.99 * (len(lat) - 1))])
    out["acks_over_1s"] = sum(1 for x in lat if x > 1000)
    out["errors"] = (wj.get("failed") or 0) + (wj.get("enqueue_errors") or 0)
    out["first_error_ms"] = wj.get("first_error_ms")
    out["last_error"] = (wj.get("last_error") or "")[:300] or None
    c = wj.get("consumer")
    if c and crash:
        b = c.get("buckets_10ms") or []
        times = [go + 10 * i + 5 for i, n in enumerate(b) if n]
        out["consumer_received"] = c.get("received")
        out["consumer_duplicates"] = c.get("duplicates")
        out["consumer_errors"] = c.get("errors")
        out["con_gap_ms"] = _longest_gap(times, crash, end)[0]
        if restart and restart < end:
            out["con_gap_ms_after_restart"] = _longest_gap(times, restart, end)[0]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--systems", default="all",
                    help="comma list: " + ", ".join(list(SYSTEMS) + list(ALIASES)))
    ap.add_argument("--modes", default="kill9,powercut")
    ap.add_argument("--targets", default="leader,all",
                    help="clusters: leader (the node leading partition 0) and/or all (every node "
                         "at once); single nodes always crash their one node")
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--rate", type=int, default=5000, help="msg/s total")
    ap.add_argument("--size", type=int, default=1024)
    ap.add_argument("--producers", type=int, default=2)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--crash-min", type=float, default=8.0, help="earliest crash, s after go")
    ap.add_argument("--crash-max", type=float, default=12.0)
    ap.add_argument("--down", type=float, default=3.0, help="seconds the victims stay down")
    ap.add_argument("--after", type=float, default=5.0, help="publishing after recovery, s")
    ap.add_argument("--live-consumer", action="store_true",
                    help="run a live consumer during the run (failover scenario: consumer gap)")
    ap.add_argument("--broker-cpus", default="2")
    ap.add_argument("--broker-mem", default="2g")
    ap.add_argument("--bench-cpus", default="2")
    ap.add_argument("--bench-mem", default="2g")
    ap.add_argument("--disk-size", default="3G", help="sparse backing file per broker")
    ap.add_argument("--fluxmq-base-image", default="fluxmq:dev", help="certs init container")
    ap.add_argument("--fluxmq-image", default="fluxmq-crash:dev")
    ap.add_argument("--kafka-image", default="kafka-crash:dev")
    ap.add_argument("--client-image", default="fluxmq-crash-client:dev")
    ap.add_argument("--min-free-gb", type=float, default=10.0)
    ap.add_argument("--stop-on-loss", action=argparse.BooleanOptionalAction, default=True,
                    help="stop when FluxMQ group commit loses an acked message (keeps the stack "
                         "for inspection)")
    ap.add_argument("--keep", action="store_true", help="leave the last stack running")
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    names = []
    for s in a.systems.split(","):
        for n in ALIASES.get(s.strip(), [s.strip()]):
            if n not in SYSTEMS:
                ap.error(f"unknown system {n}")
            if n not in names:
                names.append(n)
    modes = [m.strip() for m in a.modes.split(",") if m.strip()]
    for m in modes:
        if m not in METHOD:
            ap.error(f"unknown mode {m}")
    targets = [t.strip() for t in a.targets.split(",") if t.strip()]
    outdir = Path(a.out) if a.out else ROOT / "bench" / "results" / (
        "crash-" + dt.datetime.now().strftime("%Y%m%d-%H%M%S"))
    outdir = outdir.resolve()
    (outdir / "runs").mkdir(parents=True, exist_ok=True)
    os.chmod(outdir, 0o777)
    os.chmod(outdir / "runs", 0o777)
    for img in (a.fluxmq_image, a.kafka_image, a.client_image, a.fluxmq_base_image):
        if subprocess.run(["docker", "image", "inspect", img], capture_output=True).returncode:
            sys.exit(f"image {img} missing: run bench/crash/build.sh")
    env = {"started": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
           "argv": sys.argv[1:], "methods": METHOD,
           "docker": subprocess.run(["docker", "info", "--format",
                                     "{{.KernelVersion}} | {{.OperatingSystem}} | {{.NCPU}} CPUs | "
                                     "{{.MemTotal}} B"], capture_output=True, text=True).stdout.strip(),
           "images": {img: subprocess.run(["docker", "image", "inspect", img, "--format",
                                           "{{.Id}} {{index .Config.Labels \"org.opencontainers.image.revision\"}}"],
                                          capture_output=True, text=True).stdout.strip()
                      for img in (a.fluxmq_image, a.kafka_image, a.client_image)},
           "git": subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True,
                                 text=True).stdout.strip()}
    (outdir / "env.json").write_text(json.dumps(env, indent=2))
    log(f"results -> {outdir}")
    r = Runner(a, outdir)
    stopped = None
    try:
        for n in names:
            system = SYSTEMS[n]
            r.up(system)
            try:
                for mode in modes:
                    for target in (targets if system.cluster else ["node"]):
                        for it in range(1, a.iterations + 1):
                            try:
                                doc = r.run_one(system, mode, target, it)
                            except Exception as e:  # noqa: BLE001
                                log(f"!! {n} {mode} {target} {it}: {e}")
                                r.summary.append({"run": f"{n}-{mode}-{target}-{it}", "system": n,
                                                  "mode": mode, "target": target, "iteration": it,
                                                  "error": str(e)[:2000]})
                                (outdir / "summary.json").write_text(json.dumps(r.summary, indent=2))
                                # A broken stack: start over for the next run.
                                r.up(system)
                                continue
                            if (doc["lost"] and system.family == "fluxmq"
                                    and system.env.get("FLUXMQ_FSYNC") == "group-commit"
                                    and a.stop_on_loss):
                                stopped = doc["run"]
                                raise KeyboardInterrupt
            finally:
                if not (stopped or (a.keep and n == names[-1])):
                    r.down(system)
    except KeyboardInterrupt:
        if stopped:
            log(f"!! STOPPED: FluxMQ group commit lost acknowledged messages in {stopped}; "
                f"stack `{PROJECT}` left running for inspection, evidence in {outdir}/runs/{stopped}")
        else:
            log("interrupted")
            if r.compose:
                r.down(SYSTEMS[names[0]])
    print_table(r.summary)
    return 2 if stopped else 0


def print_table(rows):
    print()
    print(f"{'run':44} {'acked':>9} {'lost':>8} {'lost<=crash':>11} {'canary':>7} {'recov s':>7}")
    for x in rows:
        if "error" in x:
            print(f"{x['run']:44} ERROR {x['error'][:60]}")
            continue
        print(f"{x['run']:44} {x['acked'] or 0:>9,} {x['lost'] or 0:>8,} "
              f"{str(x.get('lost_acked_before_crash')):>11} "
              f"{'ok' if x.get('method_valid') else 'FAIL':>7} {x.get('recovery_s') or 0:>7}")


if __name__ == "__main__":
    sys.exit(main())

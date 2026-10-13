#!/usr/bin/env python3
"""Comparative benchmark runner: FluxMQ vs Apache Kafka vs RabbitMQ in Docker.

Use bench/run.sh (a thin wrapper). Python 3.9+ stdlib only.

For every selected system the runner brings up its compose stack (deploy/docker-compose.yml,
project `fluxmq-bench`), waits until it is ready, samples `docker stats` continuously, runs the
selected scenarios with the common harness (bench/harness, image fluxmq-harness) and tears the
stack down with `down -v`. Raw JSON goes to bench/results/<timestamp>/, then
bench/report/generate.py renders bench/report/index.html.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "deploy" / "docker-compose.yml"
PROJECT = "fluxmq-bench"

# ----------------------------------------------------------------------------------------
# Systems


class System:
    def __init__(self, name, family, profile, env, brokers, harness_args, nodes, title,
                 http=None, tls=False):
        self.name = name
        self.family = family          # fluxmq | kafka | rabbitmq
        self.profile = profile
        self.env = env                # compose env overrides
        self.brokers = brokers        # harness --brokers
        self.harness_args = harness_args
        self.nodes = nodes            # broker service names
        self.title = title
        self.http = http              # (service, scheme) for the FluxMQ REST API
        self.tls = tls


FLUX_SEEDS = ["n1:7420", "n2:7420", "n3:7420"]
SYSTEMS = {
    "fluxmq-single": System(
        "fluxmq-single", "fluxmq", "single", {"FLUXMQ_SINGLE_TLS": "off"},
        ["fluxmq:7420"], [], ["fluxmq"], "FluxMQ 1 node", http=("fluxmq", "http")),
    "fluxmq-single-tls": System(
        "fluxmq-single-tls", "fluxmq", "single", {"FLUXMQ_SINGLE_TLS": "on"},
        ["tls://fluxmq:7420"], ["--tls-ca", "/certs/ca.pem"], ["fluxmq"],
        "FluxMQ 1 node, TLS", http=("fluxmq", "https"), tls=True),
    "fluxmq-cluster": System(
        "fluxmq-cluster", "fluxmq", "cluster", {"FLUXMQ_CLUSTER_TLS": "off"},
        FLUX_SEEDS, [], ["n1", "n2", "n3"], "FluxMQ 3 nodes RF=3",
        http=("n1", "http")),
    "fluxmq-cluster-tls": System(
        "fluxmq-cluster-tls", "fluxmq", "cluster", {"FLUXMQ_CLUSTER_TLS": "on"},
        ["tls://" + s for s in FLUX_SEEDS], ["--tls-ca", "/certs/ca.pem"], ["n1", "n2", "n3"],
        "FluxMQ 3 nodes RF=3, full TLS", http=("n1", "https"), tls=True),
    # v0.10 A/B: the same TLS rows with kernel TLS (FLUXMQ_TLS_IMPL=ktls, opt-in), broker and
    # harness alike; the plain -tls rows use the default userspace record layer.
    "fluxmq-single-tls-ktls": System(
        "fluxmq-single-tls-ktls", "fluxmq", "single",
        {"FLUXMQ_SINGLE_TLS": "on", "FLUXMQ_TLS_IMPL": "ktls"},
        ["tls://fluxmq:7420"], ["--tls-ca", "/certs/ca.pem"], ["fluxmq"],
        "FluxMQ 1 node, TLS (kernel TLS)", http=("fluxmq", "https"), tls=True),
    "fluxmq-cluster-tls-ktls": System(
        "fluxmq-cluster-tls-ktls", "fluxmq", "cluster",
        {"FLUXMQ_CLUSTER_TLS": "on", "FLUXMQ_TLS_IMPL": "ktls"},
        ["tls://" + s for s in FLUX_SEEDS], ["--tls-ca", "/certs/ca.pem"], ["n1", "n2", "n3"],
        "FluxMQ 3 nodes RF=3, full TLS (kernel TLS)", http=("n1", "https"), tls=True),
    # Durability parity with Kafka defaults: ack after write(2), the OS flushes (no fsync), like
    # Kafka's default flush policy. Reported next to, never instead of, group commit (headline).
    "fluxmq-single-os": System(
        "fluxmq-single-os", "fluxmq", "single", {"FLUXMQ_SINGLE_TLS": "off", "FLUXMQ_FSYNC": "os"},
        ["fluxmq:7420"], ["--fluxmq-fsync", "os"], ["fluxmq"],
        "FluxMQ 1 node, fsync=os (durability parity with Kafka defaults)", http=("fluxmq", "http")),
    "fluxmq-cluster-os": System(
        "fluxmq-cluster-os", "fluxmq", "cluster", {"FLUXMQ_CLUSTER_TLS": "off", "FLUXMQ_FSYNC": "os"},
        FLUX_SEEDS, ["--fluxmq-fsync", "os"], ["n1", "n2", "n3"],
        "FluxMQ 3 nodes RF=3, fsync=os (durability parity with Kafka defaults)",
        http=("n1", "http")),
    # A/B only (not in any alias): pipelined group commit (FLUXMQ_COMMIT_PIPELINE=on; the broker
    # default since v0.9, so these now equal the headline systems; FLUXMQ_COMMIT_PIPELINE=off is
    # the v0.8 behaviour).
    "fluxmq-single-pipeline": System(
        "fluxmq-single-pipeline", "fluxmq", "single",
        {"FLUXMQ_SINGLE_TLS": "off", "FLUXMQ_COMMIT_PIPELINE": "on"},
        ["fluxmq:7420"], [], ["fluxmq"], "FluxMQ 1 node, commit pipeline on",
        http=("fluxmq", "http")),
    "fluxmq-cluster-pipeline": System(
        "fluxmq-cluster-pipeline", "fluxmq", "cluster",
        {"FLUXMQ_CLUSTER_TLS": "off", "FLUXMQ_COMMIT_PIPELINE": "on"},
        FLUX_SEEDS, [], ["n1", "n2", "n3"], "FluxMQ 3 nodes RF=3, commit pipeline on",
        http=("n1", "http")),
    "fluxmq-cluster-nopipe": System(
        "fluxmq-cluster-nopipe", "fluxmq", "cluster",
        {"FLUXMQ_CLUSTER_TLS": "off", "FLUXMQ_COMMIT_PIPELINE": "off"},
        FLUX_SEEDS, [], ["n1", "n2", "n3"], "FluxMQ 3 nodes RF=3, commit pipeline off (v0.8 commit path)",
        http=("n1", "http")),
    # v0.7 fast-path variants (extra rows; the headline fluxmq-* rows use the SDK defaults:
    # protocol v4 batches, individual acks, no compression).
    "fluxmq-single-lz4": System(
        "fluxmq-single-lz4", "fluxmq", "single", {"FLUXMQ_SINGLE_TLS": "off"},
        ["fluxmq:7420"], ["--fluxmq-compression", "lz4"], ["fluxmq"],
        "FluxMQ 1 node, lz4 batches", http=("fluxmq", "http")),
    "fluxmq-single-cumulative": System(
        "fluxmq-single-cumulative", "fluxmq", "single", {"FLUXMQ_SINGLE_TLS": "off"},
        ["fluxmq:7420"], ["--fluxmq-ack-mode", "cumulative"], ["fluxmq"],
        "FluxMQ 1 node, cumulative ack", http=("fluxmq", "http")),
    "fluxmq-cluster-lz4": System(
        "fluxmq-cluster-lz4", "fluxmq", "cluster", {"FLUXMQ_CLUSTER_TLS": "off"},
        FLUX_SEEDS, ["--fluxmq-compression", "lz4"], ["n1", "n2", "n3"],
        "FluxMQ 3 nodes RF=3, lz4 batches", http=("n1", "http")),
    "fluxmq-cluster-cumulative": System(
        "fluxmq-cluster-cumulative", "fluxmq", "cluster", {"FLUXMQ_CLUSTER_TLS": "off"},
        FLUX_SEEDS, ["--fluxmq-ack-mode", "cumulative"], ["n1", "n2", "n3"],
        "FluxMQ 3 nodes RF=3, cumulative ack", http=("n1", "http")),
    "kafka-single": System(
        "kafka-single", "kafka", "kafka", {}, ["kafka:9092"], ["--replication", "1"],
        ["kafka"], "Kafka 1 node"),
    "kafka-cluster": System(
        "kafka-cluster", "kafka", "kafka-cluster", {},
        ["kafka1:9092", "kafka2:9092", "kafka3:9092"],
        ["--replication", "3", "--min-insync", "2"], ["kafka1", "kafka2", "kafka3"],
        "Kafka 3 nodes RF=3"),
    # "fsync before ack": broker log.flush.interval.messages=1 (compose
    # KAFKA_LOG_FLUSH_INTERVAL_MESSAGES; topics inherit it as flush.messages=1). Kafka then
    # fsyncs the partition log on every append (each produce batch) before answering; with
    # acks=all every follower also fsyncs on its fetch append before the high watermark
    # moves. Durability comparable to FluxMQ group commit.
    "kafka-single-fsync": System(
        "kafka-single-fsync", "kafka", "kafka", {"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES": "1"},
        ["kafka:9092"], ["--replication", "1"], ["kafka"],
        "Kafka 1 node, fsync before ack (log.flush.interval.messages=1)"),
    "kafka-cluster-fsync": System(
        "kafka-cluster-fsync", "kafka", "kafka-cluster", {"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES": "1"},
        ["kafka1:9092", "kafka2:9092", "kafka3:9092"],
        ["--replication", "3", "--min-insync", "2"], ["kafka1", "kafka2", "kafka3"],
        "Kafka 3 nodes RF=3, fsync before ack (log.flush.interval.messages=1)"),
    # TLS on the client listener (gen-dev server certificate from the stack's CA, the one the
    # FluxMQ TLS rows use; librdkafka verifies chain and hostname); the 3-node row also
    # replicates over TLS (inter-broker listener), like FluxMQ's full-TLS cluster. Default
    # flush policy, compared with the fluxmq-*-tls rows for the cost of TLS within each
    # system.
    "kafka-single-tls": System(
        "kafka-single-tls", "kafka", "kafka", {"KAFKA_TLS": "on", "KAFKA_INTER_BROKER_LISTENER": "TLS"},
        ["kafka:9095"], ["--replication", "1", "--kafka-tls-ca", "/certs/ca.pem"], ["kafka"],
        "Kafka 1 node, TLS", tls=True),
    "kafka-cluster-tls": System(
        "kafka-cluster-tls", "kafka", "kafka-cluster",
        {"KAFKA_TLS": "on", "KAFKA_INTER_BROKER_LISTENER": "TLS"},
        ["kafka1:9095", "kafka2:9095", "kafka3:9095"],
        ["--replication", "3", "--min-insync", "2", "--kafka-tls-ca", "/certs/ca.pem"],
        ["kafka1", "kafka2", "kafka3"], "Kafka 3 nodes RF=3, TLS (clients + replication)",
        tls=True),
    "rabbitmq-single": System(
        "rabbitmq-single", "rabbitmq", "rabbitmq", {},
        ["amqp://guest:guest@rabbitmq:5672/%2f"], ["--queue-type", "classic"], ["rabbitmq"],
        "RabbitMQ 1 node (classic)"),
    "rabbitmq-cluster": System(
        "rabbitmq-cluster", "rabbitmq", "rabbitmq-cluster", {},
        [f"amqp://guest:guest@rmq{i}:5672/%2f" for i in (1, 2, 3)],
        ["--queue-type", "quorum"], ["rmq1", "rmq2", "rmq3"],
        "RabbitMQ 3 nodes (quorum)"),
}
ALIASES = {
    "all": [s for s in SYSTEMS if not s.endswith(("-pipeline", "-tls-ktls"))],
    "fluxmq": [s for s in SYSTEMS
               if s.startswith("fluxmq") and not s.endswith(("-pipeline", "-tls-ktls"))],
    # The userspace record layer vs kernel TLS (v0.10).
    "ktls": ["fluxmq-single", "fluxmq-single-tls", "fluxmq-single-tls-ktls",
             "fluxmq-cluster", "fluxmq-cluster-tls", "fluxmq-cluster-tls-ktls"],
    "kafka": ["kafka-single", "kafka-cluster", "kafka-single-fsync", "kafka-cluster-fsync",
              "kafka-single-tls", "kafka-cluster-tls"],
    # TLS cost within each system (v0.8).
    "tls": ["fluxmq-single", "fluxmq-single-tls", "fluxmq-cluster", "fluxmq-cluster-tls",
            "kafka-single", "kafka-single-tls", "kafka-cluster", "kafka-cluster-tls"],
    "rabbitmq": ["rabbitmq-single", "rabbitmq-cluster"],
    "single": ["fluxmq-single", "fluxmq-single-tls", "fluxmq-single-os", "fluxmq-single-lz4",
               "fluxmq-single-cumulative", "kafka-single", "kafka-single-fsync",
               "rabbitmq-single"],
    "cluster": ["fluxmq-cluster", "fluxmq-cluster-tls", "fluxmq-cluster-os", "fluxmq-cluster-lz4",
                "fluxmq-cluster-cumulative", "kafka-cluster", "kafka-cluster-fsync",
                "rabbitmq-cluster"],
    # The v0.7 additions only (to add them to an existing result set).
    "v07": ["fluxmq-single-lz4", "fluxmq-single-cumulative", "fluxmq-cluster-lz4",
            "fluxmq-cluster-cumulative", "kafka-single-fsync", "kafka-cluster-fsync"],
}
SCENARIOS = ["idle", "saturation", "latency", "catchup", "overload", "batching"]

# ----------------------------------------------------------------------------------------
# Helpers


def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def write_json(path, doc, indent=2):
    """Writes through a new file: the harness container writes its JSON as its own uid (10001),
    which this user may not be allowed to overwrite in place on Linux hosts."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(doc, indent=indent))
    os.replace(tmp, path)


def run(cmd, env=None, check=True, capture=False, timeout=None, quiet=False):
    if not quiet:
        log("$ " + " ".join(cmd))
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    r = subprocess.run(cmd, env=full_env, text=True, timeout=timeout,
                       stdout=subprocess.PIPE if capture else None,
                       stderr=subprocess.STDOUT if capture else None)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed ({r.returncode}): {' '.join(cmd)}\n{r.stdout or ''}")
    return r


def out(cmd, env=None, timeout=60):
    try:
        r = run(cmd, env=env, check=False, capture=True, timeout=timeout, quiet=True)
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


class Compose:
    def __init__(self, base_env):
        self.base_env = base_env

    def cmd(self, *args, profiles=()):
        c = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE)]
        for p in profiles:
            c += ["--profile", p]
        return c + list(args)

    def env(self, extra=None):
        e = dict(self.base_env)
        if extra:
            e.update(extra)
        return e

    def up(self, system):
        run(self.cmd("up", "-d", "--wait", "--wait-timeout", "240", profiles=[system.profile]),
            env=self.env(system.env), timeout=300)

    def down(self):
        run(self.cmd("down", "-v", "--remove-orphans", profiles=["*"]), env=self.env(),
            check=False, timeout=180)

    def exec(self, service, *args, extra_env=None, timeout=60):
        return out(self.cmd("exec", "-T", service, *args, profiles=["*"]),
                   env=self.env(extra_env), timeout=timeout)


class Abort(RuntimeError):
    """Stops the whole matrix (disk guard, volume cap); never caught per system."""


# Broker data directories (inside the containers) measured after every run.
DATA_DIRS = {"fluxmq": "/var/lib/fluxmq", "kafka": "/var/lib/kafka/data", "rabbitmq": "/var/lib/rabbitmq"}


def host_free_gb():
    return shutil.disk_usage("/").free / 1e9


# ----------------------------------------------------------------------------------------
# docker stats sampler


def parse_bytes(s):
    m = re.match(r"([\d.]+)\s*([KMGT]?i?B)", s.strip())
    if not m:
        return 0
    n = float(m.group(1))
    unit = m.group(2)
    mult = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4,
            "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "kB": 1e3}.get(unit, 1)
    return int(n * mult)


class StatsSampler(threading.Thread):
    """Samples `docker stats --no-stream` for every container of the bench project."""

    def __init__(self, path, system):
        super().__init__(daemon=True)
        self.path = path
        self.system = system
        self.stop_ev = threading.Event()

    def run(self):
        with open(self.path, "a") as f:
            while not self.stop_ev.is_set():
                ps = out(["docker", "ps", "--filter", f"label=com.docker.compose.project={PROJECT}",
                          "--format", '{{.ID}} {{.Label "com.docker.compose.service"}}'], timeout=20)
                if not ps:
                    self.stop_ev.wait(1)
                    continue
                services = dict(line.split(" ", 1) for line in ps.splitlines() if " " in line)
                ts = time.time()
                st = out(["docker", "stats", "--no-stream", "--format", "{{json .}}",
                          *services.keys()], timeout=30)
                if not st:
                    continue
                for line in st.splitlines():
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    cid = d.get("ID", "")[:12]
                    svc = next((v for k, v in services.items() if cid.startswith(k[:12])), d.get("Name"))
                    used, _, limit = d.get("MemUsage", "0B / 0B").partition("/")
                    rec = {
                        "ts": round(ts, 3),
                        "system": self.system,
                        "service": svc,
                        "cpu_pct": float(d.get("CPUPerc", "0%").rstrip("%") or 0),
                        "mem_bytes": parse_bytes(used),
                        "mem_limit": parse_bytes(limit),
                    }
                    f.write(json.dumps(rec) + "\n")
                f.flush()

    def stop(self):
        self.stop_ev.set()
        self.join(timeout=40)


# ----------------------------------------------------------------------------------------
# Readiness


def wait_ready(compose, system, timeout=180):
    deadline = time.time() + timeout
    if system.family == "fluxmq" and system.profile == "cluster":
        svc, scheme = system.http
        while time.time() < deadline:
            raw = compose.exec(svc, "curl", "-fsk", f"{scheme}://127.0.0.1:7421/api/cluster/metadata")
            try:
                nodes = json.loads(raw or "{}").get("nodes", [])
                if len(nodes) == 3 and all(n.get("status") == "up" for n in nodes):
                    return
            except json.JSONDecodeError:
                pass
            time.sleep(2)
        raise RuntimeError("FluxMQ cluster not ready (3 nodes up)")
    if system.family == "rabbitmq" and system.profile == "rabbitmq-cluster":
        while time.time() < deadline:
            raw = compose.exec("rmq1", "rabbitmqctl", "-q", "cluster_status", "--formatter", "json",
                               timeout=60)
            try:
                if len(json.loads(raw or "{}").get("running_nodes", [])) == 3:
                    return
            except json.JSONDecodeError:
                pass
            time.sleep(3)
        raise RuntimeError("RabbitMQ cluster not ready (3 running nodes)")
    if system.family == "kafka" and system.profile == "kafka-cluster":
        while time.time() < deadline:
            raw = compose.exec("kafka1", "/opt/kafka/bin/kafka-metadata-quorum.sh",
                               "--bootstrap-server", "localhost:9092", "describe", "--replication",
                               timeout=60)
            if raw and len([l for l in raw.splitlines() if re.search(r"\bLeader\b|\bFollower\b", l)]) >= 3:
                return
            time.sleep(3)
        raise RuntimeError("Kafka cluster not ready (3 voters)")


def broker_version(compose, system):
    if system.family == "fluxmq":
        svc, scheme = system.http
        raw = compose.exec(svc, "curl", "-fsk", f"{scheme}://127.0.0.1:7421/api/health")
        try:
            return "FluxMQ " + json.loads(raw)["version"]
        except Exception:
            return None
    if system.family == "kafka":
        v = compose.exec(system.nodes[0], "/opt/kafka/bin/kafka-topics.sh", "--version", timeout=90)
        return f"Apache Kafka {v.split()[0]}" if v else None
    if system.family == "rabbitmq":
        v = compose.exec(system.nodes[0], "rabbitmqctl", "-q", "version", timeout=60)
        return f"RabbitMQ {v}" if v else None


# ----------------------------------------------------------------------------------------
# Scenario plans


def plan(args):
    if args.smoke:
        return {
            "warmup": 1, "duration": 3, "drain": 5, "late_drain": 45,
            "sizes": [1024, 100], "clients": [(2, 2)],
            "rates": [1000, 10000, 50000],
            "lat_warmup": 1, "lat_duration": 3,
            "idle_settle": 3, "idle_window": 6,
            "batch_duration": 3,
            "catchup_bytes": 50 * 1024 * 1024, "catchup_timeout": 120,
            "overload_warmup": 1, "overload_duration": 5,
        }
    return {
        "warmup": args.warmup, "duration": args.duration, "drain": 15,
        "late_drain": args.late_drain,
        "sizes": args.sizes, "clients": args.clients,
        "rates": args.rates,
        "lat_warmup": args.warmup, "lat_duration": args.latency_duration,
        "idle_settle": args.idle_settle, "idle_window": 30,
        "batch_duration": args.duration,
        "catchup_bytes": int(args.catchup_gb * 1024 ** 3), "catchup_timeout": args.catchup_timeout,
        "overload_warmup": args.warmup, "overload_duration": args.overload_duration,
    }


class Runner:
    def __init__(self, args, outdir):
        self.args = args
        self.outdir = outdir
        self.p = plan(args)
        base_env = {
            "BROKER_CPUS": str(args.broker_cpus),
            "BROKER_MEM": args.broker_mem,
            "BENCH_CPUS": str(args.bench_cpus),
            "BENCH_MEM": args.bench_mem,
            "BENCH_RESULTS_DIR": str(outdir),
            "FLUXMQ_IMAGE": args.fluxmq_image,
            "HARNESS_IMAGE": args.harness_image,
            # Explicit, so a FLUXMQ_FSYNC in the caller's shell never leaks into the headline
            # systems; the fsync=os variants override it.
            "FLUXMQ_FSYNC": "group-commit",
            # Same segment size for FluxMQ (broker-wide) and Kafka (per topic, via the harness).
            "FLUXMQ_SEGMENT_BYTES": str(args.segment_mb * 1024 * 1024),
            # Broker default (on since v0.9) unless a system sets it; never inherited from the
            # caller's shell.
            "FLUXMQ_COMMIT_PIPELINE": "",
            # Kafka's default flush policy unless a system sets it (kafka-*-fsync: 1); never
            # inherited from the caller's shell.
            "KAFKA_LOG_FLUSH_INTERVAL_MESSAGES": "9223372036854775807",
            # Kafka TLS listener off unless a system turns it on (kafka-*-tls).
            "KAFKA_TLS": "",
            "KAFKA_INTER_BROKER_LISTENER": "",
        }
        if args.kafka_image:
            base_env["KAFKA_IMAGE"] = args.kafka_image
        if args.rabbitmq_image:
            base_env["RABBITMQ_IMAGE"] = args.rabbitmq_image
        # Never let a user's own compose env leak host ports into the bench project.
        self.compose = Compose(base_env)
        self.index = []
        self.sat_max = {}       # system -> best 1 KiB publish rate
        self.versions = {}

    # -- bookkeeping
    def limits(self):
        a = self.args
        return {"broker_cpus": a.broker_cpus, "broker_mem": a.broker_mem,
                "bench_cpus": a.bench_cpus, "bench_mem": a.bench_mem}

    def record(self, doc, name):
        doc.setdefault("limits", self.limits())
        path = self.outdir / "runs" / f"{name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists() or doc.get("kind") != "harness":
            write_json(path, doc)
        self.index.append(f"runs/{name}.json")
        (self.outdir / "index.json").write_text(json.dumps(self.index, indent=2))

    def check_disk(self):
        free = host_free_gb()
        if free < self.args.min_free_gb:
            raise Abort(f"host free disk {free:.1f} GB < --min-free-gb {self.args.min_free_gb}")

    def volume_check(self, system, name):
        """After every run: logs each broker's data-dir size and the host free disk; aborts
        the matrix when a data dir exceeds --max-volume-gb (data must never accumulate)."""
        sizes = {}
        for svc in system.nodes:
            raw = self.compose.exec(svc, "du", "-sk", DATA_DIRS[system.family], timeout=60)
            try:
                sizes[svc] = int(raw.split()[0]) * 1024
            except (AttributeError, IndexError, ValueError):
                sizes[svc] = None
        free = host_free_gb()
        txt = ", ".join(f"{k} {v / 1e9:.2f} GB" if v is not None else f"{k} ?" for k, v in sizes.items())
        log(f"after {name}: data {txt}; host free {free:.1f} GB")
        with open(self.outdir / "volumes.jsonl", "a") as f:
            f.write(json.dumps({"ts": round(time.time(), 1), "run": name, "system": system.name,
                                "bytes": sizes, "host_free_gb": round(free, 1)}) + "\n")
        big = {k: v for k, v in sizes.items() if v and v > self.args.max_volume_gb * 1e9}
        if big:
            raise Abort(f"{name}: broker data exceeds {self.args.max_volume_gb} GB: {big}")
        if free < self.args.min_free_gb:
            raise Abort(f"host free disk {free:.1f} GB < --min-free-gb {self.args.min_free_gb}")

    def topic_gone(self, system, topic):
        """Kafka: the run's topic must be deleted (the harness deletes it on exit)."""
        if system.family != "kafka" or not topic:
            return
        for _ in range(20):
            raw = self.compose.exec(system.nodes[0], "/opt/kafka/bin/kafka-topics.sh",
                                    "--bootstrap-server", "localhost:9092", "--list", timeout=90) or ""
            if topic not in raw.split():
                return
            self.compose.exec(system.nodes[0], "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                              "localhost:9092", "--delete", "--topic", topic, timeout=90)
            time.sleep(2)
        raise Abort(f"kafka topic {topic} still exists after the run")

    # -- harness
    def harness(self, system, name, scenario, size, producers, consumers, rate,
                warmup, duration, linger, repeat=1, extra=(), drain=None, late_drain=None,
                retention_bytes=None):
        self.check_disk()
        rel = f"runs/{name}.json"
        cmd = self.compose.cmd("run", "--rm", "-T", "bench", profiles=["bench"])
        cmd += [
            "--system", system.family, "--brokers", ",".join(system.brokers),
            "--label", system.name, "--scenario", scenario, "--run-id", name,
            "--size", str(size), "--producers", str(producers), "--consumers", str(consumers),
            "--partitions", str(self.args.partitions), "--rate", str(rate),
            "--warmup", str(warmup), "--duration", str(duration),
            "--drain", str(self.p["drain"] if drain is None else drain),
            "--linger-ms", str(linger), "--window", str(self.args.window),
            "--max-in-flight", str(self.args.max_in_flight),
            "--late-drain", str(self.p["late_drain"] if late_drain is None else late_drain),
            "--retention-bytes", str(retention_bytes or self.args.retention_mb * 1024 * 1024),
            "--segment-bytes", str(self.args.segment_mb * 1024 * 1024),
            "--out", f"/results/{rel}",
        ] + system.harness_args + list(extra)
        # + late drain and the FluxMQ audit (<= 600 s: a compressed backlog can hold tens of
        # millions of messages) when a run misses messages.
        timeout = (warmup + duration + (self.p["drain"] if drain is None else drain) * 3
                   + self.p["late_drain"] + 900)
        r = run(cmd, env=self.compose.env(system.env), check=False, timeout=timeout)
        path = self.outdir / rel
        if r.returncode != 0 or not path.exists():
            doc = {"schema": 1, "kind": "failed", "run_id": name, "label": system.name,
                   "system": system.family, "scenario": scenario,
                   "params": {"size": size, "producers": producers, "consumers": consumers,
                              "rate": rate},
                   "repeat": repeat,
                   "error": f"harness exited with {r.returncode}"}
            self.record(doc, name)
            self.delete_orphans(system)
            self.volume_check(system, name)
            return doc
        doc = json.loads(path.read_text())
        doc["limits"] = self.limits()
        doc["repeat"] = repeat
        doc["system_name"] = system.name
        doc["system_title"] = system.title
        doc["broker_version"] = self.versions.get(system.name) or doc.get("broker_version")
        write_json(path, doc)
        self.index.append(rel)
        (self.outdir / "index.json").write_text(json.dumps(self.index, indent=2))
        # Overload runs end with a backlog by design (no drain check; late_drain 0).
        if (not doc.get("drained") and doc.get("params", {}).get("consumers")
                and scenario != "overload"):
            ev = self.save_miss_evidence(system, name, doc.get("topic"))
            missing = self.classify_miss(system, doc, ev)
            write_json(path, doc)
            if system.family == "fluxmq" and missing and self.args.stop_on_missing:
                # Possible data loss: keep the stack's evidence (saved above, audit in the run
                # JSON) and stop the matrix so it is investigated before anything else runs.
                raise Abort(f"{name}: {missing} acknowledged message(s) missing (FluxMQ); "
                            f"evidence in runs/{name}-evidence.json")
        if system.family == "fluxmq" and doc.get("topic"):
            self.delete_flux_channel(system, doc["topic"])
        self.topic_gone(system, doc.get("topic"))
        time.sleep(3)  # let retention / deletes run before measuring
        self.volume_check(system, name)
        return doc

    def save_miss_evidence(self, system, name, topic):
        """A run that did not drain: before the channel is deleted (and the stack torn down),
        keep each broker's recent log and, for FluxMQ, every node's view of the channel and
        its groups (runs/<name>-evidence.json)."""
        ev = {"kind": "evidence", "run": name, "logs": {}, "channel": {}}
        for svc in system.nodes:
            # The system's own profile: `logs` with `--profile '*'` finds no service on
            # Compose v2.24 (Docker Desktop on WSL), which left the evidence logs empty.
            r = out(self.compose.cmd("logs", "--no-color", "--tail", "3000", svc,
                                     profiles=[system.profile]),
                    env=self.compose.env(system.env), timeout=60)
            ev["logs"][svc] = (r or "").splitlines()
            if system.family == "fluxmq" and topic:
                scheme = system.http[1]
                raw = self.compose.exec(svc, "curl", "-fsk",
                                        f"{scheme}://127.0.0.1:7421/api/channels/{topic}")
                try:
                    ev["channel"][svc] = json.loads(raw or "null")
                except json.JSONDecodeError:
                    ev["channel"][svc] = raw
        path = self.outdir / "runs" / f"{name}-evidence.json"
        path.write_text(json.dumps(ev, indent=1))
        log(f"{name}: not drained; broker logs and channel state saved to {path.name}")
        return ev

    def classify_miss(self, system, doc, ev):
        """Splits the harness's `missing` (acknowledged, not received by the end of the late
        drain) into: messages dropped by size retention (consumers fell further behind than the
        cap, so the oldest unconsumed segments were deleted: what a size cap does), messages
        still stored but not delivered in time (consumers behind), and the rest (true loss).
        Writes `drain_miss.classification`; returns the true-loss count.

        FluxMQ (confirmed): the audit re-reads the log; ids found there are "stored, not yet
        delivered"; ids not found are retention drops when the broker logged retention skips for
        the channel covering at least that many messages (v0.6 WARN with `skipped=N`).
        Kafka / RabbitMQ (no audit, inferred): retention drops when the consumers were behind
        (consume < 90 % of publish) and the bytes written per partition exceed the cap."""
        miss = doc.get("drain_miss") or {}
        missing = miss.get("missing") or 0
        if not missing:
            return 0
        p = doc.get("params") or {}
        cap = p.get("retention_bytes") or self.args.retention_mb * 1024 * 1024
        dropped, stored, basis = 0, 0, []
        if system.family == "fluxmq":
            audit = miss.get("audit") or {}
            stored = audit.get("found_in_log") or 0
            not_found = audit.get("not_found")
            if stored:
                basis.append(f"audit: {stored:,} of the ids are still in the log (not yet delivered)")
            skipped, warns = 0, 0
            for lines in (ev.get("logs") or {}).values():
                for l in lines:
                    l = re.sub(r"\x1b\[[0-9;]*m", "", l)
                    if ("retention removed messages a consumer group had not consumed" in l
                            and f"channel={doc.get('topic')}" in l):
                        warns += 1
                        m = re.search(r"skipped=(\d+)", l)
                        skipped += int(m.group(1)) if m else 0
            if not_found and warns and skipped >= not_found:
                dropped = not_found
                basis.append(f"{not_found:,} not in the log (log start offsets "
                             f"{audit.get('low_offsets')}); the broker logged {warns} retention "
                             f"skip(s) for the channel, {skipped:,} messages")
            elif not_found:
                basis.append(f"{not_found:,} not in the log; broker retention skips: {warns} "
                             f"({skipped:,} messages)")
            if not audit or "error" in audit:
                basis.append(f"audit failed: {audit.get('error') if audit else 'none'}")
                stored = 0
        else:
            pub, con = doc.get("publish_rate") or 0, doc.get("consume_rate") or 0
            secs = (p.get("warmup_s") or 0) + (p.get("duration_s") or 0)
            per_part = pub * secs * ((p.get("size") or 0) + 60) / max(1, p.get("partitions") or 1)
            if pub and con < 0.9 * pub and per_part > cap:
                dropped = missing
                basis.append(f"inferred: consumers at {con:,.0f} of {pub:,.0f} msg/s published; "
                             f"~{per_part / 2**20:,.0f} MiB written per partition vs a "
                             f"{cap / 2**20:,.0f} MiB cap")
        true = max(0, missing - dropped - stored)
        miss["classification"] = {"retention_dropped": dropped, "stored_undelivered": stored,
                                  "true_missing": true,
                                  "basis": "; ".join(basis) or "not explained"}
        doc["drain_miss"] = miss
        log(f"{doc.get('run_id')}: missing {missing:,}: {dropped:,} dropped by retention, "
            f"{stored:,} stored but not delivered in time, {true:,} unexplained "
            f"({miss['classification']['basis']})")
        return true

    def delete_orphans(self, system):
        """After a failed harness run (e.g. killed) its topic / channel was not deleted: remove
        every bench topic so the data cannot pile up (the next volume check would abort)."""
        if system.family == "fluxmq":
            svc, scheme = system.http
            raw = self.compose.exec(svc, "curl", "-fsk", f"{scheme}://127.0.0.1:7421/api/channels")
            try:
                data = json.loads(raw or "[]")
            except json.JSONDecodeError:
                return
            items = data.get("channels", data) if isinstance(data, dict) else data
            for c in items or []:
                n = c.get("name") if isinstance(c, dict) else c
                if isinstance(n, str) and n.startswith("bench-"):
                    log(f"deleting orphaned channel {n}")
                    self.delete_flux_channel(system, n)
        elif system.family == "kafka":
            raw = self.compose.exec(system.nodes[0], "/opt/kafka/bin/kafka-topics.sh",
                                    "--bootstrap-server", "localhost:9092", "--list", timeout=90) or ""
            for t in raw.split():
                if t.startswith("bench-"):
                    log(f"deleting orphaned topic {t}")
                    self.topic_gone(system, t)
        time.sleep(3)

    def delete_flux_channel(self, system, channel):
        svc, scheme = system.http
        self.compose.exec(svc, "curl", "-fsk", "-X", "DELETE",
                          f"{scheme}://127.0.0.1:7421/api/channels/{channel}")

    # -- scenarios
    def idle(self, system):
        time.sleep(self.p["idle_settle"])
        start = time.time()
        time.sleep(self.p["idle_window"])
        end = time.time()
        self.record({"schema": 1, "kind": "idle", "run_id": f"{system.name}-idle",
                     "label": system.name, "system_name": system.name,
                     "system_title": system.title, "system": system.family,
                     "scenario": "idle", "measure_start_ms": int(start * 1000),
                     "measure_end_ms": int(end * 1000),
                     "broker_version": self.versions.get(system.name)},
                    f"{system.name}-idle")

    def saturation(self, system):
        # Repeats are interleaved over the configurations (r1 of every config, then r2, ...)
        # so slow drift (thermal, page cache) spreads over all of them.
        reps = max(1, self.args.repeat)
        for rep in range(1, reps + 1):
            for size in self.p["sizes"]:
                for (pr, co) in self.p["clients"]:
                    name = f"{system.name}-sat-{size}B-{pr}p{co}c" + (f"-r{rep}" if reps > 1 else "")
                    log(f"saturation {name}")
                    doc = self.harness(system, name, "saturation", size, pr, co, 0,
                                       self.p["warmup"], self.p["duration"], self.args.linger_ms,
                                       repeat=rep)
                    if size == 1024 and doc.get("kind") == "harness":
                        self.sat_max[system.name] = max(self.sat_max.get(system.name, 0),
                                                        doc.get("publish_rate", 0))

    def latency(self, system):
        prev_ok = True
        for rate in self.p["rates"]:
            name = f"{system.name}-lat-{rate}"
            reason = None
            if not prev_ok:
                reason = "a lower rate was not sustained"
            elif system.name in self.sat_max and self.sat_max[system.name] < rate * 1.05:
                reason = (f"saturation throughput at 1 KiB ({self.sat_max[system.name]:.0f} msg/s) "
                          f"is below {rate} msg/s")
            if reason:
                log(f"skip {name}: {reason}")
                self.record({"schema": 1, "kind": "skipped", "run_id": name, "label": system.name,
                             "system_name": system.name, "system_title": system.title,
                             "system": system.family, "scenario": "latency",
                             "params": {"rate": rate, "size": 1024}, "reason": reason}, name)
                continue
            log(f"latency {name}")
            doc = self.harness(system, name, "latency", 1024, self.args.lat_producers,
                               self.args.lat_consumers, rate, self.p["lat_warmup"],
                               self.p["lat_duration"], self.args.latency_linger_ms)
            prev_ok = doc.get("kind") == "harness" and bool(doc.get("sustained"))

    def catchup(self, system):
        """Preload --catchup-gb (no consumer reading), then start 4 consumers and measure
        consume-only throughput until every preloaded message arrived. Retention is raised for
        the run so the preload is never trimmed (the volume guard still applies)."""
        for size in self.p["sizes"]:
            pr, co = self.args.catchup_clients
            n = max(1, self.p["catchup_bytes"] // size)
            per_part = int(self.p["catchup_bytes"] * 2 / max(1, self.args.partitions)) + (64 << 20)
            name = f"{system.name}-catchup-{size}B-{pr}p{co}c"
            log(f"catchup {name}: {n:,} messages ({self.p['catchup_bytes'] / 2**20:,.0f} MiB)")
            self.harness(system, name, "catchup", size, pr, co, 0, 0, 0, self.args.linger_ms,
                         extra=["--preload", str(n)], drain=self.p["catchup_timeout"],
                         retention_bytes=max(per_part, self.args.retention_mb * 1024 * 1024))

    def overload(self, system):
        """Producers unthrottled (closed loop, the same window as saturation) for a longer
        window: consumed msg/s and how fast the consumer lag grows (`lag_growth_per_s`, the
        per-second `lag` in the timeline). Retention drops under the size cap are expected
        and counted by the brokers; no drain check."""
        for size in self.p["sizes"]:
            pr, co = self.args.overload_clients
            name = f"{system.name}-overload-{size}B-{pr}p{co}c"
            log(f"overload {name}")
            self.harness(system, name, "overload", size, pr, co, 0, self.p["overload_warmup"],
                         self.p["overload_duration"], self.args.linger_ms, drain=5, late_drain=0)

    def batching(self):
        """fluxmq-bench: v0.4 baseline image vs the current image, single node, interleaved.

        * closed-1KiB / closed-100B / light-1k: each image's own bench against its own broker
          (old SDK + old broker vs new SDK + new broker);
        * drain-1KiB / drain-100B: the *current* bench (the v0.4 bench has no drain mode)
          against each broker: prefill N, then 4 consumers drain from the earliest offset.
          This isolates the consumer path (Deliver vs DeliverBatch).
        """
        system = SYSTEMS["fluxmq-single"]
        images = [("baseline", self.args.baseline_image), ("current", self.args.fluxmq_image)]
        d = self.p["batch_duration"]
        n_1k, n_100 = (200_000, 500_000) if self.args.smoke else (1_000_000, 3_000_000)
        runs = [
            ("closed-1KiB", None, ["--producers", "4", "--consumers", "4", "--inflight", "4",
                                   "--batch", "100", "--size", "1024", "--duration", str(d)]),
            ("closed-100B", None, ["--producers", "4", "--consumers", "4", "--inflight", "4",
                                   "--batch", "100", "--size", "100", "--duration", str(d)]),
            ("light-1k", None, ["--producers", "4", "--consumers", "4", "--inflight", "32",
                                "--batch", "1", "--size", "1024", "--rate", "1000",
                                "--duration", str(d)]),
            ("drain-1KiB", self.args.fluxmq_image,
             ["--producers", "4", "--consumers", "4", "--inflight", "4", "--batch", "100",
              "--size", "1024", "--drain", str(n_1k), "--duration", "120"]),
            ("drain-100B", self.args.fluxmq_image,
             ["--producers", "4", "--consumers", "4", "--inflight", "4", "--batch", "100",
              "--size", "100", "--drain", str(n_100), "--duration", "120"]),
        ]
        present = []
        for tag, image in images:
            if out(["docker", "image", "inspect", image, "--format", "{{.Id}}"]):
                present.append((tag, image))
            else:
                log(f"batching: image {image} missing, skipped")
                self.record({"schema": 1, "kind": "skipped", "scenario": "batching",
                             "run_id": f"batching-{tag}", "label": tag,
                             "reason": f"image {image} not built"}, f"batching-{tag}")
        reps = max(1, self.args.batch_repeat)
        for rep in range(1, reps + 1):
            for tag, image in present:
                self.check_disk()
                env = {"FLUXMQ_IMAGE": image, "FLUXMQ_SINGLE_TLS": "off"}
                self.compose.down()
                run(self.compose.cmd("up", "-d", "--wait", profiles=["single"]),
                    env=self.compose.env(env), timeout=300)
                rev = out(["docker", "image", "inspect", image, "--format",
                           '{{index .Config.Labels "org.opencontainers.image.revision"}}'])
                sampler = StatsSampler(self.outdir / "stats.jsonl", f"batching-{tag}")
                sampler.start()
                try:
                    for i, (rname, bench_image, rargs) in enumerate(runs):
                        name = f"batching-{tag}-{rname}" + (f"-r{rep}" if reps > 1 else "")
                        channel = f"bench-{tag}-{rep}-{i}"
                        bench_image = bench_image or image
                        log(f"batching {name} (bench {bench_image})")
                        start = time.time()
                        r = run(self.compose.cmd("run", "--rm", "-T", "fluxmq-bench", profiles=["bench"])
                                + ["--addr", "fluxmq:7420", "--channel", channel] + rargs,
                                env=self.compose.env(env | {"FLUXMQ_BENCH_IMAGE": bench_image}),
                                check=False, capture=True, timeout=max(d, 120) + 180)
                        end = time.time()
                        text = r.stdout or ""
                        (self.outdir / "runs" / f"{name}.txt").write_text(text)
                        doc = parse_fluxmq_bench(text)
                        doc.update({"schema": 1, "kind": "fluxmq-bench", "scenario": "batching",
                                    "run_id": name, "label": tag, "image": image, "revision": rev,
                                    "bench_image": bench_image, "run": rname, "args": rargs,
                                    "exit": r.returncode, "repeat": rep,
                                    "measure_start_ms": int(start * 1000),
                                    "measure_end_ms": int(end * 1000)})
                        self.record(doc, name)
                        self.delete_flux_channel(system, channel)
                        time.sleep(3)
                        self.volume_check(system, name)
                finally:
                    sampler.stop()
                    self.compose.down()

    # -- per system
    def system(self, system):
        log(f"==== {system.name}: {system.title}")
        stale = self.outdir / "runs" / f"{system.name}-failed.json"
        self.check_disk()
        if stale.exists():
            stale.unlink()
        self.compose.down()
        t0 = time.time()
        self.compose.up(system)
        wait_ready(self.compose, system)
        startup = time.time() - t0
        self.versions[system.name] = broker_version(self.compose, system)
        log(f"{system.name} ready in {startup:.0f}s ({self.versions[system.name]})")
        flush = None
        if system.family == "kafka":
            # Record the effective flush policy (proof for the -fsync rows).
            raw = self.compose.exec(system.nodes[0], "/opt/kafka/bin/kafka-configs.sh",
                                    "--bootstrap-server", "localhost:9092", "--describe",
                                    "--entity-type", "brokers", "--entity-name", "1", "--all",
                                    timeout=90) or ""
            m = re.search(r"log\.flush\.interval\.messages=(\d+)", raw)
            flush = int(m.group(1)) if m else None
            log(f"{system.name}: log.flush.interval.messages={flush}")
        self.record({"schema": 1, "kind": "system", "run_id": f"{system.name}-system",
                     "system_name": system.name, "system_title": system.title,
                     "system": system.family, "startup_s": round(startup, 1),
                     "broker_version": self.versions[system.name],
                     "nodes": system.nodes, "tls": system.tls,
                     "kafka_log_flush_interval_messages": flush}, f"{system.name}-system")
        sampler = StatsSampler(self.outdir / "stats.jsonl", system.name)
        sampler.start()
        try:
            if "idle" in self.args.scenarios:
                self.idle(system)
            if "saturation" in self.args.scenarios:
                self.saturation(system)
            if "latency" in self.args.scenarios:
                self.latency(system)
            if "catchup" in self.args.scenarios:
                self.catchup(system)
            if "overload" in self.args.scenarios:
                self.overload(system)
        finally:
            sampler.stop()
            self.compose.down()


def parse_fluxmq_bench(text):
    """Parses the summary printed by fluxmq-bench."""
    d = {}
    m = re.search(r"published\s+(\d+) msgs, (\d+) msg/s, ([\d.]+) MB/s", text)
    if m:
        d["published"] = int(m.group(1))
        d["publish_rate"] = int(m.group(2))
        d["publish_mb_s"] = float(m.group(3))
    m = re.search(r"consumed\s+(\d+) msgs \((\d+) during the run, (\d+) msg/s\); (.*)", text)
    if m:
        d["consumed"] = int(m.group(1))
        d["consume_rate"] = int(m.group(3))
        d["drained"] = m.group(4).startswith("all published")

    def pct(line):
        if not line:
            return None
        vals = dict(re.findall(r"(min|p50|p90|p99|p99\.9|max) ([\d.]+)", line))
        if not vals:
            return None
        return {"min": float(vals.get("min", 0)), "p50": float(vals.get("p50", 0)),
                "p90": float(vals.get("p90", 0)), "p99": float(vals.get("p99", 0)),
                "p999": float(vals.get("p99.9", 0)), "max": float(vals.get("max", 0))}

    m = re.search(r"publish rtt .*?ms: (.*)", text)
    d["publish_ms"] = pct(m.group(1) if m else None)
    m = re.search(r"end-to-end .*?ms:\s+(.*)", text)
    d["e2e_ms"] = pct(m.group(1) if m else None)
    m = re.search(r"consumed\s+(\d+) of (\d+) msgs in ([\d.]+)s: (\d+) msg/s, ([\d.]+) MB/s(.*)", text)
    if m:
        d["consumed"] = int(m.group(1))
        d["drain_total"] = int(m.group(2))
        d["drain_s"] = float(m.group(3))
        d["consume_rate"] = int(m.group(4))
        d["consume_mb_s"] = float(m.group(5))
        d["drained"] = "TIMED OUT" not in m.group(6) and m.group(1) == m.group(2)
    m = re.search(r"errors\s+(\d+)", text)
    d["errors"] = int(m.group(1)) if m else 0
    return d


# ----------------------------------------------------------------------------------------
# Environment capture


def capture_env(args, outdir):
    env = {"captured_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
           "smoke": args.smoke, "argv": sys.argv[1:]}
    if platform.system() == "Darwin":
        env["host"] = {
            "os": f"macOS {out(['sw_vers', '-productVersion'])} ({out(['sw_vers', '-buildVersion'])})",
            "model": out(["sysctl", "-n", "hw.model"]),
            "cpu": out(["sysctl", "-n", "machdep.cpu.brand_string"]),
            "cores": out(["sysctl", "-n", "hw.ncpu"]),
            "perf_cores": out(["sysctl", "-n", "hw.perflevel0.physicalcpu"]),
            "eff_cores": out(["sysctl", "-n", "hw.perflevel1.physicalcpu"]),
            "ram_gb": round(int(out(["sysctl", "-n", "hw.memsize"]) or 0) / 2**30, 1),
        }
    else:
        env["host"] = {"os": platform.platform(), "cpu": platform.processor(),
                       "cores": os.cpu_count()}
    env["host"]["disk_free_gb"] = round(host_free_gb(), 1)
    info = out(["docker", "info", "--format", "{{json .}}"])
    if info:
        i = json.loads(info)
        env["docker"] = {"server_version": i.get("ServerVersion"), "os": i.get("OperatingSystem"),
                         "kernel": i.get("KernelVersion"), "arch": i.get("Architecture"),
                         "vm_cpus": i.get("NCPU"), "vm_mem_gb": round(i.get("MemTotal", 0) / 2**30, 1),
                         "storage_driver": i.get("Driver"), "cgroup": i.get("CgroupVersion")}
    env["compose"] = out(["docker", "compose", "version", "--short"])
    env["limits"] = {"broker_cpus": args.broker_cpus, "broker_mem": args.broker_mem,
                     "bench_cpus": args.bench_cpus, "bench_mem": args.bench_mem}
    imgs = {}
    for ref in [args.fluxmq_image, args.harness_image, args.baseline_image,
                args.kafka_image or "apache/kafka:3.7.0", args.rabbitmq_image or "rabbitmq:4.1"]:
        raw = out(["docker", "image", "inspect", ref, "--format",
                   '{{.Id}}|{{.Created}}|{{.Size}}|{{index .Config.Labels "org.opencontainers.image.revision"}}'])
        if raw:
            iid, created, size, rev = raw.split("|")
            imgs[ref] = {"id": iid[:19], "created": created[:19], "size_mb": round(int(size) / 1e6),
                         "revision": rev or None}
    env["images"] = imgs
    env["git"] = {"sha": out(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
                  "dirty": bool(out(["git", "-C", str(ROOT), "status", "--porcelain", "--",
                                     "crates", "ui", "Cargo.toml", "Cargo.lock"]))}
    env["plan"] = plan(args) | {"partitions": args.partitions, "window": args.window,
                                "max_in_flight": args.max_in_flight,
                                "segment_mb": args.segment_mb,
                                "repeat": args.repeat, "batch_repeat": args.batch_repeat,
                                "linger_ms": args.linger_ms,
                                "latency_linger_ms": args.latency_linger_ms,
                                "lat_clients": [args.lat_producers, args.lat_consumers],
                                "retention_mb_per_partition": args.retention_mb}
    prev = outdir / "env.json"
    if prev.exists():  # re-run into an existing results dir: keep the earlier capture(s)
        try:
            old = json.loads(prev.read_text())
            env["earlier"] = old.pop("earlier", []) + [old]
        except json.JSONDecodeError:
            pass
    (outdir / "env.json").write_text(json.dumps(env, indent=2))
    return env


# ----------------------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="FluxMQ vs Kafka vs RabbitMQ benchmark runner (see bench/README.md)")
    ap.add_argument("--systems", default="all",
                    help="comma list of systems or aliases: " + ", ".join(list(SYSTEMS) + list(ALIASES)))
    ap.add_argument("--scenarios", default=",".join(SCENARIOS),
                    help="comma list of: " + ", ".join(SCENARIOS))
    ap.add_argument("--smoke", action="store_true", help="a few seconds per scenario")
    ap.add_argument("--build", choices=["auto", "always", "never"], default="auto",
                    help="build images first (auto = only when missing)")
    ap.add_argument("--ref", default="", help="git ref to package (default: working tree)")
    ap.add_argument("--baseline-ref", default="3a6bb81",
                    help="git ref of the FluxMQ baseline for the batching comparison (v0.4)")
    ap.add_argument("--fluxmq-image", default="fluxmq:dev")
    ap.add_argument("--baseline-image", default="fluxmq:v0.4-baseline")
    ap.add_argument("--harness-image", default="fluxmq-harness:dev")
    ap.add_argument("--kafka-image", default="")
    ap.add_argument("--rabbitmq-image", default="")
    ap.add_argument("--duration", type=int, default=30, help="saturation seconds measured")
    ap.add_argument("--latency-duration", type=int, default=30)
    ap.add_argument("--warmup", type=int, default=10)
    ap.add_argument("--sizes", default="1024,100")
    ap.add_argument("--clients", default="1x1,4x4", help="producers x consumers for saturation")
    ap.add_argument("--rates", default="1000,10000,50000")
    ap.add_argument("--lat-producers", type=int, default=4)
    ap.add_argument("--lat-consumers", type=int, default=4)
    ap.add_argument("--partitions", type=int, default=8)
    # Unacked publishes per producer. 20000: by Little's law in-flight = throughput x ack
    # latency; at the ~20-50 ms acks seen under saturation in v0.5, 20k per producer allows
    # 0.4-1M msg/s from ONE producer, above every system's measured ceiling (Kafka 1 node
    # 1x1: 383k), so the window no longer caps any system (2000 capped FluxMQ 1x1 at ~143k).
    # Larger only adds queueing latency and client memory (20k x 1 KiB = 20 MB per producer).
    ap.add_argument("--window", type=int, default=20000)
    ap.add_argument("--max-in-flight", type=int, default=32,
                    help="FluxMQ BatchingOptions.max_in_flight (Publish requests pipelined per "
                         "producer). Kafka: librdkafka's idempotent producer pipelines up to 5 "
                         "requests per broker connection; RabbitMQ: no equivalent")
    ap.add_argument("--segment-mb", type=int, default=16,
                    help="log segment size for FluxMQ (FLUXMQ_SEGMENT_BYTES) and Kafka "
                         "(segment.bytes); RabbitMQ has no equivalent")
    ap.add_argument("--late-drain", type=int, default=45,
                    help="after a missed 15 s drain, wait this much longer and classify missed "
                         "messages as late or missing (above the 30 s FluxMQ/AMQP ack timeout)")
    ap.add_argument("--idle-settle", type=int, default=30,
                    help="seconds after start-up before the 30 s idle window")
    ap.add_argument("--linger-ms", type=int, default=5, help="saturation producer linger")
    ap.add_argument("--latency-linger-ms", type=int, default=1)
    ap.add_argument("--retention-mb", type=int, default=128, help="per partition / queue")
    ap.add_argument("--catchup-gb", type=float, default=1.0,
                    help="catch-up scenario: data preloaded before the consumers start")
    ap.add_argument("--catchup-timeout", type=int, default=600,
                    help="catch-up scenario: max seconds for the consumers to read the preload")
    ap.add_argument("--catchup-clients", default="4x4")
    ap.add_argument("--overload-duration", type=int, default=60,
                    help="overload scenario: measured seconds")
    ap.add_argument("--overload-clients", default="4x4")
    ap.add_argument("--broker-cpus", default="2")
    ap.add_argument("--broker-mem", default="2g")
    ap.add_argument("--bench-cpus", default="2")
    ap.add_argument("--bench-mem", default="2g")
    ap.add_argument("--repeat", type=int, default=1,
                    help="saturation repeats per configuration (interleaved); the report shows "
                         "the median and the range")
    ap.add_argument("--batch-repeat", type=int, default=3,
                    help="rounds of the batching comparison (baseline/current interleaved)")
    ap.add_argument("--min-free-gb", type=float, default=10.0,
                    help="abort the whole matrix when host free disk drops below this (checked "
                         "before and after every run)")
    ap.add_argument("--max-volume-gb", type=float, default=5.0,
                    help="abort when a broker's data dir exceeds this after a run")
    ap.add_argument("--stop-on-missing", action=argparse.BooleanOptionalAction, default=True,
                    help="abort the matrix when a FluxMQ run reports missing (not just late) "
                         "messages, after saving the evidence")
    ap.add_argument("--out", default="", help="results dir (default bench/results/<timestamp>)")
    ap.add_argument("--no-report", action="store_true")
    args = ap.parse_args()

    names = []
    for s in args.systems.split(","):
        s = s.strip()
        for n in ALIASES.get(s, [s]):
            if n not in SYSTEMS:
                ap.error(f"unknown system {n}")
            if n not in names:
                names.append(n)
    args.scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    for s in args.scenarios:
        if s not in SCENARIOS:
            ap.error(f"unknown scenario {s}")
    args.sizes = [int(x) for x in args.sizes.split(",")]
    args.rates = [int(x) for x in args.rates.split(",")]
    args.clients = [tuple(int(v) for v in c.split("x")) for c in args.clients.split(",")]
    args.catchup_clients = tuple(int(v) for v in args.catchup_clients.split("x"))
    args.overload_clients = tuple(int(v) for v in args.overload_clients.split("x"))

    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S") + ("-smoke" if args.smoke else "")
    outdir = Path(args.out) if args.out else ROOT / "bench" / "results" / stamp
    outdir = outdir.resolve()
    try:
        if json.loads((outdir / "env.json").read_text()).get("frozen"):
            ap.error(f"{outdir} is a frozen result set (kept as published history); use a new --out")
    except (OSError, json.JSONDecodeError):
        pass
    (outdir / "runs").mkdir(parents=True, exist_ok=True)
    os.chmod(outdir, 0o777)
    os.chmod(outdir / "runs", 0o777)
    log(f"results -> {outdir}")

    # Images.
    def have(img):
        return bool(out(["docker", "image", "inspect", img, "--format", "{{.Id}}"]))
    build = [str(ROOT / "deploy" / "build.sh")]
    if args.build == "always" or (args.build == "auto" and not (have(args.fluxmq_image) and have(args.harness_image))):
        cmd = build + ["--harness", "--tag", args.fluxmq_image.split(":", 1)[1]]
        if args.ref:
            cmd += ["--ref", args.ref]
        run(cmd, timeout=3600)
    if "batching" in args.scenarios and (args.build == "always" or not have(args.baseline_image)):
        run(build + ["--ref", args.baseline_ref, "--tag", args.baseline_image.split(":", 1)[1]],
            timeout=3600)

    env = capture_env(args, outdir)
    log(f"environment: {env.get('docker', {})}")
    runner = Runner(args, outdir)

    def on_signal(sig, frame):
        log("interrupted: tearing down")
        runner.compose.down()
        sys.exit(130)
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    failures = []
    t0 = time.time()
    try:
        for n in names:
            try:
                runner.system(SYSTEMS[n])
            except Abort:
                raise
            except Exception as e:  # keep going with the next system
                log(f"!! {n} failed: {e}")
                failures.append({"system": n, "error": str(e)[:2000]})
                runner.record({"schema": 1, "kind": "system-failed", "run_id": f"{n}-failed",
                               "system_name": n, "error": str(e)[:2000]}, f"{n}-failed")
                runner.compose.down()
        if "batching" in args.scenarios:
            try:
                runner.batching()
            except Abort:
                raise
            except Exception as e:
                log(f"!! batching failed: {e}")
                failures.append({"system": "batching", "error": str(e)[:2000]})
                runner.compose.down()
    except Abort as e:
        log(f"!! ABORTED: {e}")
        failures.append({"system": "abort", "error": str(e)})
    finally:
        runner.compose.down()
    env["total_s"] = round(time.time() - t0)
    env["failures"] = failures
    (outdir / "env.json").write_text(json.dumps(env, indent=2))
    log(f"done in {env['total_s']}s, {len(failures)} failure(s); results in {outdir}")
    if not args.no_report:
        run([sys.executable, str(ROOT / "bench" / "report" / "generate.py"), str(outdir),
             "-o", str(ROOT / "bench" / "report" / "index.html")], check=False)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

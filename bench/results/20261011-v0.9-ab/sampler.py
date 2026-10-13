#!/usr/bin/env python3
"""Samples, every ~2 s, each running broker of the fluxmq-bench compose project:
data-dir size (du) and, for FluxMQ, the fsync-latency histogram from /metrics.
Appends JSON lines to argv[1]. Stops when argv[2] (a stop file) exists."""
import json, subprocess, sys, time, os, shutil

out, stop = sys.argv[1], sys.argv[2]
INTERVAL = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
NODU = len(sys.argv) > 4 and sys.argv[4] == "nodu"
DIRS = {"fluxmq": "/var/lib/fluxmq", "kafka": "/var/lib/kafka/data"}


def sh(args, timeout=10):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout).stdout
    except Exception:
        return ""


while not os.path.exists(stop):
    t0 = time.time()
    names = sh(["docker", "ps", "--filter", "label=com.docker.compose.project=fluxmq-bench",
                "--format", "{{.Names}}"]).split()
    rec = {"ts": round(t0, 2), "host_free_gb": round(shutil.disk_usage("/").free / 1e9, 1), "nodes": {}}
    for n in names:
        svc = n.replace("fluxmq-bench-", "").rsplit("-", 1)[0]
        fam = "kafka" if svc.startswith("kafka") else ("fluxmq" if svc in ("fluxmq", "n1", "n2", "n3") else None)
        if not fam:
            continue
        d = {}
        raw = "" if NODU else sh(["docker", "exec", n, "du", "-sk", DIRS[fam]])
        try:
            d["bytes"] = int(raw.split()[0]) * 1024
        except (IndexError, ValueError):
            pass
        if fam == "fluxmq":
            m = sh(["docker", "exec", n, "sh", "-c",
                    "curl -fsSk --max-time 2 https://127.0.0.1:7421/metrics 2>/dev/null || "
                    "curl -fsS --max-time 2 http://127.0.0.1:7421/metrics"])
            fs = {}
            for line in m.splitlines():
                for pre in ("fluxmq_node_commit_latency_seconds_sum", "fluxmq_node_commit_latency_seconds_count",
                            "fluxmq_channel_publish_latency_seconds_sum", "fluxmq_channel_publish_latency_seconds_count"):
                    if line.startswith(pre):
                        try:
                            d[pre] = d.get(pre, 0.0) + float(line.rpartition(" ")[2])
                        except ValueError:
                            pass
                if line.startswith("fluxmq_node_fsync_latency_seconds"):
                    k, _, v = line.rpartition(" ")
                    try:
                        fs[k.replace("fluxmq_node_fsync_latency_seconds", "")] = float(v)
                    except ValueError:
                        pass
            if fs:
                d["fsync"] = fs
        rec["nodes"][svc] = d
    if rec["nodes"]:
        with open(out, "a") as f:
            f.write(json.dumps(rec) + "\n")
    time.sleep(max(0.2, INTERVAL - (time.time() - t0)))

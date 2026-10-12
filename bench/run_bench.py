"""Benchmark runner. Runs against a stack that is already up.

    python bench/run_bench.py producer --reps 5
    python bench/run_bench.py lag --reps 5 --rate 100 --seconds 60
    python bench/run_bench.py export --reps 5
    python bench/run_bench.py datanode --reps 5
    python bench/run_bench.py sink-recovery --reps 5 --outage 10
    python bench/run_bench.py all

Output goes to bench/scratch/ unless --final is given, which writes to
bench/results/. Each run also writes <name>.meta.json with the machine, the
commit and the exact command, so a number can be traced back.
"""
import argparse
import csv
import json
import platform
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import faults  # noqa: E402
import stack  # noqa: E402
from stack import compose, say, tools  # noqa: E402


def out_dir(final: bool) -> Path:
    d = ROOT / "bench" / ("results" if final else "scratch")
    d.mkdir(parents=True, exist_ok=True)
    return d


def machine_info() -> dict:
    info = stack.docker("info", "--format", "{{.NCPU}} {{.MemTotal}} {{.ServerVersion}} {{.OperatingSystem}}").stdout
    ncpu, mem, ver, osname = info.strip().split(" ", 3)
    cpu = platform.processor()
    try:
        cpu = subprocess.run(
            ["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"],
            capture_output=True, text=True, timeout=20).stdout.strip() or cpu
    except (OSError, subprocess.SubprocessError):
        pass
    return {
        "host_cpu": cpu, "docker_cpus": int(ncpu), "docker_memory_gib": round(int(mem) / 2**30, 1),
        "docker_version": ver, "docker_os": osname, "host_os": platform.platform(),
        "commit": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
                                 text=True).stdout.strip(),
    }


def write_results(name: str, rows: list[dict], final: bool, extra: dict | None = None) -> None:
    d = out_dir(final)
    with open(d / f"{name}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    meta = {"name": name, "when": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "argv": sys.argv,
            "machine": machine_info(), **(extra or {})}
    (d / f"{name}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    say(f"wrote {d / (name + '.csv')}")


def summarize(label: str, values: list[float]) -> None:
    if values:
        say(f"{label}: median {statistics.median(values):.2f} min {min(values):.2f} max {max(values):.2f} "
            f"(n={len(values)})")


def producer(reps: int, final: bool) -> None:
    rows = []
    for rep in range(1, reps + 1):
        tag = f"{int(time.time())}-{rep}"
        env = ["-e", f"BARS_TOPIC=bench.bars.{tag}", "-e", f"PROGRESS_TOPIC=bench.progress.{tag}",
               "-e", f"PRODUCER_TXN_ID=bench-{tag}"]
        tools_args = ["run", "--rm", "-T", *env, "tools"]
        compose(*tools_args, "python", "-m", "mdp.stream.admin", timeout=300)
        res = compose(*tools_args, "python", "-m", "mdp.stream.producer", "--exit-when-idle", timeout=1800)
        m = re.search(r"done sent=(\d+) last_id=(\d+) ([\d,.]+) msg/s", res.stdout)
        if not m:
            raise RuntimeError("producer printed no summary:\n" + res.stdout + res.stderr)
        rows.append({"rep": rep, "messages": int(m[1]), "msgs_per_s": float(m[3].replace(",", ""))})
        compose(*tools_args, "python", "-m", "mdp.stream.admin", "delete", timeout=300)
        say(f"producer rep {rep}: {rows[-1]['msgs_per_s']:.0f} msg/s over {rows[-1]['messages']} messages")
    summarize("producer msg/s", [r["msgs_per_s"] for r in rows])
    write_results("producer_throughput", rows, final)


def lag(reps: int, rate: float, seconds: float, final: bool) -> None:
    rows = []
    for rep in range(1, reps + 1):
        first = int(stack.sql("SELECT COALESCE(MAX(id), 0) FROM bars")) + 1
        compose("--profile", "live", "up", "-d", "generator-live", env={"GEN_RATE": str(rate)})
        time.sleep(seconds)
        compose("stop", "generator-live")
        last = int(stack.sql("SELECT MAX(id) FROM bars"))
        res = tools("python", "-m", "mdp.bench.lag", "--from-id", str(first), "--to-id", str(last),
                    timeout=300, check=False)
        doc = json.loads(res.stdout.strip().splitlines()[-1])
        if doc["landed"] != doc["rows"]:
            raise RuntimeError(f"rows missing on HDFS: {doc}")
        rows.append({"rep": rep, "rate_bars_per_s": rate, "rows": doc["rows"],
                     "p50_s": round(doc["p50"], 3), "p95_s": round(doc["p95"], 3),
                     "p99_s": round(doc["p99"], 3), "max_s": round(doc["max"], 3)})
        say(f"lag rep {rep}: p50 {doc['p50']:.2f}s p95 {doc['p95']:.2f}s over {doc['rows']} rows")
    summarize("lag p50 (s)", [r["p50_s"] for r in rows])
    summarize("lag p95 (s)", [r["p95_s"] for r in rows])
    write_results("end_to_end_lag", rows, final)


def export(reps: int, final: bool) -> None:
    res = tools("python", "-m", "mdp.bench.export_rate", "--reps", str(reps), timeout=1800)
    rows = [json.loads(ln) for ln in res.stdout.splitlines() if ln.startswith("{")]
    summarize("export rows/s", [r["rows_per_s"] for r in rows])
    write_results("export_rate", rows, final)


def datanode(reps: int, final: bool) -> None:
    rows = []
    for rep in range(1, reps + 1):
        r = faults.datanode(seconds=25)
        rows.append({"rep": rep, "victim": r["victim"], "cache_files_lost": r["affected_files"],
                     "recovery_s": r["recovery_seconds"], "slowest_call_ms": r["slowest_call_ms"],
                     "errors": r["errors"]})
        time.sleep(15)  # let the NameNode re-replicate before the next kill
    summarize("recovery (s)", [r["recovery_s"] for r in rows])
    write_results("datanode_recovery", rows, final)


def sink_recovery(reps: int, outage: float, rate: float, final: bool) -> None:
    rows = []
    compose("--profile", "live", "up", "-d", "generator-live", env={"GEN_RATE": str(rate)})
    try:
        for rep in range(1, reps + 1):
            proc = subprocess.Popen(
                ["docker", "compose", "-p", stack.PROJECT, "run", "--rm", "-T", "tools",
                 "python", "-m", "mdp.bench.sink_recovery"],
                cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            assert proc.stdout.readline().strip() == "READY"
            stack.kill("sink")
            time.sleep(outage)
            stack.start("sink")
            proc.stdin.write("go\n")
            proc.stdin.flush()
            doc = json.loads(proc.stdout.readline())
            proc.wait(timeout=60)
            rows.append({"rep": rep, "outage_s": outage, "rate_bars_per_s": rate, **doc})
            say(f"sink recovery rep {rep}: {doc}")
            time.sleep(10)
    finally:
        compose("stop", "generator-live")
    summarize("first batch after restart (s)", [r["first_batch_s"] for r in rows if r["first_batch_s"]])
    summarize("caught up (s)", [r["caught_up_s"] for r in rows if r["caught_up_s"]])
    write_results("sink_restart_recovery", rows, final)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("which", choices=["producer", "lag", "export", "datanode", "sink-recovery", "all"])
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--rate", type=float, default=100, help="bars/s for the live generator")
    ap.add_argument("--seconds", type=float, default=60, help="length of one lag run")
    ap.add_argument("--outage", type=float, default=10, help="seconds the sink stays down")
    ap.add_argument("--final", action="store_true", help="write to bench/results instead of bench/scratch")
    args = ap.parse_args()

    todo = ["producer", "lag", "export", "datanode", "sink-recovery"] if args.which == "all" else [args.which]
    for name in todo:
        say(f"== {name}")
        if name == "producer":
            producer(args.reps, args.final)
        elif name == "lag":
            lag(args.reps, args.rate, args.seconds, args.final)
        elif name == "export":
            export(args.reps, args.final)
        elif name == "datanode":
            datanode(args.reps, args.final)
        else:
            sink_recovery(args.reps, args.outage, args.rate, args.final)
    return 0


if __name__ == "__main__":
    sys.exit(main())

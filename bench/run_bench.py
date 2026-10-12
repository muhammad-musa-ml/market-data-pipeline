"""Benchmark runner. Runs against a stack that is already up.

    python bench/run_bench.py producer --reps 5
    python bench/run_bench.py lag --reps 5 --rate 100 --seconds 60
    python bench/run_bench.py export --reps 5
    python bench/run_bench.py datanode --reps 5
    python bench/run_bench.py sink-recovery --reps 5 --outage 10
    python bench/run_bench.py all

Stage 2 (needs the analytics profile up, and the sink caught up so market.bars can be built):

    python bench/run_bench.py jobs --reps 5        # compaction, the three-API question, serving extract
    python bench/run_bench.py bucketing --reps 5 --inflate 20   # plans and timings, bucketed vs plain
    python bench/run_bench.py caching --reps 5 --inflate 20
    python bench/run_bench.py ml --reps 5          # metrics vs baselines, depth sweep
    python bench/run_bench.py text2sql --reps 3    # model calls; costs money with the vertex backend
    python bench/run_bench.py spark                # all of the above except text2sql

Stage 3 (needs the serving profile up and prod loaded; see scripts/serving_test.py):

    python bench/run_bench.py serving-latency --reps 5 --n 2000   # request latency at ONE, QUORUM, ALL
    python bench/run_bench.py serving-load --reps 5               # loader rows/s, Cassandra and MySQL
    python bench/run_bench.py serving-api --reps 5 --n 1000       # REST latency over both backends
    python bench/run_bench.py serving-matrix --reps 5             # what works with 0, 1, 2 nodes down
    python bench/run_bench.py serving                             # all four

Output goes to bench/scratch/ unless --final is given, which writes to
bench/results/. Each run also writes <name>.meta.json with the machine, the
commit and the exact command, so a number can be traced back.
"""
import argparse
import csv
import datetime as dt
import json
import os
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
        "uncommitted_changes": bool(subprocess.run(["git", "status", "--porcelain"], cwd=ROOT,
                                                   capture_output=True, text=True).stdout.strip()),
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


def spark_job(job: str, *args: str, timeout: float = 3600) -> dict:
    """Run one analytics job in the driver container and return its RESULT line."""
    passthrough = [x for k in ("OLLAMA_MODEL", "OLLAMA_HOST", "MDP_SQL_MODEL") if os.environ.get(k)
                   for x in ("-e", f"{k}={os.environ[k]}")]
    res = compose("exec", "-T", *passthrough, "driver", "spark-submit", "/app/src/mdp/analytics/jobs.py",
                  job, *args, timeout=timeout, check=False)
    lines = [ln for ln in res.stdout.splitlines() if ln.startswith("RESULT ")]
    if res.returncode != 0 or not lines:
        raise RuntimeError(f"spark job {job} failed (exit {res.returncode}):\n{res.stdout[-3000:]}\n{res.stderr[-3000:]}")
    return json.loads(lines[-1][len("RESULT "):])


def spark_meta() -> dict:
    rows = int(stack.sql("SELECT COUNT(*) FROM bars"))
    return {"source_rows": rows, "spark": "apache/spark:4.2.0, 1 master, 2 workers x 2 cores, "
            "executor memory 1g + 256m overhead, driver 768m, shuffle partitions 8, 8 buckets"}


def jobs(reps: int, final: bool) -> None:
    rows = []
    for rep in range(1, reps + 1):
        for name in ("compact", "question", "serving"):
            doc = spark_job(name)
            rows.append({"rep": rep, "job": name, "wall_seconds": doc["wall_seconds"],
                         "rows": doc.get("rows") or doc.get("source_rows") or "",
                         "table_files": doc.get("table_files", "")})
            if name == "question":
                rows += [{"rep": rep, "job": f"question-{api}", "wall_seconds": doc[f"{api}_seconds"],
                          "rows": "", "table_files": ""} for api in ("rdd", "dataframe", "sql")]
                if not doc["identical"]:
                    raise RuntimeError("the three APIs disagreed")
            say(f"jobs rep {rep}: {name} {doc['wall_seconds']}s")
    for name in sorted({r["job"] for r in rows}):
        summarize(f"{name} wall seconds", [r["wall_seconds"] for r in rows if r["job"] == name])
    write_results("analytics_job_runtime", rows, final, spark_meta())


def bucketing(reps: int, final: bool, inflate: int, inner: int = 3) -> None:
    rows, plans = [], {}
    for rep in range(1, reps + 1):
        doc = spark_job("bucketing", "--reps", str(inner), "--inflate", str(inflate))
        for variant, qs in doc["variants"].items():
            for query, d in qs.items():
                plans.setdefault((variant, query), d["plan"])
                for k, sec in enumerate(d["seconds"], 1):
                    rows.append({"rep": rep, "run": k, "rows": doc["rows"], "layout": variant, "query": query,
                                 "exchanges": d["exchanges"], "bucketed_scan": d["bucketed_scan"],
                                 "seconds": sec})
        say(f"bucketing rep {rep} done")
    for layout in ("bucketed", "flat"):
        for query in ("group_by", "join"):
            summarize(f"{layout} {query} seconds",
                      [r["seconds"] for r in rows if r["layout"] == layout and r["query"] == query])
    d = out_dir(final)
    with open(d / "bucketing_plans.txt", "w", newline="\n") as f:
        for (variant, query), plan in plans.items():
            f.write(f"=== {variant} / {query}\n{plan}\n\n")
    write_results("bucketing", rows, final, spark_meta())


def caching(reps: int, final: bool, inflate: int) -> None:
    rows = []
    for rep in range(1, reps + 1):
        doc = spark_job("caching", "--repeats", "3", "--inflate", str(inflate))
        for i, sec in enumerate(doc["uncached_seconds"], 1):
            rows.append({"rep": rep, "phase": "uncached", "run": i, "seconds": sec})
        rows.append({"rep": rep, "phase": "cache_fill", "run": 1, "seconds": doc["cache_fill_seconds"]})
        for i, sec in enumerate(doc["cached_seconds"], 1):
            rows.append({"rep": rep, "phase": "cached", "run": i, "seconds": sec})
        say(f"caching rep {rep}: uncached {doc['uncached_seconds']} cached {doc['cached_seconds']}")
    for phase in ("uncached", "cache_fill", "cached"):
        summarize(f"{phase} seconds", [r["seconds"] for r in rows if r["phase"] == phase])
    write_results("caching", rows, final, spark_meta())


def ml(reps: int, final: bool) -> None:
    metrics, sweep = [], []
    for rep in range(1, reps + 1):
        doc = spark_job("ml")
        for name, m in doc["baselines"].items():
            metrics.append({"rep": rep, "model": f"baseline:{name}", "depth": "", "test_r2": m["r2"],
                            "test_rmse": m["rmse"]})
        metrics.append({"rep": rep, "model": "tree:validation-chosen", "depth": doc["chosen_depth"],
                        "test_r2": doc["chosen"]["test_r2"], "test_rmse": ""})
        if doc.get("depth_5"):
            metrics.append({"rep": rep, "model": "tree:depth-5", "depth": 5,
                            "test_r2": doc["depth_5"]["test_r2"], "test_rmse": ""})
        for r in doc["sweep"]:
            sweep.append({"rep": rep, **{k: r[k] for k in ("depth", "train_r2", "test_r2", "fit_seconds")}})
        say(f"ml rep {rep}: depth {doc['chosen_depth']} test R2 {doc['chosen']['test_r2']:.3f}, "
            f"persistence {doc['baselines']['persistence']['r2']:.3f}, {doc['seconds']}s")
    summarize("chosen-depth test R2", [r["test_r2"] for r in metrics if r["model"] == "tree:validation-chosen"])
    extra = {**spark_meta(), "split": doc["split"], "rows": doc["rows"], "importances": doc["importances"]}
    write_results("ml_metrics", metrics, final, extra)
    write_results("ml_depth_sweep", sweep, final, extra)


def text2sql(reps: int, backend: str, final: bool) -> None:
    rows, runs = [], []
    for rep in range(1, reps + 1):
        doc = spark_job("text2sql", "--backend", backend, timeout=3600)
        runs.append(doc)
        for c in doc["cases"]:
            rows.append({"rep": rep, "id": c["id"], "backend": doc["backend"], "correct": c["correct"],
                         "first_try": c["first_try"], "attempts": c["attempts"],
                         "model_seconds": c.get("model_seconds", ""), "seconds": c["seconds"]})
        say(f"text2sql rep {rep}: {doc['correct']}/{doc['questions']} correct, "
            f"first try {doc['first_try_accuracy']:.2f}, median {doc['median_seconds']}s")
    d = out_dir(final)
    with open(d / "text2sql_runs.jsonl", "w", newline="\n") as f:
        for doc in runs:
            f.write(json.dumps(doc) + "\n")
    summarize("accuracy", [r["accuracy"] for r in runs])
    summarize("median seconds per question", [r["median_seconds"] for r in runs])
    write_results("text2sql_eval", rows, final, {**spark_meta(), "backend": runs[0]["backend"],
                                                 "questions": runs[0]["questions"]})


def serving_tools(*cmd: str, timeout: float = 3600) -> subprocess.CompletedProcess:
    return compose("--profile", "serving-tools", "run", "--rm", "-T", "serving-tools", *cmd,
                   timeout=timeout, check=False)


def serving_meta() -> dict:
    return {"cluster": "cassandra:5.0.9, 3 nodes, SimpleStrategy RF 3, heap 512M/128M new, 1280m container limit, "
                       "2 vCPU visible to each JVM", "mysql": "mysql:8.4, buffer pool 128M, 512m limit",
            "api": "gunicorn 2 workers x 4 threads", "driver": "cassandra-driver 3.30.1, protocol v5"}


def json_lines(text: str) -> list[dict]:
    return [json.loads(ln) for ln in text.splitlines() if ln.startswith("{")]


def serving_latency(reps: int, final: bool, n: int, warmup: int) -> None:
    rows = []
    for rep in range(1, reps + 1):
        res = serving_tools("python", "-m", "mdp.bench.serving_latency", "--n", str(n), "--warmup", str(warmup),
                            "--seed", str(rep))
        docs = json_lines(res.stdout)
        if res.returncode != 0 or not docs:
            raise RuntimeError(f"latency run failed (exit {res.returncode}):\n{res.stdout[-2000:]}\n{res.stderr[-2000:]}")
        rows += [{"rep": rep, **d} for d in docs]
        say(f"latency rep {rep}: " + ", ".join(f"{d['consistency']}/{d['op']} p50 {d.get('p50_ms', 'n/a')}"
                                              for d in docs if d["op"] == "write"))
    for level in ("ONE", "QUORUM", "ALL"):
        for op in ("write", "read_row", "read_30_days"):
            summarize(f"{level} {op} p50 ms", [r["p50_ms"] for r in rows if r["consistency"] == level
                                               and r["op"] == op and "p50_ms" in r])
    write_results("serving_latency", rows, final, {**serving_meta(), "requests_per_level": n, "warmup": warmup})


def serving_load(reps: int, final: bool, concurrencies=(16, 64, 256)) -> None:
    rows = []
    plan = [("cassandra", c) for c in concurrencies] + [("mysql", 64)]
    for rep in range(1, reps + 1):
        for backend, conc in plan:
            # the same number of fresh rows each time: empty the test database first
            reset = serving_tools("python", "-m", "mdp.serving.init_schema", "--reset-test", "--only", backend,
                                  timeout=600)
            if reset.returncode != 0:
                raise RuntimeError(f"reset failed:\n{reset.stdout}\n{reset.stderr}")
            res = serving_tools("python", "-m", "mdp.serving.loader", "--backend", backend, "--db", "test",
                                "--concurrency", str(conc), "--skip-verify", "--json")
            docs = json_lines(res.stdout)
            if res.returncode != 0 or not docs:
                raise RuntimeError(f"loader failed (exit {res.returncode}):\n{res.stdout[-2000:]}\n{res.stderr[-2000:]}")
            d = docs[-1]
            rows.append({"rep": rep, "backend": backend, "concurrency": conc, "rows": d["rows"],
                         "write_seconds": d["write_seconds"], "wall_seconds": d["wall_seconds"],
                         "rows_per_s": d["rows_per_s"]})
            say(f"load rep {rep}: {backend} x{conc}: {d['rows_per_s']:.0f} rows/s")
    for backend, conc in plan:
        summarize(f"{backend} x{conc} rows/s", [r["rows_per_s"] for r in rows
                                                if r["backend"] == backend and r["concurrency"] == conc])
    write_results("loader_throughput", rows, final, serving_meta())


def percentile(values: list[float], q: float) -> float:
    s = sorted(values)
    return s[max(0, -(-q * len(s) // 100) - 1)]


def serving_api(reps: int, final: bool, n: int) -> None:
    import requests

    urls = {"mysql": "http://127.0.0.1:8400", "cassandra": "http://127.0.0.1:8401"}
    rows = []
    for rep in range(1, reps + 1):
        for backend, url in urls.items():
            http = requests.Session()
            tick = "Y" + f"{int(time.time()) % 10**7:07d}"
            http.post(f"{url}/test/api/companies/{tick}", json={"name": "Bench", "sector": "Bench",
                                                                   "exchange": "TEST"}).raise_for_status()
            prod = http.get(f"{url}/prod/api/companies").json()
            if not prod:
                raise RuntimeError("prod is empty: load the extract first")
            read_tick = prod[0]["ticker"]

            def put(i):
                day = dt.date(2001, 1, 1) + dt.timedelta(days=i)
                return http.put(f"{url}/test/api/companies/{tick}/records/{day.isoformat()}",
                                json={"open": 10, "high": 11, "low": 9, "close": 10.5, "volume": i})

            ops = {
                "put_record": put,
                "get_company": lambda i: http.get(f"{url}/prod/api/companies/{read_tick}"),
                "get_30_days": lambda i: http.get(f"{url}/prod/api/companies/{read_tick}/records",
                                                  params={"start": "2023-11-01", "end": "2023-12-22"}),
                "get_monthly_all": lambda i: http.get(f"{url}/prod/api/companies/{read_tick}/records/monthly"),
            }
            for op, fn in ops.items():
                for i in range(20):  # warm-up
                    fn(i)
                ms = []
                for i in range(n):
                    t0 = time.perf_counter()
                    r = fn(20 + i)
                    ms.append((time.perf_counter() - t0) * 1000)
                    if r.status_code >= 300:
                        raise RuntimeError(f"{backend} {op}: {r.status_code} {r.text[:200]}")
                rows.append({"rep": rep, "backend": backend, "op": op, "requests": n,
                             "p50_ms": round(percentile(ms, 50), 3), "p95_ms": round(percentile(ms, 95), 3),
                             "p99_ms": round(percentile(ms, 99), 3), "max_ms": round(max(ms), 3)})
            say(f"api rep {rep}: {backend} done")
    for backend in urls:
        for op in ("put_record", "get_company", "get_30_days", "get_monthly_all"):
            summarize(f"{backend} {op} p50 ms", [r["p50_ms"] for r in rows if r["backend"] == backend and r["op"] == op])
    write_results("serving_api_latency", rows, final,
                  {**serving_meta(), "client": "requests from the host, one connection, sequential"})


def serving_matrix(reps: int, final: bool, attempts: int, mode: str) -> None:
    import serving_faults

    rows = []
    for rep in range(1, reps + 1):
        got = serving_faults.matrix(mode, attempts)
        rows += [{"rep": rep, **r} for r in got]
        (out_dir(final) / "availability_matrix.md").write_text(serving_faults.render(got) + "\n", newline="\n")
    write_results("availability_matrix", rows, final, {**serving_meta(), "kill_mode": mode, "attempts": attempts})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("which", choices=["producer", "lag", "export", "datanode", "sink-recovery", "all",
                                      "jobs", "bucketing", "caching", "ml", "text2sql", "spark",
                                      "serving-latency", "serving-load", "serving-api", "serving-matrix", "serving"])
    ap.add_argument("--backend", choices=["vertex", "ollama"], default="vertex", help="text2sql only")
    ap.add_argument("--inflate", type=int, default=1,
                    help="bucketing and caching: stack this many shifted copies of the bars first")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--n", type=int, default=2000, help="serving-latency and serving-api: requests per measurement")
    ap.add_argument("--warmup", type=int, default=200, help="serving-latency: requests discarded first")
    ap.add_argument("--attempts", type=int, default=3, help="serving-matrix: calls per operation per state")
    ap.add_argument("--kill-mode", choices=["crash", "stop"], default="crash", help="serving-matrix")
    ap.add_argument("--rate", type=float, default=100, help="bars/s for the live generator")
    ap.add_argument("--seconds", type=float, default=60, help="length of one lag run")
    ap.add_argument("--outage", type=float, default=10, help="seconds the sink stays down")
    ap.add_argument("--final", action="store_true", help="write to bench/results instead of bench/scratch")
    args = ap.parse_args()

    todo = {"all": ["producer", "lag", "export", "datanode", "sink-recovery"],
            "spark": ["jobs", "bucketing", "caching", "ml"],
            "serving": ["serving-latency", "serving-load", "serving-api", "serving-matrix"]}.get(args.which, [args.which])
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
        elif name == "jobs":
            jobs(args.reps, args.final)
        elif name == "bucketing":
            bucketing(args.reps, args.final, args.inflate)
        elif name == "caching":
            caching(args.reps, args.final, args.inflate)
        elif name == "ml":
            ml(args.reps, args.final)
        elif name == "text2sql":
            text2sql(args.reps, args.backend, args.final)
        elif name == "serving-latency":
            serving_latency(args.reps, args.final, args.n, args.warmup)
        elif name == "serving-load":
            serving_load(args.reps, args.final)
        elif name == "serving-api":
            serving_api(args.reps, args.final, args.n)
        elif name == "serving-matrix":
            serving_matrix(args.reps, args.final, args.attempts, args.kill_mode)
        else:
            sink_recovery(args.reps, args.outage, args.rate, args.final)
    return 0


if __name__ == "__main__":
    sys.exit(main())

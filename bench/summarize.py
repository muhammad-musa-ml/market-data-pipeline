"""Write bench/results/SUMMARY.md from the CSVs in bench/results.

    python bench/summarize.py [--scratch]

Every figure in the file is the median over the repetitions with the minimum and maximum next to
it; nothing is typed by hand. A CSV that is missing is skipped with a note.
"""
import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def fmt(x: float) -> str:
    return f"{x:,.0f}" if abs(x) >= 1000 else f"{x:.4g}" if abs(x) < 10 else f"{x:.1f}"


def stat(values: list[float]) -> str:
    return f"{fmt(statistics.median(values))} ({fmt(min(values))} to {fmt(max(values))}), n={len(values)}"


def read(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def table(rows, group, values, labels=None) -> list[str]:
    """One line per group, one column per value, each cell median (min to max), n."""
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        key = tuple(r.get(g, "") for g in group)
        for v in values:
            x = num(r.get(v))
            if x is not None:
                groups[key][v].append(x)
    head = list(group) + [(labels or {}).get(v, v) for v in values]
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for key, cols in groups.items():
        cells = [stat(cols[v]) if cols[v] else "" for v in values]
        out.append("| " + " | ".join(list(key) + cells) + " |")
    return out


def per_rep(rows, rep_cols, fn):
    """Reduce the rows of each repetition (and group) to one row with fn(rows) -> dict."""
    buckets = defaultdict(list)
    for r in rows:
        buckets[tuple(r[c] for c in rep_cols)].append(r)
    return [{**dict(zip(rep_cols, k)), **fn(rs)} for k, rs in buckets.items()]


def accuracy(rs):
    ok = [r["correct"] == "True" for r in rs]
    first = [r["first_try"] == "True" for r in rs]
    secs = sorted(float(r["seconds"]) for r in rs)
    return {"questions": len(rs), "accuracy": sum(ok) / len(rs), "first_try_accuracy": sum(first) / len(rs),
            "median_seconds_per_question": statistics.median(secs), "max_seconds": secs[-1]}


def per_question(rows):
    out = ["| question | correct in | first try in | seconds |", "|---|---|---|---|"]
    ids = list(dict.fromkeys(r["id"] for r in rows))
    for i in ids:
        rs = [r for r in rows if r["id"] == i]
        secs = [float(r["seconds"]) for r in rs]
        out.append(f"| {i} | {sum(r['correct'] == 'True' for r in rs)} of {len(rs)} | "
                   f"{sum(r['first_try'] == 'True' for r in rs)} of {len(rs)} | {stat(secs)} |")
    return out


def ml_metrics(rows):
    out = table(rows, ["model"], ["test_r2", "test_rmse"])
    chosen = [r for r in rows if r["model"] == "tree:validation-chosen"]
    out += ["", "Depth chosen on the validation block, per repetition: "
            + ", ".join(f"{r['depth']} (rep {r['rep']})" for r in chosen)]
    return out


def drills(rows):
    out = ["| drill | runs | bars inserted (per run) | rows on HDFS equal rows in MySQL | duplicates | gaps | value mismatches |",
           "|---|---|---|---|---|---|---|"]
    for name in sorted({r["drill"] for r in rows}):
        rs = [r for r in rows if r["drill"] == name]
        same = sum(r["rows_on_hdfs"] == r["expected_rows"] for r in rs)
        out.append(f"| {name} | {len(rs)} | {', '.join(r['bars_inserted'] for r in rs)} | {same} of {len(rs)} | "
                   f"{', '.join(r['duplicates'] for r in rs)} | {', '.join(r['gaps'] for r in rs)} | "
                   f"{', '.join(r['value_mismatches'] for r in rs)} |")
    return out


def matrix(rows):
    cols = sorted({r["nodes_down"] for r in rows})
    out = ["| operation | consistency | " + " | ".join(f"{c} down" for c in cols) + " |",
           "|---|---|" + "---|" * len(cols)]
    ops = list(dict.fromkeys((r["surface"], r["op"], r["consistency"]) for r in rows))
    for s, op, level in ops:
        cells = []
        for c in cols:
            rs = [r for r in rows if r["nodes_down"] == c and (r["surface"], r["op"]) == (s, op)]
            ok = sum(int(r["succeeded"]) for r in rs)
            tot = sum(int(r["attempts"]) for r in rs)
            codes = sorted({r["outcome"] for r in rs if r["outcome"] != "OK"})
            cells.append(f"{ok}/{tot} ok" + (f" ({', '.join(codes)})" if codes else ""))
        out.append(f"| {s} {op} | {level} | " + " | ".join(cells) + " |")
    return out


SECTIONS = [
    ("kafka_init_cold", "kafka-init on a cold cluster (exit code per fresh start)",
     lambda rows: ["| start | exit code | first try ok |", "|---|---|---|"]
     + [f"| {r['rep']} | {r['exit_code']} | {r['first_try_ok']} |" for r in rows]),
    ("producer_throughput", "Stage 1: producer throughput", lambda rows: table(rows, [], ["msgs_per_s"])),
    ("end_to_end_lag", "Stage 1: MySQL insert to trusted HDFS file, seconds",
     lambda rows: table(rows, ["rate_bars_per_s"], ["p50_s", "p95_s", "p99_s", "max_s"])),
    ("export_rate", "Stage 1: backfill export", None),
    ("datanode_recovery", "Stage 1: SymbolSummary after a DataNode kill",
     lambda rows: table(rows, [], ["recovery_s", "slowest_call_ms", "cache_files_lost", "errors"])),
    ("sink_restart_recovery", "Stage 1: sink restart", None),
    ("fault_drills", "Stage 1: fault drills", drills),
    ("analytics_job_runtime", "Stage 2: job wall time, seconds", lambda rows: table(rows, ["job"], ["wall_seconds"])),
    ("bucketing", "Stage 2: bucketed against plain files, seconds (3 runs per repetition)",
     lambda rows: table(rows, ["layout", "query", "exchanges"], ["seconds"])),
    ("caching", "Stage 2: cached against uncached aggregation, seconds",
     lambda rows: table(rows, ["phase"], ["seconds"])),
    ("ml_metrics", "Stage 2: decision tree and baselines on the held-out period", ml_metrics),
    ("ml_depth_sweep", "Stage 2: depth sweep",
     lambda rows: table(rows, ["depth"], ["train_r2", "test_r2", "fit_seconds"])),
    *[(f"text2sql_eval_{b}", f"Stage 2: text-to-SQL on {b}, 28 questions per run",
       lambda rows: table(per_rep(rows, ["backend", "rep"], accuracy), ["backend"],
                          ["accuracy", "first_try_accuracy", "median_seconds_per_question", "max_seconds"]))
      for b in ("vertex", "ollama")],
    ("text2sql_eval_vertex", "Stage 2: text-to-SQL on vertex, per question (seconds are model plus validation plus execution)",
     lambda rows: per_question(rows)),
    ("serving_latency", "Stage 3: Cassandra request latency, milliseconds, all nodes up",
     lambda rows: table(rows, ["consistency", "op"], ["p50_ms", "p95_ms", "p99_ms"])),
    ("serving_api_latency", "Stage 3: REST latency from the host, milliseconds",
     lambda rows: table(rows, ["backend", "op"], ["p50_ms", "p95_ms", "p99_ms"])),
    ("loader_throughput", "Stage 3: loader rows per second",
     lambda rows: table(rows, ["backend", "concurrency", "batch_size"], ["rows_per_s"])),
    ("availability_matrix", "Stage 3: availability with 0, 1 and 2 Cassandra nodes down (attempts summed over runs)",
     matrix),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scratch", action="store_true")
    args = ap.parse_args()
    d = ROOT / "bench" / ("scratch" if args.scratch else "results")
    out = ["# Results summary", "",
           "Generated by bench/summarize.py from the CSVs in this folder. Cells are median (min to max), n = repetitions.",
           ""]
    for name, title, fn in SECTIONS:
        path = d / f"{name}.csv"
        out += [f"## {title}", ""]
        if not path.exists():
            out += [f"{name}.csv is not here.", ""]
            continue
        rows = read(path)
        out += (fn(rows) if fn else table(rows, [], [c for c in rows[0] if c not in ("rep", "run")
                                                      and num(rows[0][c]) is not None])) + [""]
    (d / "SUMMARY.md").write_text("\n".join(out), newline="\n")
    print(f"wrote {d / 'SUMMARY.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

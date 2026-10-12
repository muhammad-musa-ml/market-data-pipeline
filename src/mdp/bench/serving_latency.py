"""Single-request latency of Cassandra reads and writes at each consistency level.

    python -m mdp.bench.serving_latency --n 2000 --warmup 200

One request at a time from one thread, so the numbers are latency, not throughput. Writes go to
a fresh ticker in the test keyspace; reads go to loaded tickers in prod. Prints one JSON line per
(consistency, operation) with the percentiles in milliseconds and the number of failed requests.
A level that cannot be met (ALL with a node down) shows up as errors, not as a crash.
"""
import argparse
import datetime as dt
import json
import math
import random
import sys
import time
import uuid

from cassandra import ConsistencyLevel, DriverException
from cassandra.cluster import NoHostAvailable

from mdp.serving import cluster
from mdp.serving.settings import ServingSettings

LEVELS = ["ONE", "QUORUM", "ALL"]
START = dt.date(2000, 1, 3)


def percentile(sorted_ms: list[float], q: float) -> float:
    """Nearest-rank percentile of an ascending list."""
    return sorted_ms[max(0, math.ceil(q / 100 * len(sorted_ms)) - 1)]


def summarize(level: str, op: str, ms: list[float], errors: int) -> dict:
    s = sorted(ms)
    row = {"consistency": level, "op": op, "requests": len(ms) + errors, "errors": errors}
    if s:
        row.update(p50_ms=round(percentile(s, 50), 3), p95_ms=round(percentile(s, 95), 3),
                   p99_ms=round(percentile(s, 99), 3), max_ms=round(s[-1], 3), mean_ms=round(sum(s) / len(s), 3))
    return row


def timed(session, stmt, params, ms: list[float]) -> int:
    t0 = time.perf_counter()
    try:
        session.execute(stmt, params)
    except (DriverException, NoHostAvailable):
        return 1
    ms.append((time.perf_counter() - t0) * 1000)
    return 0


def run(n: int, warmup: int, seed: int) -> list[dict]:
    session = cluster.connect(ServingSettings.from_env(), wait=60, consistency=ConsistencyLevel.QUORUM)
    rng = random.Random(seed)
    ticker = "B" + uuid.uuid4().hex[:7].upper()
    session.execute("INSERT INTO test.daily_bars (ticker, name, sector, exchange) VALUES (%s, 'Bench', 'Bench', 'TEST')",
                    (ticker,))
    read_tickers = [r.ticker for r in session.execute("SELECT DISTINCT ticker FROM prod.daily_bars")][:20]
    if not read_tickers:
        raise SystemExit("prod.daily_bars is empty: load the extract first")
    write = session.prepare("INSERT INTO test.daily_bars (ticker, trade_date, open, high, low, close, volume) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)")
    point = session.prepare("SELECT trade_date, open, high, low, close, volume FROM prod.daily_bars "
                            "WHERE ticker = ? AND trade_date = ?")
    window = session.prepare("SELECT trade_date, open, high, low, close, volume FROM prod.daily_bars "
                             "WHERE ticker = ? AND trade_date >= ? AND trade_date <= ?")
    # a date every loaded ticker has: the last day of the history
    last = session.execute("SELECT max(trade_date) AS d FROM prod.daily_bars WHERE ticker = %s",
                           (read_tickers[0],)).one().d.date()
    counter = 0
    rows = []
    for level in LEVELS:
        cl = ConsistencyLevel.name_to_value[level]
        for stmt in (write, point, window):
            stmt.consistency_level = cl
        w_ms: list[float] = []
        p_ms: list[float] = []
        r_ms: list[float] = []
        w_err = p_err = r_err = 0
        for i in range(warmup + n):
            measured = i >= warmup
            counter += 1
            day = START + dt.timedelta(days=counter)
            t = rng.choice(read_tickers)
            sink_w, sink_p, sink_r = (w_ms, p_ms, r_ms) if measured else ([], [], [])
            e = timed(session, write, (ticker, day, 10.0, 11.0, 9.0, 10.5, 1000), sink_w)
            w_err += e if measured else 0
            e = timed(session, point, (t, last), sink_p)
            p_err += e if measured else 0
            e = timed(session, window, (t, last - dt.timedelta(days=30), last), sink_r)
            r_err += e if measured else 0
        rows += [summarize(level, "write", w_ms, w_err), summarize(level, "read_row", p_ms, p_err),
                 summarize(level, "read_30_days", r_ms, r_err)]
    session.cluster.shutdown()
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mdp.bench.serving_latency")
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--warmup", type=int, default=200)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args(argv)
    for row in run(args.n, args.warmup, args.seed):
        print(json.dumps(row), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

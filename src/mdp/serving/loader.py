"""Load the stage 2 serving extract from HDFS into a backend.

    python -m mdp.serving.loader [--backend cassandra|mysql] [--db prod|test] [--concurrency 64] [--batch-size 50]

Reads /market/serving/_manifest.json, refuses to continue if the daily_bars files do not add up
to the row count it names, writes companies and then bars as upserts (so running it again
changes nothing), and finally checks what it wrote against the extract's own rollups.
"""
import argparse
import io
import json
import math
import sys
import time

import pyarrow.parquet as pq

from mdp import layout
from mdp.config import Settings
from mdp.hdfs import WebHdfs
from mdp.serving.backends import BACKENDS, open_store
from mdp.serving.models import Bar, Company, price
from mdp.serving.monthly import monthly
from mdp.serving.store import Store, StoreError

SERVING = f"{layout.ROOT}/serving"
MANIFEST = f"{SERVING}/_manifest.json"
WRITE_RETRIES = 3


class ExtractIncomplete(RuntimeError):
    pass


def parquet_files(hdfs, dataset: str) -> list[str]:
    names = sorted(n for n in hdfs.listdir(f"{SERVING}/{dataset}") if n.endswith(".parquet"))
    if not names:
        raise ExtractIncomplete(f"no parquet files under {SERVING}/{dataset}")
    return [f"{SERVING}/{dataset}/{n}" for n in names]


def read_parquet(hdfs, path: str):
    return pq.read_table(io.BytesIO(hdfs.read(path)))


def read_manifest(hdfs) -> dict:
    if not hdfs.exists(MANIFEST):
        raise ExtractIncomplete(f"{MANIFEST} is missing: the serving job did not finish")
    return json.loads(hdfs.read(MANIFEST))


def companies_from(table) -> list[Company]:
    seen: dict[str, Company] = {}
    cols = table.select(["ticker", "name", "sector", "exchange"]).to_pydict()
    for row in zip(cols["ticker"], cols["name"], cols["sector"], cols["exchange"]):
        seen.setdefault(row[0], Company(*row))
    return list(seen.values())


def bars_from(table) -> list[tuple[str, Bar]]:
    c = table.select(["ticker", "trade_date", "open", "high", "low", "close", "volume"]).to_pydict()
    return [(t, Bar(d, price(o), price(h), price(lo), price(cl), v))
            for t, d, o, h, lo, cl, v in zip(c["ticker"], c["trade_date"], c["open"], c["high"], c["low"],
                                              c["close"], c["volume"])]


def write_with_retry(fn, *args) -> None:
    """The writes are upserts, so repeating one that timed out is harmless."""
    for attempt in range(WRITE_RETRIES):
        try:
            fn(*args)
            return
        except StoreError:
            if attempt == WRITE_RETRIES - 1:
                raise
            time.sleep(2 * (attempt + 1))


def load(store: Store, hdfs, concurrency: int = 64, log=print, batch_size: int = 50) -> dict:
    manifest = read_manifest(hdfs)
    files = parquet_files(hdfs, "daily_bars")
    rows = tickers = 0
    t0 = time.monotonic()
    write_seconds = 0.0
    for path in files:
        table = read_parquet(hdfs, path)
        companies = companies_from(table)
        bars = bars_from(table)
        w0 = time.monotonic()
        write_with_retry(store.upsert_companies, companies)
        write_with_retry(store.upsert_bars, bars, concurrency, batch_size)
        write_seconds += time.monotonic() - w0
        rows += len(bars)
        tickers += len(companies)
        log(f"loader: {path.rsplit('/', 1)[-1]} {len(bars)} rows, {len(companies)} tickers")
    expected = manifest["datasets"]["daily_bars"]["rows"]
    if rows != expected:
        raise ExtractIncomplete(f"manifest says {expected} daily_bars rows, the files hold {rows}")
    return {"rows": rows, "tickers": tickers, "files": len(files), "write_seconds": round(write_seconds, 3),
            "wall_seconds": round(time.monotonic() - t0, 3),
            "rows_per_s": round(rows / write_seconds, 1), "manifest_built_at": manifest["built_at"]}


def verify(store: Store, hdfs) -> list[str]:
    """Compare every rollup and every month of the extract with what the backend now serves."""
    problems: list[str] = []
    rollups = {r["ticker"]: r for f in parquet_files(hdfs, "symbol_rollups")
               for r in read_parquet(hdfs, f).to_pylist()}
    months: dict[str, dict[str, dict]] = {}
    for f in parquet_files(hdfs, "monthly_bars"):
        for r in read_parquet(hdfs, f).to_pylist():
            months.setdefault(r["ticker"], {})[r["month"]] = r
    for tick, want in sorted(rollups.items()):
        got = store.bars(tick)
        if len(got) != want["n_bars"]:
            problems.append(f"{tick}: {len(got)} bars served, rollup says {want['n_bars']}")
            continue
        if got[0].trade_date != want["first_date"] or got[-1].trade_date != want["last_date"]:
            problems.append(f"{tick}: dates {got[0].trade_date}..{got[-1].trade_date} differ from the rollup")
        mean = sum(b.close for b in got) / len(got)
        if not math.isclose(mean, want["avg_close"], rel_tol=1e-9):
            problems.append(f"{tick}: mean close {mean} differs from the rollup's {want['avg_close']}")
        served = {m["month"]: m for m in monthly(got)}
        stage2 = months.get(tick, {})
        if served.keys() != stage2.keys():
            problems.append(f"{tick}: months differ ({len(served)} served, {len(stage2)} in the extract)")
            continue
        for month, m in stage2.items():
            s = served[month]
            if s["trading_days"] != m["trading_days"] or not math.isclose(s["avg_close"], m["avg_close"], abs_tol=6e-5):
                problems.append(f"{tick} {month}: avg_close {s['avg_close']} vs extract {m['avg_close']}")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mdp.serving.loader")
    ap.add_argument("--backend", choices=BACKENDS, default="cassandra")
    ap.add_argument("--db", choices=["prod", "test"], default="prod")
    ap.add_argument("--concurrency", type=int, default=64, help="requests in flight")
    ap.add_argument("--batch-size", type=int, default=50,
                    help="Cassandra: rows per unlogged single-partition batch (1 = one request per row)")
    ap.add_argument("--skip-verify", action="store_true")
    ap.add_argument("--json", action="store_true", help="print the result as one JSON line")
    args = ap.parse_args(argv)

    cfg = Settings.from_env()
    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    hdfs.wait_ready(cfg.hdfs_min_datanodes)
    store = open_store(args.backend, args.db)
    log = (lambda *_: None) if args.json else print
    result = load(store, hdfs, args.concurrency, log, args.batch_size)
    result.update(backend=args.backend, db=args.db, concurrency=args.concurrency, batch_size=args.batch_size)
    if not args.skip_verify:
        problems = verify(store, hdfs)
        result["verify_problems"] = problems
        for p in problems[:20]:
            print("verify:", p, file=sys.stderr)
    print(json.dumps(result) if args.json else
          f"loaded {result['rows']} rows ({result['tickers']} tickers) into {args.backend}/{args.db} at "
          f"{result['rows_per_s']:.0f} rows/s; "
          + ("verify skipped" if args.skip_verify else f"{len(result['verify_problems'])} verify problems"))
    return 1 if result.get("verify_problems") else 0


if __name__ == "__main__":
    sys.exit(main())

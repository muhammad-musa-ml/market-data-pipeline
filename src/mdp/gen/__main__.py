import argparse
import os
import sys

from mdp import db
from mdp.config import Settings
from mdp.gen import load, market
from mdp.hdfs import WebHdfs


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m mdp.gen")
    p.add_argument("--seed", type=int, default=int(os.environ.get("GEN_SEED", 7)))
    p.add_argument("--symbols", type=int, default=int(os.environ.get("GEN_SYMBOLS", 50)))
    sub = p.add_subparsers(dest="cmd", required=True)

    h = sub.add_parser("history", help="load trading days up to --days (idempotent)")
    h.add_argument("--days", type=int, default=int(os.environ.get("GEN_HISTORY_DAYS", 2600)))

    live = sub.add_parser("live", help="keep appending new bars")
    live.add_argument("--rate", type=float, default=float(os.environ.get("GEN_RATE", 100)),
                      help="bars per second")
    live.add_argument("--max-bars", type=int, default=None)

    sub.add_parser("export-symbols", help="write the symbols table to HDFS")

    args = p.parse_args(argv)
    cfg = Settings.from_env()
    uni = market.Universe(market.make_symbols(args.symbols, args.seed), args.seed)
    conn = db.connect(cfg)
    db.apply_schema(conn)

    if args.cmd == "history":
        n = load.load_history(conn, uni, args.days)
        print(f"history: inserted {n} bars", flush=True)
    elif args.cmd == "live":
        print(f"live: {args.rate:g} bars/s", flush=True)
        n = load.run_live(conn, uni, args.rate, args.max_bars)
        print(f"live: stopped after {n} bars", flush=True)
    else:
        hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
        hdfs.wait_ready(cfg.hdfs_min_datanodes)
        n = load.export_symbols(conn, hdfs)
        print(f"export-symbols: {n} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

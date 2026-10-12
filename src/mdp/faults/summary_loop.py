"""In-container half of the DataNode-kill drill (the host half is scripts/faults.py).

Warms every ticker's replication-1 cache, reports which DataNode holds each
cache file, then waits for a line on stdin. The host kills a DataNode and sends
"go"; from then on this process asks for summaries in a loop, affected tickers
first, and prints one JSON line per call so the host can time the recovery.
"""
import argparse
import json
import sys
import time

import grpc
import pyarrow.parquet as pq

from mdp import layout
from mdp.config import Settings
from mdp.hdfs import WebHdfs
from mdp.v1 import backfill_pb2 as pb
from mdp.v1 import backfill_pb2_grpc as rpc


def emit(**fields) -> None:
    print(json.dumps(fields), flush=True)


def call(stub, ticker: str, source: str, phase: str) -> dict:
    t = time.time()
    try:
        r = stub.SymbolSummary(pb.SymbolSummaryRequest(ticker=ticker, source_path=source), timeout=120)
        out = dict(ok=True, cache=pb.CacheOutcome.Name(r.cache), rows=r.rows, avg_close=r.avg_close)
    except grpc.RpcError as exc:
        out = dict(ok=False, error=f"{exc.code().name}: {exc.details()}")
    emit(event="call", phase=phase, t=t, ticker=ticker, latency_ms=(time.time() - t) * 1000, **out)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--seconds", type=float, default=45.0, help="how long to loop after go")
    args = ap.parse_args()

    cfg = Settings.from_env()
    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    stub = rpc.BackfillStub(grpc.insecure_channel(cfg.backfill_addr))

    f = hdfs.open(args.source)
    tickers = sorted(set(pq.read_table(f, columns=["ticker"]).column("ticker").to_pylist()))
    f.close()

    for t in tickers:
        call(stub, t, args.source, "warm")
    placement: dict[str, list[str]] = {}
    for t in tickers:
        blocks = hdfs.block_locations(layout.cache_path(args.source, t))
        placement.setdefault(blocks[0][0], []).append(t)
    emit(event="placement", placement=placement)
    for t in tickers:
        call(stub, t, args.source, "baseline")
    emit(event="ready")

    go = sys.stdin.readline().split()
    affected = go[1].split(",") if len(go) > 1 else []
    order = affected + [t for t in tickers if t not in affected]
    deadline = time.time() + args.seconds
    rounds = 0
    while time.time() < deadline:
        for t in order:
            call(stub, t, args.source, "after_kill")
        rounds += 1
    emit(event="done", rounds=rounds)
    return 0


if __name__ == "__main__":
    sys.exit(main())

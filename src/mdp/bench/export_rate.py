"""Time the ExportBars RPC. Prints one JSON line per repetition."""
import argparse
import json
import sys
import time

import grpc

from mdp.config import Settings
from mdp.v1 import backfill_pb2 as pb
from mdp.v1 import backfill_pb2_grpc as rpc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exchange", default="NASDAQ")
    ap.add_argument("--reps", type=int, default=5)
    args = ap.parse_args()

    stub = rpc.BackfillStub(grpc.insecure_channel(Settings.from_env().backfill_addr))
    req = pb.ExportBarsRequest(dimension=pb.DIMENSION_EXCHANGE, name=args.exchange,
                               dest_path="/market/exports/bench-export.parquet")
    for rep in range(1, args.reps + 1):
        t0 = time.perf_counter()
        reply = stub.ExportBars(req, timeout=600)
        secs = time.perf_counter() - t0
        print(json.dumps({"rep": rep, "rows": reply.rows, "bytes": reply.bytes,
                          "seconds": round(secs, 3), "rows_per_s": round(reply.rows / secs, 1)}),
              flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

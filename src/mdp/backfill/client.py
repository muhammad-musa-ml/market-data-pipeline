"""Command-line client for the backfill service."""
import argparse
import sys

import grpc

from mdp.config import Settings
from mdp.v1 import backfill_pb2 as pb
from mdp.v1 import backfill_pb2_grpc as rpc


def stub(addr: str) -> rpc.BackfillStub:
    return rpc.BackfillStub(grpc.insecure_channel(addr))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mdp.backfill.client")
    ap.add_argument("--addr", default=Settings.from_env().backfill_addr)
    sub = ap.add_subparsers(dest="cmd", required=True)

    ex = sub.add_parser("export", help="export bars for one sector or exchange to HDFS")
    group = ex.add_mutually_exclusive_group(required=True)
    group.add_argument("--sector")
    group.add_argument("--exchange")
    ex.add_argument("--dest", required=True)

    bl = sub.add_parser("blocks", help="blocks per DataNode for an HDFS file")
    bl.add_argument("path")

    st = sub.add_parser("summary", help="average close and VWAP for one ticker")
    st.add_argument("ticker")
    st.add_argument("--source", required=True)

    args = ap.parse_args(argv)
    s = stub(args.addr)
    try:
        if args.cmd == "export":
            req = pb.ExportBarsRequest(
                dimension=pb.DIMENSION_SECTOR if args.sector else pb.DIMENSION_EXCHANGE,
                name=args.sector or args.exchange, dest_path=args.dest)
            print(s.ExportBars(req, timeout=300))
        elif args.cmd == "blocks":
            print(s.BlockDistribution(pb.BlockDistributionRequest(path=args.path), timeout=60))
        else:
            print(s.SymbolSummary(
                pb.SymbolSummaryRequest(ticker=args.ticker, source_path=args.source), timeout=120))
    except grpc.RpcError as exc:
        print(f"{exc.code().name}: {exc.details()}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

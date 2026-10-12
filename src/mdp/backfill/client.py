"""Command-line client for the backfill service."""
import argparse
import sys

import grpc
from google.protobuf import json_format

from mdp.config import Settings
from mdp.v1 import backfill_pb2 as pb
from mdp.v1 import backfill_pb2_grpc as rpc


def stub(addr: str) -> rpc.BackfillStub:
    return rpc.BackfillStub(grpc.insecure_channel(addr))


def show(reply, as_json: bool) -> None:
    if as_json:
        print(json_format.MessageToJson(reply, indent=None, preserving_proto_field_name=True))
    else:
        print(reply)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mdp.backfill.client")
    ap.add_argument("--addr", default=Settings.from_env().backfill_addr)
    ap.add_argument("--json", action="store_true", help="print the reply as one JSON line")
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
            show(s.ExportBars(req, timeout=300), args.json)
        elif args.cmd == "blocks":
            show(s.BlockDistribution(pb.BlockDistributionRequest(path=args.path), timeout=60), args.json)
        else:
            show(s.SymbolSummary(
                pb.SymbolSummaryRequest(ticker=args.ticker, source_path=args.source), timeout=120), args.json)
    except grpc.RpcError as exc:
        print(f"{exc.code().name}: {exc.details()}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

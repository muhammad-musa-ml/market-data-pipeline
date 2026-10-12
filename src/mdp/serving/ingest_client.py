"""Client and command line for the tick ingest service.

    python -m mdp.serving.ingest_client put AAPL 187.5 --size 100
    python -m mdp.serving.ingest_client get AAPL 2024-05-01
    python -m mdp.serving.ingest_client probe     # one put, one bulk put and one get; prints each outcome
"""
import argparse
import dataclasses
import datetime as dt
import json
import os
import sys
import time
import uuid
from collections.abc import Iterable

import grpc

from mdp.v1 import ingest_pb2 as pb
from mdp.v1 import ingest_pb2_grpc as rpc


@dataclasses.dataclass
class Outcome:
    rpc: str
    ok: bool
    code: str  # OK or the gRPC status name
    detail: str
    ms: float


def now_us() -> int:
    return time.time_ns() // 1000


class IngestClient:
    def __init__(self, addr: str, keyspace: str = "prod", timeout: float = 20.0):
        self.channel = grpc.insecure_channel(addr)
        self.stub = rpc.IngestStub(self.channel)
        self.metadata = (("x-keyspace", keyspace),)
        self.timeout = timeout

    def close(self) -> None:
        self.channel.close()

    def put_tick(self, tick: pb.Tick):
        return self.stub.PutTick(tick, timeout=self.timeout, metadata=self.metadata)

    def put_ticks(self, ticks: Iterable[pb.Tick]):
        return self.stub.PutTicks(iter(ticks), timeout=self.timeout, metadata=self.metadata)

    def get_ticks(self, ticker: str, day: str, limit: int = 0):
        return self.stub.GetTicks(pb.GetTicksRequest(ticker=ticker, day=day, limit=limit),
                                  timeout=self.timeout, metadata=self.metadata)

    def attempt(self, name: str, fn) -> Outcome:
        t0 = time.perf_counter()
        try:
            fn()
            return Outcome(name, True, "OK", "", (time.perf_counter() - t0) * 1000)
        except grpc.RpcError as exc:
            return Outcome(name, False, exc.code().name, exc.details() or "", (time.perf_counter() - t0) * 1000)

    def probe(self, ticker: str = "PROBE", producer: str | None = None) -> list[Outcome]:
        """One of each RPC against a ticker/day of its own, so the read can be checked for the write."""
        producer = producer or "probe-" + uuid.uuid4().hex[:8]
        ts = now_us()
        day = dt.datetime.fromtimestamp(ts / 1e6, dt.timezone.utc).date().isoformat()
        one = pb.Tick(ticker=ticker, ts_us=ts, price=101.25, size=10, producer=producer)
        bulk = [pb.Tick(ticker=ticker, ts_us=ts + 1 + i, price=101.25 + i, size=1, producer=producer)
                for i in range(50)]
        out = [self.attempt("PutTick", lambda: self.put_tick(one)),
               self.attempt("PutTicks", lambda: self.put_ticks(bulk))]
        seen: list[int] = []

        def read():
            seen.append(len(self.get_ticks(ticker, day, 10_000).ticks))
        out.append(self.attempt("GetTicks", read))
        if out[-1].ok:
            out[-1].detail = f"{seen[0]} ticks"
        return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mdp.serving.ingest_client")
    ap.add_argument("--addr", default=os.environ.get("INGEST_ADDR", "ingest:50061"))
    ap.add_argument("--keyspace", default="prod", choices=["prod", "test"])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("put")
    p.add_argument("ticker")
    p.add_argument("price", type=float)
    p.add_argument("--size", type=int, default=1)
    p.add_argument("--producer", default="cli")
    g = sub.add_parser("get")
    g.add_argument("ticker")
    g.add_argument("day")
    g.add_argument("--limit", type=int, default=10)
    sub.add_parser("probe")
    args = ap.parse_args(argv)

    client = IngestClient(args.addr, args.keyspace)
    try:
        if args.cmd == "put":
            client.put_tick(pb.Tick(ticker=args.ticker, ts_us=now_us(), price=args.price, size=args.size,
                                    producer=args.producer))
            print("ok")
        elif args.cmd == "get":
            for t in client.get_ticks(args.ticker, args.day, args.limit).ticks:
                print(json.dumps({"ts_us": t.ts_us, "price": t.price, "size": t.size, "producer": t.producer}))
        else:
            outcomes = client.probe()
            for o in outcomes:
                print(f"{o.rpc:9} {o.code:18} {o.ms:7.1f} ms  {o.detail}")
            return 0 if all(o.ok for o in outcomes) else 1
    except grpc.RpcError as exc:
        print(f"{exc.code().name}: {exc.details()}", file=sys.stderr)
        return 1
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

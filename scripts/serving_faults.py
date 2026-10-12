"""Availability matrix: which operations work with 0, 1 and 2 Cassandra nodes down.

    python scripts/serving_faults.py matrix [--mode crash|stop] [--attempts 3] [--json out.json]

For each number of dead nodes it kills (or stops) that many, waits until a survivor reports them DN,
then makes `--attempts` calls of each operation and records the outcome of every one:

  gRPC ingest (127.0.0.1:8410, keyspace test)   PutTick, PutTicks at ONE; GetTicks at ALL
  REST over Cassandra (127.0.0.1:8401, test)    read and write at QUORUM

and brings the nodes back before the next row. Needs the serving stack up, all three nodes UN.
"""
import argparse
import json
import statistics
import sys
import time
import uuid

import requests

import serving_ctl as ctl
import stack
from stack import say

sys.path.insert(0, str(stack.ROOT / "src"))
from mdp.serving.ingest_client import IngestClient, Outcome, now_us  # noqa: E402
from mdp.v1 import ingest_pb2 as pb  # noqa: E402

INGEST = "127.0.0.1:8410"
API = "http://127.0.0.1:8401/test/api"
COMPANY = {"name": "Drill Corp", "sector": "Testing", "exchange": "TEST"}
VICTIMS = ["cassandra-2", "cassandra-3"]


class Rest:
    def __init__(self):
        self.ticker = "D" + uuid.uuid4().hex[:7].upper()
        self.http = requests.Session()
        self.n = 0

    def setup(self) -> None:
        r = self.http.post(f"{API}/companies/{self.ticker}", json=COMPANY, timeout=30)
        r.raise_for_status()

    def call(self, name: str, fn) -> Outcome:
        t0 = time.perf_counter()
        try:
            r = fn()
            ms = (time.perf_counter() - t0) * 1000
            ok = r.status_code < 300
            code = "OK" if ok else str(r.status_code)
            return Outcome(name, ok, code, "" if ok else r.json().get("error", ""), ms)
        except requests.RequestException as exc:
            return Outcome(name, False, "ERROR", type(exc).__name__, (time.perf_counter() - t0) * 1000)

    def put(self) -> Outcome:
        self.n += 1
        body = {"open": 10, "high": 12, "low": 9, "close": 11, "volume": 5}
        return self.call("PUT record", lambda: self.http.put(
            f"{API}/companies/{self.ticker}/records/2024-06-{self.n % 28 + 1:02d}", json=body, timeout=30))

    def read(self) -> Outcome:
        return self.call("GET records", lambda: self.http.get(f"{API}/companies/{self.ticker}/records", timeout=30))

    def create(self) -> Outcome:
        tick = "D" + uuid.uuid4().hex[:7].upper()
        return self.call("POST company", lambda: self.http.post(f"{API}/companies/{tick}", json=COMPANY, timeout=30))

    def listing(self) -> Outcome:
        return self.call("GET companies", lambda: self.http.get(f"{API}/companies", timeout=30))


def grpc_ops(client: IngestClient, producer: str):
    ticker = "DRILL"

    def put_tick():
        return client.attempt("PutTick", lambda: client.put_tick(
            pb.Tick(ticker=ticker, ts_us=now_us(), price=50.5, size=1, producer=producer)))

    def put_ticks():
        ts = now_us()
        return client.attempt("PutTicks", lambda: client.put_ticks(
            pb.Tick(ticker=ticker, ts_us=ts + i, price=50.5, size=1, producer=producer) for i in range(20)))

    def get_ticks():
        import datetime as dt
        day = dt.datetime.now(dt.timezone.utc).date().isoformat()
        return client.attempt("GetTicks", lambda: client.get_ticks(ticker, day, 100))

    return [("gRPC", "PutTick", "ONE", put_tick), ("gRPC", "PutTicks", "ONE", put_ticks),
            ("gRPC", "GetTicks", "ALL", get_ticks)]


def take_down(count: int, mode: str) -> float:
    ips = {n: ctl.node_ip(n) for n in VICTIMS[:count]}
    for n in ips:
        (ctl.kill if mode == "crash" else ctl.stop)(n)
    return max((ctl.wait_down_seen(n, ip) for n, ip in ips.items()), default=0.0)


def matrix(mode: str = "crash", attempts: int = 3, down_counts=(0, 1, 2)) -> list[dict]:
    ctl.restore_all()
    rest = Rest()
    rest.setup()
    client = IngestClient(INGEST, "test", timeout=30)
    producer = "drill-" + uuid.uuid4().hex[:6]
    rows: list[dict] = []
    try:
        for down in down_counts:
            say(f"== {down} node(s) down ({mode})")
            detect = take_down(down, mode)
            ops = grpc_ops(client, producer) + [
                ("REST", "PUT record", "QUORUM", rest.put), ("REST", "GET records", "QUORUM", rest.read),
                ("REST", "POST company", "QUORUM+SERIAL", rest.create),
                ("REST", "GET companies", "QUORUM", rest.listing)]
            for surface, op, level, fn in ops:
                results = [fn() for _ in range(attempts)]
                codes = sorted({r.code for r in results})
                detail = next((r.detail for r in results if not r.ok), "")
                rows.append({
                    "nodes_down": down, "mode": mode, "surface": surface, "op": op, "consistency": level,
                    "attempts": attempts, "succeeded": sum(r.ok for r in results), "outcome": "/".join(codes),
                    "detail": detail, "median_ms": round(statistics.median(r.ms for r in results), 1),
                    "max_ms": round(max(r.ms for r in results), 1), "seconds_to_detect": round(detect, 1)})
                say(f"{down} down  {surface:4} {op:13} {level:13} {rows[-1]['succeeded']}/{attempts} "
                    f"{rows[-1]['outcome']}  {detail}")
            ctl.restore_all()
            time.sleep(10)  # let the drivers reconnect before the next row
    finally:
        client.close()
        ctl.restore_all()
    return rows


def render(rows: list[dict]) -> str:
    header = ["operation", "consistency"] + sorted({f"{r['nodes_down']} down" for r in rows})
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    ops = list(dict.fromkeys((r["surface"], r["op"], r["consistency"]) for r in rows))
    for surface, op, level in ops:
        cells = []
        for down in sorted({r["nodes_down"] for r in rows}):
            r = next(x for x in rows if x["nodes_down"] == down and (x["surface"], x["op"]) == (surface, op))
            cells.append(f"{r['succeeded']}/{r['attempts']} ok" if r["succeeded"] == r["attempts"]
                         else f"{r['outcome']} ({r['succeeded']}/{r['attempts']} ok)")
        lines.append(f"| {surface} {op} | {level} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["matrix"])
    ap.add_argument("--mode", choices=["crash", "stop"], default="crash")
    ap.add_argument("--attempts", type=int, default=3)
    ap.add_argument("--json", help="also write the rows to this file")
    args = ap.parse_args()
    rows = matrix(args.mode, args.attempts)
    print(render(rows))
    if args.json:
        with open(args.json, "w", newline="\n") as f:
            json.dump(rows, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())

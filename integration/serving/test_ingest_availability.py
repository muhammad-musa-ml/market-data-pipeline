"""What the ingest service does with 0, 1 and 2 of the three Cassandra nodes down.

Writes go in at consistency ONE, so one live replica is enough. The read is at ALL, so it needs
every replica and must refuse, not guess, as soon as one is missing. These tests kill containers
of the running stack and start them again afterwards.
"""
import datetime as dt
import os
import time
import uuid

import grpc
import pytest
import serving_ctl as ctl
from helpers import INGEST

from mdp.serving.ingest_client import IngestClient
from mdp.v1 import ingest_pb2 as pb

VICTIMS = ["cassandra-2", "cassandra-3"]
T0 = int(dt.datetime(2024, 6, 3, 14, 0, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000
DAY = "2024-06-03"


@pytest.fixture
def client():
    c = IngestClient(INGEST, "test", timeout=30)
    if not c.attempt("probe", lambda: c.get_ticks("ZZZZ", DAY, 1)).ok:
        if os.environ.get("MDP_REQUIRE_BACKENDS") == "1":
            pytest.fail(f"ingest at {INGEST} is not answering")
        pytest.skip("ingest is not answering")
    ctl.restore_all()
    yield c
    c.close()
    ctl.restore_all()


def tick(sym, i):
    return pb.Tick(ticker=sym, ts_us=T0 + i, price=10.0 + i, size=1, producer="drill")


def take_down(n: int) -> None:
    """Make exactly the first n victims dead (killing only the ones still running)."""
    ips = {v: ctl.node_ip(v) for v in VICTIMS[:n] if ctl.is_running(v)}
    for v in ips:
        ctl.kill(v)
    for v, ip in ips.items():
        ctl.wait_down_seen(v, ip)


def code_of(call, *args):
    try:
        call(*args)
    except grpc.RpcError as exc:
        return exc.code(), exc.details()
    return None, ""


def test_availability_with_zero_one_and_two_nodes_down(client):
    sym = "A" + uuid.uuid4().hex[:7].upper()
    written = 0
    table = {}
    for down in (0, 1, 2):
        take_down(down)
        put = code_of(client.put_tick, tick(sym, written))
        written += 1
        bulk = code_of(client.put_ticks, [tick(sym, written + i) for i in range(30)])
        written += 30
        got = code_of(client.get_ticks, sym, DAY, 1000)
        table[down] = (put, bulk, got)

    # every write succeeded whatever the number of nodes down
    for down, (put, bulk, _) in table.items():
        assert put[0] is None and bulk[0] is None, f"{down} down: write failed with {put} {bulk}"
    # the read at ALL works with all nodes and refuses with any missing, saying how many replicas it needed
    assert table[0][2][0] is None
    for down, alive in ((1, 2), (2, 1)):
        status, detail = table[down][2]
        assert status == grpc.StatusCode.UNAVAILABLE, f"{down} down: read gave {status}"
        assert f"ALL needs 3 replicas, {alive} alive" in detail, detail

    # after recovery the read at ALL returns every tick that was acknowledged during the outage
    ctl.restore_all()
    deadline = time.monotonic() + 90
    while True:
        status, _ = code_of(client.get_ticks, sym, DAY, 1000)
        if status is None:
            break
        assert time.monotonic() < deadline, "reads never recovered"
        time.sleep(2)
    ticks = client.get_ticks(sym, DAY, 1000).ticks
    assert sorted(t.ts_us for t in ticks) == [T0 + i for i in range(written)]

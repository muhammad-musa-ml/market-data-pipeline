"""Quorum reads and writes with replication factor 3 while Cassandra nodes are down.

These tests stop and kill containers of the running stack and start them again afterwards, so they
take several minutes and must not share the cluster with anything else. Only the Cassandra API
is involved; MySQL has no nodes to lose.
"""
import time

import pytest
import serving_ctl as ctl
from helpers import COMPANY, URLS, Api, bar, require

MODES = {"crash": ctl.kill, "clean-stop": ctl.stop}


@pytest.fixture
def cassandra() -> Api:
    require("cassandra")
    ctl.restore_all()  # a failed earlier test may have left nodes down
    return Api("cassandra", URLS["cassandra"])


@pytest.fixture
def downed():
    """Nodes taken down by the test; they are started again and awaited on the way out."""
    taken: list[str] = []
    yield taken
    ctl.restore_all()
    api = Api("cassandra", URLS["cassandra"])
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:  # the API's driver reconnects every couple of seconds
        try:
            if api.get("companies").status_code == 200:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(2)
    pytest.fail("the API did not recover after the nodes came back")


def write_then_read(api: Api, tick: str, days: range, label: str) -> None:
    for i in days:
        day = f"2024-05-{i:02d}"
        close = 200.0 + i
        r = api.put(f"companies/{tick}/records/{day}", bar(close))
        assert r.status_code == 200, f"{label}: write {day} -> {r.status_code} {r.text}"
        rows = api.get(f"companies/{tick}/records", start=day, end=day)
        assert rows.status_code == 200, f"{label}: read {day} -> {rows.status_code} {rows.text}"
        assert [x["close"] for x in rows.json()] == [close], f"{label}: read after write {day} was stale"


@pytest.mark.parametrize("mode", MODES)
def test_one_node_down_reads_and_writes_succeed_and_see_each_other(cassandra, downed, ticker, mode):
    assert cassandra.post(f"companies/{ticker}", COMPANY).status_code == 201
    write_then_read(cassandra, ticker, range(1, 6), "all nodes up")

    victim = "cassandra-2"
    victim_ip = ctl.node_ip(victim)
    downed.append(victim)
    MODES[mode](victim)
    # no waiting for the survivors to notice: the first requests after the loss must work too
    write_then_read(cassandra, ticker, range(6, 16), f"{victim} {mode}, just after")

    seen_after = ctl.wait_down_seen(victim, victim_ip)
    write_then_read(cassandra, ticker, range(16, 26), f"{victim} {mode}, marked down")

    other = ticker[:7] + "N"  # a new company, which is a lightweight transaction
    assert cassandra.post(f"companies/{other}", COMPANY).status_code == 201
    assert cassandra.post(f"companies/{other}", COMPANY).status_code == 409
    assert other in [c["ticker"] for c in cassandra.get("companies").json()]
    assert len(cassandra.get(f"companies/{ticker}/records").json()) == 25
    print(f"{mode}: survivors reported {victim} DN after {seen_after:.1f}s")


def test_two_nodes_down_quorum_operations_fail_cleanly_and_nothing_is_lost(cassandra, downed, ticker):
    assert cassandra.post(f"companies/{ticker}", COMPANY).status_code == 201
    write_then_read(cassandra, ticker, range(1, 4), "all nodes up")

    ips = {n: ctl.node_ip(n) for n in ("cassandra-2", "cassandra-3")}
    for n in ips:
        downed.append(n)
        ctl.kill(n)
    for n, ip in ips.items():
        ctl.wait_down_seen(n, ip)

    start = time.monotonic()
    refused = [
        cassandra.get(f"companies/{ticker}"),
        cassandra.get(f"companies/{ticker}/records"),
        cassandra.put(f"companies/{ticker}/records/2024-05-20", bar(5)),
        cassandra.post(f"companies/{ticker[:7]}Q", COMPANY),
        cassandra.get("companies"),
    ]
    assert [r.status_code for r in refused] == [503] * 5, [(r.status_code, r.text[:80]) for r in refused]
    assert all(r.json()["error"] == "backend unavailable" for r in refused)
    assert time.monotonic() - start < 30

    ctl.restore_all()
    deadline = time.monotonic() + 90
    while True:
        r = cassandra.get(f"companies/{ticker}/records")
        if r.status_code == 200:
            break
        assert time.monotonic() < deadline, "the API never recovered"
        time.sleep(2)
    assert [x["close"] for x in r.json()] == [201.0, 202.0, 203.0]  # the refused write left no trace

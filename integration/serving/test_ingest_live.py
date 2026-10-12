"""The tick ingest service with every node up: what goes in comes out, retries do not double."""
import datetime as dt
import os
import uuid

import grpc
import pytest
from helpers import INGEST

from mdp.serving.ingest_client import IngestClient
from mdp.v1 import ingest_pb2 as pb

DAY = "2024-05-01"
T0 = int(dt.datetime(2024, 5, 1, 9, 30, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000


@pytest.fixture
def client():
    c = IngestClient(INGEST, "test", timeout=30)
    ok = c.attempt("probe", lambda: c.get_ticks("ZZZZ", DAY, 1))
    if not ok.ok:
        if os.environ.get("MDP_REQUIRE_BACKENDS") == "1":
            pytest.fail(f"ingest at {INGEST} is not answering: {ok.code} {ok.detail}")
        pytest.skip(f"ingest at {INGEST} is not answering")
    yield c
    c.close()


@pytest.fixture
def sym() -> str:
    return "I" + uuid.uuid4().hex[:7].upper()


def tick(sym, i=0, producer="p1", **over):
    return pb.Tick(**{"ticker": sym, "ts_us": T0 + i, "price": 100.0 + i, "size": 10 + i, "producer": producer, **over})


def test_a_tick_comes_back_exactly(client, sym):
    client.put_tick(tick(sym, 123))
    (got,) = client.get_ticks(sym, DAY).ticks
    assert (got.ticker, got.ts_us, got.price, got.size, got.producer) == (sym, T0 + 123, 223.0, 133, "p1")


def test_microseconds_survive(client, sym):
    client.put_ticks([tick(sym, 1), tick(sym, 2), tick(sym, 3)])
    assert [t.ts_us for t in client.get_ticks(sym, DAY).ticks] == [T0 + 3, T0 + 2, T0 + 1]


def test_bulk_upload_counts_and_reads_newest_first(client, sym):
    assert client.put_ticks(tick(sym, i) for i in range(1000)).accepted == 1000
    ticks = client.get_ticks(sym, DAY, 10_000).ticks
    assert len(ticks) == 1000 and ticks[0].ts_us == T0 + 999 and ticks[-1].ts_us == T0
    assert [t.ts_us for t in client.get_ticks(sym, DAY, 5).ticks] == [T0 + 999 - i for i in range(5)]


def test_sending_a_batch_twice_does_not_double_it(client, sym):
    batch = [tick(sym, i) for i in range(300)]
    client.put_ticks(batch)
    client.put_ticks(batch)
    assert len(client.get_ticks(sym, DAY, 10_000).ticks) == 300


def test_two_producers_at_the_same_instant_are_both_kept(client, sym):
    client.put_tick(tick(sym, 0, producer="a"))
    client.put_tick(tick(sym, 0, producer="b"))
    assert sorted(t.producer for t in client.get_ticks(sym, DAY).ticks) == ["a", "b"]


def test_ticks_land_in_the_day_of_their_utc_timestamp(client, sym):
    next_day = T0 + 24 * 3600 * 1_000_000
    client.put_tick(tick(sym, 0))
    client.put_tick(tick(sym, 0, ts_us=next_day))
    assert len(client.get_ticks(sym, "2024-05-01").ticks) == 1
    assert len(client.get_ticks(sym, "2024-05-02").ticks) == 1
    assert client.get_ticks(sym, "2024-05-03").ticks == []


def test_invalid_input_is_refused_and_stores_nothing(client, sym):
    for bad in (tick(sym, price=-1.0), tick(sym, producer=""), tick("lower")):
        with pytest.raises(grpc.RpcError) as exc:
            client.put_tick(bad)
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert client.get_ticks(sym, DAY).ticks == []


def test_prod_and_test_keyspaces_are_separate(client, sym):
    client.put_tick(tick(sym))
    prod = IngestClient(INGEST, "prod")
    try:
        assert prod.get_ticks(sym, DAY).ticks == []
    finally:
        prod.close()

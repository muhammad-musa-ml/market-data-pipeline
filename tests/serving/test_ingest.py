"""The tick ingest service over a real in-process gRPC server and a fake Cassandra session."""
import datetime as dt
from concurrent import futures

import grpc
import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout
from cassandra.cluster import NoHostAvailable

from mdp.serving import ingest_server as srv
from mdp.serving.ingest_client import IngestClient
from mdp.v1 import ingest_pb2 as pb
from mdp.v1 import ingest_pb2_grpc as rpc
from serving_fakes import FakeSession, concurrent_stand_in

TS = int(dt.datetime(2024, 5, 1, 12, 0, 0, 123456, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000 + 123456


def tick(i=0, **over):
    return pb.Tick(**{"ticker": "ACME", "ts_us": TS + i, "price": 10.5, "size": 3, "producer": "p1", **over})


@pytest.fixture
def session(monkeypatch):
    fake = FakeSession()
    monkeypatch.setattr(srv, "execute_concurrent_with_args", concurrent_stand_in(fake))
    return fake


@pytest.fixture
def client(session):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    rpc.add_IngestServicer_to_server(srv.IngestService(session), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    c = IngestClient(f"127.0.0.1:{port}", timeout=10)
    yield c
    c.close()
    server.stop(0)


def code(call, *args):
    with pytest.raises(grpc.RpcError) as exc:
        call(*args)
    return exc.value.code(), exc.value.details()


def test_writes_are_prepared_at_one_and_reads_at_all(session):
    srv.IngestService(session)
    put, get = [p for p in session.prepared if "prod.ticks" in p.cql][:2]
    assert put.consistency_level == ConsistencyLevel.ONE
    assert get.consistency_level == ConsistencyLevel.ALL
    assert srv.WRITE_CONSISTENCY == ConsistencyLevel.ONE and srv.READ_CONSISTENCY == ConsistencyLevel.ALL


def test_each_keyspace_gets_its_own_statements(session):
    srv.IngestService(session)
    assert sorted({"prod" if "prod.ticks" in p.cql else "test" for p in session.prepared}) == ["prod", "test"]
    assert len(session.prepared) == 4


def test_day_is_the_utc_day_of_the_timestamp():
    assert srv.day_of(TS) == dt.date(2024, 5, 1)
    midnight = int(dt.datetime(2024, 5, 2, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000
    assert srv.day_of(midnight - 1) == dt.date(2024, 5, 1)
    assert srv.day_of(midnight) == dt.date(2024, 5, 2)


def test_put_tick_binds_the_key_parts_in_order(client, session):
    assert client.put_tick(tick()).accepted == 1
    stmt, params = session.executed[-1]
    assert params == ("ACME", dt.date(2024, 5, 1), TS, "p1", 10.5, 3)
    assert stmt.consistency_level == ConsistencyLevel.ONE


@pytest.mark.parametrize("over", [
    {"ticker": "acme"}, {"ticker": ""}, {"ts_us": 0}, {"ts_us": -5}, {"ts_us": 10**18}, {"price": 0.0},
    {"price": -1.0}, {"price": float("nan")}, {"price": float("inf")}, {"size": -1}, {"producer": ""},
    {"producer": "x" * 65},
])
def test_invalid_ticks_are_rejected_before_any_write(client, session, over):
    status, _ = code(client.put_tick, tick(**over))
    assert status == grpc.StatusCode.INVALID_ARGUMENT
    assert session.executed == []


def test_unknown_keyspace_is_invalid_argument(client):
    client.metadata = (("x-keyspace", "staging"),)
    assert code(client.put_tick, tick())[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert code(client.get_ticks, "ACME", "2024-05-01")[0] == grpc.StatusCode.INVALID_ARGUMENT


def test_test_keyspace_uses_its_own_statement(client, session):
    client.metadata = (("x-keyspace", "test"),)
    client.put_tick(tick())
    assert "test.ticks" in session.executed[-1][0].cql


def test_a_write_that_cannot_reach_one_replica_is_unavailable(client, session):
    session.error = Unavailable("x", ConsistencyLevel.ONE, 1, 0)
    status, detail = code(client.put_tick, tick())
    assert status == grpc.StatusCode.UNAVAILABLE and "ONE needs 1 replicas, 0 alive" in detail


def test_a_read_at_all_with_a_node_down_is_unavailable_and_says_why(client, session):
    session.error = Unavailable("x", ConsistencyLevel.ALL, 3, 2)
    status, detail = code(client.get_ticks, "ACME", "2024-05-01")
    assert status == grpc.StatusCode.UNAVAILABLE and "ALL needs 3 replicas, 2 alive" in detail


@pytest.mark.parametrize("error", [
    ReadTimeout("t", consistency=ConsistencyLevel.ALL, required_responses=3, received_responses=2),
    WriteTimeout("t", consistency=ConsistencyLevel.ONE, required_responses=1, received_responses=0, write_type=0),
    OperationTimedOut(errors={}, last_host="h"),
])
def test_timeouts_are_deadline_exceeded(client, session, error):
    session.error = error
    assert code(client.get_ticks, "ACME", "2024-05-01")[0] == grpc.StatusCode.DEADLINE_EXCEEDED
    assert code(client.put_tick, tick())[0] == grpc.StatusCode.DEADLINE_EXCEEDED


def test_an_unavailable_wrapped_in_no_host_available_still_says_how_many_replicas(client, session):
    inner = Unavailable("x", ConsistencyLevel.ALL, 3, 1)
    session.error = NoHostAvailable("Unable to complete the operation against any hosts", {"10.0.0.1": inner})
    status, detail = code(client.get_ticks, "ACME", "2024-05-01")
    assert status == grpc.StatusCode.UNAVAILABLE and "ALL needs 3 replicas, 1 alive" in detail


def test_no_host_available_is_unavailable(client, session):
    session.error = NoHostAvailable("down", {})
    assert code(client.put_tick, tick())[0] == grpc.StatusCode.UNAVAILABLE
    assert code(client.get_ticks, "ACME", "2024-05-01")[0] == grpc.StatusCode.UNAVAILABLE


def test_get_ticks_returns_rows_newest_first_as_stored(client, session):
    session.results = [type("R", (), {"ts_us": TS + 2, "producer": "p2", "price": 11.0, "size": 5})(),
                       type("R", (), {"ts_us": TS, "producer": "p1", "price": 10.5, "size": 3})()]
    reply = client.get_ticks("ACME", "2024-05-01", 10)
    assert [(t.ts_us, t.producer, t.price, t.size, t.ticker) for t in reply.ticks] == [
        (TS + 2, "p2", 11.0, 5, "ACME"), (TS, "p1", 10.5, 3, "ACME")]
    stmt, params = session.executed[-1]
    assert params == ("ACME", dt.date(2024, 5, 1), 10) and stmt.consistency_level == ConsistencyLevel.ALL


def test_get_ticks_validates_its_arguments(client):
    for args in (("acme", "2024-05-01", 1), ("ACME", "May 1", 1), ("ACME", "2024-05-01", 10_001)):
        assert code(client.get_ticks, *args)[0] == grpc.StatusCode.INVALID_ARGUMENT


def test_put_ticks_streams_in_chunks_and_counts_every_tick(client, session):
    n = srv.CHUNK * 2 + 37
    assert client.put_ticks(tick(i) for i in range(n)).accepted == n
    assert len(session.executed) == n
    assert {s.consistency_level for s, _ in session.executed} == {ConsistencyLevel.ONE}


def test_an_invalid_tick_in_a_stream_names_its_position_and_stops(client, session):
    ticks = [tick(i) for i in range(5)] + [tick(5, price=-1.0)] + [tick(6)]
    status, detail = code(client.put_ticks, ticks)
    assert status == grpc.StatusCode.INVALID_ARGUMENT and "tick 5" in detail
    assert session.executed == []  # the first chunk was never flushed


def test_a_stream_that_loses_the_cluster_part_way_reports_how_far_it_got(client, session):
    session.error = Unavailable("x", ConsistencyLevel.ONE, 1, 0)
    session.fail_after = srv.CHUNK  # the first chunk lands, the second does not
    status, detail = code(client.put_ticks, [tick(i) for i in range(srv.CHUNK * 2)])
    assert status == grpc.StatusCode.UNAVAILABLE
    assert f"{srv.CHUNK} ticks were acknowledged" in detail


def test_probe_reports_each_rpc(client, session):
    session.results = [type("R", (), {"ts_us": TS, "producer": "p", "price": 1.0, "size": 1})()]
    out = client.probe()
    assert [(o.rpc, o.ok, o.code) for o in out] == [("PutTick", True, "OK"), ("PutTicks", True, "OK"),
                                                    ("GetTicks", True, "OK")]
    session.error = Unavailable("x", ConsistencyLevel.ALL, 3, 2)
    out = client.probe()
    assert [o.code for o in out] == ["UNAVAILABLE"] * 3

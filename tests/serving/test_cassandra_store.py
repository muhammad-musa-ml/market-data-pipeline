"""CassandraStore against a fake session: which statements it prepares, at what consistency, and how
driver errors come out."""
import datetime as dt
from types import SimpleNamespace

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, Unavailable as CassUnavailable
from cassandra.cluster import NoHostAvailable
from cassandra.util import Date

from mdp.serving.cassandra_store import CassandraStore
from mdp.serving.models import Company
from mdp.serving.store import Conflict, Unavailable
from serving_fakes import FakeSession, bar


def store(consistency=ConsistencyLevel.QUORUM):
    s = FakeSession()
    return CassandraStore(s, "prod", consistency), s


def test_every_statement_runs_at_quorum_by_default_and_targets_the_keyspace():
    session = FakeSession()
    CassandraStore(session, "prod")  # no consistency argument: the default is what the API gets
    assert len(session.prepared) == 6
    assert {p.consistency_level for p in session.prepared} == {ConsistencyLevel.QUORUM}
    assert all("prod.daily_bars" in p.cql for p in session.prepared)


def test_consistency_is_a_parameter():
    _, session = store(ConsistencyLevel.ONE)
    assert {p.consistency_level for p in session.prepared} == {ConsistencyLevel.ONE}


def test_only_the_create_statement_is_a_lightweight_transaction():
    _, session = store()
    lwt = [p for p in session.prepared if "IF NOT EXISTS" in p.cql]
    assert len(lwt) == 1 and lwt[0].serial_consistency_level == ConsistencyLevel.SERIAL
    assert all(p.serial_consistency_level is None for p in session.prepared if p not in lwt)


def test_add_company_conflict_when_not_applied():
    s, session = store()
    session.results = [SimpleNamespace(applied=False)]
    with pytest.raises(Conflict):
        s.add_company(Company("ACME", "Acme", "Energy", "NYSE"))
    session.results = [SimpleNamespace(applied=True)]
    s.add_company(Company("ACME", "Acme", "Energy", "NYSE"))


def test_a_partition_with_only_regular_columns_is_not_a_company():
    s, session = store()
    session.results = [SimpleNamespace(ticker="ACME", name=None, sector=None, exchange=None)]
    assert s.get_company("ACME") is None
    session.results = []
    assert s.get_company("ACME") is None


def test_list_skips_rows_without_static_data_and_sorts():
    s, session = store()
    session.results = [SimpleNamespace(ticker="B", name="b", sector="s", exchange="e"),
                       SimpleNamespace(ticker="X", name=None, sector=None, exchange=None),
                       SimpleNamespace(ticker="A", name="a", sector="s", exchange="e")]
    assert [c.ticker for c in s.list_companies()] == ["A", "B"]


def test_bars_converts_driver_dates_and_defaults_the_window():
    s, session = store()
    session.results = [SimpleNamespace(trade_date=Date(dt.date(2024, 1, 2)), open=1.0, high=2.0, low=0.5,
                                       close=1.5, volume=10)]
    out = s.bars("ACME")
    assert out[0].trade_date == dt.date(2024, 1, 2) and out[0].volume == 10
    _, params = session.executed[-1]
    assert params[0] == "ACME" and params[1] < dt.date(1901, 1, 1) and params[2] > dt.date(2900, 1, 1)
    assert params[3] >= 100_000


def test_bars_passes_window_and_limit_through():
    s, session = store()
    s.bars("ACME", dt.date(2024, 1, 1), dt.date(2024, 1, 31), 7)
    assert session.executed[-1][1] == ("ACME", dt.date(2024, 1, 1), dt.date(2024, 1, 31), 7)


@pytest.mark.parametrize("error", [
    CassUnavailable("x", ConsistencyLevel.QUORUM, 2, 1),
    OperationTimedOut(errors={}, last_host="h"),
    NoHostAvailable("down", {}),
])
def test_cluster_trouble_is_unavailable_on_every_method(error):
    s, session = store()
    session.error = error
    calls = [s.list_companies, lambda: s.get_company("A"), lambda: s.add_company(Company("A", "a", "b", "c")),
             lambda: s.put_bar("A", bar("2024-01-02")), lambda: s.bars("A"),
             lambda: s.upsert_bars([("A", bar("2024-01-02"))])]
    for call in calls:
        with pytest.raises(Unavailable):
            call()


def test_other_errors_are_not_swallowed():
    s, session = store()
    session.error = ValueError("bug")
    with pytest.raises(ValueError):
        s.get_company("A")


def rows_for(*counts: tuple[str, int]):
    return [(t, bar(f"2024-01-{d + 1:02d}")) for t, n in counts for d in range(n)]


def test_chunks_never_cross_a_ticker_or_exceed_the_size():
    from mdp.serving.cassandra_store import same_partition_chunks
    rows = rows_for(("A", 5), ("B", 1), ("C", 7))
    chunks = same_partition_chunks(rows, 3)
    assert [len(c) for c in chunks] == [3, 2, 1, 3, 3, 1]
    assert all(len({t for t, _ in c}) == 1 for c in chunks)
    assert [r for c in chunks for r in c] == rows  # nothing lost or reordered
    assert same_partition_chunks([], 3) == []


class RecordingBatch:
    """Stands in for BatchStatement, which only accepts statements the driver itself prepared."""

    def __init__(self, batch_type, consistency_level):
        self.batch_type, self.consistency_level, self.added = batch_type, consistency_level, []

    def add(self, stmt, params):
        self.added.append((stmt, params))


def test_batched_upsert_sends_unlogged_single_partition_batches_at_the_store_consistency(monkeypatch):
    from cassandra.query import BatchType
    from mdp.serving import cassandra_store
    sent = []
    monkeypatch.setattr(cassandra_store, "BatchStatement", RecordingBatch)
    monkeypatch.setattr(cassandra_store, "execute_concurrent",
                        lambda session, pairs, concurrency: sent.extend(b for b, _ in pairs))
    s, _ = store(ConsistencyLevel.QUORUM)
    s.upsert_bars(rows_for(("A", 5), ("B", 2)), concurrency=8, batch_size=2)
    assert [len(b.added) for b in sent] == [2, 2, 1, 2]
    assert all(b.batch_type == BatchType.UNLOGGED and b.consistency_level == ConsistencyLevel.QUORUM for b in sent)
    assert all(stmt is s._put for b in sent for stmt, _ in b.added)
    assert [{p[0] for _, p in b.added} for b in sent] == [{"A"}, {"A"}, {"A"}, {"B"}]


def test_batch_size_one_means_one_statement_per_row(monkeypatch):
    from mdp.serving import cassandra_store
    calls = []
    monkeypatch.setattr(cassandra_store, "execute_concurrent_with_args",
                        lambda session, stmt, args, concurrency: calls.append((stmt, len(args), concurrency)))
    s, session = store()
    s.upsert_bars(rows_for(("A", 4)), concurrency=16, batch_size=1)
    assert calls == [(s._put, 4, 16)]


def test_the_store_the_api_opens_runs_at_quorum(monkeypatch):
    from mdp.serving import backends, cluster
    from mdp.serving.settings import ServingSettings
    session = FakeSession()
    monkeypatch.setattr(cluster, "shared", lambda cfg: session)
    backends.open_store("cassandra", "test", ServingSettings.from_env())
    assert {p.consistency_level for p in session.prepared} == {ConsistencyLevel.QUORUM}
    assert all("test.daily_bars" in p.cql for p in session.prepared)

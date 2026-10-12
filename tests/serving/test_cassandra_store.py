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
from serving_fakes import bar


class Prepared(SimpleNamespace):
    pass


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def one(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class FakeFuture:
    """Just enough of ResponseFuture for cassandra.concurrent."""

    def __init__(self, error):
        self.error = error

    def add_callbacks(self, callback, errback, callback_args=(), callback_kwargs=None, errback_args=(),
                      errback_kwargs=None):
        if self.error:
            errback(self.error, *errback_args, **(errback_kwargs or {}))
        else:
            callback([], *callback_args, **(callback_kwargs or {}))


class FakeSession:
    def __init__(self):
        self.prepared: list[Prepared] = []
        self.executed: list[tuple] = []
        self.results: list = []
        self.error: Exception | None = None

    def prepare(self, cql):
        p = Prepared(cql=cql, consistency_level=None, serial_consistency_level=None)
        self.prepared.append(p)
        return p

    def execute(self, stmt, params=()):
        if self.error:
            raise self.error
        self.executed.append((stmt, params))
        return FakeResult(self.results)

    def execute_async(self, stmt, params=(), *args, **kwargs):
        self.executed.append((stmt, params))
        return FakeFuture(self.error)


def store(consistency=ConsistencyLevel.QUORUM):
    s = FakeSession()
    return CassandraStore(s, "prod", consistency), s


def test_every_statement_runs_at_quorum_by_default_and_targets_the_keyspace():
    _, session = store()
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

import datetime as dt
from collections.abc import Callable, Sequence

from cassandra import (
    ConsistencyLevel, OperationTimedOut, ReadFailure, ReadTimeout, Unavailable as CassUnavailable,
    WriteFailure, WriteTimeout,
)
from cassandra.cluster import NoHostAvailable, Session
from cassandra.concurrent import execute_concurrent_with_args

from mdp.serving.models import FIRST_DAY, LAST_DAY, Bar, Company
from mdp.serving.store import Conflict, Unavailable

# cassandra.Timeout subclasses and friends: all mean "the replicas did not answer in time"
BACKEND_DOWN = (CassUnavailable, NoHostAvailable, OperationTimedOut, ReadTimeout, WriteTimeout,
                ReadFailure, WriteFailure)

NO_LIMIT = 1_000_000


def translate(fn: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except BACKEND_DOWN as exc:
            raise Unavailable(f"{type(exc).__name__}: {exc}") from exc
    wrapper.__name__ = fn.__name__
    return wrapper


class CassandraStore:
    """The `daily_bars` table of one keyspace: a partition per ticker, the company in static columns.

    Everything runs at `consistency` (QUORUM by default). With replication factor 3 a quorum is
    two replicas, so a read always meets the replica that acknowledged the last write, and one
    node can be down without either failing.
    """

    def __init__(self, session: Session, keyspace: str, consistency: int = ConsistencyLevel.QUORUM):
        self.session = session
        t = f"{keyspace}.daily_bars"
        prep = session.prepare
        self._list = prep(f"SELECT DISTINCT ticker, name, sector, exchange FROM {t}")
        self._one = prep(f"SELECT ticker, name, sector, exchange FROM {t} WHERE ticker = ? LIMIT 1")
        self._add = prep(f"INSERT INTO {t} (ticker, name, sector, exchange) VALUES (?, ?, ?, ?) IF NOT EXISTS")
        self._set_company = prep(f"INSERT INTO {t} (ticker, name, sector, exchange) VALUES (?, ?, ?, ?)")
        self._put = prep(f"INSERT INTO {t} (ticker, trade_date, open, high, low, close, volume) "
                         f"VALUES (?, ?, ?, ?, ?, ?, ?)")
        self._range = prep(f"SELECT trade_date, open, high, low, close, volume FROM {t} "
                           f"WHERE ticker = ? AND trade_date >= ? AND trade_date <= ? "
                           f"ORDER BY trade_date ASC LIMIT ?")
        for stmt in (self._list, self._one, self._add, self._set_company, self._put, self._range):
            stmt.consistency_level = consistency
        self._add.serial_consistency_level = ConsistencyLevel.SERIAL

    @translate
    def list_companies(self) -> list[Company]:
        rows = self.session.execute(self._list)
        return sorted((Company(r.ticker, r.name, r.sector, r.exchange) for r in rows if r.name is not None),
                      key=lambda c: c.ticker)

    @translate
    def get_company(self, ticker: str) -> Company | None:
        row = self.session.execute(self._one, (ticker,)).one()
        if row is None or row.name is None:
            return None
        return Company(row.ticker, row.name, row.sector, row.exchange)

    @translate
    def add_company(self, c: Company) -> None:
        row = self.session.execute(self._add, (c.ticker, c.name, c.sector, c.exchange)).one()
        if not row.applied:
            raise Conflict(f"{c.ticker} already exists")

    @translate
    def put_bar(self, ticker: str, bar: Bar) -> None:
        self.session.execute(self._put, (ticker, bar.trade_date, bar.open, bar.high, bar.low, bar.close,
                                         bar.volume))

    @translate
    def bars(self, ticker: str, start: dt.date | None = None, end: dt.date | None = None,
             limit: int | None = None) -> list[Bar]:
        rows = self.session.execute(self._range, (ticker, start or FIRST_DAY, end or LAST_DAY,
                                                  limit or NO_LIMIT))
        return [Bar(r.trade_date.date(), r.open, r.high, r.low, r.close, r.volume) for r in rows]

    @translate
    def upsert_companies(self, companies: Sequence[Company]) -> None:
        execute_concurrent_with_args(
            self.session, self._set_company, [(c.ticker, c.name, c.sector, c.exchange) for c in companies],
            concurrency=16)

    @translate
    def upsert_bars(self, rows: Sequence[tuple[str, Bar]], concurrency: int = 64) -> None:
        args = [(t, b.trade_date, b.open, b.high, b.low, b.close, b.volume) for t, b in rows]
        execute_concurrent_with_args(self.session, self._put, args, concurrency=concurrency)

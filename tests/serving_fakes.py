import datetime as dt
from types import SimpleNamespace

from mdp.serving.models import Bar, Company
from mdp.serving.store import Conflict, Unavailable


class MemoryStore:
    """A Store that keeps everything in dicts, with a switch that makes it fail like a dead backend."""

    def __init__(self):
        self.companies: dict[str, Company] = {}
        self.rows: dict[str, dict[dt.date, Bar]] = {}
        self.down = False

    def _check(self):
        if self.down:
            raise Unavailable("memory store switched off")

    def list_companies(self):
        self._check()
        return sorted(self.companies.values(), key=lambda c: c.ticker)

    def get_company(self, ticker):
        self._check()
        return self.companies.get(ticker)

    def add_company(self, company):
        self._check()
        if company.ticker in self.companies:
            raise Conflict(f"{company.ticker} already exists")
        self.companies[company.ticker] = company
        self.rows[company.ticker] = {}

    def put_bar(self, ticker, bar):
        self._check()
        self.rows[ticker][bar.trade_date] = bar

    def bars(self, ticker, start=None, end=None, limit=None):
        self._check()
        out = [b for d, b in sorted(self.rows[ticker].items())
               if (start is None or d >= start) and (end is None or d <= end)]
        return out[:limit] if limit else out

    def upsert_companies(self, companies):
        for c in companies:
            self.companies[c.ticker] = c
            self.rows.setdefault(c.ticker, {})

    def upsert_bars(self, rows, concurrency=64, batch_size=1):
        for t, b in rows:
            self.rows[t][b.trade_date] = b


def bar(day: str, close: float = 10.0, volume: int = 100, spread: float = 1.0) -> Bar:
    return Bar(dt.date.fromisoformat(day), close, close + spread, close - spread, close, volume)


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
        self.fail_after: int | None = None  # concurrent executes allowed before `error` applies
        self.async_calls = 0

    def prepare(self, cql):
        p = Prepared(cql=cql, query_string=cql, consistency_level=None, serial_consistency_level=None)
        self.prepared.append(p)
        return p

    def execute(self, stmt, params=()):
        if self.error:
            raise self.error
        self.executed.append((stmt, params))
        return FakeResult(self.results)

    def execute_async(self, stmt, params=(), *args, **kwargs):
        self.executed.append((stmt, params))
        self.async_calls += 1
        failing = self.error is not None and (self.fail_after is None or self.async_calls > self.fail_after)
        return FakeFuture(self.error if failing else None)


def concurrent_stand_in(session):
    """Replaces cassandra.concurrent.execute_concurrent_with_args, whose result handling needs a real
    driver future: runs each parameter set through the fake session and raises like raise_on_first_error."""
    def run(sess, stmt, args, concurrency=100):
        for params in args:
            session.async_calls += 1
            if session.error is not None and (session.fail_after is None or session.async_calls > session.fail_after):
                raise session.error
            session.executed.append((stmt, params))
    return run

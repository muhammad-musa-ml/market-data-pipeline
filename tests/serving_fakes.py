import datetime as dt

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

    def upsert_bars(self, rows, concurrency=64):
        for t, b in rows:
            self.rows[t][b.trade_date] = b


def bar(day: str, close: float = 10.0, volume: int = 100, spread: float = 1.0) -> Bar:
    return Bar(dt.date.fromisoformat(day), close, close + spread, close - spread, close, volume)

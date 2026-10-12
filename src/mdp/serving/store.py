"""What the API needs from a backend. MySQL and Cassandra each implement it."""
import datetime as dt
from collections.abc import Sequence
from typing import Protocol

from mdp.serving.models import Bar, Company


class StoreError(Exception):
    pass


class Conflict(StoreError):
    """The company already exists."""


class Unavailable(StoreError):
    """The backend could not serve the request (nodes down, timeout, connection refused)."""


class Store(Protocol):
    def list_companies(self) -> list[Company]: ...
    def get_company(self, ticker: str) -> Company | None: ...
    def add_company(self, company: Company) -> None: ...
    def put_bar(self, ticker: str, bar: Bar) -> None: ...
    def bars(self, ticker: str, start: dt.date | None = None, end: dt.date | None = None,
             limit: int | None = None) -> list[Bar]: ...

    # bulk paths used by the loader
    def upsert_companies(self, companies: Sequence[Company]) -> None: ...
    def upsert_bars(self, rows: Sequence[tuple[str, Bar]], concurrency: int = 64, batch_size: int = 1) -> None: ...

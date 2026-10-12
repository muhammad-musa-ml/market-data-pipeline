"""Per-symbol aggregate with a replication-1 cache file per symbol.

The big export file is written at replication 2, so it survives losing one
DataNode. The per-symbol cache files are deliberately replication 1: a lost one
is detected by the failed read and rebuilt from the big file.
"""
import io
import logging
from dataclasses import dataclass
from enum import Enum

import numpy as np
import pyarrow.parquet as pq

from mdp import layout
from mdp.hdfs import HdfsError, HdfsNotFound

CACHE_REPLICATION = 1
log = logging.getLogger("backfill")


class Outcome(Enum):
    CREATED = "created"
    REUSED = "reused"
    RECREATED = "recreated"
    UNAVAILABLE = "unavailable"


class UnknownSymbol(LookupError):
    pass


@dataclass
class Summary:
    ticker: str
    rows: int
    avg_close: float
    vwap: float
    first_date: str
    last_date: str


def summarize_rows(ticker: str, table) -> Summary:
    close = table.column("close").to_numpy()
    typical = (table.column("high").to_numpy() + table.column("low").to_numpy() + close) / 3.0
    volume = table.column("volume").to_numpy().astype(np.float64)
    dates = table.column("trade_date").to_pylist()
    return Summary(
        ticker=ticker,
        rows=table.num_rows,
        avg_close=float(close.mean()),
        vwap=float((typical * volume).sum() / volume.sum()),
        first_date=min(dates).isoformat(),
        last_date=max(dates).isoformat(),
    )


class SymbolSummaries:
    def __init__(self, hdfs):
        self.hdfs = hdfs

    def _from_source(self, ticker: str, source: str):
        f = self.hdfs.open(source)
        try:
            table = pq.read_table(f, filters=[("ticker", "=", ticker)])
        finally:
            f.close()
        if table.num_rows == 0:
            raise UnknownSymbol(f"{ticker} not in {source}")
        return table

    def _write_cache(self, path: str, table) -> bool:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        try:
            self.hdfs.write(path, buf.getvalue(), replication=CACHE_REPLICATION)
            return True
        except HdfsError as exc:
            log.warning("cache write failed for %s: %s", path, exc)
            return False

    def summarize(self, ticker: str, source: str) -> tuple[Summary, Outcome, str]:
        path = layout.cache_path(source, ticker)
        if self.hdfs.status(path) is None:
            outcome = Outcome.CREATED
        else:
            try:
                table = pq.read_table(io.BytesIO(self.hdfs.read(path, attempts=3)))
                return summarize_rows(ticker, table), Outcome.REUSED, path
            except (HdfsError, OSError, ValueError) as exc:
                if isinstance(exc, HdfsNotFound):
                    outcome = Outcome.CREATED
                else:
                    log.info("cache %s unreadable (%s); rebuilding", path, exc)
                    outcome = Outcome.RECREATED
        table = self._from_source(ticker, source)
        if not self._write_cache(path, table):
            outcome = Outcome.UNAVAILABLE
        return summarize_rows(ticker, table), outcome, path

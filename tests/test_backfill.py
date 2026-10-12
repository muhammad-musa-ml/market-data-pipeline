import datetime as dt
import io

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakes import FakeStore
from mdp import layout
from mdp.backfill import blocks, export, summary
from mdp.hdfs import HdfsError, HdfsNotFound

SOURCE = "/market/exports/nyse.parquet"


class FakeHdfs(FakeStore):
    def __init__(self):
        super().__init__()
        self.unreadable: set[str] = set()
        self.unwritable: set[str] = set()
        self.reads: list[str] = []

    def read(self, path, offset=0, length=None, attempts=5):
        self.reads.append(path)
        if path in self.unreadable:
            raise HdfsError(f"OPEN {path}: block lost")
        return super().read(path, offset, length)

    def write(self, path, data, **kw):
        if path in self.unwritable:
            raise HdfsError("no datanode")
        super().write(path, data, **kw)

    def open(self, path):
        return io.BytesIO(self.read(path))


def bars_table():
    days = [dt.date(2020, 1, d) for d in range(1, 6)]
    rows = []
    for ticker, base in (("AAA", 10.0), ("BBB", 50.0)):
        for i, d in enumerate(days):
            c = base + i
            rows.append((ticker, d, c + 1, c - 1, c, 100 * (i + 1)))
    cols = list(zip(*rows))
    return pa.table({
        "ticker": pa.array(cols[0], pa.string()),
        "trade_date": pa.array(cols[1], pa.date32()),
        "high": pa.array(cols[2], pa.float64()),
        "low": pa.array(cols[3], pa.float64()),
        "close": pa.array(cols[4], pa.float64()),
        "volume": pa.array(cols[5], pa.int64()),
    })


@pytest.fixture
def hdfs():
    h = FakeHdfs()
    buf = io.BytesIO()
    pq.write_table(bars_table(), buf)
    h.write(SOURCE, buf.getvalue(), replication=2)
    return h


def test_first_call_creates_the_cache_at_replication_one(hdfs):
    s, outcome, path = summary.SymbolSummaries(hdfs).summarize("AAA", SOURCE)
    assert outcome is summary.Outcome.CREATED
    assert path == layout.cache_path(SOURCE, "AAA") == "/market/cache/nyse/AAA.parquet"
    assert hdfs.opts[path]["replication"] == 1
    assert s.rows == 5 and s.first_date == "2020-01-01" and s.last_date == "2020-01-05"
    assert s.avg_close == pytest.approx(12.0)


def test_vwap_uses_typical_price_weighted_by_volume(hdfs):
    s, _, _ = summary.SymbolSummaries(hdfs).summarize("BBB", SOURCE)
    closes = np.array([50, 51, 52, 53, 54.0])
    vol = np.array([100, 200, 300, 400, 500.0])
    typical = ((closes + 1) + (closes - 1) + closes) / 3
    assert s.vwap == pytest.approx((typical * vol).sum() / vol.sum())


def test_second_call_reuses_without_touching_the_source(hdfs):
    svc = summary.SymbolSummaries(hdfs)
    first, _, _ = svc.summarize("AAA", SOURCE)
    hdfs.reads.clear()
    again, outcome, _ = svc.summarize("AAA", SOURCE)
    assert outcome is summary.Outcome.REUSED
    assert again == first
    assert hdfs.reads == ["/market/cache/nyse/AAA.parquet"]


def test_lost_cache_file_is_rebuilt_from_the_source(hdfs):
    svc = summary.SymbolSummaries(hdfs)
    first, _, path = svc.summarize("AAA", SOURCE)
    hdfs.unreadable.add(path)
    again, outcome, _ = svc.summarize("AAA", SOURCE)
    assert outcome is summary.Outcome.RECREATED
    assert again == first
    hdfs.unreadable.clear()
    assert svc.summarize("AAA", SOURCE)[1] is summary.Outcome.REUSED


def test_corrupt_cache_file_is_rebuilt(hdfs):
    svc = summary.SymbolSummaries(hdfs)
    _, _, path = svc.summarize("AAA", SOURCE)
    hdfs.files[path] = b"not parquet"
    assert svc.summarize("AAA", SOURCE)[1] is summary.Outcome.RECREATED


def test_unknown_symbol_is_an_error_and_creates_nothing(hdfs):
    with pytest.raises(summary.UnknownSymbol):
        summary.SymbolSummaries(hdfs).summarize("NOPE", SOURCE)
    assert not any(p.startswith(layout.CACHE_DIR) for p in hdfs.files)


def test_missing_source_file_propagates(hdfs):
    with pytest.raises(HdfsNotFound):
        summary.SymbolSummaries(hdfs).summarize("AAA", "/market/exports/other.parquet")


def test_answer_survives_a_failed_cache_write(hdfs):
    hdfs.unwritable.add(layout.cache_path(SOURCE, "AAA"))
    s, outcome, _ = summary.SymbolSummaries(hdfs).summarize("AAA", SOURCE)
    assert outcome is summary.Outcome.UNAVAILABLE and s.rows == 5


@pytest.mark.parametrize("path", [
    "/tmp/x.parquet", "/market/exports/../bars/x.parquet", "/market/exports/x.csv", "x.parquet"])
def test_export_destination_must_stay_in_the_export_dir(path):
    with pytest.raises(export.BadRequest):
        export.check_dest(path)
    export.check_dest("/market/exports/ok.parquet")


def test_export_filters_by_name_through_a_bound_parameter():
    for dim, col in (("sector", "s.sector"), ("exchange", "s.exchange")):
        sql = export.export_query(dim)
        assert f"WHERE {col} = %s" in sql and "JOIN symbols s" in sql
    with pytest.raises(export.BadRequest):
        export.export_query("symbol_id; DROP TABLE bars")


def test_export_table_types_with_no_rows():
    t = export.rows_to_table([])
    assert t.num_rows == 0 and t.schema.field("trade_date").type == pa.date32()


class FakeCursor:
    def __init__(self, rows, symbols):
        self.rows, self.symbols, self.sql = rows, symbols, None

    def execute(self, sql, params=()):
        self.sql = sql

    def fetchone(self):
        return (self.symbols,)

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, rows, symbols=1):
        self.cur = FakeCursor(rows, symbols)

    def cursor(self):
        return self.cur

    def commit(self):
        pass


def test_export_writes_one_file_with_the_documented_layout():
    rows = [(1, "AAA", "Energy", "NYSE", dt.date(2020, 1, 1), 1, 2, 0.5, 1.5, 10),
            (2, "AAA", "Energy", "NYSE", dt.date(2020, 1, 2), 1, 2, 0.5, 1.6, 11)]
    h = FakeHdfs()
    res = export.export_bars(FakeConn(rows), h, "sector", "Energy", "/market/exports/e.parquet")
    assert res.rows == 2
    assert h.opts["/market/exports/e.parquet"] == {"replication": 2, "block_size": 1 << 20}
    got = pq.read_table(io.BytesIO(h.files["/market/exports/e.parquet"]))
    assert got.column("close").to_pylist() == [1.5, 1.6]


def test_export_rejects_a_sector_that_does_not_exist():
    with pytest.raises(export.BadRequest):
        export.export_bars(FakeConn([], symbols=0), FakeHdfs(), "sector", "Nope", "/market/exports/e.parquet")


def test_block_distribution_counts_per_datanode():
    class H:
        def status(self, p):
            return {"replication": 2, "length": 3_000_000, "blockSize": 1 << 20}

        def block_locations(self, p):
            return [["datanode1", "datanode2"], ["datanode2", "datanode3"], ["datanode1", "datanode3"],
                    ["datanode3", "datanode1"]]

    d = blocks.block_distribution(H(), "/x")
    assert d.total_blocks == 4 and d.replication == 2
    assert d.per_datanode == {"datanode1": 3, "datanode2": 2, "datanode3": 3}
    assert sum(d.per_datanode.values()) == d.total_blocks * d.replication


def test_block_distribution_of_missing_file():
    class H:
        def status(self, p):
            return None

    with pytest.raises(HdfsNotFound):
        blocks.block_distribution(H(), "/nope")

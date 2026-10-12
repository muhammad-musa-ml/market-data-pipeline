"""The migration files, the statement splitter, and how extract rows map onto the store."""
import datetime as dt
import io
import json
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdp.serving import init_schema, loader
from mdp.serving.models import Bar, Company
from serving_fakes import MemoryStore

CQL = sorted((init_schema.SQL_DIR / "cassandra").glob("*.cql"))
MYSQL = sorted((init_schema.SQL_DIR / "mysql").glob("*.sql"))


def test_statement_splitter_drops_comments_and_empty_statements():
    text = "-- a comment; with a semicolon\nCREATE TABLE a (x int); -- trailing\n\nCREATE TABLE b (y int);\n;\n"
    assert init_schema.statements(text) == ["CREATE TABLE a (x int)", "CREATE TABLE b (y int)"]


def test_cql_files_substitute_the_keyspace_and_keep_the_map_braces():
    stmts = init_schema.cql_statements(CQL[0], "test")
    assert stmts[0].startswith("CREATE KEYSPACE IF NOT EXISTS test")
    assert "'replication_factor': 3" in stmts[0] and "SimpleStrategy" in stmts[0]
    assert all("${" not in s for s in stmts)
    assert all("test." in s for s in stmts[1:])


def test_every_cql_statement_is_idempotent():
    for path in CQL:
        for stmt in init_schema.cql_statements(path, "prod"):
            assert "IF NOT EXISTS" in stmt, stmt[:60]
    for path in MYSQL:
        for stmt in init_schema.statements(path.read_text()):
            assert "IF NOT EXISTS" in stmt, stmt[:60]


def table(name: str) -> str:
    return next(s for p in CQL for s in init_schema.cql_statements(p, "prod") if f"prod.{name} " in s)


def test_daily_bars_is_partitioned_by_ticker_and_clustered_newest_first_with_static_metadata():
    ddl = table("daily_bars")
    assert "PRIMARY KEY ((ticker), trade_date)" in ddl
    assert "CLUSTERING ORDER BY (trade_date DESC)" in ddl
    for col in ("name", "sector", "exchange"):
        assert re.search(rf"\b{col} text static\b", ddl)
    assert not re.search(r"\b(open|high|low|close|volume) \w+ static", ddl)


def test_ticks_partition_is_bounded_by_day_and_the_key_includes_the_producer():
    ddl = table("ticks")
    assert "PRIMARY KEY ((ticker, day), ts_us, producer)" in ddl


def test_every_table_the_api_reads_has_a_matching_column_in_the_extract_mapping():
    # the API's columns are what bars_from produces, so the schema must have them all
    ddl = table("daily_bars")
    for col in ("ticker", "trade_date", "open", "high", "low", "close", "volume"):
        assert re.search(rf"\b{col} \w+", ddl)


def extract_table(prices=(10.0, 10.12345678)):
    n = len(prices)
    return pa.table({
        "ticker": pa.array(["AAA"] * n, pa.string()),
        "name": pa.array(["Triple A"] * n), "sector": pa.array(["Energy"] * n),
        "exchange": pa.array(["NYSE"] * n),
        "trade_date": pa.array([dt.date(2024, 1, 2 + i) for i in range(n)], pa.date32()),
        "open": pa.array(prices, pa.float64()), "high": pa.array([p + 1 for p in prices], pa.float64()),
        "low": pa.array([p - 1 for p in prices], pa.float64()), "close": pa.array(prices, pa.float64()),
        "volume": pa.array([100 + i for i in range(n)], pa.int64()),
    })


def test_companies_come_out_once_per_ticker():
    t = extract_table()
    assert loader.companies_from(t) == [Company("AAA", "Triple A", "Energy", "NYSE")]


def test_bars_are_rounded_like_api_input_and_keep_python_types():
    rows = loader.bars_from(extract_table())
    assert [t for t, _ in rows] == ["AAA", "AAA"]
    b = rows[1][1]
    assert b == Bar(dt.date(2024, 1, 3), 10.1235, 11.1235, 9.1235, 10.1235, 101)
    assert type(b.volume) is int and type(b.open) is float


class FakeHdfs:
    def __init__(self, files):
        self.files = files

    def exists(self, p):
        return p in self.files

    def read(self, p):
        return self.files[p]

    def listdir(self, p):
        pre = p.rstrip("/") + "/"
        return sorted(f[len(pre):] for f in self.files if f.startswith(pre))


def parquet(t) -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf)
    return buf.getvalue()


def hdfs_with(rows_claimed=None, manifest=True):
    t = extract_table()
    files = {f"{loader.SERVING}/daily_bars/part-0.parquet": parquet(t),
             f"{loader.SERVING}/daily_bars/_SUCCESS": b""}
    if manifest:
        files[loader.MANIFEST] = json.dumps({
            "built_at": "2026-01-01T00:00:00Z",
            "datasets": {"daily_bars": {"rows": rows_claimed if rows_claimed is not None else t.num_rows}}}).encode()
    return FakeHdfs(files)


def test_load_writes_companies_before_bars_and_counts_rows():
    store = MemoryStore()
    res = loader.load(store, hdfs_with(), log=lambda *_: None)
    assert res["rows"] == 2 and res["tickers"] == 1 and res["files"] == 1
    assert len(store.rows["AAA"]) == 2 and store.companies["AAA"].name == "Triple A"


def test_loading_twice_changes_nothing():
    store = MemoryStore()
    loader.load(store, hdfs_with(), log=lambda *_: None)
    snapshot = ({k: dict(v) for k, v in store.rows.items()}, dict(store.companies))
    loader.load(store, hdfs_with(), log=lambda *_: None)
    assert ({k: dict(v) for k, v in store.rows.items()}, dict(store.companies)) == snapshot


def test_missing_manifest_means_the_extract_is_incomplete():
    with pytest.raises(loader.ExtractIncomplete, match="manifest"):
        loader.load(MemoryStore(), hdfs_with(manifest=False), log=lambda *_: None)


def test_files_that_disagree_with_the_manifest_are_refused():
    with pytest.raises(loader.ExtractIncomplete, match="manifest says 5"):
        loader.load(MemoryStore(), hdfs_with(rows_claimed=5), log=lambda *_: None)


def test_a_dataset_directory_with_no_parquet_files_is_refused():
    h = FakeHdfs({f"{loader.SERVING}/daily_bars/_SUCCESS": b"", loader.MANIFEST: b"{}"})
    with pytest.raises(loader.ExtractIncomplete, match="no parquet"):
        loader.parquet_files(h, "daily_bars")

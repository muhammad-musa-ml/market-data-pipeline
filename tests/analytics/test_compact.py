import pytest

from mdp.analytics import compact
from mdp.analytics.names import BARS, SYMBOLS


def test_only_trusted_files_end_up_in_the_table(spark, warehouse, raw):
    bars_pd, _ = raw
    r = warehouse["result"]
    assert r["rows"] == len(bars_pd)
    assert spark.table(BARS).where("id >= 10000000").count() == 0
    # the leftover really would have changed the answer, so this is not vacuous
    everything = spark.read.parquet(*warehouse["all_files"]).dropDuplicates(["id"]).count()
    assert everything > r["rows"]


def test_replayed_batch_is_deduplicated_on_id(spark, warehouse, raw):
    bars_pd, _ = raw
    r = warehouse["result"]
    assert r["rows_read"] == len(bars_pd) + warehouse["duplicate_rows"]
    assert r["duplicates_dropped"] == warehouse["duplicate_rows"] > 0
    assert spark.table(BARS).selectExpr("count(distinct id) AS n").first()["n"] == len(bars_pd)


def test_table_is_bucketed_by_ticker_and_sorted_by_date(spark, warehouse):
    desc = {r["col_name"]: r["data_type"] for r in spark.sql(f"DESCRIBE EXTENDED {BARS}").collect()}
    assert desc["Num Buckets"] == "8"
    assert desc["Bucket Columns"] == "[`ticker`]"
    assert desc["Sort Columns"] == "[`trade_date`]"


def test_the_table_has_one_file_per_non_empty_bucket(spark, warehouse):
    # 10 tickers fill only some of the 8 buckets; with 50 tickers all of them are used
    used = spark.table(BARS).selectExpr("count(distinct pmod(hash(ticker), 8)) AS n").first()["n"]
    assert warehouse["result"]["table_files"] == used
    per_ticker = (spark.table(BARS).selectExpr("ticker", "input_file_name() AS f").distinct()
                  .groupBy("ticker").count().agg({"count": "max"}).first()[0])
    assert per_ticker == 1


def test_symbols_table_and_values_are_intact(spark, warehouse, raw):
    bars_pd, syms_pd = raw
    got = {(r.symbol_id, r.ticker, r.sector, r.exchange) for r in spark.table(SYMBOLS).collect()}
    assert got == set(syms_pd[["symbol_id", "ticker", "sector", "exchange"]].itertuples(index=False, name=None))
    row = spark.table(BARS).where("id = 12345").first()
    exp = bars_pd[bars_pd["id"] == 12345].iloc[0]
    assert (row.ticker, row.volume, row.close) == (exp["ticker"], exp["volume"], exp["close"])


def test_catalog_carries_comments(spark, warehouse):
    cols = {c.name: c.description for c in spark.catalog.listColumns(BARS)}
    assert "closing price" in cols["close"]
    tables = {t.name: t for t in spark.catalog.listTables("market")}
    assert "bucketed by ticker" in tables["bars"].description
    assert tables["daily_returns"].tableType == "VIEW"


def test_no_trusted_files_is_an_error(spark):
    with pytest.raises(ValueError, match="no trusted"):
        compact.read_trusted(spark, [])

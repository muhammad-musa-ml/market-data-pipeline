"""Compaction and load: trusted stream batch files -> bucketed Hive table of bars.

Only the files below each partition's checkpoint are read (docs/CONTRACT.md), never a glob of
/market/bars, so leftovers from a sink crash cannot get in. One file per bucket comes out instead
of one per poll batch.
"""
import time

from pyspark.sql import DataFrame, SparkSession

from mdp import layout
from mdp.analytics.names import BARS, DB, HDFS_URI, N_BUCKETS, RETURNS, SYMBOLS

BAR_COMMENTS = {
    "id": "stream position and primary key of the bar",
    "symbol_id": "key into symbols.symbol_id",
    "ticker": "ticker symbol, the bucketing column",
    "trade_date": "trading day (weekdays, no holidays)",
    "open": "opening price, USD",
    "high": "highest price of the day, USD",
    "low": "lowest price of the day, USD",
    "close": "closing price, USD",
    "volume": "shares traded that day",
    "inserted_at": "when the bar was written to MySQL, UTC",
}
SYMBOL_COMMENTS = {
    "symbol_id": "primary key; bars.symbol_id refers to it",
    "ticker": "ticker symbol, unique",
    "name": "company name",
    "sector": "one of 8 sectors",
    "exchange": "NASDAQ or NYSE",
}


def with_comments(df: DataFrame, comments: dict[str, str]) -> DataFrame:
    for col, text in comments.items():
        df = df.withMetadata(col, {"comment": text})
    return df


def read_trusted(spark: SparkSession, paths: list[str]) -> DataFrame:
    if not paths:
        raise ValueError("the sink has no trusted batch files yet")
    return spark.read.parquet(*paths)


def write_bars(bars: DataFrame, table: str = BARS, buckets: int = N_BUCKETS) -> None:
    # repartitioning by the bucket expression first means each task holds exactly one bucket,
    # so the table ends up with `buckets` files instead of one per task per bucket
    (with_comments(bars, BAR_COMMENTS)
     .repartition(buckets, "ticker")
     .write.mode("overwrite")
     .bucketBy(buckets, "ticker").sortBy("trade_date")
     .format("parquet").saveAsTable(table))


def write_symbols(symbols: DataFrame, table: str = SYMBOLS) -> None:
    (with_comments(symbols, SYMBOL_COMMENTS).coalesce(1)
     .write.mode("overwrite").format("parquet").saveAsTable(table))


def describe_tables(spark: SparkSession) -> None:
    spark.sql(f"ALTER TABLE {BARS} SET TBLPROPERTIES ('comment' = "
              "'daily OHLCV bars, one row per symbol per trading day; bucketed by ticker, sorted by trade_date')")
    spark.sql(f"ALTER TABLE {SYMBOLS} SET TBLPROPERTIES ('comment' = "
              "'symbol metadata: one row per ticker')")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {RETURNS}
          (ticker COMMENT 'ticker symbol',
           trade_date COMMENT 'trading day',
           close COMMENT 'closing price, USD',
           ret COMMENT 'log return from the previous trading day; NULL on the first day of each ticker')
          COMMENT 'daily close-to-close log returns per ticker'
        AS SELECT ticker, trade_date, close,
                  ln(close / lag(close) OVER (PARTITION BY ticker ORDER BY trade_date)) AS ret
           FROM {BARS}""")


def run(spark: SparkSession, paths: list[str], symbols_path: str = f"{HDFS_URI}{layout.SYMBOLS_DIM}",
        buckets: int = N_BUCKETS) -> dict:
    t0 = time.time()
    raw = read_trusted(spark, paths)
    rows_read = raw.count()
    bars = raw.dropDuplicates(["id"])

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    write_bars(bars, buckets=buckets)
    write_symbols(spark.read.parquet(symbols_path))
    describe_tables(spark)
    spark.sql(f"ANALYZE TABLE {BARS} COMPUTE STATISTICS")
    spark.sql(f"ANALYZE TABLE {SYMBOLS} COMPUTE STATISTICS")

    rows = spark.table(BARS).count()
    return {
        "trusted_files": len(paths),
        "rows_read": rows_read,
        "duplicates_dropped": rows_read - rows,
        "rows": rows,
        "symbols": spark.table(SYMBOLS).count(),
        "table_files": len(spark.table(BARS).inputFiles()),
        "buckets": buckets,
        "seconds": round(time.time() - t0, 2),
    }


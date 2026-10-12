"""Preprocessed extract for the serving tier (stage 3). Schemas are in docs/CONTRACT.md.

Reads the Hive tables, so it reflects the last compaction. The manifest is written last and
says what the extract covers; a reader that sees no manifest, or one that disagrees with the
data, should treat the extract as incomplete.
"""
import json
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from mdp.analytics.names import (
    BARS, HDFS_URI, N_BUCKETS, SERVING_DAILY, SERVING_MANIFEST, SERVING_MONTHLY, SERVING_ROLLUPS, SYMBOLS,
)

TRADING_DAYS = 252


def daily_bars(bars: DataFrame, symbols: DataFrame) -> DataFrame:
    w = Window.partitionBy("ticker").orderBy("trade_date")
    return (bars.join(symbols, ["symbol_id", "ticker"])
            .withColumn("ret", F.log(F.col("close") / F.lag("close", 1).over(w)))
            .withColumn("vol_20", F.stddev("ret").over(w.rowsBetween(-19, 0)))
            .withColumn("n_prior", F.count("ret").over(w.rowsBetween(-19, 0)))
            .withColumn("vol_20", F.when(F.col("n_prior") >= 20, F.col("vol_20")))
            .select("symbol_id", "ticker", "name", "sector", "exchange", "trade_date",
                    "open", "high", "low", "close", "volume", "ret", "vol_20"))


def monthly_bars(daily: DataFrame) -> DataFrame:
    w = Window.partitionBy("ticker").orderBy("month")
    m = (daily.withColumn("month", F.date_format("trade_date", "yyyy-MM"))
         .withColumn("typical", (F.col("high") + F.col("low") + F.col("close")) / 3)
         .groupBy("symbol_id", "ticker", "sector", "month")
         .agg(F.min_by("open", "trade_date").alias("open"),
              F.max("high").alias("high"),
              F.min("low").alias("low"),
              F.max_by("close", "trade_date").alias("close"),
              F.avg("close").alias("avg_close"),
              F.sum("volume").alias("volume"),
              F.count("*").alias("trading_days")))
    return (m.withColumn("prev_close", F.lag("close", 1).over(w))
            .withColumn("month_return", F.col("close") / F.col("prev_close") - 1)
            .drop("prev_close"))


def symbol_rollups(daily: DataFrame) -> DataFrame:
    w = Window.partitionBy("ticker").orderBy("trade_date")
    d = (daily.withColumn("peak", F.max("close").over(w.rowsBetween(Window.unboundedPreceding, 0)))
         .withColumn("drawdown", F.col("close") / F.col("peak") - 1)
         .withColumn("typical", (F.col("high") + F.col("low") + F.col("close")) / 3))
    return (d.groupBy("symbol_id", "ticker", "name", "sector", "exchange")
            .agg(F.min("trade_date").alias("first_date"),
                 F.max("trade_date").alias("last_date"),
                 F.count("*").alias("n_bars"),
                 F.avg("close").alias("avg_close"),
                 (F.sum(F.col("typical") * F.col("volume")) / F.sum("volume")).alias("vwap"),
                 F.min("close").alias("min_close"),
                 F.max("close").alias("max_close"),
                 F.avg("volume").alias("avg_volume"),
                 (F.max_by("close", "trade_date") / F.min_by("close", "trade_date") - 1).alias("total_return"),
                 (F.stddev("ret") * (TRADING_DAYS ** 0.5)).alias("annual_vol"),
                 F.min("drawdown").alias("max_drawdown")))


def _write(df: DataFrame, base_uri: str, path: str, sort: list[str]) -> int:
    df = df.repartition(N_BUCKETS, "ticker").sortWithinPartitions(*sort)
    df.write.mode("overwrite").parquet(base_uri + path)
    return df.sparkSession.read.parquet(base_uri + path).count()


def run(spark: SparkSession, hdfs=None, trusted_files: int | None = None, base_uri: str = HDFS_URI) -> dict:
    t0 = time.time()
    bars = spark.table(BARS)
    symbols = spark.table(SYMBOLS)
    daily = daily_bars(bars, symbols).cache()
    counts = {
        "daily_bars": _write(daily, base_uri, SERVING_DAILY, ["ticker", "trade_date"]),
        "monthly_bars": _write(monthly_bars(daily), base_uri, SERVING_MONTHLY, ["ticker", "month"]),
        "symbol_rollups": _write(symbol_rollups(daily), base_uri, SERVING_ROLLUPS, ["ticker"]),
    }
    max_id = bars.agg(F.max("id")).first()[0]
    daily.unpersist()
    manifest = {
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_table": BARS,
        "source_rows": counts["daily_bars"],
        "source_max_id": max_id,
        "trusted_files": trusted_files,
        "datasets": {"daily_bars": {"path": SERVING_DAILY, "rows": counts["daily_bars"]},
                     "monthly_bars": {"path": SERVING_MONTHLY, "rows": counts["monthly_bars"]},
                     "symbol_rollups": {"path": SERVING_ROLLUPS, "rows": counts["symbol_rollups"]}},
    }
    if hdfs is not None:
        hdfs.write(SERVING_MANIFEST, (json.dumps(manifest, indent=2) + "\n").encode())
    manifest["seconds"] = round(time.time() - t0, 1)
    return manifest

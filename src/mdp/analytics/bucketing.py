"""Does bucketing by ticker remove the shuffle?

Two copies of the same data live in the lab database: bars and a per-ticker stats table, once
bucketed by ticker and once as plain files. The same GROUP BY and the same join run on both,
with adaptive execution and broadcast joins off so the plans show the real strategy. A
`BroadcastExchange` is not a shuffle; only `Exchange` nodes are counted.
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdp.analytics.names import BARS, LAB_DB, N_BUCKETS
from mdp.analytics.session import count_exchanges, plan_text

CONF = {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1"}


def _write(df, table: str, bucketed: bool) -> None:
    w = df.write.mode("overwrite").format("parquet")
    if bucketed:
        w = w.bucketBy(N_BUCKETS, "ticker").sortBy("trade_date" if "trade_date" in df.columns else "ticker")
    w.saveAsTable(table)


def build_tables(spark: SparkSession, source: str = BARS) -> dict[str, str]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {LAB_DB}")
    bars = spark.table(source).select("ticker", "trade_date", "close", "volume")
    stats = spark.table(source).groupBy("ticker").agg(
        F.avg("close").alias("avg_close"), F.count("*").alias("n_bars"))
    names = {
        "bars_bucketed": f"{LAB_DB}.bars_bucketed", "bars_flat": f"{LAB_DB}.bars_flat",
        "stats_bucketed": f"{LAB_DB}.stats_bucketed", "stats_flat": f"{LAB_DB}.stats_flat",
    }
    # same rows, same number of files: only the bucket layout differs
    _write(bars.repartition(N_BUCKETS, "ticker"), names["bars_bucketed"], True)
    _write(bars.repartition(N_BUCKETS, "ticker"), names["bars_flat"], False)
    _write(stats.repartition(N_BUCKETS, "ticker"), names["stats_bucketed"], True)
    _write(stats.repartition(N_BUCKETS, "ticker"), names["stats_flat"], False)
    return names


def queries(suffix: str) -> dict[str, str]:
    bars, stats = f"{LAB_DB}.bars_{suffix}", f"{LAB_DB}.stats_{suffix}"
    return {
        "group_by": f"SELECT ticker, COUNT(*) AS n, AVG(close) AS avg_close, MAX(volume) AS peak "
                    f"FROM {bars} GROUP BY ticker",
        "join": f"SELECT b.ticker, COUNT(*) AS n, MAX(b.close / s.avg_close) AS peak_rel "
                f"FROM {bars} b JOIN {stats} s ON b.ticker = s.ticker GROUP BY b.ticker",
    }


def timed(spark: SparkSession, query: str, reps: int) -> list[float]:
    out = []
    for _ in range(reps):
        t0 = time.time()
        spark.sql(query).collect()
        out.append(round(time.time() - t0, 3))
    return out


def run(spark: SparkSession, reps: int = 3, source: str = BARS) -> dict:
    for key, value in CONF.items():
        spark.conf.set(key, value)
    build_tables(spark, source)
    result: dict = {"buckets": N_BUCKETS, "variants": {}}
    for suffix in ("bucketed", "flat"):
        variant = {}
        for name, query in queries(suffix).items():
            plan = plan_text(spark, query)
            spark.sql(query).collect()  # one warm-up, not timed
            variant[name] = {"exchanges": count_exchanges(plan), "bucketed_scan": "Bucketed: true" in plan,
                             "seconds": timed(spark, query, reps), "plan": plan}
        result["variants"][suffix] = variant
    return result

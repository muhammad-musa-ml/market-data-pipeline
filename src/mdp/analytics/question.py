"""One question, three Spark APIs.

    Since 2020-01-01, for every sector and exchange: how many bars, how many of them closed above
    their open, the total shares traded and the busiest single day.

The sector and exchange live in `symbols`, so every version joins bars to symbols on symbol_id.
All four measures are integers so the three answers can be compared for exact equality.
"""
import datetime as dt
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdp.analytics.names import BARS, SYMBOLS

SINCE = dt.date(2020, 1, 1)
COLUMNS = ("sector", "exchange", "bars", "up_days", "total_volume", "peak_volume")

QUESTION = ("Since 2020-01-01, per sector and exchange: bars, days that closed above the open, "
            "total volume and the largest single-day volume")


def with_rdd(spark: SparkSession, since: dt.date = SINCE, bars: str = BARS, symbols: str = SYMBOLS) -> list[tuple]:
    sym = spark.table(symbols).rdd.map(lambda r: (r.symbol_id, (r.sector, r.exchange)))
    fact = (spark.table(bars).rdd
            .filter(lambda r: r.trade_date >= since)
            .map(lambda r: (r.symbol_id, (1, 1 if r.close > r.open else 0, r.volume, r.volume))))
    grouped = (fact.join(sym)
               .map(lambda kv: (kv[1][1], kv[1][0]))
               .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], max(a[3], b[3]))))
    return sorted((s, e, *m) for (s, e), m in grouped.collect())


def with_dataframe(spark: SparkSession, since: dt.date = SINCE, bars: str = BARS, symbols: str = SYMBOLS) -> list[tuple]:
    df = (spark.table(bars).where(F.col("trade_date") >= F.lit(since))
          .join(spark.table(symbols), "symbol_id")
          .groupBy("sector", "exchange")
          .agg(F.count("*").alias("bars"),
               F.sum(F.when(F.col("close") > F.col("open"), 1).otherwise(0)).alias("up_days"),
               F.sum("volume").alias("total_volume"),
               F.max("volume").alias("peak_volume"))
          .orderBy("sector", "exchange"))
    return [tuple(r) for r in df.collect()]


def with_sql(spark: SparkSession, since: dt.date = SINCE, bars: str = BARS, symbols: str = SYMBOLS) -> list[tuple]:
    rows = spark.sql(f"""
        SELECT s.sector, s.exchange,
               COUNT(*) AS bars,
               SUM(CASE WHEN b.close > b.open THEN 1 ELSE 0 END) AS up_days,
               SUM(b.volume) AS total_volume,
               MAX(b.volume) AS peak_volume
        FROM {bars} b JOIN {symbols} s ON b.symbol_id = s.symbol_id
        WHERE b.trade_date >= DATE '{since.isoformat()}'
        GROUP BY s.sector, s.exchange
        ORDER BY s.sector, s.exchange""").collect()
    return [tuple(r) for r in rows]


def run(spark: SparkSession) -> dict:
    out: dict = {}
    answers = {}
    for name, fn in (("rdd", with_rdd), ("dataframe", with_dataframe), ("sql", with_sql)):
        t0 = time.time()
        answers[name] = fn(spark)
        out[f"{name}_seconds"] = round(time.time() - t0, 2)
    out["identical"] = answers["rdd"] == answers["dataframe"] == answers["sql"]
    out["groups"] = len(answers["sql"])
    out["answer"] = [list(r) for r in answers["sql"]]
    return out

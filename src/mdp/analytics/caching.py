"""Repeated aggregations over an expensive DataFrame, with and without caching.

The DataFrame is the window-function feature table for the Technology sector, so computing it
means a join, several windows and a shuffle. Three aggregates run over it each time.
"""
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mdp.analytics import features as feat
from mdp.analytics.names import BARS, SYMBOLS


def aggregates(df: DataFrame) -> tuple:
    row = df.agg(F.avg("vol_20"), F.avg("absret_20"), F.avg("volume_ratio_20")).first()
    return tuple(row)


def timeit(df: DataFrame) -> float:
    t0 = time.time()
    aggregates(df)
    return round(time.time() - t0, 3)


def run(spark: SparkSession, repeats: int = 3, sector: str = "Technology") -> dict:
    df = (feat.daily_features(spark.table(BARS), spark.table(SYMBOLS))
          .where(F.col("sector") == sector))
    aggregates(df)  # warm-up: the first job on a fresh cluster also pays for starting executors
    uncached = [timeit(df) for _ in range(repeats)]

    df.cache()
    fill = timeit(df)  # the first action pays for materialising the cache
    cached = [timeit(df) for _ in range(repeats)]
    df.unpersist()
    return {"sector": sector, "uncached_seconds": uncached, "cache_fill_seconds": fill,
            "cached_seconds": cached}

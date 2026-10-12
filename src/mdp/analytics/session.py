import re

from pyspark.sql import SparkSession


def get_spark(app: str, conf: dict[str, str] | None = None) -> SparkSession:
    """The cluster, metastore and warehouse settings come from spark-defaults.conf in the image."""
    builder = SparkSession.builder.appName(app)
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def plan_text(spark: SparkSession, query: str) -> str:
    """The physical plan Spark would run for a SQL query, as text."""
    return spark.sql("EXPLAIN " + query).first()[0]


def count_exchanges(plan: str) -> int:
    """Shuffle nodes in a physical plan (a BroadcastExchange is a broadcast, not a shuffle)."""
    return len(re.findall(r"(?m)^[\s+:|\-]*Exchange\b", plan))

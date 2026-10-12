"""Make a table bigger without regenerating stage 1: stack shifted copies of the bars.

The copies repeat the same price paths 20000 trading-day-lengths apart in time, so the shape of
the data (50 tickers, one row per ticker-day) is unchanged and only the row count grows. It is for
timing experiments; nothing it makes is written to the market database.
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

STEP_DAYS = 20000


def inflate(df: DataFrame, copies: int) -> DataFrame:
    if copies <= 1:
        return df
    return reduce(DataFrame.unionAll,
                  [df.withColumn("trade_date", F.date_add("trade_date", i * STEP_DAYS)) for i in range(copies)])

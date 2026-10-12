"""Window-function features for next-days volatility, and a time-based split.

The target is the mean absolute daily log return over the next HORIZON trading days. Every
feature uses only rows up to and including the day itself (window frames end at the current
row), so a row can be scored at the close of that day. tests/analytics/test_features.py
changes all future bars and checks the features of earlier days do not move.
"""
import datetime as dt

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HORIZON = 5
WARMUP = 60  # longest trailing window; earlier rows of a ticker lack some feature

NUMERIC = [
    "ret_1d", "ret_lag1", "ret_lag2", "ret_lag3", "ret_lag4",
    "vol_5", "vol_20", "vol_60", "absret_20",
    "volume_ratio_5", "volume_ratio_20", "volume_chg", "mkt_vol_20",
]
CATEGORICAL = "sector"
LABEL = "target"


def daily_features(bars: DataFrame, symbols: DataFrame) -> DataFrame:
    """One row per ticker-day with the features above, the label (null for the last days), and
    the keys. Rows inside the warm-up period are dropped."""
    w = Window.partitionBy("ticker").orderBy("trade_date")

    def trailing(n: int):
        return w.rowsBetween(-(n - 1), 0)

    df = bars.join(symbols.select("symbol_id", "sector"), "symbol_id")
    df = df.withColumn("ret_1d", F.log(F.col("close") / F.lag("close", 1).over(w)))
    for k in range(1, 5):
        df = df.withColumn(f"ret_lag{k}", F.lag("ret_1d", k).over(w))
    df = (df
          .withColumn("vol_5", F.stddev("ret_1d").over(trailing(5)))
          .withColumn("vol_20", F.stddev("ret_1d").over(trailing(20)))
          .withColumn("vol_60", F.stddev("ret_1d").over(trailing(60)))
          .withColumn("absret_20", F.avg(F.abs("ret_1d")).over(trailing(20)))
          .withColumn("volume_ratio_5", F.col("volume") / F.avg("volume").over(trailing(5)))
          .withColumn("volume_ratio_20", F.col("volume") / F.avg("volume").over(trailing(20)))
          .withColumn("volume_chg", F.log(F.col("volume") / F.lag("volume", 1).over(w)))
          .withColumn("_rn", F.row_number().over(w)))

    ahead = w.rowsBetween(1, HORIZON)
    df = df.withColumn(
        LABEL,
        F.when(F.count("ret_1d").over(ahead) == HORIZON, F.avg(F.abs("ret_1d")).over(ahead)))

    df = df.where(F.col("_rn") > WARMUP)
    df = df.withColumn("mkt_vol_20", F.avg("vol_20").over(Window.partitionBy("trade_date")))
    return df.select("ticker", "sector", "trade_date", *NUMERIC, LABEL)


def split_dates(dates: list[dt.date], test_fraction: float = 0.2, horizon: int = HORIZON) -> tuple[dt.date, dt.date]:
    """(last training date, first test date) for a sorted list of labelled dates.

    A training row's label looks `horizon` days ahead, so training stops `horizon` days before
    the test period starts; those days are dropped from both sides.
    """
    ds = sorted(set(dates))
    k = int(len(ds) * (1 - test_fraction))
    if k - horizon - 1 < 0 or k >= len(ds):
        raise ValueError("too few dates to split")
    return ds[k - horizon - 1], ds[k]


def time_split(labelled: DataFrame, test_fraction: float = 0.2, horizon: int = HORIZON):
    dates = [r[0] for r in labelled.select("trade_date").distinct().collect()]
    last_train, first_test = split_dates(dates, test_fraction, horizon)
    train = labelled.where(F.col("trade_date") <= F.lit(last_train))
    test = labelled.where(F.col("trade_date") >= F.lit(first_test))
    return train, test, {"last_train_date": last_train.isoformat(), "first_test_date": first_test.isoformat()}

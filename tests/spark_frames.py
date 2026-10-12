"""Small bars and symbols frames from the seeded generator, in the schema the sink writes."""
import datetime as dt

import numpy as np
import pandas as pd

from mdp.gen import market


def make_pandas(n: int = 10, days: int = 2300, seed: int = 3, clustering: bool = True):
    uni = market.Universe(market.make_symbols(n, seed), seed, vol_clustering=clustering)
    prev = uni.initial_close()
    rows = []
    next_id = 1
    for d in range(days):
        bars = uni.next_day(prev, d)
        prev = bars.close
        date = market.trading_date(d)
        for i, s in enumerate(uni.specs):
            rows.append((next_id, s.symbol_id, s.ticker, date, float(bars.open[i]), float(bars.high[i]),
                         float(bars.low[i]), float(bars.close[i]), int(bars.volume[i]),
                         dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=next_id)))
            next_id += 1
    bars_df = pd.DataFrame(rows, columns=["id", "symbol_id", "ticker", "trade_date", "open", "high", "low",
                                          "close", "volume", "inserted_at"])
    syms = pd.DataFrame([(s.symbol_id, s.ticker, s.name, s.sector, s.exchange) for s in uni.specs],
                        columns=["symbol_id", "ticker", "name", "sector", "exchange"])
    return bars_df, syms


BARS_DDL = ("id BIGINT, symbol_id INT, ticker STRING, trade_date DATE, open DOUBLE, high DOUBLE, "
            "low DOUBLE, close DOUBLE, volume BIGINT, inserted_at TIMESTAMP")
SYMBOLS_DDL = "symbol_id INT, ticker STRING, name STRING, sector STRING, exchange STRING"


def to_spark(spark, bars: pd.DataFrame, syms: pd.DataFrame):
    # plain python values: Spark rejects pandas Timestamps and numpy scalars
    rows = [(*rec[:-1], rec[-1].to_pydatetime()) for rec in bars.itertuples(index=False)]
    return (spark.createDataFrame(rows, BARS_DDL),
            spark.createDataFrame([tuple(r) for r in syms.itertuples(index=False)], SYMBOLS_DDL))


def pandas_returns(bars: pd.DataFrame) -> pd.DataFrame:
    b = bars.sort_values(["ticker", "trade_date"]).copy()
    b["ret"] = b.groupby("ticker")["close"].transform(lambda c: np.log(c / c.shift(1)))
    return b

import json
import math

import numpy as np
import pandas as pd
import pytest

from mdp.analytics import serving
from mdp.analytics.names import BARS, SYMBOLS
from spark_frames import pandas_returns


@pytest.fixture(scope="module")
def frames(spark, warehouse):
    daily = serving.daily_bars(spark.table(BARS), spark.table(SYMBOLS)).cache()
    yield daily, serving.monthly_bars(daily), serving.symbol_rollups(daily)
    daily.unpersist()


def test_daily_bars_carry_symbol_metadata_and_a_return(frames, raw):
    daily = frames[0].toPandas()
    bars, syms = raw
    assert len(daily) == len(bars)
    assert list(daily.columns) == ["symbol_id", "ticker", "name", "sector", "exchange", "trade_date",
                                   "open", "high", "low", "close", "volume", "ret", "vol_20"]
    t = syms.iloc[0]
    one = daily[daily["ticker"] == t["ticker"]].sort_values("trade_date").reset_index(drop=True)
    assert set(one["sector"]) == {t["sector"]} and set(one["name"]) == {t["name"]}
    assert pd.isna(one.loc[0, "ret"]) and one["ret"].iloc[1:].notna().all()
    assert one["vol_20"].iloc[:20].isna().all() and one["vol_20"].iloc[20:].notna().all()
    ref = pandas_returns(bars)
    ref = ref[ref["ticker"] == t["ticker"]].reset_index(drop=True)
    np.testing.assert_allclose(one["ret"].to_numpy(dtype=float), ref["ret"].to_numpy(dtype=float), equal_nan=True)
    np.testing.assert_allclose(one["vol_20"].to_numpy(dtype=float),
                               ref["ret"].rolling(20).std().to_numpy(dtype=float), equal_nan=True)


def test_rollups_match_pandas(frames, raw):
    roll = frames[2].toPandas().set_index("ticker")
    bars, _ = raw
    assert len(roll) == bars["ticker"].nunique()
    for ticker, g in bars.groupby("ticker"):
        g = g.sort_values("trade_date")
        r = roll.loc[ticker]
        typical = (g["high"] + g["low"] + g["close"]) / 3
        assert r["n_bars"] == len(g)
        assert r["first_date"] == g["trade_date"].iloc[0] and r["last_date"] == g["trade_date"].iloc[-1]
        assert math.isclose(r["avg_close"], g["close"].mean(), rel_tol=1e-12)
        assert math.isclose(r["vwap"], (typical * g["volume"]).sum() / g["volume"].sum(), rel_tol=1e-12)
        assert math.isclose(r["total_return"], g["close"].iloc[-1] / g["close"].iloc[0] - 1, rel_tol=1e-9)
        peak = g["close"].cummax()
        assert math.isclose(r["max_drawdown"], (g["close"] / peak - 1).min(), rel_tol=1e-9)
        rets = np.log(g["close"] / g["close"].shift(1))
        assert math.isclose(r["annual_vol"], rets.std() * math.sqrt(252), rel_tol=1e-9)


def test_monthly_bars_open_high_low_close(frames, raw):
    monthly = frames[1].toPandas()
    bars, _ = raw
    t = sorted(bars["ticker"].unique())[2]
    g = bars[bars["ticker"] == t].copy()
    g["month"] = [d.strftime("%Y-%m") for d in g["trade_date"]]
    m = monthly[monthly["ticker"] == t].set_index("month")
    assert len(m) == g["month"].nunique()
    for month, gg in g.groupby("month"):
        gg = gg.sort_values("trade_date")
        r = m.loc[month]
        assert r["open"] == gg["open"].iloc[0] and r["close"] == gg["close"].iloc[-1]
        assert r["high"] == gg["high"].max() and r["low"] == gg["low"].min()
        assert r["volume"] == gg["volume"].sum() and r["trading_days"] == len(gg)
        assert math.isclose(r["avg_close"], gg["close"].mean(), rel_tol=1e-12)
    first = m.sort_index().iloc[0]
    assert pd.isna(first["month_return"])


class MemoryStore:
    def __init__(self):
        self.files = {}

    def write(self, path, data, **kw):
        self.files[path] = data


def test_run_writes_three_datasets_and_a_manifest_last(spark, warehouse, tmp_path):
    store = MemoryStore()
    base = tmp_path.as_uri()
    out = serving.run(spark, store, trusted_files=4, base_uri=base)
    manifest = json.loads(store.files["/market/serving/_manifest.json"])
    assert manifest["source_rows"] == spark.table(BARS).count() == out["datasets"]["daily_bars"]["rows"]
    assert manifest["source_max_id"] == spark.table(BARS).agg({"id": "max"}).first()[0]
    assert manifest["trusted_files"] == 4
    assert manifest["datasets"]["symbol_rollups"]["rows"] == 10
    for name, info in manifest["datasets"].items():
        back = spark.read.parquet(base + info["path"])
        assert back.count() == info["rows"], name
    # one ticker per file group: a ticker's rows are never split over more than one file
    daily = spark.read.parquet(base + manifest["datasets"]["daily_bars"]["path"])
    files_per_ticker = daily.selectExpr("ticker", "input_file_name() AS f").distinct().groupBy("ticker").count()
    assert files_per_ticker.agg({"count": "max"}).first()[0] == 1

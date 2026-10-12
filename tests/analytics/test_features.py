import datetime as dt

import numpy as np
import pandas as pd
import pytest

from mdp.analytics import features as feat
from mdp.analytics.names import BARS, SYMBOLS
from spark_frames import pandas_returns, to_spark


@pytest.fixture(scope="module")
def table(spark, warehouse):
    return feat.daily_features(spark.table(BARS), spark.table(SYMBOLS)).toPandas()


def test_features_match_a_pandas_computation(table, raw):
    bars, _ = raw
    ref = pandas_returns(bars)
    tickers = sorted(bars["ticker"].unique())
    for ticker in (tickers[0], tickers[-1]):
        r = ref[ref["ticker"] == ticker].reset_index(drop=True)
        r["vol_20"] = r["ret"].rolling(20).std()
        r["vol_60"] = r["ret"].rolling(60).std()
        r["absret_20"] = r["ret"].abs().rolling(20).mean()
        r["volume_ratio_20"] = r["volume"] / r["volume"].rolling(20).mean()
        r["ret_lag3"] = r["ret"].shift(3)
        r["target"] = r["ret"].abs()[::-1].rolling(5).mean()[::-1].shift(-1)
        got = table[table["ticker"] == ticker].sort_values("trade_date").reset_index(drop=True)
        exp = r.iloc[feat.WARMUP:].reset_index(drop=True)
        assert len(got) == len(exp)
        for col in ("vol_20", "vol_60", "absret_20", "volume_ratio_20", "ret_lag3", "target"):
            np.testing.assert_allclose(got[col].to_numpy(dtype=float), exp[col].to_numpy(dtype=float),
                                       rtol=1e-9, atol=1e-12, equal_nan=True, err_msg=f"{ticker} {col}")


def test_warmup_rows_are_dropped_and_the_last_days_have_no_label(table, raw):
    bars, _ = raw
    per_ticker = table.groupby("ticker").size()
    assert (per_ticker == bars.groupby("ticker").size() - feat.WARMUP).all()
    assert not table[feat.NUMERIC].isna().any().any()
    last_days = sorted(table["trade_date"].unique())[-feat.HORIZON:]
    assert table[table["trade_date"].isin(last_days)]["target"].isna().all()
    assert table[~table["trade_date"].isin(last_days)]["target"].notna().all()


def test_no_feature_looks_at_a_later_day(spark, raw):
    bars, syms = raw
    cut = sorted(bars["trade_date"].unique())[1500]
    scrambled = bars.copy()
    later = scrambled["trade_date"] > cut
    rng = np.random.default_rng(1)
    scrambled.loc[later, "close"] = scrambled.loc[later, "close"] * rng.uniform(0.3, 3.0, later.sum())
    scrambled.loc[later, "volume"] = (scrambled.loc[later, "volume"] * rng.uniform(0.1, 9.0, later.sum())).astype(int) + 1

    def feats(b):
        sb, ss = to_spark(spark, b, syms)
        return feat.daily_features(sb, ss).toPandas().set_index(["ticker", "trade_date"]).sort_index()

    a, b = feats(bars), feats(scrambled)
    keep = a.index.get_level_values("trade_date") <= cut
    pd.testing.assert_frame_equal(a.loc[keep, feat.NUMERIC], b.loc[keep, feat.NUMERIC], rtol=1e-12, atol=0)
    # the label is the one thing that is supposed to see the future
    assert not np.allclose(a.loc[keep, "target"].fillna(0), b.loc[keep, "target"].fillna(0))
    horizon_start = sorted(bars["trade_date"].unique())[1500 - feat.HORIZON]
    safe = a.index.get_level_values("trade_date") < horizon_start
    np.testing.assert_allclose(a.loc[safe, "target"], b.loc[safe, "target"], rtol=1e-12)


def day(n):
    return dt.date(2020, 1, 1) + dt.timedelta(days=n)


def test_split_dates_leaves_a_gap_of_the_label_horizon():
    dates = [day(i) for i in range(100)]
    last_train, first_test = feat.split_dates(dates, 0.2, horizon=5)
    assert first_test == day(80)
    assert last_train == day(74)
    # the label of the last training day reaches 5 days ahead and still ends before the test period
    assert dates.index(last_train) + 5 < dates.index(first_test)


def test_split_dates_ignores_order_and_repeats():
    dates = [day(i) for i in range(50)] * 3
    np.random.default_rng(0).shuffle(dates)
    assert feat.split_dates(dates, 0.2, 5) == feat.split_dates(sorted(set(dates)), 0.2, 5)


def test_split_dates_refuses_a_tiny_history():
    with pytest.raises(ValueError):
        feat.split_dates([day(i) for i in range(5)], 0.2, 5)


def test_time_split_frames_do_not_overlap_in_time(spark, warehouse):
    data = feat.daily_features(spark.table(BARS), spark.table(SYMBOLS))
    labelled = data.where("target IS NOT NULL")
    train, test, info = feat.time_split(labelled)
    t_max = train.agg({"trade_date": "max"}).first()[0]
    s_min = test.agg({"trade_date": "min"}).first()[0]
    assert t_max.isoformat() == info["last_train_date"] and s_min.isoformat() == info["first_test_date"]
    all_dates = sorted(r[0] for r in labelled.select("trade_date").distinct().collect())
    assert all_dates.index(s_min) - all_dates.index(t_max) > feat.HORIZON
    assert train.count() + test.count() < labelled.count()
    assert 0.15 < test.count() / labelled.count() < 0.25

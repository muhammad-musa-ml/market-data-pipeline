import datetime as dt

import numpy as np

from mdp.gen import market


def run(seed: int, n: int = 12, days: int = 40):
    uni = market.Universe(market.make_symbols(n, seed), seed)
    prev = uni.initial_close()
    out = []
    for d in range(days):
        bars = uni.next_day(prev, d)
        prev = bars.close
        out.append(bars)
    return uni, out


def test_same_seed_same_bars():
    _, a = run(5)
    _, b = run(5)
    for x, y in zip(a, b):
        assert np.array_equal(x.close, y.close)
        assert np.array_equal(x.volume, y.volume)


def test_different_seed_differs():
    _, a = run(5)
    _, b = run(6)
    assert not np.array_equal(a[-1].close, b[-1].close)


def test_symbols_are_deterministic_and_unique():
    a = market.make_symbols(50, 3)
    b = market.make_symbols(50, 3)
    assert a == b
    assert len({s.ticker for s in a}) == 50
    assert {s.sector for s in a} == set(market.SECTORS)
    assert {s.exchange for s in a} == set(market.EXCHANGES)


def test_resuming_from_state_matches_a_straight_run():
    uni, straight = run(9, days=30)
    prev = uni.initial_close()
    for d in range(18):
        prev = uni.next_day(prev, d).close
    resumed = [uni.next_day(prev, 18)]
    prev = resumed[0].close
    for d in range(19, 30):
        resumed.append(uni.next_day(prev, d))
        prev = resumed[-1].close
    assert np.array_equal(resumed[-1].close, straight[-1].close)
    assert np.array_equal(resumed[0].open, straight[18].open)


def test_ohlc_invariants():
    _, days = run(11, n=30, days=120)
    for b in days:
        assert (b.low <= np.minimum(b.open, b.close)).all()
        assert (b.high >= np.maximum(b.open, b.close)).all()
        assert (b.low >= 1.0).all() and (b.volume >= 1).all()


def test_same_sector_moves_together_more_than_other_sectors():
    uni, days = run(21, n=48, days=500)
    closes = np.vstack([uni.initial_close()] + [d.close for d in days])
    rets = np.diff(np.log(closes), axis=0)
    corr = np.corrcoef(rets.T)
    sector = [s.sector for s in uni.specs]
    same, other = [], []
    for i in range(len(sector)):
        for j in range(i + 1, len(sector)):
            (same if sector[i] == sector[j] else other).append(corr[i, j])
    assert np.mean(same) > np.mean(other) + 0.05


def test_calendar_skips_weekends():
    assert market.trading_date(0) == dt.date(2014, 1, 6)
    assert market.trading_date(5) == dt.date(2014, 1, 13)
    assert all(market.trading_date(i).weekday() < 5 for i in range(300))
    assert market.day_index_of(market.trading_date(123)) == 123


def abs_returns(vol_clustering: bool, n: int = 20, days: int = 1500, seed: int = 3):
    uni = market.Universe(market.make_symbols(n, seed), seed, vol_clustering=vol_clustering)
    closes = [uni.initial_close()]
    volumes = []
    for d in range(days):
        b = uni.next_day(closes[-1], d)
        closes.append(b.close)
        volumes.append(b.volume)
    rets = np.diff(np.log(np.vstack(closes)), axis=0)
    return np.abs(rets), np.vstack(volumes)


def lag1_autocorr(x: np.ndarray) -> float:
    a, b = x[:-1].ravel(), x[1:].ravel()
    return float(np.corrcoef(a, b)[0, 1])


def test_volatility_clusters_only_when_switched_on():
    on, _ = abs_returns(True)
    off, _ = abs_returns(False)
    assert lag1_autocorr(on) > 0.1
    assert abs(lag1_autocorr(off)) < 0.05


def test_without_clustering_the_series_is_the_original_one():
    # the constants come from the generator as it was before volatility clustering existed
    uni = market.Universe(market.make_symbols(12, 5), 5, vol_clustering=False)
    prev = uni.initial_close()
    for d in range(40):
        b = uni.next_day(prev, d)
        prev = b.close
    assert round(float(b.close.sum()), 4) == 1124.1287
    assert int(b.volume.sum()) == 17135688
    assert round(float(b.high.sum()), 4) == 1126.9557


def test_log_vol_does_not_depend_on_the_order_of_requests():
    a = market.Universe(market.make_symbols(8, 4), 4)
    b = market.Universe(market.make_symbols(8, 4), 4)
    far = a.log_vol(5000)
    for d in range(0, 5001, 7):
        b.log_vol(d)
    assert np.array_equal(far, b.log_vol(5000))
    assert not np.array_equal(a.log_vol(10), a.log_vol(11))


def test_volume_follows_the_volatility_regime():
    uni = market.Universe(market.make_symbols(20, 3), 3)
    lv = np.vstack([uni.log_vol(d)[1:] for d in range(1500)])
    _, vol = abs_returns(True)
    ratio = np.log(vol / np.array([s.base_volume for s in uni.specs]))
    assert np.corrcoef(lv.ravel(), ratio.ravel())[0, 1] > 0.3

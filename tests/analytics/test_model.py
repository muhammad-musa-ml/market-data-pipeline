import pytest

from mdp.analytics import features as feat
from mdp.analytics import model
from mdp.analytics.names import BARS, SYMBOLS
from spark_frames import make_pandas, to_spark

DEPTHS = [1, 3, 5, 8, 12, 20, 30]


def split(spark, bars_pd, syms_pd):
    b, s = to_spark(spark, bars_pd, syms_pd)
    labelled = feat.daily_features(b, s).where("target IS NOT NULL")
    train, test, _ = feat.time_split(labelled)
    return train.cache(), test.cache()


@pytest.fixture(scope="module")
def clustered(spark, raw):
    train, test = split(spark, *raw)
    yield train, test, model.baselines(train, test), model.depth_sweep(train, test, DEPTHS)
    train.unpersist()
    test.unpersist()


def test_the_model_beats_every_baseline_on_unseen_dates(clustered):
    train, test, base, sweep = clustered
    best = max(r["test_r2"] for r in sweep)
    assert best > 0.15  # measured 0.22 on this data
    for name, m in base.items():
        assert best > m["r2"] + 0.03, name
    # predicting the training mean explains nothing of a later period
    assert abs(base["train_mean"]["r2"]) < 0.05


def test_deeper_trees_memorise_the_training_period(clustered):
    _, _, _, sweep = clustered
    by = {r["depth"]: r for r in sweep}
    assert by[1]["train_r2"] < by[8]["train_r2"] < by[30]["train_r2"]
    assert by[30]["train_r2"] > 0.9
    best_test = max(r["test_r2"] for r in sweep)
    assert by[30]["test_r2"] < best_test - 0.1  # overfitting: the unseen score drops
    assert by[1]["test_r2"] < best_test  # and a stump underfits


def test_without_volatility_clustering_there_is_much_less_to_learn(spark, clustered):
    # same sizes and seed, constant volatility: what is left is each ticker's own volatility level
    train, test = split(spark, *make_pandas(clustering=False))
    flat = max(r["test_r2"] for r in model.depth_sweep(train, test, [3, 5, 8]))
    on = max(r["test_r2"] for r in clustered[3] if r["depth"] in (3, 5, 8))
    assert on > flat + 0.06  # measured: 0.22 against 0.12
    train.unpersist()
    test.unpersist()


def test_depth_is_chosen_without_looking_at_the_test_period(spark, clustered):
    train, test, _, _ = clustered
    depth, rows = model.pick_depth(train, [2, 4, 6, 10])
    assert depth in (2, 4, 6, 10)
    assert {r["depth"] for r in rows} == {2, 4, 6, 10}
    # the validation block is cut from the training frame, so nothing from `test` can be in it
    max_train = train.agg({"trade_date": "max"}).first()[0]
    assert test.where(f"trade_date <= DATE '{max_train}'").count() == 0


def test_importances_name_the_volatility_features(spark, clustered):
    train, _, _, _ = clustered
    m = model.pipeline(6).fit(train)
    imp = model.importances(m)
    assert set(imp) == set(feat.NUMERIC) | {"sector"}
    assert abs(sum(imp.values()) - 1.0) < 0.01
    top3 = list(imp)[:3]
    assert {"vol_20", "vol_60", "absret_20", "mkt_vol_20", "vol_5"} & set(top3)


def test_run_on_the_warehouse_tables(spark, warehouse):
    out = model.run(spark, BARS, SYMBOLS, depths=[2, 6])
    assert out["rows"]["train"] > out["rows"]["test"] > 0
    assert out["chosen_depth"] in (2, 6)
    assert set(out["baselines"]) == {"train_mean", "ticker_mean", "persistence"}
    assert out["split"]["last_train_date"] < out["split"]["first_test_date"]

"""Decision-tree regression of next-days volatility, with baselines and a depth sweep.

No scaler stage: trees split on thresholds, so rescaling a feature changes nothing. The sector
goes through StringIndexer, whose metadata makes the tree treat it as categorical.
"""
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mdp.analytics import features as feat
from mdp.analytics.names import BARS, SYMBOLS

SEED = 7
DEPTHS = list(range(1, 31))
CANDIDATES = list(range(1, 13))  # depths the validation block may choose from; deeper ones are far past overfitting
HEADLINE_DEPTH = 5
PARTITIONS = 4  # one per executor core; tree training runs a task per partition at every level


def pipeline(depth: int) -> Pipeline:
    return Pipeline(stages=[
        StringIndexer(inputCol=feat.CATEGORICAL, outputCol="sector_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=feat.NUMERIC + ["sector_idx"], outputCol="features"),
        DecisionTreeRegressor(featuresCol="features", labelCol=feat.LABEL, maxDepth=depth,
                              maxBins=32, maxMemoryInMB=64, seed=SEED),
    ])


def r2(df: DataFrame, prediction: str = "prediction") -> float:
    return RegressionEvaluator(labelCol=feat.LABEL, predictionCol=prediction, metricName="r2").evaluate(df)


def rmse(df: DataFrame, prediction: str = "prediction") -> float:
    return RegressionEvaluator(labelCol=feat.LABEL, predictionCol=prediction, metricName="rmse").evaluate(df)


def baselines(train: DataFrame, test: DataFrame) -> dict[str, dict[str, float]]:
    """Three predictors that need no learning: the training mean, each ticker's training mean,
    and persistence (the trailing 20-day mean absolute return, same statistic as the target)."""
    mean = train.agg(F.avg(feat.LABEL)).first()[0]
    per_ticker = train.groupBy("ticker").agg(F.avg(feat.LABEL).alias("p_ticker"))
    scored = (test.join(per_ticker, "ticker", "left")
              .withColumn("p_ticker", F.coalesce("p_ticker", F.lit(mean)))
              .withColumn("p_mean", F.lit(mean))
              .withColumn("p_persist", F.col("absret_20")))
    return {name: {"r2": r2(scored, col), "rmse": rmse(scored, col)}
            for name, col in (("train_mean", "p_mean"), ("ticker_mean", "p_ticker"), ("persistence", "p_persist"))}


def fit_score(train: DataFrame, test: DataFrame, depth: int) -> tuple[PipelineModel, dict]:
    t0 = time.time()
    model = pipeline(depth).fit(train)
    seconds = time.time() - t0
    return model, {
        "depth": depth,
        "train_r2": r2(model.transform(train)),
        "test_r2": r2(model.transform(test)),
        "fit_seconds": round(seconds, 2),
    }


def depth_sweep(train: DataFrame, test: DataFrame, depths: list[int] = DEPTHS) -> list[dict]:
    return [fit_score(train, test, d)[1] for d in depths]


def pick_depth(train: DataFrame, depths: list[int] = CANDIDATES, horizon: int = feat.HORIZON) -> tuple[int, list[dict]]:
    """Depth with the best R^2 on the last fifth of the training period, which is held out in time
    (with the same gap as the real split), so the test set takes no part in choosing it."""
    fit_part, val_part, _ = feat.time_split(train, 0.2, horizon)
    fit_part, val_part = fit_part.cache(), val_part.cache()
    rows = [dict(depth=d, val_r2=r2(pipeline(d).fit(fit_part).transform(val_part))) for d in depths]
    fit_part.unpersist()
    val_part.unpersist()
    return max(rows, key=lambda r: r["val_r2"])["depth"], rows


def importances(model: PipelineModel) -> dict[str, float]:
    names = feat.NUMERIC + ["sector"]
    vals = model.stages[-1].featureImportances.toArray()
    return {n: round(float(v), 4) for n, v in sorted(zip(names, vals), key=lambda p: -p[1])}


def run(spark: SparkSession, bars: str = BARS, symbols: str = SYMBOLS, depths: list[int] = DEPTHS) -> dict:
    t0 = time.time()
    data = feat.daily_features(spark.table(bars), spark.table(symbols))
    labelled = data.where(F.col(feat.LABEL).isNotNull())
    train, test, split = feat.time_split(labelled)
    train, test = train.repartition(PARTITIONS).cache(), test.repartition(PARTITIONS).cache()
    n_train, n_test = train.count(), test.count()

    base = baselines(train, test)
    sweep = depth_sweep(train, test, depths)
    chosen, validation = pick_depth(train, [d for d in CANDIDATES if d in depths] or depths)
    model, headline = fit_score(train, test, chosen)
    fixed = next(r for r in sweep if r["depth"] == HEADLINE_DEPTH) if HEADLINE_DEPTH in depths else None
    out = {
        "split": split,
        "rows": {"train": n_train, "test": n_test},
        "baselines": base,
        "chosen_depth": chosen,
        "chosen": headline,
        "depth_5": fixed,
        "importances": importances(model),
        "sweep": sweep,
        "validation": validation,
        "seconds": round(time.time() - t0, 1),
    }
    train.unpersist()
    test.unpersist()
    return out

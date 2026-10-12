import importlib.util
import shutil

import pytest

# the plain unit job installs neither pyspark nor pandas; the spark job in CI and the driver
# container do, and then these tests run
if any(importlib.util.find_spec(m) is None for m in ("pyspark", "pandas")):
    collect_ignore_glob = ["test_*.py"]


def pytest_collection_modifyitems(items):
    for item in items:
        if "tests/analytics" in str(item.fspath).replace("\\", "/"):
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("no java on PATH; run these inside the driver container or on a host with JDK 17+")
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (SparkSession.builder.master("local[2]").appName("mdp-tests")
               .config("spark.sql.catalogImplementation", "in-memory")
               .config("spark.sql.warehouse.dir", warehouse.as_uri())
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw(spark):
    """(bars, symbols) as pandas frames, 10 symbols over 2300 trading days."""
    from spark_frames import make_pandas
    return make_pandas()


@pytest.fixture(scope="session")
def warehouse(spark, raw, tmp_path_factory):
    """market.bars, market.symbols and market.daily_returns built by the real compaction job from
    batch files, one of which is listed twice and one of which is a leftover that is not listed."""
    from mdp.analytics import compact
    from spark_frames import to_spark

    bars_pd, syms_pd = raw
    bars, symbols = to_spark(spark, bars_pd, syms_pd)
    root = tmp_path_factory.mktemp("hdfs")
    ids = bars_pd["id"].to_numpy()
    third = len(ids) // 3
    parts = {"b0": bars.where(bars.id <= int(ids[third])),
             "b1": bars.where((bars.id > int(ids[third])) & (bars.id <= int(ids[2 * third]))),
             "b2": bars.where(bars.id > int(ids[2 * third]))}
    leftover = bars.where(bars.id <= 50).withColumn("id", bars.id + 10_000_000)
    paths = {}
    for name, df in {**parts, "leftover": leftover}.items():
        out = root / name
        df.coalesce(1).write.parquet(out.as_uri())
        paths[name] = [p.as_uri() for p in sorted(out.glob("part-*.parquet"))]
    symbols.coalesce(1).write.parquet((root / "symbols").as_uri())
    symbols_path = [p.as_uri() for p in sorted((root / "symbols").glob("part-*.parquet"))][0]

    trusted = paths["b0"] + paths["b1"] + paths["b2"] + paths["b1"]  # b1 twice: a replayed batch
    result = compact.run(spark, trusted, symbols_path, buckets=8)
    return {"result": result, "trusted": trusted, "leftover": paths["leftover"],
            "all_files": trusted + paths["leftover"], "duplicate_rows": int(parts["b1"].count())}

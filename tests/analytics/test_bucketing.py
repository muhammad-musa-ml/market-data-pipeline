import pytest

from mdp.analytics import bucketing
from mdp.analytics.names import LAB_DB
from mdp.analytics.session import count_exchanges, plan_text


@pytest.fixture(scope="module")
def lab(spark, warehouse):
    old = {k: spark.conf.get(k) for k in bucketing.CONF}
    for k, v in bucketing.CONF.items():
        spark.conf.set(k, v)
    bucketing.build_tables(spark)
    yield
    for k, v in old.items():
        spark.conf.set(k, v)


def test_group_by_on_the_bucket_column_needs_no_shuffle(spark, lab):
    plan = plan_text(spark, bucketing.queries("bucketed")["group_by"])
    assert count_exchanges(plan) == 0
    assert "Bucketed: true" in plan


def test_the_same_group_by_on_plain_files_shuffles(spark, lab):
    plan = plan_text(spark, bucketing.queries("flat")["group_by"])
    assert count_exchanges(plan) == 1
    assert "Bucketed: true" not in plan


def test_join_on_the_bucket_column_needs_no_shuffle_on_either_side(spark, lab):
    plan = plan_text(spark, bucketing.queries("bucketed")["join"])
    assert count_exchanges(plan) == 0
    assert "SortMergeJoin" in plan


def test_the_plain_join_shuffles_both_sides(spark, lab):
    plan = plan_text(spark, bucketing.queries("flat")["join"])
    assert count_exchanges(plan) == 2


def test_both_layouts_give_the_same_answers(spark, lab):
    for name in ("group_by", "join"):
        a = sorted(map(tuple, spark.sql(bucketing.queries("bucketed")[name]).collect()))
        b = sorted(map(tuple, spark.sql(bucketing.queries("flat")[name]).collect()))
        assert len(a) == 10 and len(a) == len(b)
        assert all(x[0] == y[0] and x[1] == y[1] for x, y in zip(a, b))


def test_exchange_counter_ignores_broadcasts():
    plan = ("*(2) BroadcastHashJoin\n:- *(2) FileScan\n+- BroadcastExchange HashedRelationBroadcastMode\n"
            "   +- *(1) FileScan\n")
    assert count_exchanges(plan) == 0
    shuffled = "HashAggregate\n+- Exchange hashpartitioning(ticker, 8)\n   +- HashAggregate\n"
    assert count_exchanges(shuffled) == 1


def test_lab_tables_are_in_their_own_database(spark, lab):
    names = {t.name for t in spark.catalog.listTables(LAB_DB)}
    assert {"bars_bucketed", "bars_flat", "stats_bucketed", "stats_flat"} <= names
    assert not {t.name for t in spark.catalog.listTables("market")} & names

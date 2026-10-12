import datetime as dt

from mdp.analytics import question


def pandas_answer(bars, syms, since):
    m = bars[bars["trade_date"] >= since].merge(syms, on=["symbol_id", "ticker"])
    m["up"] = (m["close"] > m["open"]).astype(int)
    g = m.groupby(["sector", "exchange"]).agg(bars=("id", "size"), up_days=("up", "sum"),
                                              total_volume=("volume", "sum"), peak_volume=("volume", "max"))
    return [(s, e, int(r.bars), int(r.up_days), int(r.total_volume), int(r.peak_volume))
            for (s, e), r in g.sort_index().iterrows()]


def test_three_apis_give_one_answer(spark, warehouse):
    rdd = question.with_rdd(spark)
    df = question.with_dataframe(spark)
    sql = question.with_sql(spark)
    assert rdd == df == sql
    assert len(sql) > 3


def test_the_answer_is_the_right_one(spark, warehouse, raw):
    bars, syms = raw
    assert question.with_sql(spark) == pandas_answer(bars, syms, question.SINCE)


def test_cutoff_date_matters_and_is_honoured_by_all_three(spark, warehouse, raw):
    bars, syms = raw
    since = dt.date(2021, 6, 1)
    expect = pandas_answer(bars, syms, since)
    assert question.with_rdd(spark, since) == question.with_dataframe(spark, since) \
        == question.with_sql(spark, since) == expect
    assert expect != pandas_answer(bars, syms, question.SINCE)


def test_run_reports_identical(spark, warehouse):
    out = question.run(spark)
    assert out["identical"] is True
    assert out["groups"] == len(out["answer"])

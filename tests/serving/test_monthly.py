import datetime as dt

import pytest

from mdp.serving.models import Bar
from mdp.serving.monthly import monthly
from serving_fakes import bar


def test_empty_input_gives_no_months():
    assert monthly([]) == []


def test_groups_by_calendar_month_in_order_whatever_the_input_order():
    bars = [bar("2024-02-01", 30), bar("2024-01-31", 20), bar("2024-01-02", 10), bar("2023-12-29", 5)]
    out = monthly(bars)
    assert [m["month"] for m in out] == ["2023-12", "2024-01", "2024-02"]
    assert [m["trading_days"] for m in out] == [1, 2, 1]


def test_averages_and_volume_sum():
    out = monthly([
        Bar(dt.date(2024, 3, 1), 10, 14, 8, 12, 100),
        Bar(dt.date(2024, 3, 4), 12, 16, 10, 14, 300),
        Bar(dt.date(2024, 3, 5), 14, 15, 11, 13, 50),
    ])
    assert out == [{"month": "2024-03", "trading_days": 3, "avg_high": 15.0, "avg_low": 9.6667,
                    "avg_close": 13.0, "total_volume": 450}]


def test_month_boundaries_and_leap_day():
    out = monthly([bar("2024-02-29"), bar("2024-03-01"), bar("2023-02-28"), bar("2024-01-31")])
    assert [(m["month"], m["trading_days"]) for m in out] == [
        ("2023-02", 1), ("2024-01", 1), ("2024-02", 1), ("2024-03", 1)]


def test_one_bar_per_month_equals_that_bar():
    b = Bar(dt.date(2020, 6, 15), 1.5, 2.5, 1.25, 2.0, 7)
    (m,) = monthly([b])
    assert (m["avg_high"], m["avg_low"], m["avg_close"], m["total_volume"]) == (2.5, 1.25, 2.0, 7)


def test_values_are_rounded_to_four_places():
    out = monthly([bar("2024-01-02", 1.0001), bar("2024-01-03", 1.0002), bar("2024-01-04", 1.0002)])
    assert out[0]["avg_close"] == pytest.approx(1.0002, abs=1e-9)
    assert len(str(out[0]["avg_close"]).split(".")[1]) <= 4


def test_accepts_a_generator():
    (m,) = monthly(bar(f"2024-01-{d:02d}") for d in range(1, 10))
    assert m["trading_days"] == 9

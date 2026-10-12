import datetime as dt
import math

import pytest

from mdp.serving import validate
from mdp.serving.models import price

GOOD = {"open": 10, "high": 12, "low": 9.5, "close": 11, "volume": 500}


@pytest.mark.parametrize("value", ["A", "AAPL", "BRK.B", "X1", "ABCDEFGH"])
def test_ticker_accepts(value):
    assert validate.ticker(value) == value


@pytest.mark.parametrize("value", ["", "aapl", "1ABC", "ABCDEFGHI", "A B", "A/B", "A-B", ".A"])
def test_ticker_rejects(value):
    with pytest.raises(validate.BadRequest):
        validate.ticker(value)


@pytest.mark.parametrize("value", ["2024-02-29", "1900-01-01", "2999-12-31"])
def test_iso_date_accepts(value):
    assert validate.iso_date(value) == dt.date.fromisoformat(value)


@pytest.mark.parametrize("value", ["2023-02-29", "20240229", "2024-2-9", "2024-13-01", "", None, "1899-12-31",
                                   "3000-01-01", "2024-W05-1"])
def test_iso_date_rejects(value):
    with pytest.raises(validate.BadRequest):
        validate.iso_date(value)


def test_price_rounds_half_even_to_four_places_and_is_stable():
    # the decimal text of the float is rounded, so ties go to the even digit
    assert price(1.23455) == 1.2346
    assert price(1.23445) == 1.2344
    assert price(10) == 10.0
    assert price(0.1 + 0.2) == 0.3
    assert price(price(123.456789)) == price(123.456789) == 123.4568


def test_bar_normalises_prices_and_keeps_integers():
    b = validate.bar("2024-01-02", {**GOOD, "high": 12.34567})
    assert (b.open, b.high, b.low, b.close, b.volume) == (10.0, 12.3457, 9.5, 11.0, 500)
    assert b.trade_date == dt.date(2024, 1, 2)


@pytest.mark.parametrize("change", [
    {"high": 9},                      # high below low
    {"open": 13},                     # open above high
    {"close": 9},                     # close below low
    {"low": 0, "open": 0, "close": 0},    # zero price
    {"low": -1},
    {"volume": -1},
    {"volume": 1.5},
    {"volume": "10"},
    {"volume": True},
    {"high": "12"},
    {"high": None},
    {"high": math.inf},
    {"high": 1e12},
    {"volume": 2**63},
])
def test_bar_rejects(change):
    with pytest.raises(validate.BadRequest):
        validate.bar("2024-01-02", {**GOOD, **change})


def test_bar_rejects_missing_and_unknown_fields_and_non_objects():
    for body in ({k: v for k, v in GOOD.items() if k != "volume"}, {**GOOD, "adj_close": 1}, [], None, "x"):
        with pytest.raises(validate.BadRequest):
            validate.bar("2024-01-02", body)


def test_bar_rejects_a_bad_date_before_anything_else():
    with pytest.raises(validate.BadRequest, match="date"):
        validate.bar("2024-02-30", GOOD)


def test_company_strips_text_and_enforces_lengths():
    c = validate.company("ACME", {"name": "  Acme  ", "sector": "Energy", "exchange": "NYSE"})
    assert c.name == "Acme"
    for body in ({"name": "", "sector": "s", "exchange": "e"}, {"name": "n", "sector": "s"},
                 {"name": "n" * 81, "sector": "s", "exchange": "e"}, {"name": "n", "sector": "s", "exchange": "e", "x": 1},
                 {"name": 5, "sector": "s", "exchange": "e"}, None):
        with pytest.raises(validate.BadRequest):
            validate.company("ACME", body)
    with pytest.raises(validate.BadRequest):
        validate.company("acme", {"name": "n", "sector": "s", "exchange": "e"})


def test_window_parses_bounds_and_limit():
    assert validate.window({}) == (None, None, None)
    assert validate.window({"start": "2024-01-01", "end": "2024-01-31", "limit": "5"}) == (
        dt.date(2024, 1, 1), dt.date(2024, 1, 31), 5)
    assert validate.window({"start": "2024-01-01", "end": "2024-01-01"})[0] == dt.date(2024, 1, 1)


@pytest.mark.parametrize("args", [
    {"start": "2024-02-01", "end": "2024-01-01"}, {"start": "yesterday"}, {"limit": "0"}, {"limit": "-3"},
    {"limit": "ten"}, {"limit": "100001"},
])
def test_window_rejects(args):
    with pytest.raises(validate.BadRequest):
        validate.window(args)

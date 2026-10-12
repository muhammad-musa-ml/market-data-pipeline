import datetime as dt

from cassandra import ConsistencyLevel

from mdp.serving import quotes
from mdp.v1 import bar_pb2
from serving_fakes import FakeSession


def bar(i, ticker="AAA", day="2024-01-02", close=10.0):
    return bar_pb2.Bar(id=i, symbol_id=1, ticker=ticker, trade_date=day, open=close, high=close, low=close,
                       close=close, volume=100 + i)


def test_write_timestamp_is_midnight_utc_of_the_trade_date_in_microseconds():
    assert quotes.write_timestamp(dt.date(1970, 1, 1)) == 0
    assert quotes.write_timestamp(dt.date(1970, 1, 2)) == 86_400_000_000
    assert quotes.write_timestamp(dt.date(2024, 1, 3)) - quotes.write_timestamp(dt.date(2024, 1, 2)) == 86_400_000_000


def test_a_later_day_always_carries_a_later_timestamp():
    days = [dt.date(2023, 12, 29), dt.date(2024, 1, 2), dt.date(2024, 2, 29), dt.date(2025, 1, 1)]
    stamps = [quotes.write_timestamp(d) for d in days]
    assert stamps == sorted(stamps) and len(set(stamps)) == len(stamps)


def test_newest_bar_per_ticker_wins_whatever_order_they_arrive_in():
    bars = [bar(3, "AAA", "2024-01-04"), bar(1, "AAA", "2024-01-02"), bar(2, "BBB", "2024-01-03"),
            bar(5, "AAA", "2024-01-03")]
    best = quotes.newest_per_ticker(bars)
    assert {t: b.trade_date for t, b in best.items()} == {"AAA": "2024-01-04", "BBB": "2024-01-03"}
    assert quotes.newest_per_ticker(reversed(bars)).keys() == best.keys()
    assert quotes.newest_per_ticker(reversed(bars))["AAA"].id == 3


def test_same_day_ties_go_to_the_higher_id():
    best = quotes.newest_per_ticker([bar(7, day="2024-01-02"), bar(9, day="2024-01-02"), bar(8, day="2024-01-02")])
    assert best["AAA"].id == 9


def test_bind_matches_the_statement_placeholders():
    s = FakeSession()
    stmt = quotes.statement(s, "prod")
    assert stmt.consistency_level == ConsistencyLevel.QUORUM
    assert stmt.cql.index("TIMESTAMP ?") < stmt.cql.index("close = ?") < stmt.cql.index("WHERE ticker = ?")
    ts, close, volume, bar_id, day, ticker = quotes.bind(bar(4, close=12.5, day="2024-01-02"))
    assert (close, volume, bar_id, day, ticker) == (12.5, 104, 4, dt.date(2024, 1, 2), "AAA")
    assert ts == quotes.write_timestamp(day)
    assert stmt.cql.count("?") == 6

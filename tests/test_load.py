import numpy as np

from mdp.gen import load, market


def setup(n=10, seed=4):
    uni = market.Universe(market.make_symbols(n, seed), seed)
    return uni, uni.initial_close(), np.full(n, -1, dtype=np.int64)


def test_day_rows_covers_every_symbol_once():
    uni, prev, last = setup()
    rows, prev2, last2 = load.day_rows(uni, prev, last, 0)
    assert [r[0] for r in rows] == [s.symbol_id for s in uni.specs]
    assert (last2 == 0).all()
    assert not np.array_equal(prev, prev2)


def test_partial_day_is_completed_without_touching_finished_symbols():
    uni, prev, last = setup()
    rows0, prev, last = load.day_rows(uni, prev, last, 0)
    full1, _, _ = load.day_rows(uni, prev, last, 1)

    # pretend a crash left only the first four symbols of day 1 in MySQL
    prev_partial, last_partial = prev.copy(), last.copy()
    done = full1[:4]
    for i, row in enumerate(done):
        prev_partial[i] = float(row[5])
        last_partial[i] = 1
    assert int(last_partial.min()) == 0

    rest, _, last_after = load.day_rows(uni, prev_partial, last_partial, 1)
    assert [r[0] for r in rest] == [r[0] for r in full1[4:]]
    assert rest == full1[4:]
    assert (last_after == 1).all()


def test_prices_are_formatted_for_decimal_columns():
    uni, prev, last = setup(3)
    rows, _, _ = load.day_rows(uni, prev, last, 0)
    for _, date, o, h, lo, c, v in rows:
        assert len(o.split(".")[1]) == 4 and float(lo) <= float(h)
        assert date == "2014-01-06" and isinstance(v, int)

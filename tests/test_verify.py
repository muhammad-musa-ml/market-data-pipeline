import datetime as dt

import pyarrow as pa

from mdp.check import verify


def table(rows):
    """rows: (id, ticker, close, volume)"""
    return pa.table({
        "id": pa.array([r[0] for r in rows], pa.int64()),
        "ticker": pa.array([r[1] for r in rows], pa.string()),
        "trade_date": pa.array([dt.date(2020, 1, 1)] * len(rows), pa.date32()),
        **{c: pa.array([r[2] for r in rows], pa.float64()) for c in ("open", "high", "low", "close")},
        "volume": pa.array([r[3] for r in rows], pa.int64()),
    })


BASE = [(i, "AAA" if i % 2 else "BBB", 10.0 + i, 100 + i) for i in range(1, 11)]


def test_identical_data_is_clean():
    rep = verify.compare(table(BASE), table(BASE[::-1]))
    assert rep.ok and rep.expected == rep.found_rows == rep.distinct == 10


def test_duplicate_is_reported():
    rep = verify.compare(table(BASE), table(BASE + [BASE[3], BASE[3], BASE[7]]))
    assert not rep.ok
    assert rep.duplicate_count == 2 and rep.duplicates == [4, 8]
    assert rep.gap_count == 0 and rep.found_rows == 13 and rep.distinct == 10


def test_gap_is_reported():
    rep = verify.compare(table(BASE), table([r for r in BASE if r[0] not in (3, 9)]))
    assert not rep.ok and rep.gaps == [3, 9] and rep.duplicate_count == 0


def test_extra_row_is_reported():
    rep = verify.compare(table(BASE), table(BASE + [(99, "AAA", 1.0, 1)]))
    assert not rep.ok and rep.extras == [99]


def test_changed_value_is_reported():
    changed = [(i, t, c + (0.5 if i == 5 else 0), v) for i, t, c, v in BASE]
    rep = verify.compare(table(BASE), table(changed))
    assert not rep.ok and rep.mismatched == [5]
    assert rep.gap_count == rep.duplicate_count == 0


def test_changed_volume_and_ticker_are_reported():
    changed = [(i, "ZZZ" if i == 2 else t, c, v + (1 if i == 6 else 0)) for i, t, c, v in BASE]
    rep = verify.compare(table(BASE), table(changed))
    assert rep.mismatched == [2, 6]


def test_empty_hdfs_means_every_id_is_a_gap():
    rep = verify.compare(table(BASE), table([]))
    assert rep.gap_count == 10 and not rep.ok


def test_duplicates_with_different_values_still_compare_the_first_copy():
    dup = (4, "BBB", 10.0 + 4, 104)
    rep = verify.compare(table(BASE), table(BASE + [dup]))
    assert rep.duplicate_count == 1 and rep.mismatch_count == 0

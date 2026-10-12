import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest

from fakes import make_bar
from mdp.stream import codec


def test_protobuf_roundtrip():
    bar = make_bar(42)
    again = codec.decode(codec.encode(bar))
    assert again == bar


def test_bar_from_mysql_row_types():
    row = (7, 3, "KORV", dt.date(2021, 3, 4), Decimal("10.1234"), Decimal("11.0000"),
           Decimal("9.5000"), Decimal("10.9999"), 12345,
           dt.datetime(2026, 10, 12, 18, 38, 23, 63586))
    bar = codec.bar_from_row(row)
    assert (bar.id, bar.ticker, bar.trade_date, bar.volume) == (7, "KORV", "2021-03-04", 12345)
    assert bar.close == pytest.approx(10.9999)
    assert bar.inserted_at_us == 1791830303063586


def test_table_has_declared_schema_and_values():
    table = codec.bars_to_table([make_bar(i) for i in range(1, 6)])
    assert table.schema == codec.BAR_SCHEMA
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5]
    assert table.column("trade_date").to_pylist()[0] == dt.date(2020, 1, 1)
    ts = table.column("inserted_at").cast(pa.int64()).to_pylist()[0]
    assert ts == 1_700_000_000_000_001


def test_parquet_bytes_roundtrip():
    table = codec.bars_to_table([make_bar(i) for i in range(10)])
    assert codec.table_from_bytes(codec.table_to_bytes(table)).equals(table)


def test_garbage_does_not_decode_silently():
    with pytest.raises(Exception):
        codec.decode(b"\xff\xff\xff\xff\xff")

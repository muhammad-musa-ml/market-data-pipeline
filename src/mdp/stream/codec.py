import datetime as dt
import io

import pyarrow as pa
import pyarrow.parquet as pq

from mdp.v1 import bar_pb2

BAR_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("symbol_id", pa.int32()),
    ("ticker", pa.string()),
    ("trade_date", pa.date32()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.int64()),
    ("inserted_at", pa.timestamp("us", tz="UTC")),
])

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def _micros(ts: dt.datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    delta = ts - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def bar_from_row(row: tuple) -> bar_pb2.Bar:
    """Row order: id, symbol_id, ticker, trade_date, open, high, low, close, volume, inserted_at."""
    id_, symbol_id, ticker, trade_date, o, h, lo, c, volume, inserted_at = row
    return bar_pb2.Bar(
        id=id_, symbol_id=symbol_id, ticker=ticker, trade_date=trade_date.isoformat(),
        open=float(o), high=float(h), low=float(lo), close=float(c), volume=int(volume),
        inserted_at_us=_micros(inserted_at),
    )


def encode(bar: bar_pb2.Bar) -> bytes:
    return bar.SerializeToString()


def decode(raw: bytes) -> bar_pb2.Bar:
    bar = bar_pb2.Bar()
    bar.ParseFromString(raw)
    return bar


def bars_to_table(bars: list[bar_pb2.Bar]) -> pa.Table:
    columns = {
        "id": [b.id for b in bars],
        "symbol_id": [b.symbol_id for b in bars],
        "ticker": [b.ticker for b in bars],
        "trade_date": [dt.date.fromisoformat(b.trade_date) for b in bars],
        "open": [b.open for b in bars],
        "high": [b.high for b in bars],
        "low": [b.low for b in bars],
        "close": [b.close for b in bars],
        "volume": [b.volume for b in bars],
        "inserted_at": [b.inserted_at_us for b in bars],
    }
    return pa.table(
        [pa.array(columns[f.name], type=pa.int64() if f.name == "inserted_at" else f.type)
         for f in BAR_SCHEMA],
        names=BAR_SCHEMA.names,
    ).cast(BAR_SCHEMA)


def table_to_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def table_from_bytes(raw: bytes, columns: list[str] | None = None) -> pa.Table:
    return pq.read_table(io.BytesIO(raw), columns=columns)

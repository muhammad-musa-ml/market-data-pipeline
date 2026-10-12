import io
import time
from collections.abc import Iterator

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from mdp import layout
from mdp.gen import market
from mdp.hdfs import WebHdfs

INSERT_BAR = (
    "INSERT INTO bars (symbol_id, trade_date, `open`, high, low, `close`, volume) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)


def ensure_symbols(conn, uni: market.Universe) -> None:
    cur = conn.cursor()
    cur.executemany(
        "INSERT IGNORE INTO symbols (symbol_id, ticker, name, sector, exchange) "
        "VALUES (%s, %s, %s, %s, %s)",
        [(s.symbol_id, s.ticker, s.name, s.sector, s.exchange) for s in uni.specs],
    )
    conn.commit()
    cur.close()


def resume_state(conn, uni: market.Universe) -> tuple[np.ndarray, np.ndarray]:
    """Last close and last day index per symbol, from what is already in MySQL."""
    prev = uni.initial_close()
    last = np.full(len(uni), -1, dtype=np.int64)
    slot = {s.symbol_id: i for i, s in enumerate(uni.specs)}
    cur = conn.cursor()
    cur.execute(
        "SELECT b.symbol_id, b.trade_date, b.`close` FROM bars b "
        "JOIN (SELECT symbol_id, MAX(trade_date) AS d FROM bars GROUP BY symbol_id) m "
        "ON m.symbol_id = b.symbol_id AND m.d = b.trade_date"
    )
    for symbol_id, trade_date, close in cur.fetchall():
        i = slot[symbol_id]
        prev[i] = float(close)
        last[i] = market.day_index_of(trade_date)
    cur.close()
    return prev, last


def day_rows(uni: market.Universe, prev: np.ndarray, last: np.ndarray, day: int):
    """Rows for one day, only for symbols that do not have it yet."""
    bars = uni.next_day(prev, day)
    todo = last < day
    date = market.trading_date(day).isoformat()
    rows = [
        (uni.specs[i].symbol_id, date, f"{bars.open[i]:.4f}", f"{bars.high[i]:.4f}",
         f"{bars.low[i]:.4f}", f"{bars.close[i]:.4f}", int(bars.volume[i]))
        for i in np.flatnonzero(todo)
    ]
    new_prev = np.where(todo, bars.close, prev)
    new_last = np.where(todo, day, last)
    return rows, new_prev, new_last


def load_history(conn, uni: market.Universe, days: int, chunk_rows: int = 20000) -> int:
    """Bring MySQL up to `days` trading days of bars. Returns rows inserted."""
    ensure_symbols(conn, uni)
    prev, last = resume_state(conn, uni)
    cur = conn.cursor()
    pending: list[tuple] = []
    inserted = 0
    for day in range(int(last.min()) + 1, days):
        rows, prev, last = day_rows(uni, prev, last, day)
        pending.extend(rows)
        if len(pending) >= chunk_rows:
            cur.executemany(INSERT_BAR, pending)
            conn.commit()
            inserted += len(pending)
            pending = []
    if pending:
        cur.executemany(INSERT_BAR, pending)
        conn.commit()
        inserted += len(pending)
    cur.close()
    return inserted


def live_rows(conn, uni: market.Universe) -> Iterator[list[tuple]]:
    """Endless stream of per-day row lists, continuing from the database."""
    ensure_symbols(conn, uni)
    prev, last = resume_state(conn, uni)
    day = int(last.min()) + 1
    while True:
        rows, prev, last = day_rows(uni, prev, last, day)
        yield rows
        day += 1


def run_live(conn, uni: market.Universe, rate: float, max_bars: int | None = None,
             tick: float = 0.1) -> int:
    """Append bars at about `rate` per second, a few per transaction."""
    group = max(1, round(rate * tick))
    interval = group / rate
    cur = conn.cursor()
    sent = 0
    due = time.monotonic()
    for day_rows_ in live_rows(conn, uni):
        for i in range(0, len(day_rows_), group):
            part = day_rows_[i:i + group]
            if max_bars is not None:
                part = part[: max_bars - sent]
            if not part:
                cur.close()
                return sent
            cur.executemany(INSERT_BAR, part)
            conn.commit()
            sent += len(part)
            if max_bars is not None and sent >= max_bars:
                cur.close()
                return sent
            due += interval
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                due = time.monotonic()
    return sent


def export_symbols(conn, hdfs: WebHdfs) -> int:
    cur = conn.cursor()
    cur.execute("SELECT symbol_id, ticker, name, sector, exchange FROM symbols ORDER BY symbol_id")
    rows = cur.fetchall()
    cur.close()
    table = pa.table({
        "symbol_id": pa.array([r[0] for r in rows], pa.int32()),
        "ticker": [r[1] for r in rows],
        "name": [r[2] for r in rows],
        "sector": [r[3] for r in rows],
        "exchange": [r[4] for r in rows],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    hdfs.mkdirs(layout.SYMBOLS_DIM.rsplit("/", 1)[0])
    hdfs.write(layout.SYMBOLS_DIM, buf.getvalue())
    return len(rows)

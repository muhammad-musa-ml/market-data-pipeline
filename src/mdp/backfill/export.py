import io
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from mdp import layout
from mdp.hdfs import WebHdfs

EXPORT_REPLICATION = 2
EXPORT_BLOCK_SIZE = 1 << 20
ROW_GROUP_ROWS = 8192

# the only columns a caller can filter on; the value is always a bound parameter
DIMENSION_COLUMN = {"sector": "s.sector", "exchange": "s.exchange"}


class BadRequest(ValueError):
    pass


@dataclass
class ExportResult:
    rows: int
    bytes: int
    path: str


def check_dest(path: str) -> None:
    if not path.startswith(layout.EXPORT_DIR + "/") or ".." in path or not path.endswith(".parquet"):
        raise BadRequest(f"dest_path must be a .parquet file under {layout.EXPORT_DIR}/")


def export_query(dimension: str) -> str:
    try:
        column = DIMENSION_COLUMN[dimension]
    except KeyError:
        raise BadRequest(f"unknown dimension {dimension!r}") from None
    return (
        "SELECT b.id, s.ticker, s.sector, s.exchange, b.trade_date, "
        "b.`open`, b.high, b.low, b.`close`, b.volume "
        "FROM bars b JOIN symbols s ON s.symbol_id = b.symbol_id "
        f"WHERE {column} = %s ORDER BY s.ticker, b.trade_date"
    )


def rows_to_table(rows: list[tuple]) -> pa.Table:
    cols = list(zip(*rows)) if rows else [[]] * 10
    return pa.table({
        "id": pa.array(cols[0], pa.int64()),
        "ticker": pa.array(cols[1], pa.string()),
        "sector": pa.array(cols[2], pa.string()),
        "exchange": pa.array(cols[3], pa.string()),
        "trade_date": pa.array(cols[4], pa.date32()),
        "open": pa.array([float(x) for x in cols[5]], pa.float64()),
        "high": pa.array([float(x) for x in cols[6]], pa.float64()),
        "low": pa.array([float(x) for x in cols[7]], pa.float64()),
        "close": pa.array([float(x) for x in cols[8]], pa.float64()),
        "volume": pa.array(cols[9], pa.int64()),
    })


def export_bars(conn, hdfs: WebHdfs, dimension: str, name: str, dest: str) -> ExportResult:
    check_dest(dest)
    sql = export_query(dimension)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM symbols s WHERE {DIMENSION_COLUMN[dimension]} = %s", (name,))
        if cur.fetchone()[0] == 0:
            raise BadRequest(f"no symbols with {dimension} {name!r}")
        cur.execute(sql, (name,))
        table = rows_to_table(cur.fetchall())
    finally:
        cur.close()
        conn.commit()

    buf = io.BytesIO()
    pq.write_table(table, buf, row_group_size=ROW_GROUP_ROWS)
    data = buf.getvalue()
    hdfs.write(dest, data, replication=EXPORT_REPLICATION, block_size=EXPORT_BLOCK_SIZE)
    return ExportResult(rows=table.num_rows, bytes=len(data), path=dest)

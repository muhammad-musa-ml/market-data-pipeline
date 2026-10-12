import datetime as dt
from collections.abc import Sequence
from contextlib import contextmanager

import mysql.connector
from mysql.connector import errorcode, pooling

from mdp.config import Settings
from mdp.serving.models import FIRST_DAY, LAST_DAY, Bar, Company
from mdp.serving.store import Conflict, Unavailable

INSERT_CHUNK = 2000


class MySQLStore:
    """The same tables as the Cassandra keyspace, in database api_prod or api_test."""

    def __init__(self, cfg: Settings, database: str, pool_size: int = 4):
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name=f"api-{database}", pool_size=pool_size, host=cfg.mysql_host, port=cfg.mysql_port,
                user=cfg.mysql_user, password=cfg.mysql_password, database=database,
                autocommit=True, connection_timeout=5)
        except mysql.connector.Error as exc:
            raise Unavailable(f"mysql: {exc}") from exc

    @contextmanager
    def _cursor(self):
        try:
            conn = self.pool.get_connection()
            try:
                cur = conn.cursor()
                try:
                    yield cur
                finally:
                    cur.close()
            finally:
                conn.close()
        except mysql.connector.IntegrityError:
            raise
        except mysql.connector.Error as exc:
            raise Unavailable(f"mysql: {exc}") from exc

    def list_companies(self) -> list[Company]:
        with self._cursor() as cur:
            cur.execute("SELECT ticker, name, sector, exchange FROM companies ORDER BY ticker")
            return [Company(*row) for row in cur.fetchall()]

    def get_company(self, ticker: str) -> Company | None:
        with self._cursor() as cur:
            cur.execute("SELECT ticker, name, sector, exchange FROM companies WHERE ticker = %s", (ticker,))
            row = cur.fetchone()
        return Company(*row) if row else None

    def add_company(self, c: Company) -> None:
        try:
            with self._cursor() as cur:
                cur.execute("INSERT INTO companies (ticker, name, sector, exchange) VALUES (%s, %s, %s, %s)",
                            (c.ticker, c.name, c.sector, c.exchange))
        except mysql.connector.IntegrityError as exc:
            if exc.errno == errorcode.ER_DUP_ENTRY:
                raise Conflict(f"{c.ticker} already exists") from exc
            raise

    def put_bar(self, ticker: str, bar: Bar) -> None:
        with self._cursor() as cur:
            cur.execute(_UPSERT_BAR, (ticker, bar.trade_date, bar.open, bar.high, bar.low, bar.close, bar.volume))

    def bars(self, ticker: str, start: dt.date | None = None, end: dt.date | None = None,
             limit: int | None = None) -> list[Bar]:
        sql = ("SELECT trade_date, `open`, high, low, `close`, volume FROM daily_bars "
               "WHERE ticker = %s AND trade_date >= %s AND trade_date <= %s ORDER BY trade_date")
        params: list = [ticker, start or FIRST_DAY, end or LAST_DAY]
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [Bar(d, float(o), float(h), float(lo), float(c), int(v)) for d, o, h, lo, c, v in cur.fetchall()]

    def upsert_companies(self, companies: Sequence[Company]) -> None:
        with self._cursor() as cur:
            cur.executemany(
                "INSERT INTO companies (ticker, name, sector, exchange) VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE name = VALUES(name), sector = VALUES(sector), exchange = VALUES(exchange)",
                [(c.ticker, c.name, c.sector, c.exchange) for c in companies])

    def upsert_bars(self, rows: Sequence[tuple[str, Bar]], concurrency: int = 64, batch_size: int = 1) -> None:
        args = [(t, b.trade_date, b.open, b.high, b.low, b.close, b.volume) for t, b in rows]
        with self._cursor() as cur:
            for i in range(0, len(args), INSERT_CHUNK):
                cur.executemany(_UPSERT_BAR, args[i:i + INSERT_CHUNK])


_UPSERT_BAR = (
    "INSERT INTO daily_bars (ticker, trade_date, `open`, high, low, `close`, volume) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE `open` = VALUES(`open`), high = VALUES(high), low = VALUES(low), "
    "`close` = VALUES(`close`), volume = VALUES(volume)")

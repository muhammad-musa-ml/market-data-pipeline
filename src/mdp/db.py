import pathlib
import time

import mysql.connector

from mdp.config import Settings

SCHEMA_FILE = pathlib.Path(__file__).resolve().parents[2] / "sql" / "schema.sql"


def connect(cfg: Settings, retries: int = 60, delay: float = 2.0):
    """Open a connection, retrying until MySQL accepts it."""
    last: Exception | None = None
    for _ in range(retries):
        try:
            conn = mysql.connector.connect(
                host=cfg.mysql_host,
                port=cfg.mysql_port,
                user=cfg.mysql_user,
                password=cfg.mysql_password,
                database=cfg.mysql_database,
                time_zone="+00:00",
                autocommit=False,
                connection_timeout=10,
            )
            return conn
        except mysql.connector.Error as exc:
            last = exc
            time.sleep(delay)
    raise RuntimeError(f"mysql not reachable at {cfg.mysql_host}:{cfg.mysql_port}") from last


def apply_schema(conn) -> None:
    sql = SCHEMA_FILE.read_text()
    cur = conn.cursor()
    for stmt in sql.split(";"):
        if stmt.strip():
            cur.execute(stmt)
    conn.commit()
    cur.close()

"""Create the databases, keyspaces and tables the serving tier uses. Safe to run repeatedly.

    python -m mdp.serving.init_schema [--only mysql|cassandra]

The migration files are sql/mysql/*.sql and sql/cassandra/*.cql, applied in name order. The CQL
files carry a ${keyspace} placeholder and are applied once per keyspace.
"""
import argparse
import pathlib
import string
import sys
import time

import mysql.connector

from mdp.config import Settings
from mdp.serving import cluster
from mdp.serving.settings import ServingSettings

SQL_DIR = pathlib.Path(__file__).resolve().parents[3] / "sql"
ENVIRONMENTS = ("prod", "test")
REPLICAS = 3


def statements(text: str) -> list[str]:
    lines = (ln.split("--", 1)[0] for ln in text.splitlines())
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def cql_statements(path: pathlib.Path, keyspace: str) -> list[str]:
    return statements(string.Template(path.read_text()).substitute(keyspace=keyspace))


def init_mysql(cfg: Settings, root_password: str) -> None:
    deadline = time.monotonic() + 120
    while True:
        try:
            root = mysql.connector.connect(host=cfg.mysql_host, port=cfg.mysql_port, user="root",
                                           password=root_password, connection_timeout=10)
            break
        except mysql.connector.Error:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    cur = root.cursor()
    for env in ENVIRONMENTS:
        db = f"api_{env}"
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cur.execute(f"GRANT ALL ON {db}.* TO `{cfg.mysql_user}`@`%`")
        cur.execute(f"USE {db}")
        for path in sorted((SQL_DIR / "mysql").glob("*.sql")):
            for stmt in statements(path.read_text()):
                cur.execute(stmt)
        print(f"init: mysql database {db} ready", flush=True)
    root.close()


def init_cassandra(cfg: ServingSettings) -> None:
    session = cluster.connect(cfg, wait=300)
    deadline = time.monotonic() + 60
    while True:
        hosts = session.cluster.metadata.all_hosts()
        down = [str(h.address) for h in hosts if not h.is_up]
        if len(hosts) >= REPLICAS and not down:
            break
        if time.monotonic() > deadline:
            raise SystemExit(f"init: expected {REPLICAS} nodes up, saw {len(hosts)} (down: {down})")
        time.sleep(1)
    for env in ENVIRONMENTS:
        for path in sorted((SQL_DIR / "cassandra").glob("*.cql")):
            for stmt in cql_statements(path, env):
                session.execute(stmt)
        print(f"init: cassandra keyspace {env} ready", flush=True)
    session.cluster.shutdown()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["mysql", "cassandra"])
    args = ap.parse_args()
    serving = ServingSettings.from_env()
    if args.only != "cassandra":
        init_mysql(Settings.from_env(), serving.mysql_root_password)
    if args.only != "mysql":
        init_cassandra(serving)
    return 0


if __name__ == "__main__":
    sys.exit(main())

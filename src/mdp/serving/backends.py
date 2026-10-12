from cassandra.cluster import NoHostAvailable

from mdp.config import Settings
from mdp.serving import cluster
from mdp.serving.cassandra_store import CassandraStore
from mdp.serving.mysql_store import MySQLStore
from mdp.serving.settings import ServingSettings
from mdp.serving.store import Store, Unavailable

BACKENDS = ("mysql", "cassandra")


def open_store(backend: str, db: str, serving: ServingSettings | None = None) -> Store:
    """The store for "prod" or "test" on one backend, from the environment's settings."""
    serving = serving or ServingSettings.from_env()
    if backend == "mysql":
        return MySQLStore(Settings.from_env(), f"api_{db}")
    if backend == "cassandra":
        try:
            return CassandraStore(cluster.shared(serving), db)
        except NoHostAvailable as exc:
            raise Unavailable(f"cassandra: {exc}") from exc
    raise ValueError(f"backend must be one of {BACKENDS}, not {backend!r}")

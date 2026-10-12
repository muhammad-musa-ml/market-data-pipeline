import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ServingSettings:
    backend: str
    cassandra_hosts: tuple[str, ...]
    cassandra_port: int
    cassandra_timeout: float
    mysql_root_password: str
    ingest_port: int

    @classmethod
    def from_env(cls) -> "ServingSettings":
        e = os.environ.get
        return cls(
            backend=e("API_BACKEND", "cassandra"),
            cassandra_hosts=tuple(h for h in e("CASSANDRA_HOSTS", "localhost").split(",") if h),
            cassandra_port=int(e("CASSANDRA_PORT", "9042")),
            cassandra_timeout=float(e("CASSANDRA_TIMEOUT", "5")),
            mysql_root_password=e("MYSQL_ROOT_PASSWORD", "mdp-root"),
            ingest_port=int(e("INGEST_PORT", "50061")),
        )

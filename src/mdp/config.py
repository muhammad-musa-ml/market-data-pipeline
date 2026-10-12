import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    kafka_bootstrap: str
    topic: str
    progress_topic: str
    partitions: int
    webhdfs_url: str
    hdfs_user: str
    hdfs_min_datanodes: int
    backfill_addr: str

    @classmethod
    def from_env(cls) -> "Settings":
        e = os.environ.get
        return cls(
            mysql_host=e("MYSQL_HOST", "localhost"),
            mysql_port=_int("MYSQL_PORT", 3306),
            mysql_user=e("MYSQL_USER", "mdp"),
            mysql_password=e("MYSQL_PASSWORD", "mdp"),
            mysql_database=e("MYSQL_DATABASE", "market"),
            kafka_bootstrap=e("KAFKA_BOOTSTRAP", "localhost:9092"),
            topic=e("BARS_TOPIC", "market.bars.v1"),
            progress_topic=e("PROGRESS_TOPIC", "market.producer-progress.v1"),
            partitions=_int("BARS_PARTITIONS", 4),
            webhdfs_url=e("WEBHDFS_URL", "http://localhost:9870"),
            hdfs_user=e("HDFS_USER", "hadoop"),
            hdfs_min_datanodes=_int("HDFS_MIN_DATANODES", 3),
            backfill_addr=e("BACKFILL_ADDR", "localhost:50051"),
        )

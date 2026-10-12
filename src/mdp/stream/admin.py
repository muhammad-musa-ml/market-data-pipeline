import sys
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from mdp.config import Settings

REPLICATION = 3
MIN_ISR = "2"


def wanted_topics(cfg: Settings) -> list[NewTopic]:
    return [
        NewTopic(cfg.topic, cfg.partitions, REPLICATION,
                 config={"min.insync.replicas": MIN_ISR, "retention.ms": str(7 * 24 * 3600 * 1000)}),
        NewTopic(cfg.progress_topic, 1, REPLICATION,
                 config={"min.insync.replicas": MIN_ISR, "cleanup.policy": "compact"}),
    ]


def wait_for_brokers(admin: AdminClient, want: int = 3, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            if len(admin.list_topics(timeout=5).brokers) >= want:
                return
        except KafkaException:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError(f"fewer than {want} brokers after {timeout:.0f}s")
        time.sleep(2)


def ensure_topics(cfg: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})
    wait_for_brokers(admin)
    for topic, fut in admin.create_topics(wanted_topics(cfg)).items():
        try:
            fut.result()
            print(f"admin: created {topic}", flush=True)
        except KafkaException as exc:
            if exc.args[0].name() != "TOPIC_ALREADY_EXISTS":
                raise
            print(f"admin: {topic} already exists", flush=True)

    meta = admin.list_topics(cfg.topic, timeout=10).topics[cfg.topic]
    if len(meta.partitions) != cfg.partitions:
        raise RuntimeError(
            f"{cfg.topic} has {len(meta.partitions)} partitions, expected {cfg.partitions}")


def delete_topics(cfg: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})
    for topic, fut in admin.delete_topics([cfg.topic, cfg.progress_topic], operation_timeout=30).items():
        fut.result()
        print(f"admin: deleted {topic}", flush=True)


def main() -> int:
    cfg = Settings.from_env()
    if sys.argv[1:] == ["delete"]:
        delete_topics(cfg)
    else:
        ensure_topics(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())

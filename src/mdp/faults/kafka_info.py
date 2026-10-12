"""Print partition leaders and in-sync replicas as one JSON document."""
import json
import sys

from confluent_kafka.admin import AdminClient

from mdp.config import Settings


def main() -> int:
    cfg = Settings.from_env()
    admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})
    md = admin.list_topics(cfg.topic, timeout=10)
    topic = md.topics[cfg.topic]
    print(json.dumps({
        "controller": md.controller_id,
        "brokers": sorted(md.brokers),
        "partitions": {
            str(p): {"leader": m.leader, "replicas": list(m.replicas), "isr": list(m.isrs)}
            for p, m in sorted(topic.partitions.items())
        },
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())

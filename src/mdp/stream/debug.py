"""Print what is on the topic. Joins a consumer group and lets the brokers assign partitions."""
import argparse
import json
import os
import sys

from confluent_kafka import Consumer, KafkaException

from mdp.config import Settings
from mdp.stream import codec


def main(argv: list[str] | None = None) -> int:
    cfg = Settings.from_env()
    ap = argparse.ArgumentParser(prog="python -m mdp.stream.debug")
    ap.add_argument("--group", default=os.environ.get("DEBUG_GROUP", "mdp-debug"))
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--max", type=int, default=0, help="stop after this many messages")
    args = ap.parse_args(argv)

    consumer = Consumer({
        "bootstrap.servers": cfg.kafka_bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([cfg.topic], on_assign=lambda c, parts: print(
        "assigned partitions:", sorted(p.partition for p in parts), flush=True))
    seen = 0
    try:
        while not args.max or seen < args.max:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            bar = codec.decode(msg.value())
            print(json.dumps({
                "partition": msg.partition(), "offset": msg.offset(), "key": msg.key().decode(),
                "id": bar.id, "date": bar.trade_date, "close": bar.close, "volume": bar.volume,
            }), flush=True)
            seen += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

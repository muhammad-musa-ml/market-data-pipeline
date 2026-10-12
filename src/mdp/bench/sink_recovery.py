"""How long a restarted sink takes to write again and to work off the backlog.

Protocol with the host script: print READY, wait for a line on stdin (the host
sends it right after `docker start`), then poll the checkpoints and the topic's
high watermarks every 250 ms. Prints one JSON line.
"""
import json
import sys
import time

from confluent_kafka import Consumer, TopicPartition

from mdp.config import Settings
from mdp.hdfs import WebHdfs
from mdp.stream import checkpoint


def main() -> int:
    cfg = Settings.from_env()
    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    meta = Consumer({"bootstrap.servers": cfg.kafka_bootstrap, "group.id": "mdp-bench-meta",
                     "enable.auto.commit": False})
    parts = range(cfg.partitions)

    print("READY", flush=True)
    sys.stdin.readline()
    t0 = time.time()

    # a transaction marker can occupy the last offset, so "caught up" allows one slot of slack
    target = {p: meta.get_watermark_offsets(TopicPartition(cfg.topic, p), timeout=10)[1] - 1
              for p in parts}
    start = {p: checkpoint.load(hdfs, p) for p in parts}
    first = caught_up = None
    while time.time() - t0 < 180:
        now = {p: checkpoint.load(hdfs, p) for p in parts}
        if first is None and any(now[p].batch > start[p].batch for p in parts):
            first = time.time() - t0
        if all(now[p].offset >= target[p] for p in parts):
            caught_up = time.time() - t0
            break
        time.sleep(0.25)
    backlog = sum(max(0, target[p] - start[p].offset) for p in parts)
    print(json.dumps({"backlog_messages": backlog,
                      "first_batch_s": None if first is None else round(first, 2),
                      "caught_up_s": None if caught_up is None else round(caught_up, 2)}), flush=True)
    meta.close()
    return 0 if caught_up is not None else 1


if __name__ == "__main__":
    sys.exit(main())

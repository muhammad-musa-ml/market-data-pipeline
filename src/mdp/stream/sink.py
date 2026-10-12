"""HDFS sink: Kafka partitions -> one Parquet file per poll batch.

Exactly-once comes from three things together. The sink assigns partitions
itself and starts from a checkpointed offset, never from consumer-group state.
Batch files have deterministic names, so replaying a batch overwrites it.
The checkpoint (next batch number, next offset) is written only after the file,
so a crash between the two just causes the same batch to be written again.
"""
import argparse
import os
import random
import signal
import sys
from collections import defaultdict
from collections.abc import Callable, Sequence

from mdp import layout
from mdp.config import Settings
from mdp.hdfs import FileStore, WebHdfs
from mdp.stream import checkpoint, codec

CrashHook = Callable[[str], None]


def no_crash(point: str) -> None:
    pass


class RandomCrash:
    """Kill the process at a named point with some probability (fault injection)."""

    def __init__(self, prob: float, points: Sequence[str] = ("after_parquet",), seed: int | None = None):
        self.prob, self.points = prob, set(points)
        self.rng = random.Random(seed)

    def __call__(self, point: str) -> None:
        if point in self.points and self.rng.random() < self.prob:
            print(f"sink: injected crash at {point}", flush=True)
            os._exit(137)


class PartitionSink:
    def __init__(self, store: FileStore, partition: int, crash: CrashHook = no_crash):
        self.store = store
        self.partition = partition
        self.crash = crash
        self.cp = checkpoint.load(store, partition)

    @property
    def start_offset(self) -> int:
        return self.cp.offset

    def ingest(self, records: Sequence[tuple[int, bytes]]) -> int:
        """Persist one poll batch of (offset, value) pairs. Returns rows written."""
        fresh = [(o, v) for o, v in records if o >= self.cp.offset]
        if not fresh:
            return 0
        table = codec.bars_to_table([codec.decode(v) for _, v in fresh])
        path = layout.batch_path(self.partition, self.cp.batch)
        self.store.write(path, codec.table_to_bytes(table))
        self.crash("after_parquet")
        advanced = checkpoint.Checkpoint(batch=self.cp.batch + 1, offset=fresh[-1][0] + 1)
        checkpoint.save(self.store, self.partition, advanced)
        self.cp = advanced
        self.crash("after_checkpoint")
        return table.num_rows


def run(cfg: Settings, partitions: list[int], crash: CrashHook, max_batch: int,
        poll_timeout: float) -> None:
    from confluent_kafka import Consumer, TopicPartition

    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    hdfs.wait_ready(cfg.hdfs_min_datanodes)
    sinks = {p: PartitionSink(hdfs, p, crash) for p in partitions}

    consumer = Consumer({
        "bootstrap.servers": cfg.kafka_bootstrap,
        "group.id": "mdp-sink",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "fetch.min.bytes": 1,
    })
    consumer.assign([TopicPartition(cfg.topic, p, s.start_offset) for p, s in sinks.items()])
    print("sink: resuming " + ", ".join(
        f"p{p}@{s.start_offset}/batch{s.cp.batch}" for p, s in sinks.items()), flush=True)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        while not stopping:
            by_partition: dict[int, list[tuple[int, bytes]]] = defaultdict(list)
            for msg in consumer.consume(num_messages=max_batch, timeout=poll_timeout):
                if msg.error():
                    if msg.error().fatal():
                        raise RuntimeError(msg.error())
                    print(f"sink: kafka error {msg.error()}", flush=True)
                    continue
                by_partition[msg.partition()].append((msg.offset(), msg.value()))
            for p, records in by_partition.items():
                rows = sinks[p].ingest(records)
                print(f"sink: p{p} batch {sinks[p].cp.batch - 1} rows={rows} next={sinks[p].cp.offset}",
                      flush=True)
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> int:
    e = os.environ.get
    cfg = Settings.from_env()
    ap = argparse.ArgumentParser(prog="python -m mdp.stream.sink")
    ap.add_argument("--partitions", default=e("SINK_PARTITIONS", ""),
                    help="comma list; default is every partition of the topic")
    ap.add_argument("--max-batch", type=int, default=int(e("SINK_MAX_BATCH", 5000)))
    ap.add_argument("--poll-timeout", type=float, default=float(e("SINK_POLL_TIMEOUT", 1.0)))
    ap.add_argument("--crash-prob", type=float, default=float(e("SINK_CRASH_PROB", 0)),
                    help="probability of dying between the Parquet write and the checkpoint")
    args = ap.parse_args(argv)

    parts = [int(x) for x in args.partitions.split(",") if x] or list(range(cfg.partitions))
    crash = RandomCrash(args.crash_prob) if args.crash_prob > 0 else no_crash
    run(cfg, parts, crash, args.max_batch, args.poll_timeout)
    return 0


if __name__ == "__main__":
    sys.exit(main())

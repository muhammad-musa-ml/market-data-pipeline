"""Keep the latest_quote table current from the bars topic.

    python -m mdp.serving.quotes [--db prod] [--from-beginning]

A consumer in group mdp-quotes reads committed messages only (the producer writes in transactions),
keeps the newest bar per ticker of each poll, and writes it at QUORUM before committing offsets. The
write carries the bar's trade date as its Cassandra timestamp, so whichever order bars arrive in, or
however often they are replayed, the row ends up as the newest day's bar and no condition is needed.
"""
import argparse
import datetime as dt
import logging
import sys
from collections.abc import Iterable

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from confluent_kafka import Consumer, KafkaException

from mdp.config import Settings
from mdp.serving import cluster
from mdp.serving.settings import ServingSettings
from mdp.stream import codec
from mdp.v1 import bar_pb2

log = logging.getLogger("quotes")
GROUP = "mdp-quotes"
EPOCH = dt.date(1970, 1, 1)


def write_timestamp(trade_date: dt.date) -> int:
    """Microseconds at midnight UTC of the bar's day: later days win, whatever order they arrive in."""
    return (trade_date - EPOCH).days * 86_400 * 1_000_000


def newest_per_ticker(bars: Iterable[bar_pb2.Bar]) -> dict[str, bar_pb2.Bar]:
    best: dict[str, bar_pb2.Bar] = {}
    for b in bars:
        cur = best.get(b.ticker)
        if cur is None or (b.trade_date, b.id) > (cur.trade_date, cur.id):
            best[b.ticker] = b
    return best


def statement(session, keyspace: str):
    # trade_date is set in the same statement so the row never mixes two days' columns
    stmt = session.prepare(
        f"UPDATE {keyspace}.latest_quote USING TIMESTAMP ? SET close = ?, volume = ?, bar_id = ?, "
        f"trade_date = ? WHERE ticker = ?")
    stmt.consistency_level = ConsistencyLevel.QUORUM
    return stmt


def bind(bar: bar_pb2.Bar) -> tuple:
    day = dt.date.fromisoformat(bar.trade_date)
    return write_timestamp(day), bar.close, bar.volume, bar.id, day, bar.ticker


def run(db: str, from_beginning: bool, max_idle: float = 0.0) -> int:
    cfg, serving = Settings.from_env(), ServingSettings.from_env()
    session = cluster.connect(serving, wait=300)
    stmt = statement(session, db)
    consumer = Consumer({
        "bootstrap.servers": cfg.kafka_bootstrap, "group.id": GROUP, "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "auto.offset.reset": "earliest" if from_beginning else "latest",
    })
    consumer.subscribe([cfg.topic])
    applied = 0
    idle = 0.0
    try:
        while True:
            msgs = consumer.consume(num_messages=2000, timeout=1.0)
            if not msgs:
                idle += 1.0
                if max_idle and idle >= max_idle:
                    break
                continue
            idle = 0.0
            for m in msgs:
                if m.error():
                    raise KafkaException(m.error())
            latest = newest_per_ticker(codec.decode(m.value()) for m in msgs)
            execute_concurrent_with_args(session, stmt, [bind(b) for b in latest.values()], concurrency=32)
            consumer.commit(asynchronous=False)
            applied += len(msgs)
            log.info("applied %d messages, %d tickers, %d in total", len(msgs), len(latest), applied)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return applied


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m mdp.serving.quotes")
    ap.add_argument("--db", choices=["prod", "test"], default="prod")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--exit-when-idle", type=float, default=0.0, metavar="SECONDS")
    args = ap.parse_args(argv)
    run(args.db, args.from_beginning, args.exit_when_idle)
    return 0


if __name__ == "__main__":
    sys.exit(main())

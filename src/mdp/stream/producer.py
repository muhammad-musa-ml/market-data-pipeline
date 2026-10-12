"""MySQL -> Kafka producer.

Rows are read incrementally (id greater than the last one sent) and sent in
transactions. Each transaction also writes a progress record naming the last
id it covers, so a restarted producer resumes after the last committed chunk
and neither repeats rows nor skips any. Idempotence, acks=all and a bounded
retry count cover broker failures within a transaction; if a transaction still
fails it is aborted and the chunk is sent again.
"""
import argparse
import os
import sys
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from mdp import db
from mdp.config import Settings
from mdp.stream import codec
from mdp.v1 import bar_pb2

FETCH_SQL = (
    "SELECT b.id, b.symbol_id, s.ticker, b.trade_date, b.`open`, b.high, b.low, b.`close`, "
    "b.volume, b.inserted_at "
    "FROM bars b JOIN symbols s ON s.symbol_id = b.symbol_id "
    "WHERE b.id > %s ORDER BY b.id LIMIT %s"
)
PROGRESS_KEY = b"bars"
MAX_CHUNK_ATTEMPTS = 6


def producer_config(cfg: Settings, transactional_id: str) -> dict:
    return {
        "bootstrap.servers": cfg.kafka_bootstrap,
        "client.id": "mdp-producer",
        "transactional.id": transactional_id,
        "enable.idempotence": True,
        "acks": "all",
        "message.send.max.retries": 10,
        "retry.backoff.ms": 250,
        "retry.backoff.max.ms": 3000,
        "delivery.timeout.ms": 45000,
        "transaction.timeout.ms": 60000,
        "linger.ms": 10,
        "compression.type": "lz4",
    }


def read_progress(cfg: Settings) -> int:
    """Last id covered by a committed transaction, 0 if there is none."""
    c = Consumer({
        "bootstrap.servers": cfg.kafka_bootstrap,
        "group.id": "mdp-producer-progress",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "isolation.level": "read_committed",
    })
    last = 0
    try:
        c.assign([TopicPartition(cfg.progress_topic, 0, 0)])
        while True:
            msg = c.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return last
                raise KafkaException(msg.error())
            prog = bar_pb2.ProducerProgress()
            prog.ParseFromString(msg.value())
            last = max(last, prog.last_id)
    finally:
        c.close()


def commit(p: Producer) -> None:
    for attempt in range(10):
        try:
            p.commit_transaction(30)
            return
        except KafkaException as exc:
            if not exc.args[0].retriable() or attempt == 9:
                raise
            time.sleep(1)


def send_chunk(p: Producer, cfg: Settings, rows: list[tuple]) -> None:
    last_id = rows[-1][0]
    progress = bar_pb2.ProducerProgress(last_id=last_id).SerializeToString()
    for attempt in range(MAX_CHUNK_ATTEMPTS):
        try:
            p.begin_transaction()
            for row in rows:
                bar = codec.bar_from_row(row)
                while True:
                    try:
                        p.produce(cfg.topic, key=bar.ticker.encode(), value=codec.encode(bar))
                        break
                    except BufferError:
                        p.poll(0.2)
            p.produce(cfg.progress_topic, key=PROGRESS_KEY, value=progress, partition=0)
            commit(p)
            return
        except KafkaException as exc:
            err = exc.args[0]
            if err.fatal() or attempt == MAX_CHUNK_ATTEMPTS - 1:
                raise
            print(f"producer: chunk ending at id {last_id} failed ({err.name()}), "
                  f"attempt {attempt + 1}; retrying", flush=True)
            try:
                p.abort_transaction(30)
            except KafkaException:
                pass  # nothing open, or already aborted by the failed commit
            time.sleep(1 + attempt)


@dataclass
class RunStats:
    sent: int = 0
    first_sent_at: float = 0.0
    last_sent_at: float = 0.0
    last_id: int = 0

    @property
    def rate(self) -> float:
        span = self.last_sent_at - self.first_sent_at
        return self.sent / span if span > 0 else 0.0


def run(cfg: Settings, chunk: int = 2000, poll_interval: float = 0.2,
        exit_when_idle: bool = False, transactional_id: str = "mdp-bars-producer") -> RunStats:
    p = Producer(producer_config(cfg, transactional_id))
    p.init_transactions(60)  # fences and aborts anything a dead predecessor left open
    stats = RunStats(last_id=read_progress(cfg))
    print(f"producer: resuming after id {stats.last_id}", flush=True)

    conn = db.connect(cfg)
    cur = conn.cursor()
    report_at = time.monotonic() + 5
    try:
        while True:
            conn.commit()  # new snapshot for every read
            cur.execute(FETCH_SQL, (stats.last_id, chunk))
            rows = cur.fetchall()
            if not rows:
                if exit_when_idle:
                    return stats
                time.sleep(poll_interval)
                continue
            now = time.monotonic()
            if not stats.sent:
                stats.first_sent_at = now
            send_chunk(p, cfg, rows)
            stats.sent += len(rows)
            stats.last_id = rows[-1][0]
            stats.last_sent_at = time.monotonic()
            if stats.last_sent_at >= report_at:
                print(f"producer: id={stats.last_id} sent={stats.sent} {stats.rate:,.0f} msg/s",
                      flush=True)
                report_at = stats.last_sent_at + 5
    finally:
        cur.close()
        conn.close()


def main(argv: list[str] | None = None) -> int:
    e = os.environ.get
    ap = argparse.ArgumentParser(prog="python -m mdp.stream.producer")
    ap.add_argument("--chunk", type=int, default=int(e("PRODUCER_CHUNK", 2000)))
    ap.add_argument("--poll-interval", type=float, default=float(e("PRODUCER_POLL_INTERVAL", 0.2)))
    ap.add_argument("--exit-when-idle", action="store_true")
    args = ap.parse_args(argv)
    stats = run(Settings.from_env(), args.chunk, args.poll_interval, args.exit_when_idle)
    print(f"producer: done sent={stats.sent} last_id={stats.last_id} {stats.rate:,.0f} msg/s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

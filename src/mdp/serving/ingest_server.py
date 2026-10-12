"""gRPC tick ingest. Definition in proto/mdp/v1/ingest.proto.

Writes are acknowledged by one replica (ONE) so a producer can always upload as long as a single
node is alive. The read waits for all three replicas (ALL): if any is down it fails with
UNAVAILABLE instead of answering from a replica that may not have seen every write.
"""
import datetime as dt
import logging
import math
import os
import signal
import sys
from collections.abc import Iterable
from concurrent import futures

import grpc
from cassandra import (
    ConsistencyLevel, OperationTimedOut, ReadFailure, ReadTimeout, Unavailable, WriteFailure, WriteTimeout,
)
from cassandra.cluster import NoHostAvailable, Session
from cassandra.concurrent import execute_concurrent_with_args

from mdp.serving import cluster, validate
from mdp.serving.settings import ServingSettings
from mdp.v1 import ingest_pb2 as pb
from mdp.v1 import ingest_pb2_grpc as rpc

log = logging.getLogger("ingest")

KEYSPACES = ("prod", "test")
WRITE_CONSISTENCY = ConsistencyLevel.ONE
READ_CONSISTENCY = ConsistencyLevel.ALL
DEFAULT_LIMIT = 1000
MAX_LIMIT = 10_000
CHUNK = 200
MAX_PRODUCER = 64
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


class Invalid(ValueError):
    pass


def day_of(ts_us: int) -> dt.date:
    return (EPOCH + dt.timedelta(microseconds=ts_us)).date()


def check_tick(t: pb.Tick) -> tuple:
    """The bound parameters for one tick, or Invalid."""
    try:
        validate.ticker(t.ticker)
    except validate.BadRequest as exc:
        raise Invalid(str(exc)) from None
    if not 0 < t.ts_us < 253_402_300_800 * 1_000_000:  # year 9999
        raise Invalid("ts_us must be microseconds since the epoch, after 1970 and before 10000")
    if not math.isfinite(t.price) or t.price <= 0:
        raise Invalid("price must be a positive number")
    if t.size < 0:
        raise Invalid("size must not be negative")
    if not t.producer or len(t.producer) > MAX_PRODUCER:
        raise Invalid(f"producer must be 1 to {MAX_PRODUCER} characters")
    return (t.ticker, day_of(t.ts_us), t.ts_us, t.producer, t.price, t.size)


class Statements:
    """Prepared statements for one keyspace, each with the consistency level its role calls for."""

    def __init__(self, session: Session, keyspace: str,
                 write: int = WRITE_CONSISTENCY, read: int = READ_CONSISTENCY):
        table = f"{keyspace}.ticks"
        self.put = session.prepare(
            f"INSERT INTO {table} (ticker, day, ts_us, producer, price, size) VALUES (?, ?, ?, ?, ?, ?)")
        self.get = session.prepare(
            f"SELECT ts_us, producer, price, size FROM {table} WHERE ticker = ? AND day = ? LIMIT ?")
        self.put.consistency_level = write
        self.get.consistency_level = read


DRIVER_ERRORS = (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, NoHostAvailable, ReadFailure,
                 WriteFailure)


def abort_for(context, exc: Exception, note: str = ""):
    """Turn a driver error into the gRPC status a client can act on."""
    if isinstance(exc, NoHostAvailable):
        # with a single usable host the driver reports that host's Unavailable inside NoHostAvailable
        exc = next((e for e in exc.errors.values() if isinstance(e, Unavailable)), exc)
    if isinstance(exc, Unavailable):
        level = ConsistencyLevel.value_to_name.get(exc.consistency, str(exc.consistency))
        code = grpc.StatusCode.UNAVAILABLE
        msg = f"consistency {level} needs {exc.required_replicas} replicas, {exc.alive_replicas} alive"
    elif isinstance(exc, (ReadTimeout, WriteTimeout, OperationTimedOut)):
        code, msg = grpc.StatusCode.DEADLINE_EXCEEDED, f"{type(exc).__name__}: replicas did not answer in time"
    else:
        code, msg = grpc.StatusCode.UNAVAILABLE, f"{type(exc).__name__}: {exc}"
    context.abort(code, msg + note)


class IngestService(rpc.IngestServicer):
    def __init__(self, session: Session, keyspaces: Iterable[str] = KEYSPACES,
                 write: int = WRITE_CONSISTENCY, read: int = READ_CONSISTENCY):
        self.session = session
        self.statements = {ks: Statements(session, ks, write, read) for ks in keyspaces}

    def _statements(self, context) -> Statements:
        ks = dict(context.invocation_metadata()).get("x-keyspace", "prod")
        if ks not in self.statements:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"x-keyspace must be one of {sorted(self.statements)}")
        return self.statements[ks]

    def PutTick(self, request, context):
        st = self._statements(context)
        try:
            params = check_tick(request)
        except Invalid as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        try:
            self.session.execute(st.put, params)
        except DRIVER_ERRORS as exc:
            abort_for(context, exc)
        return pb.PutReply(accepted=1)

    def PutTicks(self, request_iterator, context):
        st = self._statements(context)
        accepted = 0
        chunk: list[tuple] = []

        def flush():
            nonlocal accepted
            if chunk:
                execute_concurrent_with_args(self.session, st.put, chunk, concurrency=32)
                accepted += len(chunk)
                chunk.clear()

        try:
            for tick in request_iterator:
                chunk.append(check_tick(tick))
                if len(chunk) >= CHUNK:
                    flush()
            flush()
        except Invalid as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"tick {accepted + len(chunk)}: {exc}")
        except DRIVER_ERRORS as exc:
            # the batch is idempotent, so the producer just sends all of it again
            abort_for(context, exc, f"; {accepted} ticks were acknowledged before the failure")
        return pb.PutReply(accepted=accepted)

    def GetTicks(self, request, context):
        st = self._statements(context)
        try:
            validate.ticker(request.ticker)
            day = validate.iso_date(request.day, "day")
        except validate.BadRequest as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        limit = request.limit or DEFAULT_LIMIT
        if not 1 <= limit <= MAX_LIMIT:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"limit must be between 1 and {MAX_LIMIT}")
        try:
            rows = self.session.execute(st.get, (request.ticker, day, limit))
            ticks = [pb.Tick(ticker=request.ticker, ts_us=r.ts_us, price=r.price, size=r.size, producer=r.producer)
                     for r in rows]
        except DRIVER_ERRORS as exc:
            abort_for(context, exc)
        return pb.GetTicksReply(ticks=ticks)


def serve(cfg: ServingSettings) -> None:
    session = cluster.connect(cfg, wait=300)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    rpc.add_IngestServicer_to_server(IngestService(session), server)
    server.add_insecure_port(f"[::]:{cfg.ingest_port}")
    server.start()
    log.info("ingest listening on :%d (writes %s, reads %s)", cfg.ingest_port,
             ConsistencyLevel.value_to_name[WRITE_CONSISTENCY], ConsistencyLevel.value_to_name[READ_CONSISTENCY])
    signal.signal(signal.SIGTERM, lambda *_: server.stop(2))
    server.wait_for_termination()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    serve(ServingSettings.from_env())
    return 0


if __name__ == "__main__":
    sys.exit(main())

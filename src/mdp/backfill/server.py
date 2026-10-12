import logging
import os
import signal
import sys
from concurrent import futures

import grpc
import mysql.connector

from mdp import db
from mdp.backfill import blocks, export, summary
from mdp.config import Settings
from mdp.hdfs import HdfsError, HdfsNotFound, WebHdfs
from mdp.v1 import backfill_pb2 as pb
from mdp.v1 import backfill_pb2_grpc as rpc

log = logging.getLogger("backfill")

_DIMENSION = {pb.DIMENSION_SECTOR: "sector", pb.DIMENSION_EXCHANGE: "exchange"}
_OUTCOME = {
    summary.Outcome.CREATED: pb.CACHE_CREATED,
    summary.Outcome.REUSED: pb.CACHE_REUSED,
    summary.Outcome.RECREATED: pb.CACHE_RECREATED,
    summary.Outcome.UNAVAILABLE: pb.CACHE_UNAVAILABLE,
}


class BackfillService(rpc.BackfillServicer):
    def __init__(self, cfg: Settings, hdfs: WebHdfs):
        self.cfg = cfg
        self.hdfs = hdfs
        self.summaries = summary.SymbolSummaries(hdfs)

    def _guard(self, context, fn):
        try:
            return fn()
        except (export.BadRequest, summary.UnknownSymbol) as exc:
            code = grpc.StatusCode.NOT_FOUND if isinstance(exc, summary.UnknownSymbol) \
                else grpc.StatusCode.INVALID_ARGUMENT
            context.abort(code, str(exc))
        except HdfsNotFound as exc:
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        except (HdfsError, mysql.connector.Error, RuntimeError) as exc:
            log.warning("request failed: %s", exc)
            context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))

    def ExportBars(self, request, context):
        def work():
            dimension = _DIMENSION.get(request.dimension)
            if dimension is None:
                raise export.BadRequest("dimension must be SECTOR or EXCHANGE")
            export.check_dest(request.dest_path)
            conn = db.connect(self.cfg, retries=20, delay=1.0)
            try:
                res = export.export_bars(conn, self.hdfs, dimension, request.name, request.dest_path)
            finally:
                conn.close()
            log.info("exported %d rows (%s=%s) to %s", res.rows, dimension, request.name, res.path)
            return pb.ExportBarsReply(
                rows=res.rows, path=res.path, bytes=res.bytes,
                replication=export.EXPORT_REPLICATION, block_size=export.EXPORT_BLOCK_SIZE)
        return self._guard(context, work)

    def BlockDistribution(self, request, context):
        def work():
            d = blocks.block_distribution(self.hdfs, request.path)
            return pb.BlockDistributionReply(
                total_blocks=d.total_blocks, replication=d.replication, length=d.length,
                block_size=d.block_size,
                datanodes=[pb.DatanodeBlocks(datanode=h, blocks=n) for h, n in d.per_datanode.items()])
        return self._guard(context, work)

    def SymbolSummary(self, request, context):
        def work():
            s, outcome, cache_path = self.summaries.summarize(request.ticker, request.source_path)
            log.info("%s: %s", request.ticker, outcome.value)
            return pb.SymbolSummaryReply(
                ticker=s.ticker, rows=s.rows, avg_close=s.avg_close, vwap=s.vwap,
                first_date=s.first_date, last_date=s.last_date,
                cache=_OUTCOME[outcome], cache_path=cache_path)
        return self._guard(context, work)


def serve(cfg: Settings, port: int) -> None:
    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    hdfs.wait_ready(cfg.hdfs_min_datanodes)
    db.connect(cfg).close()  # block until MySQL answers
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_BackfillServicer_to_server(BackfillService(cfg, hdfs), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info("backfill service listening on :%d", port)
    signal.signal(signal.SIGTERM, lambda *_: server.stop(2))
    server.wait_for_termination()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    serve(Settings.from_env(), int(os.environ.get("BACKFILL_PORT", 50051)))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""End-to-end lag: MySQL insert time -> time the HDFS batch file holding the row was written.

Both clocks are the host's (everything runs in containers on one kernel), so
the difference needs no skew correction. Prints one JSON line.
"""
import argparse
import calendar
import io
import json
import sys
import time

import numpy as np
import pyarrow.parquet as pq

from mdp import db
from mdp.config import Settings
from mdp.hdfs import WebHdfs
from mdp.stream import checkpoint


def inserted_at_epoch(ts) -> float:
    return calendar.timegm(ts.utctimetuple()) + ts.microsecond / 1e6


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-id", type=int, required=True)
    ap.add_argument("--to-id", type=int, required=True)
    ap.add_argument("--timeout", type=float, default=120.0)
    args = ap.parse_args()

    cfg = Settings.from_env()
    conn = db.connect(cfg)
    cur = conn.cursor()
    cur.execute("SELECT id, inserted_at FROM bars WHERE id BETWEEN %s AND %s",
                (args.from_id, args.to_id))
    inserted = {i: inserted_at_epoch(ts) for i, ts in cur.fetchall()}
    cur.close()
    conn.close()

    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    landed: dict[int, float] = {}
    seen_files: set[str] = set()
    deadline = time.monotonic() + args.timeout
    while True:
        for p in range(cfg.partitions):
            for path in checkpoint.trusted_batch_paths(checkpoint.load(hdfs, p), p):
                if path in seen_files:
                    continue
                seen_files.add(path)
                mtime = hdfs.status(path)["modificationTime"] / 1000.0
                ids = pq.read_table(io.BytesIO(hdfs.read(path)), columns=["id"]).column("id").to_pylist()
                for i in ids:
                    if i in inserted:
                        landed[i] = mtime
        if len(landed) == len(inserted) or time.monotonic() > deadline:
            break
        time.sleep(1)

    lags = np.array([landed[i] - inserted[i] for i in landed])
    out = {"rows": len(inserted), "landed": len(landed)}
    if len(lags):
        out.update(p50=float(np.percentile(lags, 50)), p95=float(np.percentile(lags, 95)),
                   p99=float(np.percentile(lags, 99)), max=float(lags.max()), min=float(lags.min()))
    print(json.dumps(out), flush=True)
    return 0 if len(landed) == len(inserted) else 1


if __name__ == "__main__":
    sys.exit(main())

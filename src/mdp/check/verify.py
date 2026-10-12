"""Compare what is on HDFS (trusted batch files only) with MySQL.

Reports duplicates, gaps, extras and value mismatches. Exit status is 0 only
when all four are empty.
"""
import argparse
import io
import json
import sys
import time
from dataclasses import asdict, dataclass, field

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from mdp import db, layout
from mdp.config import Settings
from mdp.hdfs import WebHdfs
from mdp.stream import checkpoint, codec

VALUE_COLUMNS = ("open", "high", "low", "close")
SAMPLE = 10


@dataclass
class Report:
    expected: int = 0
    found_rows: int = 0
    distinct: int = 0
    duplicates: list[int] = field(default_factory=list)
    gaps: list[int] = field(default_factory=list)
    extras: list[int] = field(default_factory=list)
    mismatched: list[int] = field(default_factory=list)
    partition_conflicts: list[str] = field(default_factory=list)
    duplicate_count: int = 0
    gap_count: int = 0
    extra_count: int = 0
    mismatch_count: int = 0

    @property
    def ok(self) -> bool:
        return not (self.duplicate_count or self.gap_count or self.extra_count
                    or self.mismatch_count or self.partition_conflicts)


def compare(expected: pa.Table, found: pa.Table) -> Report:
    """`expected` comes from MySQL, `found` from the trusted HDFS batch files.

    Both need columns id, ticker, trade_date, open, high, low, close, volume.
    """
    rep = Report(expected=expected.num_rows, found_rows=found.num_rows)
    exp_ids = expected.column("id").to_numpy()
    got_ids = found.column("id").to_numpy()

    uniq, counts = np.unique(got_ids, return_counts=True)
    rep.distinct = len(uniq)
    dup = uniq[counts > 1]
    rep.duplicate_count = int(len(dup))
    rep.duplicates = [int(i) for i in dup[:SAMPLE]]

    gaps = np.setdiff1d(exp_ids, uniq)
    rep.gap_count = int(len(gaps))
    rep.gaps = [int(i) for i in gaps[:SAMPLE]]
    extras = np.setdiff1d(uniq, exp_ids)
    rep.extra_count = int(len(extras))
    rep.extras = [int(i) for i in extras[:SAMPLE]]

    # value check on the ids both sides have, using the first copy of each
    first = np.sort(np.unique(got_ids, return_index=True)[1])
    f = found.take(pa.array(first)).sort_by("id")
    both = np.intersect1d(exp_ids, uniq)
    e = expected.filter(pc.is_in(expected.column("id"), pa.array(both))).sort_by("id")
    f = f.filter(pc.is_in(f.column("id"), pa.array(both)))
    bad = np.zeros(len(both), dtype=bool)
    for col in VALUE_COLUMNS:
        bad |= ~np.isclose(e.column(col).to_numpy(), f.column(col).to_numpy(), rtol=0, atol=1e-9)
    bad |= e.column("volume").to_numpy() != f.column("volume").to_numpy()
    bad |= np.array(e.column("ticker").to_pylist()) != np.array(f.column("ticker").to_pylist())
    bad |= e.column("trade_date").to_numpy() != f.column("trade_date").to_numpy()
    rep.mismatch_count = int(bad.sum())
    rep.mismatched = [int(i) for i in both[bad][:SAMPLE]]
    return rep


def read_hdfs_rows(hdfs: WebHdfs, partitions: int, cache: dict[str, pa.Table]) -> tuple[pa.Table, list[str]]:
    """All rows from trusted batch files. Files below a checkpoint never change, so cache them."""
    tables, conflicts = [], []
    owner: dict[str, int] = {}
    for p in range(partitions):
        cp = checkpoint.load(hdfs, p)
        for path in checkpoint.trusted_batch_paths(cp, p):
            if path not in cache:
                cache[path] = pq.read_table(io.BytesIO(hdfs.read(path)))
            t = cache[path]
            tables.append(t)
            for ticker in pc.unique(t.column("ticker")).to_pylist():
                if owner.setdefault(ticker, p) != p:
                    conflicts.append(f"{ticker} in partitions {owner[ticker]} and {p}")
    if not tables:
        return codec.BAR_SCHEMA.empty_table(), conflicts
    return pa.concat_tables(tables), sorted(set(conflicts))


def read_mysql_rows(conn, through_id: int | None) -> pa.Table:
    cur = conn.cursor()
    limit = "" if through_id is None else f"WHERE b.id <= {int(through_id)}"
    cur.execute(
        "SELECT b.id, s.ticker, b.trade_date, b.`open`, b.high, b.low, b.`close`, b.volume "
        f"FROM bars b JOIN symbols s ON s.symbol_id = b.symbol_id {limit} ORDER BY b.id"
    )
    rows = cur.fetchall()
    cur.close()
    conn.commit()
    cols = list(zip(*rows)) if rows else [[]] * 8
    return pa.table({
        "id": pa.array(cols[0], pa.int64()),
        "ticker": pa.array(cols[1], pa.string()),
        "trade_date": pa.array(cols[2], pa.date32()),
        "open": pa.array([float(x) for x in cols[3]], pa.float64()),
        "high": pa.array([float(x) for x in cols[4]], pa.float64()),
        "low": pa.array([float(x) for x in cols[5]], pa.float64()),
        "close": pa.array([float(x) for x in cols[6]], pa.float64()),
        "volume": pa.array(cols[7], pa.int64()),
    })


def wait_and_compare(cfg: Settings, through_id: int | None, timeout: float) -> Report:
    conn = db.connect(cfg)
    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    hdfs.wait_ready(1)
    if through_id is None:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM bars")
        through_id = int(cur.fetchone()[0])
        cur.close()
        conn.commit()
    expected = read_mysql_rows(conn, through_id)
    cache: dict[str, pa.Table] = {}
    deadline = time.monotonic() + timeout
    while True:
        found, conflicts = read_hdfs_rows(hdfs, cfg.partitions, cache)
        rep = compare(expected, found)
        rep.partition_conflicts = conflicts
        if rep.gap_count == 0 or time.monotonic() > deadline:
            return rep
        print(f"check: waiting, {rep.gap_count} ids not on hdfs yet", flush=True)
        time.sleep(3)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mdp.check.verify")
    ap.add_argument("--through-id", type=int, default=None,
                    help="only check ids up to this one (default: MySQL max at start)")
    ap.add_argument("--timeout", type=float, default=120.0,
                    help="seconds to wait for the sink to catch up before calling gaps real")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    rep = wait_and_compare(Settings.from_env(), args.through_id, args.timeout)
    if args.json:
        print(json.dumps({**asdict(rep), "ok": rep.ok}))
    else:
        print(f"expected rows (MySQL)   : {rep.expected}")
        print(f"rows on HDFS            : {rep.found_rows} ({rep.distinct} distinct ids)")
        print(f"duplicates              : {rep.duplicate_count} {rep.duplicates}")
        print(f"gaps                    : {rep.gap_count} {rep.gaps}")
        print(f"extras                  : {rep.extra_count} {rep.extras}")
        print(f"value mismatches        : {rep.mismatch_count} {rep.mismatched}")
        print(f"ticker/partition errors : {rep.partition_conflicts or 0}")
        print("RESULT: " + ("OK" if rep.ok else "FAIL"))
    return 0 if rep.ok else 1


if __name__ == "__main__":
    sys.exit(main())

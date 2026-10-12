"""End-to-end check of stage 2 against a running stage 1 stack.

    python scripts/analytics_check.py            # starts the analytics services if they are down
    python scripts/analytics_check.py --down     # and stops them again afterwards

It expects the sink to have caught up (python -m mdp.check.verify says OK). It compacts the
trusted batches, compares the table with MySQL, runs the three-API question, reads the
bucketing plans, builds the serving extract and checks its manifest against the table.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "bench"))

import stack  # noqa: E402
from run_bench import spark_job  # noqa: E402
from stack import compose, die, say  # noqa: E402

SERVICES = ["metastore", "spark-master", "spark-worker-1", "spark-worker-2", "driver"]


def check(ok: bool, what: str) -> None:
    say(("ok   " if ok else "FAIL ") + what)
    if not ok:
        die(what)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--down", action="store_true", help="stop the analytics services at the end")
    args = ap.parse_args()

    compose("--profile", "analytics", "up", "-d", "--build", *SERVICES, timeout=1800)
    stack.wait_for(lambda: stack.live_datanodes() >= 3, 120, what="3 DataNodes")

    mysql_rows = int(stack.sql("SELECT COUNT(*) FROM bars"))
    verdict = stack.verify(timeout=120, through_id=None)
    check(verdict.get("ok", False), f"HDFS matches MySQL ({mysql_rows} rows) before compaction")

    doc = spark_job("compact")
    say(f"compact: {doc}")
    check(doc["rows"] == mysql_rows, "market.bars has every MySQL row, once")
    check(doc["table_files"] == doc["buckets"], f"{doc['trusted_files']} batch files became {doc['buckets']} bucket files")

    doc = spark_job("question")
    check(doc["identical"], "RDD, DataFrame and SQL answers are identical")

    doc = spark_job("bucketing", "--reps", "1")
    v = doc["variants"]
    check(v["bucketed"]["group_by"]["exchanges"] == 0 and v["bucketed"]["join"]["exchanges"] == 0,
          "no Exchange in the bucketed GROUP BY and join")
    check(v["flat"]["group_by"]["exchanges"] >= 1 and v["flat"]["join"]["exchanges"] >= 2,
          "the same queries shuffle on plain files")

    doc = spark_job("serving")
    check(doc["source_rows"] == mysql_rows, "serving extract covers every bar")
    manifest = json.loads(compose("exec", "-T", "namenode", "curl", "-s", "-L",
                                  "http://localhost:9870/webhdfs/v1/market/serving/_manifest.json?op=OPEN").stdout)
    check(manifest["source_max_id"] == int(stack.sql("SELECT MAX(id) FROM bars")),
          "manifest on HDFS names the last bar id")

    if args.down:
        compose("--profile", "analytics", "stop", *SERVICES)
    say("stage 2 check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

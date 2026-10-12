"""spark-submit entry point for every analytics job.

    docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py compact
    docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py ml --depths 1 2 3
    docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py text2sql --backend vertex

The last line of stdout is `RESULT <json>`; bench/run_bench.py reads it.
"""
import argparse
import json
import sys
import time

from mdp import layout
from mdp.analytics import bucketing, caching, compact, model, question, serving
from mdp.analytics.names import HDFS_URI
from mdp.analytics.session import get_spark
from mdp.config import Settings
from mdp.hdfs import WebHdfs
from mdp.stream.trusted import trusted_paths


def trusted_uris(cfg: Settings, hdfs: WebHdfs) -> list[str]:
    return [HDFS_URI + p for p in trusted_paths(hdfs, cfg.partitions)]


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="jobs.py")
    sub = p.add_subparsers(dest="job", required=True)
    sub.add_parser("compact", help="trusted batch files -> bucketed Hive table market.bars")
    sub.add_parser("question", help="one question answered with RDD, DataFrame and SQL")
    b = sub.add_parser("bucketing", help="plans and timings, bucketed vs plain")
    b.add_argument("--reps", type=int, default=3)
    c = sub.add_parser("caching", help="repeated aggregations with and without cache()")
    c.add_argument("--repeats", type=int, default=3)
    m = sub.add_parser("ml", help="decision-tree volatility model, baselines, depth sweep")
    m.add_argument("--depths", type=int, nargs="+", default=model.DEPTHS)
    sub.add_parser("serving", help="write the extract stage 3 reads")
    t = sub.add_parser("text2sql", help="score a model on the evaluation questions")
    t.add_argument("--backend", choices=["vertex", "ollama"], default=None)
    t.add_argument("--repairs", type=int, default=1)
    t.add_argument("--only", nargs="+", help="case ids to run")
    a = sub.add_parser("ask", help="one question")
    a.add_argument("text")
    a.add_argument("--backend", choices=["vertex", "ollama"], default=None)
    sub.add_parser("schema", help="print the schema text the prompt uses")
    return p


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    cfg = Settings.from_env()
    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    spark = get_spark(f"mdp-{args.job}")
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    if args.job == "compact":
        result = compact.run(spark, trusted_uris(cfg, hdfs), HDFS_URI + layout.SYMBOLS_DIM)
    elif args.job == "question":
        result = question.run(spark)
    elif args.job == "bucketing":
        result = bucketing.run(spark, args.reps)
    elif args.job == "caching":
        result = caching.run(spark, args.repeats)
    elif args.job == "ml":
        result = model.run(spark, depths=args.depths)
    elif args.job == "serving":
        result = serving.run(spark, hdfs, trusted_files=len(trusted_uris(cfg, hdfs)))
    elif args.job == "schema":
        from mdp.analytics.text2sql import prompt
        print(prompt.schema_text(spark))
        return 0
    else:
        from mdp.analytics.text2sql import ask, backends, evalset, evaluate
        backend = backends.from_env(args.backend)
        if args.job == "ask":
            ans = ask.ask(spark, args.text, backend)
            print(ans.sql)
            for row in ans.rows:
                print(row)
            result = {"sql": ans.sql, "rows": [list(map(str, r)) for r in ans.rows],
                      "attempts": ans.attempts, "seconds": round(ans.total_seconds, 2)}
        else:
            cases = [c for c in evalset.CASES if not args.only or c.id in args.only]
            result = evaluate.run(spark, backend, cases, args.repairs)

    result = {"job": args.job, "wall_seconds": round(time.time() - t0, 2), **result}
    print("RESULT " + json.dumps(result, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

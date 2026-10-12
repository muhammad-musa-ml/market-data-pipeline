# market-data-pipeline

A market-data platform that runs on one machine with Docker Compose. Synthetic daily bars go from
MySQL through a three-broker Kafka cluster into exactly-once Parquet files on HDFS, a gRPC service
moves historical tables from MySQL to HDFS in bulk and answers per-symbol aggregates while DataNodes
die, and a small Spark cluster with a Hive metastore compacts the bars into a bucketed warehouse
table, trains a volatility model on it and answers questions asked in English. The data comes from a
seeded generator; none of it is real market data.

This repository is at stage 2 of 3. Stage 3 (a Cassandra serving tier with a REST API) is not here
yet; stage 2 leaves the dataset it will read under `/market/serving`.

## Where it came from

Two projects in CS 544 (Big Data Systems) at UW-Madison started this. In one, a Python gRPC server
queried MySQL, wrote Parquet to a small HDFS cluster and answered an aggregate from per-key cache
files, surviving the loss of a DataNode. In the other, a single Kafka broker fed a consumer that
wrote Parquet to HDFS with offset checkpoints. I rebuilt both from scratch around a different
domain (daily stock bars) and tried to make them something you could run and break on purpose:
a three-broker KRaft cluster with replication 3, a producer that restarts without repeating or
skipping rows, a sink that survives being killed between its Parquet write and its checkpoint, a
checker that compares HDFS against MySQL row by row, scripted fault drills, and a benchmark
runner. The generator, protobuf contracts, services and tests are all my own.

Stage 2 comes from the two Spark projects in the same course, which analysed a competitive-programming
dataset: Hive tables with a bucketed fact table, the same question in the RDD, DataFrame and SQL APIs,
query plans and caching, a Gemini-backed natural-language-to-SQL function, and a decision-tree
regression with a depth sweep. Here those run on the bars from stage 1 instead. What I added to make
it more than an exercise: the Hive metastore is a separate service on MySQL, so tables survive
restarts of every Spark process; compaction reads only the sink's trusted batch files and drops
duplicate ids; the bucketing and caching experiments are repeatable jobs with a benchmark runner;
the model uses a time-based split with a gap, a validation block for choosing the depth, and
three naive baselines; the text-to-SQL path validates what the model returns (one SELECT, no writes,
a LIMIT), retries once with the error, works against Vertex AI or a local Ollama model, and is scored
on 28 questions with hand-written gold SQL; and Spark writes a preprocessed extract for stage 3.
Because geometric Brownian motion has nothing to predict about tomorrow's volatility, I also changed
the generator so volatility clusters, and the model only has something to learn because of that
(`docs/CONTRACT.md` has the details).

## Running stage 1 (ingest)

You need Docker with Compose 2.20 or newer and about 6 GB of memory available to the Docker VM (the
containers' limits add up to 5.4 GB). Python 3.11+ is only needed for the host-side scripts and the
unit tests.

```
docker compose up -d --build
docker compose ps -a
```

The first start pulls and builds the images, which takes a few minutes. `loader` fills MySQL with 130000
bars (50 symbols, 2600 trading days) and exits; `kafka-init` creates the topics and exits; the
producer and sink then stream the history to HDFS. Watch it with `docker compose logs -f sink`.

When the sink has caught up, compare HDFS with MySQL:

```
docker compose run --rm tools python -m mdp.check.verify
```

which should end with

```
expected rows (MySQL)   : 130000
rows on HDFS            : 130000 (130000 distinct ids)
duplicates              : 0 []
gaps                    : 0 []
extras                  : 0 []
value mismatches        : 0 []
ticker/partition errors : 0
RESULT: OK
```

Try the backfill service (it listens on 127.0.0.1:8320 and the client runs inside the network):

```
docker compose run --rm tools python -m mdp.backfill.client export --exchange NASDAQ --dest /market/exports/nasdaq.parquet
docker compose run --rm tools python -m mdp.backfill.client blocks /market/exports/nasdaq.parquet
docker compose run --rm tools python -m mdp.backfill.client summary <ticker> --source /market/exports/nasdaq.parquet
```

Append new bars while everything is running, and watch them arrive:

```
GEN_RATE=200 docker compose --profile live up -d generator-live
docker compose --profile debug run --rm debug-consumer
```

## Running stage 2 (analytics)

This needs the stage 1 data: the sink must have caught up (the checker above says `RESULT: OK`).
After that Kafka is not needed, so stop it to leave room; the analytics services add about 6.5 GB of
container limits (about 9 GB with MySQL and HDFS), so give the Docker VM 10 GB or more.

```
docker compose stop kafka-1 kafka-2 kafka-3 producer sink backfill
docker compose --profile analytics up -d --build metastore spark-master spark-worker-1 spark-worker-2 driver
```

The first build downloads the Spark image and a JDBC driver. `metastore-init` creates the `metastore`
database in MySQL and loads the Hive schema into it; the master UI is at http://127.0.0.1:8360.
Every job is one `spark-submit` of `jobs.py` inside the driver container; each ends with a
`RESULT {...}` line of JSON:

```
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py compact
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py question
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py bucketing --inflate 20
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py caching
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py ml
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py serving
```

`compact` builds the `market.bars` Hive table (bucketed by ticker into 8 buckets, sorted by date), the
`market.symbols` table and a `market.daily_returns` view from the trusted batch files; its RESULT line
reports `rows` equal to the MySQL row count and `table_files` equal to `buckets`. `question` answers
one question three ways and reports `"identical": true`. `bucketing` prints the physical plans for a
GROUP BY and a join on ticker over a bucketed and a plain copy (the bucketed ones have no `Exchange`);
`--inflate N` stacks N shifted copies of the bars first so timings are not just scheduling noise.
`ml` fits the decision tree, three baselines and a depth sweep from 1 to 30 and takes several minutes.
`serving` writes `/market/serving` (see `docs/CONTRACT.md`).

Text-to-SQL needs a model. For Vertex AI mount your application default credentials and name a project:

```
export GCP_ADC_FILE=/path/to/application_default_credentials.json GOOGLE_CLOUD_PROJECT=<project>
docker compose --profile analytics up -d driver
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py ask "How many symbols are listed on each exchange?"
docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py text2sql
```

`ask` prints the SQL the model wrote and the rows it returned; `text2sql` scores the whole question set
(28 model calls, which cost money on Vertex). The default model is `gemini-3.5-flash`; `MDP_SQL_MODEL`
changes it. For a local model run Ollama on the host and add `--backend ollama` with
`OLLAMA_MODEL=<name>` set in the driver's environment (`docker compose exec -e OLLAMA_MODEL=... driver ...`).

`python scripts/analytics_check.py` runs the compaction, the three-API question, the bucketing plans
and the serving extract against the running stack and checks each against MySQL or the plans.
`python bench/run_bench.py spark --reps 1` runs the stage 2 measurements into `bench/scratch/`.

### Tests

```
pip install -r requirements-dev.txt
python scripts/gen_proto.py
pytest
```

The unit tests need no containers. The Spark tests (`tests/analytics`) are skipped on a machine
without pyspark and Java; run them in the driver container, which has both:

```
docker compose exec -w /app driver python3 -m pytest tests/analytics tests/test_sqlcheck.py tests/test_backends.py
```

or `pip install -r requirements-spark.txt` on a machine with JDK 17+, which is what the `spark` job in CI does. `python scripts/integration_test.py` brings up a fresh stack,
runs the checker and the backfill RPCs against it, tampers with the output to confirm the checker
notices, and tears the stack down (`--keep` leaves it up, `--faults` adds the drills below).

### Fault drills

```
python scripts/faults.py datanode     # kill a DataNode while the aggregate RPC is being called
python scripts/faults.py broker       # kill the broker leading partition 0 during production
python scripts/faults.py sink-crash   # crash the sink between Parquet write and checkpoint, restart, repeat
python scripts/faults.py producer-crash   # kill the producer at random moments and start it again
```

Each ends by running the checker where it applies and exits non-zero if anything is off.

### Tear down

```
docker compose --profile live --profile tools --profile debug --profile analytics down -v
```

`docs/CONTRACT.md` lists the schemas, topic, HDFS paths, service names, ports and environment
variables that the later stages build on.

## Measured results

Not filled in yet. The benchmark runner is `bench/run_bench.py`; the final runs (at least five
repetitions each, on a quiet machine) write CSVs to `bench/results/`.

## Limitations

- Everything is on one machine, so "three brokers" and "three DataNodes" are three containers
  sharing one disk and one kernel. Failures are real container kills, but there is no network
  partition or disk failure.
- The sink still writes one small Parquet file per poll batch. Stage 2 compacts them into a Hive
  table when you run `compact`, but the small files stay and the table is rebuilt from scratch each
  time, not appended to.
- The backfill export builds the whole result in memory before writing it; it is meant for tables
  of a few hundred thousand rows, not billions.
- The producer assumes a single MySQL writer (the generator). Concurrent writers could commit ids
  out of order and a row could be skipped.
- Containers use plaintext connections and development passwords. Nothing here is hardened.
- The Spark cluster is two workers with two cores each on the same machine as everything else, and
  the data is 130000 rows. At that size most of a query's time is scheduling, so the effect of
  bucketing or caching only shows in timings on a larger table (hence `--inflate`), while the plans
  show it at any size. The inflated copies repeat the same price paths.
- The volatility model finds structure that I put into the generator. It says the Spark pipeline,
  the split and the baselines work; it says nothing about real markets.
- The text-to-SQL question set is small, written by me for this schema, and a strong model may pass
  all of it; scoring is strict about column order and extra columns.
- The Hive schema script is Apache Hive's own (see its header); the metastore is the 2.3.10 server
  that Spark bundles, which is old, and DataNucleus cannot create its schema on MySQL 8.

## TODO

- Final measurement pass and the results section above.
- Stage 2 measurements: at least five repetitions each of the job runtimes, bucketed against plain
  GROUP BY and join on an inflated table, cached against uncached, the model metrics and depth sweep
  (with the plot from `bench/plot_depth_sweep.py`), and the text-to-SQL accuracy and latency; results go
  in `bench/results/` and in the section above.
- Stage 3: Cassandra serving tier and REST API, reading `/market/serving`.

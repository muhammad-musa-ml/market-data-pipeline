# market-data-pipeline

A market-data platform that runs on one machine with Docker Compose. Synthetic daily bars go from
MySQL through a three-broker Kafka cluster into exactly-once Parquet files on HDFS, a gRPC service
moves historical tables from MySQL to HDFS in bulk and answers per-symbol aggregates while DataNodes
die, and a small Spark cluster with a Hive metastore compacts the bars into a bucketed warehouse
table, trains a volatility model on it and answers questions asked in English. A three-node Cassandra
cluster then serves the bars through a REST API that runs over Cassandra or over MySQL with the same
tests, and through a gRPC service for uploading ticks whose writes and reads use different consistency
levels on purpose. The data comes from a seeded generator; none of it is real market data.

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

Stage 3 comes from two Cassandra projects in the same course. One ported a Flask and MySQL stock-price
REST API to a three-node Cassandra cluster (partition and clustering keys, static columns, a replication
factor and quorum sizes chosen so the API survives a dead node) and ran one pytest file against both
versions. The other was a gRPC service that took in weather-station records and favoured accepting writes
over consistent reads. Here both work on the bars. What I added: the schema comes from the queries and is
a migration file applied by an init service; a loader reads the Spark extract from HDFS with prepared
statements and bounded concurrency, can be run twice without changing anything, and checks what it wrote
against the extract's own rollups; the MySQL and Cassandra backends are one interface, and a parity test
sends the same requests to both and compares the answers byte for byte; the tests stop and kill Cassandra
nodes while the API and the ingest service are running and assert what still works; a script prints the
availability table for 0, 1 and 2 dead nodes; and a Kafka consumer keeps a latest-quote table current from
the bars topic.

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

## Running stage 3 (serving)

This needs the stage 1 data in HDFS and the serving extract from stage 2 (`jobs.py serving`), so run
those first. Spark and Kafka are not needed after that; stop them to leave room. The serving profile adds
about 4.6 GB of container limits (three Cassandra nodes at 1.25 GB each, the rest small); with MySQL and
HDFS that is about 7 GB, so give the Docker VM 8 GB or more.

```
docker compose --profile analytics stop driver spark-worker-1 spark-worker-2 spark-master metastore
docker compose stop kafka-1 kafka-2 kafka-3 producer sink backfill
docker compose up -d mysql namenode datanode1 datanode2 datanode3
docker compose --profile serving up -d --build api-mysql api-cassandra ingest
```

The last line also starts the three Cassandra nodes one after another (a couple of minutes) and the init
job that creates the keyspaces `prod` and `test` (replication factor 3), the matching MySQL databases and
the tables; the APIs and the ingest service wait for it. Load the extract into both backends:

```
docker compose --profile serving-tools run --rm serving-tools python -m mdp.serving.loader --backend cassandra
docker compose --profile serving-tools run --rm serving-tools python -m mdp.serving.loader --backend mysql
```

Each should end with `loaded 130000 rows (50 tickers) into cassandra/prod at <n> rows/s; 0 verify problems`
(and the same for mysql). Then try the API; port 8400 is MySQL, 8401 is Cassandra, and both give the same answers:

```
curl -s localhost:8401/prod/api/companies | head -c 300
curl -s -X POST localhost:8401/test/api/companies/ACME -H 'content-type: application/json' \
  -d '{"name": "Acme Corp", "sector": "Energy", "exchange": "NYSE"}'
curl -s -X PUT localhost:8401/test/api/companies/ACME/records/2024-01-02 -H 'content-type: application/json' \
  -d '{"open": 10, "high": 12, "low": 9.5, "close": 11, "volume": 5000}'
curl -s localhost:8401/test/api/companies/ACME/records/monthly
```

The endpoints and the status codes are listed in `docs/CONTRACT.md`. The ingest service listens on
127.0.0.1:8410; the client runs inside the network:

```
docker compose --profile serving-tools run --rm serving-tools python -m mdp.serving.ingest_client --keyspace test probe
```

`probe` makes one write, one bulk write and one read and prints the status of each. To see the consistency
levels work, stop a node (`docker stop market-data-pipeline-cassandra-2-1`) and probe again: the writes still
succeed and `GetTicks` answers `UNAVAILABLE` with "consistency ALL needs 3 replicas, 2 alive". The REST API
reads and writes at QUORUM, so with one node down it keeps working and a read still sees the write before it;
with two nodes down it answers 503. `docker start` brings the node back.

The latest-quote consumer needs Kafka back up:

```
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-init
docker compose --profile serving-quotes up -d quotes
docker compose --profile serving-tools run --rm serving-tools python -m mdp.serving.quotes --check
```

### Stage 3 tests

The unit tests need no cluster (`pytest tests/serving`, also part of plain `pytest`). The integration
tests need the stack above and the loaded extract:

```
python scripts/serving_test.py                 # empties test, loads prod, runs everything below
python scripts/serving_test.py --skip-faults   # without the tests that stop nodes
pytest integration/serving/test_api.py         # the REST suite alone, once per backend
```

`test_api.py` has no knowledge of which backend it is talking to; the fixture runs each test against MySQL
and then Cassandra. `test_parity.py` sends the same requests to both and compares the responses.
`test_node_down.py` kills a node (and, separately, stops it cleanly) and checks that QUORUM writes and the
reads right after them succeed, that creating a company still works, and that with two nodes down the API
says 503 and the refused write left nothing behind. `test_ingest_availability.py` does the 0, 1 and 2 nodes
down matrix for the ingest RPCs and checks that every tick acknowledged during the outage is readable at ALL
afterwards. The node tests take a few minutes because Cassandra needs about twenty seconds to notice a
crashed node and under a minute to come back. For the full table, including the REST endpoints:

```
python scripts/serving_faults.py matrix
```

Stage 3 measurements (the final runs need five repetitions and a quiet machine):

```
python bench/run_bench.py serving-latency --reps 5   # request latency at ONE, QUORUM and ALL
python bench/run_bench.py serving-load --reps 5      # loader rows/s, by concurrency and batch size
python bench/run_bench.py serving-api --reps 5       # REST latency over both backends
python bench/run_bench.py serving-matrix --reps 5    # the availability table, repeated
```

## Tests

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

## Fault drills

```
python scripts/faults.py datanode     # kill a DataNode while the aggregate RPC is being called
python scripts/faults.py broker       # kill the broker leading partition 0 during production
python scripts/faults.py sink-crash   # crash the sink between Parquet write and checkpoint, restart, repeat
python scripts/faults.py producer-crash   # kill the producer at random moments and start it again
```

Each ends by running the checker where it applies and exits non-zero if anything is off.

## Tear down

```
docker compose --profile live --profile tools --profile debug --profile analytics --profile serving \
  --profile serving-tools --profile serving-quotes down -v
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
- The three Cassandra nodes share one machine and one disk, and with replication factor 3 on three nodes
  every node holds every row. Nothing here shows how the schema behaves when data is spread over many
  nodes, and "a node is down" means a container was killed, not that a network or a disk failed.
- Listing the companies is a scan over all partitions (`SELECT DISTINCT`), and the sector or exchange filter
  runs in the application. That is fine for 50 tickers and not for tens of thousands.
- The monthly endpoint reads every daily row of the ticker (up to the 2600 loaded) and groups them in the
  application. Nothing is cached.
- Creating a company is a lightweight transaction, but the loader and the bar writes touch the same
  partitions with ordinary writes; Cassandra's documentation advises against mixing the two on one table.
  Nothing in the API can change or delete a company or a bar once written, which keeps that mix harmless here.
- A read at ALL (the ingest service's `GetTicks`) fails as soon as one replica is slow or down, and takes
  as long as the slowest replica; that is the trade the service is built around, not a defect.
- Prices are rounded to four places on the way in because MySQL stores DECIMAL(12,4); Cassandra holds a
  double, so the rounding is what keeps the two backends identical.
- The API has no authentication and the connections are plaintext.
- The Hive schema script is Apache Hive's own (see its header); the metastore is the 2.3.10 server
  that Spark bundles, which is old, and DataNucleus cannot create its schema on MySQL 8.

## TODO

- Final measurement pass and the results section above.
- Stage 2 measurements: at least five repetitions each of the job runtimes, bucketed against plain
  GROUP BY and join on an inflated table, cached against uncached, the model metrics and depth sweep
  (with the plot from `bench/plot_depth_sweep.py`), and the text-to-SQL accuracy and latency; results go
  in `bench/results/` and in the section above.
- Stage 3 measurements: at least five repetitions each of request latency at ONE, QUORUM and ALL (writes,
  a single row, a 30-day range), loader rows per second by concurrency and batch size for Cassandra and for
  MySQL, REST latency over both backends, and the availability table for 0, 1 and 2 nodes down; results go
  in `bench/results/` and in the section above.

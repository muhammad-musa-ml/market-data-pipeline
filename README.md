# market-data-pipeline

A market-data platform that runs on one machine with Docker Compose. Synthetic daily bars go from
MySQL through a three-broker Kafka cluster into exactly-once Parquet files on HDFS, and a gRPC
service moves historical tables from MySQL to HDFS in bulk and answers per-symbol aggregates while
DataNodes die. The data comes from a seeded generator; none of it is real market data.

This repository is at stage 1 of 3. Stage 2 (Spark and Hive analytics on the HDFS data) and stage 3
(a Cassandra serving tier with a REST API) are not here yet.

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

## Running stage 1

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

### Tests

```
pip install -r requirements-dev.txt
python scripts/gen_proto.py
pytest
```

The unit tests need no containers. `python scripts/integration_test.py` brings up a fresh stack,
runs the checker and the backfill RPCs against it, tampers with the output to confirm the checker
notices, and tears the stack down (`--keep` leaves it up, `--faults` adds the drills below).

### Fault drills

```
python scripts/faults.py datanode     # kill a DataNode while the aggregate RPC is being called
python scripts/faults.py broker       # kill the broker leading partition 0 during production
python scripts/faults.py sink-crash   # crash the sink between Parquet write and checkpoint, restart, repeat
```

Each ends by running the checker where it applies and exits non-zero if anything is off.

### Tear down

```
docker compose --profile live --profile tools --profile debug down -v
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
- The sink writes one small Parquet file per poll batch and nothing compacts them.
- The backfill export builds the whole result in memory before writing it; it is meant for tables
  of a few hundred thousand rows, not billions.
- The producer assumes a single MySQL writer (the generator). Concurrent writers could commit ids
  out of order and a row could be skipped.
- Containers use plaintext connections and development passwords. Nothing here is hardened.

## TODO

- Final measurement pass and the results section above.
- Stage 2: Spark and Hive analytics over `/market/bars`.
- Stage 3: Cassandra serving tier and REST API.
- Compact the sink's small files.

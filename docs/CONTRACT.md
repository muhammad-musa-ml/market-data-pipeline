# What later stages can rely on

This describes stages 1 to 3 as built: the MySQL schema, the Kafka topic, the files on HDFS, the
Hive tables and the serving extract, the Cassandra keyspaces, the REST API and the tick ingest
service, the service names and ports, and the environment variables. If the code and this file
disagree, the code is right and this file has a bug.

The data is synthetic. A seeded generator produces it; nothing here is real market data.

## Compose layout

`compose.yaml` sets the project name (`market-data-pipeline`) and includes one file per area, all
with `project_directory: .` so relative paths in them are relative to the repo root.

| file | services |
| --- | --- |
| `deploy/compose/base.yaml` | `mysql`, `namenode`, `datanode1..3`, `loader`; profile `live`: `generator-live`; profile `tools`: `tools` |
| `deploy/compose/streaming.yaml` | `kafka-1..3`, `kafka-init`, `producer`, `sink`; profile `debug`: `debug-consumer` |
| `deploy/compose/backfill.yaml` | `backfill` |
| `deploy/compose/analytics.yaml` | all in profile `analytics`: `metastore-init` (one shot), `metastore`, `spark-master`, `spark-worker-1`, `spark-worker-2`, `driver` |
| `deploy/compose/serving.yaml` | profile `serving`: `cassandra-1..3`, `serving-init` (one shot), `api-mysql`, `api-cassandra`, `ingest`; profile `serving-quotes`: `quotes`; profile `serving-tools`: `serving-tools` |

Stage 3 added `deploy/compose/serving.yaml` as the last `include:` entry in `compose.yaml`. Its Python
services use the image `mdp-serving:local` (`deploy/images/serving/Dockerfile`: the app image plus
`requirements-serving.txt`: the Cassandra driver, Flask, gunicorn); the stage 1 image is unchanged.

- All services share the default network `market-data-pipeline_default` and reach each other by
  service name.
- Python services use the image `mdp-app:local`, built from `deploy/images/app/Dockerfile` (python
  3.13-slim, requirements pinned in `requirements.txt`, protobuf bindings compiled at build time).
  Each file that defines an app service carries the same `x-app` block (image, build, `env_file:
  deploy/env/app.env`, `dns_opt`); copy it. The `dns_opt: ["timeout:1", "attempts:1"]` matters: without
  it a name lookup for a stopped container takes about eight seconds under Docker Desktop, which
  slows every failure path that touches a dead DataNode.
- `docker compose run --rm tools <command>` runs any `python -m mdp...` module on the network.
  Anything that reads HDFS file contents has to run in a container: the NameNode redirects reads
  and writes to DataNodes by container host name, which the host cannot resolve.
- Every service sets `mem_limit`. Limits of the default profile add up to 5440 MB (13 services;
  `loader` and `kafka-init` exit after a few seconds). The optional profiles add up to 896 MB
  (`generator-live` 256, `tools` 512, `debug-consumer` 128). The `analytics` profile adds 6528 MB
  (`metastore` 512, `spark-master` 384, two workers at 1792 each, `driver` 2048) plus 64 MB for
  `metastore-init`, which exits after a second. `docker compose up -d` does not start it; name the
  services to run Spark next to just MySQL and HDFS, without Kafka:
  `docker compose --profile analytics up -d metastore spark-master spark-worker-1 spark-worker-2 driver`.
- The Spark services use the image `mdp-spark:local` (`deploy/images/spark/Dockerfile`: `apache/spark:4.2.0-python3`,
  Java 21, Python 3.10, the MySQL JDBC driver, `requirements-analytics.txt`, `src/` at `/app/src`).
  Python code in `src/mdp/analytics` therefore has to run on 3.10.
- Jobs run in the `driver` container in client mode against `spark://spark-master:7077`:
  `docker compose exec driver spark-submit /app/src/mdp/analytics/jobs.py <job>`. The last stdout line
  is `RESULT <json>`. `spark-defaults.conf` in the image points Spark at the metastore and warehouse.
- The Hive metastore is the Hive 2.3.10 `HiveMetaStore` class from Spark's own jars (so client and
  server versions match), listening on `metastore:9083`, backed by the MySQL database `metastore`.
  `metastore-init` creates that database and loads `deploy/images/spark/sql/hive-schema-2.3.0.mysql.sql`
  into it once; DataNucleus cannot create the schema itself on MySQL 8 with Connector/J 8.
- `driver` mounts the Google application default credentials file read-only at `/run/gcp/adc.json`
  from the host path in `GCP_ADC_FILE` (default: an empty placeholder, which the Vertex backend reports
  as missing credentials). It also mounts `./tests` at `/app/tests` so the Spark tests can run there.
- The `serving` profile adds 4608 MB (`cassandra-1..3` 1280 each, `api-mysql` 192, `api-cassandra` 256,
  `ingest` 192, `serving-init` 128 and exits); `serving-tools` is 512 and `quotes` 256. Start only
  what is needed: `docker compose --profile serving up -d api-mysql api-cassandra ingest` brings up
  MySQL, the three Cassandra nodes, the init job and those three services, and nothing else. The
  loader and the drills also need HDFS (`namenode`, `datanode1..3`); Kafka only for `quotes`.
- Cassandra runs as `cassandra:5.0.9` with `MAX_HEAP_SIZE=512M`, `HEAP_NEWSIZE=128M`, 16 tokens per
  node, `GossipingPropertyFileSnitch` (datacenter `dc1`), nodes joining one after another. The JVM
  flags in `JVM_EXTRA_OPTS` cap direct memory, metaspace, the code cache and the GC thread counts,
  which would otherwise follow the host's 24 cores. A node uses about 970 MB at rest. Each node's
  health check passes when `nodetool status` shows at least as many `UN` lines as nodes started so
  far, so `cassandra-3` is healthy only when all three are `UN`.

## Ports

Stage 1 publishes three host ports, all on 127.0.0.1.

| host | container | what |
| --- | --- | --- |
| 8300 | `mysql:3306` | MySQL |
| 8310 | `namenode:9870` | NameNode web UI and WebHDFS (file contents are not reachable from the host) |
| 8320 | `backfill:50051` | gRPC backfill service |

Stage 1 stays inside 8300-8359 and uses only the three above. Kafka has no published port.

Stage 2 owns 8360-8399 and publishes four, all on 127.0.0.1:

| host | container | what |
| --- | --- | --- |
| 8360 | `spark-master:8080` | Spark master UI |
| 8361 | `spark-worker-1:8081` | worker UI |
| 8362 | `spark-worker-2:8081` | worker UI |
| 8365 | `driver:4040` | UI of the running application (only while a job runs) |

Stage 3 owns 8400-8449 and publishes three, all on 127.0.0.1:

| host | container | what |
| --- | --- | --- |
| 8400 | `api-mysql:8000` | REST API over MySQL |
| 8401 | `api-cassandra:8000` | REST API over Cassandra |
| 8410 | `ingest:50061` | gRPC tick ingest |

The metastore (`metastore:9083`) and the Spark master (`spark-master:7077`) are inside the network only.
Inside the network: Kafka brokers `kafka-N:9092`, NameNode RPC `namenode:9000`, WebHDFS
`namenode:9870`, DataNode HTTP `datanodeN:9864`, DataNode transfer `datanodeN:9866`, Cassandra
`cassandra-N:9042` (the nodes publish nothing to the host; use `docker compose exec cassandra-1 cqlsh`).

## MySQL

Host `mysql`, database `market`, user `mdp`, password `mdp` (root password `mdp-root`, overridable
with `MDP_MYSQL_ROOT_PASSWORD`). MySQL 8.4. Stage 2 adds a second database, `metastore` (latin1, all
privileges for `mdp`), which holds the Hive metastore tables and nothing else. Stage 3 adds `api_prod` and
`api_test` (the REST API's MySQL backend, see "Serving tier"). The schema of `market` is `sql/schema.sql`; it is applied by the
generator, not by the MySQL image, so tables exist only after the `loader` service has run.

```
symbols(symbol_id SMALLINT UNSIGNED PK, ticker VARCHAR(8) UNIQUE, name, sector, exchange)
bars(id BIGINT UNSIGNED AUTO_INCREMENT PK, symbol_id FK, trade_date DATE,
     open/high/low/close DECIMAL(12,4), volume BIGINT UNSIGNED,
     inserted_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
     UNIQUE (symbol_id, trade_date))
```

- 50 symbols, 8 sectors (Technology, Financials, Health Care, Energy, Consumer, Industrials,
  Utilities, Materials), 2 exchanges (NASDAQ 30, NYSE 20 with the default seed).
- The history is 2600 trading days (weekdays, no holidays) from 2014-01-06 to 2023-12-22, 130000
  rows, generated with seed 7. `GEN_SEED`, `GEN_SYMBOLS`, `GEN_HISTORY_DAYS` change it.
- **Volatility clusters (changed in stage 2).** Returns were plain geometric Brownian motion with a
  constant volatility per symbol, which leaves nothing to learn about next-day volatility. A
  market-wide and a per-symbol log-volatility now follow AR(1) processes (persistence 0.97, a
  half-life of about 23 days, stationary standard deviation 0.40) and scale that day's market and
  idiosyncratic shocks; volume also grows with the symbol's volatility regime (`exp(0.6 * log vol)`).
  The processes depend only on `(seed, day_index)`, so the series is still independent of chunking and
  of restarts of the live generator. `GEN_VOL_CLUSTERING=0` (or `python -m mdp.gen --no-vol-clustering`)
  gives the earlier constant-volatility series bit for bit; `tests/test_market.py` pins that. Prices,
  ids and row counts are unchanged in shape; the values are different, so anything computed from stage 1
  data before this change does not carry over. With clustering the lag-1 autocorrelation of absolute
  returns is about 0.15 (about 0 without it).
- The live generator (`generator-live`, `GEN_RATE` bars per second, default 100) continues the same
  series one trading day at a time, so ids keep increasing and no (symbol, date) repeats.
- `id` is the stream position. The producer relies on there being one writer: with concurrent
  writers a lower id could commit after a higher one and be skipped.
- Timestamps are UTC (`inserted_at` is read with the session time zone set to +00:00).

## Kafka

Three brokers in KRaft mode (each is broker and controller), `apache/kafka:4.3.1`, bootstrap
`kafka-1:9092,kafka-2:9092,kafka-3:9092`, auto topic creation off.

| topic | partitions | replication | min ISR | notes |
| --- | --- | --- | --- | --- |
| `market.bars.v1` | 4 | 3 | 2 | key = ticker (UTF-8), value = `mdp.v1.Bar`, 7 days retention |
| `market.producer-progress.v1` | 1 | 3 | 2 | compacted; value = `mdp.v1.ProducerProgress` |

Topic names come from `BARS_TOPIC` and `PROGRESS_TOPIC`. A ticker always maps to the same partition
(librdkafka's default murmur2 partitioner). Messages are written in transactions
(`transactional.id = mdp-bars-producer`, idempotence on, `acks=all`, 10 retries), so consumers must
use `isolation.level=read_committed`, and offsets are not contiguous (commit markers use offsets).

`mdp.v1.Bar` (`proto/mdp/v1/bar.proto`): `id` (MySQL bars.id), `symbol_id`, `ticker`, `trade_date`
(YYYY-MM-DD), `open`, `high`, `low`, `close` (doubles), `volume`, `inserted_at_us` (microseconds since
the epoch). The bindings are importable as `from mdp.v1 import bar_pb2`; run `python
scripts/gen_proto.py` to build them outside Docker.

## HDFS

Hadoop 3.4.3 (`apache/hadoop:3.4.3`, Java 11), one NameNode, three DataNodes, default replication 3,
permissions off, user `hadoop`. Failure detection is shortened (heartbeat 1 s, recheck interval 5 s,
stale interval 5 s, stale nodes avoided for reads and writes), so a killed DataNode is declared dead
after about 20 s; one kill measured 19.7 s. The defaults would take over ten minutes.

| path | content |
| --- | --- |
| `/market/bars/partition-{p}-batch-{n}.parquet` | one file per sink poll batch, see below |
| `/market/checkpoints/partition-{p}.json` | `{"partition": p, "batch": N, "offset": K}` |
| `/market/dim/symbols.parquet` | the `symbols` table |
| `/market/exports/*.parquet` | outputs of the backfill `ExportBars` RPC |
| `/market/cache/<export name>/<ticker>.parquet` | replication-1 caches of the `SymbolSummary` RPC |
| `/user/hive/warehouse/market.db/{bars,symbols}` | Hive warehouse of stage 2, see "Hive tables" |
| `/user/hive/warehouse/market_lab.db/*` | scratch tables of the bucketing experiments |
| `/market/serving/{daily_bars,monthly_bars,symbol_rollups}/` and `_manifest.json` | the extract for stage 3, see "Serving extract" |

**Which batch files to trust.** A checkpoint says "batches 0 to N-1 are complete and the next Kafka
offset to read is K". A file with number N or higher may be left over from a crash and is rewritten
when the sink restarts. Readers must use only batches below the checkpoint. `python -m
mdp.stream.trusted` prints those paths as `hdfs://namenode:9000/...` URIs (or import
`mdp.stream.checkpoint.trusted_batch_paths`). Globbing `/market/bars` is only safe while the sink is
quiet and has no leftover.

**Bars Parquet schema** (`mdp.stream.codec.BAR_SCHEMA`): `id` int64, `symbol_id` int32, `ticker` string,
`trade_date` date32, `open`/`high`/`low`/`close` double, `volume` int64, `inserted_at` timestamp[us,
UTC]. Each file holds the rows of one partition, so a ticker appears in exactly one partition's
files. Files are small (a poll batch, one per second per busy partition). Stage 2 compacts by reading them
into the `market.bars` Hive table; the batch files themselves are never rewritten or deleted.

**Symbols Parquet schema**: `symbol_id` int32, `ticker`, `name`, `sector`, `exchange` strings.

**Export Parquet schema**: `id` int64, `ticker`, `sector`, `exchange` strings, `trade_date` date32,
`open`/`high`/`low`/`close` double, `volume` int64; sorted by ticker then date, row groups of 8192
rows, replication 2, block size 1 MiB. Cache files hold one ticker's rows of an export in the same
schema, replication 1.

## Hive tables

Database `market` in the metastore, warehouse directory `hdfs://namenode:9000/user/hive/warehouse`,
built by `jobs.py compact` (every run overwrites them). Only the trusted batch files are read
(`mdp.stream.trusted`), rows are de-duplicated on `id`, and the symbols come from
`/market/dim/symbols.parquet`. Spark data source tables in Parquet, with comments on the table and its
columns (the text-to-SQL prompt reads them from the catalog).

| name | kind | content |
| --- | --- | --- |
| `market.bars` | table, **bucketed by `ticker` into 8 buckets, sorted by `trade_date`** | the Parquet bars schema above (`id`, `symbol_id`, `ticker`, `trade_date`, `open`, `high`, `low`, `close`, `volume`, `inserted_at`), one file per non-empty bucket |
| `market.symbols` | table | `symbol_id` int, `ticker`, `name`, `sector`, `exchange` strings |
| `market.daily_returns` | view | `ticker`, `trade_date`, `close`, `ret` (log return from the previous trading day, NULL on a ticker's first day) |

Bucketing uses Spark's murmur3 hash of `ticker`; a query that groups or joins on `ticker` over
two tables bucketed the same way needs no shuffle (`jobs.py bucketing` shows the plans).
`market_lab` holds the copies that experiment makes; nothing should depend on it.

## Serving extract

`jobs.py serving` reads the Hive tables and writes three Parquet datasets under `/market/serving`
(replication 3; 8 part files each plus Spark's empty `_SUCCESS` marker; a ticker's rows all in one file, sorted within the file) and then
`/market/serving/_manifest.json`. Every run overwrites all of it; the manifest is written last, so a
missing manifest, or one whose `source_rows` disagrees with the data, means a run did not finish.
The manifest reads `{"built_at", "source_table", "source_rows", "source_max_id", "trusted_files",
"datasets": {name: {"path", "rows"}}}`.

`/market/serving/daily_bars/` - one row per ticker-day:
`symbol_id` int, `ticker`, `name`, `sector`, `exchange` strings, `trade_date` date, `open` `high` `low`
`close` double, `volume` bigint, `ret` double (log return, null on a ticker's first day), `vol_20`
double (sample standard deviation of the last 20 returns, null until 20 exist).

`/market/serving/monthly_bars/` - one row per ticker-month:
`symbol_id` int, `ticker`, `sector`, `month` string `yyyy-MM`, `open` (first day's open), `high`,
`low`, `close` (last day's close), `avg_close` double, `volume` bigint, `trading_days` bigint,
`month_return` double (`close` over the previous month's `close`, minus 1; null for a ticker's first month).

`/market/serving/symbol_rollups/` - one row per ticker:
`symbol_id` int, `ticker`, `name`, `sector`, `exchange`, `first_date` `last_date` date, `n_bars`
bigint, `avg_close` double, `vwap` double (typical price (H+L+C)/3 weighted by volume, the same
definition as `SymbolSummary`), `min_close` `max_close` `avg_volume` double, `total_return` double (last
close over first close, minus 1), `annual_vol` double (standard deviation of daily log returns times
sqrt(252)), `max_drawdown` double (the most negative close-over-running-peak minus 1).

## gRPC backfill service

`mdp.v1.Backfill` on `backfill:50051` (plaintext), definition in `proto/mdp/v1/backfill.proto`.

- `ExportBars(dimension SECTOR|EXCHANGE, name, dest_path)`: runs the JOIN of `bars` and `symbols`
  filtered by that name, writes one Parquet file, returns the row count, size, replication and block
  size. `dest_path` must be a `.parquet` path under `/market/exports/`. Unknown names and bad paths
  are `INVALID_ARGUMENT`.
- `BlockDistribution(path)`: blocks per DataNode from WebHDFS, plus total blocks, replication,
  length and block size. Missing file is `NOT_FOUND`.
- `SymbolSummary(ticker, source_path)`: row count, average close, VWAP (typical price (H+L+C)/3
  weighted by volume), first and last date, and how the answer was produced: `CACHE_CREATED`,
  `CACHE_REUSED`, `CACHE_RECREATED` (the cache file could not be read, for example because its only
  replica was on a dead DataNode, and was rebuilt from the source file) or `CACHE_UNAVAILABLE`
  (answered from the source, cache could not be written). Unknown ticker is `NOT_FOUND`; HDFS or
  MySQL trouble is `UNAVAILABLE`.

## Serving tier

### Cassandra

Keyspaces `prod` and `test`, `SimpleStrategy`, replication factor 3 (so every node holds every row). The
tables come from `sql/cassandra/001_serving.cql`, which carries a `${keyspace}` placeholder and is applied
once per keyspace by `serving-init`; every statement is `IF NOT EXISTS`, so running it again is harmless.

| table | primary key | what |
| --- | --- | --- |
| `daily_bars` | `((ticker), trade_date)` clustering `trade_date DESC` | `open` `high` `low` `close` double, `volume` bigint; **static** `name` `sector` `exchange` text, the company |
| `ticks` | `((ticker, day), ts_us, producer)` clustering `ts_us DESC, producer ASC` | `price` double, `size` bigint; `day` is the UTC date, `ts_us` microseconds since the epoch in a bigint |
| `latest_quote` | `ticker` | `trade_date`, `close`, `volume`, `bar_id` of the newest bar |

A company exists when its partition has the static columns, with or without bars. "List the companies" is
`SELECT DISTINCT ticker, name, sector, exchange` (partition key and static columns only), filtered by sector or
exchange in the application: there is no lookup table by sector, which would need a second write that cannot be
made atomic with the first.

### MySQL backend

`serving-init` uses the root password (`MDP_MYSQL_ROOT_PASSWORD`, default `mdp-root`) to create the databases
`api_prod` and `api_test` and grant them to `mdp`, then applies `sql/mysql/001_serving.sql` to each:
`companies(ticker PK, name, sector, exchange)` and `daily_bars(ticker, trade_date, open, high, low, close
DECIMAL(12,4), volume BIGINT UNSIGNED, PK (ticker, trade_date), FK to companies)`. They are separate from
`market.symbols` and `market.bars`.

### Loader

`python -m mdp.serving.loader [--backend cassandra|mysql] [--db prod|test] [--concurrency 64]
[--batch-size 50] [--skip-verify] [--json]`, run as
`docker compose --profile serving-tools run --rm serving-tools python -m mdp.serving.loader`.
It reads `/market/serving/_manifest.json` (a missing manifest, or `daily_bars` files whose rows do not add up to
the manifest's count, stop it with "extract incomplete"), then for each `daily_bars` part file upserts the
companies and then the bars. Prices go through the same four-place rounding as API input. The writes are plain
upserts, so a second run changes nothing. Cassandra writes are at QUORUM with `--concurrency` requests in
flight, each an unlogged batch of up to `--batch-size` rows of one ticker (one row per request when it is 1);
MySQL uses 2000-row `executemany` chunks. Unless `--skip-verify` is given it then reads every ticker back
through the store and compares the row count, first and last date and mean close with `symbol_rollups` and
every month's trading days and average close with `monthly_bars` (within the 4-place rounding); exit status 1 if
anything differs. The result line has `rows`, `write_seconds` (time spent writing, not reading HDFS),
`wall_seconds` and `rows_per_s`.

### REST API

Flask under gunicorn (2 workers, 4 threads) on `api-mysql` and `api-cassandra`, the same code over either
backend, `API_BACKEND=mysql|cassandra`. `<db>` is `prod` or `test` (MySQL `api_<db>`, keyspace `<db>`); any
other name is 404. Bodies and answers are JSON; errors are `{"error": "..."}`.

| request | answer |
| --- | --- |
| `GET /<db>/api/companies[?sector=&exchange=]` | `[{"ticker","name","sector","exchange"}]` sorted by ticker |
| `GET /<db>/api/companies/<ticker>` | the company, or 404 |
| `POST /<db>/api/companies/<ticker>` body `{"name","sector","exchange"}` | 201 with the company and a `Location` header; 409 if it exists |
| `PUT /<db>/api/companies/<ticker>/records/<YYYY-MM-DD>` body `{"open","high","low","close","volume"}` | 200 with the record; creates or overwrites; 404 if the company does not exist |
| `GET /<db>/api/companies/<ticker>/records[?start=&end=&limit=]` | `[{"trade_date","open","high","low","close","volume"}]` oldest first, bounds inclusive; `[]` for a company with no records; 404 for an unknown one |
| `GET /<db>/api/companies/<ticker>/records/monthly[?start=&end=]` | `[{"month":"YYYY-MM","trading_days","avg_high","avg_low","avg_close","total_volume"}]` oldest first |
| `GET /healthz` | `{"ok": true, "backend": ...}`; does not touch the database |

Validation (400): tickers are 1 to 8 characters of `A-Z`, digits and `.`, starting with a letter; dates are
`YYYY-MM-DD` between 1900 and 2999; prices are JSON numbers above 0 and up to 99999999, `low <= open, close <=
high`; volume is a whole number from 0 up to 2^63-1; no unknown fields; `limit` is 1 to 100000. Prices are rounded
half-even to four places before they are stored (what DECIMAL(12,4) holds), so both backends answer with the
same number. **Monthly averages are computed in the application from the daily rows, for both backends**:
Cassandra cannot GROUP BY a clustering column, and a stored month would go stale as soon as a record is added
through the API. Averages are rounded to four places. 503 `{"error": "backend unavailable"}` means the backend
could not serve the request: Cassandra `Unavailable`, a timeout or no reachable node, or MySQL not answering.

Consistency: every Cassandra statement of the API runs at QUORUM (two of three replicas), and creating a company
is a lightweight transaction (`IF NOT EXISTS`, serial consistency SERIAL, commit at QUORUM). A read after a
write therefore meets at least one replica that has the write (R + W = 4 > 3), and everything keeps working with
one node down; with two down every operation is 503.

### Tick ingest

`mdp.v1.Ingest` on `ingest:50061`, definition in `proto/mdp/v1/ingest.proto`. Metadata `x-keyspace` is `prod`
(default) or `test`. `PutTick(Tick)` and the client-streaming `PutTicks(stream Tick)` write at consistency ONE;
`GetTicks(ticker, day, limit)` reads at ALL, newest first. A tick is keyed by `(ticker, day, ts_us, producer)`, so
sending a batch again overwrites itself. Status codes: `INVALID_ARGUMENT` (bad ticker, time, price, size,
producer, day or limit, or an unknown keyspace; nothing is written), `UNAVAILABLE` (the coordinator reports too
few live replicas; the message says which level and how many, for example "consistency ALL needs 3 replicas, 2
alive"; or no node can be reached), `DEADLINE_EXCEEDED` (replicas did not answer in time). With one live replica
the writes still succeed and the read is refused; with all three up everything works; after nodes return, a
read at ALL sees every tick that was acknowledged while they were away.

### Latest quotes

`python -m mdp.serving.quotes [--db prod] [--from-beginning] [--exit-when-idle S]` (service `quotes`, profile
`serving-quotes`, needs Kafka) consumes `market.bars.v1` in group `mdp-quotes` with `isolation.level=read_committed`,
keeps the newest bar per ticker of each poll, writes `latest_quote` at QUORUM with the trade date as the
write timestamp (so replays and out-of-order bars cannot move a quote backwards) and then commits offsets.
`--check` compares the table with the newest row of `daily_bars` for each ticker.

### Tests and drills

`pytest` (unit tests, `tests/serving`) needs no cluster. `integration/serving` needs the stack:
`python scripts/serving_test.py` brings it up, empties `test`, loads `prod` into both backends, runs the REST
suite once per backend (`test_api.py`, unchanged between them), the parity tests (`test_parity.py`: the same
requests give identical answers), the ingest tests, and then the destructive ones (`test_node_down.py` kills
or stops a Cassandra node and checks QUORUM reads and writes; `test_ingest_availability.py` does the 0, 1, 2
nodes down matrix for the ingest RPCs). `python scripts/serving_faults.py matrix` prints the availability table
for both the ingest RPCs and the REST endpoints.

## Environment variables

Defaults for the containers are in `deploy/env/app.env`; `mdp.config.Settings.from_env` reads them.

| variable | default in compose | used by |
| --- | --- | --- |
| `MYSQL_HOST` `MYSQL_PORT` `MYSQL_USER` `MYSQL_PASSWORD` `MYSQL_DATABASE` | `mysql` `3306` `mdp` `mdp` `market` | everything that reads MySQL |
| `KAFKA_BOOTSTRAP` | the three brokers | producer, sink, debug, admin |
| `BARS_TOPIC` `PROGRESS_TOPIC` `BARS_PARTITIONS` | `market.bars.v1` `market.producer-progress.v1` `4` | same |
| `WEBHDFS_URL` `HDFS_USER` `HDFS_MIN_DATANODES` | `http://namenode:9870` `hadoop` `3` | everything that touches HDFS |
| `BACKFILL_ADDR` | `backfill:50051` | gRPC clients |
| `GEN_SEED` `GEN_SYMBOLS` `GEN_HISTORY_DAYS` `GEN_RATE` | `7` `50` `2600` `100` | generator |
| `PRODUCER_CHUNK` `PRODUCER_POLL_INTERVAL` `PRODUCER_TXN_ID` | `2000` `0.2` `mdp-bars-producer` | producer |
| `SINK_PARTITIONS` `SINK_MAX_BATCH` `SINK_POLL_TIMEOUT` `SINK_CRASH_PROB` | all, `5000`, `1.0`, `0` | sink |
| `BACKFILL_PORT` | `50051` | backfill server |
| `GEN_VOL_CLUSTERING` | `1` | generator; `0` turns volatility clustering off |
| `GCP_ADC_FILE` | unset (compose substitution, host side) | path of the application default credentials file to mount |
| `GOOGLE_CLOUD_PROJECT` `GOOGLE_CLOUD_QUOTA_PROJECT` | unset | Vertex backend; the runs here used `musa-schools-warehouse` |
| `GOOGLE_CLOUD_LOCATION` | `global` | Vertex backend |
| `MDP_SQL_BACKEND` `MDP_SQL_MODEL` | `vertex`, `gemini-3.5-flash` | text-to-SQL; `gemini-2.5-*` is refused (retired 2026-10-20) |
| `OLLAMA_HOST` `OLLAMA_MODEL` | `http://host.docker.internal:11434`, unset | Ollama backend |
| `API_BACKEND` | `mysql` or `cassandra`, set per service | REST API |
| `CASSANDRA_HOSTS` `CASSANDRA_PORT` `CASSANDRA_TIMEOUT` | `cassandra-1,cassandra-2,cassandra-3` `9042` `5` (seconds) | everything that talks to Cassandra |
| `INGEST_PORT` | `50061` | ingest server |
| `MYSQL_ROOT_PASSWORD` | `mdp-root` (from `MDP_MYSQL_ROOT_PASSWORD`) | `serving-init` only |
| `MDP_API_MYSQL` `MDP_API_CASSANDRA` `MDP_INGEST` | `http://127.0.0.1:8400` `http://127.0.0.1:8401` `127.0.0.1:8410` | host-side integration tests |

`SINK_CRASH_PROB` makes the sink kill itself with that probability after writing a batch file and
before writing its checkpoint; only the fault drill sets it.

## Python package

`src/mdp`: `config` (settings), `layout` (HDFS paths), `db` (MySQL connection with retry), `hdfs`
(WebHDFS client with `FileStore` protocol), `gen` (generator), `stream` (`codec`, `checkpoint`, `sink`,
`producer`, `admin`, `debug`, `trusted`), `backfill` (`export`, `blocks`, `summary`, `server`, `client`),
`check.verify` (HDFS against MySQL), `faults` and `bench` (in-container halves of the drills and
benchmarks), `v1` (generated protobuf code, not committed).

Stage 3, `mdp.serving` (Python 3.13, image `mdp-serving:local`): `settings`, `cluster` (one Cassandra session),
`models`, `validate`, `monthly`, `store` (the interface both backends implement), `cassandra_store`,
`mysql_store`, `backends`, `api` and `wsgi`, `init_schema`, `loader`, `ingest_server` and `ingest_client`,
`quotes`; `mdp.bench.serving_latency` is the in-container half of the latency benchmark.

Stage 2, `mdp.analytics` (Python 3.10, runs in the Spark image): `names` (table names, paths), `session`,
`compact`, `question`, `bucketing`, `caching`, `features`, `model`, `serving`, `scale` (stack copies of
the bars to time bigger inputs), `jobs` (the `spark-submit` entry point), and `text2sql` with `prompt`,
`sqlcheck`, `backends`, `ask`, `evalset`, `evaluate`. The Spark tests are in `tests/analytics`; they run
in the `driver` container or anywhere with pyspark and a JDK (`requirements-spark.txt`), and are skipped
(not collected) when pyspark or pandas is missing.

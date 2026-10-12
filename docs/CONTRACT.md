# What later stages can rely on

This describes stage 1 as built: the MySQL schema, the Kafka topic, the files on HDFS, the
service names and ports, and the environment variables. If the code and this file disagree, the
code is right and this file has a bug.

The data is synthetic. A seeded generator produces it; nothing here is real market data.

## Compose layout

`compose.yaml` sets the project name (`market-data-pipeline`) and includes one file per area, all
with `project_directory: .` so relative paths in them are relative to the repo root.

| file | services |
| --- | --- |
| `deploy/compose/base.yaml` | `mysql`, `namenode`, `datanode1..3`, `loader`; profile `live`: `generator-live`; profile `tools`: `tools` |
| `deploy/compose/streaming.yaml` | `kafka-1..3`, `kafka-init`, `producer`, `sink`; profile `debug`: `debug-consumer` |
| `deploy/compose/backfill.yaml` | `backfill` |

Stage 2 adds `deploy/compose/analytics.yaml` and stage 3 `deploy/compose/serving.yaml`, each as one
new `include:` entry in `compose.yaml`.

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
  (`generator-live` 256, `tools` 512, `debug-consumer` 128). Stage 1 is meant to leave room for other
  stacks on a 16 GB Docker VM; stages 2 and 3 should budget their own limits on top.

## Ports

Only three host ports are published, all on 127.0.0.1.

| host | container | what |
| --- | --- | --- |
| 8300 | `mysql:3306` | MySQL |
| 8310 | `namenode:9870` | NameNode web UI and WebHDFS (file contents are not reachable from the host) |
| 8320 | `backfill:50051` | gRPC backfill service |

Stage 1 stays inside 8300-8359 and uses only the three above. Kafka has no published port.
Inside the network: Kafka brokers `kafka-N:9092`, NameNode RPC `namenode:9000`, WebHDFS
`namenode:9870`, DataNode HTTP `datanodeN:9864`, DataNode transfer `datanodeN:9866`.

## MySQL

Host `mysql`, database `market`, user `mdp`, password `mdp` (root password `mdp-root`, overridable
with `MDP_MYSQL_ROOT_PASSWORD`). MySQL 8.4. The schema is `sql/schema.sql`; it is applied by the
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

**Which batch files to trust.** A checkpoint says "batches 0 to N-1 are complete and the next Kafka
offset to read is K". A file with number N or higher may be left over from a crash and is rewritten
when the sink restarts. Readers must use only batches below the checkpoint. `python -m
mdp.stream.trusted` prints those paths as `hdfs://namenode:9000/...` URIs (or import
`mdp.stream.checkpoint.trusted_batch_paths`). Globbing `/market/bars` is only safe while the sink is
quiet and has no leftover.

**Bars Parquet schema** (`mdp.stream.codec.BAR_SCHEMA`): `id` int64, `symbol_id` int32, `ticker` string,
`trade_date` date32, `open`/`high`/`low`/`close` double, `volume` int64, `inserted_at` timestamp[us,
UTC]. Each file holds the rows of one partition, so a ticker appears in exactly one partition's
files. Files are small (a poll batch, one per second per busy partition); there is no compaction.

**Symbols Parquet schema**: `symbol_id` int32, `ticker`, `name`, `sector`, `exchange` strings.

**Export Parquet schema**: `id` int64, `ticker`, `sector`, `exchange` strings, `trade_date` date32,
`open`/`high`/`low`/`close` double, `volume` int64; sorted by ticker then date, row groups of 8192
rows, replication 2, block size 1 MiB. Cache files hold one ticker's rows of an export in the same
schema, replication 1.

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

`SINK_CRASH_PROB` makes the sink kill itself with that probability after writing a batch file and
before writing its checkpoint; only the fault drill sets it.

## Python package

`src/mdp`: `config` (settings), `layout` (HDFS paths), `db` (MySQL connection with retry), `hdfs`
(WebHDFS client with `FileStore` protocol), `gen` (generator), `stream` (`codec`, `checkpoint`, `sink`,
`producer`, `admin`, `debug`, `trusted`), `backfill` (`export`, `blocks`, `summary`, `server`, `client`),
`check.verify` (HDFS against MySQL), `faults` and `bench` (in-container halves of the drills and
benchmarks), `v1` (generated protobuf code, not committed).

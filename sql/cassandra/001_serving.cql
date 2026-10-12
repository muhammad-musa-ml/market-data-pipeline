-- Applied once per keyspace (prod, test) by serving-init; ${keyspace} is substituted.
-- Every statement is IF NOT EXISTS so the file can be run again.

CREATE KEYSPACE IF NOT EXISTS ${keyspace}
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

-- One partition per ticker, newest bar first. The company's own fields are static: stored once
-- per partition, so "does this company exist" and "list the companies" never touch the bars,
-- and a company with no bars yet is still a partition.
CREATE TABLE IF NOT EXISTS ${keyspace}.daily_bars (
  ticker text,
  trade_date date,
  name text static,
  sector text static,
  exchange text static,
  open double,
  high double,
  low double,
  close double,
  volume bigint,
  PRIMARY KEY ((ticker), trade_date)
) WITH CLUSTERING ORDER BY (trade_date DESC);

-- Raw ticks from the gRPC ingest service. One partition per ticker and UTC day keeps a partition
-- bounded; the producer id in the key makes a retried upload overwrite itself instead of doubling.
-- The time is kept as microseconds in a bigint: a timestamp column stops at milliseconds.
CREATE TABLE IF NOT EXISTS ${keyspace}.ticks (
  ticker text,
  day date,
  ts_us bigint,
  producer text,
  price double,
  size bigint,
  PRIMARY KEY ((ticker, day), ts_us, producer)
) WITH CLUSTERING ORDER BY (ts_us DESC, producer ASC);

-- Newest bar per ticker, kept current from Kafka by the quotes consumer.
CREATE TABLE IF NOT EXISTS ${keyspace}.latest_quote (
  ticker text PRIMARY KEY,
  trade_date date,
  close double,
  volume bigint,
  bar_id bigint
);

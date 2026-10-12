"""Table names, bucket count and HDFS locations used by every analytics job."""

DB = "market"
BARS = f"{DB}.bars"
SYMBOLS = f"{DB}.symbols"
RETURNS = f"{DB}.daily_returns"

# scratch copies for the bucketing experiments live apart so the text-to-SQL prompt never sees them
LAB_DB = "market_lab"

N_BUCKETS = 8

SERVING_ROOT = "/market/serving"
SERVING_DAILY = f"{SERVING_ROOT}/daily_bars"
SERVING_MONTHLY = f"{SERVING_ROOT}/monthly_bars"
SERVING_ROLLUPS = f"{SERVING_ROOT}/symbol_rollups"
SERVING_MANIFEST = f"{SERVING_ROOT}/_manifest.json"

HDFS_URI = "hdfs://namenode:9000"

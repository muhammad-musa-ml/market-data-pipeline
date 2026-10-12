"""HDFS paths that other stages depend on (see docs/CONTRACT.md)."""

ROOT = "/market"
BARS_DIR = f"{ROOT}/bars"
CHECKPOINT_DIR = f"{ROOT}/checkpoints"
EXPORT_DIR = f"{ROOT}/exports"
CACHE_DIR = f"{ROOT}/cache"
SYMBOLS_DIM = f"{ROOT}/dim/symbols.parquet"


def batch_path(partition: int, batch: int) -> str:
    return f"{BARS_DIR}/partition-{partition}-batch-{batch}.parquet"


def checkpoint_path(partition: int) -> str:
    return f"{CHECKPOINT_DIR}/partition-{partition}.json"


def cache_path(source_path: str, ticker: str) -> str:
    stem = source_path.rsplit("/", 1)[-1].removesuffix(".parquet")
    return f"{CACHE_DIR}/{stem}/{ticker}.parquet"

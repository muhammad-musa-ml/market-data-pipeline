"""List the sink's batch files that readers may use, one hdfs:// URI per line.

Anything under /market/bars that is not in this list is a leftover from a crash
and must not be read (see docs/CONTRACT.md).
"""
import sys

from mdp.config import Settings
from mdp.hdfs import WebHdfs
from mdp.stream import checkpoint


def trusted_paths(hdfs: WebHdfs, partitions: int) -> list[str]:
    paths: list[str] = []
    for p in range(partitions):
        paths += checkpoint.trusted_batch_paths(checkpoint.load(hdfs, p), p)
    return paths


def main() -> int:
    cfg = Settings.from_env()
    hdfs = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    for path in trusted_paths(hdfs, cfg.partitions):
        print(f"hdfs://namenode:9000{path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

from collections import Counter
from dataclasses import dataclass, field

from mdp.hdfs import HdfsNotFound, WebHdfs


@dataclass
class Distribution:
    total_blocks: int
    replication: int
    length: int
    block_size: int
    per_datanode: dict[str, int] = field(default_factory=dict)


def block_distribution(hdfs: WebHdfs, path: str) -> Distribution:
    st = hdfs.status(path)
    if st is None:
        raise HdfsNotFound(f"{path}: not found", 404, "FileNotFoundException")
    blocks = hdfs.block_locations(path)
    counts = Counter(host for hosts in blocks for host in hosts)
    return Distribution(
        total_blocks=len(blocks),
        replication=st["replication"],
        length=st["length"],
        block_size=st["blockSize"],
        per_datanode=dict(sorted(counts.items())),
    )

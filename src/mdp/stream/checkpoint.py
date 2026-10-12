import json
from dataclasses import dataclass

from mdp import layout
from mdp.hdfs import FileStore


@dataclass(frozen=True)
class Checkpoint:
    batch: int = 0   # number of the next batch file to write
    offset: int = 0  # next Kafka offset to read


def load(store: FileStore, partition: int) -> Checkpoint:
    path = layout.checkpoint_path(partition)
    if not store.exists(path):
        return Checkpoint()
    try:
        doc = json.loads(store.read(path))
        return Checkpoint(batch=int(doc["batch"]), offset=int(doc["offset"]))
    except (ValueError, KeyError, TypeError) as exc:
        # restarting from zero would be safe but hides a damaged checkpoint
        raise ValueError(f"unreadable checkpoint {path}") from exc


def save(store: FileStore, partition: int, cp: Checkpoint) -> None:
    """Write beside the real file and rename over it, so a crash leaves the old one."""
    path = layout.checkpoint_path(partition)
    tmp = path + ".tmp"
    doc = {"partition": partition, "batch": cp.batch, "offset": cp.offset}
    store.write(tmp, json.dumps(doc).encode())
    store.rename(tmp, path, overwrite=True)


def trusted_batch_paths(cp: Checkpoint, partition: int) -> list[str]:
    """Batch files a reader may use: everything below the checkpointed batch number.

    A file at or past cp.batch may be a leftover from a crash and is rewritten
    on restart.
    """
    return [layout.batch_path(partition, n) for n in range(cp.batch)]

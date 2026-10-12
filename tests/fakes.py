import datetime as dt

from mdp.hdfs import HdfsError, HdfsNotFound
from mdp.stream import codec
from mdp.v1 import bar_pb2


class FakeStore:
    """In-memory stand-in for HDFS. Records the write options it was given."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.opts: dict[str, dict] = {}
        self.fail_rename = False

    def write(self, path, data, **kw):
        self.files[path] = bytes(data)
        self.opts[path] = kw

    def read(self, path, offset=0, length=None):
        if path not in self.files:
            raise HdfsNotFound(path)
        data = self.files[path]
        return data[offset:] if length is None else data[offset:offset + length]

    def exists(self, path):
        return path in self.files

    def status(self, path):
        if path not in self.files:
            return None
        return {"length": len(self.files[path]), "replication": self.opts[path].get("replication")}

    def rename(self, src, dst, overwrite=False):
        if self.fail_rename:
            raise HdfsError("rename failed")
        if dst in self.files and not overwrite:
            raise HdfsError("exists")
        self.files[dst] = self.files.pop(src)
        self.opts[dst] = self.opts.pop(src)

    def listdir(self, path):
        prefix = path.rstrip("/") + "/"
        return sorted(p[len(prefix):] for p in self.files if p.startswith(prefix))


def make_bar(i: int) -> bar_pb2.Bar:
    return bar_pb2.Bar(
        id=i, symbol_id=1 + i % 5, ticker=f"T{i % 5}",
        trade_date=(dt.date(2020, 1, 1) + dt.timedelta(days=i // 5)).isoformat(),
        open=10.0 + i, high=11.0 + i, low=9.0 + i, close=10.5 + i, volume=1000 + i,
        inserted_at_us=1_700_000_000_000_000 + i,
    )


def encoded(ids):
    return [(i, codec.encode(make_bar(i))) for i in ids]

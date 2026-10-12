import io
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdp.hdfs import HdfsError, HdfsNotFound, WebHdfs


class FakeCluster:
    """Just enough of the NameNode + DataNode HTTP API for the client's code paths."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.create_params: list[dict] = []
        self.open_params: list[dict] = []
        self.first_open_goes_to_dead_node = False
        self.dead_port = self._closed_port()
        cluster = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _reply(self, code, doc=None, raw=None):
                body = raw if raw is not None else json.dumps(doc).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _parse(self):
                u = urlsplit(self.path)
                q = {k: v[0] for k, v in parse_qs(u.query).items()}
                return u.path, q

            def _missing(self, path):
                self._reply(404, {"RemoteException": {"exception": "FileNotFoundException",
                                                      "message": f"File does not exist: {path}"}})

            def do_PUT(self):
                path, q = self._parse()
                if path.startswith("/dn"):
                    n = int(self.headers.get("Content-Length", 0))
                    cluster.files[path[3:]] = self.rfile.read(n)
                    return self._reply(201, raw=b"")
                p = path.removeprefix("/webhdfs/v1")
                if q["op"] == "CREATE":
                    cluster.create_params.append(q)
                    return self._reply(200, {"Location": f"http://127.0.0.1:{cluster.port}/dn{p}"})
                if q["op"] == "RENAME":
                    if p not in cluster.files:
                        return self._missing(p)
                    dst = q["destination"]
                    if dst in cluster.files and q.get("renameoptions") != "OVERWRITE":
                        return self._reply(200, {"boolean": False})
                    cluster.files[dst] = cluster.files.pop(p)
                    return self._reply(200, {"boolean": True})
                self._reply(400, {})

            def do_GET(self):
                path, q = self._parse()
                if path.startswith("/dn"):
                    data = cluster.files[path[3:]]
                    off = int(q.get("offset", 0))
                    length = int(q["length"]) if "length" in q else None
                    return self._reply(200, raw=data[off:off + length] if length else data[off:])
                p = path.removeprefix("/webhdfs/v1")
                op = q["op"]
                if op == "OPEN":
                    cluster.open_params.append(q)
                    if p not in cluster.files:
                        return self._missing(p)
                    port = cluster.port
                    if cluster.first_open_goes_to_dead_node and "excludedatanodes" not in q:
                        port = cluster.dead_port
                    extra = "&".join(f"{k}={v}" for k, v in q.items() if k in ("offset", "length"))
                    return self._reply(200, {"Location": f"http://127.0.0.1:{port}/dn{p}?{extra}"})
                if op == "GETFILESTATUS":
                    if p not in cluster.files:
                        return self._missing(p)
                    return self._reply(200, {"FileStatus": {"length": len(cluster.files[p]), "type": "FILE"}})
                if op == "LISTSTATUS":
                    prefix = p.rstrip("/") + "/"
                    names = [f[len(prefix):] for f in sorted(cluster.files) if f.startswith(prefix)]
                    return self._reply(200, {"FileStatuses": {"FileStatus": [{"pathSuffix": n} for n in names]}})
                self._reply(400, {})

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @staticmethod
    def _closed_port() -> int:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    def close(self):
        self.server.shutdown()


@pytest.fixture
def cluster():
    c = FakeCluster()
    yield c
    c.close()


@pytest.fixture
def hdfs(cluster):
    return WebHdfs(f"http://127.0.0.1:{cluster.port}", "hadoop", timeout=5)


def test_write_then_read_roundtrip_passes_replication_and_block_size(cluster, hdfs):
    hdfs.write("/a/b.bin", b"hello", replication=2, block_size=1 << 20)
    assert hdfs.read("/a/b.bin") == b"hello"
    q = cluster.create_params[0]
    assert (q["replication"], q["blocksize"], q["overwrite"]) == ("2", "1048576", "true")


def test_range_read(hdfs):
    hdfs.write("/r.bin", b"0123456789")
    assert hdfs.read("/r.bin", offset=3, length=4) == b"3456"


def test_missing_file_is_not_found_and_status_is_none(hdfs):
    assert hdfs.status("/nope") is None
    assert not hdfs.exists("/nope")
    with pytest.raises(HdfsNotFound):
        hdfs.read("/nope")


def test_rename_over_existing_needs_overwrite(hdfs):
    hdfs.write("/x.json", b"old")
    hdfs.write("/x.tmp", b"new")
    with pytest.raises(HdfsError):
        hdfs.rename("/x.tmp", "/x.json")
    hdfs.rename("/x.tmp", "/x.json", overwrite=True)
    assert hdfs.read("/x.json") == b"new"
    assert not hdfs.exists("/x.tmp")


def test_listdir(hdfs):
    hdfs.write("/d/a.parquet", b"1")
    hdfs.write("/d/b.parquet", b"2")
    assert hdfs.listdir("/d") == ["a.parquet", "b.parquet"]
    assert hdfs.listdir("/empty") == []


def test_read_retries_when_the_first_datanode_is_unreachable(cluster, hdfs):
    hdfs.write("/f.bin", b"payload")
    hdfs.read("/f.bin")  # teaches the client the node's address
    cluster.open_params.clear()
    cluster.first_open_goes_to_dead_node = True
    assert hdfs.read("/f.bin") == b"payload"
    assert "excludedatanodes" not in cluster.open_params[0]
    assert cluster.open_params[1]["excludedatanodes"] == "127.0.0.1:9866"


def test_read_gives_up_after_bounded_attempts(cluster, hdfs, monkeypatch):
    hdfs.write("/g.bin", b"x")
    cluster.first_open_goes_to_dead_node = True
    monkeypatch.setattr("mdp.hdfs.time.sleep", lambda s: None)
    # with no cached address the exclusion cannot be built, so every attempt goes to the dead node
    hdfs._ips.clear()
    hdfs._remember = lambda host: None
    with pytest.raises(HdfsError):
        hdfs.read("/g.bin", attempts=3)
    assert len(cluster.open_params) == 3


def test_open_gives_a_seekable_file_parquet_can_read(hdfs):
    table = pa.table({"x": list(range(20000)), "y": [i * 0.5 for i in range(20000)]})
    buf = io.BytesIO()
    pq.write_table(table, buf, row_group_size=4000)
    hdfs.write("/t.parquet", buf.getvalue())
    f = hdfs.open("/t.parquet")
    pf = pq.ParquetFile(f)
    assert pf.metadata.num_row_groups == 5
    assert pf.read_row_group(3).column("x").to_pylist()[0] == 12000
    assert pq.read_table(hdfs.open("/t.parquet"), filters=[("x", ">=", 19990)]).num_rows == 10


def test_open_missing_file(hdfs):
    with pytest.raises(HdfsNotFound):
        hdfs.open("/missing.parquet")

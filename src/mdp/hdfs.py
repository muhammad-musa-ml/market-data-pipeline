"""A small WebHDFS client.

Everything the pipeline needs from HDFS goes over the NameNode's HTTP API, so
the python containers do not need a JVM or libhdfs. Writes and reads are the
usual two-step: ask the NameNode where to go (noredirect=true), then talk to
the DataNode it names.
"""
import io
import socket
import time
from typing import Protocol
from urllib.parse import urlsplit

import requests

XFER_PORT = 9866  # DataNode data-transfer port, what the NameNode's exclude list expects


class HdfsError(Exception):
    def __init__(self, message: str, status: int | None = None, exception: str = ""):
        super().__init__(message)
        self.status = status
        self.exception = exception


class HdfsNotFound(HdfsError):
    pass


class FileStore(Protocol):
    """The slice of HDFS that the sink and its checkpoints use."""

    def write(self, path: str, data: bytes, **kw) -> None: ...
    def read(self, path: str) -> bytes: ...
    def exists(self, path: str) -> bool: ...
    def rename(self, src: str, dst: str, overwrite: bool = False) -> None: ...
    def listdir(self, path: str) -> list[str]: ...


class WebHdfs:
    def __init__(self, base_url: str, user: str = "hadoop", timeout: float = 30.0):
        self.base = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout
        self._http = requests.Session()
        self._ips: dict[str, str] = {}

    def _url(self, path: str) -> str:
        return f"{self.base}/webhdfs/v1{path}"

    def _nn(self, method: str, path: str, op: str, retries: int = 3, **params) -> requests.Response:
        params = {k: v for k, v in params.items() if v is not None}
        params.update(op=op, **{"user.name": self.user})
        last: Exception | None = None
        for attempt in range(retries):
            try:
                resp = self._http.request(
                    method, self._url(path), params=params,
                    timeout=(5, self.timeout), allow_redirects=False,
                )
                break
            except (requests.ConnectionError, requests.Timeout) as exc:
                last = exc
                time.sleep(0.5 * (attempt + 1))
        else:
            raise HdfsError(f"namenode unreachable for {op} {path}: {last}") from last
        self._check(resp, op, path)
        return resp

    @staticmethod
    def _check(resp: requests.Response, op: str, path: str) -> None:
        if resp.status_code < 400:
            return
        exc, msg = "", resp.text[:200]
        try:
            remote = resp.json()["RemoteException"]
            exc, msg = remote.get("exception", ""), remote.get("message", msg)
        except (ValueError, KeyError):
            pass
        err = HdfsNotFound if resp.status_code == 404 or exc == "FileNotFoundException" else HdfsError
        raise err(f"{op} {path}: {exc or resp.status_code}: {msg}", resp.status_code, exc)

    def _remember(self, host: str) -> None:
        if host not in self._ips:
            try:
                self._ips[host] = socket.gethostbyname(host)
            except OSError:
                pass

    def _exclude_param(self, bad_hosts: list[str]) -> str | None:
        tokens = [f"{self._ips[h]}:{XFER_PORT}" for h in bad_hosts if h in self._ips]
        return ",".join(tokens) or None

    # -- namespace ---------------------------------------------------------

    def mkdirs(self, path: str) -> None:
        self._nn("PUT", path, "MKDIRS")

    def status(self, path: str) -> dict | None:
        try:
            return self._nn("GET", path, "GETFILESTATUS").json()["FileStatus"]
        except HdfsNotFound:
            return None

    def exists(self, path: str) -> bool:
        return self.status(path) is not None

    def listdir(self, path: str) -> list[str]:
        try:
            resp = self._nn("GET", path, "LISTSTATUS")
        except HdfsNotFound:
            return []
        return [e["pathSuffix"] for e in resp.json()["FileStatuses"]["FileStatus"]]

    def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        resp = self._nn(
            "PUT", src, "RENAME", destination=dst,
            renameoptions="OVERWRITE" if overwrite else None,
        )
        if resp.content and resp.json().get("boolean") is False:
            raise HdfsError(f"rename {src} -> {dst} refused")

    def delete(self, path: str, recursive: bool = False) -> bool:
        resp = self._nn("DELETE", path, "DELETE", recursive=str(recursive).lower())
        return bool(resp.json().get("boolean"))

    # -- data --------------------------------------------------------------

    def write(self, path: str, data: bytes, *, replication: int | None = None,
              block_size: int | None = None, overwrite: bool = True) -> None:
        bad: list[str] = []
        for attempt in range(4):
            resp = self._nn(
                "PUT", path, "CREATE", overwrite=str(overwrite).lower(),
                replication=replication, blocksize=block_size,
                excludedatanodes=self._exclude_param(bad),
                noredirect="true",
            )
            location = resp.json()["Location"]
            host = urlsplit(location).hostname or ""
            self._remember(host)
            try:
                put = self._http.put(location, data=data, timeout=(5, self.timeout))
            except (requests.ConnectionError, requests.Timeout):
                bad.append(host)
                time.sleep(0.5)
                continue
            self._check(put, "CREATE", path)
            return
        raise HdfsError(f"CREATE {path}: no DataNode accepted the write")

    def read(self, path: str, offset: int = 0, length: int | None = None, attempts: int = 5) -> bytes:
        bad: list[str] = []
        last = ""
        for attempt in range(attempts):
            resp = self._nn(
                "GET", path, "OPEN", offset=offset or None, length=length,
                excludedatanodes=self._exclude_param(bad), noredirect="true",
            )
            location = resp.json()["Location"]
            host = urlsplit(location).hostname or ""
            self._remember(host)
            try:
                got = self._http.get(location, timeout=(3, self.timeout))
            except (requests.ConnectionError, requests.Timeout) as exc:
                bad.append(host)
                last = f"{host}: {type(exc).__name__}"
                time.sleep(1.0)
                continue
            try:
                self._check(got, "OPEN", path)
            except HdfsNotFound:
                raise
            except HdfsError as exc:
                bad.append(host)
                last = str(exc)
                time.sleep(1.0)
                continue
            return got.content
        raise HdfsError(f"OPEN {path} failed after retries: {last}")

    def open(self, path: str, buffer_size: int = 256 * 1024) -> io.BufferedReader:
        st = self.status(path)
        if st is None:
            raise HdfsNotFound(f"OPEN {path}: not found", 404, "FileNotFoundException")
        return io.BufferedReader(_RemoteFile(self, path, st["length"]), buffer_size)

    # -- cluster info ------------------------------------------------------

    def block_locations(self, path: str) -> list[list[str]]:
        """For each block of the file, the host names of the DataNodes holding it."""
        resp = self._nn("GET", path, "GETFILEBLOCKLOCATIONS")
        blocks = resp.json()["BlockLocations"]["BlockLocation"]
        return [list(b["hosts"]) for b in blocks]

    def live_datanodes(self) -> int:
        resp = self._http.get(
            f"{self.base}/jmx",
            params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"},
            timeout=(5, self.timeout),
        )
        return int(resp.json()["beans"][0]["NumLiveDataNodes"])

    def wait_ready(self, min_datanodes: int, timeout: float = 180.0) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                if self.live_datanodes() >= min_datanodes:
                    return
            except (requests.RequestException, ValueError, KeyError, IndexError):
                pass
            if time.monotonic() > deadline:
                raise HdfsError(f"hdfs not ready: fewer than {min_datanodes} live datanodes")
            time.sleep(2)


class _RemoteFile(io.RawIOBase):
    def __init__(self, client: WebHdfs, path: str, size: int):
        self._client, self._path, self._size, self._pos = client, path, size, 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._size}[whence]
        self._pos = max(0, base + offset)
        return self._pos

    def readinto(self, buf) -> int:
        want = min(len(buf), self._size - self._pos)
        if want <= 0:
            return 0
        data = self._client.read(self._path, self._pos, want)
        buf[: len(data)] = data
        self._pos += len(data)
        return len(data)

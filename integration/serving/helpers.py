import os

import pytest
import requests

COMPANY = {"name": "Test Corp", "sector": "Testing", "exchange": "TEST"}


class Api:
    """A thin client for one backend's `test` database."""

    def __init__(self, name: str, url: str, db: str = "test"):
        self.name, self.url, self.db = name, url, db
        self.http = requests.Session()

    def _u(self, path: str) -> str:
        return f"{self.url}/{self.db}/api/{path}"

    def get(self, path, **params):
        return self.http.get(self._u(path), params=params, timeout=30)

    def post(self, path, body):
        return self.http.post(self._u(path), json=body, timeout=30)

    def put(self, path, body):
        return self.http.put(self._u(path), json=body, timeout=30)

    def on(self, db: str) -> "Api":
        return Api(self.name, self.url, db)


def bar(close: float, **over) -> dict:
    return {"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 1000, **over}


URLS = {
    "mysql": os.environ.get("MDP_API_MYSQL", "http://127.0.0.1:8400"),
    "cassandra": os.environ.get("MDP_API_CASSANDRA", "http://127.0.0.1:8401"),
}


INGEST = os.environ.get("MDP_INGEST", "127.0.0.1:8410")


def reachable(url: str) -> bool:
    try:
        return requests.get(f"{url}/healthz", timeout=3).ok
    except requests.RequestException:
        return False


def require(name: str) -> None:
    if not reachable(URLS[name]):
        if os.environ.get("MDP_REQUIRE_BACKENDS") == "1":
            pytest.fail(f"{name} API at {URLS[name]} is not answering")
        pytest.skip(f"{name} API at {URLS[name]} is not answering")

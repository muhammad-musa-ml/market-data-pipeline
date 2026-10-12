"""Fixtures for the serving integration tests. They need the serving stack up:

    docker compose --profile serving up -d api-mysql api-cassandra ingest

Each backend is reached at the host port the compose file publishes; MDP_API_MYSQL and
MDP_API_CASSANDRA override the URLs. A backend that does not answer /healthz is skipped, unless
MDP_REQUIRE_BACKENDS=1, in which case it is an error (scripts/serving_test.py sets that).
"""
import pathlib
import subprocess
import sys
import uuid

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from helpers import COMPANY, URLS, Api, require  # noqa: E402

if not (ROOT / "src" / "mdp" / "v1" / "ingest_pb2.py").exists():
    subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_proto.py")], check=True)


@pytest.fixture(params=list(URLS), ids=list(URLS))
def api(request) -> Api:
    require(request.param)
    return Api(request.param, URLS[request.param])


@pytest.fixture
def both() -> tuple[Api, Api]:
    for name in URLS:
        require(name)
    return Api("mysql", URLS["mysql"]), Api("cassandra", URLS["cassandra"])


@pytest.fixture
def ticker() -> str:
    """A ticker no other test has used (the test database is not emptied between tests)."""
    return "Z" + uuid.uuid4().hex[:7].upper()


@pytest.fixture
def company(api, ticker) -> str:
    assert api.post(f"companies/{ticker}", COMPANY).status_code == 201
    return ticker

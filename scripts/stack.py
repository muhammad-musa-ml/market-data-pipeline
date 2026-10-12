"""Helpers for the host-side scripts. Standard library only."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "market-data-pipeline"


def compose(*args: str, check: bool = True, timeout: float | None = None,
            env: dict | None = None) -> subprocess.CompletedProcess:
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, *args], cwd=ROOT, check=check, timeout=timeout,
        capture_output=True, text=True, env=full_env,
    )


def tools(*cmd: str, timeout: float | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return compose("run", "--rm", "-T", "tools", *cmd, timeout=timeout, check=check)


def container_id(service: str) -> str:
    out = compose("ps", "-q", "-a", service).stdout.strip()
    if not out:
        raise RuntimeError(f"no container for service {service}")
    return out.splitlines()[0]


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def kill(service: str) -> float:
    """docker kill the service's container. Returns the wall-clock time the kill returned."""
    docker("kill", container_id(service))
    return time.time()


def start(service: str) -> None:
    docker("start", container_id(service))


def sql(query: str) -> str:
    out = compose("exec", "-T", "mysql", "mysql", "-umdp", "-pmdp", "market", "-N", "-B", "-e", query)
    return out.stdout.strip()


def wait_for(predicate, timeout: float, interval: float = 2.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(interval)


def live_datanodes() -> int:
    out = compose("exec", "-T", "namenode", "curl", "-s",
                  "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState").stdout
    return int(json.loads(out)["beans"][0]["NumLiveDataNodes"])


def verify(timeout: float = 180.0, through_id: int | None = None) -> dict:
    cmd = ["python", "-m", "mdp.check.verify", "--json", "--timeout", str(timeout)]
    if through_id is not None:
        cmd += ["--through-id", str(through_id)]
    res = tools(*cmd, timeout=timeout + 120, check=False)
    lines = [ln for ln in res.stdout.splitlines() if ln.startswith("{")]
    if not lines:
        raise RuntimeError(f"checker printed no result:\n{res.stdout}\n{res.stderr}")
    return json.loads(lines[-1])


def say(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg: str) -> None:
    say("FAIL: " + msg)
    sys.exit(1)

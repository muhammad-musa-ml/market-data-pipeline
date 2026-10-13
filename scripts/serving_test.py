"""Bring up the serving tier and run the integration tests against it.

    python scripts/serving_test.py                 # up, load, REST suite on both backends, node-down tests
    python scripts/serving_test.py --skip-faults   # leave out the tests that stop Cassandra nodes
    python scripts/serving_test.py --down          # afterwards remove the serving containers and volumes

Needs stage 2's extract under /market/serving on HDFS (docker compose exec driver spark-submit
/app/src/mdp/analytics/jobs.py serving). Starts MySQL and HDFS but not Kafka or Spark. Exit status is
0 only if every step passed.
"""
import argparse
import os
import subprocess
import sys

import stack
from stack import compose, die, say

STAGE1_NEEDED = ["mysql", "namenode", "datanode1", "datanode2", "datanode3"]
SERVING = ["api-mysql", "api-cassandra", "ingest"]
PROFILES = ["--profile", "serving", "--profile", "serving-tools", "--profile", "serving-quotes"]


def run(*cmd: str, env: dict | None = None) -> int:
    say("$ " + " ".join(cmd))
    return subprocess.run(cmd, cwd=stack.ROOT, env={**os.environ, **(env or {})}).returncode


def tools(*cmd: str) -> int:
    return run("docker", "compose", "-p", stack.PROJECT, "--profile", "serving-tools", "run", "--rm", "-T",
               "serving-tools", *cmd)


def wait_healthy(service: str, timeout: float = 300) -> None:
    def ok():
        cid = stack.container_id(service)
        state = stack.docker("inspect", "-f", "{{.State.Health.Status}}", cid).stdout.strip()
        return state == "healthy"
    stack.wait_for(ok, timeout, what=f"{service} to be healthy")


def bring_up(build: bool) -> None:
    say("starting MySQL and HDFS")
    compose("up", "-d", *STAGE1_NEEDED, timeout=900)
    wait_healthy("namenode")
    say("starting Cassandra, the init job, both APIs and the ingest service")
    try:
        compose(*PROFILES, "up", "-d", *(["--build"] if build else []), *SERVING, timeout=1800)
    except subprocess.CalledProcessError as exc:
        say("compose up failed:\n" + (exc.stderr or "")[-2500:])
        raise
    for svc in ("api-mysql", "api-cassandra"):
        wait_healthy(svc)
    stack.wait_for(lambda: stack.docker("inspect", "-f", "{{.State.Running}}", stack.container_id("ingest")).stdout
                   .strip() == "true", 120, what="ingest")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("--skip-faults", action="store_true", help="do not run the tests that stop nodes")
    ap.add_argument("--skip-load", action="store_true", help="prod already holds the extract")
    ap.add_argument("--down", action="store_true", help="remove the serving containers and volumes afterwards")
    args = ap.parse_args()

    failures: list[str] = []
    env = {"MDP_REQUIRE_BACKENDS": "1"}
    try:
        bring_up(build=not args.no_build)
        if tools("python", "-m", "mdp.serving.init_schema", "--reset-test") != 0:
            die("could not empty the test database")
        if not args.skip_load:
            for backend in ("cassandra", "mysql"):
                if tools("python", "-m", "mdp.serving.loader", "--backend", backend, "--db", "prod") != 0:
                    die(f"loading the extract into {backend} failed")
        pytest = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider"]
        suite = "integration/serving"
        slow = [f"{suite}/test_node_down.py", f"{suite}/test_ingest_availability.py"]
        if run(*pytest, suite, *[x for f in slow for x in ("--deselect", f)], env=env) != 0:
            failures.append("REST and ingest suites")
        if not args.skip_faults:
            for name in slow:
                if run(*pytest, name, env=env) != 0:
                    failures.append(name.rsplit("/", 1)[-1])
    finally:
        if args.down:
            say("removing the serving containers and volumes")
            compose(*PROFILES, "rm", "-s", "-f", "-v", "api-mysql", "api-cassandra", "ingest", "quotes",
                    "serving-init", "serving-tools", "cassandra-1", "cassandra-2", "cassandra-3", check=False)
            for n in (1, 2, 3):
                stack.docker("volume", "rm", f"{stack.PROJECT}_cassandra-{n}-data", check=False)
    say("FAILED: " + ", ".join(failures) if failures else "all passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

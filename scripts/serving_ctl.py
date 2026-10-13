"""Start, stop and kill the Cassandra nodes, and read what the cluster thinks. Used by the serving
tests, the availability matrix and the benchmarks. Standard library only."""
import re
import time

import stack

NODES = ["cassandra-1", "cassandra-2", "cassandra-3"]


STATE_LINE = re.compile(r"^([UD][NLJM])\s+(\S+)", re.M)


def node_states(via: str | None = None) -> dict[str, str]:
    """nodetool status as {node IP: 'UN' | 'DN' | ...}, asked of the first node that answers."""
    for node in ([via] if via else NODES):
        try:
            res = stack.docker("exec", stack.container_id(node), "nodetool", "status", check=False)
        except RuntimeError:
            continue
        if res.returncode == 0:
            return {m.group(2): m.group(1) for m in STATE_LINE.finditer(res.stdout)}
    return {}


def node_ip(node: str) -> str:
    cid = stack.container_id(node)
    fmt = "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
    return stack.docker("inspect", "-f", fmt, cid).stdout.strip()


def is_running(node: str) -> bool:
    cid = stack.container_id(node)
    return stack.docker("inspect", "-f", "{{.State.Running}}", cid).stdout.strip() == "true"


def kill(node: str) -> None:
    """A crash: SIGKILL, so the others find out from the failure detector."""
    stack.docker("kill", stack.container_id(node))


def stop(node: str) -> None:
    """A clean shutdown: the node announces it is leaving."""
    stack.docker("stop", "-t", "30", stack.container_id(node))


def start(node: str) -> None:
    stack.docker("start", stack.container_id(node))


def wait_states(expect_up: int, timeout: float = 240.0) -> dict[str, str]:
    """Wait until some live node reports exactly `expect_up` nodes as UN."""
    def check():
        live = [n for n in NODES if is_running(n)]
        for n in live:
            states = node_states(n)
            if sum(1 for s in states.values() if s == "UN") == expect_up:
                return states
        return None
    return stack.wait_for(check, timeout, interval=3.0, what=f"{expect_up} nodes UN")


def wait_down_seen(node: str, ip: str, timeout: float = 90.0) -> float:
    """Seconds until every surviving node reports the node with this IP (read before it went down) as DN."""
    t0 = time.monotonic()
    survivors = [n for n in NODES if n != node and is_running(n)]
    stack.wait_for(lambda: all(node_states(n).get(ip) == "DN" for n in survivors), timeout, interval=1.0,
                   what=f"{node} to be seen DN")
    return time.monotonic() - t0


def restore_all(timeout: float = 300.0) -> None:
    for n in NODES:
        if not is_running(n):
            start(n)
    wait_states(len(NODES), timeout)

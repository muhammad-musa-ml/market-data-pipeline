"""Fault drills against the running stack.

    python scripts/faults.py datanode        # kill a DataNode while the aggregate RPC is being called
    python scripts/faults.py broker          # kill a Kafka broker while bars are being produced
    python scripts/faults.py sink-crash      # crash the HDFS sink at random points and restart it
    python scripts/faults.py producer-crash  # kill the producer at random moments and restart it
    python scripts/faults.py all

Each drill ends by running the correctness checker (HDFS vs MySQL) where it
applies and exits non-zero if anything is off. Needs the stack up.
"""
import argparse
import json
import random
import subprocess
import sys
import threading
import time

import stack
from stack import compose, die, say, tools

EXPORT_DEST = "/market/exports/drill-nasdaq.parquet"


def datanode(seconds: float = 40.0) -> dict:
    """Kill the DataNode that holds the most cache files while summaries are requested."""
    tools("python", "-m", "mdp.backfill.client", "export", "--exchange", "NASDAQ", "--dest", EXPORT_DEST,
          timeout=300)
    proc = subprocess.Popen(
        ["docker", "compose", "-p", stack.PROJECT, "run", "--rm", "-T", "tools",
         "python", "-m", "mdp.faults.summary_loop", "--source", EXPORT_DEST, "--seconds", str(seconds)],
        cwd=stack.ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    placement: dict[str, list[str]] = {}
    calls: list[dict] = []
    t_kill = 0.0
    victim = ""
    affected: list[str] = []
    for line in proc.stdout:
        ev = json.loads(line)
        if ev["event"] == "placement":
            placement = ev["placement"]
            say("cache files per datanode: " + ", ".join(f"{k}={len(v)}" for k, v in sorted(placement.items())))
        elif ev["event"] == "ready":
            victim = max(placement, key=lambda k: len(placement[k]))
            affected = placement[victim]
            say(f"killing {victim} ({len(affected)} replication-1 cache files live there)")
            t_kill = stack.kill(victim)
            proc.stdin.write("go " + ",".join(affected) + "\n")
            proc.stdin.flush()
        elif ev["event"] == "call" and ev["phase"] == "after_kill":
            calls.append(ev)
    proc.wait(timeout=120)

    errors = [c for c in calls if not c["ok"]]
    ok_affected = [c for c in calls if c["ok"] and c["ticker"] in affected]
    first_ok = min(c["t"] + c["latency_ms"] / 1000 for c in ok_affected) if ok_affected else None
    recreated = {c["ticker"] for c in calls if c["ok"] and c["cache"] == "CACHE_RECREATED"}
    result = {
        "victim": victim,
        "affected_files": len(affected),
        "calls_after_kill": len(calls),
        "errors": len(errors),
        "recreated": len(recreated),
        "recreated_unaffected": sorted(recreated - set(affected)),
        "recovery_seconds": round(first_ok - t_kill, 2) if first_ok else None,
        "slowest_call_ms": round(max(c["latency_ms"] for c in calls), 1) if calls else None,
    }
    stack.start(victim)
    stack.wait_for(lambda: stack.live_datanodes() == 3, 120, what="the DataNode to rejoin")
    say("datanode result: " + json.dumps(result))
    ok = (not errors and set(affected) <= recreated and not result["recreated_unaffected"]
          and result["recovery_seconds"] is not None)
    if not ok:
        die("aggregate RPC did not recover cleanly: " + json.dumps(result))
    return result


def _generator(rate: float) -> None:
    compose("--profile", "live", "up", "-d", "generator-live", env={"GEN_RATE": str(rate)})


def _stop_generator() -> None:
    compose("stop", "generator-live")


def _max_id() -> int:
    return int(stack.sql("SELECT COALESCE(MAX(id), 0) FROM bars"))


def broker(rate: float = 300, kill_at: float = 15, restore_at: float = 40, total: float = 60) -> dict:
    """Kill the broker leading partition 0 while the producer is busy, bring it back later."""
    first = json.loads(tools("python", "-m", "mdp.faults.kafka_info").stdout)
    leader = first["partitions"]["0"]["leader"]
    service = f"kafka-{leader}"
    start_id = _max_id()
    say(f"leader of partition 0 is broker {leader}; producing at {rate:g} bars/s")
    _generator(rate)
    t0 = time.time()
    time.sleep(kill_at)
    stack.kill(service)
    say(f"killed {service}")
    time.sleep(max(0, restore_at - kill_at))
    during = json.loads(tools("python", "-m", "mdp.faults.kafka_info").stdout)
    stack.start(service)
    say(f"restarted {service}")
    time.sleep(max(0, total - (time.time() - t0)))
    _stop_generator()
    end_id = _max_id()
    say(f"generator stopped; {end_id - start_id} bars inserted; waiting for the sink to drain")

    producer_id = stack.container_id("producer")
    restarts = int(stack.docker("inspect", "-f", "{{.RestartCount}}", producer_id).stdout)
    retries = compose("logs", "--no-log-prefix", "producer").stdout.count("failed (")
    report = stack.verify(timeout=240)
    after = json.loads(tools("python", "-m", "mdp.faults.kafka_info").stdout)
    result = {
        "killed_broker": leader,
        "bars_inserted": end_id - start_id,
        "leader_during_outage": during["partitions"]["0"]["leader"],
        "producer_restarts": restarts,
        "producer_chunk_retries": retries,
        "isr_after": {p: v["isr"] for p, v in after["partitions"].items()},
        "check": {k: report[k] for k in ("expected", "found_rows", "duplicate_count", "gap_count",
                                          "mismatch_count", "ok")},
    }
    say("broker result: " + json.dumps(result))
    if not report["ok"]:
        die("rows on HDFS do not match MySQL after the broker outage")
    return result


def producer_crash(rate: float = 300, total: float = 60, kills: int = 4, seed: int = 2) -> dict:
    """Kill the producer at random moments and start it again; rows must neither repeat nor go missing."""
    rng = random.Random(seed)
    start_id = _max_id()
    _generator(rate)
    t_end = time.time() + total
    done = 0
    while time.time() < t_end and done < kills:
        time.sleep(rng.uniform(3, total / kills))
        stack.kill("producer")
        done += 1
        say(f"killed producer ({done}/{kills})")
        time.sleep(rng.uniform(0.5, 3))
        stack.start("producer")
    time.sleep(max(0, t_end - time.time()))
    _stop_generator()
    end_id = _max_id()
    report = stack.verify(timeout=240)
    result = {
        "producer_kills": done,
        "bars_inserted": end_id - start_id,
        "check": {k: report[k] for k in ("expected", "found_rows", "duplicate_count", "gap_count",
                                          "mismatch_count", "ok")},
    }
    say("producer-crash result: " + json.dumps(result))
    if not report["ok"]:
        die("duplicates or gaps after the producer was killed")
    return result


def sink_crash(rate: float = 300, total: float = 60, prob: float = 0.25, seed: int = 1) -> dict:
    """Run the sink under a loop that makes it die between Parquet write and checkpoint."""
    rng = random.Random(seed)
    compose("stop", "sink")
    start_id = _max_id()
    _generator(rate)
    t_end = time.time() + total
    runs = injected = killed = 0

    def killer(delay: float) -> None:
        time.sleep(delay)
        stack.docker("kill", "mdp-sink-soak", check=False)

    while time.time() < t_end:
        runs += 1
        if rng.random() < 0.4:
            threading.Thread(target=killer, args=(rng.uniform(1.5, 6),), daemon=True).start()
        res = subprocess.run(
            ["docker", "compose", "-p", stack.PROJECT, "run", "--rm", "-T", "--no-deps",
             "--name", "mdp-sink-soak", "-e", f"SINK_CRASH_PROB={prob}", "sink"],
            cwd=stack.ROOT, capture_output=True, text=True, timeout=300,
        )
        n = res.stdout.count("injected crash")
        injected += n
        killed += 0 if n else 1
    _stop_generator()
    end_id = _max_id()
    say(f"{runs} sink runs, {injected} injected crashes between write and checkpoint, "
        f"{killed} runs ended by docker kill or otherwise; {end_id - start_id} bars inserted")

    compose("up", "-d", "sink")
    report = stack.verify(timeout=240)
    result = {
        "sink_runs": runs,
        "injected_crashes": injected,
        "other_exits": killed,
        "bars_inserted": end_id - start_id,
        "check": {k: report[k] for k in ("expected", "found_rows", "duplicate_count", "gap_count",
                                          "mismatch_count", "ok")},
    }
    say("sink-crash result: " + json.dumps(result))
    if injected == 0:
        die("no crash was injected, the drill proved nothing")
    if not report["ok"]:
        die("duplicates or gaps after the sink crashes")
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("drill", choices=["datanode", "broker", "sink-crash", "producer-crash", "all"])
    args = ap.parse_args()
    drills = {"datanode": datanode, "broker": broker, "sink-crash": sink_crash,
              "producer-crash": producer_crash}
    for name in (drills if args.drill == "all" else [args.drill]):
        say(f"== {name}")
        drills[name]()
    say("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())

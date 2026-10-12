"""Bring the stage-1 stack up, push the data through it and check the results.

    python scripts/integration_test.py            # up, check, tear down
    python scripts/integration_test.py --keep     # leave the stack running afterwards
    python scripts/integration_test.py --faults   # also run the three fault drills

Exit status is 0 only if every check passed.
"""
import argparse
import json
import sys

import stack
from stack import compose, say, sql, tools

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, ok, detail))
    say(("PASS " if ok else "FAIL ") + name + (f"  ({detail})" if detail else ""))
    return ok


def container_state(service: str) -> str:
    cid = stack.container_id(service)
    return stack.docker("inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", cid).stdout.strip()


def client(*args: str, check_rc: bool = True) -> dict:
    res = tools("python", "-m", "mdp.backfill.client", "--json", *args, timeout=300, check=False)
    if res.returncode != 0:
        if check_rc:
            raise RuntimeError(f"client {args} failed: {res.stderr.strip()}")
        return {"_error": res.stderr.strip()}
    return json.loads(res.stdout.strip().splitlines()[-1])


def bring_up(build: bool) -> None:
    say("starting the stack")
    compose("up", "-d", *(["--build"] if build else []), timeout=1800)
    stack.wait_for(lambda: container_state("loader") == "exited 0", 600, what="the loader")
    stack.wait_for(lambda: container_state("kafka-init") == "exited 0", 300, what="topic creation")
    for svc in ("producer", "sink", "backfill"):
        stack.wait_for(lambda s=svc: container_state(s).startswith("running"), 120, what=svc)


def pipeline_checks() -> None:
    rep = stack.verify(timeout=300)
    rows = int(sql("SELECT COUNT(*) FROM bars"))
    check("sink output matches MySQL (no duplicates, gaps or changed values)", rep["ok"],
          f"{rep['found_rows']} of {rep['expected']} rows, {rep['duplicate_count']} duplicates, "
          f"{rep['gap_count']} gaps, {rep['mismatch_count']} mismatches")
    check("history was loaded", rows >= 100_000, f"{rows} bars")

    topic = json.loads(tools("python", "-m", "mdp.faults.kafka_info").stdout)
    parts = topic["partitions"]
    check("topic has 4 partitions, each replicated on 3 brokers",
          len(parts) == 4 and all(len(p["replicas"]) == 3 for p in parts.values()),
          f"{len(parts)} partitions")
    check("all replicas in sync", all(len(p["isr"]) == 3 for p in parts.values()))


def backfill_checks() -> None:
    dest = "/market/exports/it-nasdaq.parquet"
    expect = int(sql("SELECT COUNT(*) FROM bars b JOIN symbols s ON s.symbol_id = b.symbol_id "
                     "WHERE s.exchange = 'NASDAQ'"))
    ex = client("export", "--exchange", "NASDAQ", "--dest", dest)
    check("export returns the row count of the JOIN", int(ex["rows"]) == expect, f"{ex['rows']} rows")
    check("export file is at replication 2 with 1 MiB blocks",
          ex["replication"] == 2 and int(ex["block_size"]) == 1 << 20)

    energy = client("export", "--sector", "Energy", "--dest", "/market/exports/it-energy.parquet")
    expect_energy = int(sql("SELECT COUNT(*) FROM bars b JOIN symbols s ON s.symbol_id = b.symbol_id "
                            "WHERE s.sector = 'Energy'"))
    check("export by sector name", int(energy["rows"]) == expect_energy, f"{energy['rows']} rows")

    bad = client("export", "--sector", "Nonexistent", "--dest", "/market/exports/it-none.parquet",
                 check_rc=False)
    check("unknown sector is rejected", "INVALID_ARGUMENT" in bad.get("_error", ""))
    bad = client("export", "--sector", "Energy", "--dest", "/tmp/escape.parquet", check_rc=False)
    check("destination outside the export directory is rejected", "INVALID_ARGUMENT" in bad.get("_error", ""))

    bl = client("blocks", dest)
    per_dn = {d["datanode"]: d["blocks"] for d in bl["datanodes"]}
    check("block report matches replication 2",
          bl["replication"] == 2 and bl["total_blocks"] >= 3 and sum(per_dn.values()) == 2 * bl["total_blocks"],
          f"{bl['total_blocks']} blocks over {per_dn}")

    ticker = sql("SELECT ticker FROM symbols WHERE exchange = 'NASDAQ' ORDER BY symbol_id LIMIT 1")
    avg = float(sql(f"SELECT AVG(b.`close`) FROM bars b JOIN symbols s ON s.symbol_id = b.symbol_id "
                    f"WHERE s.ticker = '{ticker}'"))
    first = client("summary", ticker, "--source", dest)
    again = client("summary", ticker, "--source", dest)
    check("first summary builds or reuses the cache", first["cache"] in ("CACHE_CREATED", "CACHE_REUSED"),
          first["cache"])
    check("second summary reuses the cache", again["cache"] == "CACHE_REUSED")
    check("summary average close matches MySQL", abs(again["avg_close"] - avg) < 1e-6,
          f"{again['avg_close']:.6f} vs {avg:.6f}")
    missing = client("summary", "ZZZZ", "--source", dest, check_rc=False)
    check("unknown ticker is NOT_FOUND", "NOT_FOUND" in missing.get("_error", ""))


def checker_bites() -> None:
    """Tamper with the HDFS output and make sure the checker notices, then undo it."""
    compose("stop", "sink")
    stack.verify(timeout=120)  # settle, so the later failure is the tampering and nothing else
    tools("python", "-m", "mdp.faults.tamper", "duplicate")
    try:
        rep = stack.verify(timeout=5)
        check("checker reports duplicated rows", not rep["ok"] and rep["duplicate_count"] > 0,
              f"{rep['duplicate_count']} duplicate ids")
    finally:
        tools("python", "-m", "mdp.faults.tamper", "restore")
    check("checker is clean again after restoring", stack.verify(timeout=60)["ok"])
    compose("up", "-d", "sink")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the stack running")
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("--faults", action="store_true", help="also run the fault drills")
    args = ap.parse_args()

    try:
        bring_up(build=not args.no_build)
        pipeline_checks()
        backfill_checks()
        checker_bites()
        if args.faults:
            import faults
            for name, fn in (("datanode", faults.datanode), ("broker", faults.broker),
                             ("sink-crash", faults.sink_crash)):
                fn()
                check(f"fault drill: {name}", True)
    except SystemExit:
        check("fault drill", False, "a drill reported failure")
    except Exception as exc:  # noqa: BLE001 - report and fall through to cleanup
        check("run completed", False, f"{type(exc).__name__}: {exc}")
    finally:
        if not args.keep:
            say("tearing the stack down")
            compose("down", "-v", timeout=600, check=False)

    failed = [r for r in results if not r[1]]
    say(f"{len(results) - len(failed)} passed, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

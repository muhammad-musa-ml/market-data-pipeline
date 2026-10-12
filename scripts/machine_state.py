"""Append a snapshot of the machine to bench/results/machine_state.txt.

    python scripts/machine_state.py "before the stage 1 runs"

Records the CPU, logical CPUs, OS, total and free memory, five CPU busy samples two seconds apart,
the Docker VM size and how many running containers belong to other projects.
"""
import datetime as dt
import platform
import subprocess
import sys
from pathlib import Path

import stack

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "bench" / "results" / "machine_state.txt"


def ps(command: str, timeout: float = 60) -> str:
    return subprocess.run(["powershell", "-NoProfile", "-Command", command], capture_output=True, text=True,
                          timeout=timeout).stdout.strip()


def main() -> int:
    label = sys.argv[1] if len(sys.argv) > 1 else "snapshot"
    cpu = ps("(Get-CimInstance Win32_Processor).Name")
    logical = ps("(Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors")
    mem = ps("$o = Get-CimInstance Win32_OperatingSystem; "
             "'{0:N1} {1:N1}' -f ($o.TotalVisibleMemorySize/1MB), ($o.FreePhysicalMemory/1MB)").split()
    busy = ps("1..5 | ForEach-Object { [math]::Round((Get-Counter '\\Processor(_Total)\\% Processor Time')"
              ".CounterSamples.CookedValue); Start-Sleep -Seconds 2 }", timeout=120).split()
    info = stack.docker("info", "--format", "{{.NCPU}} {{.MemTotal}} {{.ServerVersion}}").stdout.split()
    names = stack.docker("ps", "--format", "{{.Names}}").stdout.split()
    others = [n for n in names if not n.startswith(stack.PROJECT)]
    lines = [
        f"{label}, UTC {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}",
        f"cpu: {cpu}, {logical} logical CPUs",
        f"os: {platform.platform()}",
        f"total memory GiB: {mem[0]}, free GiB: {mem[1]}",
        f"cpu busy percent, five samples two seconds apart: {', '.join(busy)}",
        f"docker: {info[0]} CPUs, {int(info[1]) / 2**30:.1f} GiB VM memory, engine {info[2]}",
        f"docker containers running (other projects, not mine): {len(others)}",
        f"docker containers running (this project): {len(names) - len(others)}",
        "",
    ]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "a", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())

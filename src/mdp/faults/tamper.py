"""Damage the HDFS output on purpose, to show the checker notices.

duplicate  copies partition 0's first batch file to a new batch number and moves
           the checkpoint past it, so the sink output now contains repeated rows.
restore    undoes that. Run with the sink stopped.
"""
import json
import sys

from mdp import layout
from mdp.config import Settings
from mdp.hdfs import WebHdfs

SAVED = "/market/faults/checkpoint-p0.json"


def main() -> int:
    cmd = sys.argv[1]
    cfg = Settings.from_env()
    h = WebHdfs(cfg.webhdfs_url, cfg.hdfs_user)
    cp_path = layout.checkpoint_path(0)
    if cmd == "duplicate":
        original = h.read(cp_path)
        h.write(SAVED, original)
        doc = json.loads(original)
        extra = doc["batch"]
        h.write(layout.batch_path(0, extra), h.read(layout.batch_path(0, 0)))
        doc["batch"] = extra + 1
        h.write(cp_path, json.dumps(doc).encode())
        print(f"duplicated batch 0 as batch {extra}")
    elif cmd == "restore":
        original = h.read(SAVED)
        extra = json.loads(original)["batch"]
        h.write(cp_path, original)
        h.delete(layout.batch_path(0, extra))
        h.delete(SAVED)
        print(f"restored checkpoint, removed batch {extra}")
    else:
        print("usage: tamper duplicate|restore")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

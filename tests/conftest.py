import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))

if not (ROOT / "src" / "mdp" / "v1" / "bar_pb2.py").exists():
    subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_proto.py")], check=True)

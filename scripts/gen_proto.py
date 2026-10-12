"""Compile proto/ into src/mdp/v1. Run from anywhere; paths are repo-relative."""
import pathlib
import sys

from grpc_tools import protoc

ROOT = pathlib.Path(__file__).resolve().parent.parent


def main() -> int:
    protos = sorted((ROOT / "proto").rglob("*.proto"))
    out = ROOT / "src"
    args = [
        "protoc",
        f"-I{ROOT / 'proto'}",
        f"--python_out={out}",
        f"--grpc_python_out={out}",
        *[str(p) for p in protos],
    ]
    rc = protoc.main(args)
    (out / "mdp" / "v1").mkdir(parents=True, exist_ok=True)
    (out / "mdp" / "v1" / "__init__.py").touch()
    return rc


if __name__ == "__main__":
    sys.exit(main())

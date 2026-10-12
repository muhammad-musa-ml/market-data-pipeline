"""Plot train and test R^2 against tree depth from bench/results/ml_depth_sweep.csv.

    python bench/plot_depth_sweep.py [--scratch]

The lines are medians over the repetitions and the bands are min to max. Needs matplotlib
(requirements-bench.txt); the CSV is the result, the picture only draws it.
"""
import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scratch", action="store_true", help="read bench/scratch instead of bench/results")
    args = ap.parse_args()
    d = ROOT / "bench" / ("scratch" if args.scratch else "results")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = {"train_r2": defaultdict(list), "test_r2": defaultdict(list)}
    with open(d / "ml_depth_sweep.csv", newline="") as f:
        for row in csv.DictReader(f):
            for key in series:
                series[key][int(row["depth"])].append(float(row[key]))

    fig, ax = plt.subplots(figsize=(7, 4))
    for key, label in (("train_r2", "train"), ("test_r2", "test (later dates)")):
        depths = sorted(series[key])
        med = [statistics.median(series[key][k]) for k in depths]
        lo = [min(series[key][k]) for k in depths]
        hi = [max(series[key][k]) for k in depths]
        ax.plot(depths, med, marker="o", ms=3, label=label)
        ax.fill_between(depths, lo, hi, alpha=0.2)
    ax.set_xlabel("max tree depth")
    ax.set_ylabel("R^2")
    ax.set_title("Decision tree, next-5-day volatility")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    out = d / "ml_depth_sweep.png"
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

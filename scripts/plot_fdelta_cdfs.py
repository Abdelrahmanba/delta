#!/usr/bin/env python3
import argparse
import csv
import sys

import matplotlib.pyplot as plt


SAVED_COLS = [
    "saved_forward_prefix_bytes",
    "saved_forward_suffix_bytes",
    "saved_forward_post_mismatch_bytes",
    "saved_backward_bytes",
]

STORED_COLS = [
    "stored_no_boundary_bytes",
    "stored_backtrace_gap_bytes",
    "stored_no_match_first_bytes",
    "stored_no_match_second_bytes",
]

LABELS = {
    "saved_forward_prefix_bytes": "forward prefix",
    "saved_forward_suffix_bytes": "forward suffix",
    "saved_forward_post_mismatch_bytes": "forward post-mismatch",
    "saved_backward_bytes": "backward post-mismatch",
    "stored_no_boundary_bytes": "no boundary",
    "stored_backtrace_gap_bytes": "backtrace gap",
    "stored_no_match_first_bytes": "no match (first)",
    "stored_no_match_second_bytes": "no match (second)",
}


def cdf_points(values):
    values = sorted(values)
    n = len(values)
    if n == 0:
        return [], []
    ys = [(i + 1) / n for i in range(n)]
    return values, ys


def load_percentages(csv_path):
    saved = {k: [] for k in SAVED_COLS}
    stored = {k: [] for k in STORED_COLS}

    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                input_size = float(row["input_size"])
            except (KeyError, ValueError):
                continue
            if input_size <= 0:
                continue

            for col in SAVED_COLS:
                try:
                    pct = (float(row[col]) / input_size) * 100.0
                except (KeyError, ValueError):
                    continue
                saved[col].append(pct)

            for col in STORED_COLS:
                try:
                    pct = (float(row[col]) / input_size) * 100.0
                except (KeyError, ValueError):
                    continue
                stored[col].append(pct)

    return saved, stored


def plot_cdfs(saved, stored, output_path):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)

    ax = axes[0]
    for col in SAVED_COLS:
        xs, ys = cdf_points(saved[col])
        if xs:
            ax.plot(xs, ys, label=LABELS.get(col, col))
    ax.set_title("Saved Bytes (% of input)")
    ax.set_xlabel("Percent of input")
    ax.set_ylabel("CDF")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8)

    ax = axes[1]
    for col in STORED_COLS:
        xs, ys = cdf_points(stored[col])
        if xs:
            ax.plot(xs, ys, label=LABELS.get(col, col))
    ax.set_title("Stored Bytes (% of input)")
    ax.set_xlabel("Percent of input")
    ax.set_ylabel("CDF")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8)

    fig.savefig(output_path, dpi=150)


def main():
    parser = argparse.ArgumentParser(
        description="Plot CDFs for fdelta CSV stats."
    )
    parser.add_argument(
        "--csv", default="fdelta_stats.csv", help="Path to CSV stats file."
    )
    parser.add_argument(
        "--output",
        default="fdelta_cdfs.png",
        help="Output PNG path.",
    )
    args = parser.parse_args()

    try:
        saved, stored = load_percentages(args.csv)
    except FileNotFoundError:
        print(f"missing CSV: {args.csv}", file=sys.stderr)
        return 1

    plot_cdfs(saved, stored, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

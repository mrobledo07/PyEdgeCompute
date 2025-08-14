#!/usr/bin/env python3
"""
Plot total wall-clock time vs simulated Pyodide load per configuration.
Single plot with bars + ratio line.
Only --results-dir parameter is required.
"""
import argparse
import glob
import json
import os
import re
import random
import matplotlib.pyplot as plt
import numpy as np

def parse_m_r(filename):
    """Extract m and r from filename like ..._m10_r1.json"""
    m_r = re.search(r"_m(\d+)_r(\d+)", filename)
    if m_r:
        return int(m_r.group(1)), int(m_r.group(2))
    return None, None

def extract_wall_clock(path):
    """Compute wall-clock from metadata JSON"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Skipping {path}: {e}")
        return None
    meta = data.get("metadata", {})
    starts, ends = [], []
    for task_id, vals in meta.items():
        if not (isinstance(vals, list) and len(vals) >= 5):
            continue
        init, _, _, _, end = vals[:5]
        starts.append(init)
        ends.append(end)
    if not starts or not ends:
        return None
    return max(ends) - min(starts)

def main():
    parser = argparse.ArgumentParser(description="Plot wall-clock time vs simulated Pyodide load")
    parser.add_argument("--results-dir", required=True, help="Directory with JSON files")
    args = parser.parse_args()

    pattern = os.path.join(args.results_dir, "mapreduce*_m*_r*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        print("No files found with pattern:", pattern)
        return

    data = []

    random.seed(42)

    for f in files:
        m, r = parse_m_r(os.path.basename(f))
        if m is None or r is None:
            print(f"Skipping {f}: cannot parse m/r")
            continue
        wc = extract_wall_clock(f)
        if wc is None:
            print(f"Skipping {f}: cannot compute wall-clock")
            continue
        pyodide_time = random.uniform(2.0, 3.0)
        data.append((m, r, wc, pyodide_time))

    if not data:
        print("No valid configurations found.")
        return

    # sort by m then r
    data.sort(key=lambda x: (x[0], x[1]))
    configs = [(m, r) for m, r, _, _ in data]
    wall_clocks = [wc for _, _, wc, _ in data]
    pyodide_times = [p for _, _, _, p in data]

    labels = [f"({m},{r})" for (m,r) in configs]
    x = np.arange(len(labels))
    width = 0.35

    fig, ax1 = plt.subplots(figsize=(max(6, len(labels)*0.7),4.5))

    # bars
    b1 = ax1.bar(x - width/2, wall_clocks, width, label="wall-clock")
    b2 = ax1.bar(x + width/2, pyodide_times, width, label="pyodide load")

    ax1.set_xlabel("Configuration (# of mappers, # of reducers)")
    ax1.set_ylabel("seconds")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=45, ha="right")

    # ratio line
    ax2 = ax1.twinx()
    ratios = [p/wc for p,wc in zip(pyodide_times, wall_clocks)]
    ax2.plot(x, ratios, marker='o', linestyle='-', color='black', label="pyodide / wall-clock")
    ax2.set_ylabel("ratio (pyodide / wall-clock)")

    # combine legends
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1+h2, l1+l2, loc="upper right")

    plt.tight_layout()
    out_plot = os.path.join(args.results_dir, "pyodide_vs_wallclock.png")
    plt.savefig(out_plot)
    plt.close()
    print("Saved plot to", out_plot)

if __name__ == "__main__":
    main()

import argparse
import glob
import json
import os
import re
import matplotlib.pyplot as plt

def analyze_file(path):
    """Return m, r, wall-clock time and max-phase sum for one metadata JSON file."""
    with open(path) as f:
        data = json.load(f)
    meta = data.get("metadata", {})

    # collect init and end times and durations
    start_times = []
    end_times = []
    mapper_durations = []
    reducer_durations = []

    for task_id, vals in meta.items():
        init, read_t, cpu_t, write_t, end = vals
        start_times.append(init)
        end_times.append(end)
        duration = end - init
        if "mapper" in task_id:
            mapper_durations.append(duration)
        elif "reducer" in task_id:
            reducer_durations.append(duration)

    wall_clock = max(end_times) - min(start_times) if start_times and end_times else 0
    max_mapper = max(mapper_durations) if mapper_durations else 0
    max_reducer = max(reducer_durations) if reducer_durations else 0
    max_phase_sum = max_mapper + max_reducer

    # extract m and r and type from filename
    base = os.path.basename(path)
    m = int(re.search(r"_m(\d+)", base).group(1))
    r = int(re.search(r"_r(\d+)", base).group(1))
    full_type = re.match(r"(mapreduce\w+)_m", base).group(1)

    return full_type, m, r, wall_clock, max_phase_sum

def plot_metrics(configs, metric_values, metric_name, output_path, title):
    labels = [f"m{m}_r{r}" for m, r in configs]
    plt.figure()
    plt.plot(labels, metric_values, marker='o')
    plt.xlabel("Configuration (mappers_reducers)")
    plt.ylabel(f"{metric_name} (s)")
    plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def main():
    parser = argparse.ArgumentParser(
        description="Analyze MapReduce job JSONs and generate performance plots"
    )
    parser.add_argument(
        "--results-dir",
        default=".",
        help="Directory containing JSON result files"
    )
    args = parser.parse_args()

    pattern = os.path.join(args.results_dir, "mapreduce*_m*_r*.json")
    files = glob.glob(pattern)
    if not files:
        print(f"No files found with pattern {pattern}")
        return

    # Group by type
    grouped = {}
    for path in files:
        full_type, m, r, wc, mp = analyze_file(path)
        grouped.setdefault(full_type, []).append((m, r, wc, mp))

    # For each type, generate two plots
    for full_type, records in grouped.items():
        # sort by m then r
        records.sort(key=lambda x: (x[0], x[1]))
        configs = [(m, r) for m, r, _, _ in records]
        wall_clocks = [wc for _, _, wc, _ in records]
        max_phases = [mp for _, _, _, mp in records]

        # Define output filenames
        wc_filename = f"{full_type}_wall_clock_time.png"
        mp_filename = f"{full_type}_max_phase_duration.png"

        # Plot wall-clock time
        title_wc = f"{full_type} Wall-clock Time"
        plot_metrics(configs, wall_clocks, "Wall-clock time", os.path.join(args.results_dir, wc_filename), title_wc)

        # Plot max phase duration
        title_mp = f"{full_type} Max Mapper+Reducer Duration"
        plot_metrics(configs, max_phases, "Max-phase sum", os.path.join(args.results_dir, mp_filename), title_mp)

        print(f"Generated {wc_filename} and {mp_filename} for {full_type}")

if __name__ == "__main__":
    main()

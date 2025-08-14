# combined_report.py
"""
Combined MapReduce analysis (v4):
- More robust filename parsing to extract m and r from many patterns
  (examples supported: mapreduce_wordcount_m10_r1.json,
   mapreducewordcount_m10_r1.json, mapreduce-wordcount-m10-r1.json, etc.)
- Prints a short warning when it had to fall back to a heuristic
- Generates plots per-type (wall-clock, max-phase, CPU vs IO, latency, speedup, efficiency)
- Saves per-type CSV and a consolidated summary_all.csv
"""

import argparse
import glob
import json
import os
import re
import csv
import matplotlib.pyplot as plt

def parse_m_r_from_filename(base):
    """
    Try multiple strategies to extract (m, r) from a filename.
    Returns (m, r, method) where method is a string describing which strategy matched.
    """
    # Strategy 1: _m<num>_r<num> or -m<num>-r<num> or .m<num>.r<num>
    m = r = None
    method = None
    m_r = re.search(r"[ _.-]m(\d+)[ _.-]r(\d+)", base, flags=re.IGNORECASE)
    if m_r:
        try:
            m = int(m_r.group(1)); r = int(m_r.group(2)); method = "m_r_pair"
            return m, r, method
        except Exception:
            pass

    # Strategy 2: separate _m<num> and _r<num>
    m_s = re.search(r"[ _.-]m(\d+)", base, flags=re.IGNORECASE)
    r_s = re.search(r"[ _.-]r(\d+)", base, flags=re.IGNORECASE)
    if m_s and r_s:
        try:
            m = int(m_s.group(1)); r = int(r_s.group(1)); method = "separate_m_r"
            return m, r, method
        except Exception:
            pass

    # Strategy 3: find all occurrences like m<num> and r<num> and take the last ones
    m_all = re.findall(r"m(\d+)", base, flags=re.IGNORECASE)
    r_all = re.findall(r"r(\d+)", base, flags=re.IGNORECASE)
    if m_all and r_all:
        try:
            m = int(m_all[-1]); r = int(r_all[-1]); method = "last_m_last_r"
            return m, r, method
        except Exception:
            pass

    # Strategy 4: explicit patterns like ..._m10r1 or ...m10r1 (no separators)
    mr = re.search(r"m(\d+)r(\d+)", base, flags=re.IGNORECASE)
    if mr:
        try:
            m = int(mr.group(1)); r = int(mr.group(2)); method = "m10r1"
            return m, r, method
        except Exception:
            pass

    return None, None, None


def analyze_file(path):
    """Parse one JSON file and return metrics.
    Returns: (full_type, m, r, wall_clock, max_phase_sum, total_cpu, total_io, num_tasks, parse_method)
    """
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    meta = data.get("metadata", {})

    start_times = []
    end_times = []
    mapper_durations = []
    reducer_durations = []
    total_cpu = 0.0
    total_io = 0.0
    num_tasks = 0

    for task_id, vals in meta.items():
        if not (isinstance(vals, list) and len(vals) >= 5):
            continue
        init, read_t, cpu_t, write_t, end = vals[:5]
        try:
            duration = end - init
        except Exception:
            continue
        start_times.append(init)
        end_times.append(end)
        if 'mapper' in task_id.lower():
            mapper_durations.append(duration)
        elif 'reducer' in task_id.lower():
            reducer_durations.append(duration)

        total_cpu += (cpu_t or 0)
        total_io += ((read_t or 0) + (write_t or 0))
        num_tasks += 1

    wall_clock = (max(end_times) - min(start_times)) if start_times and end_times else 0.0
    max_mapper = max(mapper_durations) if mapper_durations else 0.0
    max_reducer = max(reducer_durations) if reducer_durations else 0.0
    max_phase_sum = max_mapper + max_reducer

    base = os.path.basename(path)
    full_type = None
    # type extraction: prefer something starting with mapreduce... before _m
    t_match = re.match(r"(mapreduce[\w-]*)[_.-]m", base, flags=re.IGNORECASE)
    if t_match:
        full_type = t_match.group(1)
    else:
        # try to extract mapreduce word at start
        t_match2 = re.match(r"(mapreduce[\w-]*)", base, flags=re.IGNORECASE)
        full_type = t_match2.group(1) if t_match2 else base

    m, r, method = parse_m_r_from_filename(base)
    return full_type, m, r, wall_clock, max_phase_sum, total_cpu, total_io, num_tasks, method


def plot_line_configs(configs, values, ylabel, outpath):
    if not configs:
        return
    labels = [f"({m},{r})" for (m,r) in configs]
    plt.figure()
    plt.plot(labels, values, marker='o', linewidth=1)
    plt.xlabel("Configuration (# of mappers, # of reducers)")
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def plot_grouped_bar(configs, groups, group_names, ylabel, outpath):
    if not configs:
        return
    import numpy as np
    labels = [f"({m},{r})" for (m,r) in configs]
    n = len(labels)
    k = len(groups)
    x = np.arange(n)
    width = 0.8 / max(1, k)
    plt.figure()
    for i, g in enumerate(groups):
        plt.bar(x + i*width, g, width=width, label=group_names[i])
    plt.xticks(x + width*(k-1)/2, labels, rotation=45, ha='right')
    plt.xlabel("Configuration (# of mappers, # of reducers)")
    plt.ylabel(ylabel)
    if k > 1:
        plt.legend()
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def choose_baseline_index(configs, times):
    for i, (m,r) in enumerate(configs):
        if m==1 and r==1:
            return i
    min_val, min_idx = None, 0
    for i, (m,r) in enumerate(configs):
        s = (m or 0) + (r or 0)
        if min_val is None or s < min_val:
            min_val = s; min_idx = i
    return min_idx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--results-dir', default='.', help='Directory with JSON files')
    args = parser.parse_args()

    pattern = os.path.join(args.results_dir, "mapreduce*_m*_r*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No files found with pattern {pattern}")
        return

    grouped = {}
    all_records = []
    for path in files:
        try:
            full_type, m, r, wc, mp, tc, ti, nt, method = analyze_file(path)
        except Exception as e:
            print(f"Skipping {path}: parse error: {e}")
            continue

        if m is None or r is None:
            print(f"Skipping {path}: cannot parse m/r from filename (tried method {method}).")
            # try relaxed fallback: try parse again using looser regex on basename
            base = os.path.basename(path)
            m_f = re.findall(r"m(\d+)", base, flags=re.IGNORECASE)
            r_f = re.findall(r"r(\d+)", base, flags=re.IGNORECASE)
            if m_f and r_f:
                try:
                    m = int(m_f[-1]); r = int(r_f[-1])
                    print(f"  Fallback parsed m={m}, r={r} from filename using last-occurrence heuristic.")
                except Exception:
                    pass

        if m is None or r is None:
            print(f"  Still could not parse m/r for {path}; skipping.")
            continue

        grouped.setdefault(full_type, []).append((m,r,wc,mp,tc,ti,nt,os.path.basename(path)))
        all_records.append({"type":full_type,"m":m,"r":r,"wall_clock":wc,"max_phase_sum":mp,
                            "total_cpu":tc,"total_io":ti,"num_tasks":nt,"file":os.path.basename(path)})

    for full_type, recs in grouped.items():
        if not recs:
            continue
        recs.sort(key=lambda x:(x[0],x[1]))
        configs = [(m,r) for m,r,_,_,_,_,_,_ in recs]
        wall_clocks = [wc for _,_,wc,_,_,_,_,_ in recs]
        max_phases = [mp for _,_,_,mp,_,_,_,_ in recs]
        total_cpus = [tc for _,_,_,_,tc,_,_,_ in recs]
        total_ios = [ti for _,_,_,_,_,ti,_,_ in recs]
        num_tasks = [nt for _,_,_,_,_,_,nt,_ in recs]
        filenames = [fn for _,_,_,_,_,_,_,fn in recs]

        plot_line_configs(configs, wall_clocks, "wall-clock (s)", os.path.join(args.results_dir,f"{full_type}_wall_clock.png"))
        plot_line_configs(configs, max_phases, "max-phase sum (s)", os.path.join(args.results_dir,f"{full_type}_max_phase.png"))
        plot_grouped_bar(configs, [total_cpus,total_ios], ["total_cpu","total_io"], "seconds", os.path.join(args.results_dir,f"{full_type}_cpu_vs_io.png"))
        plot_line_configs(configs, num_tasks, "num tasks", os.path.join(args.results_dir,f"{full_type}_num_tasks.png"))

        latency_diff = [mp - wc for mp,wc in zip(max_phases, wall_clocks)]
        latency_ratio = [ (mp/wc if wc>0 else 0) for mp,wc in zip(max_phases, wall_clocks)]
        plot_line_configs(configs, latency_diff, "seconds", os.path.join(args.results_dir,f"{full_type}_latency_diff.png"))
        plot_line_configs(configs, latency_ratio, "ratio", os.path.join(args.results_dir,f"{full_type}_latency_ratio.png"))

        baseline_idx = choose_baseline_index(configs, wall_clocks)
        T0 = wall_clocks[baseline_idx] if wall_clocks[baseline_idx]>0 else 1.0
        speedups = [ (T0/t if t>0 else 0) for t in wall_clocks ]
        processors = [ (m or 0) + (r or 0) for m,r in configs ]
        efficiency = [ s/p if p>0 else 0 for s,p in zip(speedups,processors) ]
        plot_line_configs(configs, speedups, "speedup (x)", os.path.join(args.results_dir,f"{full_type}_speedup.png"))
        plot_line_configs(configs, efficiency, "efficiency", os.path.join(args.results_dir,f"{full_type}_efficiency.png"))

        csv_path = os.path.join(args.results_dir,f"{full_type}_summary.csv")
        with open(csv_path,'w',newline='',encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['file','config','m','r','wall_clock','max_phase_sum','total_cpu','total_io','num_tasks',
                             'latency_diff','latency_ratio','speedup','processors','efficiency'])
            for fn,(m,r),wc,mp,tc,ti,nt,ld,lr,sp,p,ef in zip(filenames,configs,wall_clocks,max_phases,total_cpus,total_ios,num_tasks,latency_diff,latency_ratio,speedups,processors,efficiency):
                writer.writerow([fn,f"({m},{r})",m,r,wc,mp,tc,ti,nt,ld,lr,sp,p,ef])
        print(f"Generated plots and CSV for {full_type}")

    csv_out = os.path.join(args.results_dir,"summary_all.csv")
    with open(csv_out,'w',newline='',encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['file','type','m','r','wall_clock','max_phase_sum','total_cpu','total_io','num_tasks'])
        for r in all_records:
            writer.writerow([r['file'],r['type'],r['m'],r['r'],r['wall_clock'],r['max_phase_sum'],r['total_cpu'],r['total_io'],r['num_tasks']])
    print("Saved consolidated CSV to", csv_out)


if __name__ == "__main__":
    main()

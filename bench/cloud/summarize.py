#!/usr/bin/env python3
"""Tables from bench/cloud/run.sh's logs and build.sh's build.tsv, as
Markdown.

Per framework and node count: each query's median over its passes (the
first pass included; Ray starts its worker processes in it), the median of
the per-pass totals, and the slowest pass. The one-process times come from
the check each run does once.

Usage: summarize.py bench/cloud/results/<date>
"""

import collections
import glob
import os
import re
import statistics
import sys

ORDER = ["dista", "rust", "ray"]


def parse(path):
    times = collections.defaultdict(list)
    local = {}
    head = {}
    for line in open(path):
        if line.startswith("# "):
            for kv in line[2:].split():
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    head[k] = v
        elif line.startswith("TIME "):
            _, q, _, ms = line.split()
            times[q].append(int(ms))
        else:
            m = re.match(r"(q\d\w*) : .* one process (\d+) ms", line)
            if m:
                local[m.group(1)] = int(m.group(2))
    passed = any(l.startswith("PASS") for l in open(path))
    return head, times, local, passed


def main(results):
    runs = {}
    for path in glob.glob(os.path.join(results, "*-*.log")):
        name = os.path.basename(path)[:-4]
        if name.startswith("build-"):
            continue
        fw, nodes = name.rsplit("-", 1)
        runs[(fw, int(nodes))] = parse(path)
    if not runs:
        print("no runs in", results)
        return 1

    queries = []
    for _, times, _, _ in runs.values():
        for q in times:
            if q not in queries:
                queries.append(q)
    queries.sort()
    keys = sorted(runs, key=lambda k: (k[1], ORDER.index(k[0]) if k[0] in ORDER else 9))
    slots = {k: runs[k][0].get("slots", "?") for k in keys}

    print("## Distributed, median of passes (ms)\n")
    print("| query | " + " | ".join(f"{fw} ×{n}" for fw, n in keys) + " |")
    print("|---|" + "---:|" * len(keys))
    for q in queries:
        cells = []
        for k in keys:
            t = runs[k][1].get(q)
            cells.append(f"{statistics.median(t):,.0f}" if t else "")
        print(f"| {q} | " + " | ".join(cells) + " |")
    totals, worst = [], []
    for k in keys:
        times = runs[k][1]
        n = min((len(v) for v in times.values()), default=0)
        per_pass = [sum(times[q][p] for q in times) for p in range(n)]
        totals.append(f"**{statistics.median(per_pass):,.0f}**" if per_pass else "")
        worst.append(f"{max(per_pass):,.0f}" if per_pass else "")
    print("| total, median pass | " + " | ".join(totals) + " |")
    print("| total, slowest pass | " + " | ".join(worst) + " |")
    print("| passed the check | " + " | ".join("yes" if runs[k][3] else "**no**" for k in keys) + " |")
    print(f"\nSlots per node: {', '.join(sorted(set(slots.values())))}. "
          f"Passes: {', '.join(sorted({runs[k][0].get('passes', '?') for k in keys}))}.\n")

    print("## One process, on the head node (ms)\n")
    fws = [fw for fw in ORDER if any(k[0] == fw for k in keys)]
    print("| query | " + " | ".join(fws) + " |")
    print("|---|" + "---:|" * len(fws))
    first = {fw: next(k for k in keys if k[0] == fw) for fw in fws}
    for q in queries:
        print(f"| {q} | " + " | ".join(f"{runs[first[fw]][2].get(q, 0):,}" for fw in fws) + " |")
    print("| total | " + " | ".join(f"**{sum(runs[first[fw]][2].values()):,}**" for fw in fws) + " |\n")

    build = os.path.join(results, "build.tsv")
    if os.path.exists(build):
        rows = [l.rstrip("\n").split("\t") for l in open(build)][1:]
        print("## Building the images\n")
        print("| image | whole build (s) | dependencies (s) | compile (s) | on disk (MB) | gzip (MB) | built on |")
        print("|---|---:|---:|---:|---:|---:|---|")
        for r in rows:
            print("| " + " | ".join(r) + " |")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "."))

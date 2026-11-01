#!/usr/bin/env python3
"""Tables from bench/cloud/run.sh's logs and build.sh's build.tsv, as
Markdown.

Per framework and node count: each query's median over its passes (the
first pass included; Ray starts its worker processes in it), the median of
the per-pass totals, and the slowest pass. The one-process times come from
`run.sh local`, the reference runs.

Every run's answers (its ANSWER lines) are checked against the references:
row counts exactly, sums to 1e-6 relative. The references are checked
against each other the same way, so three implementations have to agree.

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
    head, times, local, answers = {}, collections.defaultdict(list), {}, {}
    passed = False
    for line in open(path):
        if line.startswith("# "):
            for kv in line[2:].split():
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    head[k] = v
        elif line.startswith("TIME "):
            _, q, _, ms = line.split()
            times[q].append(int(ms))
        elif line.startswith("ANSWER "):
            _, q, rows, a, b, z = line.split()
            answers[q] = (int(rows), float(a), float(b), float(z))
        elif line.startswith("PASS"):
            passed = True
        else:
            m = re.match(r"(q\d\w*) : .* one process (\d+) ms", line)
            if m:
                local[m.group(1)] = int(m.group(2))
    return {"head": head, "times": times, "local": local, "answers": answers, "passed": passed}


def close(x, y):
    return abs(x - y) <= 1e-6 * max(abs(x), abs(y), 1.0)


def disagreement(got, want):
    """The first query where `got` doesn't match `want`, or None."""
    for q, w in want.items():
        g = got.get(q)
        if g is None:
            return f"{q} missing"
        if g[0] != w[0]:
            return f"{q} rows {g[0]} vs {w[0]}"
        for i, name in ((1, "sum_a"), (2, "sum_b"), (3, "zones")):
            if not close(g[i], w[i]):
                return f"{q} {name} {g[i]!r} vs {w[i]!r}"
    return None


def fw_key(fw):
    return ORDER.index(fw) if fw in ORDER else len(ORDER)


def main(results):
    runs, refs = {}, {}
    for path in glob.glob(os.path.join(results, "*-*.log")):
        fw, what = os.path.basename(path)[:-4].rsplit("-", 1)
        if what == "local":
            refs[fw] = parse(path)
        elif what.isdigit():
            runs[(fw, int(what))] = parse(path)

    # The reference answers: the first framework's, once all of them agree.
    reference, ref_from, problems = None, None, []
    for fw in sorted(refs, key=fw_key):
        r = refs[fw]
        if not r["passed"]:
            problems.append(f"{fw}'s reference run didn't pass")
            continue
        if reference is None:
            reference, ref_from = r["answers"], fw
        else:
            d = disagreement(r["answers"], reference)
            if d:
                problems.append(f"{fw}'s reference disagrees with {ref_from}'s: {d}")

    if runs:
        keys = sorted(runs, key=lambda k: (k[1], fw_key(k[0])))
        queries = sorted({q for r in runs.values() for q in r["times"]})
        print("## Distributed, median of passes (ms)\n")
        print("| query | " + " | ".join(f"{fw} ×{n}" for fw, n in keys) + " |")
        print("|---|" + "---:|" * len(keys))
        for q in queries:
            cells = []
            for k in keys:
                t = runs[k]["times"].get(q)
                cells.append(f"{statistics.median(t):,.0f}" if t else "")
            print(f"| {q} | " + " | ".join(cells) + " |")
        totals, worst, agree = [], [], []
        for k in keys:
            times = runs[k]["times"]
            n = min((len(v) for v in times.values()), default=0)
            per_pass = [sum(times[q][p] for q in times) for p in range(n)]
            totals.append(f"**{statistics.median(per_pass):,.0f}**" if per_pass else "")
            worst.append(f"{max(per_pass):,.0f}" if per_pass else "")
            if not runs[k]["passed"]:
                agree.append("**failed**")
            elif reference is None:
                agree.append("no reference")
            else:
                d = disagreement(runs[k]["answers"], reference)
                agree.append("yes" if d is None else f"**no**: {d}")
                if d:
                    problems.append(f"{k[0]} on {k[1]} nodes: {d}")
        print("| total, median pass | " + " | ".join(totals) + " |")
        print("| total, slowest pass | " + " | ".join(worst) + " |")
        print("| answers match the reference | " + " | ".join(agree) + " |")
        slots = sorted({runs[k]["head"].get("slots", "?") for k in keys})
        passes = sorted({runs[k]["head"].get("passes", "?") for k in keys})
        print(f"\nSlots per node: {', '.join(slots)}. Passes: {', '.join(passes)}.\n")

    if refs:
        fws = sorted(refs, key=fw_key)
        queries = sorted({q for r in refs.values() for q in r["local"]})
        print("## One process, on the head node (ms)\n")
        print("| query | " + " | ".join(fws) + " |")
        print("|---|" + "---:|" * len(fws))
        for q in queries:
            print(f"| {q} | " + " | ".join(
                f"{refs[fw]['local'][q]:,}" if q in refs[fw]["local"] else "" for fw in fws) + " |")
        print("| total | " + " | ".join(f"**{sum(refs[fw]['local'].values()):,}**" for fw in fws) + " |\n")

    build = os.path.join(results, "build.tsv")
    if os.path.exists(build):
        rows = [l.rstrip("\n").split("\t") for l in open(build)][1:]
        print("## Building the images\n")
        print("| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |")
        print("|---|---:|---:|---:|---:|---:|---|")
        for r in rows:
            print("| " + " | ".join(r) + " |")
        print()

    if problems:
        print("## Problems\n")
        for p in problems:
            print(f"- {p}")
    elif reference is not None:
        print(f"All answers agree with the one-process references ({', '.join(sorted(refs, key=fw_key))}).")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "."))

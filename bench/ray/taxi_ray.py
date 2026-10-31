"""deploy/taxi.mojo again, on Ray: the same eight queries over the same
table, one Ray task per data file, checked against the same scans in one
Python process.

The queries and their folds are taxibench's own (`taxibench.queries`), so
the arithmetic matches the Python side of that benchmark. Each task reopens
the table, re-plans the scan and reads only its own file with PyIceberg's
`ArrowScan`, exactly as each dista task calls `to_batches_for_paths`. The
driver adds the partial states together.

    TAXI_METADATA   the table's metadata file (the path the table was written at)
    TAXI_THREADS    pyarrow's CPU and I/O threads per process; unset = pyarrow's default
    RAY_ADDRESS     the cluster; unset = a local Ray started here
"""

import math
import os
import socket
import sys
import time

import pyarrow as pa
import ray
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import StaticTable
from taxibench import queries

QUERIES = {q.name: q for q in queries.ALL}


def _threads():
    n = os.environ.get("TAXI_THREADS")
    if n:
        pa.set_cpu_count(int(n))
        pa.set_io_thread_count(int(n))


def _table(metadata):
    return StaticTable.from_metadata(metadata)


def _scan(table, q):
    return table.scan(row_filter=q.row_filter, selected_fields=q.columns or ("*",))


def _merge(a, b):
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, list):
            out[k] = [x + y for x, y in zip(out[k], v)]
        else:
            out[k] = out[k] + v
    return out


@ray.remote
def fold_file(metadata, name, path):
    """One query over one data file, folded where it was read."""
    _threads()
    q = QUERIES[name]
    table = _table(metadata)
    scan = _scan(table, q)
    tasks = [t for t in scan.plan_files() if t.file.file_path == path]
    arrow = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter, scan.case_sensitive)
    state = q.init()
    for batch in arrow.to_record_batches(tasks):
        state = q.fold(state, batch)
    return state, os.environ.get("NODE_NAME", socket.gethostname())


@ray.remote
def read_file(metadata, name, path):
    """One query over one data file, returned as an Arrow table."""
    _threads()
    q = QUERIES[name]
    table = _table(metadata)
    scan = _scan(table, q)
    tasks = [t for t in scan.plan_files() if t.file.file_path == path]
    arrow = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter, scan.case_sensitive)
    return arrow.to_table(tasks)


def _close(a, b):
    return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0)


def _check(name, got, want):
    for k, v in want.items():
        g = got[k]
        if isinstance(v, list):
            bad = [i for i, (x, y) in enumerate(zip(g, v)) if not _close(x, y)]
            if bad:
                raise SystemExit(f"{name}: {k}[{bad[0]}] {g[bad[0]]} vs {v[bad[0]]}")
        elif isinstance(v, int):
            if g != v:
                raise SystemExit(f"{name}: {k} {g} vs {v}")
        elif not _close(g, v):
            raise SystemExit(f"{name}: {k} {g} vs {v}")


def main():
    _threads()
    ray.init(address=os.environ.get("RAY_ADDRESS"), logging_level="warning")
    metadata = os.environ["TAXI_METADATA"]
    table = _table(metadata)
    nodes = set()
    total_dist = total_local = 0.0
    for q in queries.ALL:
        t0 = time.monotonic()
        files = [t.file.file_path for t in _scan(table, q).plan_files()]
        parts = ray.get([fold_file.remote(metadata, q.name, f) for f in files])
        got = q.init()
        for state, node in parts:
            got = _merge(got, state)
            nodes.add(node)
        dist_ms = (time.monotonic() - t0) * 1000

        t1 = time.monotonic()
        want = q.init()
        for batch in _scan(table, q).to_arrow_batch_reader():
            want = q.fold(want, batch)
        local_ms = (time.monotonic() - t1) * 1000
        _check(q.name, got, want)
        total_dist += dist_ms
        total_local += local_ms
        print(
            f"{q.name} : {len(files)} files, {got['n']} rows, sum {got.get('sum', 0.0)}"
            f" | distributed {dist_ms:.0f} ms, one process {local_ms:.0f} ms",
            flush=True,
        )

    q = QUERIES["q2_month_range"]
    t2 = time.monotonic()
    files = [t.file.file_path for t in _scan(table, q).plan_files()]
    got = q.init()
    for t in ray.get([read_file.remote(metadata, q.name, f) for f in files]):
        for batch in t.to_batches():
            got = q.fold(got, batch)
    want = q.init()
    for batch in _scan(table, q).to_arrow_batch_reader():
        want = q.fold(want, batch)
    _check("q2_as_batches", got, want)
    print(f"q2_as_batches : {got['n']} rows as Arrow, {(time.monotonic() - t2) * 1000:.0f} ms")

    print(f"tasks ran on {len(nodes)} nodes: {' '.join(sorted(nodes))}")
    expect = int(os.environ.get("DISTA_EXPECT_NODES", "1"))
    if len(nodes) < expect:
        raise SystemExit(f"expected {expect} nodes, saw {len(nodes)}")
    print(
        f"PASS ray taxi: 8 queries + 1 as batches, distributed == one process;"
        f" distributed {total_dist:.0f} ms, one process {total_local:.0f} ms"
    )


if __name__ == "__main__":
    main()

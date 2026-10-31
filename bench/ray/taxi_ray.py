"""deploy/taxi.mojo again, on Ray: the same eight queries over the same
table, one Ray task per data file, checked against the same scans in one
Python process.

The queries and their folds are taxibench's own (`taxibench.queries`), so
the arithmetic matches the Python side of that benchmark. The driver plans
each query once and ships every task its planned `FileScanTask`; the task
reopens the table's metadata and reads just that file with PyIceberg's
`ArrowScan`, as each dista task calls `to_batches_for_tasks`. No task reads
a manifest. The driver adds the partial states together.

    TAXI_METADATA   the table's metadata file (the path the table was written at);
                    `s3://` reads with the `AWS_*` environment
    TAXI_PASSES     distributed runs of each query (default 1); the first is
                    checked against one process, the rest against the first,
                    and each prints `TIME <query> <pass> <ms>`
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


def _s3_properties():
    """The `AWS_*` environment as PyIceberg's S3 properties, so the reads
    don't depend on which variables pyarrow happens to look at."""
    env = os.environ.get
    props = {}
    for var, prop in [
        ("AWS_ACCESS_KEY_ID", "s3.access-key-id"),
        ("AWS_SECRET_ACCESS_KEY", "s3.secret-access-key"),
        ("AWS_SESSION_TOKEN", "s3.session-token"),
    ]:
        if env(var):
            props[prop] = env(var)
    region = env("AWS_REGION") or env("AWS_DEFAULT_REGION")
    if region:
        props["s3.region"] = region
    if env("AWS_ENDPOINT_URL"):
        props["s3.endpoint"] = env("AWS_ENDPOINT_URL")
        props["s3.force-virtual-addressing"] = "false"
    return props


def _table(metadata):
    return StaticTable.from_metadata(metadata, properties=_s3_properties())


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
def fold_file(metadata, name, task):
    """One query over one planned data file, folded where it was read."""
    _threads()
    q = QUERIES[name]
    table = _table(metadata)
    scan = _scan(table, q)
    arrow = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter, scan.case_sensitive)
    state = q.init()
    for batch in arrow.to_record_batches([task]):
        state = q.fold(state, batch)
    return state, os.environ.get("NODE_NAME", socket.gethostname())


@ray.remote
def read_file(metadata, name, task):
    """One query over one planned data file, returned as an Arrow table."""
    _threads()
    q = QUERIES[name]
    table = _table(metadata)
    scan = _scan(table, q)
    arrow = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter, scan.case_sensitive)
    return arrow.to_table([task])


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


def _distributed(table, metadata, q, nodes):
    """Query `q` as one task per planned file; the answer and how many files."""
    tasks = list(_scan(table, q).plan_files())
    parts = ray.get([fold_file.remote(metadata, q.name, t) for t in tasks])
    got = q.init()
    for state, node in parts:
        got = _merge(got, state)
        nodes.add(node)
    return got, len(tasks)


def main():
    _threads()
    ray.init(address=os.environ.get("RAY_ADDRESS"), logging_level="warning")
    metadata = os.environ["TAXI_METADATA"]
    passes = int(os.environ.get("TAXI_PASSES", "1"))
    table = _table(metadata)
    nodes = set()
    total_dist = total_local = 0.0
    for q in queries.ALL:
        t0 = time.monotonic()
        got, n_files = _distributed(table, metadata, q, nodes)
        dist_ms = (time.monotonic() - t0) * 1000
        print(f"TIME {q.name} 1 {dist_ms:.0f}", flush=True)

        t1 = time.monotonic()
        want = q.init()
        for batch in _scan(table, q).to_arrow_batch_reader():
            want = q.fold(want, batch)
        local_ms = (time.monotonic() - t1) * 1000
        _check(q.name, got, want)
        for k in range(2, passes + 1):
            t = time.monotonic()
            again, _ = _distributed(table, metadata, q, nodes)
            print(f"TIME {q.name} {k} {(time.monotonic() - t) * 1000:.0f}", flush=True)
            _check(q.name, again, got)
        total_dist += dist_ms
        total_local += local_ms
        print(
            f"{q.name} : {n_files} files, {got['n']} rows, sum {got.get('sum', 0.0)}"
            f" | distributed {dist_ms:.0f} ms, one process {local_ms:.0f} ms",
            flush=True,
        )

    q = QUERIES["q2_month_range"]
    t2 = time.monotonic()
    tasks = list(_scan(table, q).plan_files())
    got = q.init()
    for t in ray.get([read_file.remote(metadata, q.name, task) for task in tasks]):
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

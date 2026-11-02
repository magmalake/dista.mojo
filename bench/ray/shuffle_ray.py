"""deploy/shuffle.mojo again, on Ray: trips by zone pair and hour, as map
tasks partitioning by key and reduce tasks taking their partition from every
map task. The query and its answer are in bench/shuffle/README.md.

A map task reads one planned data file with PyIceberg's `ArrowScan`, computes
the trips' features with NumPy, adds them up per key with `bincount`, and
returns the groups it saw as `R` partitions (`num_returns=R`), each its own
object in Ray's object store. Reducer `j` is handed partition `j` of every
map task as arguments, so Ray fetches them to it, and returns its totals and
top 10. The driver adds those up. With `TAXI_MODE=both` the same code then
runs over the whole table in this process, and the answers must agree.

    TAXI_METADATA     the table's metadata file; `s3://` reads with the
                      `AWS_*` environment
    SHUFFLE_REDUCERS  reduce tasks (default 16)
    TAXI_PASSES       distributed runs (default 1), each printing
                      `TIME zone_hours <pass> <ms>`
    TAXI_MODE         `both` (default), `distributed` or `local`
    RAY_ADDRESS       the cluster; unset = a local Ray started here
"""

import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray
from pyiceberg.io.pyarrow import ArrowScan

from taxi_ray import _table, _threads

ZONES = 266
HOURS = 24
KEYS = ZONES * ZONES * HOURS
HOUR_US = 3_600_000_000
MAX_DURATION_US = 4 * HOUR_US
TOP = 10
COLUMNS = (
    "PULocationID",
    "DOLocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "tip_amount",
)


def _scan(table):
    return table.scan(selected_fields=COLUMNS)


class Dense:
    """Every key's group, indexed by key."""

    def __init__(self):
        self.n = np.zeros(KEYS, dtype=np.int64)
        self.speed = np.zeros(KEYS)
        self.tip = np.zeros(KEYS)
        self.fare = np.zeros(KEYS)

    def add(self, batch):
        """Adds the batch's trips that count (see the README)."""
        cols = {c: batch.column(c) for c in COLUMNS}
        valid = np.ones(batch.num_rows, dtype=bool)
        for c in cols.values():
            if c.null_count:
                valid &= pc.is_valid(c).to_numpy(zero_copy_only=False)

        def ints(name):
            c = cols[name]
            if pa.types.is_timestamp(c.type):
                c = c.cast(pa.int64())
            return c.fill_null(0).to_numpy(zero_copy_only=False).astype(np.int64)

        def floats(name):
            return cols[name].fill_null(0.0).to_numpy(zero_copy_only=False).astype(np.float64)

        pu, do = ints("PULocationID"), ints("DOLocationID")
        start, end = ints("tpep_pickup_datetime"), ints("tpep_dropoff_datetime")
        miles, dollars, tips = floats("trip_distance"), floats("fare_amount"), floats("tip_amount")
        us = end - start
        keep = (
            valid
            & (pu >= 1) & (pu <= 265) & (do >= 1) & (do <= 265)
            & (miles > 0) & (dollars > 0) & (us > 0) & (us <= MAX_DURATION_US)
        )
        pu, do, start, us = pu[keep], do[keep], start[keep], us[keep]
        miles, dollars, tips = miles[keep], dollars[keep], tips[keep]
        hour = (start // HOUR_US) % HOURS
        key = (pu * ZONES + do) * HOURS + hour
        self.n += np.bincount(key, minlength=KEYS)
        self.speed += np.bincount(key, weights=miles / (us / HOUR_US), minlength=KEYS)
        self.tip += np.bincount(key, weights=tips / dollars, minlength=KEYS)
        self.fare += np.bincount(key, weights=dollars, minlength=KEYS)

    def groups(self):
        """The groups with trips: keys, n and the three sums."""
        keys = np.flatnonzero(self.n)
        return keys, self.n[keys], self.speed[keys], self.tip[keys], self.fare[keys]


def _answer(keys, n, speed, tip, fare):
    """Totals and the top 10 of some groups: most trips, then smaller key."""
    order = np.lexsort((keys, -n))[:TOP]
    top = [(int(keys[i]), int(n[i]), float(speed[i]), float(tip[i]), float(fare[i])) for i in order]
    return {
        "groups": int(len(keys)),
        "trips": int(n.sum()),
        "speed": float(speed.sum()),
        "tip": float(tip.sum()),
        "fare": float(fare.sum()),
        "top": top,
    }


def _merge(a, b):
    out = {k: a[k] + b[k] for k in ("groups", "trips", "speed", "tip", "fare")}
    both = a["top"] + b["top"]
    both.sort(key=lambda g: (-g[1], g[0]))
    out["top"] = both[:TOP]
    return out


@ray.remote
def map_zones(metadata, task, r):
    """Map task: one data file's groups, in `r` partitions."""
    _threads()
    table = _table(metadata)
    scan = _scan(table)
    arrow = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter, scan.case_sensitive)
    dense = Dense()
    for batch in arrow.to_record_batches([task]):
        dense.add(batch)
    keys, n, speed, tip, fare = dense.groups()
    part = keys % r
    out = []
    for j in range(r):
        m = part == j
        out.append((keys[m], n[m], speed[m], tip[m], fare[m]))
    return tuple(out)


@ray.remote
def reduce_zones(j, r, *parts):
    """Reducer `j`: partition `j` of every map task, merged."""
    slots = KEYS // r + 1
    n = np.zeros(slots, dtype=np.int64)
    speed, tip, fare = np.zeros(slots), np.zeros(slots), np.zeros(slots)
    for keys, pn, ps, pt, pf in parts:
        if np.any(keys % r != j):
            raise ValueError(f"a key reached reducer {j} of {r}")
        s = keys // r
        np.add.at(n, s, pn)
        np.add.at(speed, s, ps)
        np.add.at(tip, s, pt)
        np.add.at(fare, s, pf)
    seen = np.flatnonzero(n)
    return _answer(seen * r + j, n[seen], speed[seen], tip[seen], fare[seen])


def _distributed(table, metadata, r):
    tasks = list(_scan(table).plan_files())
    maps = []
    for t in tasks:
        refs = map_zones.options(num_returns=r).remote(metadata, t, r)
        maps.append([refs] if r == 1 else refs)
    reduces = [reduce_zones.remote(j, r, *[m[j] for m in maps]) for j in range(r)]
    del maps  # each partition is freed once its reducer is done with it
    got = None
    for a in ray.get(reduces):
        got = a if got is None else _merge(got, a)
    return got


def _local(table):
    t = time.monotonic()
    dense = Dense()
    for batch in _scan(table).to_arrow_batch_reader():
        dense.add(batch)
    return _answer(*dense.groups()), (time.monotonic() - t) * 1000


def _close(a, b):
    return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0)


def _check(what, got, want):
    if (got["groups"], got["trips"]) != (want["groups"], want["trips"]):
        raise SystemExit(f"{what}: {got['groups']} groups and {got['trips']} trips, "
                         f"wanted {want['groups']} and {want['trips']}")
    for k in ("speed", "tip", "fare"):
        if not _close(got[k], want[k]):
            raise SystemExit(f"{what}: sum of {k} {got[k]} vs {want[k]}")
    for i, (g, w) in enumerate(zip(got["top"], want["top"])):
        if g[:2] != w[:2] or not _close(g[2], w[2]):
            raise SystemExit(f"{what}: top {i + 1} {g} vs {w}")


def _print(a):
    print(f"ANSWER zone_hours {a['groups']} {a['trips']} {a['speed']!r} {a['tip']!r} {a['fare']!r}")
    for i, (key, n, speed, tip, _fare) in enumerate(a["top"]):
        pu, do, hour = key // HOURS // ZONES, key // HOURS % ZONES, key % HOURS
        print(f"TOP {i + 1} {key} {pu} {do} {hour} {n} {speed / n!r} {tip / n!r}")
    print(flush=True, end="")


def main():
    _threads()
    mode = os.environ.get("TAXI_MODE", "both")
    if mode not in ("both", "distributed", "local"):
        raise SystemExit(f"TAXI_MODE must be both, distributed or local, got {mode}")
    metadata = os.environ["TAXI_METADATA"]
    table = _table(metadata)
    if mode == "local":
        want, ms = _local(table)
        _print(want)
        print(f"zone_hours : {want['groups']} groups, {want['trips']} trips"
              f" | distributed - , one process {ms:.0f} ms")
        print(f"PASS ray shuffle: zone_hours in one process, {ms:.0f} ms")
        return
    ray.init(address=os.environ.get("RAY_ADDRESS"), logging_level="warning")
    r = int(os.environ.get("SHUFFLE_REDUCERS", "16"))
    passes = int(os.environ.get("TAXI_PASSES", "1"))
    t0 = time.monotonic()
    got = _distributed(table, metadata, r)
    dist_ms = (time.monotonic() - t0) * 1000
    print(f"TIME zone_hours 1 {dist_ms:.0f}", flush=True)
    local = "-"
    if mode == "both":
        want, local_ms = _local(table)
        _check("zone_hours", got, want)
        local = f"{local_ms:.0f}"
    _print(got)
    for k in range(2, passes + 1):
        t = time.monotonic()
        again = _distributed(table, metadata, r)
        print(f"TIME zone_hours {k} {(time.monotonic() - t) * 1000:.0f}", flush=True)
        _check(f"zone_hours pass {k}", again, got)
    print(f"zone_hours : {got['groups']} groups, {got['trips']} trips"
          f" | distributed {dist_ms:.0f} ms, one process {local} ms")
    checked = "checked against one process;" if mode == "both" else "passes agree;"
    print(f"PASS ray shuffle: zone_hours, {r} reducers; {checked} "
          f"distributed {dist_ms:.0f} ms, one process {local} ms")


if __name__ == "__main__":
    main()

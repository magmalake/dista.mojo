"""`DISTA_WORKER_SLOTS` on a CPU-bound fan-out, in local mode.

64 tasks, each about 0.1 s of single-threaded arithmetic, on 1 and 2
workers with 1, 2, 4 and 8 slots each. One slot is how a worker ran before
slots: one task at a time, the next prefetched (`pipeline` 2). Wall time and
the speedup over one slot on the same workers. The machine's cores bound
it: past them, slots only queue.
"""

from std.os import setenv, unsetenv
from std.time import perf_counter_ns

from threads import num_cpus

from dista import ObjectRef, Runtime, Task


def spin(n: Int) -> Int:
    """About `n` million steps of an LCG."""
    var x = UInt64(n)
    for _ in range(n * 1_000_000):
        x = x * 6364136223846793005 + 1442695040888963407
    return Int(x >> 1) & 0xFFFF


comptime Spin = Task[Int, Int, spin]


def _run(
    workers: Int, slots: Int, tasks: Int, n: Int, want: Int
) raises -> Float64:
    _ = setenv("DISTA_WORKER_SLOTS", String(slots))
    var rt = Runtime[Spin](workers=workers, pipeline=2)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for _ in range(tasks):
        refs.append(rt.remote[Spin](n))
    for r in refs:
        if rt.take(r) != want:
            raise Error("a wrong answer")
    return Float64(perf_counter_ns() - t0) / 1e9


def _r(x: Float64) -> Float64:
    """`x` to two decimals."""
    return Float64(Int(x * 100.0 + 0.5)) / 100.0


def main() raises:
    var n = 140
    var tasks = 64
    var t0 = perf_counter_ns()
    var want = spin(n)
    var one = Float64(perf_counter_ns() - t0) / 1e9
    print(
        "CPU-bound fan-out:",
        tasks,
        "tasks of",
        _r(one),
        "s each,",
        num_cpus(),
        "cores",
    )
    print("workers  slots  wall s  speedup")
    for workers in [1, 2]:
        var base = 0.0
        for slots in [1, 2, 4, 8]:
            var s = _run(workers, slots, tasks, n, want)
            if slots == 1:
                base = s
            print(
                "  ",
                workers,
                "     ",
                slots,
                "   ",
                _r(s),
                "  ",
                _r(base / s),
                "x",
            )

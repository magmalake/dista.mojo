"""`DISTA_WORKER_SLOTS=auto` against a hand-set number, on a memory-heavy mix.

One local-mode worker runs `SIZING_TASKS` tasks (default 96), alternating
64 MB and 512 MB: each fills its buffer and sums it four times, so it is
memory-bound and holds its memory while it runs. The slots come from the
environment the caller sets (`bench/sizing.sh` runs each setting in a
process of its own). Prints the wall time, tasks a second and the worker's
peak resident memory (the largest child's, from `getrusage`).
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.sys.info import CompilationTarget
from std.time import perf_counter_ns

from dista import ObjectRef, Runtime, Task


def churn(mb: Int) -> Int:
    """Fills `mb` MB and sums it four times."""
    var n = mb << 20
    var buf = List[UInt8](length=n, fill=0)
    var x = UInt64(mb)
    for i in range(n):
        x = x * 6364136223846793005 + 1442695040888963407
        buf[i] = UInt8(x >> 56)
    var total = 0
    for _ in range(4):
        for i in range(0, n, 64):
            total += Int(buf[i])
    return total & 0xFFFF


comptime Churn = Task[Int, Int, churn]


def _children_peak_mb() -> Int:
    """The peak resident memory of the largest child waited for, in MB."""
    var buf = List[UInt8](length=256, fill=0)
    _ = external_call["getrusage", c_int](c_int(-1), buf.unsafe_ptr())
    # ru_maxrss: after two timevals (32 bytes).
    var v = 0
    for k in range(8):
        v |= Int(buf[32 + k]) << (8 * k)
    comptime if CompilationTarget.is_macos():
        return v >> 20  # bytes
    else:
        return v >> 10  # kilobytes


def _run(tasks: Int) raises -> Float64:
    """The mix's wall time; the workers have exited when it returns."""
    var rt = Runtime[Churn](workers=1, pipeline=2)
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for k in range(tasks):
        refs.append(rt.remote[Churn](64 if k % 2 == 0 else 512))
    for r in refs:
        _ = rt.take(r)
    return Float64(perf_counter_ns() - t0) / 1e9


def main() raises:
    var tasks = Int(getenv("SIZING_TASKS", "96"))
    var label = getenv("SIZING_LABEL", "run")
    var wall = _run(tasks)
    var rate = Float64(tasks) / wall
    print(
        label,
        " wall_s=",
        Float64(Int(wall * 100.0)) / 100.0,
        " tasks_per_s=",
        Float64(Int(rate * 100.0)) / 100.0,
        " worker_peak_mb=",
        _children_peak_mb(),
        sep="",
    )

"""Task throughput through the head.

Three measurements:

- **Backpressure.** 200,000 tasks with 500-byte arguments through a queue
  capped at 10,000 (`DISTA_MAX_QUEUED`) on 4 workers: the driver submits
  faster than they finish, so the queue stays full the whole run. Tasks per
  second, and the head's peak resident memory, read from the operating
  system once the runtime has exited. It runs first, since that peak is the
  largest of every child process this one has waited for.
- **Gathers waiting.** 2,000 tiny map tasks on 4 workers while 384 reduce
  tasks, each gathering every map output, wait for them: the time from the
  first submission to the last reducer's answer.

- **Pipeline depth.** 10,000 tiny tasks on 4 workers, 1 to 8 tasks in flight
  per worker.
- **Workers.** Tasks per second through one head as the worker count grows:
  empty tasks, and tasks returning 1 MB, which stays on its worker, so the
  head only routes its location. Results are released as they're read. The
  counts come from `FANOUT_WORKERS` (default "4 16 64 192"); every worker is
  a forked process on this machine, so the large counts measure the head's
  loop, not the machine's cores.
"""

from std.ffi import c_int, external_call
from std.os import getenv, setenv, unsetenv
from std.sys.info import CompilationTarget
from std.time import perf_counter_ns

from dista import ObjectRef, Runtime, Seq, Task


def square(x: Int) -> Int:
    return x * x


def megabyte(x: Int) -> String:
    return String("x") * (1 << 20)


def length(s: String) -> Int:
    return s.byte_length()


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for x in xs.items:
        t += x
    return t


comptime Square = Task[Int, Int, square]
comptime Megabyte = Task[Int, String, megabyte]
comptime Length = Task[String, Int, length]


def _children_peak_mb() -> Float64:
    """The largest resident set of any child this process has waited for,
    in MB: `getrusage(RUSAGE_CHILDREN)`'s `ru_maxrss`, which follows two
    timevals and is KiB on Linux, bytes on macOS."""
    var usage = List[Int64](length=18, fill=0)
    _ = external_call["getrusage", c_int](
        c_int(-1), usage.unsafe_ptr().unsafe_bitcast[NoneType]()
    )
    var peak = Float64(usage[4])
    comptime if CompilationTarget.is_linux():
        peak *= 1024
    return peak / 1e6


def _submit_backpressured() raises -> Float64:
    """Runs the backpressure job; its tasks per second."""
    comptime N = 200_000
    var rt = Runtime[Square, Megabyte, Length](workers=4, pipeline=2)
    var arg = String("a") * 500
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]](capacity=N)
    for _ in range(N):
        refs.append(rt.remote[Length](arg))
    var total = 0
    for r in refs:
        total += rt.get(r)
    rt.release(refs)
    if total != N * 500:
        raise Error("backpressure: wrong total ", total)
    return Float64(N) / (Float64(perf_counter_ns() - t0) / 1e9)


def _backpressure() raises:
    _ = setenv("DISTA_MAX_QUEUED", "10000")
    var rate = _submit_backpressured()  # the runtime has exited here
    _ = unsetenv("DISTA_MAX_QUEUED")
    print(
        "backpressure :",
        Int(rate),
        "tasks/s, head peak",
        Int(_children_peak_mb()),
        "MB",
    )


comptime Total = Task[Seq[Int], Int, total]


def _gathers() raises:
    comptime MAPS = 2_000
    comptime REDUCERS = 384
    var rt = Runtime[Square, Megabyte, Total](workers=4, pipeline=2)
    _ = rt.get(rt.remote[Square](1))  # warm up every connection
    var t0 = perf_counter_ns()
    var maps = List[ObjectRef[Int]](capacity=MAPS)
    for i in range(MAPS):
        maps.append(rt.remote[Square](i))
    var reducers = List[ObjectRef[Int]](capacity=REDUCERS)
    for _ in range(REDUCERS):
        reducers.append(rt.remote[Total](maps))
    var want = 0
    for i in range(MAPS):
        want += i * i
    for r in reducers:
        if rt.get(r) != want:
            raise Error("gathers: a wrong total")
    var secs = Float64(perf_counter_ns() - t0) / 1e9
    rt.release(reducers)
    rt.release(maps)
    print(
        "gathers :",
        MAPS,
        "maps,",
        REDUCERS,
        "reducers waiting on all of them:",
        secs,
        "s",
    )


def _through_head(workers: Int) raises:
    var rt = Runtime[Square, Megabyte, Length, Total](
        workers=workers, pipeline=2
    )
    for i in range(workers):
        _ = rt.get(rt.remote[Square](i))  # warm up every connection

    comptime N = 20_000
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]](capacity=N)
    for i in range(N):
        refs.append(rt.remote[Square](i))
    for r in refs:
        _ = rt.get(r)
    var empty = Float64(N) / (Float64(perf_counter_ns() - t0) / 1e9)
    rt.release(refs)

    comptime M = 2_000
    var t1 = perf_counter_ns()
    var big = List[ObjectRef[String]](capacity=M)
    for i in range(M):
        big.append(rt.remote[Megabyte](i))
    for r in big:
        _ = rt.location(r)
        rt.release(r)
    var stored = Float64(M) / (Float64(perf_counter_ns() - t1) / 1e9)
    print(
        "workers",
        workers,
        ":",
        Int(empty),
        "empty tasks/s,",
        Int(stored),
        "1 MB tasks/s",
    )


def main() raises:
    _backpressure()
    _gathers()
    comptime N = 10_000
    for depth in [1, 2, 4, 8]:
        var rt = Runtime[Square, Megabyte, Length, Total](
            workers=4, pipeline=depth
        )
        _ = rt.get(rt.remote[Square](1))  # warm up every connection
        var t0 = perf_counter_ns()
        var refs = List[ObjectRef[Int]](capacity=N)
        for i in range(N):
            refs.append(rt.remote[Square](i))
        var total = 0
        for r in refs:
            total += rt.get(r)
        var us = Float64(perf_counter_ns() - t0) / 1e3 / Float64(N)
        print("pipeline", depth, ":", us, "us per task, total", total)

    var counts = getenv("FANOUT_WORKERS", "4 16 64 192")
    for w in counts.split(" "):
        var n = String(w)
        if n.byte_length() > 0:
            _through_head(Int(n))

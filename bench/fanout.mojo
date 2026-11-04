"""Task throughput through the head.

Three measurements:

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

from std.os import getenv
from std.time import perf_counter_ns

from dista import ObjectRef, Runtime, Seq, Task


def square(x: Int) -> Int:
    return x * x


def megabyte(x: Int) -> String:
    return String("x") * (1 << 20)


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for x in xs.items:
        t += x
    return t


comptime Square = Task[Int, Int, square]
comptime Megabyte = Task[Int, String, megabyte]
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
    var rt = Runtime[Square, Megabyte, Total](workers=workers, pipeline=2)
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
    _gathers()
    comptime N = 10_000
    for depth in [1, 2, 4, 8]:
        var rt = Runtime[Square, Megabyte, Total](workers=4, pipeline=depth)
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

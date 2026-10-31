"""Task throughput: 10,000 tiny tasks at several pipeline depths."""

from std.time import perf_counter_ns

from dista import ObjectRef, Runtime, Task


def square(x: Int) -> Int:
    return x * x


comptime Square = Task[Int, Int, square]


def main() raises:
    comptime N = 10_000
    for depth in [1, 2, 4, 8]:
        var rt = Runtime[Square](workers=4, pipeline=depth)
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

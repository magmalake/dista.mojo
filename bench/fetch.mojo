"""Large results on the fetch path: time, and the peak memory it costs.

One task makes a 256 MB result, and 12 tasks then each take it as input, on
4 workers with 4 tasks in flight each, so the worker holding it serves
several fetchers at once. Then the driver reads it. `FETCH_MODE` picks how
the result travels:

- `sockets`: the node store off (`DISTA_SHM=0`), so the result stays in the
  worker's memory and every reader fetches it over a socket, as between
  hosts.
- `store`: the node store on, so the readers map its file.

Each run prints the seconds the 12 readers took, the seconds the driver's
read took, and the peak resident memory of the largest child process (a
worker) and of the driver, from `getrusage`. Run one mode per process: a
child's peak is the largest of every child this process has waited for.

    pixi run bench-fetch
"""

from std.ffi import c_int, external_call
from std.os import getenv, setenv, unsetenv
from std.sys.info import CompilationTarget
from std.time import perf_counter_ns

from dista import ObjectRef, Runtime, Task

comptime SIZE = 256 << 20
comptime READERS = 12


def make(seed: Int) -> String:
    return String(seed % 10) * SIZE


def length(s: String) -> Int:
    return s.byte_length()


comptime Make = Task[Int, String, make]
comptime Length = Task[String, Int, length]


def _peak_mb(who: c_int) -> Float64:
    """`getrusage(who)`'s `ru_maxrss` in MB: after two timevals, KiB on
    Linux, bytes on macOS."""
    var usage = List[Int64](length=18, fill=0)
    _ = external_call["getrusage", c_int](
        who, usage.unsafe_ptr().unsafe_bitcast[NoneType]()
    )
    var peak = Float64(usage[4])
    comptime if CompilationTarget.is_linux():
        peak *= 1024
    return peak / 1e6


def _run() raises -> Tuple[Float64, Float64]:
    var rt = Runtime[Make, Length](workers=4, pipeline=4)
    var r = rt.remote[Make](7)
    _ = rt.location(r)
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for _ in range(READERS):
        refs.append(rt.remote[Length](r))
    for x in refs:
        if rt.get(x) != SIZE:
            raise Error("a reader got the wrong size")
    var readers = Float64(perf_counter_ns() - t0) / 1e9
    var t1 = perf_counter_ns()
    var s = rt.get(r)
    var driver = Float64(perf_counter_ns() - t1) / 1e9
    if s.byte_length() != SIZE:
        raise Error("the driver got the wrong size")
    rt.release(refs)
    rt.release(r)
    return (readers, driver)


def main() raises:
    var mode = getenv("FETCH_MODE", "sockets")
    if mode == "sockets":
        _ = setenv("DISTA_SHM", "0")
    elif mode != "store":
        raise Error("FETCH_MODE is sockets or store, not ", mode)
    var t = _run()  # the runtime has exited here
    _ = unsetenv("DISTA_SHM")
    print(
        "fetch",
        mode,
        ":",
        READERS,
        "readers",
        t[0],
        "s, driver",
        t[1],
        "s, worker peak",
        Int(_peak_mb(-1)),
        "MB, driver peak",
        Int(_peak_mb(0)),
        "MB",
    )

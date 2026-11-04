"""Cancelling tasks, waiting with a timeout, and a driver that leaves.

The first tests drive a `Head` directly, over socket pairs, as the fuzzer
does: one driver and one worker running one task at a time, so what the head
sends the worker next shows which tasks still run. The others run a local
`Runtime` with one worker.
"""

from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Task
from dista.codec import put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_CANCEL,
    KIND_HELLO,
    KIND_RESULT,
    KIND_TASK,
    KIND_TASK_AFTER,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, stream_pair

comptime HASH: UInt64 = 0x5EED


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    return head._add_conn(Conn(pair.pop()))


def _hello(mut head: Head, d: Int, w: Int) raises -> UInt64:
    """Says hello as the driver on `d` and the worker on `w`; returns the
    driver's number."""
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(d, Frame(KIND_HELLO, 0, 0, hello^))
    hello = [ROLE_WORKER]
    put_scalar[DType.uint64](hello, HASH)
    hello.extend(encode_addr([127, 0, 0, 1], 7000))
    head._handle(w, Frame(KIND_HELLO, 0, 0, hello^))
    for entry in head._driver_conn.items():
        if entry.value == d:
            return entry.key
    raise Error("the driver got no number")


def _after(dep: UInt64) -> List[UInt8]:
    """A task-after payload on one input."""
    var body: List[UInt8] = [0]
    put_scalar[DType.uint64](body, 1)
    put_scalar[DType.uint64](body, dep)
    return body^


def _step(mut head: Head):
    head._release_waiting()
    head._dispatch()


def _queued(head: Head) -> Int:
    return head._queued()


def test_a_departed_drivers_tasks_dont_run() raises:
    var head = Head(Listener(0), HASH, 1, False)
    var peers = List[Stream]()
    var d = _connect(head, peers)
    var w = _connect(head, peers)
    var n = _hello(head, d, w)
    var base = n << DRIVER_SHIFT
    for k in range(1, 6):
        head._handle(
            d,
            Frame(
                KIND_TASK, base | UInt64(k), 7, List[UInt8](length=100, fill=1)
            ),
        )
    head._handle(d, Frame(KIND_TASK_AFTER, base | 6, 7, _after(base | 5)))
    _step(head)
    assert_equal(len(head._inflight[w]), 1)  # task 1 runs
    assert_equal(_queued(head), 4)
    assert_equal(len(head._waiting), 1)
    head._drop(d)
    assert_equal(_queued(head), 0, "queued tasks outlived their driver")
    assert_equal(len(head._waiting), 0, "waiting tasks outlived their driver")
    assert_equal(head._queued_bytes + head._waiting_bytes, 0)
    # Task 1 finishes; nothing else is sent, and nothing is left.
    head._handle(w, Frame(KIND_RESULT, base | 1, 0, List[UInt8]()))
    _step(head)
    assert_equal(len(head._inflight[w]), 0, "a departed driver's task ran")
    assert_equal(len(head._running), 0)
    assert_equal(len(head._refs), 0)
    assert_equal(len(head._held), 0)
    assert_equal(len(head._dead), 0)
    assert_equal(len(head._deps_of), 0)
    _ = len(peers)


def test_cancel_drops_queued_waiting_and_running_tasks() raises:
    var head = Head(Listener(0), HASH, 1, False)
    var peers = List[Stream]()
    var d = _connect(head, peers)
    var w = _connect(head, peers)
    var n = _hello(head, d, w)
    var base = n << DRIVER_SHIFT
    for k in range(1, 4):
        head._handle(d, Frame(KIND_TASK, base | UInt64(k), 7, List[UInt8]()))
    head._handle(d, Frame(KIND_TASK_AFTER, base | 4, 7, _after(base | 3)))
    _step(head)
    assert_equal(head._inflight[w][0], base | 1)
    head._handle(d, Frame(KIND_CANCEL, base | 1, 0, List[UInt8]()))  # running
    head._handle(d, Frame(KIND_CANCEL, base | 2, 0, List[UInt8]()))  # queued
    head._handle(d, Frame(KIND_CANCEL, base | 4, 0, List[UInt8]()))  # waiting
    assert_equal(head._errors.get(base | 1, String()), "cancelled")
    assert_equal(head._errors.get(base | 2, String()), "cancelled")
    assert_equal(head._errors.get(base | 4, String()), "cancelled")
    assert_equal(len(head._waiting), 0)
    # Task 1's late result is dropped; task 3 runs next, not task 2.
    head._handle(w, Frame(KIND_RESULT, base | 1, 0, List[UInt8]()))
    assert_true(base | 1 not in head._results, "a cancelled result was kept")
    _step(head)
    assert_equal(len(head._inflight[w]), 1)
    assert_equal(head._inflight[w][0], base | 3, "a cancelled task ran")
    head._handle(w, Frame(KIND_RESULT, base | 3, 0, List[UInt8]()))
    _step(head)
    assert_equal(len(head._inflight[w]), 0)
    assert_equal(_queued(head), 0)
    assert_equal(head._queued_bytes, 0)
    assert_equal(len(head._cancelled), 0)
    # Cancelling a settled object changes nothing.
    head._handle(d, Frame(KIND_CANCEL, base | 3, 0, List[UInt8]()))
    assert_true(base | 3 in head._results)
    assert_true(base | 3 not in head._errors)
    _ = len(peers)


def nap(ms: Int) -> Int:
    sleep(Float64(ms) / 1000)
    return ms


comptime Nap = Task[Int, Int, nap]


def _seconds_since(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e9


def test_cancelled_tasks_dont_run() raises:
    var rt = Runtime[Nap](workers=1, pipeline=1)
    var t0 = perf_counter_ns()
    var first = rt.remote[Nap](500)
    var rest = List[ObjectRef[Int]]()
    for _ in range(20):
        rest.append(rt.remote[Nap](500))
    rt.cancel(rest)
    assert_equal(rt.get(first), 500)
    for r in rest:
        var msg = String()
        try:
            _ = rt.get(r)
        except e:
            msg = String(e)
        assert_equal(msg, "cancelled")
    rt.release(rest)
    var took = _seconds_since(t0)
    print("  21 tasks of 0.5 s, 20 cancelled:", took, "s")
    assert_true(took < 4.0, "cancelled tasks ran")
    # The worker is free for new work.
    assert_equal(rt.get(rt.remote[Nap](1)), 1)


def test_get_with_a_timeout() raises:
    var rt = Runtime[Nap](workers=1, pipeline=1)
    var r = rt.remote[Nap](2000)
    var t0 = perf_counter_ns()
    var msg = String()
    try:
        _ = rt.get(r, timeout=0.2)
    except e:
        msg = String(e)
    var took = _seconds_since(t0)
    assert_true(msg.startswith("timed out"), msg)
    assert_true(took >= 0.2 and took < 1.5, String("waited ", took, " s"))
    assert_equal(rt.get(r), 2000)
    assert_equal(rt.get(rt.remote[Nap](1), timeout=30.0), 1)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

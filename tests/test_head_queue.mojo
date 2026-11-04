"""The head's ready queue under backpressure: it holds what's queued, no more.

These drive a `Head` directly, as the fuzzer does: a driver and a worker are
socket pairs past their hello, frames go in through `_handle`, and the loop's
steps run by hand. The driver keeps the queue topped up while the worker
drains it, so the queue never empties, as when a driver submits faster than
workers finish.
"""

from std.testing import TestSuite, assert_equal, assert_true

from dista.codec import put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_HELLO,
    KIND_RESULT,
    KIND_TASK,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, stream_pair

comptime HASH: UInt64 = 0x5EED
comptime QUEUED = 200
"""Tasks the driver keeps queued."""
comptime ROUNDS = 2_000
comptime ARGS = 500
"""Bytes of each task's argument."""


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    return head._add_conn(Conn(pair.pop()))


def _hello(mut head: Head, i: Int, role: UInt8) raises:
    var hello: List[UInt8] = [role]
    put_scalar[DType.uint64](hello, HASH)
    if role == ROLE_WORKER:
        hello.extend(encode_addr([127, 0, 0, 1], 7000))
    head._handle(i, Frame(KIND_HELLO, 0, 0, hello^))


def _number(head: Head, driver: Int) -> UInt64:
    """The number the head gave the driver on connection `driver`."""
    for entry in head._driver_conn.items():
        if entry.value == driver:
            return entry.key
    return 0


def _submit(mut head: Head, driver: Int, mut next: UInt64) raises:
    """The driver's next task."""
    next += 1
    var obj = (_number(head, driver) << DRIVER_SHIFT) | next
    head._handle(
        driver, Frame(KIND_TASK, obj, 7, List[UInt8](length=ARGS, fill=1))
    )


def _queued_args(head: Head) raises -> Int:
    """Bytes of arguments the queue's list holds, handed out or not."""
    var n = 0
    for ref t in head._queues[HASH].tasks:
        n += len(t.args)
    return n


def test_a_queue_that_never_empties_holds_only_whats_queued() raises:
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var driver = _connect(head, peers)
    var worker = _connect(head, peers)
    _hello(head, driver, ROLE_DRIVER)
    _hello(head, worker, ROLE_WORKER)
    var next = UInt64(0)
    for _ in range(QUEUED):
        _submit(head, driver, next)
    var longest = 0
    for _ in range(ROUNDS):
        head._dispatch()
        var done = head._inflight[worker].copy()
        for obj in done:
            head._handle(worker, Frame(KIND_RESULT, obj, 0, List[UInt8]()))
            _submit(head, driver, next)
        longest = max(longest, len(head._queues[HASH].tasks))
        # What the list holds is what's queued: tasks handed out keep no
        # arguments behind, and the metric counts every byte held.
        assert_equal(_queued_args(head), head._queued_bytes)
    assert_equal(len(head._queues[HASH]), QUEUED)
    print("  longest queue list:", longest, "for", QUEUED, "queued")
    assert_true(
        longest <= 2 * QUEUED + 64,
        "the queue kept the tasks it handed out",
    )
    _ = len(peers)


def test_dispatch_leaves_one_copy_in_running() raises:
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var driver = _connect(head, peers)
    var worker = _connect(head, peers)
    _hello(head, driver, ROLE_DRIVER)
    _hello(head, worker, ROLE_WORKER)
    var obj = (_number(head, driver) << DRIVER_SHIFT) | 1
    head._handle(
        driver, Frame(KIND_TASK, obj, 7, List[UInt8](length=ARGS, fill=1))
    )
    head._dispatch()
    assert_equal(len(head._inflight[worker]), 1)
    assert_equal(len(head._running[obj].args), ARGS)
    assert_equal(len(head._queues[HASH]), 0)
    assert_equal(head._queued_bytes, 0)
    _ = len(peers)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

"""The head loop's per-iteration costs: what it rescans, and what it keeps.

These drive a `Head` directly, as the fuzzer does: peers are socket pairs,
frames go in through `_handle`, and the loop's steps run by hand.
"""

from std.ffi import c_int, external_call
from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns

from dista.codec import put_scalar
from dista.conn import Conn
from dista.frame import (
    GATHER,
    KIND_HELLO,
    KIND_RESULT,
    KIND_STORED,
    KIND_TASK,
    KIND_TASK_AFTER,
    ROLE_DRIVER,
    ROLE_OBSERVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, stream_pair
from dista.store import Location

comptime HASH: UInt64 = 0x5EED


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    return head._add_conn(Conn(pair.pop()))


def _hello(mut head: Head, i: Int, role: UInt8, port: Int = 7000) raises:
    var hello: List[UInt8] = [role]
    put_scalar[DType.uint64](hello, HASH)
    if role == ROLE_WORKER:
        hello.extend(encode_addr([127, 0, 0, 1], port))
    head._handle(i, Frame(KIND_HELLO, 0, 0, hello^))


def _id(head: Head, driver: Int, n: Int) -> UInt64:
    """Object `n` of the driver on connection `driver`."""
    for entry in head._driver_conn.items():
        if entry.value == driver:
            return (entry.key << DRIVER_SHIFT) | UInt64(n)
    return UInt64(n)


def _gather(
    mut head: Head, driver: Int, obj: UInt64, deps: List[UInt64]
) raises:
    """Submits a task gathering `deps`, which waits for them."""
    var body: List[UInt8] = [GATHER]
    put_scalar[DType.uint64](body, UInt64(len(deps)))
    for d in deps:
        put_scalar[DType.uint64](body, d)
    head._handle(driver, Frame(KIND_TASK_AFTER, obj, 9, body^))


def test_closed_connections_give_their_slots_back() raises:
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var driver = _connect(head, peers)
    _hello(head, driver, ROLE_DRIVER)
    # `dista status` every second: a connection, a hello, a hang-up.
    for _ in range(1_000):
        var mine = List[Stream]()
        var i = _connect(head, mine)
        _hello(head, i, ROLE_OBSERVER)
        head._drop(i)
        _ = len(mine)  # open until the head hangs up: no SIGPIPE
    assert_true(len(head._conns) <= 3, "every closed connection was kept")
    # A driver that leaves with results unsent leaves no buffer behind.
    var gone = _connect(head, peers)
    _hello(head, gone, ROLE_DRIVER)
    for _ in range(64):
        head._conns[gone].send(
            KIND_RESULT, 1, 0, List[UInt8](length=64 * 1024, fill=7)
        )
    assert_true(head._conns[gone].unsent() > 0)
    head._drop(gone)
    assert_equal(head._conns[gone].unsent(), 0)
    _ = len(peers)


def test_waiting_tasks_arent_rescanned_every_iteration() raises:
    """400 reducers waiting on 1,000 map outputs each, none finished: an
    iteration of the loop does nothing for them until one settles."""
    comptime MAPS = 1_000
    comptime REDUCERS = 400
    comptime ROUNDS = 2_000
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var driver = _connect(head, peers)
    _hello(head, driver, ROLE_DRIVER)
    var maps = List[UInt64]()
    for n in range(1, MAPS + 1):
        var obj = _id(head, driver, n)
        head._handle(driver, Frame(KIND_TASK, obj, 7, List[UInt8]()))
        maps.append(obj)
    for r in range(REDUCERS):
        _gather(head, driver, _id(head, driver, MAPS + 1 + r), maps)
    var t0 = perf_counter_ns()
    for _ in range(ROUNDS):
        head._release_waiting()
    var secs = Float64(perf_counter_ns() - t0) / 1e9
    print("  ", ROUNDS, "loop iterations with", REDUCERS, "waiting:", secs, "s")
    assert_equal(len(head._waiting), REDUCERS)
    # Before, each iteration checked all 400,000 inputs and copied every
    # waiting task: tens of seconds here.
    assert_true(secs < 1.0, "the loop rescans every waiting task")
    _ = len(peers)


def test_waiting_tasks_go_once_their_inputs_settle() raises:
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var driver = _connect(head, peers)
    var worker = _connect(head, peers)
    _hello(head, driver, ROLE_DRIVER)
    _hello(head, worker, ROLE_WORKER)
    var a = _id(head, driver, 1)
    var b = _id(head, driver, 2)
    var c = _id(head, driver, 3)
    for obj in [a, b, c]:
        head._handle(driver, Frame(KIND_TASK, obj, 7, List[UInt8]()))
    var both = _id(head, driver, 4)
    var twice = _id(head, driver, 5)
    var on_c = _id(head, driver, 6)
    _gather(head, driver, both, [a, b])
    _gather(head, driver, twice, [b, b])
    _gather(head, driver, on_c, [c])
    head._release_waiting()
    assert_equal(len(head._waiting), 3)
    # b stays on the worker: the tasks taking it are sent where to fetch it.
    var loc = Location([127, 0, 0, 1], 7000, "n1", "", 100_000).encode()
    head._handle(worker, Frame(KIND_STORED, b, 0, loc.copy()))
    head._release_waiting()
    assert_true(twice not in head._waiting, "its one input settled")
    assert_true(both in head._waiting, "a hasn't settled")
    head._handle(worker, Frame(KIND_RESULT, a, 0, List[UInt8]()))
    head._release_waiting()
    assert_true(both not in head._waiting)
    assert_equal(head._held_bytes(worker), 100_000)
    # An input that fails fails the task waiting on it.
    head._handle(worker, Frame(3, c, 0, List[UInt8]("boom".as_bytes())))
    head._release_waiting()
    assert_equal(len(head._waiting), 0)
    assert_true(on_c in head._errors)
    assert_equal(head._waiting_bytes, 0)
    _ = len(peers)


def test_a_span_names_its_worker_after_the_slot_is_reused() raises:
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var driver = _connect(head, peers)
    var first = _connect(head, peers)
    _hello(head, driver, ROLE_DRIVER)
    _hello(head, first, ROLE_WORKER, 7001)
    var obj = _id(head, driver, 1)
    head._handle(driver, Frame(KIND_TASK, obj, 7, List[UInt8]()))
    head._dispatch()
    head._handle(first, Frame(KIND_RESULT, obj, 0, List[UInt8]()))
    head._drop(first)
    var second = _connect(head, peers)
    _hello(head, second, ROLE_WORKER, 7002)
    var spans = head._finished[obj >> DRIVER_SHIFT].copy()
    assert_equal(len(spans), 1)
    var addr = head._book[spans[0].worker].copy()
    assert_true(addr == encode_addr([127, 0, 0, 1], 7001))
    _ = len(peers)


def main() raises:
    # Socket pairs don't set SO_NOSIGPIPE on macOS: a peer gone before the
    # head's last write must not kill the test.
    _ = external_call["signal", Int](c_int(13), Int(1))  # SIGPIPE, SIG_IGN
    TestSuite.discover_tests[__functions_in_module()]().run()

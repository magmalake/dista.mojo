"""Driver numbers across head restarts (dista#91).

Each test runs a `Head` in this process, its peers on socket pairs, and
hands it frames directly, as `fuzz/fuzz_head.mojo` does. A restarted head
is a fresh `Head` that a worker rejoins, reporting results a driver of the
head before it made.
"""

from std.os import setenv, unsetenv
from std.testing import TestSuite, assert_equal, assert_false, assert_true
from std.time import sleep

from dista.codec import get_scalar, put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_ERROR,
    KIND_FREE,
    KIND_HELLO,
    KIND_HOLDING,
    KIND_LOCATED,
    KIND_TASK,
    KIND_WELCOME,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
    message,
    read_frame,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, poll_readable, stream_pair
from dista.store import Location

comptime HASH: UInt64 = 0x5EED
comptime OLD: UInt64 = 7
"""The number a driver of the head before had."""


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    return head._add_conn(Conn(pair.pop()))


def _driver_hello(number: UInt64 = 0, epoch: UInt64 = 0) -> List[UInt8]:
    """A fresh driver's hello, or with `number` a resuming one's."""
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    if number != 0:
        put_scalar[DType.uint64](hello, number)
        if epoch != 0:
            put_scalar[DType.uint64](hello, epoch)
    return hello^


def _worker(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var i = _connect(head, peers)
    var hello: List[UInt8] = [ROLE_WORKER]
    put_scalar[DType.uint64](hello, HASH)
    hello.extend(encode_addr([127, 0, 0, 1], 7000))
    head._handle(i, Frame(KIND_HELLO, 0, 0, hello^))
    return i


def _report(mut head: Head, w: Int, obj: UInt64) raises:
    """Worker `w` rejoins holding `obj`."""
    var body = List[UInt8]()
    put_scalar[DType.uint64](body, 1)
    put_scalar[DType.uint64](body, obj)
    body.extend(Location([127, 0, 0, 1], 7000, "n", "", 100_000).encode())
    head._handle(w, Frame(KIND_HOLDING, 0, 0, body^))


def _frame(s: Stream) raises -> Frame:
    var f = read_frame(s)
    if not f:
        raise Error("the head hung up")
    return f.take()


def _welcome(mut s: Stream) raises -> Tuple[UInt64, UInt64]:
    """The driver number and epoch a welcome frame on `s` carries."""
    var f = _frame(s)
    if f.kind != KIND_WELCOME:
        raise Error("expected a welcome, got ", Int(f.kind))
    assert_equal(len(f.payload), 16)
    var pos = 0
    var n = get_scalar[DType.uint64](f.payload, pos)
    return (n, get_scalar[DType.uint64](f.payload, pos))


def _pending(s: Stream) raises -> Bool:
    return poll_readable([s.fd], 0)[0]


def test_new_driver_never_gets_old_results() raises:
    """A worker reports an old driver's result, a new driver's numbering is
    forced to start at that driver's number, and it submits the task with
    the same object ID: the task runs, rather than being answered with the
    old job's result."""
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var w = _worker(head, peers)
    var obj = (OLD << DRIVER_SHIFT) | 1
    _report(head, w, obj)
    head._next_driver = OLD
    var d = _connect(head, peers)
    head._handle(d, Frame(KIND_HELLO, 0, 0, _driver_hello()))
    var n = _welcome(peers[1])[0]
    assert_true(n != OLD, "a new driver got an old driver's number")
    var mine = (n << DRIVER_SHIFT) | 1
    head._handle(d, Frame(KIND_TASK, mine, 1, List[UInt8]()))
    assert_equal(len(head._queues[HASH]), 1, "the new task wasn't queued")
    assert_false(_pending(peers[1]), "the new driver was sent a result")
    # And forced onto the old number anyway, it still doesn't adopt it.
    head._fresh[OLD] = True
    head._driver_conn[OLD] = d
    head._handle(d, Frame(KIND_TASK, obj, 1, List[UInt8]()))
    assert_equal(len(head._queues[HASH]), 2, "the task took an orphan's result")
    assert_false(_pending(peers[1]), "the new driver was sent a result")
    # Mojo destroys a value at its last use: keep the head and the peers'
    # ends open until here.
    _ = len(head._conns)
    _ = len(peers)


def test_resuming_driver_still_claims_its_results() raises:
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var w = _worker(head, peers)
    var obj = (OLD << DRIVER_SHIFT) | 1
    _report(head, w, obj)
    var d = _connect(head, peers)
    head._handle(d, Frame(KIND_HELLO, 0, 0, _driver_hello(OLD, 12345)))
    assert_equal(_welcome(peers[1])[0], OLD)
    head._handle(d, Frame(KIND_TASK, obj, 1, List[UInt8]()))
    var f = _frame(peers[1])
    assert_equal(f.kind, KIND_LOCATED)
    assert_equal(f.object_id, obj)
    # Mojo destroys a value at its last use: keep the head and the peers'
    # ends open until here.
    _ = len(head._conns)
    _ = len(peers)


def test_other_drivers_number_is_refused() raises:
    """A driver of an earlier head resuming under a number this head gave a
    new driver is refused with a reason, and the new driver keeps it."""
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var d = _connect(head, peers)
    head._handle(d, Frame(KIND_HELLO, 0, 0, _driver_hello()))
    var w = _welcome(peers[0])
    var old = _connect(head, peers)
    head._handle(old, Frame(KIND_HELLO, 0, 0, _driver_hello(w[0], w[1] ^ 1)))
    var f = _frame(peers[1])
    assert_equal(f.kind, KIND_ERROR)
    assert_true("another driver" in message(f.payload))
    assert_equal(head._driver_conn[w[0]], d)
    # Mojo destroys a value at its last use: keep the head and the peers'
    # ends open until here.
    _ = len(head._conns)
    _ = len(peers)


def test_same_driver_replaces_its_stale_connection() raises:
    """A driver back before the head noticed its old connection go takes
    its number over: the old connection is dropped."""
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var d = _connect(head, peers)
    head._handle(d, Frame(KIND_HELLO, 0, 0, _driver_hello()))
    var w = _welcome(peers[0])
    var again = _connect(head, peers)
    head._handle(again, Frame(KIND_HELLO, 0, 0, _driver_hello(w[0], w[1])))
    assert_equal(_welcome(peers[1])[0], w[0])
    assert_equal(head._driver_conn[w[0]], again)
    assert_equal(head._roles[d], 255, "the stale connection wasn't dropped")
    # Mojo destroys a value at its last use: keep the head and the peers'
    # ends open until here.
    _ = len(head._conns)
    _ = len(peers)


def test_numbering_starts_at_random() raises:
    var a = Head(Listener(0), HASH, 2, False)
    var b = Head(Listener(0), HASH, 2, False)
    assert_true(a._epoch != b._epoch)
    assert_true(a._next_driver != b._next_driver)
    assert_true(a._next_driver >= 1 and a._next_driver < (1 << 24))


def test_unclaimed_orphans_are_freed() raises:
    _ = setenv("DISTA_ORPHAN_SECONDS", "0")
    var head = Head(Listener(0), HASH, 2, False)
    _ = unsetenv("DISTA_ORPHAN_SECONDS")
    var peers = List[Stream]()
    var w = _worker(head, peers)
    var obj = (OLD << DRIVER_SHIFT) | 1
    _report(head, w, obj)
    assert_true(obj in head._orphans)
    head._expire_orphans()
    assert_false(obj in head._orphans, "an orphan outlived its grace period")
    assert_false(obj in head._location)
    var f = _frame(peers[0])
    assert_equal(f.kind, KIND_FREE)
    assert_equal(f.object_id, obj)
    # Mojo destroys a value at its last use: keep the head and the peers'
    # ends open until here.
    _ = len(head._conns)
    _ = len(peers)


def test_orphans_wait_for_a_resuming_driver() raises:
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var w = _worker(head, peers)
    var obj = (OLD << DRIVER_SHIFT) | 1
    _report(head, w, obj)
    head._expire_orphans()
    assert_true(obj in head._orphans, "freed before its grace period")
    # Mojo destroys a value at its last use: keep the head and the peers'
    # ends open until here.
    _ = len(head._conns)
    _ = len(peers)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

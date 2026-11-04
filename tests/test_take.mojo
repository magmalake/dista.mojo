"""`take`: reading a result releases it, and the head counts what each
driver holds."""

from std.testing import TestSuite, assert_equal, assert_raises, assert_true

from dista import ObjectRef, Runtime, Seq, Task
from dista.codec import put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_HELLO,
    KIND_RELEASE,
    KIND_TASK,
    ROLE_DRIVER,
    Frame,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, stream_pair


def square(x: Int) -> Int:
    return x * x


def blob(n: Int) -> String:
    """Past `INLINE_LIMIT` it stays on its worker."""
    return String("x") * n


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    return t


comptime Square = Task[Int, Int, square]
comptime Blob = Task[Int, String, blob]
comptime Total = Task[Seq[Int], Int, total]


def test_take_returns_the_result_and_frees_it_everywhere() raises:
    comptime N = 200
    var rt = Runtime[Square, Blob](workers=2)
    var small = List[ObjectRef[Int]]()
    var big = List[ObjectRef[String]]()
    for i in range(N):
        small.append(rt.remote[Square](i))
        big.append(rt.remote[Blob](100_000 + i))
    var sum = 0
    for r in small:
        sum += rt.take(r)
    assert_equal(sum, (N - 1) * N * (2 * N - 1) // 6)
    for i in range(N):
        assert_equal(rt.take(big[i]).byte_length(), 100_000 + i)
    var s = rt.stats()
    assert_equal(s.objects, 0)
    assert_equal(s.results, 0)
    assert_equal(s.located, 0)
    # And the driver keeps nothing either.
    assert_equal(len(rt._c._results), 0)
    assert_equal(len(rt._c._located), 0)


def test_take_a_list() raises:
    var rt = Runtime[Square](workers=2)
    var refs = List[ObjectRef[Int]]()
    for i in range(10):
        refs.append(rt.remote[Square](i))
    var got = rt.take(refs)
    assert_equal(len(got), 10)
    assert_equal(got[9], 81)
    assert_equal(rt.stats().objects, 0)


def test_take_twice_raises() raises:
    var rt = Runtime[Square](workers=1)
    var r = rt.remote[Square](3)
    assert_equal(rt.take(r), 9)
    with assert_raises(contains="released"):
        _ = rt.take(r)
    with assert_raises(contains="released"):
        _ = rt.get(r)


def test_take_an_input_a_task_still_needs() raises:
    var rt = Runtime[Square, Total](workers=2)
    var maps = List[ObjectRef[Int]]()
    for i in range(50):
        maps.append(rt.remote[Square](i))
    var sum = rt.remote[Total](maps)
    var first = 0
    for r in maps:
        first += rt.take(r)
    assert_equal(rt.take(sum), first)
    assert_equal(rt.stats().objects, 0)


def _hello(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    var i = head._add_conn(Conn(pair.pop()))
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, 0)
    head._handle(i, Frame(KIND_HELLO, 0, 0, hello^))
    return i


def _number(head: Head, conn: Int) -> UInt64:
    for entry in head._driver_conn.items():
        if entry.value == conn:
            return entry.key
    return 0


def test_the_head_counts_what_each_driver_holds() raises:
    var head = Head(Listener(0), 0, 2, False)
    var peers = List[Stream]()
    var a = _hello(head, peers)
    var b = _hello(head, peers)
    var da = _number(head, a)
    var db = _number(head, b)
    for k in range(1, 4):
        var id = (da << DRIVER_SHIFT) | UInt64(k)
        head._handle(a, Frame(KIND_TASK, id, 0, List[UInt8]()))
    var idb = (db << DRIVER_SHIFT) | 1
    head._handle(b, Frame(KIND_TASK, idb, 0, List[UInt8]()))
    # Sent again, as by a driver resuming: still one object.
    head._handle(b, Frame(KIND_TASK, idb, 0, List[UInt8]()))
    assert_equal(head._held_by.get(da, 0), 3)
    assert_equal(head._held_by.get(db, 0), 1)
    assert_true("holds 3" in head._status(), head._status())
    head._handle(
        a,
        Frame(KIND_RELEASE, (da << DRIVER_SHIFT) | 2, 0, List[UInt8]()),
    )
    assert_equal(head._held_by.get(da, 0), 2)
    var js = head._status_json()
    assert_true(String('{"driver":', da, ',"held":2,"client":false}') in js, js)
    assert_true("dista_driver_held_objects_max 2" in head._page())
    # A driver that leaves holds nothing.
    head._drop(a)
    assert_equal(head._held_by.get(da, 0), 0)
    assert_equal(head._held_by.get(db, 0), 1)
    _ = len(peers)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

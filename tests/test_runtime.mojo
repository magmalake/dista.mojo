"""Runtime tests with real forked workers."""

from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Task


def square(x: Int) -> Int:
    return x * x


def fail_on_odd(x: Int) raises -> Int:
    if x % 2 == 1:
        raise Error("odd input ", x)
    return x


@fieldwise_init
struct Point(Copyable, Movable):
    var x: Float64
    var y: Float64
    var label: String


def mirror(p: Point) -> Point:
    return Point(-p.x, -p.y, p.label + " (mirrored)")


def not_registered(x: Int) -> Int:
    return x


def nap(ms: Int) -> Int:
    sleep(Float64(ms) / 1000.0)
    return ms


def text_length(s: String) -> Int:
    return s.byte_length()


comptime Square = Task[Int, Int, square]
comptime FailOnOdd = Task[Int, Int, fail_on_odd]
comptime Mirror = Task[Point, Point, mirror]
comptime NotRegistered = Task[Int, Int, not_registered]
comptime Nap = Task[Int, Int, nap]
comptime TextLength = Task[String, Int, text_length]


def test_fan_out_and_gather_1000_tasks() raises:
    """The phase-1 gate: 1,000 tasks across 4 worker processes."""
    var rt = Runtime[Square](workers=4)
    var refs = List[ObjectRef[Int]]()
    for i in range(1000):
        refs.append(rt.remote[Square](i))
    var total = 0
    for r in refs:
        total += rt.get(r)
    # Sum of squares 0..999 = 999 * 1000 * 1999 / 6.
    assert_equal(total, 332833500)


def test_struct_argument_and_result() raises:
    var rt = Runtime[Square, Mirror](workers=2)
    var r = rt.remote[Mirror](Point(1.5, -2.0, "p"))
    var p = rt.get(r)
    assert_equal(p.x, -1.5)
    assert_equal(p.y, 2.0)
    assert_equal(p.label, "p (mirrored)")
    assert_equal(rt.get(rt.remote[Square](9)), 81)


def test_task_error_reaches_driver() raises:
    var rt = Runtime[FailOnOdd](workers=2)
    var good = rt.remote[FailOnOdd](4)
    var bad = rt.remote[FailOnOdd](3)
    assert_equal(rt.get(good), 4)
    with assert_raises(contains="odd input 3"):
        _ = rt.get(bad)
    # The worker survives a raising task.
    assert_equal(rt.get(rt.remote[FailOnOdd](8)), 8)


def test_get_twice() raises:
    var rt = Runtime[Square](workers=1)
    var r = rt.remote[Square](5)
    assert_equal(rt.get(r), 25)
    assert_equal(rt.get(r), 25)


def test_unregistered_task_raises() raises:
    var rt = Runtime[Square](workers=1)
    with assert_raises(contains="not registered"):
        _ = rt.remote[NotRegistered](1)


def test_fast_task_not_blocked_by_slow_one() raises:
    """Results arrive in any order: a short task doesn't wait behind a long one.
    """
    var rt = Runtime[Nap](workers=2, pipeline=1)
    var slow = rt.remote[Nap](600)
    var fast = rt.remote[Nap](0)
    var t0 = perf_counter_ns()
    assert_equal(rt.get(fast), 0)
    var waited_ms = Float64(perf_counter_ns() - t0) / 1e6
    assert_true(waited_ms < 300, "fast task waited behind the slow one")
    assert_false(rt.ready(slow))
    assert_equal(rt.get(slow), 600)
    assert_true(rt.ready(slow))


def test_pipeline_depths() raises:
    for depth in [1, 2, 4]:
        var rt = Runtime[Square](workers=3, pipeline=depth)
        var refs = List[ObjectRef[Int]]()
        for i in range(300):
            refs.append(rt.remote[Square](i))
        var total = 0
        for r in refs:
            total += rt.get(r)
        assert_equal(total, 8955050)  # sum of squares 0..299


def test_large_arguments_with_pipelining() raises:
    """Arguments bigger than a socket buffer don't deadlock a pipelined worker.
    """
    var rt = Runtime[TextLength](workers=2, pipeline=4)
    var big = String("x") * (1 << 20)
    var refs = List[ObjectRef[Int]]()
    for _ in range(12):
        refs.append(rt.remote[TextLength](big))
    for r in refs:
        assert_equal(rt.get(r), 1 << 20)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

"""Runtime tests with real forked workers."""

from std.testing import TestSuite, assert_equal, assert_raises

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


comptime Square = Task[Int, Int, square]
comptime FailOnOdd = Task[Int, Int, fail_on_odd]
comptime Mirror = Task[Point, Point, mirror]
comptime NotRegistered = Task[Int, Int, not_registered]


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


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

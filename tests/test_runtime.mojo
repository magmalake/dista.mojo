"""Runtime tests with real forked workers."""

from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Seq, Task
from dista.socket import exit_now


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


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    return t


def increment(x: Int) -> Int:
    return x + 1


def describe(p: Point) -> String:
    return String(p.label, "@", Int(p.x), ",", Int(p.y))


def die(x: Int) -> Int:
    exit_now(9)
    return x


def blob(n: Int) -> String:
    return String("x") * n


def total_length(xs: Seq[String]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i].byte_length()
    return t


def grow(s: String) -> String:
    return s + String("y") * 1000


comptime Blob = Task[Int, String, blob]
comptime TotalLength = Task[Seq[String], Int, total_length]
comptime Grow = Task[String, String, grow]
comptime Total = Task[Seq[Int], Int, total]
comptime Die = Task[Int, Int, die]
comptime Increment = Task[Int, Int, increment]
comptime Describe = Task[Point, String, describe]
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


def test_map_reduce() raises:
    """The PR-2 gate: 100 map tasks feed one reduce."""
    var rt = Runtime[Square, Total](workers=4)
    var maps = List[ObjectRef[Int]]()
    for i in range(100):
        maps.append(rt.remote[Square](i))
    var reduced = rt.remote[Total](maps)
    assert_equal(rt.get(reduced), 328350)  # sum of squares 0..99


def test_chain_of_refs() raises:
    var rt = Runtime[Increment](workers=2)
    var r = rt.remote[Increment](0)
    for _ in range(20):
        r = rt.remote[Increment](r)
    assert_equal(rt.get(r), 21)


def test_struct_result_feeds_next_task() raises:
    var rt = Runtime[Mirror, Describe](workers=2)
    var m = rt.remote[Mirror](Point(3.0, 4.0, "p"))
    assert_equal(rt.get(rt.remote[Describe](m)), "p (mirrored)@-3,-4")


def test_failed_input_fails_dependents() raises:
    var rt = Runtime[FailOnOdd, Increment, Total](workers=2)
    var bad = rt.remote[FailOnOdd](5)
    var after = rt.remote[Increment](bad)
    with assert_raises(contains="input failed: odd input 5"):
        _ = rt.get(after)
    var refs: List[ObjectRef[Int]] = [rt.remote[FailOnOdd](2), bad]
    with assert_raises(contains="input failed"):
        _ = rt.get(rt.remote[Total](refs))


def test_seq_argument_directly() raises:
    var rt = Runtime[Total](workers=1)
    assert_equal(rt.get(rt.remote[Total](Seq[Int]([1, 2, 3, 4]))), 10)


def test_worker_death_fails_its_task() raises:
    """A worker that dies fails its in-flight task; the others carry on."""
    var rt = Runtime[Square, Die](workers=2, pipeline=1)
    var doomed = rt.remote[Die](1)
    with assert_raises(contains="worker lost"):
        _ = rt.get(doomed)
    assert_equal(rt.get(rt.remote[Square](12)), 144)


def test_large_result_stays_on_its_worker() raises:
    var rt = Runtime[Blob](workers=2)
    var small = rt.remote[Blob](100)
    var big = rt.remote[Blob](300_000)
    assert_equal(rt.location(small), "")
    assert_true(rt.location(big) != "", "a 300 KB result should stay put")
    assert_equal(rt.get(big).byte_length(), 300_000)
    assert_equal(rt.get(small).byte_length(), 100)


def test_map_outputs_reach_reduce_peer_to_peer() raises:
    """The PR-4 gate: large map outputs never pass through the head."""
    var rt = Runtime[Blob, TotalLength](workers=4)
    var maps = List[ObjectRef[String]]()
    for i in range(12):
        maps.append(rt.remote[Blob](200_000 + i))
    var reduced = rt.remote[TotalLength](maps)
    var expected = 12 * 200_000 + 66  # 0 + 1 + ... + 11
    assert_equal(rt.get(reduced), expected)
    for r in maps:
        assert_true(rt.location(r) != "", "map output went through the head")


def test_mixed_inline_and_stored_inputs() raises:
    var rt = Runtime[Blob, TotalLength](workers=3)
    var refs: List[ObjectRef[String]] = [
        rt.remote[Blob](10),
        rt.remote[Blob](150_000),
        rt.remote[Blob](20),
        rt.remote[Blob](250_000),
    ]
    assert_equal(rt.get(rt.remote[TotalLength](refs)), 400_030)


def test_chain_of_large_results_across_workers() raises:
    """Each step fetches the previous step's result from another worker."""
    var rt = Runtime[Blob, Grow](workers=3, pipeline=1)
    var r = rt.remote[Grow](rt.remote[Blob](100_000))
    for _ in range(9):
        r = rt.remote[Grow](r)
    assert_equal(rt.get(r).byte_length(), 110_000)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

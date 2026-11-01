"""Actors: create, ordered calls, results as ObjectRefs, kill."""

from std.testing import (
    TestSuite,
    assert_equal,
    assert_raises,
    assert_true,
)

from dista import Actor, Method, ObjectRef, Runtime, Task


@fieldwise_init
struct Counter(Movable):
    var total: Int


def new_counter(start: Int) raises -> Counter:
    if start < 0:
        raise Error("a counter can't start below zero")
    return Counter(start)


def add(mut c: Counter, n: Int) -> Int:
    c.total += n
    return c.total


def pad(mut c: Counter, n: Int) -> String:
    """A result too large to travel through the head."""
    return String("x") * (n + c.total)


def square(x: Int) -> Int:
    return x * x


comptime CounterActor = Actor[Counter, Int, new_counter]
comptime Add = Method[CounterActor, Int, Int, add]
comptime Pad = Method[CounterActor, Int, String, pad]
comptime Square = Task[Int, Int, square]
comptime RT = Runtime[CounterActor, Add, Pad, Square]


def test_1000_calls_in_order() raises:
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](0)
    var refs = List[ObjectRef[Int]](capacity=1000)
    for _ in range(1000):
        refs.append(rt.call[Add](c, 1))
    for i in range(1000):
        assert_equal(rt.get(refs[i]), i + 1)
    rt.kill(c)


def test_a_task_uses_a_method_result() raises:
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](5)
    var r = rt.call[Add](c, 2)
    assert_equal(rt.get(rt.remote[Square](r)), 49)


def test_large_result_stays_on_the_actor_worker() raises:
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](0)
    var r = rt.call[Pad](c, 100_000)
    assert_true(rt.location(r) != "")
    assert_equal(rt.get(r).byte_length(), 100_000)


def test_two_actors_keep_their_own_state() raises:
    var rt = RT(workers=3)
    var a = rt.create[CounterActor](0)
    var b = rt.create[CounterActor](100)
    for _ in range(10):
        _ = rt.call[Add](a, 1)
        _ = rt.call[Add](b, 2)
    assert_equal(rt.get(rt.call[Add](a, 0)), 10)
    assert_equal(rt.get(rt.call[Add](b, 0)), 120)


def test_a_failed_constructor_fails_every_call() raises:
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](-1)
    var r = rt.call[Add](c, 1)
    with assert_raises(contains="can't start below zero"):
        _ = rt.get(r)
    with assert_raises(contains="can't start below zero"):
        _ = rt.get(rt.call[Add](c, 1))
    # Its worker went back to tasks.
    assert_equal(rt.get(rt.remote[Square](3)), 9)


def test_kill_frees_the_worker() raises:
    var rt = RT(workers=1)
    var c = rt.create[CounterActor](0)
    var r = rt.call[Add](c, 4)
    rt.kill(c)
    # The call sent before the kill still ran.
    assert_equal(rt.get(r), 4)
    # The only worker runs tasks again.
    assert_equal(rt.get(rt.remote[Square](6)), 36)
    with assert_raises(contains="was killed"):
        _ = rt.call[Add](c, 1)
    # A new actor takes the worker.
    var d = rt.create[CounterActor](1)
    assert_equal(rt.get(rt.call[Add](d, 1)), 2)


def test_tasks_run_beside_an_actor() raises:
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](0)
    var squares = List[ObjectRef[Int]]()
    for i in range(100):
        squares.append(rt.remote[Square](i))
        _ = rt.call[Add](c, i)
    var total = 0
    for r in squares:
        total += rt.get(r)
    assert_equal(total, 328350)
    assert_equal(rt.get(rt.call[Add](c, 0)), 4950)
    var spans = rt.spans()
    var called = 0
    for ref sp in spans:
        if sp.task.find("add") >= 0:
            called += 1
    assert_true(called >= 100)


def test_released_call_results_leave_nothing_at_the_head() raises:
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](0)
    var refs = List[ObjectRef[Int]]()
    for _ in range(50):
        refs.append(rt.call[Add](c, 1))
    assert_equal(rt.get(refs[49]), 50)
    rt.release(refs)
    rt.kill(c)
    assert_equal(rt.stats().objects, 0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

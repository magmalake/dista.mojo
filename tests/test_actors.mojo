"""Actors: create, ordered calls, results as ObjectRefs, kill."""

from std.os import remove
from std.os.path import exists
from std.testing import (
    TestSuite,
    assert_equal,
    assert_raises,
    assert_true,
)

from dista.socket import exit_now
from dista import (
    Actor,
    ActorHandle,
    Client,
    CtxMethod,
    CtxTask,
    Method,
    ObjectRef,
    Runtime,
    Task,
)


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


comptime CRASH_FLAG = "build/test-actors-crashed"


def add_or_crash(mut c: Counter, n: Int) raises -> Int:
    """`add`, except that the worker dies the first time the count reaches
    500: the flag file says it already has, so the replay goes through."""
    c.total += n
    if c.total == 500 and not exists(CRASH_FLAG):
        with open(CRASH_FLAG, "w") as f:
            f.write("crashed")
        exit_now(1)
    return c.total


def pad(mut c: Counter, n: Int) -> String:
    """A result too large to travel through the head."""
    return String("x") * (n + c.total)


def square(x: Int) -> Int:
    return x * x


comptime CounterActor = Actor[Counter, Int, new_counter]
comptime Add = Method[CounterActor, Int, Int, add]
comptime AddOrCrash = Method[CounterActor, Int, Int, add_or_crash]
comptime Pad = Method[CounterActor, Int, String, pad]
comptime Square = Task[Int, Int, square]


def bump(mut c: Client, h: ActorHandle[CounterActor]) raises -> Int:
    """A task that calls the actor it was handed, and returns its answer."""
    var r = c.call[Add](h, 1)
    var n = c.get(r)
    c.release(r)
    return n


def pad_length(mut c: Client, h: ActorHandle[CounterActor]) raises -> Int:
    """A large method result, fetched from the actor's worker by a task."""
    var r = c.call[Pad](h, 100_000)
    var n = c.get(r).byte_length()
    c.release(r)
    return n


@fieldwise_init
struct Relay(Movable):
    """An actor that forwards to a counter: what it hands on, it counts."""

    var counter: ActorHandle[CounterActor]
    var forwarded: Int


def new_relay(counter: ActorHandle[CounterActor]) -> Relay:
    return Relay(counter, 0)


def forward(mut r: Relay, mut c: Client, n: Int) raises -> Int:
    """Calls the counter from inside an actor's method."""
    r.forwarded += 1
    var ref_ = c.call[Add](r.counter, n)
    var got = c.get(ref_)
    c.release(ref_)
    return got


comptime RelayActor = Actor[Relay, ActorHandle[CounterActor], new_relay]
comptime Forward = CtxMethod[RelayActor, Int, Int, forward]
comptime Bump = CtxTask[ActorHandle[CounterActor], Int, bump]
comptime PadLength = CtxTask[ActorHandle[CounterActor], Int, pad_length]
comptime RT = Runtime[
    CounterActor,
    Add,
    AddOrCrash,
    Pad,
    Square,
    Bump,
    PadLength,
    RelayActor,
    Forward,
]


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


def test_100_tasks_share_one_actor() raises:
    """PR 5's gate: 100 tasks, each handed the same actor, each call it once.
    The actor sees every call exactly once, and each task gets back a count
    no other task got."""
    var rt = RT(workers=4)
    var c = rt.create[CounterActor](0)
    var refs = List[ObjectRef[Int]]()
    for _ in range(100):
        refs.append(rt.remote[Bump](c))
    var seen = List[Bool](length=101, fill=False)
    for r in refs:
        var n = rt.get(r)
        assert_true(n >= 1 and n <= 100, String("count ", n))
        assert_true(not seen[n], String("count ", n, " twice"))
        seen[n] = True
    assert_equal(rt.get(rt.call[Add](c, 0)), 100)


def test_a_task_fetches_a_large_method_result() raises:
    """A method's result too large for the head comes to the task from the
    actor's worker."""
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](0)
    assert_equal(rt.get(rt.remote[PadLength](c)), 100_000)


def test_a_task_calling_a_killed_actor_fails() raises:
    var rt = RT(workers=2)
    var c = rt.create[CounterActor](0)
    rt.kill(c)
    with assert_raises(contains="no such actor"):
        _ = rt.get(rt.remote[Bump](c))


def test_an_actor_calls_another() raises:
    """A relay actor's method calls the counter actor; the counter sees each
    forwarded call once, in order."""
    var rt = RT(workers=3)
    var counter = rt.create[CounterActor](0)
    var relay = rt.create[RelayActor](counter)
    var last = 0
    for i in range(1, 21):
        last = rt.get(rt.call[Forward](relay, i))
    assert_equal(last, 210)
    assert_equal(rt.get(rt.call[Add](counter, 0)), 210)


def test_an_actor_restarts_when_its_worker_dies() raises:
    """PR 6's gate: the counter's worker dies after 500 of 1,000 calls. The
    actor is built again on another worker and its calls replayed: every call
    gets its answer once, in order, and the final count is 1,000."""
    if exists(CRASH_FLAG):
        remove(CRASH_FLAG)
    var rt = RT(workers=3)
    var c = rt.create[CounterActor](0)
    var refs = List[ObjectRef[Int]](capacity=1000)
    for _ in range(1000):
        refs.append(rt.call[AddOrCrash](c, 1))
    for i in range(1000):
        assert_equal(rt.get(refs[i]), i + 1)
    assert_true(exists(CRASH_FLAG), "the worker never died")
    assert_equal(rt.get(rt.call[Add](c, 0)), 1000)
    # Tasks still run beside the restarted actor.
    assert_equal(rt.get(rt.remote[Square](12)), 144)
    remove(CRASH_FLAG)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

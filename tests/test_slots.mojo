"""`DISTA_WORKER_SLOTS`: a worker running several tasks at once, in local
mode with real forked workers. Each test sets the variable before its
`Runtime` forks the workers, which inherit it.

Tasks that need to say where they ran write a mark, `build/slots-marks/<tag>-
<task>-<pid>-<n>`, as they start: how many marks a task left is how many
times it ran, and the pid names its worker."""

from std.ffi import c_int, external_call
from std.os import listdir, makedirs, remove, setenv, unsetenv
from std.os.path import exists
from std.testing import (
    TestSuite,
    assert_equal,
    assert_raises,
    assert_true,
)
from std.time import perf_counter_ns, sleep

from dista import Actor, Method, ObjectRef, Runtime, Task

comptime MARKS = "build/slots-marks"


def _pid() -> Int:
    return Int(external_call["getpid", c_int]())


def _mark(tag: Int, task: Int) raises:
    """Records that `task` started here."""
    var n = 0
    while True:
        var path = String(MARKS, "/", tag, "-", task, "-", _pid(), "-", n)
        if not exists(path):
            with open(path, "w") as f:
                f.write("ran")
            return
        n += 1


struct _Marks(Movable):
    """The marks of tag `tag`: per task, the pids it ran on."""

    var pids: List[List[Int]]

    def __init__(out self, tag: Int, tasks: Int) raises:
        self.pids = List[List[Int]]()
        for _ in range(tasks):
            self.pids.append(List[Int]())
        var prefix = String(tag, "-")
        for name in listdir(MARKS):
            if not name.startswith(prefix):
                continue
            var parts = name.split("-")
            var task = Int(parts[1])
            if task < tasks:
                self.pids[task].append(Int(parts[2]))

    def started(self) -> Int:
        var n = 0
        for i in range(len(self.pids)):
            if len(self.pids[i]) > 0:
                n += 1
        return n


def _clear(tag: Int) raises:
    if not exists(MARKS):
        makedirs(MARKS)
    var prefix = String(tag, "-")
    for name in listdir(MARKS):
        if name.startswith(prefix):
            remove(String(MARKS, "/", name))


def nap(ms: Int) -> Int:
    sleep(Float64(ms) / 1000.0)
    return ms


def spin(n: Int) -> Int:
    """About `n` million steps of an LCG: CPU-bound, single-threaded."""
    var x = UInt64(n)
    for _ in range(n * 1_000_000):
        x = x * 6364136223846793005 + 1442695040888963407
    return Int(x >> 1) & 0xFFFF


@fieldwise_init
struct Mark(Copyable, Movable):
    var tag: Int
    var task: Int
    var ms: Int


def marked_nap(m: Mark) raises -> Int:
    """Marks, naps, and returns the task's number times ten."""
    _mark(m.tag, m.task)
    sleep(Float64(m.ms) / 1000.0)
    return m.task * 10


def big(n: Int) -> String:
    """A result too large to go through the head: stays in the store."""
    sleep(0.2)
    return String("y") * (100_000 + n)


comptime Nap = Task[Int, Int, nap]
comptime Spin = Task[Int, Int, spin]
comptime MarkedNap = Task[Mark, Int, marked_nap]
comptime Big = Task[Int, String, big]


@fieldwise_init
struct Probe(Movable):
    var busy: Bool
    var overlaps: Int
    var calls: Int


def new_probe(n: Int) -> Probe:
    return Probe(False, 0, n)


def visit(mut p: Probe, ms: Int) -> Int:
    """Naps inside the call; a second call running meanwhile would find the
    state busy."""
    if p.busy:
        p.overlaps += 1
    p.busy = True
    sleep(Float64(ms) / 1000.0)
    p.busy = False
    p.calls += 1
    return p.overlaps


comptime ProbeActor = Actor[Probe, Int, new_probe]
comptime Visit = Method[ProbeActor, Int, Int, visit]


def _seconds_since(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e9


def _slots(n: Int):
    _ = setenv("DISTA_WORKER_SLOTS", String(n))


def _wait_started(tag: Int, tasks: Int, want: Int) raises -> _Marks:
    """Waits (up to 20 s) until `want` of the tag's tasks have started."""
    var t0 = perf_counter_ns()
    while True:
        var m = _Marks(tag, tasks)
        if m.started() >= want:
            return m^
        if _seconds_since(t0) > 20.0:
            raise Error("only ", m.started(), " of ", want, " tasks started")
        sleep(0.02)


def _wait_exit(pid: Int, seconds: Float64) -> Int:
    """The wait status of child `pid` once it exits, or -1 past `seconds`."""
    var status = Array[c_int, 1](fill=0)
    var t0 = perf_counter_ns()
    while _seconds_since(t0) < seconds:
        var r = external_call["waitpid", c_int](
            c_int(pid), status.unsafe_ptr(), c_int(1)  # WNOHANG
        )
        if r == c_int(pid):
            return Int(status[0])
        sleep(0.05)
    return -1


def test_one_slot_runs_one_at_a_time() raises:
    """The default, 1: a worker runs its tasks one after another, as before
    slots."""
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var rt = Runtime[Nap](workers=1, pipeline=2)
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for _ in range(6):
        refs.append(rt.remote[Nap](200))
    for r in refs:
        assert_equal(rt.get(r), 200)
    var took = _seconds_since(t0)
    print("  6 naps of 0.2 s, 1 worker, 1 slot:", took, "s")
    assert_true(took >= 1.15, String("tasks overlapped with one slot: ", took))


def test_slots_run_sleeping_tasks_at_once() raises:
    """8 naps of 0.5 s on one worker with 4 slots take about 1 s, not 4."""
    _slots(4)
    var rt = Runtime[Nap](workers=1, pipeline=1)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for i in range(8):
        refs.append(rt.remote[Nap](500 + i))
    for i in range(8):
        assert_equal(rt.get(refs[i]), 500 + i)
    var took = _seconds_since(t0)
    print("  8 naps of 0.5 s, 1 worker, 4 slots:", took, "s")
    assert_true(took >= 0.95, String("more than 4 ran at once: ", took))
    assert_true(took < 2.0, String("the slots didn't overlap: ", took))


def test_slots_run_cpu_bound_tasks_on_several_cores() raises:
    """CPU-bound tasks on 4 slots finish well before one slot's time."""
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var n = 150
    var want = spin(n)
    var serial: Float64
    var parallel: Float64
    var rt1 = Runtime[Spin](workers=1, pipeline=1)
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for _ in range(8):
        refs.append(rt1.remote[Spin](n))
    for r in refs:
        assert_equal(rt1.get(r), want)
    serial = _seconds_since(t0)
    _ = rt1^
    _slots(4)
    var rt4 = Runtime[Spin](workers=1, pipeline=1)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    t0 = perf_counter_ns()
    refs.clear()
    for _ in range(8):
        refs.append(rt4.remote[Spin](n))
    for r in refs:
        assert_equal(rt4.get(r), want)
    parallel = _seconds_since(t0)
    print(
        "  8 CPU-bound tasks, 1 worker: 1 slot",
        serial,
        "s, 4 slots",
        parallel,
        "s",
    )
    assert_true(
        parallel < serial / 1.5,
        String("4 slots weren't faster: ", serial, " s against ", parallel),
    )


def test_slots_keep_results_and_errors_straight() raises:
    """Many tasks across 2 workers of 3 slots, small and large results mixed:
    every answer is its own."""
    _slots(3)
    var rt = Runtime[Nap, Big](workers=2, pipeline=2)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var naps = List[ObjectRef[Int]]()
    var bigs = List[ObjectRef[String]]()
    for i in range(60):
        naps.append(rt.remote[Nap](i % 7))
        if i % 6 == 0:
            bigs.append(rt.remote[Big](i))
    for i in range(60):
        assert_equal(rt.take(naps[i]), i % 7)
    for k in range(len(bigs)):
        var s = rt.take(bigs[k])
        assert_equal(s.byte_length(), 100_000 + k * 6)


def test_a_worker_killed_with_tasks_running_has_them_all_run_again() raises:
    """2 workers of 4 slots, 8 tasks running; one worker is killed. Its 4
    tasks run again on the other, once each, and the other's 4 ran once."""
    var tag = 1
    _clear(tag)
    _slots(4)
    var rt = Runtime[MarkedNap](workers=2, pipeline=1)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var refs = List[ObjectRef[Int]]()
    for i in range(8):
        refs.append(rt.remote[MarkedNap](Mark(tag, i, 1500)))
    var m = _wait_started(tag, 8, 8)
    var victim = m.pids[0][0]
    var on_victim = 0
    for i in range(8):
        if m.pids[i][0] == victim:
            on_victim += 1
    assert_equal(on_victim, 4, "each worker should be running 4 tasks")
    _ = external_call["kill", c_int](c_int(victim), c_int(9))
    for i in range(8):
        assert_equal(rt.get(refs[i]), i * 10)
    var after = _Marks(tag, 8)
    for i in range(8):
        ref pids = after.pids[i]
        if m.pids[i][0] == victim:
            assert_equal(len(pids), 2, String("task ", i, " ran ", len(pids)))
            assert_true(
                (pids[0] == victim) != (pids[1] == victim),
                "a task ran again on the killed worker",
            )
        else:
            assert_equal(len(pids), 1, String("task ", i, " ran ", len(pids)))
    _clear(tag)


def test_cancel_with_tasks_running() raises:
    """Cancelling tasks running on slots answers them `cancelled`; the rest
    finish, and the slots take new work."""
    var tag = 2
    _clear(tag)
    _slots(4)
    var rt = Runtime[MarkedNap](workers=1, pipeline=1)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var refs = List[ObjectRef[Int]]()
    for i in range(4):
        refs.append(rt.remote[MarkedNap](Mark(tag, i, 800)))
    _ = _wait_started(tag, 4, 4)
    rt.cancel(refs[1])
    rt.cancel(refs[3])
    for i in range(4):
        if i == 1 or i == 3:
            with assert_raises(contains="cancelled"):
                _ = rt.get(refs[i])
        else:
            assert_equal(rt.get(refs[i]), i * 10)
    var more = List[ObjectRef[Int]]()
    for i in range(4):
        more.append(rt.remote[MarkedNap](Mark(tag, 4 + i, 10)))
    for i in range(4):
        assert_equal(rt.get(more[i]), (4 + i) * 10)
    var after = _Marks(tag, 8)
    for i in range(8):
        assert_equal(len(after.pids[i]), 1, String("task ", i))
    _clear(tag)


def test_drain_with_tasks_running() raises:
    """SIGTERM to a worker running 4 tasks: it finishes all 4 (none runs
    again), then leaves by itself, exiting 0; new tasks go to the other."""
    var tag = 3
    _clear(tag)
    _slots(4)
    var rt = Runtime[MarkedNap](workers=2, pipeline=1)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var refs = List[ObjectRef[Int]]()
    for i in range(8):
        refs.append(rt.remote[MarkedNap](Mark(tag, i, 1000)))
    var m = _wait_started(tag, 8, 8)
    var victim = m.pids[0][0]
    _ = external_call["kill", c_int](c_int(victim), c_int(15))
    for i in range(8):
        assert_equal(rt.get(refs[i]), i * 10)
    var status = _wait_exit(victim, 10.0)
    assert_equal(status, 0, "the drained worker didn't leave cleanly")
    var more = List[ObjectRef[Int]]()
    for i in range(4):
        more.append(rt.remote[MarkedNap](Mark(tag, 8 + i, 10)))
    for i in range(4):
        assert_equal(rt.get(more[i]), (8 + i) * 10)
    var after = _Marks(tag, 12)
    for i in range(12):
        assert_equal(len(after.pids[i]), 1, String("task ", i))
        if i >= 8:
            assert_true(after.pids[i][0] != victim, "the drained worker ran")
    _clear(tag)


def test_an_actors_calls_still_run_one_at_a_time() raises:
    """With 4 slots an actor's calls still run one after another, in
    order: none finds another running."""
    _slots(4)
    var rt = Runtime[ProbeActor, Visit, Nap](workers=2, pipeline=2)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var p = rt.create[ProbeActor](0)
    var t0 = perf_counter_ns()
    var calls = List[ObjectRef[Int]]()
    for _ in range(6):
        calls.append(rt.call[Visit](p, 100))
    var naps = List[ObjectRef[Int]]()
    for _ in range(8):
        naps.append(rt.remote[Nap](100))
    for r in calls:
        assert_equal(rt.get(r), 0, "two calls ran at once")
    for r in naps:
        assert_equal(rt.get(r), 100)
    var took = _seconds_since(t0)
    assert_true(took >= 0.55, String("the calls overlapped: ", took))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

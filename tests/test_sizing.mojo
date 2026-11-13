"""`DISTA_WORKER_SLOTS=auto`: slots and a memory budget from the run's grant.

- The sizing's arithmetic: slots from the grant's CPUs and memory, shared by
  the workers of a local-mode run; the room a budget leaves.
- The head honours a worker's `KIND_CAPACITY`, sending it no more than that
  plus `pipeline - 1`.
- Mixed versions: a worker tells only a head that announced it reads
  `KIND_CAPACITY`, so an older head, which would drop it for the frame, never
  gets one; and the announcement is a `KIND_FREE` of object 0, which a
  worker from before frees as nothing.
- End to end, a worker over its budget runs one task at a time, and one sized
  from the grant's memory runs as many as fit.
"""

from std.ffi import c_int, external_call
from std.os import setenv, unsetenv
from std.sys.info import CompilationTarget
from std.testing import TestSuite, assert_equal, assert_false, assert_true
from std.time import perf_counter_ns, sleep

from threads import num_cpus

from dista import ObjectRef, Runtime, Task
from dista.client import TaskContext
from dista.codec import get_scalar, put_scalar
from dista.conn import Conn
from dista.executors import worker_slots
from dista.frame import (
    KIND_CAPACITY,
    KIND_FREE,
    KIND_HELLO,
    KIND_TASK,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
)
from dista.head import DRIVER_SHIFT, Head
from dista.sizing import (
    CAPACITY_ANNOUNCE,
    InFlight,
    Learned,
    Sizing,
    memory_pressure,
    parse_pressure,
    resident_bytes,
)
from dista.socket import Listener, Stream, stream_pair
from dista.task import StatePtr
from dista.worker import Worker

comptime HASH: UInt64 = 0x5EED
comptime MB = 1 << 20


def _unset_all():
    _ = unsetenv("DISTA_WORKER_SLOTS")
    _ = unsetenv("MORCA_GRANT_CPUS")
    _ = unsetenv("MORCA_GRANT_MEMORY_MB")
    _ = unsetenv("DISTA_TASK_MEMORY_MB")
    _ = unsetenv("DISTA_STORE_BYTES")


def _grant(cpus: Int, memory_mb: Int):
    _ = setenv("DISTA_WORKER_SLOTS", "auto")
    _ = setenv("MORCA_GRANT_CPUS", String(cpus))
    _ = setenv("MORCA_GRANT_MEMORY_MB", String(memory_mb))


def test_slots_come_from_the_grant() raises:
    var s = Sizing.of(8, 4096, 0, 2)
    assert_equal(s.slots, 4)
    assert_equal(s.budget, 2048 * MB)
    assert_equal(s.store_limit(), 1024 * MB)
    # A task of 1 GiB: two fit in 2 GiB, though there are four CPUs.
    assert_equal(Sizing.of(8, 4096, 1024, 2).slots, 2)
    # No grant: a slot per core, and no budget.
    var none = Sizing.of(0, 0, 0, 1)
    assert_equal(none.slots, num_cpus())
    assert_equal(none.budget, 0)
    assert_equal(none.store_limit(), 0)
    # Never below one, however thin the share.
    assert_equal(Sizing.of(1, 64, 512, 4).slots, 1)


def test_worker_slots_reads_auto() raises:
    _grant(6, 0)
    assert_equal(worker_slots(), 6)
    assert_equal(worker_slots(3), 2)
    _unset_all()


def test_the_room_a_budget_leaves() raises:
    # A budget of 1000, 100 of it the worker's own, tasks of 300: three.
    var s = Sizing(4, 1000, 300, 100, 0, False, 0)
    assert_equal(s.room(0, 100), 3)
    assert_true(s.fits(2, 100))
    assert_false(s.fits(3, 100))
    # Three ran at once and left 1000 resident: 300 each, as said, so
    # memory they freed and the allocator kept doesn't count against them.
    s.started(3)
    s.finished()
    assert_equal(s.room(0, 1000), 3)
    # A footprint of 1300 says 400 each, more than given: learned, so with
    # the margin, 765 of 900, one fits.
    assert_equal(s.room(2, 1300), 1)
    assert_false(s.fits(2, 1300))
    # One task always may run.
    assert_true(s.fits(0, 1 << 40))
    assert_equal(s.room(0, 1 << 40), 1)
    # No size: one at a time until one has finished, then by its footprint.
    var open = Sizing(4, 1000, 0, 100, 0, False, 0)
    assert_equal(open.room(0, 100), 1)
    open.started(1)
    assert_equal(open.room(1, 400), 1)
    open.finished()
    # 300 learned: 765 of 900 holds two.
    assert_equal(open.room(0, 400), 2)
    # No budget: every slot.
    assert_equal(Sizing(4, 0, 0, 0, 0, False, 0).room(1, 1 << 40), 4)


def test_a_learned_size_keeps_a_margin() raises:
    # Nothing given, 200 learned from a task alone: 85% of 900 is 765,
    # three of 200.
    var s = Sizing(8, 1000, 0, 100, 0, False, 200)
    assert_equal(s.room(0, 100), 3)
    # Given 300, and nothing larger learned: the whole 900, three of 300.
    assert_equal(Sizing(8, 1000, 300, 100, 0, False, 200).room(0, 100), 3)
    # Given 150, learned 200: the learned one, with the margin.
    assert_equal(Sizing(8, 1000, 150, 100, 0, False, 200).room(0, 100), 3)


def test_what_a_task_took_alone_is_learned() raises:
    var l = Learned()
    # Kind 7 alone, from 100 to 600: 500.
    l.started(1, 7, 100)
    l.sample(600)
    l.finished(1, 600)
    assert_equal(l.peak[7], 500)
    # Again alone, reusing what the first freed: nothing added; 500 stays.
    l.started(2, 7, 600)
    l.finished(2, 600)
    assert_equal(l.peak[7], 500)
    # Two at once: neither is learned from.
    l.started(3, 9, 600)
    l.started(4, 9, 600)
    l.sample(2000)
    l.finished(3, 2000)
    l.finished(4, 2000)
    assert_false(9 in l.peak)
    assert_equal(l.most(), 500)


def _window(
    mut c: InFlight,
    mut now: UInt64,
    rate: Float64,
    latency: Float64,
    fit: Int = 64,
    spills: Int = 0,
    pressure: Float64 = -1.0,
    busy: Bool = True,
) raises -> Bool:
    """Runs one window of `c` at `rate` tasks a second, each taking
    `latency` seconds; whether it changed the count."""
    var n = max(3, 2 * c.limit)
    c.saw(c.limit if busy else 0, now, spills)
    for _ in range(n):
        c.finished(UInt64(latency * 1e9))
    now += UInt64(Float64(n) / rate * 1e9)
    assert_true(c.due(now))
    return c.close(now, spills, pressure, fit)


def test_in_flight_starts_at_one_and_doubles_while_throughput_rises() raises:
    var c = InFlight(16)
    var now = UInt64(1)
    assert_equal(c.limit, 1)
    assert_true(_window(c, now, 10.0, 0.1))
    assert_equal(c.limit, 2)
    assert_true(_window(c, now, 20.0, 0.1))
    assert_equal(c.limit, 4)
    assert_true(_window(c, now, 40.0, 0.1))
    assert_equal(c.limit, 8)
    # Flat: it holds, and the slow start is over.
    assert_false(_window(c, now, 41.0, 0.1))
    assert_equal(c.limit, 8)
    assert_false(c.slow_start)
    # Rising again: one at a time now.
    assert_true(_window(c, now, 50.0, 0.1))
    assert_equal(c.limit, 9)


def test_in_flight_grows_only_when_busy_and_within_the_budget() raises:
    var c = InFlight(16)
    var now = UInt64(1)
    # Idle: the count wasn't reached, so more wouldn't have run.
    assert_false(_window(c, now, 10.0, 0.1, busy=False))
    assert_equal(c.limit, 1)
    # The budget fits three: doubling stops there.
    assert_true(_window(c, now, 10.0, 0.1, fit=3))
    assert_true(_window(c, now, 20.0, 0.1, fit=3))
    assert_equal(c.limit, 3)
    # Memory shrinks to two: the count drops at once, not at a window's end.
    assert_true(c.cap(2))
    assert_equal(c.limit, 2)
    assert_true(c.why.startswith("memory"))


def test_in_flight_backs_off() raises:
    var c = InFlight(16)
    var now = UInt64(1)
    for r in [10.0, 20.0, 40.0, 80.0]:
        _ = _window(c, now, r, 0.1)
    assert_equal(c.limit, 16)
    # Memory pressure: a quarter off.
    assert_true(_window(c, now, 80.0, 0.1, pressure=25.0))
    assert_equal(c.limit, 12)
    assert_true(c.why.startswith("memory pressure"))
    # Results spilled.
    assert_true(_window(c, now, 80.0, 0.1, spills=1))
    assert_equal(c.limit, 9)
    # Tasks twice as slow, throughput flat.
    assert_true(_window(c, now, 80.0, 0.2, spills=1))
    assert_equal(c.limit, 7)
    assert_true(c.why.startswith("tasks slowed"))
    # Never below one.
    var one = InFlight(4)
    var t = UInt64(1)
    assert_false(_window(one, t, 10.0, 0.1, pressure=50.0))
    assert_equal(one.limit, 1)


def test_pressure_is_parsed_or_absent() raises:
    var text = String(
        "some avg10=12.50 avg60=3.00 avg300=1.00 total=123\n"
        "full avg10=1.00 avg60=0.00 avg300=0.00 total=4\n"
    )
    assert_equal(parse_pressure(text), 12.5)
    var p = memory_pressure()
    comptime if CompilationTarget.is_macos():
        assert_equal(p, -1.0)
    else:
        assert_true(p >= -1.0)


def test_resident_memory_is_read() raises:
    assert_true(resident_bytes() > MB, "no resident memory read")


def _worker_hello(slots: Int) -> List[UInt8]:
    var hello: List[UInt8] = [ROLE_WORKER]
    put_scalar[DType.uint64](hello, HASH)
    hello.extend(encode_addr([127, 0, 0, 1], 7000))
    put_scalar[DType.uint64](hello, UInt64(slots))
    return hello^


def _frames(mut c: Conn) raises -> List[Frame]:
    """The frames waiting on `c`."""
    var out = List[Frame]()
    sleep(0.05)
    c.read_available()
    while True:
        var f = c.next_frame()
        if not f:
            break
        out.append(f.take())
    return out^


def test_the_head_sends_only_what_a_worker_has_room_for() raises:
    var head = Head(Listener(0), HASH, 1, False)
    var dpair = stream_pair()
    var driver_end = dpair.pop()
    var driver = head._add_conn(Conn(dpair.pop()))
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(driver, Frame(KIND_HELLO, 0, 0, hello^))
    var wpair = stream_pair()
    var worker_end = Conn(wpair.pop())
    var w = head._add_conn(Conn(wpair.pop()))
    head._handle(w, Frame(KIND_HELLO, 0, 0, _worker_hello(4)))
    # The head announces it reads KIND_CAPACITY: a free of object 0.
    head._conns[w].flush()
    var got = _frames(worker_end)
    assert_equal(len(got), 1)
    assert_equal(got[0].kind, KIND_FREE)
    assert_equal(got[0].object_id, CAPACITY_ANNOUNCE)
    # The worker has room for one: of six tasks it gets one (pipeline 1).
    var body = List[UInt8]()
    put_scalar[DType.uint64](body, 1)
    head._handle(w, Frame(KIND_CAPACITY, 0, 0, body^))
    var number = UInt64(0)
    for entry in head._driver_conn.items():
        number = entry.key
    for n in range(1, 7):
        var obj = (number << DRIVER_SHIFT) | UInt64(n)
        head._handle(driver, Frame(KIND_TASK, obj, 9, List[UInt8]()))
    head._dispatch()
    assert_equal(len(head._inflight[w]), 1)
    # Room for three: two more go.
    var three = List[UInt8]()
    put_scalar[DType.uint64](three, 3)
    head._handle(w, Frame(KIND_CAPACITY, 0, 0, three^))
    head._dispatch()
    assert_equal(len(head._inflight[w]), 3)
    # Never past its slots, whatever it says.
    var many = List[UInt8]()
    put_scalar[DType.uint64](many, 99)
    head._handle(w, Frame(KIND_CAPACITY, 0, 0, many^))
    assert_equal(head._admit[w], 4)
    _ = driver_end^  # open until here: no SIGPIPE
    _ = worker_end.fd()


def _head_with_workers(
    mut head: Head, mut ends: List[Conn], slots: List[Int]
) raises -> Tuple[Int, List[Int]]:
    """A driver and workers with `slots` each on `head`: the driver's
    connection and the workers'."""
    var dpair = stream_pair()
    ends.append(Conn(dpair.pop()))
    var driver = head._add_conn(Conn(dpair.pop()))
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(driver, Frame(KIND_HELLO, 0, 0, hello^))
    var workers = List[Int]()
    for n in slots:
        var wpair = stream_pair()
        ends.append(Conn(wpair.pop()))
        var w = head._add_conn(Conn(wpair.pop()))
        head._handle(w, Frame(KIND_HELLO, 0, 0, _worker_hello(n)))
        workers.append(w)
    return (driver, workers^)


def _submit(mut head: Head, driver: Int, n: Int) raises:
    var number = UInt64(0)
    for entry in head._driver_conn.items():
        number = entry.key
    var obj = (number << DRIVER_SHIFT) | UInt64(n)
    head._handle(driver, Frame(KIND_TASK, obj, 9, List[UInt8]()))


def test_a_tie_goes_to_the_worker_with_the_most_room() raises:
    var head = Head(Listener(0), HASH, 1, False)
    var ends = List[Conn]()
    var made = _head_with_workers(head, ends, [1, 4])
    var driver = made[0]
    var w = made[1].copy()
    # Both idle; the second has room for four: the one task goes there,
    # though the first connected first.
    _submit(head, driver, 1)
    head._dispatch()
    assert_equal(len(head._inflight[w[0]]), 0)
    assert_equal(len(head._inflight[w[1]]), 1)
    _ = len(ends)


def test_ties_rotate_between_dispatches() raises:
    var head = Head(Listener(0), HASH, 1, False)
    var ends = List[Conn]()
    var made = _head_with_workers(head, ends, [1, 1, 1])
    var driver = made[0]
    var w = made[1].copy()
    # Three equal workers, a task per dispatch, each settled before the
    # next: they don't all go to the first.
    var took = List[Int](length=3, fill=0)
    for n in range(1, 7):
        _submit(head, driver, n)
        head._dispatch()
        for k in range(3):
            if len(head._inflight[w[k]]) > 0:
                took[k] += 1
                head._inflight[w[k]].clear()
    for k in range(3):
        assert_equal(took[k], 2)
    _ = len(ends)


# A worker with no tasks, to see what it tells its head.
def _dispatch(
    t: UInt64, a: List[UInt8], mut o: List[UInt8], mut c: TaskContext
) raises:
    raise Error("no tasks")


def _create(t: UInt64, a: List[UInt8]) raises -> StatePtr:
    raise Error("no actors")


def _call(
    t: UInt64,
    m: UInt64,
    s: StatePtr,
    a: List[UInt8],
    mut o: List[UInt8],
    mut c: TaskContext,
) raises:
    raise Error("no actors")


def _destroy(t: UInt64, s: StatePtr):
    pass


def _save(t: UInt64, s: StatePtr, mut o: List[UInt8]) raises:
    raise Error("no actors")


def _restore(t: UInt64, a: List[UInt8]) raises -> StatePtr:
    raise Error("no actors")


comptime BareWorker = Worker[
    _dispatch, _create, _call, _destroy, _save, _restore
]


def test_a_worker_tells_only_a_head_that_reads_it() raises:
    # Four CPUs and 1 MB: less than the worker's resident memory, so it has
    # room for one task.
    _grant(4, 1)
    var pair = stream_pair()
    var head_end = Conn(pair.pop())
    var w = BareWorker(pair.pop(), HASH)
    assert_equal(w._store_limit, 512 * 1024)
    # An older head: it never announces, and would drop a worker for a
    # frame it doesn't know. The worker holds tasks back but says nothing.
    w._tell_room()
    w._head.flush()
    var got = _frames(head_end)
    assert_equal(len(got), 1)
    assert_equal(got[0].kind, KIND_HELLO)
    # A head that reads it announces, and hears the worker's room at once,
    # then only when it changes.
    w._from_head(Frame(KIND_FREE, CAPACITY_ANNOUNCE, 0, List[UInt8]()))
    w._tell_room()
    w._head.flush()
    got = _frames(head_end)
    assert_equal(len(got), 1)
    assert_equal(got[0].kind, KIND_CAPACITY)
    var pos = 0
    assert_equal(Int(get_scalar[DType.uint64](got[0].payload, pos)), 1)
    # A rejoin, to a head that may be older: silent until it announces.
    var pair2 = stream_pair()
    var head2 = Conn(pair2.pop())
    w.rejoin(pair2.pop(), HASH)
    w._tell_room()
    w._head.flush()
    for f in _frames(head2):
        assert_true(f.kind != KIND_CAPACITY, "told a head that didn't ask")
    _ = head_end.fd()
    _ = head2.fd()
    _unset_all()


def test_a_worker_from_before_takes_the_announcement_as_nothing() raises:
    """A worker with a number of slots behaves as dista 0.4.0's: the
    announcement frees object 0, which it doesn't hold, and it never sends
    `KIND_CAPACITY`."""
    _unset_all()
    _ = setenv("DISTA_WORKER_SLOTS", "2")
    var pair = stream_pair()
    var head_end = Conn(pair.pop())
    var w = BareWorker(pair.pop(), HASH)
    w._from_head(Frame(KIND_FREE, CAPACITY_ANNOUNCE, 0, List[UInt8]()))
    w._tell_room()
    w._head.flush()
    var got = _frames(head_end)
    assert_equal(len(got), 1)
    assert_equal(got[0].kind, KIND_HELLO)
    assert_false(w._head.closed)
    _ = head_end.fd()
    _unset_all()


def nap(ms: Int) -> Int:
    sleep(Float64(ms) / 1000.0)
    return ms


comptime Nap = Task[Int, Int, nap]


def _seconds_since(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e9


def _four_naps(var rt: Runtime[Nap]) raises -> Float64:
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for _ in range(4):
        refs.append(rt.remote[Nap](300))
    for r in refs:
        assert_equal(rt.get(r), 300)
    return _seconds_since(t0)


def test_a_worker_over_its_budget_runs_one_task_at_a_time() raises:
    _grant(4, 1)
    var took = _four_naps(Runtime[Nap](workers=1, pipeline=2))
    _unset_all()
    print("  4 naps of 0.3 s, 4 slots, over the budget:", took, "s")
    assert_true(
        took >= 1.15, String("tasks overlapped past the budget: ", took)
    )


def test_a_worker_runs_what_the_grants_memory_fits() raises:
    # 8.5 GB, 4 GB a task and the worker's own memory: two slots, though
    # there are four CPUs. In flight starts at one: three alone, then two at
    # a time.
    _grant(4, 8704)
    _ = setenv("DISTA_TASK_MEMORY_MB", "4096")
    var rt = Runtime[Nap](workers=1, pipeline=2)
    var t0 = perf_counter_ns()
    var refs = List[ObjectRef[Int]]()
    for _ in range(10):
        refs.append(rt.remote[Nap](200))
    for r in refs:
        assert_equal(rt.get(r), 200)
    var took = _seconds_since(t0)
    _unset_all()
    print("  10 naps of 0.2 s, 2 slots from the grant:", took, "s")
    assert_true(took < 1.8, String("two didn't run at once: ", took))


def churn(mb: Int) -> Int:
    """Fills `mb` MB, and holds it a moment."""
    var n = mb << 20
    var buf = List[UInt8](length=n, fill=0)
    for i in range(0, n, 4096):
        buf[i] = UInt8(i >> 12)
    sleep(0.05)
    return Int(buf[n - 4096])


comptime Churn = Task[Int, Int, churn]


def _children_peak_mb() -> Int:
    """The largest resident peak of the children waited for, in MB."""
    var buf = List[UInt8](length=256, fill=0)
    _ = external_call["getrusage", c_int](c_int(-1), buf.unsafe_ptr())
    var v = 0
    for k in range(8):
        v |= Int(buf[32 + k]) << (8 * k)  # ru_maxrss, after two timevals
    comptime if CompilationTarget.is_macos():
        return v >> 20
    else:
        return v >> 10


def _churn_mix(tasks: Int) raises:
    var rt = Runtime[Churn](workers=1, pipeline=2)
    var refs = List[ObjectRef[Int]]()
    for k in range(tasks):
        refs.append(rt.remote[Churn](16 if k % 2 == 0 else 128))
    for r in refs:
        _ = rt.take(r)


def test_a_memory_heavy_mix_stays_inside_its_grant() raises:
    """No size given: the slow start learns what a task takes running alone,
    and the worker keeps to the grant. Eight slots of 128 MB would be 1 GB;
    the grant is 640 MB."""
    _grant(8, 640)
    _churn_mix(32)
    _unset_all()
    var peak = _children_peak_mb()
    print(
        "  16 and 128 MB tasks, 8 CPUs, grant 640 MB: worker peak", peak, "MB"
    )
    assert_true(peak <= 640, String("the worker passed its grant: ", peak))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

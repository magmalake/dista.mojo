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

from std.os import setenv, unsetenv
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
from dista.sizing import CAPACITY_ANNOUNCE, Sizing, resident_bytes
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
    var s = Sizing(4, 1000, 300, 100)
    assert_equal(s.room(0, 100), 3)
    # Memory freed tasks left resident doesn't count against them.
    assert_equal(s.room(2, 900), 3)
    assert_true(s.fits(2, 900))
    assert_false(s.fits(3, 900))
    # Past the budget, none more; one task always may run.
    assert_equal(s.room(2, 5000), 2)
    assert_false(s.fits(2, 5000))
    assert_true(s.fits(0, 5000))
    assert_equal(s.room(0, 5000), 1)
    # No size: one more at a time while memory is under the budget.
    var open = Sizing(4, 1000, 0, 0)
    assert_equal(open.room(1, 500), 2)
    assert_equal(open.room(1, 1001), 1)
    # No budget: every slot.
    assert_equal(Sizing(4, 0, 0, 0).room(1, 1 << 40), 4)


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
    # 8.5 GB, 4 GB a task and the worker's own memory: two at a time,
    # though there are four CPUs.
    _grant(4, 8704)
    _ = setenv("DISTA_TASK_MEMORY_MB", "4096")
    var took = _four_naps(Runtime[Nap](workers=1, pipeline=2))
    _unset_all()
    print("  4 naps of 0.3 s, 2 slots from the grant:", took, "s")
    assert_true(took >= 0.55, String("more than two ran at once: ", took))
    assert_true(took < 1.1, String("two didn't run at once: ", took))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

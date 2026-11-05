"""A worker's task slots, alone: the hand-off between the loop and the
executors (`executors.mojo`), without a head or a worker."""

from std.os import setenv, unsetenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)
from std.time import perf_counter_ns, sleep

from threads import num_cpus

from dista.client import TaskContext
from dista.executors import Executors, Job, worker_slots
from dista.frame import KIND_TASK
from dista.socket import POLLIN, poll


def work(
    task_id: UInt64,
    args: List[UInt8],
    mut out: List[UInt8],
    mut ctx: TaskContext,
) raises:
    """Task 1 naps `args[0]` tens of ms and echoes its arguments; task 2
    raises; task 3 echoes them backwards."""
    if task_id == 2:
        raise Error("task 2 always fails")
    if task_id == 1:
        sleep(Float64(Int(args[0])) / 100.0)
    if task_id == 3:
        for i in range(len(args) - 1, -1, -1):
            out.append(args[i])
        return
    out.extend(Span(args))


def _wait_all(mut ex: Executors[work], n: Int) raises -> List[Job]:
    """Polls the wake descriptor until `n` jobs are back, as the worker's
    loop does."""
    var got = List[Job]()
    while len(got) < n:
        var rev = poll([ex.wake_fd()], [POLLIN], 10_000)
        if rev[0] == 0:
            raise Error("no job came back in 10 s")
        var done = ex.take_done()
        while len(done) > 0:
            got.append(done.pop(0))
    return got^


def _seconds_since(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e9


def test_jobs_run_at_once() raises:
    """Four jobs of 0.3 s on four slots take about 0.3 s, not 1.2."""
    var ex = Executors[work](4)
    assert_equal(ex.slots(), 4)
    var t0 = perf_counter_ns()
    for i in range(4):
        var slot = ex.free_slot()
        assert_true(slot >= 0, "a slot should be free")
        ex.start(slot, Job(UInt64(i), 1, KIND_TASK, 0, [UInt8(30), UInt8(i)]))
    assert_equal(ex.free_slot(), -1, "every slot is running")
    assert_equal(ex.running(), 4)
    var got = _wait_all(ex, 4)
    var took = _seconds_since(t0)
    print("  4 jobs of 0.3 s on 4 slots:", took, "s")
    assert_true(took < 0.9, String("the jobs ran one after another: ", took))
    assert_equal(ex.running(), 0)
    assert_true(ex.free_slot() >= 0)
    var seen = List[Bool](length=4, fill=False)
    for k in range(len(got)):
        ref j = got[k]
        assert_true(j.ok, j.error)
        assert_equal(len(j.out), 2)
        assert_equal(Int(j.out[1]), Int(j.object_id))
        assert_true(j.ended >= j.started and j.started > 0)
        assert_equal(len(j.args), 0, "a job's arguments are freed as it ends")
        seen[Int(j.object_id)] = True
    for i in range(4):
        assert_true(seen[i], String("job ", i, " never came back"))


def test_a_job_comes_back_with_its_error() raises:
    var ex = Executors[work](2)
    ex.start(0, Job(10, 2, KIND_TASK, 0, [UInt8(1)]))
    ex.start(1, Job(11, 3, KIND_TASK, 0, [UInt8(1), UInt8(2), UInt8(3)]))
    var got = _wait_all(ex, 2)
    for k in range(len(got)):
        ref j = got[k]
        if j.object_id == 10:
            assert_false(j.ok)
            assert_equal(j.error, "task 2 always fails")
            assert_equal(len(j.out), 0)
        else:
            assert_true(j.ok, j.error)
            assert_equal(len(j.out), 3)
            assert_equal(Int(j.out[0]), 3)
            assert_equal(Int(j.out[2]), 1)


def test_job_fields_come_back_whole() raises:
    """What the loop needs to report a job travels with it, and its epoch
    too, so the loop can drop one for a head that's gone."""
    var ex = Executors[work](1)
    ex.start(0, Job(42, 3, KIND_TASK, 1234, [UInt8(7)], many=3, epoch=5))
    var got = _wait_all(ex, 1)
    ref j = got[0]
    assert_equal(j.object_id, 42)
    assert_equal(j.task_id, 3)
    assert_equal(j.arrived, 1234)
    assert_equal(j.many, 3)
    assert_equal(j.epoch, 5)


def test_slots_are_reused_many_times() raises:
    """2,000 jobs through 3 slots: each comes back once, with its own bytes;
    a slot is only handed out when the loop has taken its job back."""
    var ex = Executors[work](3)
    var n = 2000
    var next = 0
    var back = List[Int](length=n, fill=0)
    var returned = 0
    while returned < n:
        while next < n:
            var slot = ex.free_slot()
            if slot < 0:
                break
            var args = List[UInt8]()
            args.append(UInt8(next & 0xFF))
            args.append(UInt8(next >> 8))
            ex.start(slot, Job(UInt64(next), 3, KIND_TASK, 0, args^))
            next += 1
        assert_true(ex.running() <= 3)
        _ = poll([ex.wake_fd()], [POLLIN], 10_000)
        var done = ex.take_done()
        while len(done) > 0:
            var j = done.pop(0)
            assert_true(j.ok, j.error)
            var i = Int(j.object_id)
            # Reversed by task 3: high byte first.
            assert_equal(Int(j.out[0]) << 8 | Int(j.out[1]), i)
            back[i] += 1
            returned += 1
    for i in range(n):
        assert_equal(back[i], 1, String("job ", i, " came back ", back[i]))
    assert_equal(ex.running(), 0)


def test_stopping_idle_slots_joins_them() raises:
    """Dropping the slots with nothing running joins every executor."""
    var t0 = perf_counter_ns()
    for _ in range(20):
        var ex = Executors[work](4)
        ex.start(0, Job(1, 3, KIND_TASK, 0, [UInt8(1)]))
        _ = _wait_all(ex, 1)
        _ = ex^
    assert_true(_seconds_since(t0) < 5.0)


def test_stopping_with_a_job_running_doesnt_wait() raises:
    """Leaving with a task running (a drain past its deadline) can't wait
    for it: the executors are left to the process's exit."""
    var ex = Executors[work](2)
    ex.start(0, Job(1, 1, KIND_TASK, 0, [UInt8(200)]))  # 2 s
    sleep(0.05)
    var t0 = perf_counter_ns()
    _ = ex^
    var took = _seconds_since(t0)
    assert_true(took < 0.5, String("waited for the running job: ", took))


def test_worker_slots_reads_the_environment() raises:
    _ = unsetenv("DISTA_WORKER_SLOTS")
    assert_equal(worker_slots(), 1)
    _ = setenv("DISTA_WORKER_SLOTS", "6")
    assert_equal(worker_slots(), 6)
    _ = setenv("DISTA_WORKER_SLOTS", "0")
    assert_equal(worker_slots(), num_cpus())
    _ = setenv("DISTA_WORKER_SLOTS", "-1")
    with assert_raises(contains="0 or more"):
        _ = worker_slots()
    _ = setenv("DISTA_WORKER_SLOTS", "four")
    with assert_raises(contains="must be a number"):
        _ = worker_slots()
    _ = unsetenv("DISTA_WORKER_SLOTS")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

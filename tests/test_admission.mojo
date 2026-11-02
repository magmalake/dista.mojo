"""Admission control: a full queue blocks `remote` until workers drain it.

Each test sets a low limit before its `Runtime` starts, so the head forked in
local mode reads it, and submits slow tasks with 100 KB arguments, 20 MB in
all: more than the sockets between driver and head buffer. With the limit,
the submit loop can't finish before the workers have run most of the tasks,
so it takes at least as long as they need. A bound from below, not above, so
a fast machine can't make it fail.
"""

from std.os import setenv, unsetenv
from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Seq, Task

comptime N = 200
comptime SIZE = 100_000
comptime NAP = 0.02
"""Each slow task's seconds."""


def slow(s: String) -> Int:
    sleep(NAP)
    return s.byte_length()


def add_one(x: Int) -> Int:
    return x + 1


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for x in xs.items:
        t += x
    return t


comptime Slow = Task[String, Int, slow]
comptime AddOne = Task[Int, Int, add_one]
comptime Total = Task[Seq[Int], Int, total]


def _submit_slow_tasks() raises -> Float64:
    """Submits `N` slow tasks to 2 workers, one in flight each; checks every
    answer, and returns how long the submit loop took, in seconds."""
    var rt = Runtime[Slow, AddOne, Total](workers=2, pipeline=1)
    var arg = String("x") * SIZE
    var refs = List[ObjectRef[Int]]()
    var t0 = perf_counter_ns()
    for _ in range(N):
        refs.append(rt.remote[Slow](arg))
    var took = Float64(perf_counter_ns() - t0) / 1e9
    for r in refs:
        assert_equal(rt.get(r), SIZE)
    return took


def _bound() -> Float64:
    """The least the submit loop can take with a small limit: of `N` tasks,
    the queue holds a handful and the sockets at most 8 MB, 80 tasks; the
    rest wait for 2 workers at `NAP` seconds a task. Halved for slack."""
    return Float64(N - 10 - 80) / 2 * NAP / 2


def test_a_full_queue_blocks_submission() raises:
    _ = setenv("DISTA_MAX_QUEUED", "8")
    var took = _submit_slow_tasks()
    _ = unsetenv("DISTA_MAX_QUEUED")
    print("  submitting took", took, "s; at least", _bound())
    assert_true(took >= _bound(), "remote didn't wait for the queue to drain")


def test_queued_bytes_block_submission() raises:
    _ = setenv("DISTA_MAX_QUEUED", "0")
    _ = setenv("DISTA_MAX_QUEUED_BYTES", String(10 * SIZE))
    var took = _submit_slow_tasks()
    _ = unsetenv("DISTA_MAX_QUEUED")
    _ = unsetenv("DISTA_MAX_QUEUED_BYTES")
    print("  submitting took", took, "s; at least", _bound())
    assert_true(took >= _bound(), "remote didn't wait for the queue to drain")


def test_tasks_waiting_on_inputs_still_drain() raises:
    """Tasks waiting on others count toward the limit, and still run: a
    chain of 100, and a gather of them all, with room for only 4."""
    _ = setenv("DISTA_MAX_QUEUED", "4")
    var rt = Runtime[Slow, AddOne, Total](workers=2)
    var chain = List[ObjectRef[Int]]()
    chain.append(rt.remote[AddOne](0))
    for i in range(1, 100):
        chain.append(rt.remote[AddOne](chain[i - 1]))
    var all = rt.remote[Total](chain)
    _ = unsetenv("DISTA_MAX_QUEUED")
    assert_equal(rt.get(chain[99]), 100)
    assert_equal(rt.get(all), 5050)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

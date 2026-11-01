"""The task timeline: head and worker stamps per task, and the trace file."""

from std.os import makedirs
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_true
from std.time import sleep

from dista import ObjectRef, Runtime, Task, TaskSpan


def nap(ms: Int) -> Int:
    sleep(Float64(ms) / 1000.0)
    return ms


def blob(n: Int) -> String:
    return String("x") * n


def length(s: String) -> Int:
    return s.byte_length()


def fail(x: Int) raises -> Int:
    raise Error("no")


comptime Nap = Task[Int, Int, nap]
comptime Blob = Task[Int, String, blob]
comptime Length = Task[String, Int, length]
comptime Fail = Task[Int, Int, fail]


def _in_order(sp: TaskSpan) raises:
    """Head stamps in order, worker stamps in order, the worker's inside the
    head's. One machine, one clock, so these hold exactly."""
    assert_true(sp.submitted <= sp.queued, "queued before submitted")
    assert_true(sp.queued <= sp.dispatched, "dispatched before queued")
    assert_true(sp.dispatched <= sp.arrived, "arrived before dispatched")
    assert_true(sp.arrived <= sp.started, "started before arrived")
    assert_true(sp.started <= sp.ended, "ended before started")
    assert_true(sp.ended <= sp.settled, "settled before ended")


def test_every_task_has_a_span_inside_submit_and_settle() raises:
    """The PR-3 gate, minus opening Perfetto: one track per worker, and every
    task's run sits inside its submit and settle."""
    var rt = Runtime[Nap, Blob, Length, Fail](workers=4)
    var refs = List[ObjectRef[Int]]()
    for _ in range(200):
        refs.append(rt.remote[Nap](2))
    var big = rt.remote[Blob](300_000)
    var n = rt.remote[Length](big)
    for r in refs:
        _ = rt.get(r)
    assert_equal(rt.get(n), 300_000)

    var spans = rt.spans()
    assert_equal(len(spans), 202)
    var workers = Dict[String, Bool]()
    var naps = 0
    for ref sp in spans:
        _in_order(sp)
        workers[sp.worker] = True
        if "nap" in sp.task:
            naps += 1
    assert_equal(naps, 200)
    assert_true(len(workers) >= 2, String("ran on ", len(workers), " workers"))
    # The length task fetched its input: the wait shows on its worker.
    for ref sp in spans:
        if "length" in sp.task:
            assert_true(sp.started >= sp.arrived)
    assert_equal(len(rt.spans()), 0, "spans are handed over once")


def test_timeline_file() raises:
    if not exists("build"):
        makedirs("build")
    var rt = Runtime[Nap, Blob, Length, Fail](workers=3)
    var refs = List[ObjectRef[Int]]()
    for i in range(30):
        refs.append(rt.remote[Nap](i % 3))
    for r in refs:
        _ = rt.get(r)
    assert_equal(rt.timeline("build/test-timeline.json"), 30)
    var text = open("build/test-timeline.json", "r").read()
    assert_true(text.startswith("[\n"))
    assert_true(text.endswith("]\n"))
    assert_equal(text.count('"ph":"X"') - text.count('"name":"inputs"'), 30)
    assert_true(text.count('"process_name"') >= 2)


def test_a_task_that_never_ran() raises:
    """A task whose input failed settles without a worker."""
    var rt = Runtime[Nap, Blob, Length, Fail](workers=2)
    var bad = rt.remote[Fail](1)
    var after = rt.remote[Nap](rt.remote[Nap](1))
    var dep = rt.remote[Length](rt.remote[Blob](10))
    _ = rt.get(after)
    _ = rt.get(dep)
    try:
        _ = rt.get(bad)
    except:
        pass
    var spans = rt.spans()
    assert_equal(len(spans), 5)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

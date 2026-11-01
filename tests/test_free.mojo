"""Freeing results: reference counts at the head, `release`, worker deletes."""

from std.os import listdir, makedirs, setenv, unsetenv
from std.os.path import exists
from std.testing import (
    TestSuite,
    assert_equal,
    assert_raises,
    assert_true,
)
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Seq, Task


def blob(n: Int) -> String:
    return String("x") * n


def length(s: String) -> Int:
    return s.byte_length()


def total_length(xs: Seq[String]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i].byte_length()
    return t


comptime Blob = Task[Int, String, blob]
comptime Length = Task[String, Int, length]
comptime TotalLength = Task[Seq[String], Int, total_length]


def _store(name: String) raises -> String:
    """A node-store directory of this test's own, set for the workers."""
    var dir = String("build/test-free-", name)
    if not exists(dir):
        makedirs(dir)
    _ = setenv("DISTA_STORE_DIR", dir)
    return dir^


def _files(dir: String) raises -> Int:
    var n = 0
    for name in listdir(dir):
        if name.startswith("dista-"):
            n += 1
    return n


def _drained(dir: String) raises -> Int:
    """The store's file count once frees have landed, or after 10 s."""
    var deadline = Int(perf_counter_ns()) + 10_000_000_000
    var n = _files(dir)
    while n > 0 and Int(perf_counter_ns()) < deadline:
        sleep(0.05)
        n = _files(dir)
    return n


def test_10000_stored_results_leave_nothing_behind() raises:
    """The PR-1 gate: every result is freed, at the head and on its worker."""
    var dir = _store("gate")
    try:
        var rt = Runtime[Blob](workers=4)
        var peak = 0
        for _ in range(100):
            var refs = List[ObjectRef[String]]()
            for _ in range(100):
                refs.append(rt.remote[Blob](128 * 1024))
            for r in refs:
                assert_equal(rt.get(r).byte_length(), 128 * 1024)
            peak = max(peak, _files(dir))
            rt.release(refs)
        var s = rt.stats()
        assert_equal(s.objects, 0)
        assert_equal(s.located, 0)
        assert_equal(s.results, 0)
        assert_equal(_drained(dir), 0)
        assert_true(peak <= 100, String("store held ", peak, " results"))
    finally:
        _ = unsetenv("DISTA_STORE_DIR")


def test_small_results_are_freed_at_the_head() raises:
    var rt = Runtime[Blob](workers=2)
    var refs = List[ObjectRef[String]]()
    for i in range(50):
        refs.append(rt.remote[Blob](i))
    for r in refs:
        _ = rt.get(r)
    assert_equal(rt.stats().results, 50)
    rt.release(refs)
    var s = rt.stats()
    assert_equal(s.results, 0)
    assert_equal(s.objects, 0)


def test_released_input_still_reaches_its_task() raises:
    """A task already given a ref keeps it alive past the driver's release."""
    var dir = _store("input")
    try:
        var rt = Runtime[Blob, Length](workers=2)
        var big = rt.remote[Blob](300_000)
        var n = rt.remote[Length](big)
        rt.release(big)
        assert_equal(rt.get(n), 300_000)
        rt.release(n)
        assert_equal(rt.stats().objects, 0)
        assert_equal(_drained(dir), 0)
    finally:
        _ = unsetenv("DISTA_STORE_DIR")


def test_gather_holds_every_input() raises:
    var rt = Runtime[Blob, TotalLength](workers=3)
    var maps = List[ObjectRef[String]]()
    for i in range(8):
        maps.append(rt.remote[Blob](100_000 + i))
    var reduced = rt.remote[TotalLength](maps)
    rt.release(maps)
    assert_equal(rt.get(reduced), 8 * 100_000 + 28)
    rt.release(reduced)
    assert_equal(rt.stats().objects, 0)


def test_release_before_the_result_arrives() raises:
    var dir = _store("early")
    try:
        var rt = Runtime[Blob](workers=2)
        var big = rt.remote[Blob](500_000)
        rt.release(big)
        var marker = rt.remote[Blob](1)
        _ = rt.get(marker)  # the big task ran before or beside it
        rt.release(marker)
        assert_equal(rt.stats().objects, 0)
        assert_equal(_drained(dir), 0)
    finally:
        _ = unsetenv("DISTA_STORE_DIR")


def test_get_after_release_raises() raises:
    var rt = Runtime[Blob](workers=1)
    var r = rt.remote[Blob](10)
    _ = rt.get(r)
    rt.release(r)
    rt.release(r)  # a second release does nothing
    with assert_raises(contains="released"):
        _ = rt.get(r)


def test_task_on_a_released_ref_fails() raises:
    var rt = Runtime[Blob, Length](workers=1)
    var r = rt.remote[Blob](10)
    _ = rt.get(r)
    rt.release(r)
    var n = rt.remote[Length](r)
    with assert_raises(contains="released"):
        _ = rt.get(n)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

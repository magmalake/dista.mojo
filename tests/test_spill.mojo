"""Spilling: past `DISTA_STORE_BYTES`, a worker writes large results to
`DISTA_SPILL_DIR` instead of the node store, and they read the same."""

from std.os import listdir, makedirs, remove, setenv, unsetenv
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_true

from dista import ObjectRef, Runtime, Task

comptime SPILL = "build/test-spill-dir"
comptime SIZE = 300_000


def blob(i: Int) -> String:
    """A result too large for the head, different for each `i`."""
    return String(i % 10) * (SIZE + i)


def length(s: String) -> Int:
    return s.byte_length()


comptime Blob = Task[Int, String, blob]
comptime Length = Task[String, Int, length]


def _spilled() raises -> Int:
    """How many results sit in the spill directory."""
    var n = 0
    for name in listdir(SPILL):
        if name.startswith("dista-") and not name.endswith(".tmp"):
            n += 1
    return n


def test_results_past_the_limit_spill_and_read_the_same() raises:
    if not exists(SPILL):
        makedirs(SPILL)
    for name in listdir(SPILL):
        if name.startswith("dista-"):
            remove(String(SPILL, "/", name))
    # Room in the store for one result per worker; the rest spill.
    _ = setenv("DISTA_STORE_BYTES", String(SIZE + 100_000))
    _ = setenv("DISTA_SPILL_DIR", SPILL)
    var rt = Runtime[Blob, Length](workers=2)
    _ = unsetenv("DISTA_STORE_BYTES")
    _ = unsetenv("DISTA_SPILL_DIR")
    var refs = List[ObjectRef[String]]()
    for i in range(8):
        refs.append(rt.remote[Blob](i))
    var spilled = 0
    for r in refs:
        if SPILL in rt.location(r):
            spilled += 1
    # Each worker keeps its first result in the store.
    assert_true(spilled >= 6, String("only ", spilled, " of 8 spilled"))
    assert_equal(_spilled(), spilled)
    for i in range(8):
        # Read by the driver, and by a task on either worker.
        assert_equal(rt.get(refs[i]).byte_length(), SIZE + i)
        assert_equal(rt.get(rt.remote[Length](refs[i])), SIZE + i)
    rt.release(refs)
    _ = rt.get(rt.remote[Length](rt.remote[Blob](0)))  # the frees land first
    assert_equal(_spilled(), 0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

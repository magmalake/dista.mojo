"""Frames over a real loopback socket, and the process helpers."""

from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from dista.frame import (
    KIND_ERROR,
    KIND_RESULT,
    KIND_TASK,
    read_frame,
    write_frame,
)
from dista.socket import Listener, Stream, exit_now, fork, wait_child


def bytes_of(s: String) -> List[UInt8]:
    var out = List[UInt8]()
    for b in s.as_bytes():
        out.append(b)
    return out^


def test_listener_gets_a_port() raises:
    var l = Listener()
    assert_true(l.port > 0)


def test_frame_roundtrip() raises:
    var l = Listener()
    var c = Stream.connect(l.port)
    var s = l.accept()
    write_frame(c, KIND_TASK, 7, 0xDEADBEEF, bytes_of("args"))
    write_frame(c, KIND_ERROR, 8, 0, bytes_of("boom"))
    var f1 = read_frame(s)
    assert_true(Bool(f1))
    ref a = f1.value()
    assert_equal(a.kind, KIND_TASK)
    assert_equal(a.object_id, 7)
    assert_equal(a.task_id, 0xDEADBEEF)
    assert_equal(String(unsafe_from_utf8=a.payload), "args")
    var f2 = read_frame(s)
    ref b = f2.value()
    assert_equal(b.kind, KIND_ERROR)
    assert_equal(b.object_id, 8)
    assert_equal(String(unsafe_from_utf8=b.payload), "boom")


def test_large_payload() raises:
    """A payload bigger than a socket buffer arrives whole."""
    var l = Listener()
    var c = Stream.connect(l.port)
    var s = l.accept()
    var payload = List[UInt8](length=3 << 20, fill=0)
    for i in range(len(payload)):
        payload[i] = UInt8(i % 251)
    var pid = fork()
    if pid == 0:
        # The child writes while the parent reads, so neither side's buffer
        # has to hold the whole payload.
        try:
            write_frame(c, KIND_RESULT, 1, 0, payload)
        except:
            exit_now(1)
        exit_now(0)
    var f = read_frame(s)
    _ = wait_child(pid)
    ref got = f.value().payload
    assert_equal(len(got), len(payload))
    assert_equal(got[12345], payload[12345])
    assert_equal(got[len(got) - 1], payload[len(payload) - 1])


def test_clean_close_reads_none() raises:
    var l = Listener()
    var c = Stream.connect(l.port)
    var s = l.accept()
    c.close()
    assert_false(Bool(read_frame(s)))


def test_close_mid_frame_raises() raises:
    var l = Listener()
    var c = Stream.connect(l.port)
    var s = l.accept()
    # A length prefix promising 100 bytes, then only 3 of them.
    var partial: List[UInt8] = [100, 0, 0, 0, 1, 2, 3]
    c.write_all(partial)
    c.close()
    with assert_raises(contains="closed"):
        _ = read_frame(s)


def test_fork_and_exit_status() raises:
    var pid = fork()
    if pid == 0:
        exit_now(3)
    var status = wait_child(pid)
    # WEXITSTATUS: the exit code sits in the second byte on Linux and macOS.
    assert_equal((status >> 8) & 0xFF, 3)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

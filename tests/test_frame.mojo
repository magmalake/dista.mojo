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
from dista.socket import (
    Listener,
    POLLOUT,
    Stream,
    exit_now,
    fork,
    poll,
    send_some,
    wait_child,
)
from std.time import perf_counter_ns


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


def test_a_full_socket_doesnt_block_a_send() raises:
    """`send_some` to a peer that doesn't read returns 0 once the socket is
    full, rather than wait: macOS ignored MSG_DONTWAIT there, and the head's
    loop stalled on one slow driver. A blocked send hangs this test."""
    var l = Listener()
    var c = Stream.connect(l.port)
    var s = l.accept()
    var buf = List[UInt8](length=60_000, fill=1)
    var total = 0
    var full = False
    for _ in range(10_000):  # 600 MB: more than any socket buffers
        var n = send_some(s.fd, buf.unsafe_ptr(), len(buf))
        total += n
        if n == 0:
            full = True
            break
    assert_true(full, "the socket never filled")
    assert_true(total > 0, "nothing was sent")
    # The bytes sent still arrive, and `read_into` waits for them.
    var got = List[UInt8](length=len(buf), fill=0)
    assert_equal(c.read_into(got.unsafe_ptr(), len(got)), len(got))
    assert_equal(got[0], 1)


def test_connect_to_a_silent_address_times_out() raises:
    """A listener whose backlog is full drops new SYNs, as a vanished node or
    a firewall does: a connect to it would wait the kernel's two minutes.
    With a timeout it raises once that passes. A connect that waits for the
    kernel hangs this test."""
    var l = Listener(backlog=1)
    var held = List[Stream]()
    var timed_out = False
    var took = 0
    for _ in range(64):
        var t0 = perf_counter_ns()
        try:
            held.append(Stream.connect(l.port, timeout_ms=200))
        except e:
            took = Int(perf_counter_ns() - t0) // 1_000_000
            assert_true("timed out" in String(e), String(e))
            timed_out = True
            break
    assert_true(timed_out, "the backlog never filled")
    assert_true(took >= 150 and took < 2000, String("took ", took, " ms"))


def loopback() -> Array[UInt8, 4]:
    var ip = Array[UInt8, 4](fill=0)
    ip[0] = 127
    ip[3] = 1
    return ip^


def test_start_connect_completes_in_the_background() raises:
    var l = Listener()
    var c = Stream.start_connect(loopback(), l.port)
    _ = poll([c.fd], [POLLOUT], 2000)
    c.finish_connect()
    var s = l.accept()
    write_frame(c, KIND_RESULT, 1, 0, bytes_of("hi"))
    assert_equal(read_frame(s).value().object_id, 1)


def test_start_connect_to_a_closed_port_fails() raises:
    var l = Listener()
    var port = l.port
    l.close()
    var c = Stream.start_connect(loopback(), port)
    _ = poll([c.fd], [POLLOUT], 2000)
    with assert_raises(contains="connect failed"):
        c.finish_connect()


def test_fork_and_exit_status() raises:
    var pid = fork()
    if pid == 0:
        exit_now(3)
    var status = wait_child(pid)
    # WEXITSTATUS: the exit code sits in the second byte on Linux and macOS.
    assert_equal((status >> 8) & 0xFF, 3)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

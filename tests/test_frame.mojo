"""Frames over a real loopback socket, and the process helpers."""

from std.os import setenv, unsetenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from dista.frame import (
    KIND_ERROR,
    KIND_PART,
    KIND_RESULT,
    KIND_TASK,
    MAX_PAYLOAD,
    Parts,
    append_frame,
    append_parts,
    parse_body,
    read_frame,
    write_frame,
)
from dista.socket import (
    IPPROTO_TCP,
    SOL_SOCKET,
    SO_KEEPALIVE,
    TCP_KEEPCNT,
    TCP_KEEPIDLE,
    TCP_KEEPINTVL,
    get_int_option,
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


def test_a_payload_too_large_for_a_frame_raises() raises:
    """1.5 GiB passes the receiver's 1 GiB limit, and 4.5 GiB wrapped the u32
    length to garbage. Writing either raises, and appends nothing. The span
    is never read, so it needs no memory behind it."""
    var small = List[UInt8](length=16, fill=0)
    for size in [MAX_PAYLOAD + 1, 3 << 29, 9 << 29]:
        var huge = Span[UInt8, origin_of(small)](
            unsafe_ptr=small.unsafe_ptr(), length=size
        )
        var out = List[UInt8]()
        with assert_raises(contains="can't carry"):
            append_frame(out, KIND_RESULT, 1, 0, huge)
        assert_equal(len(out), 0)
    var out = List[UInt8]()
    append_frame(out, KIND_RESULT, 1, 0, small)
    assert_equal(len(out), 4 + 17 + 16)


def _frames(buf: List[UInt8]) raises -> List[Int]:
    """The body offsets of the frames in `buf`."""
    var at = List[Int]()
    var pos = 0
    while pos < len(buf):
        var n = (
            Int(buf[pos])
            | Int(buf[pos + 1]) << 8
            | Int(buf[pos + 2]) << 16
            | Int(buf[pos + 3]) << 24
        )
        at.append(pos)
        pos += 4 + n
    assert_equal(pos, len(buf))
    return at^


def test_parts_rebuild_the_object() raises:
    var data = List[UInt8](capacity=1000)
    for i in range(1000):
        data.append(UInt8(i % 251))
    var out = List[UInt8]()
    append_parts(out, 9, data, 300)
    var at = _frames(out)
    assert_equal(len(at), 4)  # 300, 300, 300, 100
    var parts = Parts()
    for k in range(len(at)):
        var end = at[k + 1] if k + 1 < len(at) else len(out)
        var f = parse_body(Span(out)[at[k] + 4 : end])
        assert_equal(f.kind, KIND_PART)
        assert_equal(f.object_id, 9)
        assert_equal(parts.add(f.payload), k == len(at) - 1)
    var whole = parts^.take()
    assert_equal(len(whole), 1000)
    for i in range(1000):
        assert_equal(whole[i], UInt8(i % 251))


def test_parts_out_of_order_raise() raises:
    var data = List[UInt8](length=10, fill=7)
    var out = List[UInt8]()
    append_parts(out, 1, data, 4)
    var at = _frames(out)
    var parts = Parts()
    var second = parse_body(Span(out)[at[1] + 4 : at[2]])
    with assert_raises(contains="bad part"):
        _ = parts.add(second.payload)


def test_sockets_keep_alive() raises:
    """Both ends of a connection probe a silent peer: after 10 s idle, every
    5 s, 3 times. Without it, a peer whose host froze or lost its network
    was never noticed."""
    var l = Listener()
    var c = Stream.connect(l.port)
    var s = l.accept()
    for fd in [c.fd, s.fd]:
        assert_true(get_int_option(fd, SOL_SOCKET, SO_KEEPALIVE) != 0)
        assert_equal(get_int_option(fd, IPPROTO_TCP, TCP_KEEPIDLE), 10)
        assert_equal(get_int_option(fd, IPPROTO_TCP, TCP_KEEPINTVL), 5)
        assert_equal(get_int_option(fd, IPPROTO_TCP, TCP_KEEPCNT), 3)
    c.close()  # alive until here
    s.close()


def test_keepalive_follows_the_environment() raises:
    _ = setenv("DISTA_KEEPALIVE_SECONDS", "4")
    var l = Listener()
    var c = Stream.connect(l.port)
    _ = setenv("DISTA_KEEPALIVE_SECONDS", "0")
    var off = Stream.connect(l.port)
    _ = unsetenv("DISTA_KEEPALIVE_SECONDS")
    assert_equal(get_int_option(c.fd, IPPROTO_TCP, TCP_KEEPIDLE), 4)
    assert_equal(get_int_option(c.fd, IPPROTO_TCP, TCP_KEEPINTVL), 2)
    assert_equal(get_int_option(off.fd, SOL_SOCKET, SO_KEEPALIVE), 0)
    c.close()
    off.close()
    l.close()


def test_fork_and_exit_status() raises:
    var pid = fork()
    if pid == 0:
        exit_now(3)
    var status = wait_child(pid)
    # WEXITSTATUS: the exit code sits in the second byte on Linux and macOS.
    assert_equal((status >> 8) & 0xFF, 3)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

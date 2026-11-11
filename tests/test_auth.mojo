"""The cluster token (`dista.auth`, #124): a gated connection hands out
nothing until `auth` with the token has come, and is closed for a missing,
wrong or oversized start; and a local cluster runs with `DISTA_TOKEN` set."""

from std.os import setenv, unsetenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_true,
)
from std.time import sleep

from dista import Runtime, Task
from dista.auth import cluster_token, token_matches
from dista.conn import Conn
from dista.frame import KIND_AUTH, KIND_TASK, write_frame
from dista.socket import Listener, Stream


def square(x: Int) -> Int:
    return x * x


comptime Square = Task[Int, Int, square]


def _frame_or_close(mut c: Conn) raises -> Int:
    """The kind of the first frame `c` hands out, or -1 once it's closed;
    waits up to 2 s."""
    for _ in range(200):
        if not c.closed:
            try:
                c.read_available()
            except:
                c.closed = True
        var f = c.next_frame()
        if f:
            return Int(f.value().kind)
        if c.closed:
            return -1
        sleep(0.01)
    raise Error("neither a frame nor a close in 2 s")


def _pair(gate: String) raises -> Tuple[Stream, Conn]:
    var l = Listener()
    var out = Stream.connect(l.port)
    var c = Conn(l.accept())
    c.require_token(gate)
    return (out^, c^)


def _bytes(s: String) -> List[UInt8]:
    return List[UInt8](s.as_bytes())


def test_token_matches() raises:
    assert_true(token_matches(Span(_bytes("s3cret")), "s3cret"))
    assert_false(token_matches(Span(_bytes("s3creT")), "s3cret"))
    assert_false(token_matches(Span(_bytes("s3cre")), "s3cret"))
    assert_false(token_matches(Span(_bytes("s3crett")), "s3cret"))
    assert_false(token_matches(Span(_bytes("")), "s3cret"))


def test_the_right_token_lets_frames_through() raises:
    var p = _pair("s3cret")
    write_frame(p[0], KIND_AUTH, 0, 0, _bytes("s3cret"))
    write_frame(p[0], KIND_TASK, 7, 1, _bytes("x"))
    assert_equal(_frame_or_close(p[1]), Int(KIND_TASK))
    assert_true(p[1].authed)


def test_no_token_is_refused() raises:
    var p = _pair("s3cret")
    write_frame(p[0], KIND_TASK, 7, 1, _bytes("x"))
    assert_equal(_frame_or_close(p[1]), -1)
    assert_true("cluster token" in p[1].why)


def test_a_wrong_token_is_refused() raises:
    var p = _pair("s3cret")
    write_frame(p[0], KIND_AUTH, 0, 0, _bytes("guess"))
    write_frame(p[0], KIND_TASK, 7, 1, _bytes("x"))
    assert_equal(_frame_or_close(p[1]), -1)


def test_a_large_frame_before_auth_is_refused() raises:
    var p = _pair("s3cret")
    var big = List[UInt8](length=300 * 1024, fill=7)
    write_frame(p[0], KIND_TASK, 7, 1, big)
    assert_equal(_frame_or_close(p[1]), -1)


def test_ungated_ignores_auth() raises:
    var p = _pair("")
    write_frame(p[0], KIND_AUTH, 0, 0, _bytes("anything"))
    write_frame(p[0], KIND_TASK, 7, 1, _bytes("x"))
    assert_equal(_frame_or_close(p[1]), Int(KIND_TASK))


def test_the_token_file() raises:
    _ = unsetenv("DISTA_TOKEN")
    var path = "build/test-auth-token"
    with open(path, "w") as f:
        f.write("from-file\nignored\n")
    _ = setenv("DISTA_TOKEN_FILE", path, True)
    assert_equal(cluster_token(), "from-file")
    _ = setenv("DISTA_TOKEN", "from-env", True)
    assert_equal(cluster_token(), "from-env")
    _ = unsetenv("DISTA_TOKEN")
    _ = unsetenv("DISTA_TOKEN_FILE")
    assert_equal(cluster_token(), "")


def test_a_cluster_runs_with_a_token() raises:
    """Local mode, every process given the token: the forked head and the
    workers gate, the driver and workers send it, and the results are
    those without one."""
    _ = setenv("DISTA_TOKEN", "run-42-token", True)
    var rt = Runtime[Square](workers=2)
    var refs = List[Int]()
    var total = 0
    for i in range(50):
        total += rt.take(rt.remote[Square](i))
    _ = refs^
    assert_equal(total, 40425)
    _ = unsetenv("DISTA_TOKEN")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

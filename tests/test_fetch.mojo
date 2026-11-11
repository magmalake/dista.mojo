"""The fetch path's copies and buffers.

- A `Conn` reads a large frame straight into a buffer of its own, not into
  its input buffer first, and hands out the frames around it in order.
- A worker serving a fetched result queues a window of it at a time: twelve
  fetchers of one result hold twelve windows, not twelve copies, from memory
  or from a node-store file, in one frame or in parts.
- End to end, large results reach tasks and the driver intact, over sockets
  (`DISTA_SHM=0`) and through the node store.
"""


from std.os import makedirs, setenv, unsetenv
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_true

from dista import ObjectRef, Runtime, Seq, Task
from dista.client import TaskContext
from dista.conn import BIG_FRAME, Conn
from dista.frame import (
    KIND_ERROR,
    KIND_FETCH,
    KIND_PART,
    KIND_RESULT,
    Frame,
    Parts,
    fetch_payload,
)
from dista.socket import Stream, stream_pair
from dista.store import Location, write_shared
from dista.task import StatePtr
from dista.worker import SERVE_CHUNK, SERVE_WINDOW, Worker, _Outgoing


def _bytes(n: Int, seed: Int) -> List[UInt8]:
    var out = List[UInt8](capacity=n)
    for i in range(n):
        out.append(UInt8((i * 31 + seed) % 251))
    return out^


def _nonblocking_pair() raises -> List[Conn]:
    var pair = stream_pair()
    var out = List[Conn]()
    out.append(Conn(pair.pop()))
    out.append(Conn(pair.pop()))
    return out^


def test_a_large_frame_is_read_into_its_own_buffer() raises:
    var conns = _nonblocking_pair()
    var big = _bytes(5 << 20, 3)
    conns[0].send(KIND_RESULT, 1, 0, List[UInt8]("before".as_bytes()))
    conns[0].send(KIND_RESULT, 2, 9, big)
    conns[0].send(KIND_RESULT, 3, 0, List[UInt8]("after".as_bytes()))
    var got = List[Frame]()
    var most = 0
    while len(got) < 3:
        conns[0].flush()
        conns[1].read_available()
        most = max(most, conns[1]._in.capacity())
        while True:
            var f = conns[1].next_frame()
            if not f:
                break
            got.append(f.take())
    assert_equal(got[0].object_id, 1)
    assert_equal(got[1].object_id, 2)
    assert_equal(got[1].task_id, 9)
    assert_equal(got[2].object_id, 3)
    assert_true(got[1].payload == big, "the large payload changed")
    assert_true(got[2].payload == List[UInt8]("after".as_bytes()))
    # The input buffer never held the large frame: only frames around it.
    print("  input buffer's largest capacity:", most)
    assert_true(most < 2 * BIG_FRAME, "the large frame went through _in")


def test_a_failed_read_leaves_no_bytes_behind() raises:
    """A read that fails, as a reset from a head that died does on Linux,
    adds nothing to the input buffer: what follows must not parse the
    buffer's uninitialized room as frames ("bad frame length 0")."""
    var conns = _nonblocking_pair()
    conns[0].send(KIND_RESULT, 1, 0, List[UInt8]("whole".as_bytes()))
    conns[0].send(KIND_RESULT, 2, 0, List[UInt8](length=BIG_FRAME, fill=1))
    conns[1].read_available()
    var whole = conns[1].next_frame()
    assert_true(Bool(whole) and whole.value().object_id == 1)
    var before = len(conns[1]._in) + len(conns[1]._big)
    conns[1].stream.close()  # the next recv fails, as a reset's does
    var failed = False
    try:
        conns[1].read_available()
    except:
        failed = True
    assert_true(failed, "a read on a closed socket succeeded")
    assert_equal(len(conns[1]._in) + len(conns[1]._big), before)
    assert_true(not conns[1].next_frame(), "a frame came out of nothing")


# A worker with no tasks: these only serve.
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


comptime ServingWorker = Worker[
    _dispatch, _create, _call, _destroy, _save, _restore
]

comptime FETCHERS = 12
comptime BOUND = SERVE_WINDOW + SERVE_CHUNK + 1024
"""The most a fetcher's buffer may hold: a window, a chunk, headers."""


def _serve_all(
    mut w: ServingWorker,
    obj: UInt64,
    want: List[UInt8],
    part: Int,
) raises:
    """`FETCHERS` fetchers ask for `obj` at once, none reading at first; then
    each reads its answer, checked against `want`."""
    var readers = List[Conn]()
    var first = len(w._serving)
    for _ in range(FETCHERS):
        var pair = stream_pair()
        w._serving.append(Conn(pair.pop()))
        w._sends.append(List[_Outgoing]())
        readers.append(Conn(pair.pop()))
    for k in range(FETCHERS):
        var payload = fetch_payload(part) if part > 0 else List[UInt8]()
        w._serve_fetch(first + k, Frame(KIND_FETCH, obj, 0, payload^))
    for k in range(FETCHERS):
        var held = w._serving[first + k].unsent()
        assert_true(
            held <= BOUND,
            String("a fetcher's buffer holds ", held, " bytes"),
        )
    for k in range(FETCHERS):
        var parts = Parts()
        var whole = List[UInt8]()
        var done = False
        while not done:
            # What the worker's loop does when the socket polls writable.
            w._serving[first + k].flush()
            w._refill(first + k)
            assert_true(w._serving[first + k].unsent() <= BOUND)
            readers[k].read_available()
            while True:
                var f = readers[k].next_frame()
                if not f:
                    break
                var fr = f.take()
                if fr.kind == KIND_RESULT:
                    assert_equal(part > 0 and len(want) > part, False)
                    whole = fr^.take_payload()
                    done = True
                else:
                    assert_equal(fr.kind, KIND_PART)
                    if parts.add(fr.payload):
                        whole = parts^.take()
                        parts = Parts()
                        done = True
        assert_true(whole == want, "a fetcher got different bytes")


def test_fetchers_of_one_result_hold_a_window_each() raises:
    var head = stream_pair()
    var w = ServingWorker(head.pop(), 0)
    # In memory, as with DISTA_SHM=0: one frame, then parts of 1 MiB.
    var data = _bytes(6 << 20, 5)
    w._objects[77] = data.copy()
    _serve_all(w, 77, data, 0)
    _serve_all(w, 77, data, 1 << 20)
    # From a node-store file, mapped rather than read into memory.
    var dir = String("build/test-fetch-store")
    if not exists(dir):
        makedirs(dir)
    var path = String(dir, "/object-78")
    write_shared(path, data)
    w._files[78] = Location([127, 0, 0, 1], 1, "n", path, len(data))
    _serve_all(w, 78, data, 0)
    _serve_all(w, 78, data, 1 << 20)
    _ = len(head)


comptime SIZE = 3 << 20


def blob(seed: Int) -> String:
    return String(seed % 10) * (SIZE + seed)


def length(s: String) -> Int:
    return s.byte_length()


def total_length(xs: Seq[String]) -> Int:
    var t = 0
    for x in xs.items:
        t += x.byte_length()
    return t


comptime Blob = Task[Int, String, blob]
comptime Length = Task[String, Int, length]
comptime TotalLength = Task[Seq[String], Int, total_length]


def _end_to_end() raises:
    var rt = Runtime[Blob, Length, TotalLength](workers=3, pipeline=1)
    var refs = List[ObjectRef[String]]()
    for i in range(6):
        refs.append(rt.remote[Blob](i))
    var want = 0
    for i in range(6):
        want += SIZE + i
        # One input, fetched for one task: it becomes the task's argument.
        assert_equal(rt.get(rt.remote[Length](refs[i])), SIZE + i)
    # Every input, gathered: each copied once into the arguments.
    assert_equal(rt.get(rt.remote[TotalLength](refs)), want)
    var s = rt.get(refs[5])
    assert_equal(s.byte_length(), SIZE + 5)
    assert_equal(s.as_bytes()[SIZE], UInt8(ord("5")))
    rt.release(refs)


def test_large_results_over_sockets() raises:
    _ = setenv("DISTA_SHM", "0")
    _end_to_end()
    _ = unsetenv("DISTA_SHM")


def test_large_results_over_sockets_with_a_token() raises:
    """The same with a cluster token (#124): every fetch, a worker's of
    another's result for a task's inputs and the driver's, opens with it,
    and every worker's object server asks for it."""
    _ = setenv("DISTA_SHM", "0")
    _ = setenv("DISTA_TOKEN", "fetch-token-124")
    _end_to_end()
    _ = unsetenv("DISTA_TOKEN")
    _ = unsetenv("DISTA_SHM")


def test_large_results_through_the_node_store() raises:
    _end_to_end()


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

"""Length-prefixed messages between drivers, the head and workers.

Every frame is a 4-byte little-endian length, then that many bytes:

| offset | field |
|---|---|
| 0 | kind (below) |
| 1 | object ID, u64 |
| 9 | task ID, u64 (0 unless the kind uses it) |
| 17 | payload |

| kind | sent by | payload |
|---|---|---|
| 1 task | driver to head, head to worker | the encoded argument |
| 2 result | worker to head, head to driver | the encoded result |
| 3 error | worker to head, head to driver | a UTF-8 message |
| 4 hello | driver or worker to head | role byte (`ROLE_*`), then the registry hash, u64 |
| 5 welcome | head to driver | the driver's number, u64 |
| 6 task-after | driver to head | gather byte, count u64, then that many object IDs |

A task-after frame queues a task whose argument is built from other results:
the single result as is, or with gather set, a `Seq` of all of them.
"""

from std.memory import bitcast

from dista.codec import get_scalar, put_scalar
from dista.socket import Stream

comptime KIND_TASK: UInt8 = 1
comptime KIND_RESULT: UInt8 = 2
comptime KIND_ERROR: UInt8 = 3
comptime KIND_HELLO: UInt8 = 4
comptime KIND_WELCOME: UInt8 = 5
comptime KIND_TASK_AFTER: UInt8 = 6
comptime ROLE_DRIVER: UInt8 = 1
comptime ROLE_WORKER: UInt8 = 2
comptime HEADER = 17
comptime MAX_FRAME = 1 << 30


struct Frame(Movable):
    var kind: UInt8
    var object_id: UInt64
    var task_id: UInt64
    var payload: List[UInt8]

    def __init__(
        out self,
        kind: UInt8,
        object_id: UInt64,
        task_id: UInt64,
        var payload: List[UInt8],
    ):
        self.kind = kind
        self.object_id = object_id
        self.task_id = task_id
        self.payload = payload^

    def take_payload(deinit self) -> List[UInt8]:
        """Consumes the frame and returns its payload without copying it."""
        return self.payload^


def append_frame(
    mut out: List[UInt8],
    kind: UInt8,
    object_id: UInt64,
    task_id: UInt64,
    payload: Span[UInt8, _],
):
    """Appends one whole frame to `out`."""
    put_scalar[DType.uint32](out, UInt32(HEADER + len(payload)))
    out.append(kind)
    put_scalar[DType.uint64](out, object_id)
    put_scalar[DType.uint64](out, task_id)
    out.extend(payload)


def write_frame(
    s: Stream,
    kind: UInt8,
    object_id: UInt64,
    task_id: UInt64,
    payload: Span[UInt8, _],
) raises:
    var out = List[UInt8](capacity=4 + HEADER + len(payload))
    append_frame(out, kind, object_id, task_id, payload)
    s.write_all(out)


def frame_length(head: Span[UInt8, _]) raises -> Int:
    """The body length a 4-byte frame prefix announces, checked."""
    var n = Int(
        bitcast[DType.uint32, 1](
            SIMD[DType.uint8, 4](head[0], head[1], head[2], head[3])
        )
    )
    if n < HEADER or n > MAX_FRAME:
        raise Error("bad frame length ", n)
    return n


def parse_body(body: Span[UInt8, _]) raises -> Frame:
    """A frame from its body: everything after the length prefix."""
    var pos = 1
    var object_id = get_scalar[DType.uint64](body, pos)
    var task_id = get_scalar[DType.uint64](body, pos)
    return Frame(body[0], object_id, task_id, List[UInt8](body[HEADER:]))


def read_frame(s: Stream) raises -> Optional[Frame]:
    """Reads the next frame, or returns `None` if the peer closed cleanly."""
    var head = Array[UInt8, 4](fill=0)
    if s.read_into(head.unsafe_ptr(), 4) == 0:
        return None
    var n = frame_length(Span(head))
    var body = List[UInt8](length=n, fill=0)
    if s.read_into(body.unsafe_ptr(), n) == 0:
        raise Error("connection closed mid-frame")
    return parse_body(body)

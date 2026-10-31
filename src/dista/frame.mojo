"""Length-prefixed messages between the driver and its workers.

Every frame is a 4-byte little-endian length, then that many bytes:

| offset | field |
|---|---|
| 0 | kind: 1 task, 2 result, 3 error |
| 1 | object ID, u64 |
| 9 | task ID, u64 (0 except on a task frame) |
| 17 | payload: argument, result or UTF-8 error message |
"""

from std.memory import bitcast

from dista.codec import get_scalar, put_scalar
from dista.socket import Stream

comptime KIND_TASK: UInt8 = 1
comptime KIND_RESULT: UInt8 = 2
comptime KIND_ERROR: UInt8 = 3
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


def write_frame(
    s: Stream,
    kind: UInt8,
    object_id: UInt64,
    task_id: UInt64,
    payload: Span[UInt8, _],
) raises:
    var out = List[UInt8](capacity=4 + HEADER + len(payload))
    put_scalar[DType.uint32](out, UInt32(HEADER + len(payload)))
    out.append(kind)
    put_scalar[DType.uint64](out, object_id)
    put_scalar[DType.uint64](out, task_id)
    out.extend(payload)
    s.write_all(out)


def read_frame(s: Stream) raises -> Optional[Frame]:
    """Reads the next frame, or returns `None` if the peer closed cleanly."""
    var head = Array[UInt8, 4](fill=0)
    if s.read_into(head.unsafe_ptr(), 4) == 0:
        return None
    var n = Int(
        bitcast[DType.uint32, 1](
            SIMD[DType.uint8, 4](head[0], head[1], head[2], head[3])
        )
    )
    if n < HEADER or n > MAX_FRAME:
        raise Error("bad frame length ", n)
    var body = List[UInt8](length=n, fill=0)
    if s.read_into(body.unsafe_ptr(), n) == 0:
        raise Error("connection closed mid-frame")
    var pos = 1
    var object_id = get_scalar[DType.uint64](body, pos)
    var task_id = get_scalar[DType.uint64](body, pos)
    var payload = List[UInt8](body[HEADER:])
    return Frame(body[0], object_id, task_id, payload^)

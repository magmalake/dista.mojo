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
| 4 hello | driver or worker to head | role byte (`ROLE_*`), the registry hash u64; a worker adds its object server's address |
| 5 welcome | head to driver | the driver's number, u64 |
| 6 task-after | driver to head | gather byte, count u64, then that many object IDs |
| 7 stored | worker to head | the result's size, u64: it stayed on the worker |
| 8 located | head to driver | the address of the worker holding the result |
| 9 fetch | driver or worker to a worker | none: send that object back as a result frame |
| 10 task-inputs | head to worker | gather byte, count u64, then per input a tag byte: 0, a length u64 and the bytes; or 1, an address |
| 11 release | driver to head | none: the driver is done with this object |
| 12 free | head to worker | none: delete this object |
| 13 stats | driver to head, head to driver | none; the reply is five u64 counts (see `HeadStats`) |
| 14 holding | worker to head, on rejoining | count u64, then per object its ID u64 and its location |
| 15 claim | driver to head, on resuming | none: this driver still holds this object, which lives on a worker |
| 16 resumed | driver to head | none: this driver has restored, claimed or resubmitted everything |
| 17 span | worker to head, before the result | three u64 wall-clock nanoseconds: when the task arrived, started and ended |
| 18 timeline | driver to head, head to driver | none; the reply is a count u64, then per finished task its object ID, task ID, the four head stamps (submitted, queued, dispatched, settled), the three worker stamps, and the worker's address |

After a head restart, a driver's hello carries its old number as a u64 after
the registry hash, so its object IDs stay valid. It then sends, in order of
submission, each object it still needs: a small result or error back as a
result or error frame, a result on a worker as claim, and an unfinished task
as the task or task-after frame it first sent; then resumed. A rejoining
worker sends holding.

A task-after frame queues a task whose argument is built from other results:
the single result as is, or with gather set, a `Seq` of all of them. When
some of those results live on workers, the head sends the task as
task-inputs, and the worker fetches them before it runs the task. An address
is 12 bytes: the IPv4 address, then the port as a u64.

The head counts references to every object: its driver's, until a release,
and one per submitted task that takes it as input, until that task settles.
At zero it forgets the object, and sends free to the worker holding it.
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
comptime KIND_STORED: UInt8 = 7
comptime KIND_LOCATED: UInt8 = 8
comptime KIND_FETCH: UInt8 = 9
comptime KIND_TASK_INPUTS: UInt8 = 10
comptime KIND_RELEASE: UInt8 = 11
comptime KIND_FREE: UInt8 = 12
comptime KIND_STATS: UInt8 = 13
comptime KIND_HOLDING: UInt8 = 14
comptime KIND_CLAIM: UInt8 = 15
comptime KIND_RESUMED: UInt8 = 16
comptime KIND_SPAN: UInt8 = 17
comptime KIND_TIMELINE: UInt8 = 18
comptime ADDR_SIZE = 12
comptime ROLE_DRIVER: UInt8 = 1
comptime ROLE_WORKER: UInt8 = 2
comptime HEADER = 17
comptime MAX_FRAME = 1 << 30


def message(payload: Span[UInt8, _]) -> String:
    """An error frame's text. It comes from another process, so bytes that
    aren't UTF-8 become replacement characters instead of a broken String."""
    return String(from_utf8_lossy=payload)


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


def encode_addr(ip: Array[UInt8, 4], port: Int) -> List[UInt8]:
    """An address as frames carry it: the IPv4 address, then the port, u64."""
    var out = List[UInt8](capacity=ADDR_SIZE)
    for i in range(4):
        out.append(ip[i])
    put_scalar[DType.uint64](out, UInt64(port))
    return out^


def decode_addr(
    buf: Span[UInt8, _], mut pos: Int
) raises -> Tuple[Array[UInt8, 4], Int]:
    """Reads an address written by `encode_addr` and advances `pos`."""
    if pos + ADDR_SIZE > len(buf):
        raise Error("truncated address")
    var ip = Array[UInt8, 4](fill=0)
    for i in range(4):
        ip[i] = buf[pos + i]
    pos += 4
    var port = Int(get_scalar[DType.uint64](buf, pos))
    return (ip^, port)

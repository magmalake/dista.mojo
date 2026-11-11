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
| 4 hello | driver, worker or observer to head | role byte (`ROLE_*`), the registry hash u64 (an observer's is ignored); a worker adds its object server's address |
| 5 welcome | head to driver | the driver's number, u64, then the head's epoch, u64 |
| 6 task-after | driver to head | flags byte (`GATHER`, `WITH_VALUE`), count u64, then that many object IDs; with `WITH_VALUE`, a length u64 and the encoded value |
| 7 stored | worker to head | the result's size, u64: it stayed on the worker |
| 8 located | head to driver | the address of the worker holding the result |
| 9 fetch | driver or worker to a worker | none, or the largest part the fetcher takes, u64: send that object back as a result frame, or if it's larger than that part, as part frames |
| 10 task-inputs | head to worker | flags byte, count u64, then per input a tag byte, its object ID, then for tag 0 a length u64 and the bytes, or for tag 1 its location; with `WITH_VALUE`, a length u64 and the value |
| 11 release | driver to head | none: the driver is done with this object |
| 12 free | head to worker | none: delete this object |
| 13 stats | driver to head, head to driver | none; the reply is five u64 counts (see `HeadStats`) |
| 14 holding | worker to head, on rejoining | count u64, then per object its ID u64 and its location |
| 15 claim | driver to head, on resuming | none: this driver still holds this object, which lives on a worker |
| 16 resumed | driver to head | none: this driver has restored, claimed or resubmitted everything |
| 17 span | worker to head, before the result | three u64 wall-clock nanoseconds: when the task arrived, started and ended |
| 18 timeline | driver to head, head to driver | none; the reply is a count u64, then per finished task its object ID, task ID, the four head stamps (submitted, queued, dispatched, settled), the three worker stamps, and the worker's address |
| 19 actor-create | driver to head, head to worker | from the driver, `checkpoint_every` u64 first; then the encoded constructor argument. The object ID names the actor, the task ID its type |
| 20 actor-call | driver to head, head to worker | the actor's ID u64, then the encoded argument; the task ID is the method's |
| 21 actor-kill | driver to head, head to worker | none: destroy the actor the object ID names |
| 22 checkpoint | driver to head, head to worker | from the driver, the actor's ID u64, and the object ID is the checkpoint's own; from the head, the calls covered by the checkpoint it would restore from, i64 (-1 for none), the object ID names the actor and the task ID is the number of calls the new one covers |
| 23 checkpointed | worker to head | none if the checkpoint is written, else why not; the object and task IDs are the checkpoint frame's |
| 24 actor-restore | head to worker | the number of calls the checkpoint covers, u64; the object ID names the actor, the task ID its type |
| 25 covered | head to driver | the newest of this driver's calls to the actor a checkpoint now includes, u64; the object ID names the actor |
| 26 status | observer to head, head to observer | none; the reply is the head's state as UTF-8 text |
| 27 stop | observer to head, head to worker | none: shut down; a worker exits instead of rejoining |
| 28 task-many | driver to head, head to worker | the number of results u64, then the encoded argument; the results are this object ID and the ones after it |
| 29 drain | worker to head | none: the worker was told to stop; it takes no more work, and leaves once the head says stop |
| 30 adopt | head to worker | a draining worker's location of the object: fetch it and keep it |
| 31 adopted | worker to head | 1 and the object's new location, or 0 and why not |
| 32 cancel | driver to head | none: fail this object's task with "cancelled", and drop it if it hasn't run |
| 33 part | worker to a driver or worker that fetched | the object's size u64, this part's offset u64, then its bytes; parts come in order, and the object is whole when they reach its size |
| 34 auth | any process to a head or a worker's object server, first on the connection | the cluster token's bytes (`dista.auth`, only with `DISTA_TOKEN` set) |

A frame holds at most `MAX_FRAME` bytes, so a result larger than that
crosses hosts in parts. A fetcher says it takes parts by putting the largest
it wants in the fetch frame. One that sends an empty fetch, as before parts
existed, gets the whole object in one result frame, or an error frame if it
doesn't fit. Writing a frame that doesn't fit raises; it used to wrap the
length past 4 GiB and garble the stream.

An observer (`dista status`, `timeline`, `stop`) says hello without a
registry and owns no objects. Its timeline frame gets every driver's
finished tasks, and leaves them for the drivers too.

After a head restart, a driver's hello carries its old number as a u64 after
the registry hash, so its object IDs stay valid, and then the epoch of the
head that first numbered it, u64. A head numbers new drivers from a random
start, so a driver of the head before rarely shares a number with one of
this head's; if it does, the epoch tells them apart, and the head refuses
the old one with an error frame rather than give it the new one's objects.
A driver resuming under a number that is still connected, from the same
epoch, is the same driver on a new connection: the old one is dropped. It
then sends, in order of
submission, each object it still needs: a small result or error back as a
result or error frame, a result on a worker as claim, and an unfinished task
as the task or task-after frame it first sent; then resumed. A rejoining
worker sends holding.

An actor lives on one worker, which runs nothing else while it hosts it. The
worker answers actor-create with an empty result, or the constructor's
error, and each call like a task: a result, stored or error frame for the
call's object ID. Calls run in the order the driver sent them. A checkpoint
runs between calls too: the worker writes the actor's state to
`DISTA_CHECKPOINT_URI` and answers checkpointed. Actor-restore builds an
actor from its checkpoint instead of its constructor, and is answered like
actor-create.

Actors survive a head restart through their checkpoints. Every caller keeps
its calls to an actor until covered says a checkpoint includes them. A
resuming driver sends each live actor's actor-create again, in order with
everything else, then every call it still keeps: those answered too, after
their answers, since the actor's state hasn't seen them. The new head
restores the actor from its newest checkpoint; a call that checkpoint
already includes isn't run again. A rejoining worker destroys the actor it
hosted: its state may hold calls no caller would resend.

A task-after frame queues a task whose argument is built from other results:
the single result as is, or with gather set, a `Seq` of all of them; with
`WITH_VALUE` set, after an encoded value (a `TaskWith`). With `MANY` set it's
a `TaskMany`, whose results, the object ID and the ones after it, number the
u64 that follows the flags byte; the task-inputs frame carries it on.

A task-many frame runs a `TaskMany`: its worker answers each of its results
with its own result, stored or error frame, as if each were a task. When
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
comptime KIND_ACTOR_CREATE: UInt8 = 19
comptime KIND_ACTOR_CALL: UInt8 = 20
comptime KIND_ACTOR_KILL: UInt8 = 21
comptime KIND_CHECKPOINT: UInt8 = 22
comptime KIND_CHECKPOINTED: UInt8 = 23
comptime KIND_ACTOR_RESTORE: UInt8 = 24
comptime KIND_COVERED: UInt8 = 25
comptime KIND_STATUS: UInt8 = 26
comptime KIND_STOP: UInt8 = 27
comptime KIND_TASK_MANY: UInt8 = 28
comptime KIND_DRAIN: UInt8 = 29
"""Worker to head: it was told to stop (SIGTERM), so send it no more work and
move what it holds; the head says `KIND_STOP` once it may go."""
comptime KIND_ADOPT: UInt8 = 30
"""Head to worker: fetch object `object_id` from the location in the payload,
a draining worker's, and keep it as its own."""
comptime KIND_ADOPTED: UInt8 = 31
"""Worker to head: 1 and the object's new location, or 0 and why it couldn't
take it."""
comptime KIND_CANCEL: UInt8 = 32
"""Driver to head: this driver won't read `object_id`. A task still queued or
waiting for inputs never runs; one running finishes, and its result is
dropped. Either way the object fails with "cancelled"."""
comptime KIND_PART: UInt8 = 33
"""Worker to a fetcher: one part of an object too large for one frame."""
comptime KIND_AUTH: UInt8 = 34
"""The cluster token, first on a connection (`dista.auth`)."""
comptime GATHER: UInt8 = 1
"""Task-after and task-inputs flag: the inputs gather into a `Seq`."""
comptime WITH_VALUE: UInt8 = 2
"""Task-after and task-inputs flag: an encoded value goes before them."""
comptime MANY: UInt8 = 4
"""Task-after and task-inputs flag: the task is a `TaskMany`, and the number
of its results, a u64, follows the flags byte."""
comptime ADDR_SIZE = 12
comptime ROLE_DRIVER: UInt8 = 1
comptime ROLE_WORKER: UInt8 = 2
comptime ROLE_CLIENT: UInt8 = 3
comptime ROLE_OBSERVER: UInt8 = 4
"""A driver inside a worker, for a task that calls actors: the head treats
it as a driver, except that it doesn't keep a local-mode head alive."""
comptime HEADER = 17
comptime MAX_FRAME = 1 << 30
"""The largest frame body a receiver takes: the kind, the IDs and the
payload."""
comptime MAX_PAYLOAD = MAX_FRAME - HEADER
comptime PART_SIZE = 64 << 20
"""The largest part a fetcher asks for: an object larger than this comes in
parts of it."""
comptime _PART_HEADER = 16
"""A part's payload starts with the object's size and the part's offset."""
comptime MAX_OBJECT = 1 << 40
"""A part frame announcing an object larger than this is garbage."""


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


def check_payload(n: Int) raises:
    """Raises unless a frame can carry a payload of `n` bytes."""
    if n > MAX_PAYLOAD:
        raise Error(
            "a frame can't carry ",
            n,
            " bytes: the limit is ",
            MAX_PAYLOAD,
            " (a result that large crosses hosts in parts; arguments can't)",
        )


def append_frame(
    mut out: List[UInt8],
    kind: UInt8,
    object_id: UInt64,
    task_id: UInt64,
    payload: Span[UInt8, _],
) raises:
    """Appends one whole frame to `out`. Raises, appending nothing, if the
    payload is too large for a frame."""
    check_payload(len(payload))
    put_scalar[DType.uint32](out, UInt32(HEADER + len(payload)))
    out.append(kind)
    put_scalar[DType.uint64](out, object_id)
    put_scalar[DType.uint64](out, task_id)
    out.extend(payload)


def append_parts(
    mut out: List[UInt8], object_id: UInt64, data: Span[UInt8, _], part: Int
) raises:
    """Appends `data`, object `object_id`, as part frames of at most `part`
    bytes each."""
    var size = part_payload_size(part)
    var total = len(data)
    out.reserve(len(out) + total + (total // size + 1) * (4 + HEADER + 16))
    var offset = 0
    while True:
        var n = min(size, total - offset)
        put_scalar[DType.uint32](out, UInt32(HEADER + _PART_HEADER + n))
        out.append(KIND_PART)
        put_scalar[DType.uint64](out, object_id)
        put_scalar[DType.uint64](out, 0)
        put_scalar[DType.uint64](out, UInt64(total))
        put_scalar[DType.uint64](out, UInt64(offset))
        out.extend(data[offset : offset + n])
        offset += n
        if offset >= total:
            return


def append_header(
    mut out: List[UInt8],
    kind: UInt8,
    object_id: UInt64,
    task_id: UInt64,
    size: Int,
) raises:
    """Appends a frame's length and header, for a payload of `size` bytes
    that the caller appends after it: a payload sent from where it lies."""
    check_payload(size)
    put_scalar[DType.uint32](out, UInt32(HEADER + size))
    out.append(kind)
    put_scalar[DType.uint64](out, object_id)
    put_scalar[DType.uint64](out, task_id)


def append_part_header(
    mut out: List[UInt8], object_id: UInt64, total: Int, offset: Int, n: Int
) raises:
    """Appends a part frame's length, header and part fields, for the `n`
    bytes of an object of `total` at `offset`, which the caller appends."""
    append_header(out, KIND_PART, object_id, 0, _PART_HEADER + n)
    put_scalar[DType.uint64](out, UInt64(total))
    put_scalar[DType.uint64](out, UInt64(offset))


def part_payload_size(part: Int) -> Int:
    """The bytes of an object one part frame carries, for a fetch asking for
    parts of `part`: what `append_parts` cuts it into."""
    return min(max(part, 1), MAX_PAYLOAD - _PART_HEADER)


def fetch_payload(part: Int = PART_SIZE) -> List[UInt8]:
    """A fetch frame's payload: the largest part this fetcher takes."""
    var out = List[UInt8](capacity=8)
    put_scalar[DType.uint64](out, UInt64(part))
    return out^


def fetch_part(payload: Span[UInt8, _]) raises -> Int:
    """The largest part a fetch frame asks for; 0 for an empty fetch, which
    wants the whole object in one frame."""
    if len(payload) == 0:
        return 0
    if len(payload) != 8:
        raise Error("bad fetch payload of ", len(payload), " bytes")
    var pos = 0
    var part = Int(get_scalar[DType.uint64](payload, pos))
    if part < 1:
        raise Error("bad fetch part size ", part)
    return part


struct Parts(Movable):
    """An object arriving in part frames: `add` each, in order."""

    var data: List[UInt8]
    var size: Int

    def __init__(out self):
        self.data = List[UInt8]()
        self.size = -1

    def add(mut self, payload: Span[UInt8, _]) raises -> Bool:
        """Adds one part frame's payload; whether the object is now whole."""
        if len(payload) < _PART_HEADER:
            raise Error("truncated part")
        var pos = 0
        var size = Int(get_scalar[DType.uint64](payload, pos))
        var offset = Int(get_scalar[DType.uint64](payload, pos))
        if self.size < 0:
            if size < 0 or size > MAX_OBJECT:
                raise Error("bad part: object size ", size)
            self.size = size
            self.data = List[UInt8](capacity=size)
        var n = len(payload) - _PART_HEADER
        if size != self.size or offset != len(self.data) or n > size - offset:
            raise Error(
                "bad part: ",
                n,
                " bytes at ",
                offset,
                " of ",
                size,
                ", after ",
                len(self.data),
                " of ",
                self.size,
            )
        self.data.extend(payload[_PART_HEADER:])
        return len(self.data) == self.size

    def take(deinit self) -> List[UInt8]:
        return self.data^


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
    # The header, then the payload straight into a buffer of its own: no
    # second copy of it.
    var header = Array[UInt8, HEADER](fill=0)
    if s.read_into(header.unsafe_ptr(), HEADER) == 0:
        raise Error("connection closed mid-frame")
    var payload = List[UInt8](capacity=n - HEADER)
    payload.resize(unsafe_uninit_length=n - HEADER)
    if n > HEADER and s.read_into(payload.unsafe_ptr(), n - HEADER) == 0:
        raise Error("connection closed mid-frame")
    var pos = 1
    var object_id = get_scalar[DType.uint64](Span(header), pos)
    var task_id = get_scalar[DType.uint64](Span(header), pos)
    return Frame(header[0], object_id, task_id, payload^)


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

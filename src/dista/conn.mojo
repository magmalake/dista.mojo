"""A non-blocking framed connection, for the head's event loop.

The head talks to every driver and worker at once, so it must never block on
one of them. A `Conn` reads and writes with `MSG_DONTWAIT`, so no call
blocks. It reads whatever has arrived into an input buffer and
hands out complete frames, and queues outgoing frames in an output buffer
that `flush` drains as the socket accepts it. The event loop polls for
writability only while something is queued.

A large frame isn't assembled in the input buffer. Once its header has
arrived, its payload gets a buffer of its own, of the size the header gives,
and the socket is read straight into that: the payload is copied once, out
of the kernel, and handed out as the frame's without another copy. Frames
that arrived before it are handed out first, and those after it once it's
whole.
"""

from std.ffi import c_int

from dista.frame import (
    HEADER,
    Frame,
    append_frame,
    append_parts,
    frame_length,
    parse_body,
)
from dista.socket import Stream, recv_some, send_some

comptime _READ_CHUNK = 64 * 1024
comptime BIG_FRAME = 256 * 1024
"""Payloads at least this large are read into a buffer of their own."""


struct Conn(Movable):
    var stream: Stream
    var _in: List[UInt8]
    var _in_pos: Int
    var _out: List[UInt8]
    var _out_pos: Int
    var closed: Bool
    """The peer hung up, or the connection failed."""
    var connecting: Bool
    """Started with `Stream.start_connect` and not yet connected: output
    queues until it is."""
    var deadline: UInt64
    """While connecting: when to give up, in `wall_ns`; 0 for never."""
    var why: String
    """Why the connection failed, if it's known."""
    var _scan: Int
    """Where in `_in` the first frame that hasn't fully arrived starts, as
    far as `_go_big` has looked."""
    var _big: List[UInt8]
    """A large frame's payload, being read straight from the socket."""
    var _big_need: Int
    """Its length; 0 when no large frame is arriving."""
    var _big_at: Int
    """Where in `_in` it goes: the frames before that come out first."""
    var _big_kind: UInt8
    var _big_object: UInt64
    var _big_task: UInt64

    def __init__(out self, var stream: Stream) raises:
        self.stream = stream^
        self._in = List[UInt8]()
        self._in_pos = 0
        self._out = List[UInt8]()
        self._out_pos = 0
        self.closed = False
        self.connecting = False
        self.deadline = 0
        self.why = String()
        self._scan = 0
        self._big = List[UInt8]()
        self._big_need = 0
        self._big_at = 0
        self._big_kind = 0
        self._big_object = 0
        self._big_task = 0

    def fd(self) -> c_int:
        return self.stream.fd

    def read_available(mut self, limit: Int = 0) raises:
        """Reads what's waiting on the socket into the input buffer, or a
        large frame's own: all of it, or with `limit`, about that many bytes,
        leaving the rest in the socket, where `poll` still reports it."""
        var got = 0
        while limit == 0 or got < limit:
            var n: Int
            if self._big_need > 0 and len(self._big) == self._big_need:
                # A large frame is whole: it goes out before anything after
                # it is read, which waits in the socket instead of in `_in`.
                return
            if self._big_need > len(self._big):
                var have = len(self._big)
                var room = self._big_need - have
                self._big.resize(unsafe_uninit_length=self._big_need)
                try:
                    n = recv_some(
                        self.stream.fd,
                        self._big.unsafe_ptr().unsafe_offset(have),
                        room,
                    )
                except e:
                    # A reset: the bytes not read must not count as read.
                    self._big.shrink(have)
                    raise e^
                self._big.shrink(have + max(n, 0))
            else:
                var have = len(self._in)
                if self._in.capacity() < have + _READ_CHUNK:
                    # Doubling: `reserve` grows to exactly what it's asked.
                    self._in.reserve(
                        max(2 * self._in.capacity(), have + _READ_CHUNK)
                    )
                self._in.resize(unsafe_uninit_length=have + _READ_CHUNK)
                try:
                    n = recv_some(
                        self.stream.fd,
                        self._in.unsafe_ptr().unsafe_offset(have),
                        _READ_CHUNK,
                    )
                except e:
                    # A reset: the bytes not read must not count as read.
                    self._in.shrink(have)
                    raise e^
                self._in.shrink(have + max(n, 0))
                if n > 0 and self._big_need == 0:
                    self._go_big()
            if n < 0:
                return
            if n == 0:
                self.closed = True
                return
            got += n

    def _go_big(mut self) raises:
        """If the first frame in `_in` that hasn't fully arrived is large and
        its header is here, moves what's come of its payload into a buffer
        of its own, where the rest will be read."""
        var pos = max(self._scan, self._in_pos)
        while len(self._in) - pos >= 4:
            var n = frame_length(Span(self._in)[pos : pos + 4])
            if len(self._in) - pos >= 4 + n:
                pos += 4 + n
                continue
            self._scan = pos
            if n - HEADER < BIG_FRAME or len(self._in) - pos < 4 + HEADER:
                return
            var f = parse_body(Span(self._in)[pos + 4 : pos + 4 + HEADER])
            self._big_kind = f.kind
            self._big_object = f.object_id
            self._big_task = f.task_id
            self._big_need = n - HEADER
            self._big = List[UInt8](capacity=self._big_need)
            self._big.extend(Span(self._in)[pos + 4 + HEADER :])
            self._in.shrink(pos)
            self._big_at = pos
            return
        self._scan = pos

    def next_frame(mut self) raises -> Optional[Frame]:
        """The next complete frame, if there is one."""
        if self._big_need > 0:
            if self._in_pos < self._big_at:
                return self._parse_next()  # one that came before it
            if len(self._big) < self._big_need:
                return None
            var f = Frame(
                self._big_kind, self._big_object, self._big_task, self._big^
            )
            self._big = List[UInt8]()
            self._big_need = 0
            self._big_at = 0
            self._scan = self._in_pos
            return f^
        var avail = len(self._in) - self._in_pos
        if avail < 4:
            self._compact()
            return None
        var n = frame_length(Span(self._in)[self._in_pos : self._in_pos + 4])
        if avail < 4 + n:
            self._compact()
            # The large frame whose header came in a read with the frames
            # before it: the rest goes to its own buffer.
            self._go_big()
            return None
        return self._parse_next()

    def _parse_next(mut self) raises -> Optional[Frame]:
        """The whole frame at `_in_pos`."""
        var n = frame_length(Span(self._in)[self._in_pos : self._in_pos + 4])
        var start = self._in_pos + 4
        var f = parse_body(Span(self._in)[start : start + n])
        self._in_pos = start + n
        return f^

    def _compact(mut self):
        if self._in_pos == 0:
            return
        self._scan = max(self._scan - self._in_pos, 0)
        if self._in_pos == len(self._in):
            self._in.clear()
        else:
            self._in = List[UInt8](Span(self._in)[self._in_pos :])
        self._in_pos = 0

    def send(
        mut self,
        kind: UInt8,
        object_id: UInt64,
        task_id: UInt64,
        payload: Span[UInt8, _],
    ) raises:
        """Queues a frame and sends as much as the socket takes now."""
        append_frame(self._out, kind, object_id, task_id, payload)
        if not self.connecting:
            self.flush()

    def finish_connect(mut self) raises:
        """The socket polled writable while connecting: connected, or raises
        why not."""
        self.stream.finish_connect()
        self.connecting = False

    def send_parts(
        mut self, object_id: UInt64, data: Span[UInt8, _], part: Int
    ) raises:
        """Queues object `object_id` as part frames of at most `part` bytes,
        and sends as much as the socket takes now."""
        append_parts(self._out, object_id, data, part)
        self.flush()

    def queue(mut self, data: Span[UInt8, _]):
        """Adds bytes to the output buffer without sending them: part of a
        frame whose header went before; `flush` sends them."""
        self._out.extend(data)

    def flush(mut self) raises:
        """Sends queued output until the socket would block."""
        while self._out_pos < len(self._out):
            var n = send_some(
                self.stream.fd,
                self._out.unsafe_ptr().unsafe_offset(self._out_pos),
                len(self._out) - self._out_pos,
            )
            if n == 0:
                return
            self._out_pos += n
        self._out.clear()
        self._out_pos = 0

    def release_buffers(mut self):
        """Frees both buffers, of a connection that's closed."""
        self._in = List[UInt8]()
        self._in_pos = 0
        self._out = List[UInt8]()
        self._out_pos = 0
        self._big = List[UInt8]()
        self._big_need = 0

    def wants_write(self) -> Bool:
        return self.connecting or self._out_pos < len(self._out)

    def unsent(self) -> Int:
        """Bytes queued to send that the peer hasn't taken yet."""
        return len(self._out) - self._out_pos

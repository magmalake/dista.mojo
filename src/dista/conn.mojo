"""A non-blocking framed connection, for the head's event loop.

The head talks to every driver and worker at once, so it must never block on
one of them. A `Conn` reads and writes with `MSG_DONTWAIT`, so no call
blocks. It reads whatever has arrived into an input buffer and
hands out complete frames, and queues outgoing frames in an output buffer
that `flush` drains as the socket accepts it. The event loop polls for
writability only while something is queued.
"""

from std.ffi import c_int

from dista.frame import Frame, append_frame, frame_length, parse_body
from dista.socket import Stream, recv_some, send_some

comptime _READ_CHUNK = 64 * 1024


struct Conn(Movable):
    var stream: Stream
    var _in: List[UInt8]
    var _in_pos: Int
    var _out: List[UInt8]
    var _out_pos: Int
    var closed: Bool
    """The peer hung up, or the connection failed."""

    def __init__(out self, var stream: Stream) raises:
        self.stream = stream^
        self._in = List[UInt8]()
        self._in_pos = 0
        self._out = List[UInt8]()
        self._out_pos = 0
        self.closed = False

    def fd(self) -> c_int:
        return self.stream.fd

    def read_available(mut self) raises:
        """Reads everything waiting on the socket into the input buffer."""
        var chunk = List[UInt8](length=_READ_CHUNK, fill=0)
        while True:
            var n = recv_some(self.stream.fd, chunk.unsafe_ptr(), _READ_CHUNK)
            if n < 0:
                return
            if n == 0:
                self.closed = True
                return
            self._in.extend(Span(chunk)[:n])

    def next_frame(mut self) raises -> Optional[Frame]:
        """The next complete frame in the input buffer, if there is one."""
        var avail = len(self._in) - self._in_pos
        if avail < 4:
            self._compact()
            return None
        var n = frame_length(Span(self._in)[self._in_pos : self._in_pos + 4])
        if avail < 4 + n:
            self._compact()
            return None
        var start = self._in_pos + 4
        var f = parse_body(Span(self._in)[start : start + n])
        self._in_pos = start + n
        return f^

    def _compact(mut self):
        if self._in_pos == 0:
            return
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
        self.flush()

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

    def wants_write(self) -> Bool:
        return self._out_pos < len(self._out)

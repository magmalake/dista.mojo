"""Loopback TCP and process control over libc, for the phase-1 runtime.

Just enough of `socket(2)` and friends to run a driver and its workers on one
machine: a listener on 127.0.0.1 with an OS-assigned port, blocking streams
that read and write whole buffers, and `fork`/`waitpid`/`_exit` to start and
reap workers. macOS and Linux differ in `sockaddr_in`'s first byte, in the
`SOL_SOCKET` constants and in how a write to a closed peer is kept from
raising SIGPIPE; `platform_map` picks the right value for each.
"""

from std.ffi import c_int, c_size_t, c_ssize_t, c_uint, external_call, get_errno
from std.memory import Pointer, stack_allocation
from std.sys.info import CompilationTarget, platform_map

comptime _pm = platform_map[T=Int, ...]

comptime AF_INET: c_int = 2
comptime SOCK_STREAM: c_int = 1
comptime SOL_SOCKET: c_int = c_int(_pm["SOL_SOCKET", linux=1, macos=0xFFFF]())
comptime SO_REUSEADDR: c_int = c_int(_pm["SO_REUSEADDR", linux=2, macos=4]())
comptime SO_NOSIGPIPE: c_int = c_int(
    _pm["SO_NOSIGPIPE", linux=0, macos=0x1022]()
)
comptime MSG_NOSIGNAL: c_int = c_int(
    _pm["MSG_NOSIGNAL", linux=0x4000, macos=0]()
)
comptime EINTR: c_int = 4
comptime IPPROTO_TCP: c_int = 6
comptime TCP_NODELAY: c_int = 1
comptime SOCKADDR_IN_SIZE = 16


def _fail(what: String) raises:
    raise Error(what, " failed: errno ", get_errno().value)


def _loopback(port: Int) -> Array[UInt8, SOCKADDR_IN_SIZE]:
    """A `sockaddr_in` for 127.0.0.1:`port`."""
    var a = Array[UInt8, SOCKADDR_IN_SIZE](fill=0)
    comptime if CompilationTarget.is_linux():
        a[0] = UInt8(AF_INET)
    else:
        a[0] = UInt8(SOCKADDR_IN_SIZE)  # sin_len
        a[1] = UInt8(AF_INET)
    a[2] = UInt8((port >> 8) & 0xFF)  # sin_port, network order
    a[3] = UInt8(port & 0xFF)
    a[4] = 127
    a[7] = 1
    return a^


def _new_socket() raises -> c_int:
    var fd = external_call["socket", c_int](AF_INET, SOCK_STREAM, c_int(0))
    if fd < 0:
        _fail("socket")
    var one = stack_allocation[1, c_int]()
    one[] = 1
    _ = external_call["setsockopt", c_int](
        fd, SOL_SOCKET, SO_REUSEADDR, one.unsafe_bitcast[NoneType](), c_uint(4)
    )
    # Frames are small request/response pairs: without this, Nagle's algorithm
    # and delayed ACKs stall each one for milliseconds.
    _ = external_call["setsockopt", c_int](
        fd, IPPROTO_TCP, TCP_NODELAY, one.unsafe_bitcast[NoneType](), c_uint(4)
    )
    comptime if not CompilationTarget.is_linux():
        _ = external_call["setsockopt", c_int](
            fd,
            SOL_SOCKET,
            SO_NOSIGPIPE,
            one.unsafe_bitcast[NoneType](),
            c_uint(4),
        )
    return fd


struct Stream(Movable):
    """A connected, blocking TCP socket. Closes itself when dropped."""

    var fd: c_int

    def __init__(out self, fd: c_int):
        self.fd = fd

    @staticmethod
    def connect(port: Int) raises -> Stream:
        """Connects to 127.0.0.1:`port`."""
        var fd = _new_socket()
        var addr = _loopback(port)
        if (
            external_call["connect", c_int](
                fd,
                addr.unsafe_ptr().unsafe_bitcast[NoneType](),
                c_uint(SOCKADDR_IN_SIZE),
            )
            != 0
        ):
            _ = external_call["close", c_int](fd)
            _fail("connect")
        return Stream(fd)

    def write_all(self, data: Span[UInt8, _]) raises:
        """Writes every byte of `data`."""
        var sent = 0
        while sent < len(data):
            var n = external_call["send", c_ssize_t](
                self.fd,
                data.unsafe_ptr()
                .unsafe_offset(sent)
                .unsafe_bitcast[NoneType](),
                c_size_t(len(data) - sent),
                MSG_NOSIGNAL,
            )
            if n < 0:
                if get_errno().value == EINTR:
                    continue
                _fail("send")
            sent += Int(n)

    def read_into(self, buf: Pointer[UInt8, _], size: Int) raises -> Int:
        """Reads exactly `size` bytes into `buf`.

        Returns `size`, or 0 when the peer closed before sending anything.
        Raises if the peer closes partway through.
        """
        var got = 0
        while got < size:
            var n = external_call["recv", c_ssize_t](
                self.fd,
                buf.unsafe_offset(got).unsafe_bitcast[NoneType](),
                c_size_t(size - got),
                c_int(0),
            )
            if n < 0:
                if get_errno().value == EINTR:
                    continue
                _fail("recv")
            if n == 0:
                if got == 0:
                    return 0
                raise Error("connection closed mid-message")
            got += Int(n)
        return got

    def close(mut self):
        if self.fd >= 0:
            _ = external_call["close", c_int](self.fd)
            self.fd = -1

    def __deinit__(deinit self):
        if self.fd >= 0:
            _ = external_call["close", c_int](self.fd)


struct Listener(Movable):
    """A TCP listener on 127.0.0.1. Closes itself when dropped."""

    var fd: c_int
    var port: Int

    def __init__(out self, backlog: Int = 128) raises:
        """Binds 127.0.0.1 on an OS-assigned port and starts listening."""
        self.fd = _new_socket()
        var addr = _loopback(0)
        if (
            external_call["bind", c_int](
                self.fd,
                addr.unsafe_ptr().unsafe_bitcast[NoneType](),
                c_uint(SOCKADDR_IN_SIZE),
            )
            != 0
        ):
            _fail("bind")
        if external_call["listen", c_int](self.fd, c_int(backlog)) != 0:
            _fail("listen")
        var len = stack_allocation[1, c_uint]()
        len[] = c_uint(SOCKADDR_IN_SIZE)
        if (
            external_call["getsockname", c_int](
                self.fd, addr.unsafe_ptr().unsafe_bitcast[NoneType](), len
            )
            != 0
        ):
            _fail("getsockname")
        self.port = (Int(addr[2]) << 8) | Int(addr[3])

    def accept(self) raises -> Stream:
        """Waits for the next connection."""
        while True:
            var fd = external_call["accept", c_int](self.fd, Int(0), Int(0))
            if fd >= 0:
                return Stream(fd)
            if get_errno().value != EINTR:
                _fail("accept")

    def close(mut self):
        if self.fd >= 0:
            _ = external_call["close", c_int](self.fd)
            self.fd = -1

    def __deinit__(deinit self):
        if self.fd >= 0:
            _ = external_call["close", c_int](self.fd)


comptime POLLIN: Int16 = 0x1
comptime POLLERR: Int16 = 0x8
comptime POLLHUP: Int16 = 0x10
comptime POLLFD_SIZE = 8  # struct pollfd { int fd; short events; short revents; }


def poll_readable(fds: List[c_int], timeout_ms: Int = -1) raises -> List[Bool]:
    """Waits until at least one of `fds` is readable, or `timeout_ms` passes.

    A closed or failed socket counts as readable, so the read that follows
    reports it. A timeout of -1 waits indefinitely.
    """
    var n = len(fds)
    var buf = List[UInt8](length=n * POLLFD_SIZE, fill=0)
    var base = buf.unsafe_ptr()
    for i in range(n):
        var at = base.unsafe_offset(i * POLLFD_SIZE)
        at.unsafe_bitcast[Int32]().unsafe_write(Int32(fds[i]))
        at.unsafe_offset(4).unsafe_bitcast[Int16]().unsafe_write(POLLIN)
    while True:
        var r = external_call["poll", c_int](
            base.unsafe_bitcast[NoneType](), c_uint(n), c_int(timeout_ms)
        )
        if r >= 0:
            break
        if get_errno().value != EINTR:
            _fail("poll")
    var ready = List[Bool](length=n, fill=False)
    for i in range(n):
        var revents = base.unsafe_offset(i * POLLFD_SIZE + 6).unsafe_bitcast[
            Int16
        ]()[]
        ready[i] = (revents & (POLLIN | POLLERR | POLLHUP)) != 0
    return ready^


def fork() raises -> Int:
    """Forks the process. Returns 0 in the child, the child's pid in the parent.
    """
    var pid = external_call["fork", c_int]()
    if pid < 0:
        _fail("fork")
    return Int(pid)


def wait_child(pid: Int) -> Int:
    """Waits for child `pid` to exit and returns its raw wait status."""
    var status = stack_allocation[1, c_int]()
    status[] = 0
    while True:
        var r = external_call["waitpid", c_int](c_int(pid), status, c_int(0))
        if r >= 0 or get_errno().value != EINTR:
            return Int(status[])


def exit_now(code: Int):
    """Ends the process immediately, without running atexit handlers.

    A forked worker leaves through here, so it never returns into the driver's
    `main` or flushes the driver's buffered output a second time.
    """
    external_call["_exit", NoneType](c_int(code))

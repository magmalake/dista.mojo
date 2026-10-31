"""IPv4 TCP and process control over libc.

Just enough of `socket(2)` and friends for dista's processes: listeners on
loopback or on every interface, streams to a host name or to loopback, the
non-blocking reads and writes the head's event loop uses, `poll`, and
`fork`/`waitpid`/`_exit` to start and reap local processes. macOS and Linux
differ in `sockaddr_in`'s first byte, in `addrinfo`'s field order, in the
`SOL_SOCKET`, `MSG_DONTWAIT` and `EAGAIN` constants, and in how a write to a
closed peer is kept from raising SIGPIPE; `platform_map` picks the right value
for each.
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
comptime EAGAIN: c_int = c_int(_pm["EAGAIN", linux=11, macos=35]())
# Non-blocking I/O is per call, with MSG_DONTWAIT, not per socket: setting
# O_NONBLOCK needs fcntl, which is variadic, and Apple's arm64 ABI passes
# variadic arguments on the stack where `external_call` doesn't put them.
comptime MSG_DONTWAIT: c_int = c_int(
    _pm["MSG_DONTWAIT", linux=0x40, macos=0x80]()
)
comptime ADDRINFO_SIZE = 48
comptime ADDRINFO_FAMILY_OFF = 4
comptime ADDRINFO_SOCKTYPE_OFF = 8
comptime ADDRINFO_ADDR_OFF = _pm["AI_ADDR_OFF", linux=24, macos=32]()


def _fail(what: String) raises:
    raise Error(what, " failed: errno ", get_errno().value)


def _sockaddr(ip: Array[UInt8, 4], port: Int) -> Array[UInt8, SOCKADDR_IN_SIZE]:
    """A `sockaddr_in` for `ip`:`port`."""
    var a = Array[UInt8, SOCKADDR_IN_SIZE](fill=0)
    comptime if CompilationTarget.is_linux():
        a[0] = UInt8(AF_INET)
    else:
        a[0] = UInt8(SOCKADDR_IN_SIZE)  # sin_len
        a[1] = UInt8(AF_INET)
    a[2] = UInt8((port >> 8) & 0xFF)  # sin_port, network order
    a[3] = UInt8(port & 0xFF)
    for i in range(4):
        a[4 + i] = ip[i]
    return a^


def _loopback(port: Int) -> Array[UInt8, SOCKADDR_IN_SIZE]:
    """A `sockaddr_in` for 127.0.0.1:`port`."""
    var ip = Array[UInt8, 4](fill=0)
    ip[0] = 127
    ip[3] = 1
    return _sockaddr(ip, port)


def resolve_ipv4(host: String) raises -> Array[UInt8, 4]:
    """The first IPv4 address of `host`, a name or a dotted quad."""
    var hints = stack_allocation[ADDRINFO_SIZE, UInt8]()
    for i in range(ADDRINFO_SIZE):
        hints.unsafe_offset(i).unsafe_write(0)
    hints.unsafe_offset(ADDRINFO_FAMILY_OFF).unsafe_bitcast[
        Int32
    ]().unsafe_write(Int32(AF_INET))
    hints.unsafe_offset(ADDRINFO_SOCKTYPE_OFF).unsafe_bitcast[
        Int32
    ]().unsafe_write(Int32(SOCK_STREAM))
    var slot = stack_allocation[1, UInt64]()
    slot[] = 0
    var name = host
    var rc = external_call["getaddrinfo", c_int](
        name.as_c_string_span(),
        Optional[Pointer[UInt8, MutUntrackedOrigin]](None),
        hints.unsafe_bitcast[NoneType](),
        slot.unsafe_bitcast[NoneType](),
    )
    if rc != 0 or slot[] == 0:
        raise Error("cannot resolve host ", host, " (getaddrinfo ", rc, ")")
    var head = Int(slot[])
    var node = Pointer[UInt8, MutUntrackedOrigin](unsafe_from_address=head)
    var sa = Int(
        node.unsafe_offset(ADDRINFO_ADDR_OFF).unsafe_bitcast[UInt64]()[]
    )
    var ip = Array[UInt8, 4](fill=0)
    if sa != 0:
        var p = Pointer[UInt8, MutUntrackedOrigin](unsafe_from_address=sa)
        for i in range(4):
            ip[i] = p.unsafe_offset(4 + i)[]
    _ = external_call["freeaddrinfo", NoneType](
        Pointer[NoneType, MutUntrackedOrigin](unsafe_from_address=head)
    )
    if sa == 0:
        raise Error("cannot resolve host ", host, ": no IPv4 address")
    return ip^


def send_some(fd: c_int, data: Pointer[UInt8, _], size: Int) raises -> Int:
    """Sends what the socket takes without blocking. Returns the bytes sent."""
    while True:
        var n = external_call["send", c_ssize_t](
            fd,
            data.unsafe_bitcast[NoneType](),
            c_size_t(size),
            MSG_NOSIGNAL | MSG_DONTWAIT,
        )
        if n >= 0:
            return Int(n)
        var e = get_errno().value
        if e == EAGAIN:
            return 0
        if e != EINTR:
            _fail("send")


def recv_some(fd: c_int, buf: Pointer[UInt8, _], size: Int) raises -> Int:
    """Reads what has arrived, without blocking.

    Returns the bytes read, 0 at end of stream, or -1 if nothing is waiting.
    """
    while True:
        var n = external_call["recv", c_ssize_t](
            fd, buf.unsafe_bitcast[NoneType](), c_size_t(size), MSG_DONTWAIT
        )
        if n >= 0:
            return Int(n)
        var e = get_errno().value
        if e == EAGAIN:
            return -1
        if e != EINTR:
            _fail("recv")


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

    @staticmethod
    def connect(host: String, port: Int) raises -> Stream:
        """Connects to `host`:`port`; `host` is a name or a dotted quad."""
        var fd = _new_socket()
        var addr = _sockaddr(resolve_ipv4(host), port)
        if (
            external_call["connect", c_int](
                fd,
                addr.unsafe_ptr().unsafe_bitcast[NoneType](),
                c_uint(SOCKADDR_IN_SIZE),
            )
            != 0
        ):
            _ = external_call["close", c_int](fd)
            _fail(String("connect to ", host, ":", port))
        return Stream(fd)

    @staticmethod
    def connect(ip: Array[UInt8, 4], port: Int) raises -> Stream:
        """Connects to `ip`:`port`."""
        var fd = _new_socket()
        var addr = _sockaddr(ip, port)
        if (
            external_call["connect", c_int](
                fd,
                addr.unsafe_ptr().unsafe_bitcast[NoneType](),
                c_uint(SOCKADDR_IN_SIZE),
            )
            != 0
        ):
            _ = external_call["close", c_int](fd)
            _fail(
                String(
                    "connect to ",
                    Int(ip[0]),
                    ".",
                    Int(ip[1]),
                    ".",
                    Int(ip[2]),
                    ".",
                    Int(ip[3]),
                    ":",
                    port,
                )
            )
        return Stream(fd)

    def local_ip(self) raises -> Array[UInt8, 4]:
        """The local IPv4 address this connection goes out from."""
        var addr = Array[UInt8, SOCKADDR_IN_SIZE](fill=0)
        var len = stack_allocation[1, c_uint]()
        len[] = c_uint(SOCKADDR_IN_SIZE)
        if (
            external_call["getsockname", c_int](
                self.fd, addr.unsafe_ptr().unsafe_bitcast[NoneType](), len
            )
            != 0
        ):
            _fail("getsockname")
        var ip = Array[UInt8, 4](fill=0)
        for i in range(4):
            ip[i] = addr[4 + i]
        return ip^

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
    """A TCP listener. Closes itself when dropped."""

    var fd: c_int
    var port: Int

    def __init__(
        out self, port: Int = 0, any_interface: Bool = False, backlog: Int = 128
    ) raises:
        """Binds and starts listening.

        Args:
            port: The port, or 0 for one the OS picks.
            any_interface: Listen on every interface instead of loopback.
            backlog: How many connections may wait to be accepted.
        """
        self.fd = _new_socket()
        var addr = _sockaddr(
            Array[UInt8, 4](fill=0), port
        ) if any_interface else _loopback(port)
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
comptime POLLOUT: Int16 = 0x4
comptime POLLERR: Int16 = 0x8
comptime POLLHUP: Int16 = 0x10
comptime POLLFD_SIZE = 8  # struct pollfd { int fd; short events; short revents; }


def poll(
    fds: List[c_int], events: List[Int16], timeout_ms: Int = -1
) raises -> List[Int16]:
    """Waits for any of `events[i]` on `fds[i]`, or until `timeout_ms` passes.

    Returns each descriptor's `revents`. A timeout of -1 waits indefinitely.
    """
    var n = len(fds)
    var buf = List[UInt8](length=n * POLLFD_SIZE, fill=0)
    var base = buf.unsafe_ptr()
    for i in range(n):
        var at = base.unsafe_offset(i * POLLFD_SIZE)
        at.unsafe_bitcast[Int32]().unsafe_write(Int32(fds[i]))
        at.unsafe_offset(4).unsafe_bitcast[Int16]().unsafe_write(events[i])
    while True:
        var r = external_call["poll", c_int](
            base.unsafe_bitcast[NoneType](), c_uint(n), c_int(timeout_ms)
        )
        if r >= 0:
            break
        if get_errno().value != EINTR:
            _fail("poll")
    var revents = List[Int16](length=n, fill=0)
    for i in range(n):
        revents[i] = base.unsafe_offset(i * POLLFD_SIZE + 6).unsafe_bitcast[
            Int16
        ]()[]
    return revents^


def poll_readable(fds: List[c_int], timeout_ms: Int = -1) raises -> List[Bool]:
    """Waits until at least one of `fds` is readable, or `timeout_ms` passes.

    A closed or failed socket counts as readable, so the read that follows
    reports it. A timeout of -1 waits indefinitely.
    """
    var revents = poll(
        fds, List[Int16](length=len(fds), fill=POLLIN), timeout_ms
    )
    var ready = List[Bool](length=len(fds), fill=False)
    for i in range(len(fds)):
        ready[i] = (revents[i] & (POLLIN | POLLERR | POLLHUP)) != 0
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

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
from std.os import getenv
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
comptime SO_KEEPALIVE: c_int = c_int(_pm["SO_KEEPALIVE", linux=9, macos=8]())
comptime TCP_KEEPIDLE: c_int = c_int(
    _pm["TCP_KEEPIDLE", linux=4, macos=0x10]()  # TCP_KEEPALIVE on macOS
)
comptime TCP_KEEPINTVL: c_int = c_int(
    _pm["TCP_KEEPINTVL", linux=5, macos=0x101]()
)
comptime TCP_KEEPCNT: c_int = c_int(_pm["TCP_KEEPCNT", linux=6, macos=0x102]())
comptime KEEPALIVE_PROBES = 3
comptime SOCKADDR_IN_SIZE = 16
comptime EAGAIN: c_int = c_int(_pm["EAGAIN", linux=11, macos=35]())
comptime EINPROGRESS: c_int = c_int(_pm["EINPROGRESS", linux=115, macos=36]())
comptime SO_ERROR: c_int = c_int(_pm["SO_ERROR", linux=4, macos=0x1007]())
# Every connected socket is non-blocking (O_NONBLOCK), and the calls that
# should wait, `write_all` and `read_into`, wait in `poll`. MSG_DONTWAIT alone
# isn't enough: macOS blocks a send with it once the socket's buffer is full,
# so a peer that stopped reading stalled the head's whole event loop. Setting
# O_NONBLOCK needs fcntl, which is variadic: Apple's arm64 ABI passes the
# variadic argument on the stack, which `num_fixed_args` tells
# `external_call`.
comptime F_GETFL: c_int = 3
comptime F_SETFL: c_int = 4
comptime O_NONBLOCK: c_int = c_int(_pm["O_NONBLOCK", linux=0x800, macos=0x4]())
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


def fcntl(fd: c_int, cmd: c_int, arg: c_int) -> c_int:
    """`fcntl(2)` with one int argument: the only declaration of it in dista,
    since two with different signatures don't link."""
    # Two fixed arguments, then the variadic one, where Apple's arm64 ABI
    # expects it.
    return external_call["fcntl", c_int, num_fixed_args=2](fd, cmd, arg)


def set_nonblocking(fd: c_int) -> Bool:
    """Makes `fd` non-blocking; whether that worked."""
    var flags = fcntl(fd, F_GETFL, 0)
    if flags < 0:
        return False
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0


def _wait_for(fd: c_int, event: Int16) raises:
    """Waits until `fd` is ready for `event`, or has failed."""
    _ = poll([fd], [event], -1)


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
    set_keepalive(fd)
    return fd


def keepalive_seconds() -> Int:
    """`DISTA_KEEPALIVE_SECONDS`: how long a connection may be silent before
    the kernel probes it; 0 turns keepalive off. Default 10."""
    try:
        return max(Int(getenv("DISTA_KEEPALIVE_SECONDS", "10")), 0)
    except:
        return 10


def _setopt(fd: c_int, level: c_int, name: c_int, value: Int):
    var v = stack_allocation[1, c_int]()
    v[] = c_int(value)
    _ = external_call["setsockopt", c_int](
        fd, level, name, v.unsafe_bitcast[NoneType](), c_uint(4)
    )


def set_keepalive(fd: c_int):
    """Has the kernel notice a peer that went silent without closing: a host
    that froze, lost its network or was hard-stopped, which no TCP close
    reports. After `keepalive_seconds()` of silence it probes every half of
    that, and after `KEEPALIVE_PROBES` unanswered probes the socket fails,
    about 25 s by default; `poll` then reports it as an error, and dista
    drops it as it does a closed one.

    Probes go only while nothing sent is unacknowledged; with data in flight
    the kernel's retransmission limit applies instead (about 15 minutes on
    Linux). `TCP_USER_TIMEOUT` would shorten that, but it also fails a
    connection whose peer is alive and only not reading, as a driver busy
    between `get`s, or one the head holds back for admission, is.
    `DISTA_TASK_TIMEOUT` bounds a task however its worker stalls."""
    var idle = keepalive_seconds()
    if idle == 0:
        return
    var every = max(idle // 2, 1)
    _setopt(fd, SOL_SOCKET, SO_KEEPALIVE, 1)
    _setopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, idle)
    _setopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, every)
    _setopt(fd, IPPROTO_TCP, TCP_KEEPCNT, KEEPALIVE_PROBES)


def get_int_option(fd: c_int, level: c_int, name: c_int) -> Int:
    """A socket option's int value, or -1 if it can't be read."""
    var v = stack_allocation[1, c_int]()
    v[] = 0
    var len = stack_allocation[1, c_uint]()
    len[] = c_uint(4)
    if (
        external_call["getsockopt", c_int](
            fd, level, name, v.unsafe_bitcast[NoneType](), len
        )
        != 0
    ):
        return -1
    return Int(v[])


def _refuse_self(fd: c_int) raises:
    """Closes `fd` and raises if it connected to itself.

    Connecting to a local port nothing listens on can pick that same port as
    the source and complete a connection to itself (TCP simultaneous open).
    A head restarting on an ephemeral port is exposed to this: a peer
    reconnecting meanwhile would hold the port, talking to itself, and the
    new head couldn't bind it.
    """
    var mine = Array[UInt8, SOCKADDR_IN_SIZE](fill=0)
    var theirs = Array[UInt8, SOCKADDR_IN_SIZE](fill=0)
    var len = stack_allocation[1, c_uint]()
    len[] = c_uint(SOCKADDR_IN_SIZE)
    if (
        external_call["getsockname", c_int](
            fd, mine.unsafe_ptr().unsafe_bitcast[NoneType](), len
        )
        != 0
    ):
        return
    len[] = c_uint(SOCKADDR_IN_SIZE)
    if (
        external_call["getpeername", c_int](
            fd, theirs.unsafe_ptr().unsafe_bitcast[NoneType](), len
        )
        != 0
    ):
        return
    # The port and the IPv4 address: bytes 2 to 8 of a sockaddr_in.
    for i in range(2, 8):
        if mine[i] != theirs[i]:
            return
    _ = external_call["close", c_int](fd)
    raise Error("connect: the socket connected to itself")


def socket_error(fd: c_int) -> c_int:
    """The error pending on `fd` (`SO_ERROR`): how a non-blocking connect
    ended, 0 if it succeeded."""
    var err = stack_allocation[1, c_int]()
    err[] = 0
    var len = stack_allocation[1, c_uint]()
    len[] = c_uint(4)
    if (
        external_call["getsockopt", c_int](
            fd, SOL_SOCKET, SO_ERROR, err.unsafe_bitcast[NoneType](), len
        )
        != 0
    ):
        return get_errno().value
    return err[]


def _connect(
    addr: Array[UInt8, SOCKADDR_IN_SIZE], what: String, timeout_ms: Int
) raises -> c_int:
    """Connects a new socket to `addr`, waiting at most `timeout_ms` (-1:
    as long as the kernel keeps trying, about two minutes on Linux for an
    address that drops SYNs). The socket is non-blocking first, so the wait
    is a `poll` that can end."""
    var fd = _new_socket()
    _ = set_nonblocking(fd)
    if (
        external_call["connect", c_int](
            fd,
            addr.unsafe_ptr().unsafe_bitcast[NoneType](),
            c_uint(SOCKADDR_IN_SIZE),
        )
        != 0
    ):
        var e = get_errno().value
        if e != EINPROGRESS and e != EINTR:
            _ = external_call["close", c_int](fd)
            raise Error(what, " failed: errno ", e)
        var rev = poll([fd], [POLLOUT], timeout_ms)
        if rev[0] == 0:
            _ = external_call["close", c_int](fd)
            raise Error(what, " timed out after ", timeout_ms, " ms")
        var err = socket_error(fd)
        if err != 0:
            _ = external_call["close", c_int](fd)
            raise Error(what, " failed: errno ", err)
    _refuse_self(fd)
    return fd


struct Stream(Movable):
    """A connected TCP socket. It's non-blocking, but `write_all` and
    `read_into` wait for it, so to its users it blocks; the head's loop uses
    `send_some` and `recv_some`, which don't. Closes itself when dropped."""

    var fd: c_int

    def __init__(out self, fd: c_int):
        self.fd = fd
        _ = set_nonblocking(fd)

    @staticmethod
    def connect(port: Int, timeout_ms: Int = -1) raises -> Stream:
        """Connects to 127.0.0.1:`port`, waiting at most `timeout_ms` (-1:
        as long as the kernel tries)."""
        return Stream(_connect(_loopback(port), String("connect"), timeout_ms))

    @staticmethod
    def connect(host: String, port: Int, timeout_ms: Int = -1) raises -> Stream:
        """Connects to `host`:`port`; `host` is a name or a dotted quad.
        Waits at most `timeout_ms` (-1: as long as the kernel tries)."""
        return Stream(
            _connect(
                _sockaddr(resolve_ipv4(host), port),
                String("connect to ", host, ":", port),
                timeout_ms,
            )
        )

    @staticmethod
    def connect(
        ip: Array[UInt8, 4], port: Int, timeout_ms: Int = -1
    ) raises -> Stream:
        """Connects to `ip`:`port`, waiting at most `timeout_ms` (-1: as
        long as the kernel tries)."""
        return Stream(
            _connect(
                _sockaddr(ip, port),
                String("connect to ", addr_key(ip, port)),
                timeout_ms,
            )
        )

    @staticmethod
    def start_connect(ip: Array[UInt8, 4], port: Int) raises -> Stream:
        """Starts connecting to `ip`:`port` and returns at once. The socket
        is writable once the connect completes or fails; `finish_connect`
        then says which. An event loop connects this way, so a peer that
        never answers doesn't stall it."""
        var addr = _sockaddr(ip, port)
        var fd = _new_socket()
        _ = set_nonblocking(fd)
        if (
            external_call["connect", c_int](
                fd,
                addr.unsafe_ptr().unsafe_bitcast[NoneType](),
                c_uint(SOCKADDR_IN_SIZE),
            )
            != 0
        ):
            var e = get_errno().value
            if e != EINPROGRESS and e != EINTR:
                _ = external_call["close", c_int](fd)
                raise Error(
                    "connect to ", addr_key(ip, port), " failed: errno ", e
                )
        return Stream(fd)

    def finish_connect(mut self) raises:
        """Raises if the connect `start_connect` began failed. Call it once
        the socket polls writable (or failed)."""
        var e = socket_error(self.fd)
        if e != 0:
            raise Error("connect failed: errno ", e)
        _refuse_self(self.fd)

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
                var e = get_errno().value
                if e == EINTR:
                    continue
                if e == EAGAIN:
                    _wait_for(self.fd, POLLOUT)
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
                var e = get_errno().value
                if e == EINTR:
                    continue
                if e == EAGAIN:
                    _wait_for(self.fd, POLLIN)
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


def wall_ns() -> UInt64:
    """Nanoseconds since the epoch, from CLOCK_REALTIME.

    The timeline stamps tasks on the head and on workers with this, so the
    stamps compare across processes and, NTP permitting, machines; a
    monotonic clock only compares within one host.
    """
    var ts = Array[Int64, 2](fill=0)
    _ = external_call["clock_gettime", c_int](c_int(0), ts.unsafe_ptr())
    return UInt64(ts[0]) * 1_000_000_000 + UInt64(ts[1])


def stream_pair() raises -> List[Stream]:
    """Two connected local stream sockets, for tests and fuzzing."""
    var fds = Array[c_int, 2](fill=0)
    if (
        external_call["socketpair", c_int](
            c_int(1), c_int(1), c_int(0), fds.unsafe_ptr()
        )
        != 0
    ):
        _fail("socketpair")
    var out = List[Stream]()
    out.append(Stream(fds[0]))
    out.append(Stream(fds[1]))
    return out^


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
                # Accepted sockets don't reliably inherit the listener's
                # keepalive settings.
                set_keepalive(fd)
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


def addr_key(ip: Array[UInt8, 4], port: Int) -> String:
    return String(
        Int(ip[0]), ".", Int(ip[1]), ".", Int(ip[2]), ".", Int(ip[3]), ":", port
    )

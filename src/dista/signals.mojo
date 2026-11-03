"""SIGTERM: the head exits, and a worker drains, even as a container's PID 1.

Kubernetes stops a pod with SIGTERM, and the kernel drops any signal sent to
a container's PID 1, as dista's process is, unless it has a handler for it.
The head's handler exits at once, as the signal's default action would. A
worker's writes a byte to a pipe its event loop polls, so the worker can
drain: finish what it's running, hand what it holds to other workers, and
leave (see `worker.mojo`). A second SIGTERM, or one that arrives before the
worker's loop runs, ends it at once.

A signal handler can't reach a Mojo variable, so the pipe's write end is
duplicated onto a fixed descriptor, `WAKE_FD`, which the handler writes to.
"""

from std.ffi import c_int, external_call

from dista.socket import fcntl

comptime SIGTERM: c_int = 15
comptime WAKE_FD: c_int = 250
"""The descriptor a worker's handler writes to: below macOS's default limit
of 256 open files, and above what a process has open when it starts."""
comptime _F_GETFD: c_int = 1


def _on_sigterm(sig: c_int):
    # Only async-signal-safe calls in here: `_exit` is one.
    external_call["_exit", NoneType](c_int(128) + sig)


def _wake(sig: c_int):
    # `write` is async-signal-safe too. It would block only on a full pipe,
    # which a few signals don't fill.
    var b = Array[UInt8, 1](fill=1)
    _ = external_call["write", Int](WAKE_FD, b.unsafe_ptr(), 1)


def exit_on_sigterm():
    """Makes SIGTERM end this process, even where it would be dropped."""
    _ = external_call["signal", Int](SIGTERM, _on_sigterm)


def wake_on_sigterm() -> c_int:
    """Makes SIGTERM write a byte to a pipe, and returns its read end; or -1,
    with SIGTERM ending the process, if `WAKE_FD` is taken or the pipe can't
    be made. Both ends block (setting O_NONBLOCK needs a variadic `fcntl`,
    which `external_call` can't make on Apple's arm64), so read it only once
    `poll` says it's readable."""
    if fcntl(WAKE_FD, _F_GETFD, 0) != -1:
        exit_on_sigterm()
        return -1
    var fds = Array[c_int, 2](fill=-1)
    if external_call["pipe", c_int](fds.unsafe_ptr()) != 0:
        exit_on_sigterm()
        return -1
    if external_call["dup2", c_int](fds[1], WAKE_FD) != WAKE_FD:
        _ = external_call["close", c_int](fds[0])
        _ = external_call["close", c_int](fds[1])
        exit_on_sigterm()
        return -1
    _ = external_call["close", c_int](fds[1])
    _ = external_call["signal", Int](SIGTERM, _wake)
    return fds[0]


def take_wakes(fd: c_int) -> Int:
    """Reads, once, the bytes SIGTERMs wrote to `fd`, which `poll` found
    readable; how many there were."""
    var buf = Array[UInt8, 16](fill=0)
    return max(external_call["read", Int](fd, buf.unsafe_ptr(), 16), 0)

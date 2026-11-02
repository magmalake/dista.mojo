"""SIGTERM ends the head and the workers, even as a container's PID 1.

Kubernetes stops a pod with SIGTERM, and the kernel drops any signal sent to
a container's PID 1, as dista's process is, unless it has a handler for it.
So the head and the workers install one, which exits at once, as the signal's
default action would. A worker outside Kubernetes leaves its node-store files
behind when it's ended this way, as it does when it's killed.
"""

from std.ffi import c_int, external_call

comptime SIGTERM: c_int = 15


def _on_sigterm(sig: c_int):
    # Only async-signal-safe calls in here: `_exit` is one.
    external_call["_exit", NoneType](c_int(128) + sig)


def exit_on_sigterm():
    """Makes SIGTERM end this process, even where it would be dropped."""
    _ = external_call["signal", Int](SIGTERM, _on_sigterm)

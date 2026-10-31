"""Tasks: remote functions, their IDs, and how a worker runs one.

A task wraps a top-level `def` with its argument and result types:

```mojo
def square(x: Int) -> Int:
    return x * x

comptime Square = Task[Int, Int, square]
```

The types are spelled out because Mojo can't infer a function's result type
from the function value. The function may raise or not; both bind to
`def(A) thin raises -> R`.

Every node runs the same binary, so a task message names the task by ID: a
64-bit FNV-1a hash of the function's linkage name, which includes its module
and argument types. The hash is fixed, unlike `hash()`, so a driver and a
worker started separately agree on it.
"""

from std.reflection import reflect_fn

from dista.codec import decode, encode


def fnv1a64(s: StringSlice) -> UInt64:
    """The 64-bit FNV-1a hash of `s`'s bytes."""
    var h: UInt64 = 0xCBF29CE484222325
    for b in s.as_bytes():
        h ^= UInt64(b)
        h *= 0x100000001B3
    return h


trait TaskLike:
    """What the runtime needs from a task, without knowing its function."""

    comptime A: Movable
    """The argument type."""
    comptime R: Movable
    """The result type."""

    @staticmethod
    def name() -> StaticString:
        """The function's linkage name."""
        ...

    @staticmethod
    def id() -> UInt64:
        """The task's wire ID."""
        ...

    @staticmethod
    def run(args: Span[UInt8, _], mut out: List[UInt8]) raises:
        """Decodes the argument, calls the function, appends the encoded result.
        """
        ...


struct Task[
    A_: Movable & Deinitable,
    R_: Movable & Deinitable,
    f: def(A_) thin raises -> R_,
](TaskLike):
    """A remote function taking one `A_` and returning an `R_`."""

    comptime A = Self.A_
    comptime R = Self.R_

    @staticmethod
    def name() -> StaticString:
        return reflect_fn[Self.f].linkage_name()

    @staticmethod
    def id() -> UInt64:
        return fnv1a64(Self.name())

    @staticmethod
    def run(args: Span[UInt8, _], mut out: List[UInt8]) raises:
        var pos = 0
        var a = decode[Self.A](args, pos)
        if pos != len(args):
            raise Error(
                Self.name(), ": ", len(args) - pos, " argument bytes left over"
            )
        var r = Self.f(a)
        encode(r, out)

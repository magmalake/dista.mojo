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

An actor is a state type and the function that builds it, and each of its
methods is a function taking the state `mut`:

```mojo
def new_counter(start: Int) -> Counter:
    return Counter(start)

def add(mut c: Counter, n: Int) -> Int:
    c.total += n
    return c.total

comptime CounterActor = Actor[Counter, Int, new_counter]
comptime Add = Method[CounterActor, Int, Int, add]
```

Actors and methods go in the runtime's registry with the tasks. The worker
hosting an actor keeps its state behind a `StatePtr`, which only the actor's
own `create`, `destroy`, `save` and `restore` and its methods' `call` cast
back to its type. A checkpoint is the state through the codec, so a state
type that implements `Codec` chooses what it saves.
"""

from std.memory.alloc import unsafe_alloc
from std.reflection import reflect_fn

from dista.codec import decode, encode


def fnv1a64(s: StringSlice) -> UInt64:
    """The 64-bit FNV-1a hash of `s`'s bytes."""
    var h: UInt64 = 0xCBF29CE484222325
    for b in s.as_bytes():
        h ^= UInt64(b)
        h *= 0x100000001B3
    return h


comptime StatePtr = Pointer[NoneType, MutUntrackedOrigin]
"""An actor's state on its worker, with its type erased."""


trait TaskLike:
    """What the runtime needs from a task, an actor or a method, without
    knowing its function. Each implements the part that applies to it."""

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
        raise Error(Self.name(), " is not a task")

    @staticmethod
    def is_actor() -> Bool:
        return False

    @staticmethod
    def owner() -> UInt64:
        """For a method, its actor's ID; 0 for anything else."""
        return 0

    @staticmethod
    def create(args: Span[UInt8, _]) raises -> StatePtr:
        """An actor: decodes the argument and builds the state."""
        raise Error(Self.name(), " is not an actor")

    @staticmethod
    def destroy(state: StatePtr):
        """An actor: destroys the state `create` built."""
        pass

    @staticmethod
    def save(state: StatePtr, mut out: List[UInt8]) raises:
        """An actor: appends its state's encoding, a checkpoint."""
        raise Error(Self.name(), " is not an actor")

    @staticmethod
    def restore(bytes: Span[UInt8, _]) raises -> StatePtr:
        """An actor: builds the state again from a checkpoint `save` wrote."""
        raise Error(Self.name(), " is not an actor")

    @staticmethod
    def call(
        state: StatePtr, args: Span[UInt8, _], mut out: List[UInt8]
    ) raises:
        """A method: runs on the actor's state, appends the encoded result."""
        raise Error(Self.name(), " is not a method")


trait ActorLike(TaskLike):
    comptime S: Movable
    """The actor's state type."""


trait MethodLike(TaskLike):
    comptime Act: ActorLike
    """The actor whose method this is."""


def _decode_all[
    T: Movable & Deinitable
](name: StaticString, args: Span[UInt8, _]) raises -> T:
    var pos = 0
    var a = decode[T](args, pos)
    if pos != len(args):
        raise Error(name, ": ", len(args) - pos, " argument bytes left over")
    return a^


struct Task[
    A_: Movable & Deinitable,
    R_: Movable & Deinitable,
    f: def(A_) thin raises -> R_,
](TaskLike):
    """A remote function taking one `A_` and returning an `R_`."""

    comptime A = Self.A_
    comptime R = Self.R_
    comptime Act = Self.Act_

    @staticmethod
    def name() -> StaticString:
        return reflect_fn[Self.f].linkage_name()

    @staticmethod
    def id() -> UInt64:
        return fnv1a64(Self.name())

    @staticmethod
    def run(args: Span[UInt8, _], mut out: List[UInt8]) raises:
        var a = _decode_all[Self.A](Self.name(), args)
        var r = Self.f(a)
        encode(r, out)


struct Actor[
    S_: Movable & Deinitable,
    A_: Movable & Deinitable,
    new: def(A_) thin raises -> S_,
](ActorLike):
    """An actor whose state is an `S_`, built by `new` from an `A_`."""

    comptime S = Self.S_
    comptime A = Self.A_
    comptime R = NoneType

    @staticmethod
    def name() -> StaticString:
        return reflect_fn[Self.new].linkage_name()

    @staticmethod
    def id() -> UInt64:
        return fnv1a64(Self.name())

    @staticmethod
    def is_actor() -> Bool:
        return True

    @staticmethod
    def create(args: Span[UInt8, _]) raises -> StatePtr:
        var a = _decode_all[Self.A](Self.name(), args)
        var p = unsafe_alloc[Self.S](1)
        try:
            p.unsafe_write(Self.new(a))
        except e:
            p.unsafe_free()
            raise e^
        return p.unsafe_bitcast[NoneType]()

    @staticmethod
    def destroy(state: StatePtr):
        var p = state.unsafe_bitcast[Self.S]()
        _ = p.unsafe_take_pointee()
        p.unsafe_free()

    @staticmethod
    def save(state: StatePtr, mut out: List[UInt8]) raises:
        encode(state.unsafe_bitcast[Self.S]()[], out)

    @staticmethod
    def restore(bytes: Span[UInt8, _]) raises -> StatePtr:
        var p = unsafe_alloc[Self.S](1)
        try:
            p.unsafe_write(_decode_all[Self.S](Self.name(), bytes))
        except e:
            p.unsafe_free()
            raise e^
        return p.unsafe_bitcast[NoneType]()


struct Method[
    Act_: ActorLike,
    A_: Movable & Deinitable,
    R_: Movable & Deinitable,
    f: def(mut Act_.S, A_) thin raises -> R_,
](MethodLike):
    """A method of actor `Act_`: `f` on its state and an `A_`, returning `R_`.
    """

    comptime A = Self.A_
    comptime R = Self.R_
    comptime Act = Self.Act_

    @staticmethod
    def name() -> StaticString:
        return reflect_fn[Self.f].linkage_name()

    @staticmethod
    def id() -> UInt64:
        return fnv1a64(Self.name())

    @staticmethod
    def owner() -> UInt64:
        return Self.Act.id()

    @staticmethod
    def call(
        state: StatePtr, args: Span[UInt8, _], mut out: List[UInt8]
    ) raises:
        var a = _decode_all[Self.A](Self.name(), args)
        var r = Self.f(state.unsafe_bitcast[Self.Act.S]()[], a)
        encode(r, out)

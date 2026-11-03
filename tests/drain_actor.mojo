"""The actor half of tests/drain.sh: an actor whose worker drains moves.

It makes 500 calls to a counter that remembers the process that built it,
then prints the process hosting it: the script sends that worker SIGTERM. It
makes 500 more while the worker drains, without waiting. The head moves the
actor from a checkpoint after the calls the worker had, and the rest run on
the new worker. Every answer must be exact, and the counter must be in a new
process, restored, not built again.
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.os.path import exists
from std.time import sleep

from dista import Actor, Method, ObjectRef, Runtime


@fieldwise_init
struct Counter(Movable):
    var total: Int
    var built_by: Int


def _pid() -> Int:
    return Int(external_call["getpid", c_int]())


def new_counter(start: Int) -> Counter:
    return Counter(start, _pid())


def add(mut c: Counter, n: Int) -> Int:
    c.total += n
    return c.total


def host(mut c: Counter, n: Int) -> Int:
    """The process running this call."""
    return _pid()


def builder(mut c: Counter, n: Int) -> Int:
    """The process that built this state, which a checkpoint carries."""
    return c.built_by


comptime CounterActor = Actor[Counter, Int, new_counter]
comptime Add = Method[CounterActor, Int, Int, add]
comptime Host = Method[CounterActor, Int, Int, host]
comptime Builder = Method[CounterActor, Int, Int, builder]


def main() raises:
    var rt = Runtime[CounterActor, Add, Host, Builder](workers=3)
    var c = rt.create[CounterActor](0)
    var refs = List[ObjectRef[Int]](capacity=1000)
    for _ in range(500):
        refs.append(rt.call[Add](c, 1))
    var first = rt.get(rt.call[Host](c, 0))
    print("ACTOR", first, flush=True)
    for _ in range(500):
        refs.append(rt.call[Add](c, 1))
    var go = getenv("DRAIN_GO")
    while not exists(go):
        sleep(0.05)
    for i in range(1000):
        if rt.get(refs[i]) != i + 1:
            raise Error("call ", i, " answered ", rt.get(refs[i]))
    var now = rt.get(rt.call[Host](c, 0))
    if now == first:
        raise Error("the actor is still on the stopped worker")
    if rt.get(rt.call[Builder](c, 0)) != first:
        raise Error("the actor was built again, not restored")
    print("PASS drain actor: moved from process", first, "to", now)

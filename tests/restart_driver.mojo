"""The driver tests/head_restart.sh runs while it kills and restarts the head.

Large results that stay on workers, a reduce over them after the driver has
released them, small results through the head, and an actor checkpointed
every 50 calls, all slow enough that the restart lands in the middle. The
actor's count must come out exact: the new head restores it from its newest
checkpoint, and the driver resends the calls no checkpoint includes.
"""

from std.time import sleep

from dista import Actor, Method, ObjectRef, Runtime, Seq, Task


def blob(n: Int) -> String:
    sleep(0.01)
    return String("x") * n


def total_length(xs: Seq[String]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i].byte_length()
    return t


def square(x: Int) -> Int:
    sleep(0.01)
    return x * x


@fieldwise_init
struct Counter(Movable):
    var total: Int


def new_counter(start: Int) -> Counter:
    return Counter(start)


def slow_add(mut c: Counter, n: Int) -> Int:
    sleep(0.01)
    c.total += n
    return c.total


comptime Blob = Task[Int, String, blob]
comptime TotalLength = Task[Seq[String], Int, total_length]
comptime Square = Task[Int, Int, square]
comptime CounterActor = Actor[Counter, Int, new_counter]
comptime SlowAdd = Method[CounterActor, Int, Int, slow_add]

comptime CALLS = 300


def main() raises:
    var rt = Runtime[Blob, TotalLength, Square, CounterActor, SlowAdd](
        workers=4
    )
    var counter = rt.create[CounterActor](0, checkpoint_every=50)
    var adds = List[ObjectRef[Int]](capacity=CALLS)
    for _ in range(CALLS):
        adds.append(rt.call[SlowAdd](counter, 1))
    var maps = List[ObjectRef[String]]()
    for i in range(400):
        maps.append(rt.remote[Blob](100_000 + i))
    var reduced = rt.remote[TotalLength](maps)
    rt.release(maps)
    var squares = List[ObjectRef[Int]]()
    for i in range(400):
        squares.append(rt.remote[Square](i))
    var sum = 0
    for r in squares:
        sum += rt.get(r)
    for i in range(CALLS):
        var got = rt.get(adds[i])
        if got != i + 1:
            raise Error("call ", i, " answered ", got, ", expected ", i + 1)
    var count = rt.get(rt.call[SlowAdd](counter, 0))
    rt.kill(counter)
    print("lengths", rt.get(reduced), "squares", sum, "counter", count)

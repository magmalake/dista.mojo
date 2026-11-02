"""The cluster smoke test: what the kind job in CI runs as its driver.

It checks three things, and prints `PASS` once they all hold:

1. Tasks fan out to every node. Each task reports the Kubernetes node it ran
   on (`NODE_NAME`, from the downward API, or the host name outside
   Kubernetes), and the driver expects `DISTA_EXPECT_NODES` distinct ones.
2. A map-reduce over small results comes out right.
3. Large results cross nodes. Each `make` result is over the inline limit, so
   it stays on its worker; a `total` task on another worker fetches it, and
   the driver fetches one itself. Pods don't share `/dev/shm`, so in a
   cluster those fetches go over sockets.

Run it locally too: with no environment it forks a head and four workers.

With `DISTA_SMOKE=actor` it runs the actor gate instead: a counter,
checkpointed every 50 calls, takes 2,000 slow calls while `tests/kind.sh`
deletes the pod hosting it. The driver prints `ACTOR_POD <pod>` once the
counter is up, so the script knows which pod to delete. Every call must
still be answered once, in order, the count must come out at exactly 2,000,
and the counter must end up on another pod, restored from a checkpoint
rather than rebuilt from its constructor: its state still names the pod
whose constructor built it.
"""

from std.os import getenv
from std.time import sleep

from dista import Actor, Method, ObjectRef, Runtime, Seq, Task
from dista.store import node_name


def node_of(x: Int) -> String:
    var node = getenv("NODE_NAME")
    if node == "":
        return node_name()
    return node


def square(x: Int) -> Int:
    return x * x


def make(n: Int) -> Seq[Int]:
    var xs = List[Int](capacity=n)
    for i in range(n):
        xs.append(i)
    return Seq[Int](xs^)


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    return t


@fieldwise_init
struct Counter(Movable):
    var total: Int
    var built_on: String
    """The pod that ran the constructor: a counter restored from a
    checkpoint still names it, one built again names its own."""


def new_counter(start: Int) -> Counter:
    return Counter(start, pod_name())


def pod_name() -> String:
    return getenv("HOSTNAME", node_name())


def slow_add(mut c: Counter, n: Int) -> Int:
    """Slow enough that the run outlasts deleting a pod."""
    sleep(0.01)
    c.total += n
    return c.total


def pod_of(mut c: Counter, n: Int) -> String:
    return pod_name()


def built_on(mut c: Counter, n: Int) -> String:
    return c.built_on.copy()


comptime NodeOf = Task[Int, String, node_of]
comptime Square = Task[Int, Int, square]
comptime Make = Task[Int, Seq[Int], make]
comptime Total = Task[Seq[Int], Int, total]
comptime CounterActor = Actor[Counter, Int, new_counter]
comptime SlowAdd = Method[CounterActor, Int, Int, slow_add]
comptime PodOf = Method[CounterActor, Int, String, pod_of]
comptime BuiltOn = Method[CounterActor, Int, String, built_on]
comptime RT = Runtime[
    NodeOf, Square, Make, Total, CounterActor, SlowAdd, PodOf, BuiltOn
]

comptime CALLS = 2000
comptime EVERY = 50


def actor_gate(mut rt: RT) raises:
    var c = rt.create[CounterActor](0, checkpoint_every=EVERY)
    var first = rt.get(rt.call[PodOf](c, 0))
    print("ACTOR_POD", first, flush=True)
    var refs = List[ObjectRef[Int]](capacity=CALLS)
    for _ in range(CALLS):
        refs.append(rt.call[SlowAdd](c, 1))
    for i in range(CALLS):
        var got = rt.get(refs[i])
        if got != i + 1:
            raise Error("call ", i, " answered ", got, ", expected ", i + 1)
    var last = rt.get(rt.call[PodOf](c, 0))
    var built = rt.get(rt.call[BuiltOn](c, 0))
    print("the counter moved from", first, "to", last, "; built on", built)
    if last == first:
        raise Error("the counter's pod was never deleted")
    if built != first:
        raise Error("rebuilt from the constructor, not from a checkpoint")
    rt.kill(c)
    print("PASS actor restored on", last, "with the count exact")


def main() raises:
    var rt = RT(workers=4)
    if getenv("DISTA_SMOKE") == "actor":
        actor_gate(rt)
        return
    var expect = Int(getenv("DISTA_EXPECT_NODES", "1"))

    # Workers connect as their pods come up, so the first rounds may not
    # reach every node yet; try for a while before giving up.
    var nodes = List[String]()
    for _ in range(60):
        var placed = List[ObjectRef[String]]()
        for i in range(300):
            placed.append(rt.remote[NodeOf](i))
        for r in placed:
            var n = rt.get(r)
            if n not in nodes:
                nodes.append(n^)
        if len(nodes) >= expect:
            break
        sleep(1.0)
    print("tasks ran on", len(nodes), "nodes:", end="")
    for n in nodes:
        print("", n, end="")
    print()
    if len(nodes) < expect:
        raise Error("expected ", expect, " nodes, saw ", len(nodes))

    var maps = List[ObjectRef[Int]]()
    for i in range(1000):
        maps.append(rt.remote[Square](i))
    var sum_squares = rt.get(rt.remote[Total](maps))
    print("sum of squares", sum_squares)
    if sum_squares != 332833500:
        raise Error("wrong sum of squares: ", sum_squares)

    comptime N = 250_000  # 2 MB of Int, well over the inline limit
    var bigs = List[ObjectRef[Seq[Int]]]()
    var sums = List[ObjectRef[Int]]()
    for _ in range(8):
        var b = rt.remote[Make](N)
        sums.append(rt.remote[Total](b))
        bigs.append(b)
    for s in sums:
        var t = rt.get(s)
        if t != N * (N - 1) // 2:
            raise Error("wrong total of a large result: ", t)
    var one = rt.get(bigs[0])
    if len(one) != N or one[N - 1] != N - 1:
        raise Error("large result came back wrong")
    print(
        "large results: 8 x",
        N * 8,
        "bytes; the driver fetched",
        rt.bytes_fetched(),
        "bytes over sockets",
    )
    if expect > 1 and rt.bytes_fetched() == 0:
        raise Error("expected the driver to fetch across nodes")
    print("PASS smoke on", len(nodes), "nodes")

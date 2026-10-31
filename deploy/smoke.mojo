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
"""

from std.os import getenv
from std.time import sleep

from dista import ObjectRef, Runtime, Seq, Task
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


comptime NodeOf = Task[Int, String, node_of]
comptime Square = Task[Int, Int, square]
comptime Make = Task[Int, Seq[Int], make]
comptime Total = Task[Seq[Int], Int, total]


def main() raises:
    var rt = Runtime[NodeOf, Square, Make, Total](workers=4)
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

"""Sum of squares as map-reduce: one binary for driver, head and workers.

Local mode, everything forked on this machine:

    ./mapreduce

A cluster, one process per role (DISTA_PORT=0 lets the OS pick a port; the
head prints it):

    DISTA_ROLE=head DISTA_PORT=6390 ./mapreduce
    DISTA_ROLE=worker DISTA_HEAD=headhost:6390 ./mapreduce    # once per worker
    DISTA_HEAD=headhost:6390 ./mapreduce
"""

from dista import ObjectRef, Runtime, Seq, Task


def square(x: Int) -> Int:
    return x * x


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    return t


comptime Square = Task[Int, Int, square]
comptime Total = Task[Seq[Int], Int, total]


def main() raises:
    var rt = Runtime[Square, Total](workers=4)
    var maps = List[ObjectRef[Int]]()
    for i in range(1000):
        maps.append(rt.remote[Square](i))
    print("total", rt.get(rt.remote[Total](maps)))

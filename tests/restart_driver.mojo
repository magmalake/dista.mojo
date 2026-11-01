"""The driver tests/head_restart.sh runs while it kills and restarts the head.

Large results that stay on workers, a reduce over them after the driver has
released them, and small results through the head, slow enough that the
restart lands in the middle.
"""

from std.time import sleep

from dista import ObjectRef, Runtime, Seq, Task


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


comptime Blob = Task[Int, String, blob]
comptime TotalLength = Task[Seq[String], Int, total_length]
comptime Square = Task[Int, Int, square]


def main() raises:
    var rt = Runtime[Blob, TotalLength, Square](workers=4)
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
    print("lengths", rt.get(reduced), "squares", sum)

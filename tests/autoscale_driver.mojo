"""The driver and worker tests/autoscale.sh runs: 200 tasks of half a second,
submitted at once to a head with no workers. The script's provider loop
starts and drains workers from `dista scale --once`; every result must come
back exact.
"""

from std.os import getenv
from std.time import sleep

from dista import ObjectRef, Runtime, Task

comptime N = 200


def slow_square(i: Int) -> Int:
    sleep(0.5)
    return i * i


comptime SlowSquare = Task[Int, Int, slow_square]


def main() raises:
    var rt = Runtime[SlowSquare](workers=0)
    var refs = List[ObjectRef[Int]](capacity=N)
    for i in range(N):
        refs.append(rt.remote[SlowSquare](i))
    var bad = 0
    for i in range(N):
        if rt.get(refs[i]) != i * i:
            print("task", i, "came back wrong")
            bad += 1
    if bad > 0:
        raise Error(bad, " results wrong")
    print("PASS autoscale driver:", N, "results exact")

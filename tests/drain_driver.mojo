"""The driver tests/drain.sh runs: a worker that drains loses nothing.

Thirty tasks make a large result each, which stays on its worker. Then six
slow tasks go out, two to each of the three workers, and the driver says
READY: the script sends one worker SIGTERM. That worker finishes its slow
tasks and the others adopt its results. Once it has left (the script says
so by making the file named in `DRAIN_GO`), the driver reads every result
itself, from where it last heard each was, and thirty more tasks each check
one. Nothing may fail, and nothing may run twice.
"""

from std.os import getenv
from std.os.path import exists
from std.time import sleep

from dista import ObjectRef, Runtime, Task

comptime N = 30
comptime SLOW = 6


def make(i: Int) -> String:
    """A result too large to go through the head, ending with its number."""
    return String("x") * 200_000 + String(i)


def slow(i: Int) -> Int:
    sleep(1.5)
    return i


def check(s: String) -> Int:
    """The number a `make` result ends with, if it's whole; else -1."""
    if s.byte_length() < 200_001:
        return -1
    try:
        return Int(String(s[byte=200_000:]))
    except:
        return -1


comptime Make = Task[Int, String, make]
comptime Slow = Task[Int, Int, slow]
comptime Check = Task[String, Int, check]


def main() raises:
    var rt = Runtime[Make, Slow, Check](workers=3)
    var made = List[ObjectRef[String]]()
    for i in range(N):
        made.append(rt.remote[Make](i))
    for r in made:
        _ = rt.location(r)  # every result stored before the drain
    var slows = List[ObjectRef[Int]]()
    for i in range(SLOW):
        slows.append(rt.remote[Slow](i))
    sleep(0.3)  # sent to the workers
    print("READY", flush=True)
    var go = getenv("DRAIN_GO")
    while not exists(go):
        sleep(0.05)
    var bad = 0
    for i in range(N):
        if check(rt.get(made[i])) != i:
            print("result", i, "came back wrong")
            bad += 1
    var checks = List[ObjectRef[Int]]()
    for r in made:
        checks.append(rt.remote[Check](r))
    for i in range(N):
        if rt.get(checks[i]) != i:
            print("check", i, "failed")
            bad += 1
    for i in range(SLOW):
        if rt.get(slows[i]) != i:
            print("slow task", i, "failed")
            bad += 1
    if bad > 0:
        raise Error(bad, " results wrong")
    print("PASS drain driver")

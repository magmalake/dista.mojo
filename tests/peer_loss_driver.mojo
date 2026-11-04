"""The driver tests/peer_loss.sh runs: a worker dies while others fetch from
it.

One task makes a result too large to go through the head, which stays on
its worker, and ends with that worker's process ID; the driver prints it as
HOLDER. The script stops that worker (SIGSTOP) and says go by making the file
named in `PEER_LOSS_GO`. Six tasks then take the result: the ones given to
the other workers fetch it from the stopped one, which never answers. Once
the driver says SUBMITTED, the script kills the holder. Every task must then
fail, with its input lost, instead of waiting forever.

Then a second result, HOLDER2, on one of the two workers left, and six tasks
that read it: the other worker fetches it and keeps its connection to the
holder open. The driver says FETCHED, and the script kills that holder while
nothing is asked of it, to see that the last worker doesn't spin on the
closed connection. `PEER_LOSS_DONE` names the file that says it's checked.
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.os.path import exists
from std.time import sleep

from dista import ObjectRef, Runtime, Task

comptime USES = 6


def make(i: Int) -> String:
    """A result too large to go through the head, ending with this worker's
    process ID."""
    return String("x") * 200_000 + String(external_call["getpid", c_int]())


def use(s: String) -> Int:
    return s.byte_length()


comptime Make = Task[Int, String, make]
comptime Use = Task[String, Int, use]


def main() raises:
    var rt = Runtime[Make, Use](workers=3)
    var made = rt.remote[Make](0)
    var s = rt.get(made)
    print("HOLDER", String(s[byte=200_000:]), flush=True)
    var go = getenv("PEER_LOSS_GO")
    while not exists(go):
        sleep(0.05)
    var uses = List[ObjectRef[Int]]()
    for _ in range(USES):
        uses.append(rt.remote[Use](made))
    print("SUBMITTED", flush=True)
    var failed = 0
    for i in range(USES):
        try:
            var n = rt.get(uses[i])
            print("task", i, "read the lost input:", n)
        except e:
            var msg = String(e)
            if "lost" not in msg:
                raise Error("task ", i, " failed for another reason: ", msg)
            failed += 1
    if failed != USES:
        raise Error(USES - failed, " tasks didn't fail")
    var made2 = rt.remote[Make](1)
    var s2 = rt.get(made2)
    print("HOLDER2", String(s2[byte=200_000:]), flush=True)
    var reads = List[ObjectRef[Int]]()
    for _ in range(USES):
        reads.append(rt.remote[Use](made2))
    for i in range(USES):
        if rt.get(reads[i]) != s2.byte_length():
            raise Error("task ", i, " read the wrong input")
    print("FETCHED", flush=True)
    var done = getenv("PEER_LOSS_DONE")
    while not exists(done):
        sleep(0.05)
    print("PASS peer loss driver", flush=True)

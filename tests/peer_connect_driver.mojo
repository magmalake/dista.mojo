"""The driver tests/peer_connect.sh runs: a worker connecting to a peer that
never answers keeps working meanwhile.

Two workers, one task in flight each. One result stays on worker A, another
on worker B; the driver prints A's process ID and address as HOLDER and
waits for the file named in `PEER_CONNECT_GO`. By then the script has
stopped A and filled its listener's backlog, so a connect to A gets no
answer, as with a vanished node. Two tasks then take A's result: one goes to
B, which starts connecting to A. While it waits, the driver reads B's own
result from B, which must answer at once; and with
`DISTA_PEER_CONNECT_TIMEOUT` at 2 s, B's task must fail as unreachable soon
after.
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.os.path import exists
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Task


def make(i: Int) -> String:
    """A result too large to go through the head, ending with this worker's
    process ID."""
    return String("x") * 200_000 + String(external_call["getpid", c_int]())


def use(s: String) -> Int:
    return s.byte_length()


comptime Make = Task[Int, String, make]
comptime Use = Task[String, Int, use]


def pid_of(s: String) -> String:
    return String(s[byte=200_000:])


def main() raises:
    var rt = Runtime[Make, Use](workers=2, pipeline=1)
    # Made at once, so both workers make some.
    var made = List[ObjectRef[String]]()
    for i in range(4):
        made.append(rt.remote[Make](i))
    var on_a = made[0]
    var a = pid_of(rt.get(on_a))
    var where_a = rt.location(on_a)
    var on_b = made[0]
    var found = False
    for r in made:
        if rt.location(r) != where_a:
            on_b = r
            found = True
    if not found:
        raise Error("every result landed on one worker")
    print("HOLDER", a, where_a, flush=True)
    var go = getenv("PEER_CONNECT_GO")
    while not exists(go):
        sleep(0.05)
    var uses = List[ObjectRef[Int]]()
    for _ in range(2):
        uses.append(rt.remote[Use](on_a))
    sleep(0.5)  # B has the task and is connecting to A
    var t0 = perf_counter_ns()
    var b = rt.get(on_b)
    var served = Float64(perf_counter_ns() - t0) / 1e6
    print("SERVED", b.byte_length(), "in", served, "ms", flush=True)
    if served > 1000:
        raise Error("worker B stalled ", served, " ms while connecting to A")
    # One task is on B and fails once its connect times out; the other sits
    # on the stopped A.
    var deadline = perf_counter_ns() + 10_000_000_000
    while perf_counter_ns() < deadline:
        for i in range(2):
            if rt.ready(uses[i]):
                try:
                    _ = rt.get(uses[i])
                    raise Error("a task read A's result")
                except e:
                    var msg = String(e)
                    if "cannot reach" not in msg or "timed out" not in msg:
                        raise Error("failed for another reason: ", msg)
                    var took = Float64(perf_counter_ns() - t0) / 1e9
                    print("FAILED", took, "s:", msg, flush=True)
                    print("PASS peer connect driver", flush=True)
                    return
        sleep(0.05)
    raise Error("no task failed within 10 s of A going silent")

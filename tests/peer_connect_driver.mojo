"""The driver tests/peer_connect.sh runs: a worker connecting to a peer that
never answers keeps working meanwhile.

Two workers, one task in flight each. Worker A advertises 192.0.2.1
(TEST-NET-1, never routed), so nothing answers a connect to it, as with a
vanished node or a firewall dropping SYNs (#126). One result stays on A,
another on B; the driver prints A's address as HOLDER and waits for the
file named in `PEER_CONNECT_GO`. By then the script has stopped A, so A
runs no task either. Two tasks then take A's result: one goes to B, which
starts connecting to A. While it waits, the driver reads B's own result
from B, which must answer at once; and with `DISTA_PEER_CONNECT_TIMEOUT` at
2 s, B's task must fail as unreachable soon after.
"""

from std.os import getenv
from std.os.path import exists
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Task


def make(i: Int) -> String:
    """A result too large to go through the head."""
    return String("x") * 200_000 + String(i)


def use(s: String) -> Int:
    return s.byte_length()


comptime Make = Task[Int, String, make]
comptime Use = Task[String, Int, use]


def main() raises:
    var rt = Runtime[Make, Use](workers=2, pipeline=1)
    # Made four at a time, until one result is on A (192.0.2.1) and one on
    # B; again while the second worker may not have joined yet, on a loaded
    # machine.
    var made = List[ObjectRef[String]]()
    var on_a = rt.remote[Make](0)
    var on_b = on_a
    var where_a = String()
    var a_found = False
    var b_found = False
    for round in range(50):
        made.clear()
        for i in range(4):
            made.append(rt.remote[Make](round * 4 + i))
        for r in made:
            var place = rt.location(r)
            if place.startswith("192.0.2.1:"):
                if not a_found:
                    on_a = r
                    where_a = place
                    a_found = True
            elif not b_found:
                on_b = r
                b_found = True
        if a_found and b_found:
            break
        sleep(0.1)
    if not (a_found and b_found):
        raise Error("every result landed on one worker")
    print("HOLDER", where_a, flush=True)
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

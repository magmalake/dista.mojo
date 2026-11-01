"""The driver tests/chaos.sh runs while it kills and restarts the head.

`CHAOS_SEED` picks a random task graph: leaves that make strings, small or
large enough to stay on a worker, and joins over one to four earlier
results. Some results are released as soon as their dependents are
submitted. Every result still held is checked against the same functions run
in this process, so the cluster's answers have to be exactly the ones a
single process gets, however often the head died along the way.
"""

from std.os import getenv
from std.time import sleep

from dista import ObjectRef, Runtime, Seq, Task


@fieldwise_init
struct Leaf(Copyable, Movable):
    var seed: Int
    var size: Int


def make(l: Leaf) -> String:
    sleep(0.005)
    return String(chr(97 + l.seed % 26)) * l.size


def join(xs: Seq[String]) -> String:
    sleep(0.005)
    var h = 0
    var total = 0
    for i in range(len(xs)):
        total += xs[i].byte_length()
        var bytes = xs[i].as_bytes()
        for b in bytes[: min(16, len(bytes))]:
            h = (h * 31 + Int(b)) % 1_000_003
    return String(chr(97 + h % 26)) * (total % 200_000 + 1)


comptime Make = Task[Leaf, String, make]
comptime Join = Task[Seq[String], String, join]


struct Rng:
    var state: UInt64

    def __init__(out self, seed: Int):
        self.state = UInt64(seed) * 6364136223846793005 + 1442695040888963407

    def next(mut self, n: Int) -> Int:
        self.state = self.state * 6364136223846793005 + 1442695040888963407
        return Int((self.state >> 33) % UInt64(n))


def _progress(path: String, n: Int):
    """Writes how far the driver has got, for chaos.sh to kill only after
    some: a head killed faster than the driver can recover never lets it
    finish, which is a livelock of the test, not of dista."""
    if path == "":
        return
    try:
        with open(path, "w") as f:
            f.write(String(n))
    except:
        pass


def main() raises:
    var seed = Int(getenv("CHAOS_SEED", "1"))
    var progress = getenv("CHAOS_PROGRESS")
    var steps = 0
    var rng = Rng(seed)
    var rt = Runtime[Make, Join](workers=4)
    var refs = List[ObjectRef[String]]()
    var local = List[String]()
    var released = List[Bool]()
    var n = 80 + rng.next(40)
    for k in range(n):
        if k < 8 or rng.next(3) == 0:
            var leaf = Leaf(rng.next(1000), 150_000 if rng.next(2) == 0 else 40)
            refs.append(rt.remote[Make](leaf.copy()))
            local.append(make(leaf))
        else:
            var deps = List[ObjectRef[String]]()
            var values = List[String]()
            for _ in range(1 + rng.next(4)):
                var d = rng.next(k)
                if released[d]:
                    d = rng.next(8)  # the first eight are never released
                deps.append(refs[d])
                values.append(local[d].copy())
            refs.append(rt.remote[Join](deps))
            local.append(join(Seq[String](values^)))
            if rng.next(4) == 0:
                var d = rng.next(k)
                if not released[d] and d >= 8:
                    rt.release(refs[d])
                    released[d] = True
        released.append(False)
        steps += 1
        _progress(progress, steps)
    var checked = 0
    for k in range(n):
        if released[k]:
            continue
        var got = rt.get(refs[k])
        if got != local[k]:
            raise Error("seed ", seed, ": result ", k, " differs")
        checked += 1
        steps += 1
        _progress(progress, steps)
    print("PASS chaos seed", seed, ":", n, "tasks,", checked, "checked")

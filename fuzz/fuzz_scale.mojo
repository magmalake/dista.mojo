"""Fuzz: `dista.scale`. `Demand.parse` on arbitrary bytes must raise or
read, never crash; and `decide`, on any demand and policy the bytes make,
must stay inside the policy's range: it never starts more than `max_step`,
never asks past `max_workers`, never drains below `min_workers`, a worker
twice, a draining one, a busy one, or an actor's without a checkpoint store.

    pixi run -e fuzz fuzz-scale
"""

from std.os import getenv

from mozz import FuzzConfig, fuzz

from dista.scale import Demand, Policy, WorkerDemand, decide


def _byte(data: List[UInt8], mut at: Int) -> Int:
    if at >= len(data):
        return 0
    at += 1
    return Int(data[at - 1])


def _check(data: List[UInt8]) raises:
    var at = 0
    var policy = Policy(
        min_workers=_byte(data, at) % 8,
        max_workers=_byte(data, at) % 16,
        tasks_per_worker=_byte(data, at) % 5,
        up_after=Float64(_byte(data, at) % 10),
        idle_before_down=Float64(_byte(data, at) % 100),
        max_step=_byte(data, at) % 6,
    )
    var d = Demand(
        tasks_ready=_byte(data, at) * 3,
        tasks_waiting=_byte(data, at) % 7,
        tasks_running=_byte(data, at) % 40,
        actors_waiting=_byte(data, at) % 3,
        pipeline=_byte(data, at) % 4,
        checkpoints=_byte(data, at) % 2 == 1,
    )
    var n = _byte(data, at) % 20
    for i in range(n):
        var flags = _byte(data, at)
        d.workers.append(
            WorkerDemand(
                String("w", i),
                flags % 3,
                Float64(_byte(data, at)),
                (flags >> 2) % 2 * 100,
                UInt64((flags >> 3) % 2 * (i + 1)),
                (flags >> 4) % 4 == 0,
            )
        )
    var pending = _byte(data, at) % 8
    var now = Float64(_byte(data, at) * 10)
    var since = Float64(_byte(data, at) * 10)
    var got = decide(d, policy, now, pending, since)
    var live = d.live()
    if got.start < 0 or got.start > policy.max_step:
        raise Error("assertion failed: started ", got.start)
    if got.start > 0 and live + pending + got.start > policy.max_workers:
        raise Error("assertion failed: past max_workers")
    if got.start > 0 and len(got.drain) > 0:
        raise Error("assertion failed: started and drained at once")
    if len(got.drain) > 0 and live - len(got.drain) < policy.min_workers:
        raise Error("assertion failed: below min_workers")
    var seen = List[String]()
    for a in got.drain:
        if a in seen:
            raise Error("assertion failed: drained ", a, " twice")
        seen.append(a)
        var found = False
        for w in d.workers:
            if w.address != a:
                continue
            found = True
            if w.draining or w.running > 0:
                raise Error("assertion failed: drained a busy worker")
            if w.actor != 0 and not d.checkpoints:
                raise Error("assertion failed: drained an actor's worker")
        if not found:
            raise Error("assertion failed: drained an unknown worker")


def target(data: List[UInt8]) raises:
    with open("build/fuzz-scale-last.bin", "w") as f:
        f.write_bytes(Span(data))
    if len(data) == 0:
        return
    if data[0] % 2 == 0:
        _check(data)
        return
    var text = String()
    for b in Span(data)[1:]:
        text += chr(Int(b) % 128)
    try:
        _ = Demand.parse(text)
    except e:
        if "assertion failed" in String(e):
            raise e^


def main() raises:
    var replay = getenv("FUZZ_REPLAY")
    if replay != "":
        var data = List[UInt8]()
        with open(replay, "r") as f:
            data = f.read_bytes()
        target(data)
        print("replayed", replay, "without a crash")
        return
    var seeds = List[List[UInt8]]()
    seeds.append([0, 1, 6, 0, 5, 60, 4, 30, 0, 2, 0, 2, 1, 3, 0, 5, 0, 7, 1, 2])
    var json: List[UInt8] = [1]
    json.extend(
        String(
            '{"tasks_ready":3,"pipeline":2,"workers":[{"address":"a:1",',
            '"running":0,"idle_seconds":4.5,"draining":false}]}',
        ).as_bytes()
    )
    seeds.append(json^)
    fuzz(
        target,
        FuzzConfig(
            max_runs=Int(getenv("FUZZ_RUNS", "100000")),
            seed=0,
            crash_dir="build/fuzz-crashes/scale",
            max_input_len=512,
        ),
        seeds,
    )

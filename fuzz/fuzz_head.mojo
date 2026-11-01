"""Fuzz: the head's protocol, driven by a fake driver and a fake worker.

Each input is a fresh head with one driver and one worker connected over
socket pairs, both past their hello. The rest of the input is a script of
frames, each sent by one of the two, or a disconnect: random kinds, object
IDs from a small range (so they collide with submitted tasks), random
payloads. A frame the head rejects drops that peer, as the real loop does.

After every step the head's bookkeeping must hold together: counts positive,
every held object counted, every location naming a worker connection, and
nothing pending for an object that no longer exists.

    pixi run -e fuzz fuzz-head
"""

from std.os import getenv

from mozz import FuzzConfig, fuzz

from dista.codec import put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_HELLO,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, stream_pair

comptime HASH: UInt64 = 0x5EED
comptime DRIVER = 0
comptime WORKER = 1


def _check(head: Head) raises:
    for entry in head._refs.items():
        if entry.value <= 0:
            raise Error("assertion failed: count ", entry.value)
    for entry in head._held.items():
        if entry.key not in head._refs:
            raise Error("assertion failed: held but uncounted")
    for entry in head._location.items():
        if entry.value < 0 or entry.value >= len(head._conns):
            raise Error("assertion failed: location names no connection")
        if entry.key not in head._where:
            raise Error("assertion failed: located but no address")
    for entry in head._actors.items():
        var w = entry.value.worker
        if w >= 0 and head._hosting[w] != entry.key and not entry.value.failed:
            raise Error("assertion failed: placed actor not hosted")
    for i in range(len(head._hosting)):
        if head._hosting[i] != 0 and head._hosting[i] not in head._actors:
            raise Error("assertion failed: worker hosts a gone actor")
    for entry in head._deps_of.items():
        if entry.key not in head._refs and entry.key not in head._dead:
            raise Error("assertion failed: inputs held for a gone object")


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    head._conns.append(Conn(pair.pop()))
    head._roles.append(0)
    head._inflight.append(List[UInt64]())
    head._worker_addr.append(List[UInt8]())
    head._hosting.append(0)
    return len(head._conns) - 1


def target(data: List[UInt8]) raises:
    with open("build/fuzz-head-last.bin", "w") as f:
        f.write_bytes(Span(data))
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var conn: List[Int] = [_connect(head, peers), _connect(head, peers)]
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(conn[DRIVER], Frame(KIND_HELLO, 0, 0, hello^))
    hello = [ROLE_WORKER]
    put_scalar[DType.uint64](hello, HASH)
    hello.extend(encode_addr([127, 0, 0, 1], 7000))
    head._handle(conn[WORKER], Frame(KIND_HELLO, 0, 0, hello^))
    var alive: List[Bool] = [True, True]

    # Each step: who (bit 0) and whether it hangs up (bit 1), kind, object,
    # task, payload length, payload.
    var pos = 0
    while pos + 5 <= len(data):
        var who = Int(data[pos] & 1)
        var hang_up = (data[pos] & 2) != 0
        var kind = data[pos + 1] % 22
        var obj = (UInt64(1) << DRIVER_SHIFT) | UInt64(data[pos + 2] % 16)
        var task = UInt64(data[pos + 3] % 4)
        var n = min(Int(data[pos + 4]), len(data) - pos - 5)
        var payload = List[UInt8](Span(data)[pos + 5 : pos + 5 + n])
        pos += 5 + n
        if not alive[who]:
            continue
        var i = conn[who]
        if hang_up:
            head._drop(i)
            alive[who] = False
        else:
            try:
                head._handle(i, Frame(kind, obj, task, payload^))
            except:
                head._drop(i)  # what the loop does with a peer that misbehaves
                alive[who] = False
        head._release_waiting()
        head._dispatch()
        _check(head)
    # Mojo destroys a value at its last use: keep the peers' ends open until
    # here, or the head's replies hit closed sockets.
    _ = len(peers)


def main() raises:
    var replay = getenv("FUZZ_REPLAY")
    if replay != "":
        # Run one saved input, e.g. build/fuzz-<name>-last.bin after an abort.
        var data = List[UInt8]()
        with open(replay, "r") as f:
            data = f.read_bytes()
        target(data)
        print("replayed", replay, "without a crash")
        return
    var seeds = List[List[UInt8]]()
    # The driver submits a task (kind 1) on object 1, the worker answers it
    # (kind 2), the driver releases it (kind 11) and asks for stats (13).
    var first: List[UInt8] = [
        0,
        1,
        1,
        0,
        1,
        7,
        1,
        2,
        1,
        0,
        1,
        9,
        0,
        11,
        1,
        0,
        0,
        0,
        13,
        0,
        0,
        0,
    ]
    seeds.append(first^)
    # A task-after (6) on object 1, then the worker stores 1 (7), errors 2.
    var second: List[UInt8] = [
        0,
        1,
        1,
        0,
        0,
        0,
        6,
        2,
        0,
        17,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        7,
        1,
        0,
        0,
        1,
        3,
        2,
        0,
        1,
        33,
    ]
    seeds.append(second^)
    fuzz(
        target,
        FuzzConfig(
            max_runs=Int(getenv("FUZZ_RUNS", "20000")),
            seed=0,
            crash_dir="build/fuzz-crashes/head",
            max_input_len=2048,
        ),
        seeds,
    )

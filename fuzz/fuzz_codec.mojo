"""Fuzz: `decode[T]` on arbitrary bytes, for each kind of type the codec
handles: scalars, strings, reflected structs, `Seq` and Arrow `RecordBatch`.

Task arguments and results arrive from other processes; decoding a malformed
one must raise, never read out of bounds or allocate without limit.

    pixi run -e fuzz fuzz-codec
"""

from std.os import getenv

from arrow_mlake import RecordBatch
from mozz import FuzzConfig, fuzz

from dista import Seq, decode, encode


@fieldwise_init
struct Point(Copyable, Movable):
    var x: Float64
    var y: Int
    var label: String


def _decode_one(kind: UInt8, body: Span[UInt8, _]) raises:
    var pos = 0
    if kind % 6 == 0:
        _ = decode[Int](body, pos)
    elif kind % 6 == 1:
        _ = decode[String](body, pos)
    elif kind % 6 == 2:
        _ = decode[Point](body, pos)
    elif kind % 6 == 3:
        _ = decode[Seq[Int]](body, pos)
    elif kind % 6 == 4:
        _ = decode[Seq[String]](body, pos)
    else:
        _ = decode[RecordBatch](body, pos)
    if pos < 0 or pos > len(body):
        raise Error("assertion failed: decode moved past its input")


def target(data: List[UInt8]) raises:
    with open("build/fuzz-codec-last.bin", "w") as f:
        f.write_bytes(Span(data))
    if len(data) == 0:
        return
    try:
        _decode_one(data[0], Span(data)[1:])
    except e:
        if "assertion failed" in String(e):
            raise e^
        # anything else is an expected rejection


def _seed(kind: UInt8, var body: List[UInt8]) -> List[UInt8]:
    var out: List[UInt8] = [kind]
    out.extend(body^)
    return out^


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
    var b = List[UInt8]()
    encode(42, b)
    seeds.append(_seed(0, b^))
    b = List[UInt8]()
    encode(String("hello"), b)
    seeds.append(_seed(1, b^))
    b = List[UInt8]()
    encode(Point(1.5, 2, "p"), b)
    seeds.append(_seed(2, b^))
    b = List[UInt8]()
    encode(Seq[Int]([1, 2, 3]), b)
    seeds.append(_seed(3, b^))
    b = List[UInt8]()
    encode(Seq[String]([String("a"), String("bc")]), b)
    seeds.append(_seed(4, b^))
    fuzz(
        target,
        FuzzConfig(
            max_runs=Int(getenv("FUZZ_RUNS", "100000")),
            seed=0,
            crash_dir="build/fuzz-crashes/codec",
            max_input_len=4096,
        ),
        seeds,
    )

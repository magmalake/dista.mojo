"""Fuzz: frame parsing over an arbitrary byte stream.

Every peer's bytes go through `frame_length` and `parse_body` before anything
checks who the peer is. A malformed stream must raise, never read out of
bounds.

    pixi run -e fuzz fuzz-frames
"""

from std.os import getenv

from mozz import FuzzConfig, fuzz

from dista.frame import KIND_TASK, append_frame, frame_length, parse_body


def target(data: List[UInt8]) raises:
    with open("build/fuzz-frames-last.bin", "w") as f:
        f.write_bytes(Span(data))
    var pos = 0
    while pos + 4 <= len(data):
        var n: Int
        try:
            n = frame_length(Span(data)[pos : pos + 4])
        except:
            return  # a bad length is an expected rejection
        if n > len(data) - pos - 4:
            return  # incomplete: a real reader waits for more
        var frame = parse_body(Span(data)[pos + 4 : pos + 4 + n])
        if len(frame.payload) != n - 17:
            raise Error("assertion failed: payload length")
        pos += 4 + n


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
    var one = List[UInt8]()
    var body: List[UInt8] = [1, 2, 3]
    append_frame(one, KIND_TASK, 7, 9, body)
    seeds.append(one.copy())
    append_frame(one, KIND_TASK, 8, 9, List[UInt8]())
    seeds.append(one^)
    var bad: List[UInt8] = [255, 255, 255, 255]
    seeds.append(bad^)
    fuzz(
        target,
        FuzzConfig(
            max_runs=Int(getenv("FUZZ_RUNS", "100000")),
            seed=0,
            crash_dir="build/fuzz-crashes/frames",
            max_input_len=4096,
        ),
        seeds,
    )

"""Shuffles: several results from one task (`TaskMany`), and a task taking a
value beside other tasks' results (`TaskWith`), then a group-by built from
the two."""

from std.testing import TestSuite, assert_equal, assert_raises, assert_true

from dista import ObjectRef, Runtime, Seq, Task, TaskMany, TaskWith
from dista.socket import exit_now


def split3(x: Int) -> Seq[Int]:
    return Seq[Int]([x, x + 1, x + 2])


def wrong_count(x: Int) -> Seq[Int]:
    return Seq[Int]([x])


def die(x: Int) -> Seq[Int]:
    """Its worker dies before answering."""
    exit_now(1)
    return Seq[Int]()


def blocks(n: Int) -> Seq[String]:
    """Two parts too large to travel through the head."""
    return Seq[String]([String("a") * n, String("b") * (n + 1)])


def length(s: String) -> Int:
    return s.byte_length()


def scaled_sum(k: Int, xs: Seq[Int]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    return k * t


def square(x: Int) -> Int:
    return x * x


@fieldwise_init
struct Pair(Copyable, Movable):
    var key: Int
    var value: Int


comptime ROWS = 20_000
comptime KEYS = 1_000
comptime REDUCERS = 4


def row(m: Int, i: Int) -> Pair:
    """Map task `m`'s row `i`: a pseudo-random key and a value."""
    var h = (m * 1_000_003 + i * 7_919) % 1_000_000_007
    return Pair(h % KEYS, (h // KEYS) % 100)


def map_rows(m: Int) -> Seq[Seq[Pair]]:
    """Map task `m`: its rows, one partition per reducer, by key."""
    var parts = List[Seq[Pair]]()
    for _ in range(REDUCERS):
        parts.append(Seq[Pair]())
    for i in range(ROWS):
        var r = row(m, i)
        parts[r.key % REDUCERS].append(r^)
    return Seq[Seq[Pair]](parts^)


def reduce_part(j: Int, parts: Seq[Seq[Pair]]) raises -> Seq[Pair]:
    """Reducer `j`: the sum per key over every map task's partition `j`."""
    var sums = Dict[Int, Int]()
    for p in range(len(parts)):
        ref part = parts.items[p]
        for i in range(len(part)):
            ref r = part.items[i]
            if r.key % REDUCERS != j:
                raise Error("key ", r.key, " reached reducer ", j)
            sums[r.key] = sums.get(r.key, 0) + r.value
    var out = Seq[Pair]()
    for entry in sums.items():
        out.append(Pair(entry.key, entry.value))
    return out^


comptime Split3 = TaskMany[Int, Int, split3]
comptime WrongCount = TaskMany[Int, Int, wrong_count]
comptime Blocks = TaskMany[Int, String, blocks]
comptime Die = TaskMany[Int, Int, die]
comptime Length = Task[String, Int, length]
comptime ScaledSum = TaskWith[Int, Int, Int, scaled_sum]
comptime Square = Task[Int, Int, square]
comptime MapRows = TaskMany[Int, Seq[Pair], map_rows]
comptime ReducePart = TaskWith[Int, Seq[Pair], Seq[Pair], reduce_part]
comptime RT = Runtime[
    Split3,
    WrongCount,
    Blocks,
    Die,
    Length,
    ScaledSum,
    Square,
    MapRows,
    ReducePart,
]


def test_a_task_with_three_results() raises:
    var rt = RT(workers=2)
    var r = rt.remote_many[Split3](10, 3)
    assert_equal(len(r), 3)
    assert_equal(rt.get(r[0]), 10)
    assert_equal(rt.get(r[1]), 11)
    assert_equal(rt.get(r[2]), 12)
    # Each result is its own object: a task can take one of them.
    assert_equal(rt.get(rt.remote[Square](r[2])), 144)


def test_the_wrong_number_of_results_fails_them_all() raises:
    var rt = RT(workers=2)
    var r = rt.remote_many[WrongCount](1, 2)
    for k in range(2):
        with assert_raises(contains="returned 1 results, not 2"):
            _ = rt.get(r[k])


def test_a_lost_worker_fails_every_result() raises:
    var rt = RT(workers=2)
    var r = rt.remote_many[Die](0, 3)
    for k in range(3):
        with assert_raises(contains="worker lost"):
            _ = rt.get(r[k])
    assert_equal(rt.get(rt.remote[Square](5)), 25)


def test_large_results_stay_on_the_worker_one_by_one() raises:
    var rt = RT(workers=2)
    comptime N = 200_000
    var r = rt.remote_many[Blocks](N, 2)
    assert_equal(rt.get(rt.remote[Length](r[0])), N)
    assert_equal(rt.get(rt.remote[Length](r[1])), N + 1)
    assert_true(rt.location(r[0]) != "", "the first part didn't stay put")
    rt.release(r[0])
    assert_equal(rt.get(r[1]).byte_length(), N + 1)


def test_a_value_beside_other_results() raises:
    var rt = RT(workers=2)
    var refs = List[ObjectRef[Int]]()
    for i in range(10):
        refs.append(rt.remote[Square](i))
    assert_equal(rt.get(rt.remote[ScaledSum](3, refs)), 3 * 285)


def test_a_group_by_shuffle() raises:
    """8 map tasks partition their rows by key for 4 reducers; reducer `j`
    takes partition `j` of every map task. The sums match one process's."""
    comptime MAPS = 8
    var rt = RT(workers=4)
    var outputs = List[List[ObjectRef[Seq[Pair]]]]()
    for m in range(MAPS):
        outputs.append(rt.remote_many[MapRows](m, REDUCERS))
    var reduced = List[ObjectRef[Seq[Pair]]]()
    for j in range(REDUCERS):
        var parts = List[ObjectRef[Seq[Pair]]]()
        for m in range(MAPS):
            parts.append(outputs[m][j])
        reduced.append(rt.remote[ReducePart](j, parts))
    var got = Dict[Int, Int]()
    for j in range(REDUCERS):
        var sums = rt.get(reduced[j])
        for i in range(len(sums)):
            ref p = sums.items[i]
            assert_true(p.key not in got, "a key reached two reducers")
            got[p.key] = p.value
    var want = Dict[Int, Int]()
    for m in range(MAPS):
        for i in range(ROWS):
            var r = row(m, i)
            want[r.key] = want.get(r.key, 0) + r.value
    assert_equal(len(got), len(want))
    for entry in want.items():
        assert_equal(got.get(entry.key, -1), entry.value)
    # Each map task's partitions are large: they stayed on its worker.
    assert_true(rt.location(outputs[0][0]) != "", "partitions came inline")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

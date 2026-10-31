"""Arrow `RecordBatch`es as task arguments and results, with real workers."""

from std.os import setenv, unsetenv
from std.testing import TestSuite, assert_equal, assert_false, assert_true

from arrow_mlake import (
    AT_INT64,
    AT_UTF8,
    ArrayData,
    ArrowType,
    RecordBatch,
    bit_set,
    store_u64,
)
from dista import ObjectRef, Runtime, Seq, Task, decode, encode


def make_batch(n: Int) -> RecordBatch:
    """`id` int64 counting from 0 with every tenth null, and `name` utf8."""
    var ids = ArrayData(ArrowType(AT_INT64), String("id"))
    var names = ArrayData(ArrowType(AT_UTF8), String("name"))
    ids.length = n
    names.length = n
    names.offsets.append(0)
    for i in range(n):
        var null = i % 10 == 9
        store_u64(ids.values, UInt64(0 if null else i))
        bit_set(ids.validity, i, not null)
        if null:
            ids.null_count += 1
        names.values.extend(String("row-", i).as_bytes())
        names.offsets.append(Int32(len(names.values)))
    var b = RecordBatch()
    b.roots.append(b.arena.add(ids^))
    b.roots.append(b.arena.add(names^))
    b.num_rows = n
    return b^


def sum_ids(b: RecordBatch) raises -> Int:
    var col = b.column_i64(0)
    var t = 0
    for i in range(len(col[0])):
        if col[1][i]:
            t += Int(col[0][i])
    return t


def expected_sum(n: Int) -> Int:
    var t = 0
    for i in range(n):
        if i % 10 != 9:
            t += i
    return t


def evens(b: RecordBatch) raises -> RecordBatch:
    """The rows with an even `id`, as a new batch: a batch in, a batch out."""
    var col = b.column_i64(0)
    var ids = ArrayData(ArrowType(AT_INT64), String("id"))
    for i in range(len(col[0])):
        if col[1][i] and col[0][i] % 2 == 0:
            store_u64(ids.values, UInt64(col[0][i]))
            ids.length += 1
    var out = RecordBatch()
    out.roots.append(out.arena.add(ids^))
    out.num_rows = out.arena.nodes[0].length
    return out^


def total_rows(bs: Seq[RecordBatch]) -> Int:
    var t = 0
    for i in range(len(bs)):
        t += bs[i].num_rows
    return t


comptime MakeBatch = Task[Int, RecordBatch, make_batch]
comptime SumIds = Task[RecordBatch, Int, sum_ids]
comptime Evens = Task[RecordBatch, RecordBatch, evens]
comptime TotalRows = Task[Seq[RecordBatch], Int, total_rows]


def test_a_batch_survives_the_codec() raises:
    var b = make_batch(1000)
    var buf = List[UInt8]()
    encode(b, buf)
    var pos = 0
    var back = decode[RecordBatch](Span(buf), pos)
    assert_equal(pos, len(buf))
    assert_equal(back.num_rows, 1000)
    assert_equal(back.name(1), "name")
    assert_equal(back.column(0).null_count, 100)
    assert_equal(sum_ids(back), expected_sum(1000))
    assert_equal(back.column_str(1)[0][999], "row-999")


def test_a_small_batch_comes_back_with_the_reply() raises:
    var rt = Runtime[MakeBatch, SumIds](workers=2)
    var b = rt.remote[MakeBatch](100)
    assert_equal(rt.location(b), "")
    assert_equal(sum_ids(rt.get(b)), expected_sum(100))
    assert_equal(rt.get(rt.remote[SumIds](b)), expected_sum(100))


def test_a_large_batch_stays_on_its_worker() raises:
    var rt = Runtime[MakeBatch, SumIds](workers=2)
    var b = rt.remote[MakeBatch](100_000)
    assert_true(" " in rt.location(b), "a 2 MB batch should be in the store")
    assert_equal(rt.get(rt.remote[SumIds](b)), expected_sum(100_000))
    var got = rt.get(b)
    assert_equal(got.num_rows, 100_000)
    assert_equal(got.column_str(1)[0][12345], "row-12345")


def test_batches_flow_from_task_to_task() raises:
    var rt = Runtime[MakeBatch, Evens, SumIds](workers=3)
    var e = rt.remote[Evens](rt.remote[MakeBatch](50_000))
    var want = 0
    for i in range(50_000):
        if i % 10 != 9 and i % 2 == 0:
            want += i
    assert_equal(rt.get(rt.remote[SumIds](e)), want)


def test_batches_gather_into_a_seq() raises:
    var rt = Runtime[MakeBatch, TotalRows](workers=4)
    var parts = List[ObjectRef[RecordBatch]]()
    for i in range(8):
        parts.append(rt.remote[MakeBatch](20_000 + i))
    assert_equal(rt.get(rt.remote[TotalRows](parts)), 8 * 20_000 + 28)


def test_without_the_node_store_batches_come_over_sockets() raises:
    _ = setenv("DISTA_SHM", "0")
    try:
        var rt = Runtime[MakeBatch, SumIds](workers=2)
        var b = rt.remote[MakeBatch](100_000)
        assert_false(" " in rt.location(b))
        assert_equal(rt.get(rt.remote[SumIds](b)), expected_sum(100_000))
        assert_equal(rt.get(b).num_rows, 100_000)
        assert_true(rt.bytes_fetched() > 1_000_000)
    finally:
        _ = unsetenv("DISTA_SHM")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

"""The phase-2 PR-8 gate: tasks read Parquet with parquet.mojo, return the
row groups as Arrow batches, and other tasks consume them.

`pixi run -e data test-parquet`. The `data` environment compiles parquet.mojo
and its siblings from checkouts next to this one, and reads parquet.mojo's
own 100,000-row fixture: four row groups of five columns, nulls in each.
"""

from std.testing import TestSuite, assert_equal, assert_true

from arrow_mlake import RecordBatch
from parquet import ParquetReader

from dista import ObjectRef, Runtime, Seq, Task

comptime FIXTURE = "../parquet.mojo/tests/fixtures/big.parquet"


@fieldwise_init
struct RowGroup(Copyable, Movable):
    var path: String
    var group: Int


def read_group(g: RowGroup) raises -> RecordBatch:
    """One row group of a Parquet file, as one batch."""
    var r = ParquetReader.open(g.path)
    r.select_row_groups([g.group])
    r.batch_size = 1 << 30
    return r.read_batch()


def sum_i(b: RecordBatch) raises -> Int:
    """The sum of the non-null values of column `i`."""
    var col = b.column_i64(0)
    var t = 0
    for k in range(len(col[0])):
        if col[1][k]:
            t += Int(col[0][k])
    return t


def total(xs: Seq[Int]) -> Int:
    var t = 0
    for k in range(len(xs)):
        t += xs[k]
    return t


def rows(bs: Seq[RecordBatch]) -> Int:
    var t = 0
    for k in range(len(bs)):
        t += bs[k].num_rows
    return t


comptime ReadGroup = Task[RowGroup, RecordBatch, read_group]
comptime SumI = Task[RecordBatch, Int, sum_i]
comptime Total = Task[Seq[Int], Int, total]
comptime Rows = Task[Seq[RecordBatch], Int, rows]


def _local_sum() raises -> Int:
    var r = ParquetReader.open(FIXTURE)
    var t = r.read_table()
    var s = 0
    for b in t.batches:
        s += sum_i(b)
    return s


def test_tasks_scan_row_groups_and_reduce() raises:
    var groups = ParquetReader.open(FIXTURE).num_row_groups()
    assert_equal(groups, 4)
    var rt = Runtime[ReadGroup, SumI, Total, Rows](workers=4)
    var batches = List[ObjectRef[RecordBatch]]()
    var sums = List[ObjectRef[Int]]()
    for g in range(groups):
        var b = rt.remote[ReadGroup](RowGroup(String(FIXTURE), g))
        sums.append(rt.remote[SumI](b))
        batches.append(b)
    assert_equal(rt.get(rt.remote[Total](sums)), _local_sum())
    assert_equal(rt.get(rt.remote[Rows](batches)), 100_000)
    # Each row group is far over the inline limit, so it stayed on the
    # worker that read it.
    for b in batches:
        assert_true(rt.location(b) != "", "a row group went through the head")


def test_a_scanned_batch_matches_a_local_read() raises:
    var rt = Runtime[ReadGroup, SumI, Total, Rows](workers=2)
    var got = rt.get(rt.remote[ReadGroup](RowGroup(String(FIXTURE), 2)))
    var want = read_group(RowGroup(String(FIXTURE), 2))
    assert_equal(got.num_rows, want.num_rows)
    assert_equal(got.num_columns(), 5)
    for c in range(5):
        assert_equal(got.name(c), want.name(c))
        assert_equal(String(got.type(c)), String(want.type(c)))
        assert_equal(got.column(c).null_count, want.column(c).null_count)
    var gs = got.column_str(2)
    var ws = want.column_str(2)
    for k in range(got.num_rows):
        assert_equal(gs[1][k], ws[1][k])
        if ws[1][k]:
            assert_equal(gs[0][k], ws[0][k])
    var gf = got.column_f64(1)
    var wf = want.column_f64(1)
    for k in range(got.num_rows):
        if wf[1][k]:
            assert_equal(gf[0][k], wf[0][k])


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

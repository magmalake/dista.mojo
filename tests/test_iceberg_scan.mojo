"""The phase-2 PR-9 gate: a distributed Iceberg scan returns what one
process's scan returns.

The driver plans the scan with iceberg.mojo and sends one task per data file.
Each task reopens the table from its metadata file and reads just that file
with `TableScan.to_batches_for_paths`, so partition pruning, the residual
filter and the delete files attached to it all still apply. The tasks'
batches come back as Arrow, and their rows, sorted, must equal a local
`to_batches` over the whole table.

`pixi run -e data test-iceberg`. The tables are iceberg.mojo's own fixtures,
from a checkout next to this one: partitioned by identity, bucket and day,
position deletes, equality deletes, deletion vectors and schema evolution.
"""

from std.testing import TestSuite, assert_equal, assert_true

from arrow_mlake import (
    AT_BINARY,
    AT_BOOL,
    AT_FLOAT16,
    AT_FLOAT32,
    AT_FLOAT64,
    AT_UTF8,
    ArrayData,
    RecordBatch,
    array_bool,
    array_f64,
    array_i64,
    array_str,
    bit_get,
)
from iceberg import FileIO, TableScan
from iceberg.catalog.filesystem import read_metadata_file
from iceberg.json import parse_json

from dista import ObjectRef, Runtime, Seq, Task

comptime FIXTURES = "../iceberg.mojo/tests/fixtures"
comptime WAREHOUSE = (
    "file:///Users/mseritan/dev/magmalake/iceberg.mojo/build/warehouse-root"
    "/warehouse/db"
)
"""Where the fixtures were generated. Their metadata names every file by
this prefix, and `_io` redirects it to the checkout."""


@fieldwise_init
struct DataFileTask(Copyable, Movable):
    """One data file of a planned scan."""

    var metadata: String
    var filter: String
    var path: String


def _io() -> FileIO:
    var io = FileIO.local()
    io.rebase(String(WAREHOUSE), String(FIXTURES))
    return io^


def _scan(metadata: String, filter: String) raises -> TableScan:
    var io = _io()
    var scan = TableScan(read_metadata_file(io, metadata), io^)
    if filter != "":
        scan = scan.filter(filter)
    return scan^


def scan_file(t: DataFileTask) raises -> Seq[RecordBatch]:
    """The rows one data file contributes to the scan."""
    var paths: List[String] = [t.path.copy()]
    return Seq[RecordBatch](
        _scan(t.metadata, t.filter).to_batches_for_paths(paths)
    )


comptime ScanFile = Task[DataFileTask, Seq[RecordBatch], scan_file]


def _cell(a: ArrayData, i: Int) raises -> String:
    if not bit_get(Span(a.validity), i) and a.null_count > 0:
        return String("null")
    var id = a.type.id
    if id == AT_UTF8 or id == AT_BINARY:
        return array_str(a)[0][i]
    if id == AT_BOOL:
        return String(array_bool(a)[0][i])
    if id == AT_FLOAT16 or id == AT_FLOAT32 or id == AT_FLOAT64:
        return String(array_f64(a)[0][i])
    var w = a.type.fixed_width()
    if w > 0 and w <= 8:
        return String(array_i64(a)[0][i])
    # Decimals, UUIDs and other wide fixed-width values: their bytes.
    var out = String("0x")
    for k in range(w):
        out += hex(Int(a.values[i * w + k]), prefix="")
    return out^


def rows(batches: List[RecordBatch]) raises -> List[String]:
    """Every row as one string, sorted, so two scans compare as sets."""
    var out = List[String]()
    for ref b in batches:
        for r in range(b.num_rows):
            var line = String()
            for c in range(b.num_columns()):
                line += b.name(c) + "=" + _cell(b.column(c), r) + " "
            out.append(line^)
    sort(out)
    return out^


def _current_metadata(table: String) raises -> String:
    var text: String
    with open(String(FIXTURES, "/index.json"), "r") as f:
        text = f.read()
    var idx = parse_json(text)
    var e = idx.get(idx.root, table)
    return String(
        FIXTURES,
        "/",
        table,
        "/metadata/",
        idx.req_string(e, "current_metadata"),
    )


def _check_table(
    mut rt: Runtime[ScanFile], table: String, filter: String = ""
) raises -> Int:
    """Scans `table` one task per data file, checks it against a local scan,
    and returns how many files it read."""
    var metadata = _current_metadata(table)
    var scan = _scan(metadata, filter)
    var tasks = scan.plan_files()
    var refs = List[ObjectRef[Seq[RecordBatch]]]()
    for t in tasks:
        refs.append(
            rt.remote[ScanFile](
                DataFileTask(
                    metadata.copy(), filter.copy(), t.data_file.file_path.copy()
                )
            )
        )
    var distributed = List[RecordBatch]()
    for r in refs:
        var part = rt.get(r)
        for k in range(len(part)):
            distributed.append(part[k])
    var got = rows(distributed)
    var want = rows(scan.to_batches())
    assert_equal(len(got), len(want), table + ": row counts differ")
    for k in range(len(want)):
        assert_equal(got[k], want[k], table + ": rows differ")
    print("   ", table, filter, ":", len(tasks), "files,", len(want), "rows")
    return len(tasks)


def test_a_distributed_scan_equals_a_local_one() raises:
    var rt = Runtime[ScanFile](workers=4)
    var files = 0
    for table in [
        "unpartitioned",
        "ident_part",
        "bucket_part",
        "day_part",
        "trunc_part",
        "evolved",
        "deletes_v2",
        "dv_v3",
        "eq_deletes_v2",
    ]:
        files += _check_table(rt, table)
    assert_true(files > 20, "the fixtures should span many data files")


def test_a_filtered_distributed_scan_equals_a_local_one() raises:
    """With a filter, the plan prunes files and each task applies the
    residual."""
    var rt = Runtime[ScanFile](workers=3)
    _ = _check_table(rt, "day_part", '[">=","id",3]')
    _ = _check_table(rt, "deletes_v2", '["is-null","amount"]')


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

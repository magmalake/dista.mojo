"""Where one taxi file task's time goes: the table's metadata, planning, and
reading one planned data file, each timed on its own.

    TAXI_METADATA=s3://... AWS_*=... pixi run -e data bench-readpath

`READPATH_PREFIX=s3://bucket/warehouse READPATH_LOCAL=<dir>` also reads the
same file from a local copy (its path below the prefix, under `<dir>`),
which leaves only the decode.
"""

from std.os import getenv
from std.time import perf_counter_ns

from iceberg import FileIO, FileScanTask, TableScan
from iceberg.catalog.filesystem import read_metadata_file


def _ms(t0: Int) -> String:
    return String(Float64(Int(perf_counter_ns()) - t0) / 1e6)


def _scan(io: FileIO, metadata: String, cols: List[String]) raises -> TableScan:
    var scan = TableScan(read_metadata_file(io, metadata), io.copy())
    scan = scan.filter('[">","trip_distance",0.0]')
    return scan.select(cols.copy())


def main() raises:
    var metadata = getenv("TAXI_METADATA")
    var io = FileIO.local()
    for i in range(3):
        var t = Int(perf_counter_ns())
        _ = read_metadata_file(io, metadata)
        print("metadata.json read + parse", i, ":", _ms(t), "ms")
    var cols: List[String] = ["trip_distance"]
    var t = Int(perf_counter_ns())
    var tasks = _scan(io, metadata, cols).plan_files()
    print("plan_files:", _ms(t), "ms,", len(tasks), "files")
    var one = tasks[0].copy()
    print(
        "file:",
        one.data_file.file_path,
        one.data_file.file_size_in_bytes,
        "bytes",
    )
    var local = getenv("READPATH_LOCAL")
    if local != "":
        var lio = FileIO.local()
        lio.rebase(getenv("READPATH_PREFIX"), local)
        for i in range(3):
            t = Int(perf_counter_ns())
            _ = read_metadata_file(lio, metadata)
            print("local metadata.json read + parse", i, ":", _ms(t), "ms")
        for c in [1, 4]:
            var wide: List[String] = ["trip_distance"]
            if c == 4:
                wide = [
                    "trip_distance",
                    "fare_amount",
                    "tip_amount",
                    "PULocationID",
                ]
            for i in range(3):
                var scan = TableScan(
                    read_metadata_file(lio, metadata), lio.copy()
                )
                scan = scan.filter('[">","trip_distance",0.0]').select(
                    wide.copy()
                )
                t = Int(perf_counter_ns())
                var rows = 0
                var ts: List[FileScanTask] = [one.copy()]
                for b in scan.to_batches_for_tasks(ts^):
                    rows += b.num_rows
                print(
                    "local copy, columns",
                    c,
                    "pass",
                    i,
                    ":",
                    _ms(t),
                    "ms,",
                    rows,
                    "rows",
                )
        return
    for c in [1, 4]:
        var wide: List[String] = ["trip_distance"]
        if c == 4:
            wide = [
                "trip_distance",
                "fare_amount",
                "tip_amount",
                "PULocationID",
            ]
        for i in range(3):
            t = Int(perf_counter_ns())
            var scan = _scan(io, metadata, wide)
            var t_read = Int(perf_counter_ns())
            var rows = 0
            var ts: List[FileScanTask] = [one.copy()]
            for b in scan.to_batches_for_tasks(ts^):
                rows += b.num_rows
            print(
                "task, columns",
                c,
                "pass",
                i,
                ": total",
                _ms(t),
                "ms, of which the read",
                _ms(t_read),
                "ms,",
                rows,
                "rows",
            )

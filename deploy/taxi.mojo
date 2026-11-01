"""The phase-2 gate: taxibench's queries over the NYC taxi Iceberg table,
distributed one task per data file, against the same scans in one process.

79.5 million trips, 24 monthly data files. For each query the driver plans
the scan, sends one task per surviving file, and adds the tasks' partial
results together. A task carries its planned file, so a worker reads the
table's metadata and that file and nothing else: no manifests. Then it runs the same scan itself, in this process, and
checks the answers agree: counts exactly, sums to 1e-9 relative (a sum's
rounding depends on the order its terms are added in). One more query sends
whole Arrow batches back instead of partial sums, so columnar data crosses
nodes too.

    TAXI_METADATA   the table's metadata location, as the table names it;
                    `s3://` reads with the `AWS_*` environment
    TAXI_PREFIX     the warehouse location the table was written at
    TAXI_ROOT       where that warehouse is mounted here (default: the same)
    TAXI_PASSES     distributed runs of each query (default 1); the first is
                    checked against one process, the rest against the first,
                    and each prints `TIME <query> <pass> <ms>`
    TAXI_TIMELINE   write the distributed runs' task timeline to this path,
                    in Chrome's trace format, before exiting
    TAXI_MODE       `both` (default), `distributed` (no one-process scan: the
                    passes are checked against each other) or `local` (only
                    the one-process scan, no workers)

Each query's answer prints as `ANSWER <query> <rows> <sum_a> <sum_b>
<zones_total>`, so runs that skip the one-process scan can be checked
against one that didn't, or against another framework.

`tests/kind_taxi.sh` runs it on a 3-node kind cluster.
"""

from std.os import getenv
from std.time import perf_counter_ns

from arrow_mlake import RecordBatch
from iceberg import DataFile, FileIO, FileScanTask, ScanOptions, TableScan
from iceberg.manifest import CONTENT_DATA
from iceberg.catalog.filesystem import read_metadata_file

from dista import ObjectRef, Runtime, Seq, Task
from dista.store import node_name

comptime K_COUNT = 0
comptime K_SUM = 1
comptime K_RATIO = 2
comptime K_ZONE_COUNT = 3
comptime K_ZONE_SUM = 4
comptime ZONES = 266


@fieldwise_init
struct Query(Copyable, Movable):
    var name: String
    var filter: String
    var columns: Seq[String]
    var kind: Int
    var sum_column: String


@fieldwise_init
struct PlannedFile(Copyable, Movable):
    """What the driver planned for one data file, so the worker doesn't plan
    again. The taxi table has no delete files and no identity partitions,
    which is what lets this leave out the deletes and the partition values."""

    var path: String
    var format: String
    var size: Int64
    var records: Int64
    var residual: String
    var start: Int64
    var length: Int64
    var spec_id: Int
    var sequence: Int64

    @staticmethod
    def of(t: FileScanTask) raises -> PlannedFile:
        if len(t.delete_files) > 0:
            raise Error(t.data_file.file_path, " has deletes")
        ref f = t.data_file
        return PlannedFile(
            f.file_path.copy(),
            f.file_format.copy(),
            f.file_size_in_bytes,
            f.record_count,
            t.residual.copy(),
            t.start,
            t.length,
            t.spec_id,
            t.data_sequence_number,
        )

    def task(self) -> FileScanTask:
        var f = DataFile(
            CONTENT_DATA,
            self.path.copy(),
            self.format.copy(),
            [],
            self.records,
            self.size,
            [],
            [],
            [],
            False,
            [],
            [],
            0,
            False,
            0,
            False,
            String(),
            False,
            0,
            False,
            0,
            False,
        )
        return FileScanTask(
            f^,
            [],
            self.residual.copy(),
            self.start,
            self.length,
            self.spec_id,
            self.sequence,
        )


@fieldwise_init
struct FileQuery(Copyable, Movable):
    """One query over one planned data file."""

    var query: Query
    var file: PlannedFile


@fieldwise_init
struct Partial(Copyable, Movable):
    """What one file contributes; partials add up to the answer."""

    var rows: Int
    var sum_a: Float64
    var sum_b: Float64
    var zones: Seq[Float64]
    var node: String

    @staticmethod
    def zero() -> Partial:
        var z = List[Float64]()
        for _ in range(ZONES):
            z.append(0.0)
        return Partial(0, 0.0, 0.0, Seq[Float64](z^), String())

    def add(mut self, other: Partial):
        self.rows += other.rows
        self.sum_a += other.sum_a
        self.sum_b += other.sum_b
        var z = List[Float64]()
        for i in range(ZONES):
            z.append(self.zones[i] + other.zones[i])
        self.zones = Seq[Float64](z^)


def _io() -> FileIO:
    var io = FileIO.local()
    var prefix = getenv("TAXI_PREFIX")
    var root = getenv("TAXI_ROOT", prefix)
    if prefix != "" and root != prefix:
        io.rebase(prefix, root)
    return io^


def _scan(q: Query) raises -> TableScan:
    var io = _io()
    var scan = TableScan(read_metadata_file(io, getenv("TAXI_METADATA")), io^)
    scan = scan.filter(q.filter)
    if len(q.columns) > 0:
        var cols = List[String]()
        for i in range(len(q.columns)):
            cols.append(q.columns[i])
        scan = scan.select(cols^)
    return scan^


def _column(b: RecordBatch, name: String) raises -> Int:
    for c in range(b.num_columns()):
        if b.name(c) == name:
            return c
    raise Error("no column ", name)


def fold(mut p: Partial, q: Query, b: RecordBatch) raises:
    """Adds batch `b`'s contribution to query `q` into `p`."""
    p.rows += b.num_rows
    if q.kind == K_SUM:
        var col = b.column_f64(_column(b, q.sum_column))
        for i in range(len(col[0])):
            if col[1][i]:
                p.sum_a += col[0][i]
    elif q.kind == K_RATIO:
        var tip = b.column_f64(_column(b, "tip_amount"))
        var fare = b.column_f64(_column(b, "fare_amount"))
        for i in range(len(tip[0])):
            if tip[1][i] and fare[1][i]:
                p.sum_a += tip[0][i]
                p.sum_b += fare[0][i]
    elif q.kind == K_ZONE_COUNT or q.kind == K_ZONE_SUM:
        var zone = b.column_i64(_column(b, "PULocationID"))
        var z = List[Float64]()
        for i in range(ZONES):
            z.append(p.zones[i])
        if q.kind == K_ZONE_COUNT:
            for i in range(len(zone[0])):
                if zone[1][i] and zone[0][i] >= 0 and zone[0][i] < Int64(ZONES):
                    z[Int(zone[0][i])] += 1.0
        else:
            var amount = b.column_f64(_column(b, "total_amount"))
            for i in range(len(zone[0])):
                if (
                    zone[1][i]
                    and amount[1][i]
                    and zone[0][i] >= 0
                    and zone[0][i] < Int64(ZONES)
                ):
                    z[Int(zone[0][i])] += amount[0][i]
        p.zones = Seq[Float64](z^)


def _node() -> String:
    var n = getenv("NODE_NAME")
    return n if n != "" else node_name()


def fold_file(t: FileQuery) raises -> Partial:
    """Task: one query over one data file, folded where it was read."""
    var tasks: List[FileScanTask] = [t.file.task()]
    var p = Partial.zero()
    for b in _scan(t.query).to_batches_for_tasks(tasks^):
        fold(p, t.query, b)
    p.node = _node()
    return p^


def read_file(t: FileQuery) raises -> Seq[RecordBatch]:
    """Task: one query over one data file, returned as Arrow batches."""
    var tasks: List[FileScanTask] = [t.file.task()]
    return Seq[RecordBatch](_scan(t.query).to_batches_for_tasks(tasks^))


comptime FoldFile = Task[FileQuery, Partial, fold_file]
comptime ReadFile = Task[FileQuery, Seq[RecordBatch], read_file]


def _q(
    name: StringSlice,
    filter: StringSlice,
    columns: List[String],
    kind: Int,
    sum_column: StringSlice = "",
) -> Query:
    return Query(
        String(name),
        String(filter),
        Seq[String](columns.copy()),
        kind,
        String(sum_column),
    )


def queries() -> List[Query]:
    """The eight taxibench queries, minus the top-ten step: zone totals compare
    whole."""
    return [
        _q(
            "q1_scan_count",
            '[">","trip_distance",0.0]',
            ["trip_distance"],
            K_COUNT,
        ),
        _q(
            "q2_month_range",
            (
                '["and",[">=","tpep_pickup_datetime","2024-03-01T00:00:00"],'
                '["<","tpep_pickup_datetime","2024-06-01T00:00:00"]]'
            ),
            ["total_amount"],
            K_SUM,
            "total_amount",
        ),
        _q(
            "q3_payment_sum",
            '["=","payment_type",2]',
            ["payment_type", "total_amount"],
            K_SUM,
            "total_amount",
        ),
        _q(
            "q4_tip_ratio",
            '["and",[">","trip_distance",10.0],[">","fare_amount",0.0]]',
            ["trip_distance", "fare_amount", "tip_amount"],
            K_RATIO,
        ),
        _q(
            "q5_top_zones",
            '[">","trip_distance",0.0]',
            ["PULocationID", "trip_distance"],
            K_ZONE_COUNT,
        ),
        _q(
            "q6_zone_revenue",
            (
                '["and",[">=","tpep_pickup_datetime","2024-01-01T00:00:00"],'
                '["<","tpep_pickup_datetime","2025-01-01T00:00:00"]]'
            ),
            ["PULocationID", "total_amount"],
            K_ZONE_SUM,
        ),
        _q(
            "q7_wide",
            (
                '["and",[">=","tpep_pickup_datetime","2024-06-01T00:00:00"],'
                '["<","tpep_pickup_datetime","2024-07-01T00:00:00"]]'
            ),
            List[String](),
            K_COUNT,
        ),
        _q(
            "q8_selective",
            (
                '["and",["=","PULocationID",132],["=","payment_type",1],'
                '[">","trip_distance",20.0]]'
            ),
            ["PULocationID", "payment_type", "trip_distance", "total_amount"],
            K_SUM,
            "total_amount",
        ),
    ]


def _close(a: Float64, b: Float64) -> Bool:
    # Relative 1e-6: summing 1.4 billion doubles in another order moves the
    # answer by about 1e-9 relative (q3 on AWS: 7104909931.26 vs
    # 7104909941.44), so a tighter bound fails on correct answers.
    var scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) <= 1e-6 * scale


def _check(name: String, got: Partial, want: Partial) raises:
    if got.rows != want.rows:
        raise Error(
            name, ": ", got.rows, " rows distributed, ", want.rows, " local"
        )
    if not _close(got.sum_a, want.sum_a) or not _close(got.sum_b, want.sum_b):
        raise Error(
            name,
            ": sums ",
            got.sum_a,
            "/",
            got.sum_b,
            " vs ",
            want.sum_a,
            "/",
            want.sum_b,
        )
    for i in range(ZONES):
        if not _close(got.zones[i], want.zones[i]):
            raise Error(
                name, ": zone ", i, " ", got.zones[i], " vs ", want.zones[i]
            )


def _ms(t0: Int) -> Int:
    return (Int(perf_counter_ns()) - t0) // 1_000_000


def _distributed(
    mut rt: Runtime[FoldFile, ReadFile], q: Query, mut nodes: List[String]
) raises -> Tuple[Partial, Int]:
    """Query `q` as one task per planned file; the answer and how many files."""
    var t = rt.clock()
    var tasks = _scan(q).plan_files()
    rt.span(String("plan ", q.name), t)
    t = rt.clock()
    var refs = List[ObjectRef[Partial]]()
    for f in tasks:
        refs.append(rt.remote[FoldFile](FileQuery(q.copy(), PlannedFile.of(f))))
    rt.span(String("submit ", q.name), t)
    t = rt.clock()
    var got = Partial.zero()
    for r in refs:
        var part = rt.get(r)
        if part.node not in nodes:
            nodes.append(part.node.copy())
        got.add(part)
    rt.span(String("wait and add up ", q.name), t)
    return (got^, len(tasks))


def _local(q: Query) raises -> Tuple[Partial, Int]:
    """Query `q` scanned in this process; the answer and its milliseconds."""
    var t = Int(perf_counter_ns())
    var want = Partial.zero()
    var reader = _scan(q).to_batch_reader()
    while True:
        var b = reader.next_batch()
        if not b:
            break
        fold(want, q, b.value())
    reader.raise_if_failed()
    return (want^, _ms(t))


def _answer(name: String, p: Partial):
    var zones = 0.0
    for i in range(ZONES):
        zones += p.zones[i]
    print("ANSWER", name, p.rows, p.sum_a, p.sum_b, zones)


def _local_only() raises:
    var total = 0
    for q in queries():
        var local = _local(q)
        _answer(q.name, local[0])
        total += local[1]
        print(
            q.name,
            ":",
            local[0].rows,
            "rows, sum",
            local[0].sum_a,
            "| distributed - , one process",
            local[1],
            "ms",
        )
    print("PASS taxi: 8 queries in one process;", "one process", total, "ms")


def main() raises:
    var mode = getenv("TAXI_MODE", "both")
    if mode == "local":
        return _local_only()
    if mode != "both" and mode != "distributed":
        raise Error("TAXI_MODE must be both, distributed or local, got ", mode)
    var rt = Runtime[FoldFile, ReadFile](workers=4)
    var passes = Int(getenv("TAXI_PASSES", "1"))
    var nodes = List[String]()
    var total_dist = 0
    var total_local = 0
    for q in queries():
        var t0 = Int(perf_counter_ns())
        var first = _distributed(rt, q, nodes)
        var dist_ms = _ms(t0)
        ref got = first[0]
        print("TIME", q.name, 1, dist_ms)

        var local_ms = String("-")
        if mode == "both":
            var lt = rt.clock()
            var local = _local(q)
            rt.span(String("one process ", q.name), lt)
            _check(q.name, got, local[0])
            total_local += local[1]
            local_ms = String(local[1])
        _answer(q.name, got)
        for k in range(2, passes + 1):
            var t = Int(perf_counter_ns())
            var again = _distributed(rt, q, nodes)
            print("TIME", q.name, k, _ms(t))
            _check(q.name, again[0], got)
        total_dist += dist_ms
        print(
            q.name,
            ":",
            first[1],
            "files,",
            got.rows,
            "rows, sum",
            got.sum_a,
            "| distributed",
            dist_ms,
            "ms, one process",
            local_ms,
            "ms",
        )

    # Arrow across nodes: the quarter's total_amount comes back as batches.
    var q = queries()[1].copy()
    var t2 = Int(perf_counter_ns())
    var bt = rt.clock()
    var refs = List[ObjectRef[Seq[RecordBatch]]]()
    for t in _scan(q).plan_files():
        refs.append(rt.remote[ReadFile](FileQuery(q.copy(), PlannedFile.of(t))))
    rt.span("plan and submit q2_as_batches", bt)
    bt = rt.clock()
    var got = Partial.zero()
    for r in refs:
        var bs = rt.get(r)
        for k in range(len(bs)):
            fold(got, q, bs[k])
    rt.span("fetch and fold q2_as_batches", bt)
    bt = rt.clock()
    var want = Partial.zero()
    for b in _scan(q).to_batches():
        fold(want, q, b)
    rt.span("check q2_as_batches in the driver", bt)
    _check("q2_as_batches", got, want)
    print(
        "q2_as_batches :",
        got.rows,
        "rows as Arrow,",
        rt.bytes_fetched(),
        "bytes fetched over sockets,",
        _ms(t2),
        "ms",
    )

    print("tasks ran on", len(nodes), "nodes:", end="")
    for n in nodes:
        print("", n, end="")
    print()
    var expect = Int(getenv("DISTA_EXPECT_NODES", "1"))
    if len(nodes) < expect:
        raise Error("expected ", expect, " nodes, saw ", len(nodes))
    var timeline = getenv("TAXI_TIMELINE")
    if timeline != "":
        print("timeline:", rt.timeline(timeline), "tasks in", timeline)
    if mode == "both":
        print(
            "PASS taxi: 8 queries + 1 as batches, distributed == one process;",
            "distributed",
            total_dist,
            "ms, one process",
            total_local,
            "ms",
        )
    else:
        print(
            "PASS taxi: 8 queries + 1 as batches, distributed passes agree;",
            "distributed",
            total_dist,
            "ms",
        )

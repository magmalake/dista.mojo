"""The shuffle benchmark on dista: trips by zone pair and hour, as map tasks
partitioning by key and reduce tasks taking their partition from every map
task. The query, its answer and its settings are in bench/shuffle/README.md.

A map task (`MapZones`, a `TaskMany`) reads one data file, computes each
trip's features, adds them into dense arrays indexed by key, and returns the
groups it saw split into `R` partitions, each its own object, left on its
worker. Reducer `j` (`ReduceZones`, a `TaskWith`) takes its partition from
every map task, merges them, and returns its totals and its top 10. The
driver adds those up. With `TAXI_MODE=both` the same per-trip code then runs
over the whole table in this process, and the answers must agree.

Build with `-I deploy`: the table code is `taxi_table.mojo`'s.
"""

from std.os import getenv
from std.time import perf_counter_ns

from arrow_mlake import RecordBatch
from iceberg import FileScanTask, ScanOptions, TableMetadata, TableScan
from iceberg.manifest import ManifestCache

from dista import Codec, ObjectRef, Runtime, Seq, TaskMany, TaskWith
from dista.codec import get_scalar, put_scalar

from taxi_table import PlannedFile, taxi_io, taxi_table

comptime ZONES = 266
comptime HOURS = 24
comptime KEYS = ZONES * ZONES * HOURS
comptime HOUR_US: Int64 = 3_600_000_000
comptime MAX_DURATION_US: Int64 = 4 * HOUR_US
comptime TOP = 10
comptime SLOT = 4


def _columns() -> List[String]:
    return [
        "PULocationID",
        "DOLocationID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "tip_amount",
    ]


struct Groups(Codec, Copyable, Movable, Sized):
    """Groups by key, as parallel columns: a map task's partition, or what a
    reducer saw. Encoded as a count, then each column's raw values."""

    var keys: List[Int]
    var n: List[Int]
    var speed: List[Float64]
    var tip: List[Float64]
    var fare: List[Float64]

    def __init__(out self):
        self.keys = List[Int]()
        self.n = List[Int]()
        self.speed = List[Float64]()
        self.tip = List[Float64]()
        self.fare = List[Float64]()

    def __len__(self) -> Int:
        return len(self.keys)

    def append(
        mut self, key: Int, n: Int, speed: Float64, tip: Float64, fare: Float64
    ):
        self.keys.append(key)
        self.n.append(n)
        self.speed.append(speed)
        self.tip.append(tip)
        self.fare.append(fare)

    def encode(self, mut out: List[UInt8]):
        var count = len(self.keys)
        put_scalar[DType.int64](out, Int64(count))
        out.reserve(len(out) + count * 40)
        for i in range(count):
            put_scalar[DType.int64](out, Int64(self.keys[i]))
        for i in range(count):
            put_scalar[DType.int64](out, Int64(self.n[i]))
        for i in range(count):
            put_scalar[DType.float64](out, self.speed[i])
        for i in range(count):
            put_scalar[DType.float64](out, self.tip[i])
        for i in range(count):
            put_scalar[DType.float64](out, self.fare[i])

    @staticmethod
    def decode(buf: Span[UInt8, _], mut pos: Int) raises -> Self:
        var count = Int(get_scalar[DType.int64](buf, pos))
        if count < 0 or count > (len(buf) - pos) // 40:
            raise Error("decode: bad Groups count ", count)
        var g = Self()
        g.keys.reserve(count)
        g.n.reserve(count)
        g.speed.reserve(count)
        g.tip.reserve(count)
        g.fare.reserve(count)
        for _ in range(count):
            g.keys.append(Int(get_scalar[DType.int64](buf, pos)))
        for _ in range(count):
            g.n.append(Int(get_scalar[DType.int64](buf, pos)))
        for _ in range(count):
            g.speed.append(get_scalar[DType.float64](buf, pos))
        for _ in range(count):
            g.tip.append(get_scalar[DType.float64](buf, pos))
        for _ in range(count):
            g.fare.append(get_scalar[DType.float64](buf, pos))
        return g^


struct Dense(Movable):
    """Every key's group, indexed by key: what a map task, or the one
    process, adds trips into.

    A key's four numbers sit together (`SLOT` floats: n, speed, tip, fare),
    so adding a trip touches one cache line, not four arrays' worth. Counts
    are stored as floats, exact far past any table's trip count.
    """

    var slots: List[Float64]
    var keys: List[Int]
    """Scratch for `add`: the batch's kept trips' keys..."""
    var weights: List[Float64]
    """...and their speed, tip and fare, three per kept trip."""

    def __init__(out self):
        self.slots = List[Float64](length=KEYS * SLOT, fill=0.0)
        self.keys = List[Int]()
        self.weights = List[Float64]()

    def n(self, key: Int) -> Int:
        return Int(self.slots[key * SLOT])

    def speed(self, key: Int) -> Float64:
        return self.slots[key * SLOT + 1]

    def tip(self, key: Int) -> Float64:
        return self.slots[key * SLOT + 2]

    def fare(self, key: Int) -> Float64:
        return self.slots[key * SLOT + 3]

    def add(mut self, b: RecordBatch) raises:
        """Adds batch `b`'s trips that count (see the README), in two
        passes: first every row's key and features, without branches, keeping
        the trips that count; then those added into their keys' slots."""
        var pu = b.column_i64(_column(b, "PULocationID"))
        var do_ = b.column_i64(_column(b, "DOLocationID"))
        var start = b.column_i64(_column(b, "tpep_pickup_datetime"))
        var end = b.column_i64(_column(b, "tpep_dropoff_datetime"))
        var dist = b.column_f64(_column(b, "trip_distance"))
        var fare = b.column_f64(_column(b, "fare_amount"))
        var tip = b.column_f64(_column(b, "tip_amount"))
        var rows = b.num_rows
        self.keys.resize(rows, 0)
        self.weights.resize(rows * 3, 0.0)
        var keys = self.keys.unsafe_ptr()
        var weights = self.weights.unsafe_ptr()
        # Pass 1. Every row is written at `kept`, which only moves past the
        # trips that count, so the dropped ones are overwritten.
        var pu_v = pu[0].unsafe_ptr()
        var pu_ok = pu[1].unsafe_ptr()
        var do_v = do_[0].unsafe_ptr()
        var do_ok = do_[1].unsafe_ptr()
        var start_v = start[0].unsafe_ptr()
        var start_ok = start[1].unsafe_ptr()
        var end_v = end[0].unsafe_ptr()
        var end_ok = end[1].unsafe_ptr()
        var dist_v = dist[0].unsafe_ptr()
        var dist_ok = dist[1].unsafe_ptr()
        var fare_v = fare[0].unsafe_ptr()
        var fare_ok = fare[1].unsafe_ptr()
        var tip_v = tip[0].unsafe_ptr()
        var tip_ok = tip[1].unsafe_ptr()
        var kept = 0
        for i in range(rows):
            var p = pu_v[unsafe_offset=i]
            var d = do_v[unsafe_offset=i]
            var miles = dist_v[unsafe_offset=i]
            var dollars = fare_v[unsafe_offset=i]
            var t = start_v[unsafe_offset=i]
            var us = end_v[unsafe_offset=i] - t
            var keep = (
                pu_ok[unsafe_offset=i]
                & do_ok[unsafe_offset=i]
                & start_ok[unsafe_offset=i]
                & end_ok[unsafe_offset=i]
                & dist_ok[unsafe_offset=i]
                & fare_ok[unsafe_offset=i]
                & tip_ok[unsafe_offset=i]
                & (p >= 1)
                & (p <= 265)
                & (d >= 1)
                & (d <= 265)
                & (miles > 0.0)
                & (dollars > 0.0)
                & (us > 0)
                & (us <= MAX_DURATION_US)
            )
            var hour = Int((t // HOUR_US) % 24)
            keys[unsafe_offset=kept] = (Int(p) * ZONES + Int(d)) * HOURS + hour
            weights[unsafe_offset=kept * 3] = miles / (
                Float64(us) / Float64(HOUR_US)
            )
            weights[unsafe_offset=kept * 3 + 1] = (
                tip_v[unsafe_offset=i] / dollars
            )
            weights[unsafe_offset=kept * 3 + 2] = dollars
            kept += Int(keep)
        # Pass 2: the kept trips into their slots.
        var slots = self.slots.unsafe_ptr()
        for j in range(kept):
            var s = slots.unsafe_offset(keys[unsafe_offset=j] * SLOT)
            s[unsafe_offset=0] += 1.0
            s[unsafe_offset=1] += weights[unsafe_offset=j * 3]
            s[unsafe_offset=2] += weights[unsafe_offset=j * 3 + 1]
            s[unsafe_offset=3] += weights[unsafe_offset=j * 3 + 2]

    def partitions(self, r: Int) -> Seq[Groups]:
        """The groups with trips, split into `r` partitions by key."""
        var parts = List[Groups]()
        for _ in range(r):
            parts.append(Groups())
        for key in range(KEYS):
            if self.n(key) > 0:
                parts[key % r].append(
                    key,
                    self.n(key),
                    self.speed(key),
                    self.tip(key),
                    self.fare(key),
                )
        return Seq[Groups](parts^)


@fieldwise_init
struct Answer(Copyable, Movable):
    """A reducer's totals and top 10, or the whole query's."""

    var groups: Int
    var trips: Int
    var speed: Float64
    var tip: Float64
    var fare: Float64
    var top: Groups
    """The groups with the most trips, most first, ties to the smaller key."""

    @staticmethod
    def zero() -> Answer:
        return Answer(0, 0, 0.0, 0.0, 0.0, Groups())

    def add(mut self, other: Answer):
        self.groups += other.groups
        self.trips += other.trips
        self.speed += other.speed
        self.tip += other.tip
        self.fare += other.fare
        var both = self.top.copy()
        for i in range(len(other.top)):
            both.append(
                other.top.keys[i],
                other.top.n[i],
                other.top.speed[i],
                other.top.tip[i],
                other.top.fare[i],
            )
        self.top = _top(both)


def _before(g: Groups, a: Int, b: Int) -> Bool:
    """Whether group `a` ranks before group `b`: more trips, then smaller key.
    """
    if g.n[a] != g.n[b]:
        return g.n[a] > g.n[b]
    return g.keys[a] < g.keys[b]


def _top(g: Groups) -> Groups:
    """`g`'s first `TOP` groups by rank."""
    var chosen = List[Int]()
    for i in range(len(g)):
        # Insertion into a list of at most TOP: cheap, and stable.
        var at = len(chosen)
        while at > 0 and _before(g, i, chosen[at - 1]):
            at -= 1
        if at < TOP:
            chosen.insert(at, i)
            if len(chosen) > TOP:
                _ = chosen.pop()
    var out = Groups()
    for i in chosen:
        out.append(g.keys[i], g.n[i], g.speed[i], g.tip[i], g.fare[i])
    return out^


def _answer_of(dense: Dense) -> Answer:
    """The whole query's answer from every key's group, in one process."""
    var a = Answer.zero()
    var all = Groups()
    for key in range(KEYS):
        if dense.n(key) > 0:
            a.groups += 1
            a.trips += dense.n(key)
            a.speed += dense.speed(key)
            a.tip += dense.tip(key)
            a.fare += dense.fare(key)
            all.append(
                key,
                dense.n(key),
                dense.speed(key),
                dense.tip(key),
                dense.fare(key),
            )
    a.top = _top(all)
    return a^


def _column(b: RecordBatch, name: String) raises -> Int:
    for c in range(b.num_columns()):
        if b.name(c) == name:
            return c
    raise Error("no column ", name)


def _scan(var table: TableMetadata) raises -> TableScan:
    var scan = TableScan(table^, taxi_io())
    return scan.select(_columns())


@fieldwise_init
struct MapFile(Copyable, Movable):
    """One map task: a planned data file, the table's metadata for reading,
    and how many partitions to make."""

    var file: PlannedFile
    var metadata: String
    var reducers: Int


def map_zones(t: MapFile) raises -> Seq[Groups]:
    """Map task: one data file's groups, in `t.reducers` partitions."""
    var tasks: List[FileScanTask] = [t.file.task()]
    var dense = Dense()
    for b in _scan(TableMetadata.parse(t.metadata)).to_batches_for_tasks(
        tasks^
    ):
        dense.add(b)
    return dense.partitions(t.reducers)


@fieldwise_init
struct Reducer(Copyable, Movable):
    """Which reducer: partition `j` of `r`."""

    var j: Int
    var r: Int


def reduce_zones(which: Reducer, parts: Seq[Groups]) raises -> Answer:
    """Reducer `j`: partition `j` of every map task, merged."""
    var j = which.j
    var r = which.r
    # Partition j holds keys j, j + r, j + 2r...: slot key // r.
    var slots = KEYS // r + 1
    var n = List[Int](length=slots, fill=0)
    var speed = List[Float64](length=slots, fill=0.0)
    var tip = List[Float64](length=slots, fill=0.0)
    var fare = List[Float64](length=slots, fill=0.0)
    for p in range(len(parts)):
        ref g = parts.items[p]
        for i in range(len(g)):
            var key = g.keys[i]
            if key % r != j:
                raise Error("key ", key, " reached reducer ", j, " of ", r)
            var s = key // r
            n[s] += g.n[i]
            speed[s] += g.speed[i]
            tip[s] += g.tip[i]
            fare[s] += g.fare[i]
    var a = Answer.zero()
    var seen = Groups()
    for s in range(slots):
        if n[s] > 0:
            a.groups += 1
            a.trips += n[s]
            a.speed += speed[s]
            a.tip += tip[s]
            a.fare += fare[s]
            seen.append(s * r + j, n[s], speed[s], tip[s], fare[s])
    a.top = _top(seen)
    return a^


comptime MapZones = TaskMany[MapFile, Groups, map_zones]
comptime ReduceZones = TaskWith[Reducer, Groups, Answer, reduce_zones]
comptime RT = Runtime[MapZones, ReduceZones]


def _ms(t0: Int) -> Int:
    return (Int(perf_counter_ns()) - t0) // 1_000_000


def _distributed(
    mut rt: RT, table: TableMetadata, reading: String, reducers: Int
) raises -> Answer:
    """The query as a shuffle: one map task per data file, `reducers` reduce
    tasks."""
    var t = rt.clock()
    var manifests = ManifestCache()
    var files = _scan(table.copy()).plan_files_with(manifests)
    rt.span("plan", t)
    t = rt.clock()
    var maps = List[List[ObjectRef[Groups]]]()
    for f in files:
        maps.append(
            rt.remote_many[MapZones](
                MapFile(PlannedFile.of(f), reading, reducers), reducers
            )
        )
    var reduces = List[ObjectRef[Answer]]()
    for j in range(reducers):
        var mine = List[ObjectRef[Groups]](capacity=len(maps))
        for m in range(len(maps)):
            mine.append(maps[m][j])
        reduces.append(rt.remote[ReduceZones](Reducer(j, reducers), mine))
    # The reducers hold the partitions until they settle; the driver lets go
    # now, so each is freed once its reducer has it.
    for m in range(len(maps)):
        rt.release(maps[m])
    rt.span("submit", t)
    t = rt.clock()
    var got = Answer.zero()
    for r in reduces:
        got.add(rt.get(r))
    rt.span("wait and add up", t)
    return got^


def _local(table: TableMetadata) raises -> Tuple[Answer, Int]:
    """The query in this process: every trip into one dense array."""
    var t = Int(perf_counter_ns())
    var dense = Dense()
    var opts = ScanOptions()
    opts.num_workers = Int(getenv("TAXI_THREADS", "0"))
    var reader = _scan(table.copy()).to_batch_reader(opts)
    while True:
        var b = reader.next_batch()
        if not b:
            break
        dense.add(b.value())
    reader.raise_if_failed()
    return (_answer_of(dense), _ms(t))


def _close(a: Float64, b: Float64) -> Bool:
    var scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) <= 1e-9 * scale


def _check(what: String, got: Answer, want: Answer) raises:
    if got.groups != want.groups or got.trips != want.trips:
        raise Error(
            what,
            ": ",
            got.groups,
            " groups and ",
            got.trips,
            " trips, wanted ",
            want.groups,
            " and ",
            want.trips,
        )
    if not (
        _close(got.speed, want.speed)
        and _close(got.tip, want.tip)
        and _close(got.fare, want.fare)
    ):
        raise Error(what, ": the sums differ")
    if len(got.top) != len(want.top):
        raise Error(what, ": the top lists differ in length")
    for i in range(len(want.top)):
        if got.top.keys[i] != want.top.keys[i] or got.top.n[i] != want.top.n[i]:
            raise Error(what, ": top ", i + 1, " differs")
        if not _close(got.top.speed[i], want.top.speed[i]):
            raise Error(what, ": top ", i + 1, "'s speed differs")


def _print(a: Answer):
    print("ANSWER zone_hours", a.groups, a.trips, a.speed, a.tip, a.fare)
    for i in range(len(a.top)):
        var key = a.top.keys[i]
        var n = a.top.n[i]
        print(
            "TOP",
            i + 1,
            key,
            key // HOURS // ZONES,
            key // HOURS % ZONES,
            key % HOURS,
            n,
            a.top.speed[i] / Float64(n),
            a.top.tip[i] / Float64(n),
        )


def main() raises:
    var mode = getenv("TAXI_MODE", "both")
    if mode != "both" and mode != "distributed" and mode != "local":
        raise Error("TAXI_MODE must be both, distributed or local, got ", mode)
    var table = taxi_table()
    if mode == "local":
        var local = _local(table)
        _print(local[0])
        print(
            "zone_hours :",
            local[0].groups,
            "groups,",
            local[0].trips,
            "trips | distributed - , one process",
            local[1],
            "ms",
        )
        print("PASS shuffle: zone_hours in one process,", local[1], "ms")
        return
    var rt = RT(workers=4)
    var reading = table.for_reading().to_json()
    var reducers = Int(getenv("SHUFFLE_REDUCERS", "16"))
    var passes = Int(getenv("TAXI_PASSES", "1"))
    var t0 = Int(perf_counter_ns())
    var got = _distributed(rt, table, reading, reducers)
    var dist_ms = _ms(t0)
    print("TIME zone_hours 1", dist_ms)
    var local_ms = String("-")
    if mode == "both":
        var local = _local(table)
        _check("zone_hours", got, local[0])
        local_ms = String(local[1])
    _print(got)
    for k in range(2, passes + 1):
        var t = Int(perf_counter_ns())
        var again = _distributed(rt, table, reading, reducers)
        print("TIME zone_hours", k, _ms(t))
        _check(String("zone_hours pass ", k), again, got)
    print(
        "zone_hours :",
        got.groups,
        "groups,",
        got.trips,
        "trips | distributed",
        dist_ms,
        "ms, one process",
        local_ms,
        "ms",
    )
    var timeline = getenv("TAXI_TIMELINE")
    if timeline != "":
        print("timeline:", rt.timeline(timeline), "tasks in", timeline)
    print(
        "PASS shuffle: zone_hours,",
        reducers,
        "reducers;",
        "checked against one process;" if mode == "both" else "passes agree;",
        "distributed",
        dist_ms,
        "ms, one process",
        local_ms,
        "ms",
    )

"""Sizing a worker from its run's grant: `DISTA_WORKER_SLOTS=auto` (#123).

With `auto`, a worker takes its slots and a memory budget from the grant
morca gives the run: `MORCA_GRANT_CPUS` and `MORCA_GRANT_MEMORY_MB`, shared
by the workers a local-mode `Runtime` forks, and the whole grant for a worker
started on its own (`DISTA_ROLE=worker`). Without a grant, the host's cores,
and no budget.

  - **Slots**: the grant's CPUs, at most its memory over
    `DISTA_TASK_MEMORY_MB` (what one task may use) when that's set; at least 1.
  - **Budget**: the grant's memory. The worker runs as many tasks as fit it
    beside its own memory, each taken at `DISTA_TASK_MEMORY_MB`, or at what
    its footprint shows if more: its resident memory past its own over the
    most tasks it ran at once. Without a size, the first task runs alone
    and its footprint sizes the rest. One task always may run, so a task
    bigger than the budget still runs, alone. The rest wait at
    the head: the worker tells it how many tasks it has room for (its
    *admit*, `KIND_CAPACITY`), and the head sends it no more than that plus
    `pipeline - 1`, so another worker with room takes them.
  - **Store**: with `DISTA_STORE_BYTES` unset, half the budget, so results
    spill to disk only once the grant's memory is in use.

A number in `DISTA_WORKER_SLOTS` works as before: no budget, no admits.

**Mixed versions.** A head and its workers may run different dista
versions mid-deploy, and each drops a peer that sends a frame kind it
doesn't know. So a head that reads `KIND_CAPACITY` says so first: on a
worker's hello it sends `KIND_FREE` of object 0 (`CAPACITY_ANNOUNCE`), which
names no object, so a worker from before ignores it. A worker sends
`KIND_CAPACITY` only after that; under an older head it still holds back
tasks past its budget, though they wait at the worker rather than the head.
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.sys.info import CompilationTarget

from threads import num_cpus

from dista.socket import wall_ns

comptime GRANT_CPUS = "MORCA_GRANT_CPUS"
comptime GRANT_MEMORY_MB = "MORCA_GRANT_MEMORY_MB"
comptime TASK_MEMORY_MB = "DISTA_TASK_MEMORY_MB"
comptime AUTO = "auto"

comptime CAPACITY_ANNOUNCE: UInt64 = 0
"""The object a head's `KIND_FREE` names to say it reads `KIND_CAPACITY`."""

comptime RSS_EVERY_NS: UInt64 = 5_000_000
"""How long a resident-memory reading is reused: the loop may ask often."""

comptime MARGIN_PCT = 85
"""Of the budget beside the worker's own memory, the share tasks sized by a
learned estimate may fill."""

comptime WINDOW_NS: UInt64 = 200_000_000
"""The shortest window the in-flight count is judged over."""
comptime WINDOW_TASKS = 3
"""The fewest tasks a window ends with: twice the count in flight, and at
least this, so the window sees the mix turn over."""
comptime GAIN = 1.05
"""Throughput must rise this much for the count to grow again."""
comptime SLOWER = 1.5
"""Task latency this much higher, throughput flat, backs the count off."""
comptime PRESSURE = 10.0
"""PSI's memory `some avg10` (percent) past which the count backs off."""


def _env_count(name: String) raises -> Int:
    """A non-negative integer from the environment; 0 when unset."""
    var text = getenv(name)
    if text == "":
        return 0
    var n: Int
    try:
        n = Int(text)
    except:
        raise Error(name, " must be a number, got ", text)
    if n < 0:
        raise Error(name, " must be 0 or more, got ", n)
    return n


@fieldwise_init
struct Sizing(Copyable, Movable):
    """What `auto` makes of a grant, for one worker."""

    var slots: Int
    var budget: Int
    """Bytes of resident memory the worker keeps to; 0, none."""
    var task_memory: Int
    """Bytes one task is taken to use; 0, unknown."""
    var base: Int
    """The worker's own resident bytes, before any task ran."""
    var peak_running: Int
    """The most tasks the worker has run at once."""
    var seen: Bool
    """A task has finished, so the footprint counts."""
    var learned: Int
    """The most a task took, of those that ran alone (`Learned`); 0, none
    yet."""

    @staticmethod
    def of(
        cpus: Int, memory_mb: Int, task_memory_mb: Int, sharing: Int
    ) -> Sizing:
        """The sizing from a grant of `cpus` and `memory_mb` (0 for none),
        shared by `sharing` workers."""
        var share = max(sharing, 1)
        var slots = num_cpus()
        if cpus > 0:
            slots = cpus // share
        var budget = (memory_mb << 20) // share
        var task = task_memory_mb << 20
        if budget > 0 and task > 0:
            slots = min(slots, budget // task)
        return Sizing(max(slots, 1), budget, task, 0, 0, False, 0)

    @staticmethod
    def from_env(sharing: Int = 1) raises -> Sizing:
        return Sizing.of(
            _env_count(GRANT_CPUS),
            _env_count(GRANT_MEMORY_MB),
            _env_count(TASK_MEMORY_MB),
            sharing,
        )

    def store_limit(self) -> Int:
        """`DISTA_STORE_BYTES`'s default: half the budget; 0 (no limit)
        without one."""
        return self.budget // 2

    def started(mut self, running: Int):
        """A task started, `running` of them now."""
        self.peak_running = max(self.peak_running, running)

    def finished(mut self):
        """A task finished: the footprint now says what tasks take."""
        self.seen = True

    def estimate(self, rss: Int) -> Int:
        """Bytes one task takes: its size, or more if what the worker saw
        says so: the most a task took running alone, or its resident memory
        past its own over the most tasks it ran at once; 0 while nothing
        says. An allocator keeps memory a task freed (`malloc_trim` doesn't
        return it), so what's resident is the footprint at the peak, which
        later tasks reuse."""
        var seen = self.learned
        if self.seen and self.peak_running > 0 and rss > self.base:
            seen = max(seen, (rss - self.base) // self.peak_running)
        return max(self.task_memory, seen)

    def room(self, running: Int, rss: Int) -> Int:
        """Tasks the worker has room for at `rss` bytes resident: as many as
        fit the budget beside its own memory, between 1 and `slots`. Until
        something says what a task takes, one at a time."""
        if self.budget == 0:
            return self.slots
        var each = self.estimate(rss)
        if each == 0:
            return 1
        var free = self.budget - self.base
        if each > self.task_memory:
            # Learned, not given: keep a margin for the task bigger than
            # any seen.
            free = free * MARGIN_PCT // 100
        var n = free // each
        return max(min(n, self.slots), 1)

    def fits(self, running: Int, rss: Int) -> Bool:
        """Whether one more task may start now: one always may."""
        return running == 0 or running < self.room(running, rss)


struct InFlight(Copyable, Movable):
    """How many tasks a worker sized by `auto` keeps in flight: it starts at
    one and, window by window, doubles while throughput rises (a slow start),
    then adds one at a time; it backs off by a quarter when memory runs out,
    PSI shows memory pressure, results spill, or tasks slow down without
    throughput rising. Never past what fits the budget (`Sizing.room`)."""

    var limit: Int
    var slots: Int
    var slow_start: Bool
    var changes: Int
    var why: String
    """Why the count last changed."""
    var _start: UInt64
    var _done: Int
    var _latency: UInt64
    var _busy: Bool
    """The count was reached in this window: more might have run."""
    var _spills: Int
    var _rate: Float64
    """The window before's throughput, tasks a second; 0, none."""
    var _mean: Float64
    """The window before's mean task latency, seconds."""

    def __init__(out self, slots: Int):
        self.limit = 1
        self.slots = max(slots, 1)
        self.slow_start = True
        self.changes = 0
        self.why = String()
        self._start = 0
        self._done = 0
        self._latency = 0
        self._busy = False
        self._spills = 0
        self._rate = 0.0
        self._mean = 0.0

    def finished(mut self, latency_ns: UInt64):
        self._done += 1
        self._latency += latency_ns

    def saw(mut self, running: Int, now: UInt64, spills: Int):
        """The loop's view: `running` tasks started, `spills` so far."""
        if self._start == 0:
            self._start = now
            self._spills = spills
        if running >= self.limit:
            self._busy = True

    def due(self, now: UInt64) -> Bool:
        """Whether the window has run long enough to judge."""
        return (
            self._start != 0
            and self._done >= max(WINDOW_TASKS, 2 * self.limit)
            and now - self._start >= WINDOW_NS
        )

    def _set(mut self, n: Int, var why: String) -> Bool:
        var m = max(min(n, self.slots), 1)
        if m == self.limit:
            return False
        self.limit = m
        self.why = why^
        self.changes += 1
        return True

    def _back_off(mut self, fit: Int, var why: String) -> Bool:
        self.slow_start = False
        return self._set(min(self.limit - max(self.limit // 4, 1), fit), why^)

    def cap(mut self, fit: Int) -> Bool:
        """Lowers the count at once to what fits the budget."""
        if fit < self.limit:
            self.slow_start = False
            return self._set(fit, "memory: no room for more")
        return False

    def close(
        mut self, now: UInt64, spills: Int, pressure: Float64, fit: Int
    ) -> Bool:
        """Ends the window: judges it, maybe changes the count (`why` says
        why), and starts the next. Whether the count changed."""
        var secs = Float64(now - self._start) / 1e9
        var rate = Float64(self._done) / secs
        var mean = Float64(self._latency) / 1e9 / Float64(self._done)
        var changed: Bool
        if fit < self.limit:
            changed = self.cap(fit)
        elif pressure > PRESSURE:
            changed = self._back_off(
                fit, String("memory pressure, PSI some ", pressure, "%")
            )
        elif spills > self._spills:
            changed = self._back_off(fit, "results spilled")
        elif (
            self._rate > 0.0
            and mean > self._mean * SLOWER
            and rate < self._rate * GAIN
        ):
            changed = self._back_off(fit, "tasks slowed, throughput flat")
        elif self._busy and (self._rate == 0.0 or rate >= self._rate * GAIN):
            var n = self.limit * 2 if self.slow_start else self.limit + 1
            changed = self._set(
                min(n, fit),
                String(
                    "throughput rising, ",
                    Float64(Int(rate * 100.0)) / 100.0,
                    " tasks/s",
                ),
            )
            if not changed:
                self.slow_start = False
        elif not self._busy:
            # Idle: the count wasn't reached, so the window says nothing
            # about more. Keep the window before as the one to beat.
            self._next(now, spills)
            return False
        else:
            # Flat: hold here.
            self.slow_start = False
            changed = False
        self._rate = rate
        self._mean = mean
        self._next(now, spills)
        return changed

    def _next(mut self, now: UInt64, spills: Int):
        self._start = now
        self._done = 0
        self._latency = 0
        self._busy = False
        self._spills = spills


def memory_pressure() -> Float64:
    """PSI's memory `some avg10`, in percent, for this process's cgroup (or
    the host's); -1 where there's none: macOS, or a kernel without PSI."""
    comptime if CompilationTarget.is_macos():
        return -1.0
    else:
        var paths = List[String]()
        try:
            with open("/proc/self/cgroup", "r") as f:
                for line in f.read().splitlines():
                    if line.startswith("0::"):
                        var rel = String(line[byte=3:])
                        paths.append(
                            String("/sys/fs/cgroup", rel, "/memory.pressure")
                        )
        except:
            pass
        paths.append("/proc/pressure/memory")
        for path in paths:
            try:
                var text: String
                with open(path, "r") as f:
                    text = f.read()
                return parse_pressure(text)
            except:
                continue
        return -1.0


def parse_pressure(text: String) raises -> Float64:
    """`some avg10` from a PSI file's text."""
    for line in text.splitlines():
        if not line.startswith("some "):
            continue
        for field in line.split(" "):
            if field.startswith("avg10="):
                return Float64(String(field[byte=6:]))
    raise Error("no some avg10 in PSI")


struct Learned(Movable):
    """What each kind of task took running alone: the resident memory it
    added, the most seen. Alone, so what it added is its own; since freed
    memory stays resident and is reused, a later run may add nothing, and
    the most seen is kept."""

    var peak: Dict[UInt64, Int]
    var _watch: Dict[UInt64, Tuple[UInt64, Int, Int, Bool]]
    """Running tasks, by object: kind, resident at start, the most resident
    since, and whether it's run alone."""

    def __init__(out self):
        self.peak = Dict[UInt64, Int]()
        self._watch = Dict[UInt64, Tuple[UInt64, Int, Int, Bool]]()

    def started(mut self, obj: UInt64, kind: UInt64, rss: Int):
        var alone = len(self._watch) == 0
        for key in self._keys():
            try:
                var w = self._watch[key]
                self._watch[key] = (w[0], w[1], w[2], False)
            except:
                pass
        self._watch[obj] = (kind, rss, rss, alone)

    def sample(mut self, rss: Int):
        for key in self._keys():
            try:
                var w = self._watch[key]
                if rss > w[2]:
                    self._watch[key] = (w[0], w[1], rss, w[3])
            except:
                pass

    def _keys(self) -> List[UInt64]:
        var keys = List[UInt64]()
        for key in self._watch:
            keys.append(key)
        return keys^

    def finished(mut self, obj: UInt64, rss: Int):
        try:
            var w = self._watch.pop(obj)
            if w[3]:
                var took = max(w[2], rss) - w[1]
                self.peak[w[0]] = max(self.peak.get(w[0], 0), took)
        except:
            pass

    def most(self) -> Int:
        """The most any kind took."""
        var n = 0
        for entry in self.peak.items():
            n = max(n, entry.value)
        return n


def wants_auto() -> Bool:
    """Whether `DISTA_WORKER_SLOTS` is `auto`."""
    return getenv("DISTA_WORKER_SLOTS") == AUTO


def resident_bytes() -> Int:
    """This process's resident memory, in bytes; 0 if it can't be read."""
    comptime if CompilationTarget.is_macos():
        # PROC_PIDTASKINFO (4): struct proc_taskinfo, whose second field is
        # pti_resident_size.
        var info = List[UInt8](length=96, fill=0)
        var pid = external_call["getpid", c_int]()
        var n = external_call["proc_pidinfo", c_int](
            pid, c_int(4), UInt64(0), info.unsafe_ptr(), c_int(96)
        )
        if n < 16:
            return 0
        var rss = 0
        for k in range(8):
            rss |= Int(info[8 + k]) << (8 * k)
        return rss
    else:
        # /proc/self/statm: total and resident pages.
        try:
            var text: String
            with open("/proc/self/statm", "r") as f:
                text = f.read()
            var fields = text.split()
            if len(fields) < 2:
                return 0
            var page = Int(external_call["getpagesize", c_int]())
            return Int(fields[1]) * page
        except:
            return 0


struct Resident(Movable):
    """Resident memory, read at most every `RSS_EVERY_NS`."""

    var bytes: Int
    var _at: UInt64

    def __init__(out self):
        self.bytes = 0
        self._at = 0

    def now(mut self) -> Int:
        var t = wall_ns()
        if self._at == 0 or t - self._at >= RSS_EVERY_NS:
            self.bytes = resident_bytes()
            self._at = t
        return self.bytes

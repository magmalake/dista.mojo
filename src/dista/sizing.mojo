"""Sizing a worker from its run's grant: `DISTA_WORKER_SLOTS=auto` (#123).

With `auto`, a worker takes its slots and a memory budget from the grant
morca gives the run: `MORCA_GRANT_CPUS` and `MORCA_GRANT_MEMORY_MB`, shared
by the workers a local-mode `Runtime` forks, and the whole grant for a worker
started on its own (`DISTA_ROLE=worker`). Without a grant, the host's cores,
and no budget.

  - **Slots**: the grant's CPUs, at most its memory over
    `DISTA_TASK_MEMORY_MB` (what one task may use) when that's set; at least 1.
  - **Budget**: the grant's memory. The worker starts another task only while
    its resident memory plus one task's estimate fits; one task always may
    run, so a task bigger than the budget still runs, alone. The rest wait at
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
        return Sizing(max(slots, 1), budget, task)

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

    def _used(self, running: Int, rss: Int) -> Int:
        """The memory taken: what's resident, or if more, what the running
        tasks are expected to take, since one just started hasn't yet."""
        return max(rss, running * self.task_memory)

    def room(self, running: Int, rss: Int) -> Int:
        """Tasks the worker has room for, `running` of them started, at
        `rss` bytes resident: its admit, between 1 and `slots`."""
        if self.budget == 0:
            return self.slots
        var used = self._used(running, rss)
        var more: Int
        if used >= self.budget:
            more = 0
        elif self.task_memory == 0:
            # Nothing says what a task needs: one more at a time while
            # there's room.
            more = 1
        else:
            more = (self.budget - used) // self.task_memory
        return max(min(running + more, self.slots), 1)

    def fits(self, running: Int, rss: Int) -> Bool:
        """Whether one more task may start now."""
        if running == 0 or self.budget == 0:
            return True
        return self._used(running, rss) + self.task_memory <= self.budget


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

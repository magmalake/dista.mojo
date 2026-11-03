"""How many workers a running job wants: a policy over the head's demand.

dista starts no machines. Whatever owns them (orca's `ec2` runner, a shell
loop, a Kubernetes operator) reads the head's demand, `dista status --json`,
asks `decide` what to do, starts that many workers and drains the ones it
names with `dista drain`:

    var d = Demand.parse(observer.status_json())
    var got = decide(d, Policy(min_workers=0, max_workers=12), now, pending,
                     short_since)
    # start got.start workers; drain each of got.drain; keep got.short_since

`decide` has no clock and no I/O: `now` is the provider's, in seconds, and
`short_since`, when the job first ran short of workers, comes back in the
decision for the next call. So it's tested on tables of demand over time.

Up: tasks ready with no free slot for `up_after` seconds start workers, at
most `max_step` at once, up to `max_workers`; with no worker at all, they
start at once. Workers asked for but not yet joined (`pending`) count as
there, so a slow boot isn't asked for twice.

Down: workers idle for `idle_before_down` seconds drain, fewest held bytes
first, down to `min_workers`, which may be 0. A worker hosting an actor
drains only with a checkpoint store to move it through; and the last worker
drains only once no task is left and it holds nothing, since its results
would have nowhere to go.
"""


@fieldwise_init
struct WorkerDemand(Copyable, Movable):
    """One worker, as the head sees it."""

    var address: String
    """Its object server, `ip:port`: what `dista drain` takes."""
    var running: Int
    """Tasks in flight on it."""
    var idle_seconds: Float64
    """How long it's had nothing to do; 0 while it has."""
    var held_bytes: Int
    """Bytes of results stored on it."""
    var actor: UInt64
    """The actor it hosts, or 0."""
    var draining: Bool


struct Demand(Copyable, Movable):
    """The head's demand, from `dista status --json`."""

    var tasks_ready: Int
    """Tasks whose inputs are ready, waiting for a worker."""
    var tasks_waiting: Int
    """Tasks waiting for their inputs."""
    var tasks_running: Int
    var actors_waiting: Int
    """Actors waiting for a worker of their own."""
    var pipeline: Int
    """Tasks the head keeps in flight per worker."""
    var checkpoints: Bool
    """Whether actors can move, through a checkpoint store."""
    var workers: List[WorkerDemand]

    def __init__(
        out self,
        tasks_ready: Int = 0,
        tasks_waiting: Int = 0,
        tasks_running: Int = 0,
        actors_waiting: Int = 0,
        pipeline: Int = 2,
        checkpoints: Bool = False,
        var workers: List[WorkerDemand] = List[WorkerDemand](),
    ):
        self.tasks_ready = tasks_ready
        self.tasks_waiting = tasks_waiting
        self.tasks_running = tasks_running
        self.actors_waiting = actors_waiting
        self.pipeline = pipeline
        self.checkpoints = checkpoints
        self.workers = workers^

    @staticmethod
    def parse(text: String) raises -> Demand:
        """Reads `dista status --json`'s object. Unknown fields are skipped,
        so a newer head's output still reads."""
        var p = _Reader(text)
        var d = Demand()
        p.expect("{")
        if p.take("}"):
            return d^
        while True:
            var key = p.string()
            p.expect(":")
            if key == "tasks_ready":
                d.tasks_ready = Int(p.number())
            elif key == "tasks_waiting":
                d.tasks_waiting = Int(p.number())
            elif key == "tasks_running":
                d.tasks_running = Int(p.number())
            elif key == "actors_waiting":
                d.actors_waiting = Int(p.number())
            elif key == "pipeline":
                d.pipeline = Int(p.number())
            elif key == "checkpoints":
                d.checkpoints = p.boolean()
            elif key == "workers":
                p.expect("[")
                if not p.take("]"):
                    while True:
                        d.workers.append(_worker(p))
                        if p.take("]"):
                            break
                        p.expect(",")
            else:
                p.skip_value()
            if p.take("}"):
                return d^
            p.expect(",")

    def live(self) -> Int:
        """Workers that aren't draining."""
        var n = 0
        for w in self.workers:
            if not w.draining:
                n += 1
        return n


def _worker(mut p: _Reader) raises -> WorkerDemand:
    var w = WorkerDemand("", 0, 0, 0, 0, False)
    p.expect("{")
    if p.take("}"):
        return w^
    while True:
        var key = p.string()
        p.expect(":")
        if key == "address":
            w.address = p.string()
        elif key == "running":
            w.running = Int(p.number())
        elif key == "idle_seconds":
            w.idle_seconds = p.number()
        elif key == "held_bytes":
            w.held_bytes = Int(p.number())
        elif key == "actor":
            w.actor = UInt64(Int(p.number()))
        elif key == "draining":
            w.draining = p.boolean()
        else:
            p.skip_value()
        if p.take("}"):
            return w^
        p.expect(",")


struct _Reader:
    """Just enough JSON for the head's status: objects, arrays, strings,
    numbers and booleans."""

    var text: String
    var at: Int

    def __init__(out self, text: String):
        self.text = text
        self.at = 0

    def _byte(self) -> Int:
        """The byte at `at`; past the end, 0."""
        if self.at >= self.text.byte_length():
            return 0
        return Int(self.text.as_bytes()[self.at])

    def _space(mut self):
        while True:
            var c = self._byte()
            if (
                c != ord(" ")
                and c != ord("\n")
                and c != ord("\t")
                and c != ord("\r")
            ):
                return
            self.at += 1

    def take(mut self, s: StaticString) -> Bool:
        self._space()
        var n = s.byte_length()
        if self.at + n > self.text.byte_length():
            return False
        if String(self.text[byte = self.at : self.at + n]) == s:
            self.at += n
            return True
        return False

    def expect(mut self, s: StaticString) raises:
        if not self.take(s):
            raise Error("status JSON: expected ", s, " at byte ", self.at)

    def string(mut self) raises -> String:
        self.expect('"')
        var out = String()
        var n = self.text.byte_length()
        while self.at < n:
            var c = String(self.text[byte=self.at])
            self.at += 1
            if c == '"':
                return out^
            if c == "\\":
                if self.at >= n:
                    break
                c = String(self.text[byte=self.at])
                self.at += 1
            out += c
        raise Error("status JSON: unterminated string")

    def number(mut self) raises -> Float64:
        self._space()
        var start = self.at
        while True:
            var c = self._byte()
            if not (
                (c >= ord("0") and c <= ord("9"))
                or c == ord("-")
                or c == ord("+")
                or c == ord(".")
                or c == ord("e")
                or c == ord("E")
            ):
                break
            self.at += 1
        if self.at == start:
            raise Error("status JSON: expected a number at byte ", start)
        return Float64(String(self.text[byte = start : self.at]))

    def boolean(mut self) raises -> Bool:
        if self.take("true"):
            return True
        if self.take("false"):
            return False
        raise Error("status JSON: expected true or false at byte ", self.at)

    def skip_value(mut self) raises:
        self._space()
        if self.at >= self.text.byte_length():
            raise Error("status JSON: expected a value")
        var c = self._byte()
        if c == ord('"'):
            _ = self.string()
        elif c == ord("{") or c == ord("["):
            var close: StaticString = "}" if c == ord("{") else "]"
            self.at += 1
            if self.take(close):
                return
            while True:
                if close == "}":
                    _ = self.string()
                    self.expect(":")
                self.skip_value()
                if self.take(close):
                    return
                self.expect(",")
        elif self.take("true") or self.take("false") or self.take("null"):
            return
        else:
            _ = self.number()


struct Policy(Copyable, Movable):
    var min_workers: Int
    """May be 0: a job with nothing to do then runs no worker."""
    var max_workers: Int
    var tasks_per_worker: Int
    """Tasks one worker takes at once; 0 for the head's pipeline depth."""
    var up_after: Float64
    """Seconds tasks stay ready with no free slot before workers start."""
    var idle_before_down: Float64
    """Seconds a worker stays idle before it drains."""
    var max_step: Int
    """Most workers started by one decision."""

    def __init__(
        out self,
        min_workers: Int = 1,
        max_workers: Int = 4,
        tasks_per_worker: Int = 0,
        up_after: Float64 = 5,
        idle_before_down: Float64 = 60,
        max_step: Int = 4,
    ):
        self.min_workers = max(min_workers, 0)
        self.max_workers = max(max_workers, self.min_workers)
        self.tasks_per_worker = max(tasks_per_worker, 0)
        self.up_after = max(up_after, 0)
        self.idle_before_down = max(idle_before_down, 0)
        self.max_step = max(max_step, 1)


struct Decision(Copyable, Movable, Writable):
    var start: Int
    """Workers to start."""
    var drain: List[String]
    """Workers to drain, by address."""
    var want: Int
    """The workers the demand calls for, within the policy's range."""
    var short_since: Float64
    """When the job ran short of workers, for the next `decide`; 0 if it
    isn't."""

    def __init__(out self):
        self.start = 0
        self.drain = List[String]()
        self.want = 0
        self.short_since = 0

    def write_to(self, mut writer: Some[Writer]):
        """`dista scale --once`'s output: one line each."""
        writer.write("want ", self.want, "\n")
        writer.write("start ", self.start, "\n")
        for a in self.drain:
            writer.write("drain ", a, "\n")
        writer.write("short_since ", self.short_since, "\n")


def _ceil_div(a: Int, b: Int) -> Int:
    return (a + b - 1) // b


def decide(
    demand: Demand,
    policy: Policy,
    now: Float64,
    pending: Int = 0,
    short_since: Float64 = 0,
) -> Decision:
    """What to do now: workers to start and workers to drain. `pending` is
    workers started that haven't joined the head yet."""
    var out = Decision()
    var per = policy.tasks_per_worker
    if per == 0:
        per = max(demand.pipeline, 1)
    var live = demand.live()
    var have = live + max(pending, 0)
    # Workers hosting an actor take no tasks.
    var hosts = demand.actors_waiting
    for w in demand.workers:
        if not w.draining and w.actor != 0:
            hosts += 1
    var tasks = demand.tasks_ready + demand.tasks_running
    var want = _ceil_div(tasks, per) + hosts
    want = min(max(want, policy.min_workers), policy.max_workers)
    out.want = want

    var short = want > have and (
        demand.tasks_ready > 0 or demand.actors_waiting > 0 or have == 0
    )
    if short and want > have:
        var since = short_since if short_since > 0 else now
        if have == 0 or now - since >= policy.up_after:
            out.start = min(want - have, policy.max_step)
            out.short_since = 0
        else:
            out.short_since = since
        return out^

    # Down: idle workers past the wait, fewest held bytes first.
    var keep = max(want, policy.min_workers)
    if live <= keep:
        return out^
    var idle = List[Int]()
    for i in range(len(demand.workers)):
        ref w = demand.workers[i]
        if (
            w.draining
            or w.running > 0
            or w.idle_seconds < policy.idle_before_down
        ):
            continue
        if w.actor != 0 and not demand.checkpoints:
            continue
        idle.append(i)
    for i in range(1, len(idle)):
        var j = i
        while j > 0 and (
            demand.workers[idle[j - 1]].held_bytes
            > demand.workers[idle[j]].held_bytes
        ):
            idle.swap_elements(j - 1, j)
            j -= 1
    var left = live
    var work = tasks + demand.tasks_waiting + demand.actors_waiting
    for i in idle:
        if left <= keep:
            break
        ref w = demand.workers[i]
        if left == 1 and (work > 0 or w.held_bytes > 0 or pending > 0):
            break  # the last one: its results would have nowhere to go
        out.drain.append(w.address)
        left -= 1
    return out^

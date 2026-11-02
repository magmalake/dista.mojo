"""The runtime: one binary that runs as driver, head or worker.

```mojo
comptime Square = Task[Int, Int, square]

def main() raises:
    var rt = Runtime[Square](workers=4)
    var r = rt.remote[Square](7)
    print(rt.get(r))  # 49
```

`Runtime(...)` reads the environment to decide what this process is:

| Environment | Role |
|---|---|
| `DISTA_ROLE=head` | runs the head on `DISTA_PORT` (default 6390) and never returns |
| `DISTA_ROLE=worker` | serves the head at `DISTA_HEAD=host:port` and never returns |
| `DISTA_HEAD=host:port` | a driver that submits to that head |
| neither | local mode: forks a head and `workers` workers, then drives them |

So a cluster runs one image everywhere, and the same program runs on a laptop
unchanged. The registry is the runtime's parameter list: workers dispatch
with a `comptime for` over it, and every process's hello carries a hash of it
so the head refuses a mismatched build.

A driver sends tasks to the head and reads results back. Small results come
with the reply; a large one stays on the worker that computed it. On the same
host `get` maps it from the node store and decodes it in place; from another
host it fetches it from that worker directly. A task can take
another task's result: pass an `ObjectRef[A]` instead of an `A`, or a
`List[ObjectRef[X]]` to a task that takes a `Seq[X]`. The head runs it once
every input exists, and fails it if an input failed.

`create` starts an actor on a worker of its own and returns its handle;
`call` runs one of its methods there, in the order of the calls, and returns
an `ObjectRef` like a task does; `checkpoint` saves its state to
`DISTA_CHECKPOINT_URI`; `kill` destroys it.
"""

from std.os import getenv
from std.time import perf_counter_ns, sleep

from dista.client import (
    ActorHandle,
    Client,
    CtxMethodLike,
    CtxTaskLike,
    HeadStats,
    ObjectRef,
    TaskContext,
    TaskSpan,
    connect_head,
)
from dista.head import Head
from dista.signals import exit_on_sigterm
from dista.socket import Listener, Stream, exit_now, fork, wait_child, wall_ns
from dista.task import ActorLike, MethodLike, StatePtr, TaskLike, fnv1a64
from dista.worker import Worker

comptime DEFAULT_PORT = 6390


def _us(ns: UInt64) -> String:
    return String(Float64(ns) / 1000.0)


def _json_string(s: String) -> String:
    var out = String('"')
    for cp in s.codepoint_slices():
        if cp == '"' or cp == "\\":
            out += "\\"
        out += cp
    return out + '"'


def _meta(pid: Int, what: String, name: String, tid: Int = 0) -> String:
    return String(
        '{"name":"',
        what,
        '","ph":"M","pid":',
        pid,
        ',"tid":',
        tid,
        ',"args":{"name":',
        _json_string(name),
        "}},\n",
    )


def _event(
    name: String, pid: Int, tid: Int, ts: UInt64, dur: UInt64, args: String
) -> String:
    return String(
        '{"name":',
        _json_string(name),
        ',"ph":"X","pid":',
        pid,
        ',"tid":',
        tid,
        ',"ts":',
        _us(ts),
        ',"dur":',
        _us(dur),
        ',"args":',
        args,
        "},\n",
    )


struct Runtime[*Ts: TaskLike](Movable):
    """A driver for tasks `Ts`, or a head or worker if the environment says so.
    """

    var _c: Client
    var _pids: List[Int]

    def __init__(out self, workers: Int = 4, pipeline: Int = 2) raises:
        """Starts this process in the role its environment names.

        Args:
            workers: Workers to fork in local mode; ignored otherwise.
            pipeline: Tasks in flight per worker, if this process runs a head.
        """
        Self._check_registry()
        if pipeline < 1:
            raise Error("pipeline must be at least 1")
        var role = getenv("DISTA_ROLE")
        if role == "head" or role == "worker":
            # A container's PID 1 would otherwise ignore Kubernetes' SIGTERM.
            exit_on_sigterm()
        if role == "head":
            var port = Int(getenv("DISTA_PORT", String(DEFAULT_PORT)))
            var listener = Listener(port, any_interface=True)
            print("dista head listening on port", listener.port)
            var head = Head(listener^, Self._registry_hash(), pipeline, False)
            head.run()
            exit_now(0)
        if role == "worker":
            var at = getenv("DISTA_HEAD")
            Self._work(connect_head(at), at, at)
            exit_now(0)
        if role != "":
            raise Error("DISTA_ROLE must be head or worker, got ", role)

        var pids = List[Int]()
        var head_addr = getenv("DISTA_HEAD")
        var stream: Stream
        if head_addr != "":
            stream = connect_head(head_addr)
        else:
            if workers < 1:
                raise Error("need at least one worker")
            var listener = Listener()
            var port = listener.port
            var pid = fork()
            if pid == 0:
                var code = 0
                try:
                    var head = Head(
                        listener^, Self._registry_hash(), pipeline, True
                    )
                    head.run()
                except e:
                    print("dista head:", e)
                    code = 1
                exit_now(code)
            else:
                # The head child has its own copy of the listening socket.
                var mine = listener^
                mine.close()
            pids.append(pid)
            for _ in range(workers):
                pid = fork()
                if pid == 0:
                    var code = 0
                    try:
                        Self._work(
                            Stream.connect(port), "", String("127.0.0.1:", port)
                        )
                    except e:
                        print("dista worker:", e)
                        code = 1
                    exit_now(code)
                pids.append(pid)
            stream = Stream.connect(port)
        self._c = Client(stream^, head_addr, Self._registry_hash())
        self._pids = pids^

    def __deinit__(deinit self):
        # In local mode the head exits once its only driver leaves, and closes
        # the workers' connections, which is their signal to exit.
        self._c.close()
        for pid in self._pids:
            _ = wait_child(pid)

    @staticmethod
    def _registry_hash() -> UInt64:
        """A hash of every registered task's name, in order."""
        var all = String()
        comptime for i in range(Self.Ts.length):
            all += Self.Ts[i].name()
            all += "\n"
        return fnv1a64(all)

    @staticmethod
    def _check_registry() raises:
        """Raises if two registered tasks share a wire ID."""
        comptime for i in range(Self.Ts.length):
            comptime for j in range(i + 1, Self.Ts.length):
                if Self.Ts[i].id() == Self.Ts[j].id():
                    raise Error(
                        "tasks ",
                        Self.Ts[i].name(),
                        " and ",
                        Self.Ts[j].name(),
                        " share an ID",
                    )

    @staticmethod
    def _dispatch(
        task_id: UInt64,
        args: Span[UInt8, _],
        mut out: List[UInt8],
        mut ctx: TaskContext,
    ) raises:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == task_id:
                comptime if conforms_to(Self.Ts[i], CtxTaskLike):
                    comptime assert conforms_to(Self.Ts[i], CtxTaskLike)
                    Self.Ts[i].run_ctx(args, out, ctx)
                else:
                    Self.Ts[i].run(args, out)
                return
        raise Error("unknown task ID ", task_id)

    @staticmethod
    def _create(type_id: UInt64, args: List[UInt8]) raises -> StatePtr:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].is_actor() and Self.Ts[i].id() == type_id:
                return Self.Ts[i].create(args)
        raise Error("unknown actor ID ", type_id)

    @staticmethod
    def _call(
        method_id: UInt64,
        type_id: UInt64,
        state: StatePtr,
        args: List[UInt8],
        mut out: List[UInt8],
        mut ctx: TaskContext,
    ) raises:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == method_id:
                # The state is only valid as its own actor's type.
                if Self.Ts[i].owner() != type_id:
                    raise Error(
                        Self.Ts[i].name(), " is not a method of this actor"
                    )
                comptime if conforms_to(Self.Ts[i], CtxMethodLike):
                    comptime assert conforms_to(Self.Ts[i], CtxMethodLike)
                    Self.Ts[i].call_ctx(state, args, out, ctx)
                else:
                    Self.Ts[i].call(state, args, out)
                return
        raise Error("unknown method ID ", method_id)

    @staticmethod
    def _destroy(type_id: UInt64, state: StatePtr):
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].is_actor() and Self.Ts[i].id() == type_id:
                Self.Ts[i].destroy(state)
                return

    @staticmethod
    def _save(type_id: UInt64, state: StatePtr, mut out: List[UInt8]) raises:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].is_actor() and Self.Ts[i].id() == type_id:
                Self.Ts[i].save(state, out)
                return
        raise Error("unknown actor ID ", type_id)

    @staticmethod
    def _restore(type_id: UInt64, bytes: List[UInt8]) raises -> StatePtr:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].is_actor() and Self.Ts[i].id() == type_id:
                return Self.Ts[i].restore(bytes)
        raise Error("unknown actor ID ", type_id)

    @staticmethod
    def _dispatch_list(
        task_id: UInt64,
        args: List[UInt8],
        mut out: List[UInt8],
        mut ctx: TaskContext,
    ) raises:
        Self._dispatch(task_id, args, out, ctx)

    @staticmethod
    def _work(
        var head: Stream, rejoin_at: String = "", head_addr: String = ""
    ) raises:
        """A worker: run tasks and serve results until the head hangs up.

        Given the head's address, a worker whose head goes away reconnects
        (for up to `DISTA_CONNECT_TIMEOUT` seconds) and rejoins it with its
        results, so a restarted head picks up where it left off. `head_addr`
        is where its tasks' client connects.
        """
        var w = Worker[
            Self._dispatch_list,
            Self._create,
            Self._call,
            Self._destroy,
            Self._save,
            Self._restore,
        ](head^, Self._registry_hash(), head_addr, rejoin_at)
        while True:
            w.run()
            if rejoin_at == "":
                return
            print("dista worker: head gone, rejoining", rejoin_at)
            w.rejoin(connect_head(rejoin_at), Self._registry_hash())

    @staticmethod
    def _check_registered[T: TaskLike]() raises:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == T.id():
                return
        raise Error(T.name(), " is not registered with this runtime")

    def remote[T: TaskLike](mut self, arg: T.A) raises -> ObjectRef[T.R]:
        """Submits task `T` on `arg` and returns a handle to its result."""
        Self._check_registered[T]()
        return self._c.remote[T](arg)

    def remote[
        T: TaskLike
    ](mut self, arg: ObjectRef[T.A]) raises -> ObjectRef[T.R]:
        """Submits task `T` to run on another task's result, once it exists."""
        Self._check_registered[T]()
        return self._c.remote[T](arg)

    def remote[
        X: AnyType, //, T: TaskLike
    ](mut self, args: List[ObjectRef[X]]) raises -> ObjectRef[T.R]:
        """Submits task `T`, which takes a `Seq[X]`, to run on these results.

        The task runs once every result exists, and receives them in order.
        """
        Self._check_registered[T]()
        return self._c.remote[T](args)

    def create[
        T: ActorLike
    ](mut self, arg: T.A, checkpoint_every: Int = 0) raises -> ActorHandle[T]:
        """Starts actor `T` on a worker of its own, built from `arg`.

        It returns at once; calls made before the actor is up wait for it.
        If the constructor raises, every call fails with its error. The
        handle is a value: a task given it can call the actor too.

        With `checkpoint_every` set, the actor's state is written to
        `DISTA_CHECKPOINT_URI` after every that many calls, and a restart
        restores it from there instead of running every call again.
        """
        Self._check_registered[T]()
        return self._c.create[T](arg, checkpoint_every)

    def checkpoint[
        T: ActorLike
    ](mut self, actor: ActorHandle[T]) raises -> ObjectRef[Int]:
        """Checkpoints `actor` after the calls made to it so far. The result
        is the number of calls the checkpoint covers, once it's written."""
        return self._c.checkpoint(actor)

    def call[
        M: MethodLike
    ](mut self, actor: ActorHandle[M.Act], arg: M.A) raises -> ObjectRef[M.R]:
        """Calls method `M` on `actor` with `arg`, after every earlier call."""
        Self._check_registered[M]()
        return self._c.call[M](actor, arg)

    def kill[T: ActorLike](mut self, actor: ActorHandle[T]) raises:
        """Destroys `actor` once its earlier calls have run, and frees its
        worker. Their results stay until released."""
        self._c.kill(actor)

    def ready[R: AnyType](mut self, r: ObjectRef[R]) raises -> Bool:
        """Returns whether `r`'s task has finished, without waiting for it."""
        return self._c.ready(r)

    def get[R: Movable](mut self, r: ObjectRef[R]) raises -> R:
        """Waits for `r`'s task and returns its result, or raises its error."""
        return self._c.get(r)

    def release[R: AnyType](mut self, r: ObjectRef[R]) raises:
        """Says this driver is done with `r`: see `Client.release`."""
        self._c.release(r)

    def release[R: AnyType](mut self, refs: List[ObjectRef[R]]) raises:
        """Releases every ref in `refs`."""
        self._c.release(refs)

    def stats(mut self) raises -> HeadStats:
        """Asks the head what it holds. Frames that arrive first are kept."""
        return self._c.stats()

    def location[R: AnyType](mut self, r: ObjectRef[R]) raises -> String:
        """Where `r`'s result lives: see `Client.location`."""
        return self._c.location(r)

    def bytes_fetched(self) -> Int:
        """How many result bytes this driver has fetched over sockets."""
        return self._c.bytes_fetched()

    @staticmethod
    def clock() -> UInt64:
        """Wall-clock nanoseconds, the clock the timeline uses: the start to
        give `span`."""
        return wall_ns()

    def span(mut self, name: String, start: UInt64, end: UInt64 = 0):
        """Records a stretch of this driver's own work for the timeline, from
        `start` (from `clock`) to `end`, or to now. `timeline` draws these as
        the driver's track, above the workers."""
        self._c.span(name, start, end)

    @staticmethod
    def _task_name(task_id: UInt64) -> String:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == task_id:
                return String(Self.Ts[i].name())
        return String("task ", task_id)

    def spans(mut self) raises -> List[TaskSpan]:
        """Every task this driver submitted that has finished, with its
        timeline, since the last call. A head restart loses the spans it held
        and those of tasks running then.
        """
        var out = self._c.spans()
        for ref sp in out:
            sp.task = Self._task_name(sp.task_id)
        return out^

    def timeline(mut self, path: String) raises -> Int:
        """Writes the finished tasks' timeline to `path` in Chrome's trace
        format, for Perfetto (ui.perfetto.dev) or chrome://tracing, and
        returns how many tasks it holds.

        Each worker is a process with two tracks: its tasks running, and its
        tasks waiting for their inputs. The driver's own spans, from `span`,
        are a process above them. Times are microseconds from the first
        submission or driver span; each run's arguments carry the head's
        waits too. Driver spans are written once, like task spans.
        """
        var spans = self.spans()
        var mine = self._c.take_driver_spans()
        var t0 = UInt64(0)
        for ref sp in spans:
            if t0 == 0 or sp.submitted < t0:
                t0 = sp.submitted
        for ref d in mine:
            if t0 == 0 or d.start < t0:
                t0 = d.start
        var pids = Dict[String, Int]()
        var out = String("[\n")
        if len(mine) > 0:
            out += _meta(0, "process_name", "driver")
            out += _meta(0, "thread_name", "driver", 1)
        for ref d in mine:
            out += _event(
                d.name,
                0,
                1,
                d.start - t0,
                d.end - d.start if d.end > d.start else 0,
                String("{}"),
            )
        for ref sp in spans:
            if sp.worker == "" or sp.started == 0:
                continue
            if sp.worker not in pids:
                var p = len(pids) + 1
                pids[sp.worker] = p
                out += _meta(p, "process_name", "worker " + sp.worker)
                out += _meta(p, "thread_name", "running", 1)
                out += _meta(p, "thread_name", "waiting for inputs", 2)
            var pid = pids.get(sp.worker, 0)
            if sp.started > sp.arrived:
                out += _event(
                    "inputs",
                    pid,
                    2,
                    sp.arrived - t0,
                    sp.started - sp.arrived,
                    String('{"object":', sp.object_id, "}"),
                )
            out += _event(
                sp.task,
                pid,
                1,
                sp.started - t0,
                sp.ended - sp.started,
                String(
                    '{"object":',
                    sp.object_id,
                    ',"queued_after_submit_us":',
                    _us(sp.queued - sp.submitted) if sp.queued else String(0),
                    ',"dispatch_wait_us":',
                    _us(sp.dispatched - sp.queued) if sp.queued else String(0),
                    ',"settle_after_end_us":',
                    _us(sp.settled - sp.ended) if sp.settled
                    > sp.ended else String(0),
                    "}",
                ),
            )
        out += '{"name":"end","ph":"i","ts":0,"pid":0,"tid":0,"s":"g"}\n]\n'
        with open(path, "w") as f:
            f.write(out)
        return len(spans)

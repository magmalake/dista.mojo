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
an `ObjectRef` like a task does; `kill` destroys it.
"""

from std.os import getenv
from std.time import perf_counter_ns, sleep

from dista.codec import decode, encode, get_scalar, put_scalar
from dista.frame import (
    message,
    KIND_ACTOR_CALL,
    KIND_ACTOR_CREATE,
    KIND_ACTOR_KILL,
    KIND_CLAIM,
    KIND_ERROR,
    KIND_FETCH,
    KIND_HELLO,
    KIND_LOCATED,
    KIND_RELEASE,
    KIND_RESULT,
    KIND_RESUMED,
    KIND_STATS,
    KIND_TASK,
    KIND_TASK_AFTER,
    KIND_TIMELINE,
    KIND_WELCOME,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    decode_addr,
    read_frame,
    write_frame,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import (
    Listener,
    Stream,
    exit_now,
    fork,
    poll_readable,
    wait_child,
    wall_ns,
)
from dista.task import ActorLike, MethodLike, StatePtr, TaskLike, fnv1a64
from dista.store import Location
from dista.worker import Worker, addr_key
from memory_region import SharedMapping

comptime DEFAULT_PORT = 6390


struct ObjectRef[T: AnyType](ImplicitlyCopyable, Movable, Writable):
    """A handle to a task's result, which may not exist yet."""

    var id: UInt64

    def __init__(out self, id: UInt64):
        self.id = id

    def write_to(self, mut writer: Some[Writer]):
        writer.write("ObjectRef(", self.id, ")")


struct ActorHandle[T: ActorLike](ImplicitlyCopyable, Movable, Writable):
    """A handle to an actor of type `T`, from `Runtime.create`."""

    var id: UInt64

    def __init__(out self, id: UInt64):
        self.id = id

    def write_to(self, mut writer: Some[Writer]):
        writer.write("ActorHandle(", self.id, ")")


@fieldwise_init
struct _Submission(Copyable, Movable):
    """A task this driver sent, kept until it settles so it can be resent."""

    var kind: UInt8
    var task_id: UInt64
    var payload: List[UInt8]
    var deps: List[UInt64]


@fieldwise_init
struct TaskSpan(Copyable, Movable, Writable):
    """One finished task, from `Runtime.spans`: wall-clock nanoseconds.

    The first four are the head's, the last three the worker's. On a
    cluster the two clocks agree only as well as NTP keeps them.
    """

    var object_id: UInt64
    var task: String
    """The task's registered name."""
    var worker: String
    """The worker's `ip:port`, or "" if it never ran (an input failed)."""
    var submitted: UInt64
    var queued: UInt64
    """When its inputs existed and it joined the queue."""
    var dispatched: UInt64
    var settled: UInt64
    var arrived: UInt64
    """On the worker: when the task reached it, before any input fetches."""
    var started: UInt64
    var ended: UInt64
    """When it finished, its result stored."""


@fieldwise_init
struct HeadStats(Copyable, Movable, Writable):
    """What the head holds, from `Runtime.stats`."""

    var objects: Int
    """Objects with a live reference: unreleased, or an input still pending."""
    var results: Int
    """Small results kept at the head."""
    var errors: Int
    var located: Int
    """Results the head knows live on a worker."""
    var pending: Int
    """Tasks queued or waiting on inputs."""


comptime MAX_DRIVER_SPANS = 1_000_000
"""Driver spans kept until `timeline` writes them; later ones are dropped."""


@fieldwise_init
struct _DriverSpan(Copyable, Movable):
    var name: String
    var start: UInt64
    var end: UInt64


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


def parse_host_port(addr: String) raises -> Tuple[String, Int]:
    """Splits `host:port`."""
    var parts = addr.split(":")
    if len(parts) != 2:
        raise Error("expected host:port, got ", addr)
    return (String(parts[0]), Int(parts[1]))


def connect_head(addr: String) raises -> Stream:
    """Connects to the head at `host:port`, retrying while it isn't up yet.

    In a cluster, processes start in any order: a worker's pod can come up
    before the head's, or before its name resolves. This keeps trying for
    `DISTA_CONNECT_TIMEOUT` seconds (default 60), then raises the last error.
    """
    var hp = parse_host_port(addr)
    var limit = Int(getenv("DISTA_CONNECT_TIMEOUT", "60"))
    var deadline = Int(perf_counter_ns()) + limit * 1_000_000_000
    while True:
        try:
            return Stream.connect(hp[0], hp[1])
        except e:
            if Int(perf_counter_ns()) >= deadline:
                raise e^
        sleep(0.25)


struct Runtime[*Ts: TaskLike](Movable):
    """A driver for tasks `Ts`, or a head or worker if the environment says so.
    """

    var _head: Stream
    var _driver: UInt64
    var _counter: UInt64
    var _pids: List[Int]
    var _results: Dict[UInt64, List[UInt8]]
    var _errors: Dict[UInt64, String]
    var _located: Dict[UInt64, List[UInt8]]
    """Results that stayed on a worker: object ID to that worker's address."""
    var _fetchers: List[Stream]
    var _socket_bytes: Int
    var _fetcher_index: Dict[String, Int]
    var _released: Dict[UInt64, Bool]
    var _stats: Optional[HeadStats]
    var _head_addr: String
    """The head's `host:port` in a cluster, or "" in local mode."""
    var _subs: Dict[UInt64, _Submission]
    """Tasks not settled yet, resent if the head restarts."""
    var _dependents: Dict[UInt64, Int]
    """For an object, how many unsettled submissions take it as input."""
    var _live: Dict[UInt64, Bool]
    """Objects this driver would restore after a head restart."""
    var _resumes: Int
    """How many times this driver has resumed with a restarted head."""
    var _claimed: Dict[UInt64, Bool]
    """Results claimed from a restarted head that hasn't said where yet."""
    var _spans: List[TaskSpan]
    var _timeline_answered: Bool
    var _actors: Dict[UInt64, Bool]
    """Actors this driver created and hasn't killed."""
    var _lost_actors: Dict[UInt64, Bool]
    """Actors a head restart took with it."""
    var _driver_spans: List[_DriverSpan]

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
        if role == "head":
            var port = Int(getenv("DISTA_PORT", String(DEFAULT_PORT)))
            var listener = Listener(port, any_interface=True)
            print("dista head listening on port", listener.port)
            var head = Head(listener^, Self._registry_hash(), pipeline, False)
            head.run()
            exit_now(0)
        if role == "worker":
            var at = getenv("DISTA_HEAD")
            Self._work(connect_head(at), at)
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
                        Self._work(Stream.connect(port))
                    except e:
                        print("dista worker:", e)
                        code = 1
                    exit_now(code)
                pids.append(pid)
            stream = Stream.connect(port)
        self._head = stream^
        self._pids = pids^
        self._driver = Self._hello(self._head)
        self._counter = 0
        self._results = Dict[UInt64, List[UInt8]]()
        self._errors = Dict[UInt64, String]()
        self._located = Dict[UInt64, List[UInt8]]()
        self._fetchers = List[Stream]()
        self._socket_bytes = 0
        self._fetcher_index = Dict[String, Int]()
        self._released = Dict[UInt64, Bool]()
        self._stats = None
        self._head_addr = head_addr
        self._subs = Dict[UInt64, _Submission]()
        self._dependents = Dict[UInt64, Int]()
        self._live = Dict[UInt64, Bool]()
        self._resumes = 0
        self._claimed = Dict[UInt64, Bool]()
        self._spans = List[TaskSpan]()
        self._timeline_answered = False
        self._actors = Dict[UInt64, Bool]()
        self._lost_actors = Dict[UInt64, Bool]()
        self._driver_spans = List[_DriverSpan]()

    def __deinit__(deinit self):
        # In local mode the head exits once its only driver leaves, and closes
        # the workers' connections, which is their signal to exit.
        self._head.close()
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
    def _hello_payload(role: UInt8) -> List[UInt8]:
        var body: List[UInt8] = [role]
        put_scalar[DType.uint64](body, Self._registry_hash())
        return body^

    @staticmethod
    def _hello(s: Stream) raises -> UInt64:
        """Introduces a driver to the head and returns its driver number."""
        write_frame(s, KIND_HELLO, 0, 0, Self._hello_payload(ROLE_DRIVER))
        var f = read_frame(s)
        if not f:
            raise Error("the head closed the connection")
        ref frame = f.value()
        if frame.kind == KIND_ERROR:
            raise Error(
                "the head refused this driver: ",
                message(frame.payload),
            )
        if frame.kind != KIND_WELCOME:
            raise Error("expected welcome, got frame kind ", Int(frame.kind))
        var pos = 0
        return get_scalar[DType.uint64](frame.payload, pos)

    @staticmethod
    def _dispatch(
        task_id: UInt64, args: Span[UInt8, _], mut out: List[UInt8]
    ) raises:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == task_id:
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
    ) raises:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == method_id:
                # The state is only valid as its own actor's type.
                if Self.Ts[i].owner() != type_id:
                    raise Error(
                        Self.Ts[i].name(), " is not a method of this actor"
                    )
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
    def _dispatch_list(
        task_id: UInt64, args: List[UInt8], mut out: List[UInt8]
    ) raises:
        Self._dispatch(task_id, args, out)

    @staticmethod
    def _work(var head: Stream, rejoin_at: String = "") raises:
        """A worker: run tasks and serve results until the head hangs up.

        Given the head's address, a worker whose head goes away reconnects
        (for up to `DISTA_CONNECT_TIMEOUT` seconds) and rejoins it with its
        results, so a restarted head picks up where it left off.
        """
        var w = Worker[
            Self._dispatch_list, Self._create, Self._call, Self._destroy
        ](head^, Self._registry_hash())
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

    def _new_id(mut self) -> UInt64:
        self._counter += 1
        return (self._driver << DRIVER_SHIFT) | self._counter

    def remote[T: TaskLike](mut self, arg: T.A) raises -> ObjectRef[T.R]:
        """Submits task `T` on `arg` and returns a handle to its result."""
        Self._check_registered[T]()
        var args = List[UInt8]()
        encode(arg, args)
        var id = self._new_id()
        self._submit(id, KIND_TASK, T.id(), args^, List[UInt64]())
        return ObjectRef[T.R](id)

    def remote[
        T: TaskLike
    ](mut self, arg: ObjectRef[T.A]) raises -> ObjectRef[T.R]:
        """Submits task `T` to run on another task's result, once it exists."""
        Self._check_registered[T]()
        return self._submit_after[T.R](T.id(), [arg.id], False)

    def remote[
        X: AnyType, //, T: TaskLike
    ](mut self, args: List[ObjectRef[X]]) raises -> ObjectRef[T.R]:
        """Submits task `T`, which takes a `Seq[X]`, to run on these results.

        The task runs once every result exists, and receives them in order.
        """
        comptime assert (
            reflect[T.A].base_name() == "Seq"
        ), "a task given a list of ObjectRefs must take a Seq"
        Self._check_registered[T]()
        var deps = List[UInt64](capacity=len(args))
        for r in args:
            deps.append(r.id)
        return self._submit_after[T.R](T.id(), deps, True)

    def create[T: ActorLike](mut self, arg: T.A) raises -> ActorHandle[T]:
        """Starts actor `T` on a worker of its own, built from `arg`.

        It returns at once; calls made before the actor is up wait for it.
        If the constructor raises, every call fails with its error.
        """
        Self._check_registered[T]()
        var args = List[UInt8]()
        encode(arg, args)
        var id = self._new_id()
        self._actors[id] = True
        self._submit(id, KIND_ACTOR_CREATE, T.id(), args^, List[UInt64]())
        return ActorHandle[T](id)

    def call[
        M: MethodLike
    ](mut self, actor: ActorHandle[M.Act], arg: M.A) raises -> ObjectRef[M.R]:
        """Calls method `M` on `actor` with `arg`, after every earlier call."""
        Self._check_registered[M]()
        if actor.id in self._lost_actors:
            raise Error(actor, " was lost when the head restarted")
        if actor.id not in self._actors:
            raise Error(actor, " was killed")
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, actor.id)
        encode(arg, body)
        var id = self._new_id()
        self._submit(id, KIND_ACTOR_CALL, M.id(), body^, List[UInt64]())
        return ObjectRef[M.R](id)

    def kill[T: ActorLike](mut self, actor: ActorHandle[T]) raises:
        """Destroys `actor` once its earlier calls have run, and frees its
        worker. Their results stay until released."""
        if not self._actors.pop(actor.id, False):
            return
        _ = self._lost_actors.pop(actor.id, False)
        self._released[actor.id] = True
        self._drop_local(actor.id)
        self._send(KIND_ACTOR_KILL, actor.id, List[UInt8]())

    def _submit_after[
        R: AnyType
    ](
        mut self, task_id: UInt64, deps: List[UInt64], gather: Bool
    ) raises -> ObjectRef[R]:
        var body: List[UInt8] = [UInt8(1) if gather else UInt8(0)]
        put_scalar[DType.uint64](body, UInt64(len(deps)))
        for d in deps:
            put_scalar[DType.uint64](body, d)
        var id = self._new_id()
        self._submit(id, KIND_TASK_AFTER, task_id, body^, deps.copy())
        return ObjectRef[R](id)

    def _submit(
        mut self,
        id: UInt64,
        kind: UInt8,
        task_id: UInt64,
        var payload: List[UInt8],
        var deps: List[UInt64],
    ) raises:
        for d in deps:
            self._dependents[d] = self._dependents.get(d, 0) + 1
        self._live[id] = True
        self._subs[id] = _Submission(kind, task_id, payload^, deps^)
        var sent = True
        ref body = self._subs[id].payload
        try:
            write_frame(self._head, kind, id, task_id, body)
        except:
            sent = False
        if not sent:
            # Resuming resends this submission with the rest.
            self._resume(Error("lost the head while submitting"))

    def _send(mut self, kind: UInt8, id: UInt64, payload: List[UInt8]) raises:
        """Sends a frame to the head, resuming with a new head if it's gone."""
        try:
            write_frame(self._head, kind, id, 0, payload)
        except e:
            self._resume(e)
            write_frame(self._head, kind, id, 0, payload)

    def _settle(mut self, id: UInt64):
        """`id`'s task is done: it no longer needs its inputs kept."""
        var sub = self._subs.pop(id, _Submission(0, 0, [], []))
        for d in sub.deps:
            var n = self._dependents.get(d, 0) - 1
            if n > 0:
                self._dependents[d] = n
                continue
            _ = self._dependents.pop(d, 0)
            if d in self._released:
                self._drop_local(d)

    def _drop_local(mut self, id: UInt64):
        _ = self._results.pop(id, List[UInt8]())
        _ = self._errors.pop(id, String())
        _ = self._located.pop(id, List[UInt8]())
        _ = self._claimed.pop(id, False)
        _ = self._live.pop(id, False)

    def _resume(mut self, cause: Error) raises:
        """Reconnects to a restarted head and gives it back this driver's state.

        In local mode the head is this process's child and doesn't come back,
        so the error stands.
        """
        if self._head_addr == "":
            raise cause.copy()
        print("dista driver: lost the head (", cause, "), resuming")
        self._resumes += 1
        while True:
            # Raises once the head has been unreachable for
            # DISTA_CONNECT_TIMEOUT seconds.
            self._head.close()
            self._head = connect_head(self._head_addr)
            try:
                self._replay()
                return
            except e:
                print("dista driver: lost the head again resuming (", e, ")")

    def _replay(mut self) raises:
        """Says hello under this driver's number and sends back its state."""
        var hello = Self._hello_payload(ROLE_DRIVER)
        put_scalar[DType.uint64](hello, self._driver)
        write_frame(self._head, KIND_HELLO, 0, 0, hello)
        var f = read_frame(self._head)
        if not f or f.value().kind != KIND_WELCOME:
            raise Error("the restarted head didn't welcome this driver back")
        # Actors don't survive a head restart yet: this driver's are lost,
        # with their unfinished calls.
        for entry in self._actors.items():
            self._lost_actors[entry.key] = True
        # Oldest first, so every task's inputs are back before it is.
        for n in range(1, Int(self._counter) + 1):
            var id = (self._driver << DRIVER_SHIFT) | UInt64(n)
            if id not in self._live:
                continue
            if id in self._lost_actors:
                _ = self._subs.pop(id, _Submission(0, 0, [], []))
                self._drop_local(id)
                continue
            if id in self._subs and self._subs[id].kind == KIND_ACTOR_CALL:
                self._errors[id] = String(
                    "actor lost: the head restarted before this call finished"
                )
                self._settle(id)
                continue
            if id in self._results:
                write_frame(self._head, KIND_RESULT, id, 0, self._results[id])
            elif id in self._errors:
                write_frame(
                    self._head, KIND_ERROR, id, 0, self._errors[id].as_bytes()
                )
            elif id in self._located or id in self._claimed:
                write_frame(self._head, KIND_CLAIM, id, 0, List[UInt8]())
                # Where it was may be stale: a task run twice across restarts
                # leaves copies, and the new head may keep the other one. It
                # says where it has the result in reply; until then the claim
                # stands, through further restarts.
                _ = self._located.pop(id, List[UInt8]())
                self._claimed[id] = True
            elif id in self._subs:
                ref sub = self._subs[id]
                write_frame(self._head, sub.kind, id, sub.task_id, sub.payload)
        write_frame(
            self._head,
            KIND_RESUMED,
            self._driver << DRIVER_SHIFT,
            0,
            List[UInt8](),
        )
        # Released objects came back only for the tasks that need them.
        for entry in self._live.items():
            if entry.key in self._released:
                write_frame(
                    self._head, KIND_RELEASE, entry.key, 0, List[UInt8]()
                )

    def _settled(self, id: UInt64) -> Bool:
        return id in self._results or id in self._errors or id in self._located

    def _read_one(mut self) raises:
        var f: Optional[Frame]
        try:
            f = read_frame(self._head)
        except e:
            self._resume(e)
            return
        if not f:
            self._resume(Error("the head closed the connection"))
            return
        ref frame = f.value()
        var id = frame.object_id
        # A released object is still kept while an unsettled task needs it:
        # a restarted head would need it back.
        var keep = id not in self._released or id in self._dependents
        if frame.kind == KIND_RESULT or frame.kind == KIND_ERROR:
            if keep:
                if frame.kind == KIND_RESULT:
                    self._results[id] = frame.payload.copy()
                else:
                    self._errors[id] = message(frame.payload)
            self._settle(id)
        elif frame.kind == KIND_LOCATED:
            _ = self._claimed.pop(id, False)
            if keep:
                self._located[id] = frame.payload.copy()
            self._settle(id)
        elif frame.kind == KIND_TIMELINE:
            var pos = 0
            var n = Int(get_scalar[DType.uint64](frame.payload, pos))
            for _ in range(n):
                var obj = get_scalar[DType.uint64](frame.payload, pos)
                var task = get_scalar[DType.uint64](frame.payload, pos)
                var t = List[UInt64]()
                for _ in range(7):
                    t.append(get_scalar[DType.uint64](frame.payload, pos))
                var addr = decode_addr(frame.payload, pos)
                var worker = String()
                if addr[1] != 0:
                    worker = addr_key(addr[0], addr[1])
                self._spans.append(
                    TaskSpan(
                        obj,
                        Self._task_name(task),
                        worker^,
                        t[0],
                        t[1],
                        t[2],
                        t[3],
                        t[4],
                        t[5],
                        t[6],
                    )
                )
            self._timeline_answered = True
        elif frame.kind == KIND_STATS:
            var pos = 0
            var n = List[Int]()
            for _ in range(5):
                n.append(Int(get_scalar[DType.uint64](frame.payload, pos)))
            self._stats = HeadStats(n[0], n[1], n[2], n[3], n[4])
        else:
            raise Error("driver got frame kind ", Int(frame.kind))

    def ready[R: AnyType](mut self, r: ObjectRef[R]) raises -> Bool:
        """Returns whether `r`'s task has finished, without waiting for it."""
        while not self._settled(r.id):
            if not poll_readable([self._head.fd], 0)[0]:
                return False
            self._read_one()
        return True

    def get[R: Movable](mut self, r: ObjectRef[R]) raises -> R:
        """Waits for `r`'s task and returns its result, or raises its error."""
        if r.id in self._released:
            raise Error(r, " was released")
        while not self._settled(r.id):
            self._read_one()
        if r.id in self._errors:
            raise Error(self._errors[r.id])
        var pos = 0
        if r.id not in self._results:
            var loc = self._where(r.id)
            if loc.is_local():
                # The file is visible here: decode straight from the mapping.
                var m = SharedMapping(loc.path)
                if loc.size > m.size():
                    raise Error("shared object ", loc.path, " is truncated")
                return decode[R](m.bytes()[: loc.size], pos)
            self._fetch(r.id, loc)
        return decode[R](self._results[r.id], pos)

    def release[R: AnyType](mut self, r: ObjectRef[R]) raises:
        """Says this driver is done with `r`.

        The result is freed, here, at the head and on its worker, once no
        submitted task still needs it; a task already given `r` as input
        still gets it. Releasing before the task has finished is fine: the
        result is dropped when it arrives. `get(r)` raises from now on, and a
        task submitted on `r` afterwards fails.
        """
        if r.id in self._released:
            return
        self._released[r.id] = True
        if self._dependents.get(r.id, 0) == 0:
            self._drop_local(r.id)
        # Otherwise its copy stays here until the tasks that take it settle:
        # a restarted head would need it back.
        self._send(KIND_RELEASE, r.id, List[UInt8]())

    def release[R: AnyType](mut self, refs: List[ObjectRef[R]]) raises:
        """Releases every ref in `refs`."""
        for r in refs:
            self.release(r)

    def stats(mut self) raises -> HeadStats:
        """Asks the head what it holds. Frames that arrive first are kept."""
        self._stats = None
        self._send(KIND_STATS, 0, List[UInt8]())
        var asked = self._resumes
        while not self._stats:
            self._read_one()
            if self._resumes != asked:
                # The head that was asked is gone: ask the new one.
                asked = self._resumes
                self._send(KIND_STATS, 0, List[UInt8]())
        return self._stats.value().copy()

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
        self._timeline_answered = False
        self._send(KIND_TIMELINE, 0, List[UInt8]())
        var asked = self._resumes
        while not self._timeline_answered:
            self._read_one()
            if self._resumes != asked:
                asked = self._resumes
                self._send(KIND_TIMELINE, 0, List[UInt8]())
        var out = self._spans^
        self._spans = List[TaskSpan]()
        return out^

    @staticmethod
    def clock() -> UInt64:
        """Wall-clock nanoseconds, the clock the timeline uses: the start to
        give `span`."""
        return wall_ns()

    def span(mut self, name: String, start: UInt64, end: UInt64 = 0):
        """Records a stretch of this driver's own work for the timeline, from
        `start` (from `clock`) to `end`, or to now. `timeline` draws these as
        the driver's track, above the workers."""
        if len(self._driver_spans) < MAX_DRIVER_SPANS:
            self._driver_spans.append(
                _DriverSpan(name, start, end if end != 0 else wall_ns())
            )

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
        var mine = self._driver_spans^
        self._driver_spans = List[_DriverSpan]()
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

    def bytes_fetched(self) -> Int:
        """How many result bytes this driver has fetched over sockets."""
        return self._socket_bytes

    def _where(self, id: UInt64) raises -> Location:
        var pos = 0
        return Location.decode(self._located[id], pos)

    def location[R: AnyType](mut self, r: ObjectRef[R]) raises -> String:
        """Where `r`'s result lives: a worker's `ip:port`, then its node-store
        file if it has one; "" if it came back with the reply. Waits for the
        task to finish.
        """
        while not self._settled(r.id):
            self._read_one()
        if r.id not in self._located:
            return String()
        var loc = self._where(r.id)
        var place = addr_key(loc.ip, loc.port)
        if loc.path != "":
            place += " " + loc.path
        return place

    def _fetch(mut self, id: UInt64, loc: Location) raises:
        """Copies a result from the worker holding it, over a socket."""
        var key = addr_key(loc.ip, loc.port)
        if key not in self._fetcher_index:
            self._fetchers.append(Stream.connect(loc.ip, loc.port))
            self._fetcher_index[key] = len(self._fetchers) - 1
        var i = self._fetcher_index.get(key, -1)
        write_frame(self._fetchers[i], KIND_FETCH, id, 0, List[UInt8]())
        var f = read_frame(self._fetchers[i])
        if not f:
            raise Error("the worker holding object ", id, " hung up")
        ref frame = f.value()
        if frame.kind == KIND_RESULT:
            self._socket_bytes += len(frame.payload)
            self._results[id] = frame.payload.copy()
        else:
            raise Error(message(frame.payload))

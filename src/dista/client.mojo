"""The driver's side of the protocol: submitting tasks, calling actors and
getting results back from the head.

`Runtime` is the program's entry point: it starts the process in its role
and checks that what it's given is registered. `Client` is the part that
talks to the head, without the registry, so it can also run inside a
worker: a `CtxTask` gets one, and can call actors or submit tasks from
there. A worker's client connects with `ROLE_CLIENT`, which the head treats
as a driver, except that it doesn't keep a local-mode head alive.

A client checks only what it can know: that it didn't kill an actor it
calls. Whether the task, actor or method exists is the binary's: every
process runs the same registry, and the head refuses a peer that doesn't.
"""

from std.os import getenv
from std.reflection import reflect_fn
from std.time import perf_counter_ns, sleep

from dista.codec import decode, encode, get_scalar, put_scalar
from dista.frame import (
    message,
    KIND_ACTOR_CALL,
    KIND_ACTOR_CREATE,
    KIND_ACTOR_KILL,
    KIND_CHECKPOINT,
    KIND_CLAIM,
    KIND_COVERED,
    KIND_ERROR,
    KIND_FETCH,
    KIND_HELLO,
    KIND_LOCATED,
    KIND_RELEASE,
    KIND_RESULT,
    KIND_RESUMED,
    KIND_STATS,
    KIND_STATUS,
    KIND_STOP,
    KIND_TASK,
    KIND_TASK_AFTER,
    KIND_TASK_MANY,
    GATHER,
    WITH_VALUE,
    KIND_TIMELINE,
    KIND_WELCOME,
    ROLE_CLIENT,
    ROLE_DRIVER,
    ROLE_OBSERVER,
    Frame,
    decode_addr,
    read_frame,
    write_frame,
)
from dista.head import DRIVER_SHIFT
from dista.socket import Stream, addr_key, poll_readable, wall_ns
from dista.task import (
    ActorLike,
    ManyLike,
    MethodLike,
    StatePtr,
    TaskLike,
    fnv1a64,
)
from dista.store import Location
from memory_region import SharedMapping


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
    var task_id: UInt64
    """The task's wire ID."""


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


def parse_timeline(payload: Span[UInt8, _]) raises -> List[TaskSpan]:
    """A timeline frame's spans, unnamed."""
    var out = List[TaskSpan]()
    var pos = 0
    var n = Int(get_scalar[DType.uint64](payload, pos))
    for _ in range(n):
        var obj = get_scalar[DType.uint64](payload, pos)
        var task = get_scalar[DType.uint64](payload, pos)
        var t = List[UInt64]()
        for _ in range(7):
            t.append(get_scalar[DType.uint64](payload, pos))
        var addr = decode_addr(payload, pos)
        var worker = String()
        if addr[1] != 0:
            worker = addr_key(addr[0], addr[1])
        out.append(
            TaskSpan(
                obj,
                String(),
                worker^,
                t[0],
                t[1],
                t[2],
                t[3],
                t[4],
                t[5],
                t[6],
                task,
            )
        )
    return out^


struct Observer(Movable):
    """A connection to the head that only looks, and can stop it: what
    `dista status`, `timeline` and `stop` use. It owns no objects and needs
    no registry."""

    var _head: Stream

    def __init__(out self, addr: String) raises:
        """Connects to the head at `addr`, `host:port`, once: an observer
        reports a head that isn't there rather than wait for one."""
        var hp = parse_host_port(addr)
        self._head = Stream.connect(hp[0], hp[1])
        _ = _hello(self._head, ROLE_OBSERVER, 0)

    def _ask(mut self, kind: UInt8) raises -> Frame:
        write_frame(self._head, kind, 0, 0, List[UInt8]())
        var f = read_frame(self._head)
        if not f:
            raise Error("the head closed the connection")
        if f.value().kind != kind:
            raise Error(
                "expected frame kind ", Int(kind), ", got ", Int(f.value().kind)
            )
        return f.take()

    def status(mut self) raises -> String:
        """The head's workers, drivers and actors, as text."""
        return message(self._ask(KIND_STATUS).payload)

    def spans(mut self) raises -> List[TaskSpan]:
        """Every driver's finished tasks the head still holds."""
        return parse_timeline(self._ask(KIND_TIMELINE).payload)

    def stop(mut self) raises:
        """Shuts the head down, and its workers with it."""
        _ = self._ask(KIND_STOP)


def write_trace(
    path: String,
    spans: List[TaskSpan],
    mine: List[_DriverSpan] = List[_DriverSpan](),
) raises -> Int:
    """Writes finished tasks, and a driver's own spans, to `path` in Chrome's
    trace format, for Perfetto (ui.perfetto.dev) or chrome://tracing, and
    returns how many tasks it holds.

    Each worker is a process with two tracks: its tasks running, and its
    tasks waiting for their inputs. The driver's spans are a process above
    them. Times are microseconds from the first submission or driver span;
    each run's arguments carry the head's waits too. A task without a name
    is named by its wire ID.
    """
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
        var name = sp.task if sp.task != "" else String(
            "task ", hex(sp.task_id)
        )
        out += _event(
            name,
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

    The wait between tries starts at 10 ms and doubles to 250 ms: a head that
    restarts is usually back within a fraction of a second, and a flat 250 ms
    added that much to every recovery.
    """
    var hp = parse_host_port(addr)
    var limit = Int(getenv("DISTA_CONNECT_TIMEOUT", "60"))
    var deadline = Int(perf_counter_ns()) + limit * 1_000_000_000
    var wait = 0.01
    while True:
        try:
            return Stream.connect(hp[0], hp[1])
        except e:
            if Int(perf_counter_ns()) >= deadline:
                raise e^
        sleep(wait)
        wait = min(wait * 2.0, 0.25)


def _hello_payload(role: UInt8, registry_hash: UInt64) -> List[UInt8]:
    var body: List[UInt8] = [role]
    put_scalar[DType.uint64](body, registry_hash)
    return body^


def _hello(s: Stream, role: UInt8, registry_hash: UInt64) raises -> UInt64:
    """Introduces a driver to the head and returns its driver number."""
    write_frame(s, KIND_HELLO, 0, 0, _hello_payload(role, registry_hash))
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


struct Client(Movable):
    """A connection to the head, as a driver: see the module docstring."""

    var _head: Stream
    var _driver: UInt64
    var _counter: UInt64
    var _hash: UInt64
    var _role: UInt8
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
    """The head's `host:port`, to resume with after a restart, or "" in local
    mode, where the head doesn't come back."""
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
    """Actors this client created and hasn't killed."""
    var _killed: Dict[UInt64, Bool]
    """Actors this client killed: calls on them fail here."""
    var _creates: Dict[UInt64, _Submission]
    """How each actor this client created and hasn't killed was created: a
    restarted head is told again."""
    var _kept_calls: Dict[UInt64, _Submission]
    """Calls to actors that no checkpoint includes yet: a restarted head
    restores the actor from its checkpoint and these run again."""
    var _driver_spans: List[_DriverSpan]

    def __init__(
        out self,
        var head: Stream,
        head_addr: String,
        registry_hash: UInt64,
        role: UInt8 = ROLE_DRIVER,
    ) raises:
        """Says hello to the head on `head` and becomes a driver.

        Args:
            head: A connection to the head.
            head_addr: Where to reconnect if the head restarts, or "".
            registry_hash: The registry every peer must share.
            role: `ROLE_DRIVER`, or `ROLE_CLIENT` for one inside a worker.
        """
        self._driver = _hello(head, role, registry_hash)
        self._head = head^
        self._hash = registry_hash
        self._role = role
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
        self._killed = Dict[UInt64, Bool]()
        self._creates = Dict[UInt64, _Submission]()
        self._kept_calls = Dict[UInt64, _Submission]()
        self._driver_spans = List[_DriverSpan]()

    def close(mut self):
        """Hangs up on the head: everything this client held is released."""
        self._head.close()

    def take_driver_spans(mut self) -> List[_DriverSpan]:
        """The driver's own spans recorded with `span`, handed over once."""
        var out = self._driver_spans^
        self._driver_spans = List[_DriverSpan]()
        return out^

    def _new_id(mut self) -> UInt64:
        self._counter += 1
        return (self._driver << DRIVER_SHIFT) | self._counter

    def remote[T: TaskLike](mut self, arg: T.A) raises -> ObjectRef[T.R]:
        """Submits task `T` on `arg` and returns a handle to its result."""
        if T.is_many():
            raise Error(T.name(), " has several results: use remote_many")
        var args = List[UInt8]()
        encode(arg, args)
        var id = self._new_id()
        self._submit(id, KIND_TASK, T.id(), args^, List[UInt64]())
        return ObjectRef[T.R](id)

    def remote[
        T: TaskLike
    ](mut self, arg: ObjectRef[T.A]) raises -> ObjectRef[T.R]:
        """Submits task `T` to run on another task's result, once it exists."""
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
        var deps = List[UInt64](capacity=len(args))
        for r in args:
            deps.append(r.id)
        return self._submit_after[T.R](T.id(), deps, True)

    def remote[
        X: AnyType, //, T: TaskLike
    ](mut self, arg: T.A, args: List[ObjectRef[X]]) raises -> ObjectRef[T.R]:
        """Submits task `T`, a `TaskWith`, on `arg` and these results: it runs
        once every result exists, and receives them in order as a `Seq`."""
        comptime assert (
            reflect[T].base_name() == "TaskWith"
        ), "a task given a value and a list of ObjectRefs must be a TaskWith"
        var value = List[UInt8]()
        encode(arg, value)
        var deps = List[UInt64](capacity=len(args))
        for r in args:
            deps.append(r.id)
        return self._submit_after[T.R](T.id(), deps, True, value)

    def remote_many[
        T: ManyLike
    ](mut self, arg: T.A, n: Int) raises -> List[ObjectRef[T.Part]]:
        """Submits task `T`, a `TaskMany`, on `arg`, for `n` results: each is
        an object of its own, the `n` values of the `Seq` it returns."""
        if n < 1:
            raise Error("remote_many needs at least one result")
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, UInt64(n))
        encode(arg, body)
        var refs = List[ObjectRef[T.Part]](capacity=n)
        for _ in range(n):
            refs.append(ObjectRef[T.Part](self._new_id()))
        for k in range(1, n):
            self._live[refs[k].id] = True
        self._submit(refs[0].id, KIND_TASK_MANY, T.id(), body^, List[UInt64]())
        return refs^

    def create[
        T: ActorLike
    ](mut self, arg: T.A, checkpoint_every: Int = 0) raises -> ActorHandle[T]:
        """Starts actor `T` on a worker of its own, built from `arg`.

        It returns at once; calls made before the actor is up wait for it.
        If the constructor raises, every call fails with its error. With
        `checkpoint_every` set, its state is checkpointed after every that
        many calls.
        """
        if checkpoint_every < 0:
            raise Error("checkpoint_every can't be negative")
        var args = List[UInt8]()
        put_scalar[DType.uint64](args, UInt64(checkpoint_every))
        encode(arg, args)
        var id = self._new_id()
        self._actors[id] = True
        self._creates[id] = _Submission(
            KIND_ACTOR_CREATE, T.id(), args.copy(), List[UInt64]()
        )
        self._submit(id, KIND_ACTOR_CREATE, T.id(), args^, List[UInt64]())
        return ActorHandle[T](id)

    def call[
        M: MethodLike
    ](mut self, actor: ActorHandle[M.Act], arg: M.A) raises -> ObjectRef[M.R]:
        """Calls method `M` on `actor` with `arg`, after every earlier call."""
        if actor.id in self._killed:
            raise Error(actor, " was killed")
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, actor.id)
        encode(arg, body)
        var id = self._new_id()
        self._kept_calls[id] = _Submission(
            KIND_ACTOR_CALL, M.id(), body.copy(), List[UInt64]()
        )
        self._submit(id, KIND_ACTOR_CALL, M.id(), body^, List[UInt64]())
        return ObjectRef[M.R](id)

    def checkpoint[
        T: ActorLike
    ](mut self, actor: ActorHandle[T]) raises -> ObjectRef[Int]:
        """Checkpoints `actor` after the calls made to it so far. The result
        is the number of calls the checkpoint covers, once it's written."""
        if actor.id in self._killed:
            raise Error(actor, " was killed")
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, actor.id)
        var id = self._new_id()
        self._submit(id, KIND_CHECKPOINT, 0, body^, List[UInt64]())
        return ObjectRef[Int](id)

    def kill[T: ActorLike](mut self, actor: ActorHandle[T]) raises:
        """Destroys `actor` once its earlier calls have run, and frees its
        worker. Their results stay until released."""
        if actor.id in self._killed:
            return
        self._killed[actor.id] = True
        _ = self._actors.pop(actor.id, False)
        _ = self._creates.pop(actor.id, _Submission(0, 0, [], []))
        self._forget_calls(actor.id, UInt64.MAX)
        self._released[actor.id] = True
        self._drop_local(actor.id)
        self._send(KIND_ACTOR_KILL, actor.id, List[UInt8]())

    def _submit_after[
        R: AnyType
    ](
        mut self,
        task_id: UInt64,
        deps: List[UInt64],
        gather: Bool,
        value: List[UInt8] = List[UInt8](),
    ) raises -> ObjectRef[R]:
        var flags = GATHER if gather else UInt8(0)
        if len(value) > 0:
            flags |= WITH_VALUE
        var body: List[UInt8] = [flags]
        put_scalar[DType.uint64](body, UInt64(len(deps)))
        for d in deps:
            put_scalar[DType.uint64](body, d)
        if len(value) > 0:
            put_scalar[DType.uint64](body, UInt64(len(value)))
            body.extend(value.copy())
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

    def _forget_calls(mut self, actor: UInt64, upto: UInt64):
        """Stops keeping the calls to `actor` up to object `upto`: a
        checkpoint has them."""
        var done = List[UInt64]()
        for entry in self._kept_calls.items():
            if entry.key > upto:
                continue
            var pos = 0
            try:
                if get_scalar[DType.uint64](entry.value.payload, pos) == actor:
                    done.append(entry.key)
            except:
                pass
        for id in done:
            _ = self._kept_calls.pop(id, _Submission(0, 0, [], []))

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
        var hello = _hello_payload(self._role, self._hash)
        put_scalar[DType.uint64](hello, self._driver)
        write_frame(self._head, KIND_HELLO, 0, 0, hello)
        var f = read_frame(self._head)
        if not f or f.value().kind != KIND_WELCOME:
            raise Error("the restarted head didn't welcome this driver back")
        # Oldest first, so every task's inputs are back before it is, and
        # every actor before its calls.
        for n in range(1, Int(self._counter) + 1):
            var id = (self._driver << DRIVER_SHIFT) | UInt64(n)
            if id in self._creates:
                # Its checkpoint, if it has one, is in object storage: the
                # new head restores it from there.
                ref c = self._creates[id]
                write_frame(self._head, c.kind, id, c.task_id, c.payload)
                continue
            if id not in self._live:
                if id in self._kept_calls:
                    # Released, but the actor's state needs it: run it
                    # again, and let go of its answer.
                    ref c = self._kept_calls[id]
                    write_frame(self._head, c.kind, id, c.task_id, c.payload)
                    write_frame(self._head, KIND_RELEASE, id, 0, List[UInt8]())
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
                continue
            if id in self._kept_calls:
                # Answered, but no checkpoint includes it: the restored actor
                # runs it again, and the head keeps the answer it has.
                ref c = self._kept_calls[id]
                write_frame(self._head, c.kind, id, c.task_id, c.payload)
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
            self._spans.extend(parse_timeline(frame.payload))
            self._timeline_answered = True
        elif frame.kind == KIND_COVERED:
            var pos = 0
            self._forget_calls(id, get_scalar[DType.uint64](frame.payload, pos))
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


struct TaskContext(Movable):
    """What a worker hands a `CtxTask`: a client of its own to the head,
    opened the first time a task asks for it."""

    var _connect_to: String
    var _resume_at: String
    var _hash: UInt64
    var _client: Optional[Client]

    def __init__(
        out self, connect_to: String, resume_at: String, registry_hash: UInt64
    ):
        """`connect_to` is the head's `host:port`; `resume_at` the same in a
        cluster, or "" in local mode, where the head doesn't come back."""
        self._connect_to = connect_to
        self._resume_at = resume_at
        self._hash = registry_hash
        self._client = None

    def client(mut self) raises -> ref[self._client._value] Client:
        if not self._client:
            if self._connect_to == "":
                raise Error("this worker has no head address for a client")
            self._client = Client(
                connect_head(self._connect_to),
                self._resume_at,
                self._hash,
                ROLE_CLIENT,
            )
        return self._client.value()


trait CtxTaskLike(TaskLike):
    """A task that runs with a `Client`, to call actors or submit tasks."""

    @staticmethod
    def run_ctx(
        args: Span[UInt8, _], mut out: List[UInt8], mut ctx: TaskContext
    ) raises:
        ...


struct CtxTask[
    A_: Movable & Deinitable,
    R_: Movable & Deinitable,
    f: def(mut Client, A_) thin raises -> R_,
](CtxTaskLike):
    """A remote function that also gets a `Client`: from inside the task it
    can call actors, whose handles it was given, and submit tasks.

    ```mojo
    def bump(mut c: Client, h: ActorHandle[CounterActor]) raises -> Int:
        var r = c.call[Add](h, 1)
        var n = c.get(r)
        c.release(r)
        return n

    comptime Bump = CtxTask[ActorHandle[CounterActor], Int, bump]
    ```

    A call from a task blocks that task until the answer comes; an actor
    calling back into the worker that's waiting on it would deadlock, and
    tasks never run on an actor's worker, so a task can't.
    """

    comptime A = Self.A_
    comptime R = Self.R_

    @staticmethod
    def name() -> StaticString:
        return reflect_fn[Self.f].linkage_name()

    @staticmethod
    def id() -> UInt64:
        return fnv1a64(Self.name())

    @staticmethod
    def run(args: Span[UInt8, _], mut out: List[UInt8]) raises:
        raise Error(Self.name(), " needs a context: run it through run_ctx")

    @staticmethod
    def run_ctx(
        args: Span[UInt8, _], mut out: List[UInt8], mut ctx: TaskContext
    ) raises:
        var pos = 0
        var a = decode[Self.A](args, pos)
        if pos != len(args):
            raise Error(
                Self.name(), ": ", len(args) - pos, " argument bytes left over"
            )
        var r = Self.f(ctx.client(), a)
        encode(r, out)


trait CtxMethodLike(MethodLike):
    """A method that runs with a `Client`, to call other actors."""

    @staticmethod
    def call_ctx(
        state: StatePtr,
        args: Span[UInt8, _],
        mut out: List[UInt8],
        mut ctx: TaskContext,
    ) raises:
        ...


struct CtxMethod[
    Act_: ActorLike,
    A_: Movable & Deinitable,
    R_: Movable & Deinitable,
    f: def(mut Act_.S, mut Client, A_) thin raises -> R_,
](CtxMethodLike):
    """A method of `Act_` that also gets a `Client`, so an actor can call
    another actor, or submit tasks.

    The call blocks this actor until the answer comes. An actor that calls
    one which, before answering, calls it back waits on itself: a cycle of
    actor calls deadlocks, as it would with one thread per actor anywhere.
    """

    comptime A = Self.A_
    comptime R = Self.R_
    comptime Act = Self.Act_

    @staticmethod
    def name() -> StaticString:
        return reflect_fn[Self.f].linkage_name()

    @staticmethod
    def id() -> UInt64:
        return fnv1a64(Self.name())

    @staticmethod
    def owner() -> UInt64:
        return Self.Act.id()

    @staticmethod
    def call(
        state: StatePtr, args: Span[UInt8, _], mut out: List[UInt8]
    ) raises:
        raise Error(Self.name(), " needs a context: run it through call_ctx")

    @staticmethod
    def call_ctx(
        state: StatePtr,
        args: Span[UInt8, _],
        mut out: List[UInt8],
        mut ctx: TaskContext,
    ) raises:
        var pos = 0
        var a = decode[Self.A](args, pos)
        if pos != len(args):
            raise Error(
                Self.name(), ": ", len(args) - pos, " argument bytes left over"
            )
        var r = Self.f(state.unsafe_bitcast[Self.Act.S]()[], ctx.client(), a)
        encode(r, out)

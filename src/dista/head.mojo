"""The head: one event loop that schedules every driver's tasks onto workers.

Drivers and workers connect to the head and say hello with their role and
registry hash; a mismatched hash is refused, so every process in a cluster
runs the same tasks. A driver gets a number, which becomes the top bits of
every object ID it creates, so the head knows where each result goes.

The loop `poll`s the listener and every connection. Drivers send tasks, ready
to run or waiting on other results; the head queues them, hands them to
workers round-robin up to `pipeline` in flight each, and forwards each result
or error to the driver that owns it. Every socket is non-blocking with its
own output queue (`Conn`), so a slow peer can't stall anyone else.

Small results come through the head, which keeps them, because a task
submitted later may take one as input. A large result stays on the worker
that computed it: the head records where, tells the owning driver, and sends
a task that needs it as task-inputs, so the worker running that task fetches
it from its peer. A worker that goes away fails the tasks it had in flight
and loses the results it held.

Every object is reference counted: its driver holds it until it releases it
(or disconnects), and each submitted task taking it as input holds it until
that task settles. At zero the head forgets the object and tells the worker
holding it, if one does, to delete it. An object released before its task
finishes is freed when the task settles.

The head keeps nothing on disk. When it restarts, its state comes back from
its peers: workers rejoin and report the results they hold, and each driver
reconnects under its old number and restores, claims or resubmits everything
it still needs (see `frame.mojo`). A reported result no resumed driver
claimed is freed. A task that was running when the head died may run again.

An actor is placed on a worker that hosts no other, the one with the fewest
tasks in flight, as soon as it's created; the worker builds it once those
tasks finish. From then on the head sends that worker only the actor's calls,
each as it arrives, so they run in order. An actor created when every worker
hosts one waits for a worker to join or an actor to be killed. Killing an
actor, or its driver leaving, frees its worker for tasks; a worker that goes
away fails its actor and the calls it had.
"""

from std.ffi import c_int

from dista.codec import get_scalar, put_scalar
from dista.conn import Conn
from dista.frame import (
    message,
    ADDR_SIZE,
    KIND_ACTOR_CALL,
    KIND_ACTOR_CREATE,
    KIND_ACTOR_KILL,
    KIND_CLAIM,
    KIND_ERROR,
    KIND_FREE,
    KIND_HELLO,
    KIND_HOLDING,
    KIND_LOCATED,
    KIND_RELEASE,
    KIND_RESULT,
    KIND_RESUMED,
    KIND_SPAN,
    KIND_STATS,
    KIND_STORED,
    KIND_TASK,
    KIND_TASK_AFTER,
    KIND_TASK_INPUTS,
    KIND_TIMELINE,
    KIND_WELCOME,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
)
from dista.socket import (
    POLLERR,
    POLLHUP,
    POLLIN,
    POLLOUT,
    Listener,
    poll,
    wall_ns,
)
from dista.store import Location

comptime DRIVER_SHIFT = 40
"""Object IDs carry their driver's number above this bit."""
comptime MAX_SPANS = 1_000_000
"""Finished task spans the head keeps per driver until it asks for them."""
comptime _ROLE_NONE: UInt8 = 0
comptime _ROLE_GONE: UInt8 = 255


def owner_of(object_id: UInt64) -> UInt64:
    """The number of the driver that created `object_id`."""
    return object_id >> DRIVER_SHIFT


@fieldwise_init
struct _Span(Copyable, Movable):
    """One task's timeline: the head's stamps and the worker's."""

    var object_id: UInt64
    var task_id: UInt64
    var worker: Int
    """The connection that ran it, or -1."""
    var submitted: UInt64
    var queued: UInt64
    var dispatched: UInt64
    var settled: UInt64
    var arrived: UInt64
    """On the worker: when the task reached it, before any input fetches."""
    var started: UInt64
    var ended: UInt64


struct _Task(Copyable, Movable):
    var object_id: UInt64
    var task_id: UInt64
    var args: List[UInt8]
    var deps: List[UInt64]
    var gather: Bool
    var kind: UInt8
    """`KIND_TASK` with the argument, or `KIND_TASK_INPUTS` with input specs."""

    def __init__(
        out self,
        object_id: UInt64,
        task_id: UInt64,
        var args: List[UInt8],
        var deps: List[UInt64] = List[UInt64](),
        gather: Bool = False,
        kind: UInt8 = KIND_TASK,
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.args = args^
        self.deps = deps^
        self.gather = gather
        self.kind = kind


struct _Actor(Copyable, Movable):
    var type_id: UInt64
    var args: List[UInt8]
    var worker: Int
    """The connection hosting it, or -1 until it's placed."""
    var pending: List[_Task]
    """Calls that arrived before it was placed."""
    var failed: String
    """Why it can't take calls, once its constructor failed or its worker
    went away."""
    var killed: Bool
    """Killed before it was placed: placed anyway, to run its calls, then
    killed."""

    def __init__(out self, type_id: UInt64, var args: List[UInt8]):
        self.type_id = type_id
        self.args = args^
        self.worker = -1
        self.pending = List[_Task]()
        self.failed = String()
        self.killed = False


struct Head(Movable):
    var _listener: Listener
    var _registry_hash: UInt64
    var _pipeline: Int
    var _exit_when_idle: Bool
    var _conns: List[Conn]
    var _roles: List[UInt8]
    var _inflight: List[List[UInt64]]
    """For a worker, the objects it is computing, oldest first."""
    var _driver_conn: Dict[UInt64, Int]
    var _next_driver: UInt64
    var _drivers_seen: Bool
    var _queue: List[_Task]
    var _queue_head: Int
    var _waiting: List[_Task]
    var _results: Dict[UInt64, List[UInt8]]
    var _errors: Dict[UInt64, String]
    var _worker_addr: List[List[UInt8]]
    """For a worker, its object server's address, as frames carry it."""
    var _location: Dict[UInt64, Int]
    """Results that stayed on a worker: object ID to that worker's connection.
    """
    var _where: Dict[UInt64, List[UInt8]]
    """Those results' encoded locations, as the worker reported them."""
    var _refs: Dict[UInt64, Int]
    """Every live object's reference count."""
    var _held: Dict[UInt64, Bool]
    """Objects whose driver hasn't released them; each counts once in `_refs`.
    """
    var _deps_of: Dict[UInt64, List[UInt64]]
    """A waiting task's inputs, each holding a reference until it settles."""
    var _dead: Dict[UInt64, Bool]
    """Objects freed before their task settled: dropped when it does."""
    var _orphans: Dict[UInt64, Bool]
    """Results a rejoining worker reported that no driver has claimed yet."""
    var _resumed: Dict[UInt64, Bool]
    """Drivers that have finished resuming with this head."""
    var _spans: Dict[UInt64, _Span]
    """Tasks in progress, by object."""
    var _finished: Dict[UInt64, List[_Span]]
    """Settled tasks' spans, by driver, until the driver asks for them."""
    var _actors: Dict[UInt64, _Actor]
    """Live actors, by ID."""
    var _unplaced: List[UInt64]
    """Actors waiting for a worker, oldest first."""
    var _hosting: List[UInt64]
    """For a worker, the actor it hosts, or 0."""

    def __init__(
        out self,
        var listener: Listener,
        registry_hash: UInt64,
        pipeline: Int,
        exit_when_idle: Bool,
    ):
        """A head serving `listener`.

        Args:
            listener: Where drivers and workers connect.
            registry_hash: What every peer's hello must carry.
            pipeline: Tasks in flight per worker.
            exit_when_idle: Return once the last driver leaves (local mode).
        """
        self._listener = listener^
        self._registry_hash = registry_hash
        self._pipeline = pipeline
        self._exit_when_idle = exit_when_idle
        self._conns = List[Conn]()
        self._roles = List[UInt8]()
        self._inflight = List[List[UInt64]]()
        self._driver_conn = Dict[UInt64, Int]()
        self._next_driver = 1
        self._drivers_seen = False
        self._queue = List[_Task]()
        self._queue_head = 0
        self._waiting = List[_Task]()
        self._results = Dict[UInt64, List[UInt8]]()
        self._errors = Dict[UInt64, String]()
        self._worker_addr = List[List[UInt8]]()
        self._location = Dict[UInt64, Int]()
        self._where = Dict[UInt64, List[UInt8]]()
        self._refs = Dict[UInt64, Int]()
        self._held = Dict[UInt64, Bool]()
        self._deps_of = Dict[UInt64, List[UInt64]]()
        self._dead = Dict[UInt64, Bool]()
        self._orphans = Dict[UInt64, Bool]()
        self._resumed = Dict[UInt64, Bool]()
        self._spans = Dict[UInt64, _Span]()
        self._finished = Dict[UInt64, List[_Span]]()
        self._actors = Dict[UInt64, _Actor]()
        self._unplaced = List[UInt64]()
        self._hosting = List[UInt64]()

    def run(mut self) raises:
        """Serves until the last driver leaves, in local mode; else forever."""
        while True:
            self._release_waiting()
            self._dispatch()
            if self._exit_when_idle and self._drivers_seen:
                if len(self._driver_conn) == 0:
                    self._shutdown()
                    return
            var fds: List[c_int] = [self._listener.fd]
            var events: List[Int16] = [POLLIN]
            var which: List[Int] = [-1]
            for i in range(len(self._conns)):
                if self._roles[i] == _ROLE_GONE:
                    continue
                fds.append(self._conns[i].fd())
                var ev = POLLIN
                if self._conns[i].wants_write():
                    ev |= POLLOUT
                events.append(ev)
                which.append(i)
            var revents = poll(fds, events, -1)
            if revents[0] & POLLIN:
                self._accept()
            for k in range(1, len(fds)):
                var i = which[k]
                if revents[k] == 0 or self._roles[i] == _ROLE_GONE:
                    continue
                try:
                    if revents[k] & POLLOUT:
                        self._conns[i].flush()
                    if revents[k] & (POLLIN | POLLERR | POLLHUP):
                        self._conns[i].read_available()
                        while True:
                            var f = self._conns[i].next_frame()
                            if not f:
                                break
                            self._handle(i, f.take())
                            if self._roles[i] == _ROLE_GONE:
                                break
                    if self._conns[i].closed:
                        self._drop(i)
                except e:
                    print("dista head: dropping connection:", e)
                    self._drop(i)

    def _accept(mut self) raises:
        var s = self._listener.accept()
        self._conns.append(Conn(s^))
        self._roles.append(_ROLE_NONE)
        self._inflight.append(List[UInt64]())
        self._worker_addr.append(List[UInt8]())
        self._hosting.append(0)

    def _handle(mut self, i: Int, var f: Frame) raises:
        var role = self._roles[i]
        if role == _ROLE_NONE:
            self._hello(i, f^)
        elif role == ROLE_DRIVER:
            if f.kind == KIND_TASK:
                var obj = f.object_id
                var task = f.task_id
                if self._known(obj):
                    return
                self._hold(obj)
                if self._adopt(obj):
                    return
                self._open_span(obj, task)
                self._queue.append(_Task(obj, task, f^.take_payload()))
                self._stamp_queued(obj)
            elif f.kind == KIND_TASK_AFTER:
                if len(f.payload) < 9:
                    raise Error("truncated task-after")
                var gather = f.payload[0] != 0
                var pos = 1
                var n = Int(get_scalar[DType.uint64](f.payload, pos))
                if n < 0 or n > (len(f.payload) - pos) // 8:
                    raise Error("task-after with a bad input count ", n)
                var deps = List[UInt64](capacity=n)
                for _ in range(n):
                    deps.append(get_scalar[DType.uint64](f.payload, pos))
                if self._known(f.object_id):
                    return
                self._hold(f.object_id)
                if self._adopt(f.object_id):
                    return
                var missing = False
                for d in deps:
                    if d not in self._refs:
                        missing = True
                if missing:
                    self._fail(
                        f.object_id,
                        String("an input was released before this task ran"),
                    )
                    return
                for d in deps:
                    self._refs[d] = self._refs.get(d, 0) + 1
                self._deps_of[f.object_id] = deps.copy()
                self._open_span(f.object_id, f.task_id)
                self._waiting.append(
                    _Task(
                        f.object_id,
                        f.task_id,
                        List[UInt8](),
                        deps=deps^,
                        gather=gather,
                    )
                )
            elif f.kind == KIND_RESULT or f.kind == KIND_ERROR:
                # Resuming: a small result or error the driver kept.
                var obj = f.object_id
                if self._known(obj):
                    return
                self._hold(obj)
                if f.kind == KIND_RESULT:
                    self._results[obj] = f^.take_payload()
                else:
                    self._errors[obj] = message(f.payload)
            elif f.kind == KIND_CLAIM:
                var obj = f.object_id
                if self._known(obj):
                    return
                self._hold(obj)
                _ = self._orphans.pop(obj, False)
                # The driver forgot where it was; say where this head has it.
                # If no worker has reported it yet, `_holding` says when one
                # does.
                if obj in self._location:
                    self._deliver(
                        obj, KIND_LOCATED, self._where.get(obj, List[UInt8]())
                    )
            elif f.kind == KIND_RESUMED:
                var d = owner_of(f.object_id)
                self._resumed[d] = True
                var unclaimed = List[UInt64]()
                for entry in self._orphans.items():
                    if owner_of(entry.key) == d:
                        unclaimed.append(entry.key)
                for obj in unclaimed:
                    _ = self._orphans.pop(obj, False)
                    if obj not in self._refs:
                        self._free(obj)
            elif f.kind == KIND_RELEASE:
                if self._held.pop(f.object_id, False):
                    self._dec(f.object_id)
            elif f.kind == KIND_ACTOR_CREATE:
                var a = f.object_id
                var type_id = f.task_id
                if self._known(a):
                    return
                self._hold(a)
                self._open_span(a, type_id)
                self._actors[a] = _Actor(type_id, f^.take_payload())
                self._unplaced.append(a)
            elif f.kind == KIND_ACTOR_CALL:
                self._call(f^)
            elif f.kind == KIND_ACTOR_KILL:
                self._kill(f.object_id)
            elif f.kind == KIND_TIMELINE:
                self._send_timeline(i)
            elif f.kind == KIND_STATS:
                var body = List[UInt8]()
                put_scalar[DType.uint64](body, UInt64(len(self._refs)))
                put_scalar[DType.uint64](body, UInt64(len(self._results)))
                put_scalar[DType.uint64](body, UInt64(len(self._errors)))
                put_scalar[DType.uint64](body, UInt64(len(self._location)))
                put_scalar[DType.uint64](
                    body, UInt64(self._queued() + len(self._waiting))
                )
                self._conns[i].send(KIND_STATS, 0, 0, body)
            else:
                raise Error("driver sent frame kind ", Int(f.kind))
        elif role == ROLE_WORKER:
            if f.kind == KIND_HOLDING:
                self._holding(i, f.payload)
                return
            if f.kind == KIND_SPAN:
                if f.object_id in self._spans:
                    var pos = 0
                    ref sp = self._spans[f.object_id]
                    sp.arrived = get_scalar[DType.uint64](f.payload, pos)
                    sp.started = get_scalar[DType.uint64](f.payload, pos)
                    sp.ended = get_scalar[DType.uint64](f.payload, pos)
                return
            if (
                f.kind != KIND_RESULT
                and f.kind != KIND_ERROR
                and f.kind != KIND_STORED
            ):
                raise Error("worker sent frame kind ", Int(f.kind))
            ref flight = self._inflight[i]
            for k in range(len(flight)):
                if flight[k] == f.object_id:
                    _ = flight.pop(k)
                    break
            var obj = f.object_id
            if self._dead.pop(obj, False):
                # Released while it ran: keep nothing, and free its copy.
                if f.kind == KIND_STORED:
                    self._conns[i].send(KIND_FREE, obj, 0, List[UInt8]())
            elif f.kind == KIND_RESULT:
                self._results[obj] = f.payload.copy()
                self._deliver(obj, f.kind, f.payload)
            elif f.kind == KIND_STORED:
                if obj in self._location:
                    # A task run twice across a head restart: keep one copy.
                    # On the same worker it's the same copy, rewritten.
                    if self._location.get(obj, -1) != i:
                        self._conns[i].send(KIND_FREE, obj, 0, List[UInt8]())
                else:
                    self._location[obj] = i
                    self._where[obj] = f.payload.copy()
                    self._deliver(obj, KIND_LOCATED, f.payload)
            else:
                self._errors[obj] = message(f.payload)
                self._deliver(obj, f.kind, f.payload)
                if obj in self._actors and self._hosting[i] == obj:
                    # Its constructor failed: the worker hosts nothing.
                    self._actors[obj].failed = String(
                        "actor failed to start: ", message(f.payload)
                    )
                    self._hosting[i] = 0
            self._settle(obj)

    # ── actors ───────────────────────────────────────────────────────────
    def _call(mut self, var f: Frame) raises:
        """A method call: to the actor's worker, or held until it's placed."""
        if len(f.payload) < 8:
            raise Error("truncated actor-call")
        var obj = f.object_id
        var pos = 0
        var a = get_scalar[DType.uint64](f.payload, pos)
        if self._known(obj):
            return
        self._hold(obj)
        self._open_span(obj, f.task_id)
        self._stamp_queued(obj)
        if a not in self._actors:
            self._fail(obj, String("no such actor: killed, or never created"))
            return
        ref act = self._actors[a]
        if act.killed:
            self._fail(obj, String("no such actor: killed, or never created"))
            return
        if act.failed:
            self._fail(obj, act.failed.copy())
            return
        var method = f.task_id
        var t = _Task(obj, method, f^.take_payload(), kind=KIND_ACTOR_CALL)
        if act.worker < 0:
            act.pending.append(t^)
        else:
            self._send_to(act.worker, t)

    def _send_to(mut self, i: Int, t: _Task):
        """Sends a task or call to worker `i` and counts it in flight there."""
        self._inflight[i].append(t.object_id)
        if t.object_id in self._spans:
            try:
                ref sp = self._spans[t.object_id]
                sp.worker = i
                sp.dispatched = wall_ns()
            except:
                pass
        try:
            self._conns[i].send(t.kind, t.object_id, t.task_id, t.args)
        except e:
            print("dista head: lost worker:", e)
            self._drop(i)

    def _place(mut self) raises:
        """Gives each waiting actor a worker of its own, while there are any."""
        while len(self._unplaced) > 0:
            var best = -1
            for i in range(len(self._conns)):
                if self._roles[i] != ROLE_WORKER or self._hosting[i] != 0:
                    continue
                if best < 0 or len(self._inflight[i]) < len(
                    self._inflight[best]
                ):
                    best = i
            if best < 0:
                return
            var a = self._unplaced.pop(0)
            if a not in self._actors:
                continue  # killed before it was placed
            self._hosting[best] = a
            ref act = self._actors[a]
            act.worker = best
            var create = _Task(
                a, act.type_id, act.args.copy(), kind=KIND_ACTOR_CREATE
            )
            var calls = act.pending.copy()
            act.pending.clear()
            var killed = act.killed
            self._send_to(best, create)
            for ref t in calls:
                if self._roles[best] != ROLE_WORKER:
                    # Lost sending: `_drop` failed the actor; fail the rest.
                    self._fail(
                        t.object_id, String("actor lost: its worker went away")
                    )
                else:
                    self._send_to(best, t)
            if killed:
                self._kill(a)

    def _kill(mut self, a: UInt64) raises:
        """Destroys actor `a` and frees its worker; its driver's done with it.
        """
        if a in self._actors and self._actors[a].worker < 0:
            ref act = self._actors[a]
            if len(act.pending) > 0 and not act.failed:
                # Its calls were sent first: they run, then it's killed.
                act.killed = True
            else:
                _ = self._actors.pop(a)
        elif a in self._actors:
            var act = self._actors.pop(a)
            var i = act.worker
            if i >= 0 and self._hosting[i] == a:
                self._hosting[i] = 0
                if self._roles[i] == ROLE_WORKER:
                    # After its calls: the worker runs frames in order.
                    try:
                        self._conns[i].send(
                            KIND_ACTOR_KILL, a, 0, List[UInt8]()
                        )
                    except e:
                        print("dista head: lost worker:", e)
                        self._drop(i)
            for ref t in act.pending:
                self._fail(t.object_id, String("the actor was killed"))
        if self._held.pop(a, False):
            self._dec(a)

    def _adopt(mut self, obj: UInt64) -> Bool:
        """A resubmitted task whose result a rejoining worker already reported:
        settle it with that result instead of running it again."""
        if not self._orphans.pop(obj, False):
            return False
        self._deliver(obj, KIND_LOCATED, self._where.get(obj, List[UInt8]()))
        return True

    def _holding(mut self, i: Int, payload: Span[UInt8, _]) raises:
        """A rejoining worker's results: keep the claimed, free the rest."""
        var pos = 0
        var n = Int(get_scalar[DType.uint64](payload, pos))
        for _ in range(n):
            var obj = get_scalar[DType.uint64](payload, pos)
            var start = pos
            _ = Location.decode(payload, pos)
            var loc = List[UInt8](payload[start:pos])
            if obj in self._location:
                # The task ran again first: one copy is enough. On the same
                # worker it's the same copy, rewritten.
                if self._location.get(obj, -1) != i:
                    self._conns[i].send(KIND_FREE, obj, 0, List[UInt8]())
                continue
            self._location[obj] = i
            self._where[obj] = loc.copy()
            if obj in self._refs:
                # Claimed already: tell the driver where it is again.
                self._deliver(obj, KIND_LOCATED, loc)
            elif owner_of(obj) in self._resumed:
                self._free(obj)
            else:
                self._orphans[obj] = True

    def _hello(mut self, i: Int, var f: Frame) raises:
        if f.kind != KIND_HELLO or len(f.payload) < 9:
            raise Error("expected a hello frame")
        var pos = 1
        var hash = get_scalar[DType.uint64](f.payload, pos)
        if hash != self._registry_hash:
            var msg = String(
                "registry mismatch: this peer was built with different tasks"
            )
            self._conns[i].send(KIND_ERROR, 0, 0, msg.as_bytes())
            self._roles[i] = _ROLE_GONE
            self._conns[i].stream.close()
            return
        if f.payload[0] == ROLE_DRIVER:
            var n = self._next_driver
            if len(f.payload) == 17:
                # A driver resuming after a head restart keeps its number.
                var p = 9
                n = get_scalar[DType.uint64](f.payload, p)
                if n in self._driver_conn:
                    raise Error("driver ", n, " is already connected")
                if n >= self._next_driver:
                    self._next_driver = n + 1
            else:
                self._next_driver += 1
            self._roles[i] = ROLE_DRIVER
            self._driver_conn[n] = i
            self._drivers_seen = True
            var body = List[UInt8]()
            put_scalar[DType.uint64](body, n)
            self._conns[i].send(KIND_WELCOME, 0, 0, body)
        elif f.payload[0] == ROLE_WORKER:
            if len(f.payload) != 9 + ADDR_SIZE:
                raise Error("a worker's hello must carry its address")
            self._roles[i] = ROLE_WORKER
            self._worker_addr[i] = List[UInt8](f.payload[9:])
        else:
            raise Error("unknown role ", Int(f.payload[0]))

    def _deliver(
        mut self, object_id: UInt64, kind: UInt8, payload: Span[UInt8, _]
    ):
        """Forwards a result or error to the driver that owns it, if it's here.
        """
        var d = owner_of(object_id)
        if d not in self._driver_conn:
            return
        var i = self._driver_conn.get(d, -1)
        try:
            self._conns[i].send(kind, object_id, 0, payload)
        except e:
            print("dista head: lost driver", d, ":", e)
            self._drop(i)

    def _fail(mut self, object_id: UInt64, var msg: String):
        if not self._dead.pop(object_id, False):
            self._errors[object_id] = msg.copy()
            self._deliver(object_id, KIND_ERROR, msg.as_bytes())
        self._settle(object_id)

    # ── reference counts ─────────────────────────────────────────────────
    def _known(mut self, object_id: UInt64) -> Bool:
        """Whether a driver's new object is one this head already has: sent
        again, as by a driver that lost its connection. It's the same object,
        held by its driver again, not a second one."""
        if self._dead.pop(object_id, False):
            # Its driver let go while its task ran: held again, kept when the
            # task settles.
            self._hold(object_id)
            return True
        if object_id not in self._refs:
            return False
        if not self._held.get(object_id, False):
            self._held[object_id] = True
            self._refs[object_id] = self._refs.get(object_id, 0) + 1
        return True

    def _hold(mut self, object_id: UInt64):
        """A new object, held by its driver."""
        self._refs[object_id] = 1
        self._held[object_id] = True

    def _settle(mut self, object_id: UInt64):
        """`object_id`'s task is done: it no longer holds its inputs."""
        var deps = self._deps_of.pop(object_id, List[UInt64]())
        for d in deps:
            self._dec(d)
        self._close_span(object_id)

    # ── timeline ─────────────────────────────────────────────────────────
    def _open_span(mut self, object_id: UInt64, task_id: UInt64):
        self._spans[object_id] = _Span(
            object_id, task_id, -1, wall_ns(), 0, 0, 0, 0, 0, 0
        )

    def _stamp_queued(mut self, object_id: UInt64):
        if object_id in self._spans:
            try:
                self._spans[object_id].queued = wall_ns()
            except:
                pass

    def _close_span(mut self, object_id: UInt64):
        var sp = self._spans.pop(
            object_id, _Span(0, 0, -1, 0, 0, 0, 0, 0, 0, 0)
        )
        if sp.submitted == 0:
            return
        sp.settled = wall_ns()
        var d = owner_of(object_id)
        if d not in self._finished:
            self._finished[d] = List[_Span]()
        try:
            ref done = self._finished[d]
            if len(done) < MAX_SPANS:
                done.append(sp^)
        except:
            pass

    def _send_timeline(mut self, i: Int) raises:
        """Sends driver `i` its finished spans, and forgets them."""
        var d = UInt64(0)
        for entry in self._driver_conn.items():
            if entry.value == i:
                d = entry.key
        var done = self._finished.pop(d, List[_Span]())
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, UInt64(len(done)))
        for ref sp in done:
            put_scalar[DType.uint64](body, sp.object_id)
            put_scalar[DType.uint64](body, sp.task_id)
            for v in [
                sp.submitted,
                sp.queued,
                sp.dispatched,
                sp.settled,
                sp.arrived,
                sp.started,
                sp.ended,
            ]:
                put_scalar[DType.uint64](body, v)
            if sp.worker >= 0 and sp.worker < len(self._worker_addr):
                body.extend(self._worker_addr[sp.worker].copy())
            else:
                body.extend(List[UInt8](length=ADDR_SIZE, fill=0))
        self._conns[i].send(KIND_TIMELINE, 0, 0, body)

    def _dec(mut self, object_id: UInt64):
        if object_id not in self._refs:
            return
        var n = self._refs.get(object_id, 0) - 1
        if n > 0:
            self._refs[object_id] = n
            return
        _ = self._refs.pop(object_id, 0)
        if (
            object_id in self._results
            or object_id in self._errors
            or object_id in self._location
        ):
            self._free(object_id)
        else:
            self._dead[object_id] = True

    def _free(mut self, object_id: UInt64):
        """Forgets a settled object, and has its worker delete it."""
        _ = self._results.pop(object_id, List[UInt8]())
        _ = self._errors.pop(object_id, String())
        _ = self._where.pop(object_id, List[UInt8]())
        var i = self._location.pop(object_id, -1)
        if i >= 0 and self._roles[i] == ROLE_WORKER:
            try:
                self._conns[i].send(KIND_FREE, object_id, 0, List[UInt8]())
            except e:
                print("dista head: lost worker:", e)
                self._drop(i)

    def _drop(mut self, i: Int):
        """Forgets connection `i`. A worker's in-flight tasks fail."""
        var role = self._roles[i]
        if role == _ROLE_GONE:
            return
        self._roles[i] = _ROLE_GONE
        self._conns[i].stream.close()
        if role == ROLE_DRIVER:
            var gone = UInt64(0)
            for entry in self._driver_conn.items():
                if entry.value == i:
                    gone = entry.key
            if gone != 0:
                _ = self._driver_conn.pop(gone, -1)
                # A driver that's gone holds nothing.
                var mine = List[UInt64]()
                for entry in self._held.items():
                    if owner_of(entry.key) == gone:
                        mine.append(entry.key)
                var actors = List[UInt64]()
                for entry in self._actors.items():
                    if owner_of(entry.key) == gone:
                        actors.append(entry.key)
                for a in actors:
                    try:
                        self._kill(a)
                    except:
                        pass
                for obj in mine:
                    if self._held.pop(obj, False):
                        self._dec(obj)
        elif role == ROLE_WORKER:
            var a = self._hosting[i]
            self._hosting[i] = 0
            if a in self._actors:
                try:
                    self._actors[a].failed = String(
                        "actor lost: its worker went away"
                    )
                except:
                    pass
            var lost = self._inflight[i].copy()
            self._inflight[i].clear()
            for obj in lost:
                self._fail(obj, String("worker lost while running this task"))
            var held = List[UInt64]()
            for entry in self._location.items():
                if entry.value == i:
                    held.append(entry.key)
            for obj in held:
                _ = self._location.pop(obj, -1)
                _ = self._where.pop(obj, List[UInt8]())
                if self._orphans.pop(obj, False):
                    continue
                self._fail(obj, String("result lost: its worker went away"))

    def _shutdown(mut self):
        for i in range(len(self._conns)):
            if self._roles[i] != _ROLE_GONE:
                self._roles[i] = _ROLE_GONE
                self._conns[i].stream.close()

    def _release_waiting(mut self):
        if len(self._waiting) == 0:
            return
        var still = List[_Task]()
        for i in range(len(self._waiting)):
            ref t = self._waiting[i]
            var failed = String()
            var settled = True
            var remote = False
            for d in t.deps:
                if d in self._errors:
                    failed = self._errors.get(d, String())
                elif d in self._location:
                    remote = True
                elif d not in self._results:
                    settled = False
            if failed:
                self._fail(t.object_id, String("input failed: ", failed))
            elif not settled:
                still.append(t.copy())
            elif not remote:
                var args = List[UInt8]()
                if t.gather:
                    put_scalar[DType.int64](args, Int64(len(t.deps)))
                for d in t.deps:
                    args.extend(self._results.get(d, List[UInt8]()))
                self._queue.append(_Task(t.object_id, t.task_id, args^))
                self._stamp_queued(t.object_id)
            else:
                # Some inputs live on workers: tell the worker that runs this
                # task where to fetch them, and pass the rest inline.
                var spec: List[UInt8] = [UInt8(1) if t.gather else UInt8(0)]
                put_scalar[DType.uint64](spec, UInt64(len(t.deps)))
                for d in t.deps:
                    if d in self._location:
                        spec.append(1)
                        put_scalar[DType.uint64](spec, d)
                        spec.extend(self._where.get(d, List[UInt8]()))
                    else:
                        var r = self._results.get(d, List[UInt8]())
                        spec.append(0)
                        put_scalar[DType.uint64](spec, d)
                        put_scalar[DType.uint64](spec, UInt64(len(r)))
                        spec.extend(r^)
                self._queue.append(
                    _Task(t.object_id, t.task_id, spec^, kind=KIND_TASK_INPUTS)
                )
                self._stamp_queued(t.object_id)
        self._waiting = still^

    def _queued(self) -> Int:
        return len(self._queue) - self._queue_head

    def _dispatch(mut self):
        """Places waiting actors, then hands queued tasks to the workers that
        host none, one round per pipeline slot."""
        try:
            self._place()
        except e:
            print("dista head: placing an actor:", e)
        for depth in range(self._pipeline):
            for i in range(len(self._conns)):
                if self._queued() == 0:
                    break
                if self._roles[i] != ROLE_WORKER or self._hosting[i] != 0:
                    continue
                if len(self._inflight[i]) != depth:
                    continue
                ref t = self._queue[self._queue_head]
                self._queue_head += 1
                self._inflight[i].append(t.object_id)
                if t.object_id in self._spans:
                    try:
                        ref sp = self._spans[t.object_id]
                        sp.worker = i
                        sp.dispatched = wall_ns()
                    except:
                        pass
                try:
                    self._conns[i].send(t.kind, t.object_id, t.task_id, t.args)
                except e:
                    print("dista head: lost worker:", e)
                    self._drop(i)
        if self._queue_head == len(self._queue):
            self._queue.clear()
            self._queue_head = 0

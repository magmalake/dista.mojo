"""A worker: runs tasks for the head and serves its results to peers.

A small result (up to `INLINE_LIMIT` bytes) goes back to the head, which
forwards it. A larger one stays here: in a shared mapping in the node's store
(see `store.mojo`), or in memory if the store is off. The worker tells the
head where it is; a reader on this host maps the file, and one elsewhere
fetches it from this worker's object server.

So a worker is an event loop over the head, its listener, the drivers and
workers fetching from it, and the workers it fetches from. A task whose
inputs live on other workers waits, parked, until they've all arrived; the
worker keeps serving fetches meanwhile. No worker ever waits on another one
synchronously, so two workers that need each other's results can't deadlock.
Running a task does block the loop, so fetches from this worker wait until
the current task finishes.

A result here lives until the head says free, once nothing references it.
Inputs fetched from peers are copies, kept only until the tasks waiting on
them have their arguments.

A worker the head gives an actor builds its state and then runs only that
actor's calls, in the order they come. A kill destroys the state, and the
worker goes back to running tasks. Between calls the head may ask for a
checkpoint: the worker writes the state through the codec to
`DISTA_CHECKPOINT_URI` (see `checkpoint.mojo`), and deletes the checkpoint
before it. An actor restarted elsewhere is restored from the last one.

If the head goes away, `run` returns and the results here stay. `rejoin`
introduces the worker to a new head and reports what it holds; the tasks it
had queued are dropped, since their drivers resubmit them. An actor it
hosted is destroyed: actors don't survive a head restart yet, though their
checkpoints stay.
"""

from std.ffi import c_int, external_call
from std.os import getenv

from dista.codec import get_scalar, put_scalar
from dista.conn import Conn
from dista.frame import (
    message,
    KIND_ACTOR_CALL,
    KIND_ACTOR_CREATE,
    KIND_ACTOR_KILL,
    KIND_ACTOR_RESTORE,
    KIND_CHECKPOINT,
    KIND_CHECKPOINTED,
    KIND_ERROR,
    KIND_FETCH,
    KIND_FREE,
    KIND_HELLO,
    KIND_HOLDING,
    KIND_RESULT,
    KIND_SPAN,
    KIND_STORED,
    KIND_TASK,
    KIND_TASK_INPUTS,
    ROLE_WORKER,
    Frame,
    encode_addr,
)
from dista.checkpoint import (
    checkpoint_location,
    delete_checkpoint,
    list_checkpoints,
    read_checkpoint,
    write_checkpoint,
)
from dista.client import TaskContext
from dista.head import owner_of
from dista.task import StatePtr
from dista.store import (
    Location,
    node_name,
    worker_token,
    read_shared,
    remove_file,
    store_dir,
    write_shared,
)
from dista.socket import (
    POLLERR,
    POLLHUP,
    POLLIN,
    POLLOUT,
    Listener,
    Stream,
    addr_key,
    poll,
    resolve_ipv4,
    wall_ns,
)

comptime INLINE_LIMIT = 64 * 1024
"""Results up to this size travel through the head; larger ones stay put."""

comptime DispatchFn = def(
    UInt64, List[UInt8], mut List[UInt8], mut TaskContext
) thin raises
"""Runs task `task_id` on encoded arguments, appending the encoded result; a
`CtxTask` gets the worker's `TaskContext`."""

comptime CreateFn = def(UInt64, List[UInt8]) thin raises -> StatePtr
"""Builds actor type `type_id`'s state from its encoded argument."""

comptime CallFn = def(
    UInt64, UInt64, StatePtr, List[UInt8], mut List[UInt8], mut TaskContext
) thin raises
"""Runs method `method_id` on the state of an actor of type `type_id`."""

comptime DestroyFn = def(UInt64, StatePtr) thin
"""Destroys the state of an actor of type `type_id`."""

comptime SaveFn = def(UInt64, StatePtr, mut List[UInt8]) thin raises
"""Appends the encoded state of an actor of type `type_id`."""

comptime RestoreFn = def(UInt64, List[UInt8]) thin raises -> StatePtr
"""Builds actor type `type_id`'s state from a checkpoint."""


@fieldwise_init
struct InputSpec(Copyable, Movable):
    """One input in a task-inputs frame: its bytes, or where to get them."""

    var object_id: UInt64
    var inline: Bool
    var bytes: List[UInt8]
    var location: Optional[Location]


@fieldwise_init
struct TaskInputs(Movable):
    var gather: Bool
    var inputs: List[InputSpec]


def parse_task_inputs(payload: Span[UInt8, _]) raises -> TaskInputs:
    """A task-inputs payload, checked: a malformed one raises."""
    if len(payload) < 9:
        raise Error("truncated task-inputs")
    var gather = payload[0] != 0
    var pos = 1
    var n = Int(get_scalar[DType.uint64](payload, pos))
    if n < 0 or n > len(payload):
        raise Error("bad task-inputs count ", n)
    var inputs = List[InputSpec](capacity=n)
    for _ in range(n):
        if pos >= len(payload):
            raise Error("truncated task-inputs")
        var tag = payload[pos]
        pos += 1
        var obj = get_scalar[DType.uint64](payload, pos)
        if tag == 0:
            var size = Int(get_scalar[DType.uint64](payload, pos))
            if size < 0 or size > len(payload) - pos:
                raise Error("bad task-inputs length ", size)
            inputs.append(
                InputSpec(
                    obj, True, List[UInt8](payload[pos : pos + size]), None
                )
            )
            pos += size
        elif tag == 1:
            inputs.append(
                InputSpec(
                    obj, False, List[UInt8](), Location.decode(payload, pos)
                )
            )
        else:
            raise Error("bad task-inputs tag ", Int(tag))
    return TaskInputs(gather, inputs^)


struct _Input(Copyable, Movable):
    """One input of a parked task: bytes already here, or an object to fetch."""

    var object_id: UInt64
    var have: Bool
    var bytes: List[UInt8]

    def __init__(
        out self, object_id: UInt64, have: Bool, var bytes: List[UInt8]
    ):
        self.object_id = object_id
        self.have = have
        self.bytes = bytes^


struct _Parked(Copyable, Movable):
    var object_id: UInt64
    var task_id: UInt64
    var gather: Bool
    var inputs: List[_Input]
    var arrived: UInt64

    def __init__(
        out self,
        object_id: UInt64,
        task_id: UInt64,
        gather: Bool,
        var inputs: List[_Input],
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.gather = gather
        self.inputs = inputs^
        self.arrived = wall_ns()


@fieldwise_init
struct _Answer(Copyable, Movable):
    """A call's answer, kept so a checkpoint can carry it."""

    var calls: Int
    """The calls the actor had run once it ran this one."""
    var kind: UInt8
    var bytes: List[UInt8]


struct _Ready(Movable):
    var object_id: UInt64
    var task_id: UInt64
    var args: List[UInt8]
    var arrived: UInt64
    """When the task reached this worker, before any of its inputs did."""
    var kind: UInt8
    """`KIND_TASK`, or the actor frame this is: create, call or kill."""

    def __init__(
        out self,
        object_id: UInt64,
        task_id: UInt64,
        var args: List[UInt8],
        arrived: UInt64 = 0,
        kind: UInt8 = KIND_TASK,
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.args = args^
        self.arrived = arrived if arrived != 0 else wall_ns()
        self.kind = kind


struct Worker[
    dispatch: DispatchFn,
    create: CreateFn,
    call: CallFn,
    destroy: DestroyFn,
    save: SaveFn,
    restore: RestoreFn,
](Movable):
    var _head: Conn
    var _listener: Listener
    var _key: String
    """This worker's own address, `ip:port`."""
    var _serving: List[Conn]
    """Connections from drivers and workers that fetch from here."""
    var _peers: List[Conn]
    """Connections to workers this one fetches from."""
    var _peer_index: Dict[String, Int]
    var _objects: Dict[UInt64, List[UInt8]]
    """Results stored here, and inputs fetched from peers."""
    var _lost: Dict[UInt64, String]
    var _requested: Dict[UInt64, Bool]
    var _parked: List[_Parked]
    var _ready: List[_Ready]
    var _ip: Array[UInt8, 4]
    var _port: Int
    var _node: String
    var _dir: String
    """The node store's directory, or "" when results stay in memory."""
    var _files: Dict[UInt64, Location]
    """Results this worker wrote into the node store."""
    var _token: UInt64
    """Names this worker's node-store files: `dista-<token>-<object>`."""
    var _fetched: Dict[UInt64, Bool]
    """Objects in `_objects` that are copies of inputs, not results made here.
    """
    var _debug: Bool
    """`DISTA_DEBUG=1`: log each task's inputs as they're assembled."""
    var _actor: UInt64
    """The actor hosted here, or 0."""
    var _actor_type: UInt64
    var _state: Optional[StatePtr]
    """Its state, unless its constructor failed."""
    var _actor_error: String
    """Why its constructor failed."""
    var _marks: Dict[UInt64, UInt64]
    """For each caller, the newest of its calls the actor's state includes:
    saved with each checkpoint, so a call resent after a head restart that
    the restored state has already seen doesn't run twice."""
    var _answers: Dict[UInt64, _Answer]
    """Answers to the actor's recent calls, by object, from its first
    checkpoint on: a checkpoint carries those since the one before it, so
    a call resent after a head restart that the restored state has seen is
    answered rather than failed."""
    var _calls: Int
    """The calls the actor has run, its checkpoint's included."""
    var _checkpointed: Bool
    var _written: List[Int]
    """The actor's checkpoints written or restored here, by the calls each
    covers, that may still be restored from."""
    var _ctx: TaskContext
    """The client a `CtxTask` calls actors through, opened on first use."""

    def __init__(
        out self,
        var head: Stream,
        registry_hash: UInt64,
        head_addr: String = "",
        resume_at: String = "",
    ) raises:
        """Starts the object server and says hello to the head on `head`.

        `head_addr` is where a task's client connects, and `resume_at` where
        it reconnects after a head restart ("" in local mode)."""
        var listener = Listener(0, any_interface=True)
        var advertise = getenv("DISTA_ADVERTISE")
        var ip = resolve_ipv4(advertise) if advertise != "" else head.local_ip()
        self._key = addr_key(ip, listener.port)
        self._head = Conn(head^)
        self._head.send(
            KIND_HELLO, 0, 0, Self._hello(registry_hash, ip, listener.port)
        )
        self._listener = listener^
        self._serving = List[Conn]()
        self._peers = List[Conn]()
        self._peer_index = Dict[String, Int]()
        self._objects = Dict[UInt64, List[UInt8]]()
        self._lost = Dict[UInt64, String]()
        self._requested = Dict[UInt64, Bool]()
        self._parked = List[_Parked]()
        self._ready = List[_Ready]()
        self._ip = ip.copy()
        self._port = self._listener.port
        self._node = node_name()
        self._dir = store_dir()
        self._files = Dict[UInt64, Location]()
        self._token = worker_token()
        self._fetched = Dict[UInt64, Bool]()
        self._debug = getenv("DISTA_DEBUG") == "1"
        self._actor = 0
        self._actor_type = 0
        self._state = None
        self._actor_error = String()
        self._written = List[Int]()
        self._marks = Dict[UInt64, UInt64]()
        self._answers = Dict[UInt64, _Answer]()
        self._calls = 0
        self._checkpointed = False
        self._ctx = TaskContext(head_addr, resume_at, registry_hash)

    @staticmethod
    def _hello(
        registry_hash: UInt64, ip: Array[UInt8, 4], port: Int
    ) -> List[UInt8]:
        var hello: List[UInt8] = [ROLE_WORKER]
        put_scalar[DType.uint64](hello, registry_hash)
        hello.extend(encode_addr(ip, port))
        return hello^

    def rejoin(mut self, var head: Stream, registry_hash: UInt64) raises:
        """Says hello to a new head and reports the results held here."""
        self._head = Conn(head^)
        self._end_actor()
        self._ready.clear()
        self._parked.clear()
        self._requested.clear()
        for entry in self._fetched.items():
            _ = self._objects.pop(entry.key, List[UInt8]())
        self._fetched.clear()
        self._head.send(
            KIND_HELLO, 0, 0, Self._hello(registry_hash, self._ip, self._port)
        )
        var body = List[UInt8]()
        put_scalar[DType.uint64](
            body, UInt64(len(self._files) + len(self._objects))
        )
        for entry in self._files.items():
            put_scalar[DType.uint64](body, entry.key)
            body.extend(entry.value.encode())
        for entry in self._objects.items():
            put_scalar[DType.uint64](body, entry.key)
            body.extend(
                Location(
                    self._ip, self._port, self._node, "", len(entry.value)
                ).encode()
            )
        self._head.send(KIND_HOLDING, 0, 0, body)

    def __deinit__(deinit self):
        # Results in the node store are files: remove them with the worker.
        for entry in self._files.items():
            remove_file(entry.value.path)

    def run(mut self) raises:
        """Serves until the head hangs up."""
        while True:
            self._poll_once(timeout_ms=0 if len(self._ready) > 0 else -1)
            if self._head.closed:
                return
            self._unpark()
            if len(self._ready) > 0:
                self._run_one()

    def _poll_once(mut self, timeout_ms: Int) raises:
        var fds: List[c_int] = [self._head.fd(), self._listener.fd]
        var events: List[Int16] = [self._events(self._head), POLLIN]
        for i in range(len(self._serving)):
            fds.append(self._serving[i].fd())
            events.append(self._events(self._serving[i]))
        for i in range(len(self._peers)):
            fds.append(self._peers[i].fd())
            events.append(self._events(self._peers[i]))
        # Handling frames can accept a fetcher or open a peer connection; only
        # the connections that were polled have results.
        var n_serving = len(self._serving)
        var n_peers = len(self._peers)
        var rev = poll(fds, events, timeout_ms)
        if rev[0] != 0:
            try:
                self._pump(self._head, rev[0])
            except:
                # A reset from a head that died: the same as it hanging up.
                self._head.closed = True
            while True:
                var f = self._head.next_frame()
                if not f:
                    break
                self._from_head(f.take())
        var k = 2
        var dead = List[Int]()
        for i in range(n_serving):
            if rev[k] != 0:
                try:
                    self._pump(self._serving[i], rev[k])
                    while True:
                        var f = self._serving[i].next_frame()
                        if not f:
                            break
                        self._serve_fetch(i, f.take())
                except:
                    self._serving[i].closed = True
            if self._serving[i].closed:
                dead.append(i)
            k += 1
        for i in range(n_peers):
            if rev[k] != 0:
                self._pump(self._peers[i], rev[k])
                while True:
                    var f = self._peers[i].next_frame()
                    if not f:
                        break
                    self._from_peer(f.take())
            k += 1
        for j in range(len(dead) - 1, -1, -1):
            _ = self._serving.pop(dead[j])
        if rev[1] & POLLIN:
            self._serving.append(Conn(self._listener.accept()))

    @staticmethod
    def _events(c: Conn) -> Int16:
        return (POLLIN | POLLOUT) if c.wants_write() else POLLIN

    @staticmethod
    def _pump(mut c: Conn, revents: Int16) raises:
        if revents & POLLOUT:
            c.flush()
        if revents & (POLLIN | POLLERR | POLLHUP):
            c.read_available()

    def _from_head(mut self, var f: Frame) raises:
        if (
            f.kind == KIND_TASK
            or f.kind == KIND_ACTOR_CREATE
            or f.kind == KIND_ACTOR_CALL
            or f.kind == KIND_ACTOR_KILL
            or f.kind == KIND_CHECKPOINT
            or f.kind == KIND_ACTOR_RESTORE
        ):
            var obj = f.object_id
            var task = f.task_id
            var kind = f.kind
            self._ready.append(_Ready(obj, task, f^.take_payload(), kind=kind))
        elif f.kind == KIND_TASK_INPUTS:
            var spec = parse_task_inputs(f.payload)
            var gather = spec.gather
            var inputs = List[_Input](capacity=len(spec.inputs))
            for ref inp in spec.inputs:
                var obj = inp.object_id
                if inp.inline:
                    inputs.append(_Input(obj, True, inp.bytes.copy()))
                else:
                    var loc = inp.location.value().copy()
                    if loc.is_local() and obj not in self._objects:
                        try:
                            self._objects[obj] = read_shared(loc.path, loc.size)
                            self._fetched[obj] = True
                        except e:
                            self._lost[obj] = String(e)
                    else:
                        self._request(obj, loc.ip, loc.port)
                    inputs.append(_Input(obj, False, List[UInt8]()))
            self._parked.append(
                _Parked(f.object_id, f.task_id, gather, inputs^)
            )
        elif f.kind == KIND_FREE:
            _ = self._objects.pop(f.object_id, List[UInt8]())
            if f.object_id in self._files:
                remove_file(self._files.pop(f.object_id).path)
        elif f.kind == KIND_ERROR:
            raise Error(
                "the head refused this worker: ",
                message(f.payload),
            )
        else:
            raise Error("worker got frame kind ", Int(f.kind), " from the head")

    def _request(mut self, obj: UInt64, ip: Array[UInt8, 4], port: Int) raises:
        """Starts fetching `obj` from the worker at `ip:port`, once."""
        if obj in self._objects or obj in self._requested:
            return
        var key = addr_key(ip, port)
        if key == self._key:
            if obj not in self._objects:
                self._lost[obj] = String(
                    "object ", obj, " is not on its worker"
                )
            return
        self._requested[obj] = True
        if key not in self._peer_index:
            try:
                self._peers.append(Conn(Stream.connect(ip, port)))
            except e:
                self._lost[obj] = String(
                    "cannot reach the worker holding it: ", e
                )
                return
            self._peer_index[key] = len(self._peers) - 1
        var i = self._peer_index.get(key, -1)
        self._peers[i].send(KIND_FETCH, obj, 0, List[UInt8]())

    def _from_peer(mut self, var f: Frame) raises:
        _ = self._requested.pop(f.object_id, False)
        if f.kind == KIND_RESULT:
            var obj = f.object_id
            self._fetched[obj] = True
            self._objects[obj] = f^.take_payload()
        else:
            self._lost[f.object_id] = message(f.payload)

    def _serve_fetch(mut self, i: Int, var f: Frame) raises:
        if f.kind != KIND_FETCH:
            raise Error("expected a fetch, got frame kind ", Int(f.kind))
        if f.object_id in self._objects:
            self._serving[i].send(
                KIND_RESULT, f.object_id, 0, self._objects[f.object_id]
            )
        elif f.object_id in self._files:
            ref loc = self._files[f.object_id]
            var data = read_shared(loc.path, loc.size)
            self._serving[i].send(KIND_RESULT, f.object_id, 0, data)
        else:
            var msg = String("object ", f.object_id, " is not on this worker")
            self._serving[i].send(KIND_ERROR, f.object_id, 0, msg.as_bytes())

    def _unpark(mut self):
        if len(self._parked) == 0:
            return
        var still = List[_Parked]()
        for i in range(len(self._parked)):
            ref p = self._parked[i]
            var failed = String()
            var complete = True
            for ref inp in p.inputs:
                if inp.have:
                    continue
                if inp.object_id in self._lost:
                    failed = self._lost.get(inp.object_id, String())
                elif inp.object_id not in self._objects:
                    complete = False
            if failed:
                self._report_error(p.object_id, String("input lost: ", failed))
            elif not complete:
                still.append(p.copy())
            else:
                var args = List[UInt8]()
                if p.gather:
                    put_scalar[DType.int64](args, Int64(len(p.inputs)))
                if self._debug:
                    print("dista worker: task", p.object_id, "inputs:", end="")
                    for ref inp in p.inputs:
                        var n = len(inp.bytes) if inp.have else len(
                            self._objects.get(inp.object_id, List[UInt8]())
                        )
                        print(
                            " ",
                            inp.object_id,
                            "inline" if inp.have else "fetched",
                            n,
                            end="",
                        )
                    print()
                for ref inp in p.inputs:
                    if inp.have:
                        args.extend(inp.bytes.copy())
                    else:
                        args.extend(
                            self._objects.get(inp.object_id, List[UInt8]())
                        )
                self._ready.append(
                    _Ready(p.object_id, p.task_id, args^, p.arrived)
                )
        self._parked = still^
        # Fetched copies nothing parked still waits on have done their job.
        var done = List[UInt64]()
        for entry in self._fetched.items():
            var wanted = False
            for ref p in self._parked:
                for ref inp in p.inputs:
                    if inp.object_id == entry.key:
                        wanted = True
            if not wanted:
                done.append(entry.key)
        for obj in done:
            _ = self._fetched.pop(obj, False)
            _ = self._objects.pop(obj, List[UInt8]())

    def _report_error(mut self, obj: UInt64, msg: String):
        try:
            self._head.send(KIND_ERROR, obj, 0, msg.as_bytes())
        except:
            self._head.closed = True

    def _end_actor(mut self):
        """Destroys the actor hosted here, if any."""
        if self._state:
            Self.destroy(self._actor_type, self._state.value())
        self._state = None
        self._actor = 0
        self._actor_type = 0
        self._actor_error = String()
        self._written.clear()
        self._marks = Dict[UInt64, UInt64]()
        self._answers = Dict[UInt64, _Answer]()
        self._calls = 0
        self._checkpointed = False

    def _run_actor(mut self, var t: _Ready) raises:
        """Runs an actor frame: build the actor, call it, or destroy it."""
        var started = wall_ns()
        if t.kind == KIND_ACTOR_CREATE or t.kind == KIND_ACTOR_RESTORE:
            self._end_actor()
            self._actor = t.object_id
            self._actor_type = t.task_id
            try:
                if t.kind == KIND_ACTOR_CREATE:
                    self._state = Self.create(t.task_id, t.args)
                else:
                    var pos = 0
                    var calls = Int(get_scalar[DType.uint64](t.args, pos))
                    var bytes = read_checkpoint(
                        checkpoint_location(t.object_id, calls)
                    )
                    var at = self._read_header(bytes)
                    self._state = Self.restore(
                        t.task_id, List[UInt8](bytes[at:])
                    )
                    self._calls = calls
                    self._checkpointed = True
                    # Checkpoints the last worker wrote are this one's to
                    # delete once they're behind the head's.
                    self._written = list_checkpoints(t.object_id)
                    if calls not in self._written:
                        self._written.append(calls)
            except e:
                self._actor_error = String(e)
                self._span(t.object_id, t.arrived, started)
                self._report_error(t.object_id, self._actor_error.copy())
                return
            self._span(t.object_id, t.arrived, started)
            self._to_head(KIND_RESULT, t.object_id, List[UInt8]())
        elif t.kind == KIND_ACTOR_KILL:
            if t.object_id == self._actor:
                for c in self._written:
                    try:
                        delete_checkpoint(checkpoint_location(self._actor, c))
                    except:
                        pass
                self._end_actor()
        elif t.kind == KIND_CHECKPOINT:
            var pos = 0
            var keep = -1
            try:
                keep = Int(get_scalar[DType.int64](t.args, pos))
            except:
                pass
            self._checkpoint(t.object_id, Int(t.task_id), keep)
        else:
            self._calls += 1
            var obj = t.object_id
            var out = List[UInt8]()
            var ran = False
            try:
                var pos = 0
                var a = get_scalar[DType.uint64](t.args, pos)
                if a != self._actor:
                    raise Error("actor ", a, " is not on this worker")
                if not self._state:
                    raise Error("actor failed to start: ", self._actor_error)
                var caller = owner_of(obj)
                if obj <= self._marks.get(caller, 0):
                    # Resent after a head restart, and already in the state
                    # restored from the checkpoint: running it again would
                    # apply it twice. Its answer came with the checkpoint,
                    # unless it was too large to.
                    self._span(obj, t.arrived, started)
                    if obj in self._answers:
                        var ans = self._answers[obj].copy()
                        self._to_head(ans.kind, obj, ans.bytes)
                        return
                    raise Error(
                        "this call ran, but its answer was lost when the head"
                        " restarted"
                    )
                self._marks[caller] = obj
                ran = True
                Self.call(
                    t.task_id,
                    self._actor_type,
                    self._state.value(),
                    List[UInt8](t.args[8:]),
                    out,
                    self._ctx,
                )
            except e:
                var msg = String(e)
                if ran and self._checkpointed:
                    self._answers[obj] = _Answer(
                        self._calls, KIND_ERROR, List[UInt8](msg.as_bytes())
                    )
                self._span(obj, t.arrived, started)
                self._report_error(obj, msg)
                return
            if self._checkpointed and len(out) <= INLINE_LIMIT:
                self._answers[obj] = _Answer(
                    self._calls, KIND_RESULT, out.copy()
                )
            self._finish(obj, t.arrived, started, out^)

    def _read_header(mut self, bytes: Span[UInt8, _]) raises -> Int:
        """Reads a checkpoint's callers and answers; returns where the state
        starts."""
        var at = 0
        var n = Int(get_scalar[DType.uint64](bytes, at))
        if n < 0 or n > (len(bytes) - at) // 16:
            raise Error("a checkpoint with a bad caller count")
        for _ in range(n):
            var caller = get_scalar[DType.uint64](bytes, at)
            self._marks[caller] = get_scalar[DType.uint64](bytes, at)
        n = Int(get_scalar[DType.uint64](bytes, at))
        if n < 0 or n > (len(bytes) - at) // 25:
            raise Error("a checkpoint with a bad answer count")
        for _ in range(n):
            var obj = get_scalar[DType.uint64](bytes, at)
            var calls = Int(get_scalar[DType.uint64](bytes, at))
            if at >= len(bytes):
                raise Error("a truncated checkpoint")
            var kind = bytes[at]
            at += 1
            var size = Int(get_scalar[DType.uint64](bytes, at))
            if size < 0 or size > len(bytes) - at:
                raise Error("a truncated checkpoint")
            self._answers[obj] = _Answer(
                calls, kind, List[UInt8](bytes[at : at + size])
            )
            at += size
        return at

    def _header(mut self, after: Int) -> List[UInt8]:
        """A checkpoint's callers, and the answers to the calls after the
        `after`th; drops the answers before them, which it doesn't need."""
        var out = List[UInt8]()
        put_scalar[DType.uint64](out, UInt64(len(self._marks)))
        for entry in self._marks.items():
            put_scalar[DType.uint64](out, entry.key)
            put_scalar[DType.uint64](out, entry.value)
        var old = List[UInt64]()
        var body = List[UInt8]()
        var n = 0
        for entry in self._answers.items():
            if entry.value.calls <= after:
                old.append(entry.key)
                continue
            n += 1
            put_scalar[DType.uint64](body, entry.key)
            put_scalar[DType.uint64](body, UInt64(entry.value.calls))
            body.append(entry.value.kind)
            put_scalar[DType.uint64](body, UInt64(len(entry.value.bytes)))
            body.extend(entry.value.bytes.copy())
        for obj in old:
            _ = self._answers.pop(obj, _Answer(0, 0, List[UInt8]()))
        put_scalar[DType.uint64](out, UInt64(n))
        out.extend(body^)
        return out^

    def _checkpoint(mut self, a: UInt64, calls: Int, keep: Int):
        """Writes actor `a`'s state after `calls` calls, and says so.

        The head would restore from the checkpoint after `keep` calls (-1 for
        none) until it hears of this one, so only those before it go."""
        var why = String()
        try:
            if a != self._actor:
                raise Error("actor ", a, " is not on this worker")
            if not self._state:
                raise Error("actor failed to start: ", self._actor_error)
            if calls != self._calls:
                raise Error(
                    "the head counts ",
                    calls,
                    " calls, this worker ran ",
                    self._calls,
                )
            # Answers since the checkpoint before this one go with it: a
            # head that dies may not have passed them on.
            var before = -1
            for c in self._written:
                if c < calls:
                    before = max(before, c)
            self._checkpointed = True
            var bytes = self._header(before)
            Self.save(self._actor_type, self._state.value(), bytes)
            write_checkpoint(checkpoint_location(a, calls), bytes)
            var still = List[Int]()
            for c in self._written:
                if c < keep:
                    delete_checkpoint(checkpoint_location(a, c))
                elif c != calls:
                    still.append(c)
            still.append(calls)
            self._written = still^
        except e:
            why = String(e)
        try:
            self._head.send(KIND_CHECKPOINTED, a, UInt64(calls), why.as_bytes())
        except:
            self._head.closed = True

    def _run_one(mut self) raises:
        var t = self._ready.pop(0)
        if t.kind != KIND_TASK:
            self._run_actor(t^)
            return
        if t.object_id in self._files:
            # A task resubmitted across a head restart, whose result this
            # worker already has. Tasks are pure, so report that copy rather
            # than run it again and rewrite a file someone may be reading.
            self._to_head(
                KIND_STORED, t.object_id, self._files[t.object_id].encode()
            )
            return
        var out = List[UInt8]()
        var started = wall_ns()
        try:
            Self.dispatch(t.task_id, t.args, out, self._ctx)
        except e:
            self._span(t.object_id, t.arrived, started)
            self._report_error(t.object_id, String(e))
            return
        self._finish(t.object_id, t.arrived, started, out^)

    def _finish(
        mut self,
        obj: UInt64,
        arrived: UInt64,
        started: UInt64,
        var out: List[UInt8],
    ):
        """Sends a small result to the head, or stores a large one here."""
        if len(out) <= INLINE_LIMIT:
            self._span(obj, arrived, started)
            self._to_head(KIND_RESULT, obj, out)
        else:
            var path = String()
            if self._dir != "":
                path = String(self._dir, "/dista-", hex(self._token), "-", obj)
                try:
                    write_shared(path, out)
                except e:
                    print("dista worker: node store write failed:", e)
                    path = String()
            var loc = Location(self._ip, self._port, self._node, path, len(out))
            if path != "":
                self._files[obj] = loc.copy()
            else:
                self._objects[obj] = out^
            self._span(obj, arrived, started)  # storing included
            self._to_head(KIND_STORED, obj, loc.encode())

    def _span(mut self, obj: UInt64, arrived: UInt64, started: UInt64):
        """Tells the head when `obj`'s task arrived, started and ended here."""
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, arrived)
        put_scalar[DType.uint64](body, started)
        put_scalar[DType.uint64](body, wall_ns())
        self._to_head(KIND_SPAN, obj, body)

    def _to_head(mut self, kind: UInt8, obj: UInt64, payload: Span[UInt8, _]):
        """Sends to the head; a head that's gone is noticed by `run`."""
        try:
            self._head.send(kind, obj, 0, payload)
        except:
            self._head.closed = True

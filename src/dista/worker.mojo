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
With one slot (`DISTA_WORKER_SLOTS`, default 1) running a task blocks the
loop, so fetches from this worker wait until the current task finishes. With
more, tasks run on that many threads (`executors.mojo`) and the loop keeps
serving; it still builds each task's arguments and stores and reports each
result itself, so the store and the connections stay on its thread. Actor
frames always run on the loop, one at a time.

A result here lives until the head says free, once nothing references it.
Inputs fetched from peers are copies, kept only until the tasks waiting on
them have their arguments.
A peer that hangs up or resets is dropped, and what was being fetched
from it is lost: the tasks waiting on it fail with `input lost`.

A worker the head gives an actor builds its state and then runs only that
actor's calls, in the order they come. A kill destroys the state, and the
worker goes back to running tasks. Between calls the head may ask for a
checkpoint: the worker writes the state through the codec to
`DISTA_CHECKPOINT_URI` (see `checkpoint.mojo`), and deletes the checkpoint
before it. An actor restarted elsewhere is restored from the last one.

A worker told to stop (SIGTERM, as Kubernetes sends) drains: it tells the
head, which sends it no more work and has other workers adopt the results it
holds, each fetching its copy from here. It finishes what it was given, and
once the head says stop, it serves fetches a moment longer, for tasks given
its old locations, and exits. `DISTA_DRAIN_SECONDS` (default 25, inside
Kubernetes' default grace period of 30) bounds the whole drain.

If the head goes away, `run` returns and the results here stay. `rejoin`
introduces the worker to a new head and reports what it holds; the tasks it
had queued are dropped, since their drivers resubmit them. An actor it
hosted is destroyed: actors don't survive a head restart yet, though their
checkpoints stay.
"""

from std.ffi import c_int, external_call
from std.os import getenv

from dista.auth import cluster_token
from dista.codec import get_scalar, put_scalar
from dista.conn import Conn
from dista.frame import (
    message,
    KIND_ACTOR_CALL,
    KIND_ACTOR_CREATE,
    KIND_ACTOR_KILL,
    KIND_ACTOR_RESTORE,
    KIND_ADOPT,
    KIND_ADOPTED,
    KIND_CAPACITY,
    KIND_CHECKPOINT,
    KIND_CHECKPOINTED,
    KIND_DRAIN,
    KIND_ERROR,
    KIND_FETCH,
    KIND_FREE,
    KIND_HELLO,
    KIND_HOLDING,
    KIND_PART,
    KIND_RESULT,
    KIND_SPAN,
    KIND_STOP,
    KIND_STORED,
    KIND_TASK,
    KIND_TASK_INPUTS,
    KIND_TASK_MANY,
    GATHER,
    WITH_VALUE,
    MANY,
    ROLE_WORKER,
    MAX_PAYLOAD,
    Frame,
    Parts,
    append_header,
    append_part_header,
    encode_addr,
    fetch_part,
    fetch_payload,
    part_payload_size,
)
from dista.checkpoint import (
    checkpoint_location,
    delete_checkpoint,
    list_checkpoints,
    read_checkpoint,
    write_checkpoint,
)
from dista.client import TaskContext, peer_connect_timeout_ms
from dista.executors import DispatchFn, Executors, Job, worker_slots
from dista.metrics import (
    Histogram,
    answer_scrape,
    metrics_listener,
    write_metric,
    write_outcomes,
)
from dista.head import owner_of
from memory_region import SharedMapping
from dista.signals import take_wakes, wake_on_sigterm
from dista.task import StatePtr, split_parts
from dista.sizing import (
    CAPACITY_ANNOUNCE,
    InFlight,
    Learned,
    Resident,
    Sizing,
    memory_pressure,
    resident_bytes,
    wants_auto,
)
from dista.store import (
    Location,
    node_name,
    worker_token,
    map_shared,
    read_shared,
    remove_file,
    spill_dir,
    store_dir,
    store_limit,
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
comptime SERVE_WINDOW = 1 << 20
"""Bytes of a result queued to a fetcher's socket at a time: past this, the
rest waits in its file or in memory, not in the connection's buffer."""
comptime SERVE_CHUNK = 256 * 1024
"""Bytes of a result added to a fetcher's buffer at once."""
comptime LINGER_NS: UInt64 = 1_000_000_000
"""How long a drained worker keeps serving fetches after the head says stop:
a task sent before its inputs moved may still fetch them from here."""
comptime SAMPLE_MS = 20
"""With `auto`, how often the loop samples memory while tasks run."""

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
    var many: Int
    """A `TaskMany`'s number of results; 0 for any other task."""
    var gather: Bool
    var inputs: List[InputSpec]
    var value: List[UInt8]
    """A `TaskWith`'s value, which goes before the inputs; or empty."""


def parse_task_inputs(payload: Span[UInt8, _]) raises -> TaskInputs:
    """A task-inputs payload, checked: a malformed one raises."""
    if len(payload) < 9:
        raise Error("truncated task-inputs")
    var flags = payload[0]
    var gather = (flags & GATHER) != 0
    var pos = 1
    var many = 0
    if flags & MANY:
        many = Int(get_scalar[DType.uint64](payload, pos))
        if many < 1 or many > 1 << 20:
            raise Error("bad task-inputs result count ", many)
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
    var value = List[UInt8]()
    if flags & WITH_VALUE:
        var size = Int(get_scalar[DType.uint64](payload, pos))
        if size < 0 or size != len(payload) - pos:
            raise Error("bad task-inputs value length ", size)
        value = List[UInt8](payload[pos:])
    elif pos != len(payload):
        raise Error("task-inputs with ", len(payload) - pos, " bytes left over")
    return TaskInputs(many, gather, inputs^, value^)


struct _Input(Movable):
    """One input of a parked task: bytes already here, a node-store file
    mapped where it lies, or an object to fetch."""

    var object_id: UInt64
    var have: Bool
    var bytes: List[UInt8]
    var mapped: Optional[SharedMapping]
    """A result in the node store, read from the mapping as the task's
    arguments are built: never copied in whole before that."""
    var size: Int
    """The mapped result's size: the file may be padded."""

    def __init__(
        out self,
        object_id: UInt64,
        have: Bool,
        var bytes: List[UInt8],
        var mapped: Optional[SharedMapping] = None,
        size: Int = 0,
    ):
        self.object_id = object_id
        self.have = have
        self.bytes = bytes^
        self.mapped = mapped^
        self.size = size

    def here(self) -> Bool:
        """Whether its bytes are in the task or mapped."""
        return self.have or Bool(self.mapped)


struct _Outgoing(Movable):
    """A result being sent to a fetcher, a window at a time: from a mapping
    of its node-store file, or from this worker's memory."""

    var object_id: UInt64
    var size: Int
    var frame: Int
    """The object's bytes per frame: all of them in one result frame, or a
    part frame's."""
    var parts: Bool
    var offset: Int
    """The bytes sent so far."""
    var mapped: Optional[SharedMapping]
    """Its file, mapped; None for one in memory. A mapping outlives the
    file's removal, so a result freed meanwhile still finishes sending."""

    def __init__(
        out self,
        object_id: UInt64,
        size: Int,
        part: Int,
        var mapped: Optional[SharedMapping],
    ):
        self.object_id = object_id
        self.size = size
        self.parts = part > 0 and size > part
        self.frame = part_payload_size(part) if self.parts else size
        self.offset = 0
        self.mapped = mapped^


struct _Parked(Movable):
    var object_id: UInt64
    var task_id: UInt64
    var gather: Bool
    var inputs: List[_Input]
    var value: List[UInt8]
    var arrived: UInt64
    var many: Int
    """A `TaskMany`'s number of results; 0 for any other task."""

    def __init__(
        out self,
        object_id: UInt64,
        task_id: UInt64,
        gather: Bool,
        var inputs: List[_Input],
        var value: List[UInt8],
        many: Int = 0,
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.many = many
        self.gather = gather
        self.inputs = inputs^
        self.value = value^
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
    var inputs: Optional[_Parked]
    """A task-inputs task whose inputs are all here: its arguments are built
    from them as it runs, not before, so the tasks queued here don't each
    hold a copy of their inputs while they wait."""

    def __init__(
        out self,
        object_id: UInt64,
        task_id: UInt64,
        var args: List[UInt8],
        arrived: UInt64 = 0,
        kind: UInt8 = KIND_TASK,
        var inputs: Optional[_Parked] = None,
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.args = args^
        self.arrived = arrived if arrived != 0 else wall_ns()
        self.kind = kind
        self.inputs = inputs^


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
    var _sends: List[List[_Outgoing]]
    """For each of those, the results it fetched still being sent, in
    order."""
    var _copied: Int
    """Bytes of results this worker copied in memory: into fetchers'
    buffers, into tasks' arguments, and adopted."""
    var _peers: List[Conn]
    """Connections to workers this one fetches from."""
    var _peer_index: Dict[String, Int]
    var _objects: Dict[UInt64, List[UInt8]]
    """Results stored here, and inputs fetched from peers."""
    var _lost: Dict[UInt64, String]
    var _connect_ms: Int
    """`DISTA_PEER_CONNECT_TIMEOUT`: how long a connect to a peer may take."""
    var _requested: Dict[UInt64, String]
    """Objects being fetched from peers, each with the peer's `ip:port`: if
    that peer goes away, they're lost."""
    var _parts: Dict[UInt64, Parts]
    """Objects arriving from peers in parts, so far."""
    var _parked: List[_Parked]
    var _ready: List[_Ready]
    var _ip: Array[UInt8, 4]
    var _port: Int
    var _node: String
    var _dir: String
    """The node store's directory, or "" when results stay in memory."""
    var _files: Dict[UInt64, Location]
    """Results this worker wrote into the node store, or spilled to disk."""
    var _spill: String
    """Where results go that don't fit the store (`DISTA_SPILL_DIR`)."""
    var _store_limit: Int
    """`DISTA_STORE_BYTES`: the store bytes this worker may hold; 0, any."""
    var _store_used: Int
    """Bytes of this worker's results in the store, spilled ones aside."""
    var _spilled: Int
    """Results spilled to disk, for the metrics."""
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
    var _metrics: Optional[Listener]
    """Where `/metrics` is served, with `DISTA_METRICS_PORT` set."""
    var _ran: Int
    """Tasks and actor frames finished here, failed ones included."""
    var _failed: Int
    var _durations: Histogram
    var _wake_fd: c_int
    """The pipe SIGTERM writes to (see `signals.mojo`), or -1."""
    var _draining: Bool
    var _deadline: UInt64
    """When a drain gives up and the worker leaves anyway."""
    var _leave_at: UInt64
    """Once the head lets a draining worker go: when it exits; else 0."""
    var _adopting: Dict[UInt64, Bool]
    """Results the head asked this worker to adopt, being fetched."""
    var _adopted: Int
    var stopped: Bool
    """The head said stop, or the worker drained: `run` returned for good."""
    var _slots: Int
    """`DISTA_WORKER_SLOTS`: tasks run at once. With 1, on the loop's thread,
    as before slots; with more, on `_pool`'s threads."""
    var _pool: Optional[Executors[Self.dispatch]]
    var _sizing: Optional[Sizing]
    """With `DISTA_WORKER_SLOTS=auto`, the slots and memory budget from the
    run's grant (`sizing.mojo`)."""
    var _resident: Resident
    var _head_reads_capacity: Bool
    """The head announced it reads `KIND_CAPACITY`."""
    var _admit: Int
    """The room last told the head; 0 before it's told any."""
    var _held: Int
    """Times a task waited here for memory, not for a slot."""
    var _inflight: Optional[InFlight]
    """With `auto` and slots, how many tasks to keep in flight."""
    var _learned: Learned
    var _holding: Bool
    """A task is waiting for memory now: `_held` counts each wait once."""
    var _epoch: Int
    """Head connections so far: a task started under an earlier one is
    dropped when it ends, since that head's gone and its driver resubmits
    it."""

    def __init__(
        out self,
        var head: Stream,
        registry_hash: UInt64,
        head_addr: String = "",
        resume_at: String = "",
        sharing: Int = 1,
    ) raises:
        """Starts the object server and says hello to the head on `head`.

        `head_addr` is where a task's client connects, and `resume_at` where
        it reconnects after a head restart ("" in local mode). `sharing`
        workers share the run's grant (`DISTA_WORKER_SLOTS=auto`)."""
        var listener = Listener(0, any_interface=True)
        var advertise = getenv("DISTA_ADVERTISE")
        var ip = resolve_ipv4(advertise) if advertise != "" else head.local_ip()
        self._key = addr_key(ip, listener.port)
        self._slots = worker_slots(sharing)
        self._head = Conn(head^)
        self._head.send(
            KIND_HELLO,
            0,
            0,
            Self._hello(registry_hash, ip, listener.port, self._slots),
        )
        self._listener = listener^
        self._serving = List[Conn]()
        self._sends = List[List[_Outgoing]]()
        self._copied = 0
        self._peers = List[Conn]()
        self._peer_index = Dict[String, Int]()
        self._objects = Dict[UInt64, List[UInt8]]()
        self._lost = Dict[UInt64, String]()
        self._requested = Dict[UInt64, String]()
        self._connect_ms = peer_connect_timeout_ms()
        self._parts = Dict[UInt64, Parts]()
        self._parked = List[_Parked]()
        self._ready = List[_Ready]()
        self._ip = ip.copy()
        self._port = self._listener.port
        self._node = node_name()
        self._dir = store_dir()
        self._files = Dict[UInt64, Location]()
        self._spill = spill_dir()
        self._store_limit = store_limit()
        self._sizing = None
        if wants_auto():
            var sizing = Sizing.from_env(sharing)
            if getenv("DISTA_STORE_BYTES") == "":
                self._store_limit = sizing.store_limit()
            self._sizing = sizing^
        self._resident = Resident()
        self._head_reads_capacity = False
        self._admit = 0
        self._held = 0
        self._holding = False
        self._store_used = 0
        self._spilled = 0
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
        self._metrics = None
        try:
            self._metrics = metrics_listener()
        except e:
            print("dista worker: no metrics:", e)
        self._ran = 0
        self._failed = 0
        self._durations = Histogram()
        self._wake_fd = wake_on_sigterm()
        self._draining = False
        self._deadline = 0
        self._leave_at = 0
        self._adopting = Dict[UInt64, Bool]()
        self._adopted = 0
        self.stopped = False
        self._epoch = 0
        self._pool = None
        if self._slots > 1:
            self._pool = Executors[Self.dispatch](
                self._slots, head_addr, resume_at, registry_hash
            )
        self._inflight = None
        self._learned = Learned()
        if self._sizing:
            # The worker's own memory, its executors started: what the
            # budget holds besides its tasks.
            self._sizing.value().base = resident_bytes()
            if self._pool:
                self._inflight = InFlight(self._slots)

    @staticmethod
    def _hello(
        registry_hash: UInt64, ip: Array[UInt8, 4], port: Int, slots: Int
    ) -> List[UInt8]:
        var hello: List[UInt8] = [ROLE_WORKER]
        put_scalar[DType.uint64](hello, registry_hash)
        hello.extend(encode_addr(ip, port))
        # Its slots, when not 1: a hello without them is a worker's from
        # before slots, which a head before them requires.
        if slots != 1:
            put_scalar[DType.uint64](hello, UInt64(slots))
        return hello^

    def rejoin(mut self, var head: Stream, registry_hash: UInt64) raises:
        """Says hello to a new head and reports the results held here."""
        self._head = Conn(head^)
        self._epoch += 1  # what's running here now is the old head's
        self._head_reads_capacity = False  # until this head says so
        self._admit = 0
        self._end_actor()
        self._ready.clear()
        self._parked.clear()
        self._requested.clear()
        self._parts.clear()
        for entry in self._fetched.items():
            _ = self._objects.pop(entry.key, List[UInt8]())
        self._fetched.clear()
        self._head.send(
            KIND_HELLO,
            0,
            0,
            Self._hello(registry_hash, self._ip, self._port, self._slots),
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
        """Serves until the head hangs up, or until a drain ends."""
        while True:
            var timeout = -1
            if self._can_start():
                timeout = 0
            elif self._draining:
                timeout = 100  # to notice the deadline, or the linger's end
            if (
                self._inflight
                and self._pool.value().running() > 0
                and (timeout < 0 or timeout > SAMPLE_MS)
            ):
                timeout = SAMPLE_MS  # to sample memory as tasks run
            var wait = self._connect_wait_ms()
            if wait >= 0 and (timeout < 0 or wait < timeout):
                timeout = wait
            self._poll_once(timeout_ms=timeout)
            if self._draining:
                var now = wall_ns()
                if self._leave_at != 0 and now >= self._leave_at:
                    print("dista worker: drained")
                    self.stopped = True
                    return
                if now >= self._deadline:
                    print(
                        "dista worker: DISTA_DRAIN_SECONDS passed; leaving"
                        " undrained"
                    )
                    self.stopped = True
                    return
            if self._head.closed:
                if self._draining:
                    self.stopped = True
                return
            self._unpark()
            if not self._pool:
                if len(self._ready) > 0:
                    self._run_one()
            else:
                self._start_some()
                self._adapt()
                self._tell_room()

    def _poll_once(mut self, timeout_ms: Int) raises:
        var fds: List[c_int] = [self._head.fd(), self._listener.fd]
        var events: List[Int16] = [self._events(self._head), POLLIN]
        for i in range(len(self._serving)):
            fds.append(self._serving[i].fd())
            events.append(self._events(self._serving[i]))
        for i in range(len(self._peers)):
            fds.append(self._peers[i].fd())
            events.append(self._events(self._peers[i]))
        if self._metrics:
            fds.append(self._metrics.value().fd)
            events.append(POLLIN)
        if self._wake_fd >= 0:
            fds.append(self._wake_fd)
            events.append(POLLIN)
        if self._pool:
            fds.append(self._pool.value().wake_fd())
            events.append(POLLIN)
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
                    self._refill(i)
                except:
                    self._serving[i].closed = True
            if self._serving[i].closed:
                dead.append(i)
            k += 1
        for i in range(n_peers):
            if rev[k] != 0:
                try:
                    self._pump(self._peers[i], rev[k])
                    while True:
                        var f = self._peers[i].next_frame()
                        if not f:
                            break
                        self._from_peer(f.take())
                except e:
                    # A reset from a peer that died, a connect that failed,
                    # or a frame it garbled.
                    self._peers[i].closed = True
                    self._peers[i].why = String(e)
            k += 1
        if self._metrics:
            if rev[k] & POLLIN:
                answer_scrape(self._metrics.value(), self._page())
            k += 1
        if self._wake_fd >= 0 and rev[k] & POLLIN:
            if take_wakes(self._wake_fd) > 0:
                if self._draining:
                    # A second SIGTERM: leave now.
                    external_call["_exit", NoneType](c_int(143))
                self._start_drain()
        if self._wake_fd >= 0:
            k += 1
        if self._pool and rev[k] != 0:
            self._reap()
            self._tell_room()
        for j in range(len(dead) - 1, -1, -1):
            _ = self._serving.pop(dead[j])
            _ = self._sends.pop(dead[j])
        # A connect that hasn't finished by its deadline has failed.
        var now = wall_ns()
        for j in range(len(self._peers)):
            ref c = self._peers[j]
            if c.connecting and c.deadline != 0 and now >= c.deadline:
                c.closed = True
                c.why = String(
                    "connect timed out after ", self._connect_ms, " ms"
                )
        # A peer that hung up stays readable at end of stream: polled again,
        # it would return at once, forever. Drop it, and what it owed is lost.
        for j in range(len(self._peers) - 1, -1, -1):
            if self._peers[j].closed:
                self._lose_peer(j)
        if rev[1] & POLLIN:
            var c = Conn(self._listener.accept())
            c.require_token(cluster_token())
            self._serving.append(c^)
            self._sends.append(List[_Outgoing]())

    @staticmethod
    def _events(c: Conn) -> Int16:
        return (POLLIN | POLLOUT) if c.wants_write() else POLLIN

    def _connect_wait_ms(self) -> Int:
        """How long until the first pending peer connect's deadline, in ms;
        -1 if none is pending."""
        var first = UInt64(0)
        for i in range(len(self._peers)):
            ref c = self._peers[i]
            if c.connecting and c.deadline != 0:
                if first == 0 or c.deadline < first:
                    first = c.deadline
        if first == 0:
            return -1
        var now = wall_ns()
        if now >= first:
            return 0
        return Int((first - now) // 1_000_000) + 1

    @staticmethod
    def _pump(mut c: Conn, revents: Int16) raises:
        if c.connecting:
            # Writable, or failed: the connect is done, one way or the
            # other. Queued fetches go out below.
            c.finish_connect()
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
            or f.kind == KIND_TASK_MANY
        ):
            var obj = f.object_id
            var task = f.task_id
            var kind = f.kind
            self._ready.append(_Ready(obj, task, f^.take_payload(), kind=kind))
        elif f.kind == KIND_TASK_INPUTS:
            var spec = parse_task_inputs(f.payload)
            var gather = spec.gather
            var many = spec.many
            var inputs = List[_Input](capacity=len(spec.inputs))
            for ref inp in spec.inputs:
                var obj = inp.object_id
                if inp.inline:
                    inputs.append(_Input(obj, True, inp.bytes.copy()))
                else:
                    var loc = inp.location.value().copy()
                    if loc.is_local() and obj not in self._objects:
                        # Mapped now, read when the task's arguments are
                        # built: copied once, not twice.
                        try:
                            inputs.append(
                                _Input(
                                    obj,
                                    False,
                                    List[UInt8](),
                                    map_shared(loc.path, loc.size),
                                    loc.size,
                                )
                            )
                            continue
                        except e:
                            self._lost[obj] = String(e)
                    else:
                        self._request(obj, loc.ip, loc.port)
                    inputs.append(_Input(obj, False, List[UInt8]()))
            self._parked.append(
                _Parked(
                    f.object_id,
                    f.task_id,
                    gather,
                    inputs^,
                    spec.value.copy(),
                    many,
                )
            )
        elif f.kind == KIND_STOP:
            if self._draining:
                # Drained: serve late fetches a moment, then go.
                if self._leave_at == 0:
                    self._leave_at = wall_ns() + LINGER_NS
            else:
                # `dista stop`: leave for good, instead of waiting to rejoin.
                self.stopped = True
                self._head.closed = True
        elif f.kind == KIND_DRAIN:
            # `dista drain`: the same drain as SIGTERM's.
            if not self._draining:
                self._start_drain()
        elif f.kind == KIND_ADOPT:
            var pos = 0
            var loc = Location.decode(f.payload, pos)
            self._adopt(f.object_id, loc)
        elif f.kind == KIND_FREE and f.object_id == CAPACITY_ANNOUNCE:
            # Object 0 names nothing: the head reads `KIND_CAPACITY`.
            self._head_reads_capacity = True
            self._tell_room()
        elif f.kind == KIND_FREE:
            _ = self._objects.pop(f.object_id, List[UInt8]())
            if f.object_id in self._files:
                self._remove(self._files.pop(f.object_id))
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
        if key not in self._peer_index:
            # Connected from the event loop: a peer that never answers
            # (a vanished node, dropped SYNs) costs this worker nothing until
            # the deadline, when what's asked of it is lost.
            try:
                var c = Conn(Stream.start_connect(ip, port))
                c.connecting = True
                c.send_auth()
                if self._connect_ms > 0:
                    c.deadline = (
                        wall_ns() + UInt64(self._connect_ms) * 1_000_000
                    )
                self._peers.append(c^)
            except e:
                self._lost[obj] = String(
                    "cannot reach the worker holding it: ", e
                )
                return
            self._peer_index[key] = len(self._peers) - 1
        self._requested[obj] = key
        var i = self._peer_index.get(key, -1)
        try:
            self._peers[i].send(KIND_FETCH, obj, 0, fetch_payload())
        except:
            # Gone already: the end of this poll drops it.
            self._peers[i].closed = True

    def _lose_peer(mut self, i: Int):
        """Drops the connection to peer `i`, which hung up or failed. What
        was being fetched from it is lost: tasks waiting on it fail with
        `input lost`, and adoptions from it are refused."""
        var key = String()
        for entry in self._peer_index.items():
            if entry.value == i:
                key = entry.key
        var owed = List[UInt64]()
        for entry in self._requested.items():
            if entry.value == key:
                owed.append(entry.key)
        ref c = self._peers[i]
        var why = String("the worker holding it (", key, ") went away")
        if c.connecting:
            why = String(
                "cannot reach the worker holding it (", key, "): ", c.why
            )
        for obj in owed:
            _ = self._requested.pop(obj, String())
            if obj in self._parts:
                try:
                    _ = self._parts.pop(obj)  # the parts that came
                except:
                    pass
            self._lost[obj] = why
            if self._adopting.pop(obj, False):
                self._refuse(obj, why)
        _ = self._peers.pop(i)
        var index = Dict[String, Int]()
        for entry in self._peer_index.items():
            if entry.value != i:
                index[entry.key] = entry.value - (1 if entry.value > i else 0)
        self._peer_index = index^

    def _from_peer(mut self, var f: Frame) raises:
        if f.kind == KIND_PART:
            # Parts of a result too large for one frame: whole at the last.
            var obj = f.object_id
            if obj not in self._parts:
                self._parts[obj] = Parts()
            if not self._parts[obj].add(f.payload):
                return
            var whole = self._parts.pop(obj).take()
            f = Frame(KIND_RESULT, obj, 0, whole^)
        _ = self._requested.pop(f.object_id, String())
        var obj = f.object_id
        var adopting = self._adopting.pop(obj, False)
        if f.kind == KIND_RESULT:
            self._fetched[obj] = True
            self._objects[obj] = f^.take_payload()
            if adopting:
                self._keep(obj, self._objects[obj].copy())
        else:
            self._lost[obj] = message(f.payload)
            if adopting:
                var why = self._lost[obj]
                self._refuse(obj, why)

    def _start_drain(mut self):
        """SIGTERM, or the head passing on `dista drain`: ask the head for no
        more work, and to move what's here."""
        var secs = 25
        try:
            secs = Int(getenv("DISTA_DRAIN_SECONDS", "25"))
        except:
            pass
        self._draining = True
        self._deadline = wall_ns() + UInt64(max(secs, 0)) * 1_000_000_000
        print("dista worker: draining")
        self._to_head(KIND_DRAIN, 0, List[UInt8]())

    def _adopt(mut self, obj: UInt64, loc: Location) raises:
        """Takes `obj` from a draining worker at `loc`, as this one's own."""
        if obj in self._files or (
            obj in self._objects and obj not in self._fetched
        ):
            self._refuse(obj, String("object ", obj, " is already here"))
            return
        if obj in self._objects:
            # Fetched already, as a task's input.
            self._keep(obj, self._objects[obj].copy())
            return
        if loc.is_local():
            try:
                self._keep(obj, read_shared(loc.path, loc.size))
            except e:
                self._refuse(obj, String(e))
            return
        self._adopting[obj] = True
        if obj in self._requested:
            return  # on its way, for a task: `_from_peer` keeps it too
        self._request(obj, loc.ip, loc.port)
        if obj not in self._requested:
            _ = self._adopting.pop(obj, False)
            self._refuse(obj, self._lost.pop(obj, String("unreachable")))

    def _keep(mut self, obj: UInt64, var data: List[UInt8]):
        """Stores an adopted result as if this worker had made it, and tells
        the head where it is now."""
        var path = self._write(obj, data)
        var loc = Location(self._ip, self._port, self._node, path, len(data))
        if path != "":
            self._files[obj] = loc.copy()
        else:
            self._objects[obj] = data^
            _ = self._fetched.pop(obj, False)
        self._adopted += 1
        var body: List[UInt8] = [1]
        body.extend(loc.encode())
        self._to_head(KIND_ADOPTED, obj, body)

    def _refuse(mut self, obj: UInt64, why: String):
        """Tells the head this worker couldn't adopt `obj`."""
        print("dista worker: couldn't adopt", obj, ":", why)
        var body: List[UInt8] = [0]
        body.extend(why.as_bytes())
        self._to_head(KIND_ADOPTED, obj, body)

    def _serve_fetch(mut self, i: Int, var f: Frame) raises:
        """Starts sending a fetched result: a window at a time from where it
        lies (see `_refill`), not copied whole into the connection."""
        if f.kind != KIND_FETCH:
            raise Error("expected a fetch, got frame kind ", Int(f.kind))
        var part = fetch_part(f.payload)
        var obj = f.object_id
        var size = -1
        var mapped: Optional[SharedMapping] = None
        if obj in self._objects:
            size = len(self._objects[obj])
        elif obj in self._files:
            ref loc = self._files[obj]
            try:
                mapped = map_shared(loc.path, loc.size)
                size = loc.size
            except e:
                var msg = String("object ", obj, " can't be read here: ", e)
                self._serving[i].send(KIND_ERROR, obj, 0, msg.as_bytes())
                return
        if size < 0:
            var msg = String("object ", obj, " is not on this worker")
            self._serving[i].send(KIND_ERROR, obj, 0, msg.as_bytes())
        elif not (part > 0 and size > part) and size > MAX_PAYLOAD:
            var msg = String(
                "object ",
                obj,
                " is ",
                size,
                " bytes, too large for one frame, and its fetcher doesn't",
                " take parts",
            )
            self._serving[i].send(KIND_ERROR, obj, 0, msg.as_bytes())
        else:
            self._sends[i].append(_Outgoing(obj, size, part, mapped^))
        self._refill(i)

    def _refill(mut self, i: Int) raises:
        """Tops fetcher `i`'s buffer up to `SERVE_WINDOW` from the results
        it's owed, in order, and sends what its socket takes. N fetchers of
        one result hold N windows, not N copies of it."""
        ref c = self._serving[i]
        ref owed = self._sends[i]
        while len(owed) > 0 and c.unsent() < SERVE_WINDOW:
            ref o = owed[0]
            var obj = o.object_id
            var in_frame = o.offset % o.frame if o.frame > 0 else 0
            if in_frame == 0:
                var header = List[UInt8](capacity=64)
                var n = min(o.frame, o.size - o.offset)
                if o.parts:
                    append_part_header(header, obj, o.size, o.offset, n)
                else:
                    append_header(header, KIND_RESULT, obj, 0, o.size)
                c.queue(header)
            var end = min(o.offset - in_frame + o.frame, o.size)
            var n = min(SERVE_CHUNK, end - o.offset)
            if o.mapped:
                c.queue(o.mapped.value().bytes()[o.offset : o.offset + n])
            elif obj in self._objects:
                c.queue(Span(self._objects[obj])[o.offset : o.offset + n])
            else:
                # Freed mid-send: the frame can't be finished. The fetcher
                # sees the connection close, and the result as lost.
                c.closed = True
                return
            o.offset += n
            self._copied += n
            if o.offset >= o.size:
                _ = owed.pop(0)
            c.flush()

    def _unpark(mut self) raises:
        if len(self._parked) == 0:
            return
        var parked = self._parked^
        self._parked = List[_Parked]()
        for i in range(len(parked)):
            ref p = parked[i]
            var failed = String()
            var complete = True
            for _k in range(len(p.inputs)):
                ref inp = p.inputs[_k]
                if inp.here():
                    continue
                if inp.object_id in self._lost:
                    failed = self._lost.get(inp.object_id, String())
                elif inp.object_id not in self._objects:
                    complete = False
            if failed:
                var first = p.object_id
                var parts = max(p.many, 1)
                for k in range(parts):
                    self._report_error(
                        first + UInt64(k), String("input lost: ", failed)
                    )
                continue
            if not complete:
                var keep = _Parked(0, 0, False, List[_Input](), List[UInt8]())
                swap(keep, p)
                self._parked.append(keep^)
                continue
            var kind = KIND_TASK_MANY if p.many > 0 else KIND_TASK
            var whole = _Parked(0, 0, False, List[_Input](), List[UInt8]())
            swap(whole, p)
            var obj = whole.object_id
            var task = whole.task_id
            var arrived = whole.arrived
            self._ready.append(
                _Ready(obj, task, List[UInt8](), arrived, kind, whole^)
            )
        self._drop_fetched()

    def _drop_fetched(mut self):
        """Drops the fetched copies no task here still needs."""
        var done = List[UInt64]()
        for entry in self._fetched.items():
            if not self._wanted(entry.key):
                done.append(entry.key)
        for obj in done:
            _ = self._fetched.pop(obj, False)
            _ = self._objects.pop(obj, List[UInt8]())

    def _assemble(mut self, var p: _Parked) raises -> List[UInt8]:
        """A task-inputs task's arguments, from its inputs: each copied once,
        from where it lies, or the one input fetched for it alone moved."""
        if self._debug:
            print("dista worker: task", p.object_id, "inputs:", end="")
            for _k in range(len(p.inputs)):
                ref inp = p.inputs[_k]
                var n = len(inp.bytes)
                var how = "inline"
                if inp.mapped:
                    n = inp.size
                    how = "mapped"
                elif not inp.have:
                    n = len(self._objects[inp.object_id])
                    how = "fetched"
                print(" ", inp.object_id, how, n, end="")
            print()
        var args = List[UInt8]()
        var obj = p.inputs[0].object_id if len(p.inputs) == 1 else 0
        if (
            p.many == 0
            and not p.gather
            and len(p.value) == 0
            and len(p.inputs) == 1
            and not p.inputs[0].here()
            and obj in self._fetched
            and not self._wanted(obj)
        ):
            # The task's one input, fetched for it alone: its arguments.
            _ = self._fetched.pop(obj, False)
            return self._objects.pop(obj)
        var size = len(p.value) + 16
        for _k in range(len(p.inputs)):
            ref inp = p.inputs[_k]
            if inp.have:
                size += len(inp.bytes)
            elif inp.mapped:
                size += inp.size
            else:
                size += len(self._objects[inp.object_id])
        args.reserve(size)
        # A `TaskMany` runs on its number of results, then the input.
        if p.many > 0:
            put_scalar[DType.uint64](args, UInt64(p.many))
        args.extend(Span(p.value))
        if p.gather:
            put_scalar[DType.int64](args, Int64(len(p.inputs)))
        for _k in range(len(p.inputs)):
            ref inp = p.inputs[_k]
            if inp.have:
                args.extend(Span(inp.bytes))
            elif inp.mapped:
                args.extend(inp.mapped.value().bytes()[: inp.size])
                self._copied += inp.size
            else:
                ref data = self._objects[inp.object_id]
                args.extend(Span(data))
                self._copied += len(data)
        p.inputs.clear()  # its mappings, unmapped
        self._drop_fetched()
        return args^

    def _wanted(self, obj: UInt64) -> Bool:
        """Whether a task parked or queued here takes `obj`."""
        for _q in range(len(self._parked)):
            ref p = self._parked[_q]
            for _k in range(len(p.inputs)):
                if p.inputs[_k].object_id == obj:
                    return True
        for _q in range(len(self._ready)):
            if not self._ready[_q].inputs:
                continue
            ref p = self._ready[_q].inputs.value()
            for _k in range(len(p.inputs)):
                if p.inputs[_k].object_id == obj:
                    return True
        return False

    def _page(mut self) -> String:
        """This worker's `/metrics` page."""
        var held = 0
        for entry in self._files.items():
            held += entry.value.size
        for entry in self._objects.items():
            if entry.key not in self._fetched:
                held += len(entry.value)
        var out = String()
        write_outcomes(
            out,
            "dista_worker_tasks_finished_total",
            "Tasks and actor calls this worker finished, by outcome.",
            self._ran - self._failed,
            self._failed,
        )
        self._durations.write(
            out,
            "dista_worker_task_duration_seconds",
            "How long tasks and actor calls ran here.",
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_results",
            "Large results this worker holds.",
            len(self._files) + len(self._objects) - len(self._fetched),
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_result_bytes",
            "Bytes of the large results this worker holds.",
            held,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_store_bytes",
            "Bytes of this worker's results in the node store.",
            self._store_used,
        )
        write_metric(
            out,
            "counter",
            "dista_worker_results_spilled_total",
            (
                "Large results written to DISTA_SPILL_DIR because the store was"
                " full."
            ),
            self._spilled,
        )
        var buffered = 0
        for i in range(len(self._serving)):
            buffered += self._serving[i].unsent()
        write_metric(
            out,
            "counter",
            "dista_worker_bytes_copied_total",
            (
                "Bytes of results this worker copied in memory: into fetchers'"
                " buffers, into tasks' arguments, and adopted."
            ),
            self._copied,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_fetch_buffer_bytes",
            "Bytes queued to fetchers that their sockets haven't taken.",
            buffered,
        )
        write_metric(
            out,
            "counter",
            "dista_worker_results_adopted_total",
            "Results this worker took from draining workers.",
            self._adopted,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_draining",
            "1 while this worker drains, after SIGTERM.",
            1 if self._draining else 0,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_queue_depth",
            "Tasks here waiting to run, or for their inputs.",
            len(self._ready) + len(self._parked),
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_slots",
            "Tasks this worker runs at once (DISTA_WORKER_SLOTS).",
            self._slots,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_tasks_running",
            "Tasks running on this worker's slots now.",
            self._pool.value().running() if self._pool else 0,
        )
        var budget = self._sizing.value().budget if self._sizing else 0
        write_metric(
            out,
            "gauge",
            "dista_worker_admit",
            (
                "Tasks this worker has memory to run now, out of its slots"
                " (DISTA_WORKER_SLOTS=auto)."
            ),
            self._room(),
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_memory_budget_bytes",
            (
                "The resident memory this worker keeps to, from the run's"
                " grant; 0, none."
            ),
            budget,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_resident_bytes",
            "This worker's resident memory.",
            self._resident.now() if self._sizing else 0,
        )
        var limit = self._slots
        var changes = 0
        if self._inflight:
            limit = self._inflight.value().limit
            changes = self._inflight.value().changes
        write_metric(
            out,
            "gauge",
            "dista_worker_inflight_limit",
            (
                "Tasks this worker keeps in flight, adapted to throughput and"
                " memory (DISTA_WORKER_SLOTS=auto)."
            ),
            limit,
        )
        write_metric(
            out,
            "counter",
            "dista_worker_inflight_changes_total",
            "Times the tasks kept in flight changed; each is logged.",
            changes,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_task_memory_estimate_bytes",
            "The memory a task is taken to need, given or learned.",
            self._sizing.value().estimate(
                self._resident.now()
            ) if self._sizing else 0,
        )
        write_metric(
            out,
            "counter",
            "dista_worker_memory_holds_total",
            "Times a task waited to start for memory, not for a slot.",
            self._held,
        )
        write_metric(
            out,
            "gauge",
            "dista_worker_hosts_actor",
            "1 while this worker hosts an actor.",
            1 if self._actor != 0 else 0,
        )
        return out^

    def _report_error(mut self, obj: UInt64, msg: String):
        self._failed += 1
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

    def _prepare(mut self, mut t: _Ready) raises -> Bool:
        """Builds a task-inputs task's arguments from its inputs; False, with
        its results failed, if one can't be read."""
        if not t.inputs:
            return True
        var p = t.inputs.take()
        var parts = max(p.many, 1)
        try:
            t.args = self._assemble(p^)
        except e:
            for k in range(parts):
                self._report_error(
                    t.object_id + UInt64(k), String("input lost: ", e)
                )
            return False
        return True

    def _can_start(mut self) -> Bool:
        """Whether the front of `_ready` can start now: with slots, a task
        waits for a free one."""
        if len(self._ready) == 0:
            return False
        if not self._pool:
            return True
        var kind = self._ready[0].kind
        if kind != KIND_TASK and kind != KIND_TASK_MANY:
            return True
        return self._pool.value().free_slot() >= 0 and self._fits()

    def _fits(mut self) -> Bool:
        """Whether memory has room for one more task to start
        (`DISTA_WORKER_SLOTS=auto`); without a budget, always."""
        if not self._sizing or not self._pool:
            return True
        var running = self._pool.value().running()
        var ok = running == 0 or running < self._room()
        if ok:
            self._holding = False
        elif not self._holding:
            self._holding = True
            self._held += 1
        return ok

    def _room(mut self) -> Int:
        """The tasks this worker has room to run now: its slots, or with a
        budget, what fits it."""
        if not self._sizing or not self._pool:
            return self._slots
        var n = self._sizing.value().room(
            self._pool.value().running(), self._resident.now()
        )
        if self._inflight:
            n = min(n, self._inflight.value().limit)
        return n

    def _adapt(mut self):
        """Samples memory for the running tasks and, window by window, sets
        how many tasks to keep in flight (`InFlight`), telling the head when
        that changes."""
        if not self._inflight:
            return
        var now = wall_ns()
        var running = self._pool.value().running()
        var rss = self._resident.now()
        self._learned.sample(rss)
        var old = self._inflight.value().limit
        self._inflight.value().saw(running, now, self._spilled)
        var fit = self._sizing.value().room(running, rss)
        var changed = self._inflight.value().cap(fit)
        if not changed and self._inflight.value().due(now):
            changed = self._inflight.value().close(
                now, self._spilled, memory_pressure(), fit
            )
        if changed:
            print(
                "dista worker: tasks in flight",
                old,
                "->",
                self._inflight.value().limit,
                "(" + self._inflight.value().why + ")",
            )
            self._tell_room()

    def _tell_room(mut self):
        """Tells a head that reads it the worker's room, when it changed."""
        if not self._sizing or not self._head_reads_capacity:
            return
        var n = self._room()
        if n == self._admit:
            return
        self._admit = n
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, UInt64(n))
        self._to_head(KIND_CAPACITY, 0, body)

    def _start_some(mut self) raises:
        """With slots: starts the tasks at the front of `_ready` on the free
        ones, in order. An actor frame runs here, on the loop, as with one
        slot, so an actor's calls still run one at a time, in order; then
        the loop polls again."""
        while len(self._ready) > 0:
            var kind = self._ready[0].kind
            if kind != KIND_TASK and kind != KIND_TASK_MANY:
                var t = self._ready.pop(0)
                if self._prepare(t):
                    self._run_actor(t^)
                return
            var slot = self._pool.value().free_slot()
            if slot < 0 or not self._fits():
                self._tell_room()
                return
            var t = self._ready.pop(0)
            if not self._prepare(t):
                continue
            if kind == KIND_TASK and t.object_id in self._files:
                # As in `_run_one`: a resubmitted task whose result is here.
                self._to_head(
                    KIND_STORED, t.object_id, self._files[t.object_id].encode()
                )
                continue
            var many = 0
            var args = t.args^
            t.args = List[UInt8]()
            if kind == KIND_TASK_MANY:
                if len(args) < 8:
                    self._report_error(t.object_id, "a truncated TaskMany")
                    continue
                var pos = 0
                many = Int(get_scalar[DType.uint64](args, pos))
                var rest = List[UInt8](args[8:])
                args = rest^
            if self._inflight:
                self._learned.started(
                    t.object_id, t.task_id, self._resident.now()
                )
            self._pool.value().start(
                slot,
                Job(
                    t.object_id,
                    t.task_id,
                    kind,
                    t.arrived,
                    args^,
                    many,
                    self._epoch,
                ),
            )
            if self._sizing:
                self._sizing.value().started(self._pool.value().running())

    def _reap(mut self):
        """Stores and reports the tasks the slots finished, as `_run_one`
        and `_run_many` do theirs."""
        var done = self._pool.value().take_done()
        if self._sizing and len(done) > 0:
            self._sizing.value().finished()
        while len(done) > 0:
            var j = done.pop(0)
            if self._inflight:
                self._learned.finished(j.object_id, resident_bytes())
                self._sizing.value().learned = self._learned.most()
                if j.ended > j.started:
                    self._inflight.value().finished(j.ended - j.started)
            if j.epoch != self._epoch:
                continue  # the head it was for is gone
            if j.kind == KIND_TASK_MANY:
                var parts = List[List[UInt8]]()
                var why = j.error.copy()
                if j.ok:
                    try:
                        parts = split_parts(j.out)
                        if len(parts) != j.many:
                            raise Error(
                                "the task returned ",
                                len(parts),
                                " results, not ",
                                j.many,
                            )
                    except e:
                        why = String(e)
                for k in range(j.many - 1, -1, -1):
                    var obj = j.object_id + UInt64(k)
                    if why:
                        self._span(obj, j.arrived, j.started)
                        self._report_error(obj, why)
                    else:
                        self._finish(obj, j.arrived, j.started, parts[k].copy())
            elif j.ok:
                var out = List[UInt8]()
                swap(out, j.out)
                self._finish(j.object_id, j.arrived, j.started, out^)
            else:
                self._span(j.object_id, j.arrived, j.started)
                self._report_error(j.object_id, j.error)

    def _run_one(mut self) raises:
        var t = self._ready.pop(0)
        if not self._prepare(t):
            return
        if t.kind == KIND_TASK_MANY:
            self._run_many(t^)
            return
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

    def _run_many(mut self, var t: _Ready):
        """Runs a `TaskMany` and answers each of its results on its own: the
        last first, so the head sees the task in flight until all are."""
        var first = t.object_id
        var started = wall_ns()
        var n = 0
        var parts = List[List[UInt8]]()
        var why = String()
        try:
            var pos = 0
            n = Int(get_scalar[DType.uint64](t.args, pos))
            var out = List[UInt8]()
            Self.dispatch(t.task_id, List[UInt8](t.args[8:]), out, self._ctx)
            parts = split_parts(out)
            if len(parts) != n:
                raise Error(
                    "the task returned ", len(parts), " results, not ", n
                )
        except e:
            why = String(e)
        for k in range(n - 1, -1, -1):
            var obj = first + UInt64(k)
            if why:
                self._span(obj, t.arrived, started)
                self._report_error(obj, why)
            else:
                self._finish(obj, t.arrived, started, parts[k].copy())

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
            var path = self._write(obj, out)
            var loc = Location(self._ip, self._port, self._node, path, len(out))
            if path != "":
                self._files[obj] = loc.copy()
            else:
                self._objects[obj] = out^
            self._span(obj, arrived, started)  # storing included
            self._to_head(KIND_STORED, obj, loc.encode())

    def _write(mut self, obj: UInt64, data: Span[UInt8, _]) -> String:
        """Writes a large result into the node store, or past
        `DISTA_STORE_BYTES` or a failed write, spills it to disk; its path,
        or "" to keep it in memory, where the store is off or both fail."""
        if self._dir == "":
            return String()
        var name = String("/dista-", hex(self._token), "-", obj)
        var n = len(data)
        if self._store_limit == 0 or self._store_used + n <= self._store_limit:
            var path = String(self._dir, name)
            try:
                write_shared(path, data)
                self._store_used += n
                return path
            except e:
                print("dista worker: node store write failed, spilling:", e)
        if self._spill != "" and self._spill != self._dir:
            var path = String(self._spill, name)
            try:
                write_shared(path, data)
                self._spilled += 1
                return path
            except e:
                print("dista worker: spilling failed, keeping it in memory:", e)
        return String()

    def _remove(mut self, loc: Location):
        """Deletes a result's file, and counts it out of the store."""
        if loc.path.startswith(String(self._dir, "/")):
            self._store_used -= loc.size
        remove_file(loc.path)

    def _span(mut self, obj: UInt64, arrived: UInt64, started: UInt64):
        """Tells the head when `obj`'s task arrived, started and ended here."""
        var ended = wall_ns()
        self._ran += 1
        if ended > started:
            self._durations.observe(Float64(ended - started) / 1e9)
        var body = List[UInt8]()
        put_scalar[DType.uint64](body, arrived)
        put_scalar[DType.uint64](body, started)
        put_scalar[DType.uint64](body, ended)
        self._to_head(KIND_SPAN, obj, body)

    def _to_head(mut self, kind: UInt8, obj: UInt64, payload: Span[UInt8, _]):
        """Sends to the head; a head that's gone is noticed by `run`."""
        try:
            self._head.send(kind, obj, 0, payload)
        except:
            self._head.closed = True

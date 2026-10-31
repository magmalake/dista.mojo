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
"""

from std.ffi import c_int

from dista.codec import get_scalar, put_scalar
from dista.conn import Conn
from dista.frame import (
    ADDR_SIZE,
    KIND_ERROR,
    KIND_HELLO,
    KIND_LOCATED,
    KIND_RESULT,
    KIND_STORED,
    KIND_TASK,
    KIND_TASK_AFTER,
    KIND_TASK_INPUTS,
    KIND_WELCOME,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
)
from dista.socket import POLLERR, POLLHUP, POLLIN, POLLOUT, Listener, poll

comptime DRIVER_SHIFT = 40
"""Object IDs carry their driver's number above this bit."""
comptime _ROLE_NONE: UInt8 = 0
comptime _ROLE_GONE: UInt8 = 255


def owner_of(object_id: UInt64) -> UInt64:
    """The number of the driver that created `object_id`."""
    return object_id >> DRIVER_SHIFT


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

    def _handle(mut self, i: Int, var f: Frame) raises:
        var role = self._roles[i]
        if role == _ROLE_NONE:
            self._hello(i, f^)
        elif role == ROLE_DRIVER:
            if f.kind == KIND_TASK:
                var obj = f.object_id
                var task = f.task_id
                self._queue.append(_Task(obj, task, f^.take_payload()))
            elif f.kind == KIND_TASK_AFTER:
                var gather = f.payload[0] != 0
                var pos = 1
                var n = Int(get_scalar[DType.uint64](f.payload, pos))
                var deps = List[UInt64](capacity=n)
                for _ in range(n):
                    deps.append(get_scalar[DType.uint64](f.payload, pos))
                self._waiting.append(
                    _Task(
                        f.object_id,
                        f.task_id,
                        List[UInt8](),
                        deps=deps^,
                        gather=gather,
                    )
                )
            else:
                raise Error("driver sent frame kind ", Int(f.kind))
        elif role == ROLE_WORKER:
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
            if f.kind == KIND_RESULT:
                self._results[f.object_id] = f.payload.copy()
                self._deliver(f.object_id, f.kind, f.payload)
            elif f.kind == KIND_STORED:
                self._location[f.object_id] = i
                var addr = self._worker_addr[i].copy()
                self._deliver(f.object_id, KIND_LOCATED, addr)
            else:
                self._errors[f.object_id] = String(unsafe_from_utf8=f.payload)
                self._deliver(f.object_id, f.kind, f.payload)

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
        self._errors[object_id] = msg.copy()
        self._deliver(object_id, KIND_ERROR, msg.as_bytes())

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
        elif role == ROLE_WORKER:
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
            else:
                # Some inputs live on workers: tell the worker that runs this
                # task where to fetch them, and pass the rest inline.
                var spec: List[UInt8] = [UInt8(1) if t.gather else UInt8(0)]
                put_scalar[DType.uint64](spec, UInt64(len(t.deps)))
                for d in t.deps:
                    if d in self._location:
                        spec.append(1)
                        put_scalar[DType.uint64](spec, d)
                        spec.extend(
                            self._worker_addr[self._location.get(d, 0)].copy()
                        )
                    else:
                        var r = self._results.get(d, List[UInt8]())
                        spec.append(0)
                        put_scalar[DType.uint64](spec, d)
                        put_scalar[DType.uint64](spec, UInt64(len(r)))
                        spec.extend(r^)
                self._queue.append(
                    _Task(t.object_id, t.task_id, spec^, kind=KIND_TASK_INPUTS)
                )
        self._waiting = still^

    def _queued(self) -> Int:
        return len(self._queue) - self._queue_head

    def _dispatch(mut self):
        """Hands queued tasks to workers, one round per pipeline slot."""
        for depth in range(self._pipeline):
            for i in range(len(self._conns)):
                if self._queued() == 0:
                    break
                if self._roles[i] != ROLE_WORKER:
                    continue
                if len(self._inflight[i]) != depth:
                    continue
                ref t = self._queue[self._queue_head]
                self._queue_head += 1
                self._inflight[i].append(t.object_id)
                try:
                    self._conns[i].send(t.kind, t.object_id, t.task_id, t.args)
                except e:
                    print("dista head: lost worker:", e)
                    self._drop(i)
        if self._queue_head == len(self._queue):
            self._queue.clear()
            self._queue_head = 0

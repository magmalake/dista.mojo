"""The phase-1 runtime: one driver and its forked workers on one machine.

```mojo
comptime Square = Task[Int, Int, square]

def main() raises:
    var rt = Runtime[Square](workers=4)
    var r = rt.remote[Square](7)
    print(rt.get(r))  # 49
```

`Runtime(...)` binds a loopback listener, forks the workers and waits for each
to connect back. Forking happens before any thread exists, and gives each
worker the registry without an exec: the registry is the runtime's parameter
list, so a worker dispatches with a `comptime for` over it.

Scheduling is a FIFO queue. `remote` only enqueues; `get` pumps. A pump hands
queued tasks to idle workers, then reads the result of the oldest task in
flight. Workers compute in parallel while the driver waits on one of them,
because finished results wait in their socket buffers. The driver writes only
to idle workers, so neither side can block the other with a full buffer.
"""

from dista.codec import decode, encode
from dista.frame import (
    KIND_ERROR,
    KIND_RESULT,
    KIND_TASK,
    read_frame,
    write_frame,
)
from dista.socket import Listener, Stream, exit_now, fork, wait_child
from dista.task import TaskLike


struct ObjectRef[T: AnyType](ImplicitlyCopyable, Movable, Writable):
    """A handle to a task's result, which may not exist yet."""

    var id: UInt64

    def __init__(out self, id: UInt64):
        self.id = id

    def write_to(self, mut writer: Some[Writer]):
        writer.write("ObjectRef(", self.id, ")")


struct _Pending(Movable):
    var object_id: UInt64
    var task_id: UInt64
    var args: List[UInt8]

    def __init__(
        out self, object_id: UInt64, task_id: UInt64, var args: List[UInt8]
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.args = args^


struct Runtime[*Ts: TaskLike](Movable):
    """A driver with `workers` forked worker processes, serving tasks `Ts`."""

    var _workers: List[Stream]
    var _pids: List[Int]
    var _busy: List[Bool]
    var _queue: List[_Pending]
    var _queue_head: Int
    var _inflight: List[Int]
    var _results: Dict[UInt64, List[UInt8]]
    var _errors: Dict[UInt64, String]
    var _next_id: UInt64

    def __init__(out self, workers: Int = 4) raises:
        """Starts `workers` worker processes and connects to each."""
        if workers < 1:
            raise Error("need at least one worker")
        Self._check_registry()
        var listener = Listener()
        var pids = List[Int]()
        for _ in range(workers):
            var pid = fork()
            if pid == 0:
                listener.close()
                var code = 0
                try:
                    Self._serve(listener.port)
                except e:
                    print("dista worker:", e)
                    code = 1
                exit_now(code)
            pids.append(pid)
        var streams = List[Stream]()
        for _ in range(workers):
            streams.append(listener.accept())
        self._workers = streams^
        self._pids = pids^
        self._busy = List[Bool](length=workers, fill=False)
        self._queue = List[_Pending]()
        self._queue_head = 0
        self._inflight = List[Int]()
        self._results = Dict[UInt64, List[UInt8]]()
        self._errors = Dict[UInt64, String]()
        self._next_id = 1

    def __deinit__(deinit self):
        # Closing a worker's socket is its signal to exit.
        for i in range(len(self._workers)):
            self._workers[i].close()
        for pid in self._pids:
            _ = wait_child(pid)

    def num_workers(self) -> Int:
        return len(self._workers)

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
        task_id: UInt64, args: Span[UInt8, _], mut out: List[UInt8]
    ) raises:
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == task_id:
                Self.Ts[i].run(args, out)
                return
        raise Error("unknown task ID ", task_id)

    @staticmethod
    def _serve(port: Int) raises:
        """A worker's loop: run tasks until the driver hangs up."""
        var s = Stream.connect(port)
        while True:
            var f = read_frame(s)
            if not f:
                return
            ref frame = f.value()
            if frame.kind != KIND_TASK:
                raise Error("worker got frame kind ", Int(frame.kind))
            var out = List[UInt8]()
            try:
                Self._dispatch(frame.task_id, frame.payload, out)
            except e:
                write_frame(
                    s, KIND_ERROR, frame.object_id, 0, String(e).as_bytes()
                )
                continue
            write_frame(s, KIND_RESULT, frame.object_id, 0, out)

    def remote[T: TaskLike](mut self, arg: T.A) raises -> ObjectRef[T.R]:
        """Queues task `T` on `arg` and returns a handle to its result."""
        var registered = False
        comptime for i in range(Self.Ts.length):
            if Self.Ts[i].id() == T.id():
                registered = True
        if not registered:
            raise Error(T.name(), " is not registered with this runtime")
        var args = List[UInt8]()
        encode(arg, args)
        var id = self._next_id
        self._next_id += 1
        self._queue.append(_Pending(id, T.id(), args^))
        return ObjectRef[T.R](id)

    def get[R: Movable](mut self, r: ObjectRef[R]) raises -> R:
        """Waits for `r`'s task and returns its result, or raises its error."""
        while r.id not in self._results and r.id not in self._errors:
            self._pump()
        if r.id in self._errors:
            raise Error(self._errors[r.id])
        var pos = 0
        return decode[R](self._results[r.id], pos)

    def _queued(self) -> Int:
        return len(self._queue) - self._queue_head

    def _pump(mut self) raises:
        for w in range(len(self._workers)):
            if self._busy[w] or self._queued() == 0:
                continue
            ref p = self._queue[self._queue_head]
            write_frame(
                self._workers[w], KIND_TASK, p.object_id, p.task_id, p.args
            )
            self._queue_head += 1
            self._busy[w] = True
            self._inflight.append(w)
        if self._queue_head == len(self._queue):
            self._queue.clear()
            self._queue_head = 0
        if len(self._inflight) == 0:
            raise Error("get: no such object in this runtime")
        var w = self._inflight.pop(0)
        var f = read_frame(self._workers[w])
        if not f:
            raise Error("worker ", w, " exited with a task in flight")
        self._busy[w] = False
        ref frame = f.value()
        if frame.kind == KIND_RESULT:
            self._results[frame.object_id] = frame.payload.copy()
        elif frame.kind == KIND_ERROR:
            self._errors[frame.object_id] = String(
                unsafe_from_utf8=frame.payload
            )
        else:
            raise Error("driver got frame kind ", Int(frame.kind))

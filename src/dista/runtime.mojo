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
"""

from std.os import getenv
from std.time import perf_counter_ns, sleep

from dista.codec import decode, encode, get_scalar, put_scalar
from dista.frame import (
    message,
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
    KIND_WELCOME,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
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
)
from dista.task import TaskLike, fnv1a64
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


@fieldwise_init
struct _Submission(Copyable, Movable):
    """A task this driver sent, kept until it settles so it can be resent."""

    var kind: UInt8
    var task_id: UInt64
    var payload: List[UInt8]
    var deps: List[UInt64]


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
        var w = Worker[Self._dispatch_list](head^, Self._registry_hash())
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
        self._head.close()
        self._head = connect_head(self._head_addr)
        var hello = Self._hello_payload(ROLE_DRIVER)
        put_scalar[DType.uint64](hello, self._driver)
        write_frame(self._head, KIND_HELLO, 0, 0, hello)
        var f = read_frame(self._head)
        if not f or f.value().kind != KIND_WELCOME:
            raise Error("the restarted head didn't welcome this driver back")
        # Oldest first, so every task's inputs are back before it is.
        for n in range(1, Int(self._counter) + 1):
            var id = (self._driver << DRIVER_SHIFT) | UInt64(n)
            if id not in self._live:
                continue
            if id in self._results:
                write_frame(self._head, KIND_RESULT, id, 0, self._results[id])
            elif id in self._errors:
                write_frame(
                    self._head, KIND_ERROR, id, 0, self._errors[id].as_bytes()
                )
            elif id in self._located:
                write_frame(self._head, KIND_CLAIM, id, 0, List[UInt8]())
                # Where it was may be stale: a task run twice across restarts
                # leaves copies, and the new head may keep the other one. It
                # says where it has the result in reply.
                _ = self._located.pop(id, List[UInt8]())
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
            if keep:
                self._located[id] = frame.payload.copy()
            self._settle(id)
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

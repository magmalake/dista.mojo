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

If the head goes away, `run` returns and the results here stay. `rejoin`
introduces the worker to a new head and reports what it holds; the tasks it
had queued are dropped, since their drivers resubmit them.
"""

from std.ffi import c_int, external_call
from std.os import getenv

from dista.codec import get_scalar, put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_ERROR,
    KIND_FETCH,
    KIND_FREE,
    KIND_HELLO,
    KIND_HOLDING,
    KIND_RESULT,
    KIND_STORED,
    KIND_TASK,
    KIND_TASK_INPUTS,
    ROLE_WORKER,
    Frame,
    encode_addr,
)
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
    poll,
    resolve_ipv4,
)

comptime INLINE_LIMIT = 64 * 1024
"""Results up to this size travel through the head; larger ones stay put."""

comptime DispatchFn = def(UInt64, List[UInt8], mut List[UInt8]) thin raises
"""Runs task `task_id` on encoded arguments, appending the encoded result."""


def addr_key(ip: Array[UInt8, 4], port: Int) -> String:
    return String(
        Int(ip[0]), ".", Int(ip[1]), ".", Int(ip[2]), ".", Int(ip[3]), ":", port
    )


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


struct _Ready(Movable):
    var object_id: UInt64
    var task_id: UInt64
    var args: List[UInt8]

    def __init__(
        out self, object_id: UInt64, task_id: UInt64, var args: List[UInt8]
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.args = args^


struct Worker[dispatch: DispatchFn](Movable):
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

    def __init__(out self, var head: Stream, registry_hash: UInt64) raises:
        """Starts the object server and says hello to the head on `head`."""
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
        if f.kind == KIND_TASK:
            var obj = f.object_id
            var task = f.task_id
            self._ready.append(_Ready(obj, task, f^.take_payload()))
        elif f.kind == KIND_TASK_INPUTS:
            var gather = f.payload[0] != 0
            var pos = 1
            var n = Int(get_scalar[DType.uint64](f.payload, pos))
            var inputs = List[_Input](capacity=n)
            for _ in range(n):
                var tag = f.payload[pos]
                pos += 1
                var obj = get_scalar[DType.uint64](f.payload, pos)
                if tag == 0:
                    var len = Int(get_scalar[DType.uint64](f.payload, pos))
                    inputs.append(
                        _Input(
                            obj, True, List[UInt8](f.payload[pos : pos + len])
                        )
                    )
                    pos += len
                else:
                    var loc = Location.decode(f.payload, pos)
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
                String(unsafe_from_utf8=f.payload),
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
            self._lost[f.object_id] = String(unsafe_from_utf8=f.payload)

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
                for ref inp in p.inputs:
                    if inp.have:
                        args.extend(inp.bytes.copy())
                    else:
                        args.extend(
                            self._objects.get(inp.object_id, List[UInt8]())
                        )
                self._ready.append(_Ready(p.object_id, p.task_id, args^))
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

    def _run_one(mut self) raises:
        var t = self._ready.pop(0)
        var out = List[UInt8]()
        try:
            Self.dispatch(t.task_id, t.args, out)
        except e:
            self._report_error(t.object_id, String(e))
            return
        if len(out) <= INLINE_LIMIT:
            self._to_head(KIND_RESULT, t.object_id, out)
        else:
            var path = String()
            if self._dir != "":
                path = String(
                    self._dir, "/dista-", hex(self._token), "-", t.object_id
                )
                try:
                    write_shared(path, out)
                except e:
                    print("dista worker: node store write failed:", e)
                    path = String()
            var loc = Location(self._ip, self._port, self._node, path, len(out))
            if path != "":
                self._files[t.object_id] = loc.copy()
            else:
                self._objects[t.object_id] = out^
            self._to_head(KIND_STORED, t.object_id, loc.encode())

    def _to_head(mut self, kind: UInt8, obj: UInt64, payload: Span[UInt8, _]):
        """Sends to the head; a head that's gone is noticed by `run`."""
        try:
            self._head.send(kind, obj, 0, payload)
        except:
            self._head.closed = True

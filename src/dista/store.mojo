"""The node-local object store: large results in shared mappings.

A worker writes each large result into a file in a node-local directory
(`/dev/shm` on Linux, `$TMPDIR` on macOS, or `DISTA_STORE_DIR`) through
memory-region's `MappedRegion`. A reader that can see that file maps it
read-only and reads the bytes where they lie: no socket, no copy through
the kernel's network stack. A reader that can't fetches the result over a
socket from the worker, as before.

"Can see the file" is the test, not a matching host name: file names carry a
random token per worker, so a file with the right name is the right file.
That covers one machine, and Kubernetes pods that share a volume. (Host
names turned out to be unreliable: on GitHub's macOS runners, processes on
one machine reported different ones.)

`DISTA_SHM=0` turns the store off; large results then stay in the worker's
memory and are always fetched over sockets.

A location says where a result is: the worker's object server address, its
host name, the file's path ("" when it's only in memory) and the exact size,
since mapped files are padded.
"""

from std.ffi import c_int, c_size_t, external_call
from std.memory import stack_allocation, unsafe_memcpy
from std.os import getenv
from std.sys.info import CompilationTarget
from std.time import perf_counter_ns

from memory_region import MappedRegion, SharedMapping

from dista.codec import get_scalar, put_scalar
from dista.frame import decode_addr, encode_addr

comptime _F_OK: c_int = 0


def node_name() -> String:
    """This host's name, which identifies a shared `/dev/shm`."""
    var buf = stack_allocation[256, UInt8]()
    for i in range(256):
        buf.unsafe_offset(i).unsafe_write(0)
    if (
        external_call["gethostname", c_int](
            buf.unsafe_bitcast[NoneType](), c_size_t(255)
        )
        != 0
    ):
        return String("unknown-host")
    var n = 0
    while n < 255 and buf.unsafe_offset(n)[] != 0:
        n += 1
    return String(
        unsafe_from_utf8=Span[UInt8, MutUntrackedOrigin](
            unsafe_ptr=buf.unsafe_bitcast[UInt8](), length=n
        )
    )


def worker_token() -> UInt64:
    """A number naming one worker's files, so that no two workers, on this
    host or another, write the same file name: a hash of the host name, the
    process ID and a nanosecond clock."""
    var pid = external_call["getpid", c_int]()
    var seed = String(node_name(), "/", Int(pid), "/", Int(perf_counter_ns()))
    var h: UInt64 = 0xCBF29CE484222325
    for b in seed.as_bytes():
        h ^= UInt64(b)
        h *= 0x100000001B3
    return h


def _exists(path: String) -> Bool:
    var p = path
    return external_call["access", c_int](p.as_c_string_span(), _F_OK) == 0


def store_dir() -> String:
    """Where large results go, or "" when the shared store is off."""
    if getenv("DISTA_SHM") == "0":
        return String()
    var configured = getenv("DISTA_STORE_DIR")
    if configured != "":
        return configured
    comptime if CompilationTarget.is_linux():
        if _exists("/dev/shm"):
            return String("/dev/shm")
    var tmp = getenv("TMPDIR", "/tmp")
    if tmp.endswith("/"):
        return String(tmp.removesuffix("/"))
    return tmp


def remove_file(path: String):
    var p = path
    _ = external_call["unlink", c_int](p.as_c_string_span())


def write_shared(path: String, data: Span[UInt8, _]) raises:
    """Writes `data` into a new shared mapping at `path`."""
    var region = MappedRegion(path, len(data))
    var dst = Pointer[UInt8, MutUntrackedOrigin](
        unsafe_from_address=region.base()
    )
    if len(data) > 0:
        unsafe_memcpy(dest=dst, src=data.unsafe_ptr(), count=len(data))
    # Mojo destroys a value after its last use, which would otherwise be
    # `base()` above: the region would be unmapped before the writes.
    region^.close()


def read_shared(path: String, size: Int) raises -> List[UInt8]:
    """The first `size` bytes of the shared mapping at `path`."""
    var m = SharedMapping(path)
    if size > m.size():
        raise Error("shared object ", path, " is shorter than ", size, " bytes")
    return List[UInt8](m.bytes()[:size])


struct Location(Copyable, Movable):
    """Where a large result lives."""

    var ip: Array[UInt8, 4]
    var port: Int
    var node: String
    var path: String
    var size: Int

    def __init__(
        out self,
        ip: Array[UInt8, 4],
        port: Int,
        var node: String,
        var path: String,
        size: Int,
    ):
        self.ip = ip.copy()
        self.port = port
        self.node = node^
        self.path = path^
        self.size = size

    def is_local(self) -> Bool:
        """Whether this process can map it instead of fetching it."""
        return self.path != "" and _exists(self.path)

    def encode(self) -> List[UInt8]:
        var out = encode_addr(self.ip, self.port)
        put_scalar[DType.uint64](out, UInt64(self.node.byte_length()))
        out.extend(self.node.as_bytes())
        put_scalar[DType.uint64](out, UInt64(self.path.byte_length()))
        out.extend(self.path.as_bytes())
        put_scalar[DType.uint64](out, UInt64(self.size))
        return out^

    @staticmethod
    def decode(buf: Span[UInt8, _], mut pos: Int) raises -> Location:
        var addr = decode_addr(buf, pos)
        var node = _get_string(buf, pos)
        var path = _get_string(buf, pos)
        var size = Int(get_scalar[DType.uint64](buf, pos))
        return Location(addr[0], addr[1], node^, path^, size)


def _get_string(buf: Span[UInt8, _], mut pos: Int) raises -> String:
    var n = Int(get_scalar[DType.uint64](buf, pos))
    if n < 0 or n > len(buf) - pos:
        raise Error("truncated location")
    var s = String(from_utf8=buf[pos : pos + n])
    pos += n
    return s^

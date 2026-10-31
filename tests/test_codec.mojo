from std.testing import TestSuite, assert_equal, assert_raises, assert_true

from dista import Codec, Seq, Task, decode, encode
from dista.task import fnv1a64


@fieldwise_init
struct Inner(Copyable, Movable):
    var tag: Int32
    var label: String


@fieldwise_init
struct Outer(Copyable, Movable):
    var id: Int
    var score: Float64
    var ok: Bool
    var inner: Inner
    var count: UInt8


struct Pair(Codec, Copyable, Movable):
    """Encodes itself, to exercise the `Codec` path."""

    var a: Int
    var b: Int

    def __init__(out self, a: Int, b: Int):
        self.a = a
        self.b = b

    def encode(self, mut out: List[UInt8]):
        encode(self.a + self.b, out)
        encode(self.a - self.b, out)

    @staticmethod
    def decode(buf: Span[UInt8, _], mut pos: Int) raises -> Self:
        var s = decode[Int](buf, pos)
        var d = decode[Int](buf, pos)
        return Pair((s + d) // 2, (s - d) // 2)


def roundtrip[T: Movable & Deinitable](value: T) raises -> T:
    var buf = List[UInt8]()
    encode(value, buf)
    var pos = 0
    var out = decode[T](buf, pos)
    assert_equal(pos, len(buf))
    return out^


def test_scalars() raises:
    assert_equal(roundtrip(Int(-123456789)), -123456789)
    assert_equal(roundtrip(UInt8(250)), 250)
    assert_equal(roundtrip(Int16(-3)), -3)
    assert_equal(roundtrip(UInt64(0xFFFF_FFFF_FFFF)), 0xFFFF_FFFF_FFFF)
    assert_equal(roundtrip(Float64(2.5)), 2.5)
    assert_equal(roundtrip(Float32(-0.25)), -0.25)
    assert_equal(roundtrip(True), True)
    assert_equal(roundtrip(False), False)


def test_int_is_eight_bytes_little_endian() raises:
    var buf = List[UInt8]()
    encode(Int(0x0102), buf)
    assert_equal(len(buf), 8)
    assert_equal(buf[0], 0x02)
    assert_equal(buf[1], 0x01)


def test_string() raises:
    assert_equal(roundtrip(String("")), "")
    assert_equal(roundtrip(String("héllo, dista")), "héllo, dista")


def test_nested_struct() raises:
    var o = roundtrip(Outer(7, -1.5, True, Inner(42, "deep"), 9))
    assert_equal(o.id, 7)
    assert_equal(o.score, -1.5)
    assert_true(o.ok)
    assert_equal(o.inner.tag, 42)
    assert_equal(o.inner.label, "deep")
    assert_equal(o.count, 9)


def test_codec_type() raises:
    var p = roundtrip(Pair(10, 3))
    assert_equal(p.a, 10)
    assert_equal(p.b, 3)


def test_truncated_buffer_raises() raises:
    var buf = List[UInt8]()
    encode(String("truncate me"), buf)
    _ = buf.pop()
    var pos = 0
    with assert_raises(contains="decode"):
        _ = decode[String](buf, pos)


@fieldwise_init
struct Bag(Copyable, Movable):
    var name: String
    var points: Seq[Inner]


def test_seq() raises:
    var words = roundtrip(Seq[String](["a", "", "dista"]))
    assert_equal(len(words), 3)
    assert_equal(words[2], "dista")
    assert_equal(len(roundtrip(Seq[Int]())), 0)


def test_seq_inside_struct() raises:
    var pts = Seq[Inner]()
    pts.append(Inner(1, "one"))
    pts.append(Inner(2, "two"))
    var b = roundtrip(Bag("bag", pts^))
    assert_equal(b.name, "bag")
    assert_equal(len(b.points), 2)
    assert_equal(b.points[1].label, "two")


def square(x: Int) -> Int:
    return x * x


def cube(x: Int) -> Int:
    return x * x * x


def test_task_ids() raises:
    comptime Square = Task[Int, Int, square]
    comptime Cube = Task[Int, Int, cube]
    assert_true(Square.id() != Cube.id())
    assert_equal(Square.id(), fnv1a64(Square.name()))
    # FNV-1a test vector: the hash is fixed across processes and machines.
    assert_equal(fnv1a64("a"), 0xAF63DC4C8601EC8C)


def test_task_run() raises:
    comptime Square = Task[Int, Int, square]
    var args = List[UInt8]()
    encode(Int(12), args)
    var out = List[UInt8]()
    Square.run(args, out)
    var pos = 0
    assert_equal(decode[Int](out, pos), 144)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

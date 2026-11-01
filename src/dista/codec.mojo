"""Turning task arguments and results into bytes and back.

`encode` and `decode` work on any type, chosen at compile time:

- built-in scalars, `Bool` and `String` have fixed encodings (little-endian
  fixed width; `String` is an 8-byte length then its UTF-8 bytes);
- arrow-mlake's `RecordBatch` is an 8-byte length then one Arrow IPC stream
  holding the schema and the batch, so columnar data crosses processes in
  Arrow's own format;
- a type that conforms to `Codec` encodes itself;
- any other struct is encoded field by field, in declaration order, by
  reflection.

Built-ins are recognised by `reflect[T].name()`, because `Int` or `String`
can't adopt a dista trait. `List` and other generic containers aren't
supported: reflection can't recover their element type. Wrap them in a type
that implements `Codec`, or send columnar data as Arrow.

Both ends are the same binary, so there's no versioning or schema.
"""

from std.memory import bitcast
from std.memory.alloc import unsafe_alloc
from std.os import abort
from std.sys import size_of

from arrow_mlake import IpcWriter, RecordBatch, read_ipc_stream


trait Codec:
    """A type that writes and reads its own bytes."""

    def encode(self, mut out: List[UInt8]):
        """Appends this value's bytes to `out`."""
        ...

    @staticmethod
    def decode(buf: Span[UInt8, _], mut pos: Int) raises -> Self:
        """Reads a value starting at `buf[pos]` and advances `pos` past it."""
        ...


def _need(buf: Span[UInt8, _], pos: Int, n: Int) raises:
    # Lengths come off the wire: a negative one would pass a plain
    # `pos + n > len` and slice out of bounds.
    if n < 0 or pos < 0 or n > len(buf) - pos:
        raise Error(
            "decode: needed ", n, " bytes at ", pos, ", have ", len(buf) - pos
        )


def put_scalar[dt: DType](mut out: List[UInt8], v: Scalar[dt]):
    """Appends `v` as its little-endian bytes."""
    comptime n = size_of[Scalar[dt]]()
    var b = bitcast[DType.uint8, n](v)
    comptime for i in range(n):
        out.append(b[i])


def get_scalar[
    dt: DType
](buf: Span[UInt8, _], mut pos: Int) raises -> Scalar[dt]:
    """Reads a little-endian scalar at `buf[pos]` and advances `pos`."""
    comptime n = size_of[Scalar[dt]]()
    _need(buf, pos, n)
    var b = SIMD[DType.uint8, n]()
    comptime for i in range(n):
        b[i] = buf[pos + i]
    pos += n
    return bitcast[dt, 1](b)


# `Bool` reflects as "Bool", never as a SIMD name, so `DType.bool` is free to
# mean "not a scalar" here.
comptime _NOT_SCALAR = DType.bool


def _scalar_dtype[name: StaticString]() -> DType:
    """The dtype of a reflected scalar type name, or `_NOT_SCALAR`."""
    comptime if name == "SIMD[DType.int, 1]":
        return DType.int
    elif name == "SIMD[DType.uint, 1]":
        return DType.uint
    elif name == "SIMD[DType.int8, 1]":
        return DType.int8
    elif name == "SIMD[DType.int16, 1]":
        return DType.int16
    elif name == "SIMD[DType.int32, 1]":
        return DType.int32
    elif name == "SIMD[DType.int64, 1]":
        return DType.int64
    elif name == "SIMD[DType.uint8, 1]":
        return DType.uint8
    elif name == "SIMD[DType.uint16, 1]":
        return DType.uint16
    elif name == "SIMD[DType.uint32, 1]":
        return DType.uint32
    elif name == "SIMD[DType.uint64, 1]":
        return DType.uint64
    elif name == "SIMD[DType.float32, 1]":
        return DType.float32
    elif name == "SIMD[DType.float64, 1]":
        return DType.float64
    else:
        return _NOT_SCALAR


def _encode_batch(batch: RecordBatch, mut out: List[UInt8]):
    var stream: List[UInt8]
    try:
        var w = IpcWriter(batch)
        w.write(batch)
        stream = w^.finish()
    except e:
        # Only a malformed batch fails to write, such as one whose offsets
        # don't cover its rows. `encode` can't raise; abort with the reason.
        abort(String("dista: can't encode a RecordBatch: ", e))
    put_scalar[DType.int64](out, Int64(len(stream)))
    out.extend(Span(stream))


def _decode_batch(buf: Span[UInt8, _], mut pos: Int) raises -> RecordBatch:
    var n = Int(get_scalar[DType.int64](buf, pos))
    _need(buf, pos, n)
    var s = read_ipc_stream(buf[pos : pos + n])
    pos += n
    if len(s.batches) != 1:
        raise Error(
            "decode: a RecordBatch stream holds ", len(s.batches), " batches"
        )
    return s.batches.pop()


def encode[T: AnyType](ref value: T, mut out: List[UInt8]):
    """Appends the bytes of `value` to `out`."""
    comptime name = reflect[T].name()
    comptime dt = _scalar_dtype[name]()
    comptime if dt != _NOT_SCALAR:
        put_scalar[dt](out, rebind[Scalar[dt]](value))
    elif name == "Bool":
        out.append(UInt8(1) if rebind[Bool](value) else UInt8(0))
    elif name == "String":
        ref s = rebind[String](value)
        put_scalar[DType.int64](out, Int64(s.byte_length()))
        for b in s.as_bytes():
            out.append(b)
    elif name == "arrow_mlake.batch.RecordBatch":
        _encode_batch(rebind[RecordBatch](value), out)
    elif conforms_to(T, Codec):
        comptime assert conforms_to(T, Codec)
        value.encode(out)
    else:
        comptime assert reflect[T].is_struct(), "dista can't encode this type"
        comptime for i in range(reflect[T].field_count()):
            encode(reflect[T].field_ref[i](value), out)


def decode[T: Movable](buf: Span[UInt8, _], mut pos: Int) raises -> T:
    """Reads a `T` starting at `buf[pos]` and advances `pos` past it."""
    comptime name = reflect[T].name()
    comptime dt = _scalar_dtype[name]()
    comptime if dt != _NOT_SCALAR:
        return rebind_var[T](get_scalar[dt](buf, pos))
    elif name == "Bool":
        _need(buf, pos, 1)
        pos += 1
        return rebind_var[T](buf[pos - 1] != 0)
    elif name == "String":
        var n = Int(get_scalar[DType.int64](buf, pos))
        _need(buf, pos, n)
        var s = String(from_utf8=buf[pos : pos + n])
        pos += n
        return rebind_var[T](s^)
    elif name == "arrow_mlake.batch.RecordBatch":
        return rebind_var[T](_decode_batch(buf, pos))
    elif conforms_to(T, Codec):
        comptime assert conforms_to(T, Codec)
        return T.decode(buf, pos)
    else:
        comptime assert reflect[T].is_struct(), "dista can't decode this type"
        # Build the struct field by field in raw storage, then move it out:
        # no default constructor needed. A failure partway leaks the fields
        # already decoded, which only happens on a corrupt message.
        var p = unsafe_alloc[T](1)
        try:
            comptime for i in range(reflect[T].field_count()):
                comptime F = reflect[T].field_at[i].T
                comptime assert conforms_to(
                    F, Movable
                ), "struct fields must be Movable"
                var at = p.unsafe_bitcast[UInt8]().unsafe_offset(
                    reflect[T].field_offset[index=i]()
                )
                at.unsafe_bitcast[F]().unsafe_write(decode[F](buf, pos))
        except e:
            p.unsafe_free()
            raise e^
        var v = p.unsafe_take_pointee()
        p.unsafe_free()
        return v^

"""`Seq[T]`: a list that goes through the codec.

`List` can't be encoded by reflection, because reflection can't recover its
element type. `Seq[T]` carries `T` as a parameter, so it can encode each
element with the generic codec:

```mojo
def total(xs: Seq[Int]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i]
    return t

comptime Total = Task[Seq[Int], Int, total]
```

On the wire a `Seq` is an 8-byte count, then each element's encoding. That is
also how the runtime builds one from a list of `ObjectRef`s: the count, then
each result's bytes as they arrived, without decoding them.
"""

from dista.codec import Codec, decode, encode, get_scalar, put_scalar


struct Seq[T: Copyable & Movable & Deinitable](Codec, Copyable, Movable, Sized):
    """A list of `T` that tasks can take and return."""

    var items: List[Self.T]

    def __init__(out self):
        self.items = List[Self.T]()

    def __init__(out self, var items: List[Self.T]):
        self.items = items^

    def append(mut self, var value: Self.T):
        self.items.append(value^)

    def __len__(self) -> Int:
        return len(self.items)

    def __getitem__(self, i: Int) -> Self.T:
        """A copy of element `i`. Use `items` to borrow instead."""
        return self.items[i].copy()

    def encode(self, mut out: List[UInt8]):
        put_scalar[DType.int64](out, Int64(len(self.items)))
        for i in range(len(self.items)):
            encode(self.items[i], out)

    @staticmethod
    def decode(buf: Span[UInt8, _], mut pos: Int) raises -> Self:
        var n = Int(get_scalar[DType.int64](buf, pos))
        if n < 0:
            raise Error("decode: negative Seq length ", n)
        var items = List[Self.T](capacity=n)
        for _ in range(n):
            items.append(decode[Self.T](buf, pos))
        return Self(items^)

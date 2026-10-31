"""A driver built with different tasks than examples/mapreduce.mojo."""

from dista import Runtime, Task


def negate(x: Int) -> Int:
    return -x


comptime Negate = Task[Int, Int, negate]


def main() raises:
    var rt = Runtime[Negate]()
    print(rt.get(rt.remote[Negate](1)))

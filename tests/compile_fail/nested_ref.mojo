"""Must not compile: an `ObjectRef` inside a task's argument. dista sends
refs only as the argument, in a list, or beside a value; nested ones would
need Ray's borrowing. tests/compile_fail.sh checks the error names the fix.
"""

from dista import ObjectRef, Runtime, Seq, Task


@fieldwise_init
struct Holder(Copyable, Movable):
    var r: ObjectRef[Int]


def peek(h: Holder) -> Int:
    return 0


comptime Peek = Task[Holder, Int, peek]


def main() raises:
    var rt = Runtime[Peek](workers=1)
    print(rt.get(rt.remote[Peek](Holder(ObjectRef[Int](1)))))

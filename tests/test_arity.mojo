"""Tasks with two and three arguments (`Task2`, `Task3`)."""

from std.testing import TestSuite, assert_equal, assert_raises

from dista import ObjectRef, Runtime, Task, Task2, Task3


def scale(x: Int, factor: Float64) -> Float64:
    return Float64(x) * factor


def label(name: String, n: Int, unit: String) -> String:
    return String(name, ": ", n, " ", unit)


def square(x: Int) -> Int:
    return x * x


comptime Scale = Task2[Int, Float64, Float64, scale]
comptime Label = Task3[String, Int, String, String, label]
comptime Square = Task[Int, Int, square]


def test_a_task_with_two_arguments() raises:
    var rt = Runtime[Scale, Square](workers=2)
    assert_equal(rt.get(rt.remote[Scale](5, 2.5)), 12.5)


def test_a_task_with_three_arguments() raises:
    var rt = Runtime[Label](workers=2)
    assert_equal(rt.get(rt.remote[Label]("trips", 42, "km")), "trips: 42 km")


def test_a_large_argument_and_many_tasks() raises:
    """Arguments of any size, and many tasks at once, each answered."""
    var rt = Runtime[Label](workers=2)
    var big = String("x") * 200_000
    assert_equal(
        rt.get(rt.remote[Label](big, 1, "y")).byte_length(), 200_000 + 5
    )
    var refs = List[ObjectRef[String]]()
    for i in range(50):
        refs.append(rt.remote[Label]("n", i, "u"))
    for i in range(50):
        assert_equal(rt.get(refs[i]), String("n: ", i, " u"))


def test_too_few_arguments_are_refused() raises:
    """Calling with too few arguments raises at the call, by name, instead
    of sending something the worker can't decode."""
    var rt = Runtime[Scale, Label](workers=1)
    with assert_raises(contains="takes 2 arguments"):
        _ = rt.remote[Scale](5)
    with assert_raises(contains="takes 3 arguments, not 2"):
        _ = rt.remote[Label]("a", 1)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

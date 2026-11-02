"""The driver tests/locality.sh runs: tasks go to the node holding their
input.

Four workers run as two nodes, two each, by `DISTA_NODE`. Forty tasks make a
large result each, which stays on its worker, and the result names the node
it was made on. Then forty more tasks each take one of those results and
answer whether they ran on the node that holds it. Without placement about
half would; with it, a free worker takes the queued task whose input is on
its own node.
"""

from dista import ObjectRef, Runtime, Task
from dista.store import node_name

comptime N = 40


def make(i: Int) -> String:
    """A result too large to go through the head, starting with its node."""
    return node_name() + "|" + String("x") * (200_000 + i)


def same_node(s: String) -> Int:
    """1 if this task runs on the node its input was made on."""
    var made_on = String(s.split("|")[0])
    return 1 if made_on == node_name() else 0


comptime Make = Task[Int, String, make]
comptime SameNode = Task[String, Int, same_node]


def main() raises:
    var rt = Runtime[Make, SameNode](workers=4)
    var made = List[ObjectRef[String]]()
    for i in range(N):
        made.append(rt.remote[Make](i))
    for r in made:
        _ = rt.location(r)  # every input stored before any consumer queues
    var local = List[ObjectRef[Int]]()
    for r in made:
        local.append(rt.remote[SameNode](r))
    var n = 0
    for r in local:
        n += rt.get(r)
    print("local", n, "of", N)

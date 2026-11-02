"""`dista`: a head for any dista program, and a look at a running cluster.

    dista head [--port 6390] [--pipeline 2]
    dista status [--head host:port]
    dista timeline -o trace.json [--head host:port]
    dista stop [--head host:port]

The head here is generic: it holds no tasks of its own, so it serves drivers
and workers built from any program, and gives each driver's tasks and actors
only to workers built from the same one. `status`, `timeline` and `stop`
connect as observers. Their head is `--head`, else `DISTA_HEAD`, else
`localhost:6390`.

It depends on dista's core alone, so it's built and released on its own,
and the program's own binary stays the driver and the worker.
"""

from std.os import getenv
from std.sys import argv

from dista.client import Observer, write_trace
from dista.head import Head
from dista.signals import exit_on_sigterm
from dista.socket import Listener, exit_now

comptime USAGE = """usage:
  dista head [--port 6390] [--pipeline 2]   run a head for any program
  dista status [--head host:port]           the head's workers, drivers and actors
  dista timeline -o FILE [--head host:port] every driver's finished tasks, as a Chrome trace
  dista stop [--head host:port]             shut the head and its workers down"""


def main() raises:
    var args = argv()
    if len(args) < 2:
        print(USAGE)
        exit_now(2)
    var cmd = String(args[1])
    var port = 6390
    var pipeline = 2
    var head = getenv("DISTA_HEAD", "localhost:6390")
    var out = String()
    var i = 2
    while i < len(args):
        var a = String(args[i])
        if i + 1 >= len(args):
            print("dista: ", a, " needs a value\n", USAGE, sep="")
            exit_now(2)
        var v = String(args[i + 1])
        if a == "--port":
            port = Int(v)
        elif a == "--pipeline":
            pipeline = Int(v)
        elif a == "--head":
            head = v
        elif a == "-o":
            out = v
        else:
            print("dista: unknown option ", a, "\n", USAGE, sep="")
            exit_now(2)
        i += 2

    if cmd == "head":
        exit_on_sigterm()
        var listener = Listener(port, any_interface=True)
        print("dista head listening on port", listener.port)
        var h = Head(listener^, 0, pipeline, False)
        h.run()
    elif cmd == "status":
        var o = Observer(head)
        print(o.status(), end="")
    elif cmd == "timeline":
        if out == "":
            print("dista: timeline needs -o FILE\n", USAGE, sep="")
            exit_now(2)
        var o = Observer(head)
        var n = write_trace(out, o.spans())
        print("wrote", n, "tasks to", out)
    elif cmd == "stop":
        var o = Observer(head)
        o.stop()
        print("stopped the head at", head)
    else:
        print("dista: unknown command ", cmd, "\n", USAGE, sep="")
        exit_now(2)

"""`dista`: a head for any dista program, and a look at a running cluster.

    dista head [--port 6390] [--pipeline 2]
    dista status [--json] [--head host:port]
    dista drain WORKER [--head host:port]
    dista scale --once [--min 1] [--max 4] [--pending 0] [--short-since 0] ...
    dista timeline -o trace.json [--head host:port]
    dista stop [--head host:port]

The head here is generic: it holds no tasks of its own, so it serves drivers
and workers built from any program, and gives each driver's tasks and actors
only to workers built from the same one. `status`, `drain`, `timeline` and
`stop` connect as observers. `status --json` is the head's demand, for an
autoscaler; `drain` takes a worker's address as `status` shows it. `scale
--once` reads the demand and prints `dista.scale.decide`'s decision, a line
each: `want N`, `start N`, `drain ADDRESS` per worker to drain, and
`short_since T` to pass back next time; a provider loop runs it. Their head
is `--head`, else `DISTA_HEAD`, else `localhost:6390`.

It depends on dista's core alone, so it's built and released on its own,
and the program's own binary stays the driver and the worker.
"""

from std.os import getenv
from std.sys import argv

from dista.client import Observer, write_trace
from dista.head import Head
from dista.scale import Demand, Policy, decide
from dista.signals import exit_on_sigterm
from dista.socket import Listener, exit_now, wall_ns

comptime USAGE = """usage:
  dista head [--port 6390] [--pipeline 2]   run a head for any program
  dista status [--json] [--head host:port]  the head's workers, drivers and actors
  dista drain WORKER [--head host:port]     drain the worker at ip:port, as SIGTERM would
  dista scale --once [--head host:port]     what an autoscaler should do now, from the demand:
      [--min 1] [--max 4] [--per 0] [--up-after 5] [--idle 60] [--step 4]
      [--pending 0] [--short-since 0]
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
    var json = False
    var worker = String()
    var policy = Policy()
    var pending = 0
    var short_since = Float64(0)
    var i = 2
    if cmd == "drain" and i < len(args) and not String(args[i]).startswith("-"):
        worker = String(args[i])
        i += 1
    while i < len(args):
        var a = String(args[i])
        if a == "--json" or a == "--once":
            json = a == "--json" or json
            i += 1
            continue
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
        elif a == "--min":
            policy.min_workers = Int(v)
        elif a == "--max":
            policy.max_workers = Int(v)
        elif a == "--per":
            policy.tasks_per_worker = Int(v)
        elif a == "--up-after":
            policy.up_after = Float64(v)
        elif a == "--idle":
            policy.idle_before_down = Float64(v)
        elif a == "--step":
            policy.max_step = Int(v)
        elif a == "--pending":
            pending = Int(v)
        elif a == "--short-since":
            short_since = Float64(v)
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
        print(o.status_json() if json else o.status(), end="")
    elif cmd == "drain":
        if worker == "":
            print("dista: drain needs a worker, ip:port\n", USAGE, sep="")
            exit_now(2)
        var o = Observer(head)
        try:
            o.drain(worker)
        except e:
            print("dista: ", e, sep="")
            exit_now(1)
        print("draining", worker)
    elif cmd == "scale":
        # The same checks Policy's constructor makes.
        policy = Policy(
            policy.min_workers,
            policy.max_workers,
            policy.tasks_per_worker,
            policy.up_after,
            policy.idle_before_down,
            policy.max_step,
        )
        var o = Observer(head)
        var d = Demand.parse(o.status_json())
        var now = Float64(wall_ns()) / 1e9
        print(decide(d, policy, now, pending, short_since), end="")
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

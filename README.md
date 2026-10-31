# dista

A distributed task framework for Mojo: mark a function remote, call it, and the work runs across a cluster. Every node runs the same compiled binary, and data moves between tasks as Arrow.

```mojo
from dista import ObjectRef, Runtime, Task

def square(x: Int) -> Int:
    return x * x

comptime Square = Task[Int, Int, square]

def main() raises:
    var rt = Runtime[Square](workers=4)
    var refs = List[ObjectRef[Int]]()
    for i in range(1000):
        refs.append(rt.remote[Square](i))
    var total = 0
    for r in refs:
        total += rt.get(r)
    print(total)  # 332833500
```

**Status: phase 2 in progress.** One binary runs as driver, head or worker, chosen by environment. Run it on its own and it forks a head and workers on one machine; point it at a head and it joins a cluster:

```
DISTA_ROLE=head DISTA_PORT=6390 ./app                  # the head
DISTA_ROLE=worker DISTA_HEAD=headhost:6390 ./app       # once per worker
DISTA_HEAD=headhost:6390 ./app                         # the driver
```

What works today:
- **Arguments and results:** scalars, `String`, plain structs of those, `Seq[T]` lists, and types that implement `Codec`.
- **Results feeding tasks:** a task can take another task's `ObjectRef`, or gather a list of them into a `Seq`, as in [examples/mapreduce.mojo](examples/mapreduce.mojo).
- **Errors:** a task that raises re-raises in `get`, and a worker that dies fails only its own tasks.
- **Speed:** small tasks cost about 5 µs each.

Results kept on the workers that made them, a shared-memory store, Kubernetes, Arrow results and actors come next.

- [VISION.md](VISION.md): what dista is for, its principles and phases.
- [DESIGN.md](DESIGN.md): how it works, and what's verified on Mojo 1.2.0.

```
pixi run test               # codec, frame, runtime and cluster tests, Mojo 1.2.0
pixi run -e nightly test    # the same on the 1.3 nightly
pixi run bench              # task throughput
pixi run -e fmt fmt         # mojo format
```

Part of [magmalake](https://github.com/magmalake). Apache-2.0.

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
- **Arguments and results:** scalars, `String`, plain structs of those, `Seq[T]` lists, types that implement `Codec`, and arrow-mlake `RecordBatch`es, which travel as Arrow IPC streams.
- **Results feeding tasks:** a task can take another task's `ObjectRef`, or gather a list of them into a `Seq`, as in [examples/mapreduce.mojo](examples/mapreduce.mojo).
- **Errors:** a task that raises re-raises in `get`, and a worker that dies fails only its own tasks.
- **Large results stay on the worker that made them.** On the same host other processes map them from shared memory; across hosts they fetch them directly from that worker. Small ones come back through the head.
- **Speed:** small tasks cost about 6 µs each.
- **Kubernetes:** one image for every role, and manifests for a head and workers, in [deploy/](deploy). CI runs a smoke test on a 3-node kind cluster.

- **Parquet:** tasks can read row groups with parquet.mojo and hand them to other tasks as batches ([tests/test_parquet_tasks.mojo](tests/test_parquet_tasks.mojo)).

The Iceberg scan and actors come next.

- [VISION.md](VISION.md): what dista is for, its principles and phases.
- [DESIGN.md](DESIGN.md): how it works, and what's verified on Mojo 1.2.0.

```
pixi run test               # codec, frame, runtime and cluster tests, Mojo 1.2.0
pixi run -e nightly test    # the same on the 1.3 nightly
pixi run -e data test-parquet  # tasks over Parquet; needs parquet.mojo and its siblings checked out next door
pixi run bench              # task throughput
pixi run test-kind          # the smoke test on a kind cluster (needs docker, kind, kubectl)
pixi run -e fmt fmt         # mojo format
```

Part of [magmalake](https://github.com/magmalake). Apache-2.0.

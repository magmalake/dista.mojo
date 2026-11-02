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

**Status: phase 3 done** (actors, restarts, checkpoints, metrics). Phase 2's gate, a distributed Iceberg scan of the NYC taxi table on a 3-node cluster matching one machine, passes ([tests/kind_taxi.sh](tests/kind_taxi.sh)). One binary runs as driver, head or worker, chosen by environment. Run it on its own and it forks a head and workers on one machine; point it at a head and it joins a cluster:

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
- **Iceberg:** a scan planned by iceberg.mojo runs as one task per data file, deletes and filters included, and returns what a local scan returns ([tests/test_iceberg_scan.mojo](tests/test_iceberg_scan.mojo)).
- **Actors:** state on a worker of its own, with methods called in order, from the driver, from tasks or from other actors ([tests/test_actors.mojo](tests/test_actors.mojo)). An actor whose worker dies comes back on another one, from its last checkpoint if it has one (`checkpoint_every`, `rt.checkpoint`, written to `DISTA_CHECKPOINT_URI`, a directory or `s3://`), with the calls since run again. On kind, deleting the actor's pod mid-run leaves the count exact. Actors survive a head restart too: the new head restores them from their newest checkpoint, and their callers resend the calls since.
- **Shuffles:** a `TaskMany` returns several results, each its own object (`rt.remote_many[T](arg, n)`), and a `TaskWith` takes a value beside a list of results (`rt.remote[T](j, refs)`): a map task's partitions and a reducer's inputs ([tests/test_shuffle.mojo](tests/test_shuffle.mojo)).
- **The `dista` command:** `dista head` is a head for any program, serving several at once; `dista status`, `dista timeline -o trace.json` and `dista stop` look at or shut down a running cluster. Build it with `pixi run cli`.
- **Metrics:** with `DISTA_METRICS_PORT` set, the head and every worker serve Prometheus metrics at `/metrics`: tasks by outcome, task durations, queue depth, objects held, actors and checkpoints. The kind manifests turn it on at 9090.

- [VISION.md](VISION.md): what dista is for, its principles and phases.
- [DESIGN.md](DESIGN.md): how it works, and what's verified on Mojo 1.2.0.

```
pixi run test               # codec, frame, runtime and cluster tests, Mojo 1.2.0
pixi run -e nightly test    # the same on the 1.3 nightly
pixi run -e data test-data    # tasks over Parquet and Iceberg; needs those tins and their siblings checked out next door
pixi run bench              # task throughput
pixi run test-kind          # the smoke test on a kind cluster (needs docker, kind, kubectl)
pixi run -e fmt fmt         # mojo format
```

Part of [magmalake](https://github.com/magmalake). Apache-2.0.

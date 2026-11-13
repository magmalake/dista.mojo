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
        total += rt.take(r)  # get and release: read once, then freed
    print(total)  # 332833500
```

**Status: phase 4's gate passes.** The taxi queries on a 3-node kind cluster finish exact with a worker pod killed mid-run, its tasks run again elsewhere ([tests/kind_taxi.sh](tests/kind_taxi.sh)). A worker that Kubernetes stops drains instead of dying, so a cluster can shrink mid-job. dista creates no pods: Kubernetes or [orca](https://github.com/magmalake/orca.mojo) does ([DESIGN.md](DESIGN.md#kubernetes-dista-and-orca)). One binary runs as driver, head or worker, chosen by environment. Run it on its own and it forks a head and workers on one machine; point it at a head and it joins a cluster:

```
DISTA_ROLE=head DISTA_PORT=6390 ./app                  # the head
DISTA_ROLE=worker DISTA_HEAD=headhost:6390 ./app       # once per worker
DISTA_HEAD=headhost:6390 ./app                         # the driver
```

What works today:
- **Up to three arguments:** `Task2` and `Task3` take two and three, as `rt.remote[T](a, b)` and `rt.remote[T](a, b, c)` ([tests/test_arity.mojo](tests/test_arity.mojo)); more travel as a struct.
- **Arguments and results:** scalars, `String`, plain structs of those, `Seq[T]` lists, types that implement `Codec`, and arrow-mlake `RecordBatch`es, which travel as Arrow IPC streams.
- **Results feeding tasks:** a task can take another task's `ObjectRef`, or gather a list of them into a `Seq`, as in [examples/mapreduce.mojo](examples/mapreduce.mojo).
- **Freeing results:** a result stays in the driver, at the head and on its worker until its ref is released. `rt.take(r)` reads it and releases it, the way to read a result used once; `rt.get(r)` keeps it, for reading again, until `rt.release(r)`. `dista status` shows how many objects each driver holds, and the head logs a driver holding more than `DISTA_WARN_HELD` (default 100,000) ([tests/test_take.mojo](tests/test_take.mojo)).
- **Giving up on work:** `rt.cancel(ref)` drops a task that hasn't run and discards the result of one that's running; `get` then raises `cancelled`. `rt.get(ref, timeout=s)` raises an error starting `timed out` after `s` seconds, leaving the ref valid. A driver that disconnects takes its queued and waiting tasks with it ([tests/test_cancel.mojo](tests/test_cancel.mojo)).
- **Errors:** a task that raises re-raises in `get` and `take`. A worker that dies has its tasks run again elsewhere, up to `DISTA_TASK_RETRIES` times; the results it held are lost, and a task that needs one fails, naming it. A worker whose host freezes or drops off the network is noticed by TCP keepalive within about 25 s (`DISTA_KEEPALIVE_SECONDS`), and `DISTA_TASK_TIMEOUT` fails a task that runs too long, however its worker stalls.
- **Draining:** a worker sent SIGTERM, as Kubernetes stops a pod, finishes its tasks while the others adopt its results and its actor, restored from a checkpoint, then leaves: nothing fails or runs again ([tests/drain.sh](tests/drain.sh)). `DISTA_DRAIN_SECONDS` (default 25) bounds it.
- **Several tasks per worker:** `DISTA_WORKER_SLOTS=N` (0 for one per core) runs up to N tasks at once on a worker's threads, while it keeps serving its results; the default, 1, runs one at a time. N slots hold N tasks' inputs and results, and tasks must be thread-safe ([tests/test_slots.mojo](tests/test_slots.mojo)).
- **Sizing from the grant:** `DISTA_WORKER_SLOTS=auto` takes a worker's slots from the run's grant (`MORCA_GRANT_CPUS`, shared by a local run's workers) and keeps its resident memory to the grant's `MORCA_GRANT_MEMORY_MB`: past it, tasks wait at the head for a worker with room, and the store spills at half of it ([tests/test_sizing.mojo](tests/test_sizing.mojo)).
- **Large results stay on the worker that made them.** On the same host other processes map them from shared memory; across hosts they fetch them directly from that worker. Small ones come back through the head.
- **Spilling:** past `DISTA_STORE_BYTES` of results, or when the store is full, a worker writes the rest to `DISTA_SPILL_DIR` on disk, and readers map or fetch them as before ([tests/test_spill.mojo](tests/test_spill.mojo)).
- **Data-aware scheduling:** a task goes to the node holding most of its inputs' bytes, unless that node has no worker free ([tests/locality.sh](tests/locality.sh)).
- **Speed:** small tasks cost about 6 µs each.
- **Backpressure:** past `DISTA_MAX_QUEUED` tasks (default 100,000) or `DISTA_MAX_QUEUED_BYTES` of arguments (default 1 GiB) waiting at the head, `remote` blocks until workers catch up; past `DISTA_MAX_DRIVER_BUFFER` bytes of results waiting for a driver that isn't reading, its tasks wait ([tests/test_admission.mojo](tests/test_admission.mojo)).
- **Kubernetes:** one image for every role, and manifests for a head and workers, in [deploy/](deploy). CI runs a smoke test on a 3-node kind cluster.
- **Parquet:** tasks can read row groups with parquet.mojo and hand them to other tasks as batches ([tests/test_parquet_tasks.mojo](tests/test_parquet_tasks.mojo)).
- **Iceberg:** a scan planned by iceberg.mojo runs as one task per data file, deletes and filters included, and returns what a local scan returns ([tests/test_iceberg_scan.mojo](tests/test_iceberg_scan.mojo)).
- **Actors:** state on a worker of its own, with methods called in order, from the driver, from tasks or from other actors ([tests/test_actors.mojo](tests/test_actors.mojo)). An actor whose worker dies comes back on another one, from its last checkpoint if it has one (`checkpoint_every`, `rt.checkpoint`, written to `DISTA_CHECKPOINT_URI`, a directory or `s3://`), with the calls since run again. On kind, deleting the actor's pod mid-run leaves the count exact. Actors survive a head restart too: the new head restores them from their newest checkpoint, and their callers resend the calls since.
- **Shuffles:** a `TaskMany` returns several results, each its own object (`rt.remote_many[T](arg, n)`, or on other tasks' results, `rt.remote_many[T](refs, n)`), and a `TaskWith` takes a value beside a list of results (`rt.remote[T](j, refs)`): a map task's partitions and a reducer's inputs ([tests/test_shuffle.mojo](tests/test_shuffle.mojo)).
- **The `dista` command:** `dista head` is a head for any program, serving several at once; `dista status` (`--json` for an autoscaler), `dista drain <ip:port>`, `dista scale --once`, `dista timeline -o trace.json` and `dista stop` look at, shrink or shut down a running cluster. Build it with `pixi run cli`.
- **Metrics:** with `DISTA_METRICS_PORT` set, the head and every worker serve Prometheus metrics at `/metrics`: tasks by outcome, task durations, queue depth, objects held, actors and checkpoints. The kind manifests turn it on at 9090.

## Install

dista is a [mojoshelf](https://mojoshelf.org) tin:

```sh
pixi shelf add dista-mojo     # with arrow-mlake, memory-region and objectstore
```

Then `from dista import Runtime, Task` as above. Build your program once and
run the same binary as the head, the workers and the driver.

## More

- [CHANGELOG.md](CHANGELOG.md): what each release changed.
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

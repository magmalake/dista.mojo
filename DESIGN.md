# dista: design

October 31, 2026 · targets stable Mojo 1.2.0

This turns [VISION.md](VISION.md) into a buildable design. It covers phases 1 and 2 in detail and sketches 3 and 4. Everything marked *verified* was compiled and run on Mojo 1.2.0 while writing this.

## Programming model

A remote function is a plain top-level `def`. It's declared once as a task, with its argument and result types spelled out, then called through the task:

```mojo
from dista import Task, Runtime

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
```

- **Why declare tasks instead of `remote[square](7)`.** Mojo 1.2 can't infer a function's result type from the function value, so `R` must be written somewhere. The declaration is that place. It's also Ray's shape: `@ray.remote` once, `.remote()` at each call. *Verified.*
- **The function type is `def(A) thin -> R`.** A top-level `def` is a `thin` function; without `thin` the parameter doesn't bind. Argument and result types need `Movable & Deinitable` (1.2 makes destruction explicit for generic types). *Verified.*
- **One argument per task in phase 1.** Several values travel as one struct, which the codec handles by reflection. `Task2`, `Task3` overloads can come later; they're mechanical.
- **Results as arguments.** `rt.remote[T](ref)` takes an `ObjectRef[T.A]` in place of a `T.A`. `rt.remote[T](refs)` takes a `List[ObjectRef[X]]` when `T` takes a `Seq[X]`. Either way the task waits until every input exists. Its argument is then assembled from the inputs' encoded bytes, without decoding them. If an input failed, the task fails with that error and never runs.
- **`ObjectRef[R]`** is a typed handle to a result that may not exist yet. `rt.get(ref)` blocks until it does and returns an `R`. Getting the same ref twice decodes it twice; phase 1 keeps every result until the runtime ends.
- **Errors.** A task that raises sends its message back; `get` re-raises it in the driver, and the worker carries on. Raising and non-raising functions both bind to `def(A) thin raises -> R`, so one `Task` type covers both. *Verified.*

## Function registry and dispatch

Every node runs the same binary, so every task already exists everywhere. A task message carries a **task ID**, not code.

- **Task ID** = a 64-bit hash of the function's linkage name, `reflect_fn[f].linkage_name()`, e.g. `app::square(::SIMD[DType.int, 1])`. Linkage names include the module and the argument types, so overloads differ. *Verified.*
- **The registry is the runtime's parameter list:** `Runtime[Square, Shout, ...]`. A worker dispatches with a `comptime for` over that list, comparing IDs, decoding the argument, calling the function and encoding the result. Everything is resolved at compile time, so there's no table to build or keep in sync. *Verified*, with `Ts.length` to iterate a variadic type pack.
- **Collisions.** At startup the runtime checks that no two registered tasks share an ID and raises if they do.
- **Version skew** is impossible within one image. Across a rolling upgrade (phase 2), the head refuses a worker whose registry hash differs from its own.

## Serialization

`encode(value, out)` and `decode[T](buf, pos)` are generic functions over any `T`, chosen at compile time:

| Type | Encoding |
|---|---|
| `Int`, `IntN`, `UIntN` | little-endian, fixed width (8 bytes for `Int`) |
| `Float32`, `Float64` | IEEE bits, little-endian |
| `Bool` | 1 byte |
| `String` | 8-byte length, then UTF-8 bytes |
| a type conforming to `Codec` | its own `encode` / `decode` |
| any other struct | each field in declaration order, recursively, by reflection |

- **Built-ins are recognised by `reflect[T].name()`**, because `Int` or `String` can't adopt a dista trait. Note `Int` reports as `SIMD[DType.int, 1]`. *Verified.*
- **User types opt in with `comptime assert conforms_to(T, Codec)`**, which makes `T`'s methods callable inside that branch. *Verified.*
- **Reflection decoding builds the struct field by field** into allocated storage, at `reflect[T].field_offset[index=i]()`, then takes it out. No `Defaultable` needed. *Verified:* a struct of `Int` plus a `Codec` type round-trips.
- **`List[T]` isn't encodable; `Seq[T]` is.** Reflection can't recover a `List`'s element type, but `Seq[T]` carries `T` as a parameter and implements `Codec`: an 8-byte count, then each element. Use it for list-valued arguments and results, including as a struct field. Arrow is for anything large.
- **No schema evolution.** Both ends are the same binary.

## Data plane: columnar results

Columnar data does **not** go through `Serializable`. The one columnar type is arrow-mlake's `RecordBatch`, which is what parquet.mojo's `read_batch()` returns, and it travels as **Arrow IPC** everywhere:

| Where | How | Phase |
|---|---|---|
| Same node | IPC stream written once into the node's shared-memory store; readers map it | 2 |
| Between nodes | the same bytes, fetched from the worker's object server | 2 |
| Spill to object storage | the same bytes as `.arrow` files, readable by pyarrow, DuckDB, Polars | 4 |
| Phase 1 (one machine) | IPC bytes over the task socket | 1 |

- **One object per batch**, roughly one per Parquet row group, so a scan task returns one `ObjectRef` per batch.
- **How it's built:** the codec recognises `RecordBatch` by its reflected name, `arrow_mlake.batch.RecordBatch`, and writes an 8-byte length, then one IPC stream: the schema and the batch, written by arrow-mlake's `IpcWriter` (arrow-mlake 0.2.0). So a batch is an argument or a result like any other: inline when small, in the node store or fetched from its worker when large, and gathered into a `Seq[RecordBatch]`. Every process is the same binary, so a lighter encoding without the schema would work too, but IPC is what other tools read, and phase 4's spill files need it anyway.
- **Verified:** `tests/test_batches.mojo`, and the PR-8 gate in `tests/test_parquet_tasks.mojo`: one task per row group of parquet.mojo's 100,000-row fixture returns the group as a batch, other tasks sum a column and count rows, and the answers match a local read. It runs in the `data` environment, which compiles parquet.mojo and its siblings from source (`pixi run -e data test-parquet`, and the CI `parquet` job), because parquet-mojo's package pins an older arrow-mlake.
- **The Iceberg scan** (the PR-9 gate) is `tests/test_iceberg_scan.mojo`. The driver plans the scan with iceberg.mojo and sends one task per data file. Each task reopens the table from its metadata file and reads its file with `TableScan.to_batches_for_paths`, so partition pruning, the residual filter and the attached delete files all still apply. Each task returns a `Seq[RecordBatch]`. On nine of iceberg.mojo's fixture tables (identity, bucket, day and truncate partitions, schema evolution, position deletes, deletion vectors, equality deletes), and with two filters, the sorted rows equal a local `to_batches`. Getting there needed memory-region to stop declaring `lseek` itself: the standard library declares it too, for `FileHandle.seek`, with other argument types, and one binary can't hold both (memory-region.mojo#7, released as 0.1.1).
- **Work still needed in the tins:**
  - Replace `export_shared`'s JSON manifest with IPC framing, so shared memory and Flight use one encoder. This changes a published tin's format.
  - Zero-copy reads need `ArrayData` buffers that borrow from a mapped region. Until then, reading a batch copies it once. That's accepted for phases 1 and 2.
- **Compression** (IPC's per-buffer LZ4/ZSTD) applies between nodes only.

## Processes and transport

### Roles and the head

Since phase 2's PR 3, scheduling lives in one place: a head process. Every program is one binary, and `Runtime(...)` picks its role from the environment:

| Environment | Role |
|---|---|
| `DISTA_ROLE=head` | runs the head on `DISTA_PORT` (default 6390; 0 lets the OS pick, and the head prints it) |
| `DISTA_ROLE=worker` | serves the head at `DISTA_HEAD=host:port` |
| `DISTA_HEAD=host:port` | a driver that submits to that head |
| neither | local mode: fork a head and `workers` workers, then drive them over loopback |

- **Local mode runs the cluster path.** The forked head is the same code a cluster runs, so every test exercises it. The extra hop costs about 1.4 µs per task.
- **Hello and registry hash.** Every driver and worker opens with a hello carrying its role and a hash of its registry, the task names in order. The head refuses a mismatch, so a cluster can't mix builds. Drivers get a number, which fills the top 24 bits of every object ID they create. That tells the head which driver owns a result.
- **The head's event loop** `poll`s the listener and every connection. Each connection is a `Conn`: reads and writes use `MSG_DONTWAIT`, input accumulates until frames are whole, and output queues until the socket takes it. A slow peer never blocks anyone else, so the phase-1 rule limiting pipelined argument size is gone.
- **Why `MSG_DONTWAIT` rather than `O_NONBLOCK`:** `fcntl` is variadic, and Apple's arm64 ABI passes variadic arguments on the stack. A plain `external_call` puts them in registers, so `F_SETFL` silently set nothing, and the head hung in its second `recv`. `external_call[..., num_fixed_args=2]` declares the variadic split and fixes it; memory-region calls `open` that way. dista sidesteps the question with per-call `MSG_DONTWAIT`.
- **Scheduling:** tasks queue in arrival order. Tasks waiting on inputs are held aside until every input exists. Queued tasks go to workers round-robin, up to `pipeline` in flight each (default 2). Each result or error goes back to the driver that owns it.
- **Small results go through the head; large ones stay on their worker.** A result of 64 KiB or less comes back inline. The head keeps it, because a later task may take it as input, and forwards it to the owning driver. A larger result stays on the worker that computed it. The worker reports only its size; the head records where it lives and tells the owner, using located frames. The driver's `get` fetches it straight from that worker.
- **Every worker is also an object server.** Its loop `poll`s the head, its listener, the drivers and workers fetching from it, and the workers it fetches from. A task whose inputs live elsewhere arrives as task-inputs, with each input either inline or as an address. It waits, parked, until every input has arrived, and the worker keeps serving fetches meanwhile. No worker waits on another synchronously, so two workers that need each other's results can't deadlock. Running a task blocks the loop, so a fetch from a busy worker waits until its current task ends.
- **On one host, large results go through shared memory.** A worker writes each large result into a file in the node store, through memory-region's `MappedRegion`. The store is `/dev/shm` on Linux, `$TMPDIR` on macOS, or `DISTA_STORE_DIR`. Files are named `dista-<token>-<object>`, where the token is random per worker. The location the worker reports holds its address, its host name (for diagnostics), the file's path and the exact size. A reader that can see that file maps it read-only instead of fetching it. With a random token in the name, a visible file is the right one. Host names would have been the obvious test, but on GitHub's macOS runners processes on one machine reported different ones. A worker copies the bytes into the task's argument; the driver's `get` decodes straight from the mapping. A reader on another host fetches over a socket. Workers delete their files when they exit. `DISTA_SHM=0` turns the store off. In Kubernetes each pod has its own `/dev/shm`, so pods use sockets unless they share a volume for `DISTA_STORE_DIR`. Measured: a 100 MB result goes from one worker to a second worker and to the driver in about 0.2 s, with no socket bytes.
- **A worker advertises the address it reaches the head from** (`getsockname` on that connection), or `DISTA_ADVERTISE` if set. The Kubernetes manifests set it to the pod IP.
- **Workers and drivers retry connecting to the head** for `DISTA_CONNECT_TIMEOUT` seconds (default 60), because in a cluster processes start in any order.
- **Kubernetes** runs one image for every role (`deploy/Dockerfile`): it builds the program with pixi and copies the binary and the Mojo runtime libraries it links into a slim Debian image, about 140 MB. `deploy/k8s/cluster.yaml` runs the head as a StatefulSet behind a headless Service `dista-head`, and the workers as a Deployment spread one per node. A driver is a Job with `DISTA_HEAD=dista-head:6390` (`deploy/k8s/driver-job.yaml`). `tests/kind.sh` runs `deploy/smoke.mojo` this way on a kind cluster of three worker nodes, in CI too. Tasks reach all three nodes, and large results cross pods over sockets.
- **A worker that dies** fails the tasks it had in flight, with `worker lost while running this task`. The results it held become `result lost: its worker went away`. The other workers carry on. Results are never freed yet: workers keep them until they exit.
- **Drivers use blocking sockets.** A driver talks to the head and to workers it fetches from, and both always drain their sockets.
- **Transport** is IPv4 TCP over libc in `src/dista/socket.mojo`: `getaddrinfo` for host names, and `TCP_NODELAY`. dista has no dependencies. flare's TCP works from source, but it brings native shims and its threads and json dependencies for something a few hundred lines of libc covers.

**Frames** are a 4-byte little-endian length, then a fixed 17-byte header and a payload:

| Offset | Field |
|---|---|
| 0 | kind |
| 1 | object ID, u64 |
| 9 | task ID, u64 (0 unless the kind uses it) |
| 17 | payload |

| Kind | Sent by | Payload |
|---|---|---|
| 1 task | driver to head, head to worker | the encoded argument |
| 2 result | worker to head, head to driver | the encoded result |
| 3 error | worker to head, head to driver | a UTF-8 message |
| 4 hello | driver or worker to head | role byte, then the registry hash, u64 |
| 5 welcome | head to driver | the driver's number, u64 |
| 6 task-after | driver to head | gather byte, count u64, then that many object IDs |
| 7 stored | worker to head | the result's location: it stayed on the worker |
| 8 located | head to driver | the result's location |
| 9 fetch | driver or worker to a worker | none: send that object back as a result frame |
| 10 task-inputs | head to worker | gather byte, count u64, then per input a tag: 0 with its bytes, or 1 with a location |

An address is 12 bytes: the IPv4 address, then the port as a u64. A worker's hello carries its own. A location is the holding worker's address, its host name, the node-store path ("" if the result is only in memory) and the size, each string length-prefixed.

**Measured** on an M4 MacBook, local mode with 4 workers (`pixi run bench`, 10,000 tasks of `square`): 11.0 µs per task with no pipelining, 6.2 µs at depth 2, 5.3 µs at depth 4. Worker-held results added about 1 µs per task, because each worker now polls its listener and peers between tasks. All tests pass on Mojo 1.2.0 and the 1.3 nightly. `tests/cluster.sh` runs a head, 4 workers and a driver as separate processes found by host name, and checks that a build with different tasks is refused.

### Still to come in phase 2

- **Freeing results.** Nothing is freed yet. Ray frees a result once its owner and every task that takes it are done with it.
- **Locality:** a task goes to the node that holds the most bytes of its arguments, falling back to the least-loaded node.

## Concurrency

A `poll` event loop, plus threads where work must overlap inside one process, not async. Mojo's async is unfinished: on the 1.3 nightly, `async`/`await` were renamed `__async`/`__await` (modular `b1bb00ad53`, October 2, 2026) to mark them unstable, while the compiler's coroutine lowering is being rebuilt. Task execution stays behind dista's own interface, so coroutines can replace threads later without changing the user API.

## Failure handling

- **Now:** a worker that dies fails the tasks it had in flight; `get` raises `worker lost while running this task`. No retry.
- **Phase 4:** lost objects are rebuilt by re-running the tasks that produced them (lineage), and actors restart from checkpoints or through restate.mojo.

## Testing

ThreadSanitizer fails on allocating threaded Mojo programs today, so the runtime is tested without it:

- **Deterministic unit tests** for the codec, the dispatcher and frame parsing.
- **Process tests:** fan-out and gather across real forked workers, with checked totals.
- **Fault injection:** a task that kills its worker, a task that raises, a frame cut short.
- **AddressSanitizer** on the test suite. Mojo's startup supports `SanitizeAddress`.

## Phase 1 deliverable

`remote` and `get` across forked workers on one machine. The vision's gate, a fan-out and gather of 1,000 tasks across 4 workers, passes. Layout:

| File | Holds |
|---|---|
| `src/dista/codec.mojo` | `encode`, `decode`, the `Codec` trait |
| `src/dista/task.mojo` | `Task`, `TaskLike`, task IDs, `dispatch` |
| `src/dista/frame.mojo` | frame encoding and decoding |
| `src/dista/socket.mojo` | loopback TCP, `fork`, `waitpid`, `_exit` over libc |
| `src/dista/runtime.mojo` | `Runtime`, `ObjectRef`, roles, the driver and worker loops |
| `src/dista/head.mojo` | the head's event loop and scheduler (phase 2) |
| `src/dista/conn.mojo` | non-blocking framed connections for the head (phase 2) |
| `src/dista/seq.mojo` | `Seq[T]` (phase 2) |
| `src/dista/worker.mojo` | the worker's event loop and object server (phase 2) |
| `src/dista/store.mojo` | the node-local shared-memory store and locations (phase 2) |
| `tests/` | codec, dispatch, and process tests |

## Phase 2 plan

Phase 2 takes dista from one machine to a cluster. Its gate, from the vision: a distributed Iceberg scan of the taxi data on a 3-node cluster returns the same answer as one machine.

**The gate passed on 2026-10-31.** `tests/kind_taxi.sh` runs `deploy/taxi.mojo` on a kind cluster of three worker nodes (colima on Apple silicon, arm64), with the 1.3 GB taxibench table copied to each node. All eight taxibench queries over 77.9 million trips, run one task per data file, equal the same scans run in one process: counts exactly, sums to 1e-9 relative. A ninth run returns the quarter's `total_amount` as Arrow batches, and 86.6 MB of them crossed nodes over sockets. Tasks ran on all three nodes. The distributed run took 2.0 s in total and the single process 3.9 s; it's one VM, so that measures parallelism, not a network. The table is too big for CI, so the script is run by hand. It lands as stacked PRs of at most about 2,000 lines, each with its own gate, in two tracks that can run in parallel.

**Runtime track** (dista.mojo, in order):

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **Event loop and pipelining.** `poll` over worker sockets, several tasks in flight per worker, results taken in any order. | A fast task's `get` returns while a slow task is still running. | ~400 |
| 2 | **Object refs as arguments, and `Seq[T]`.** A task can take another task's `ObjectRef`; it runs once its input exists. `Seq[T]` carries a list through the codec, since `List` can't be reflected. | A map-reduce: 100 maps feed one reduce. | ~500 |
| 3 | **A separate head, and roles.** One binary runs as head, worker or driver, chosen by environment. Workers and drivers reach the head over TCP by host name. A handshake checks that both sides share a registry. Local mode still forks everything. | Head, driver and 4 workers started as separate processes run the fan-out. | ~700 |
| 4 | **Results stay on workers.** A result stays where it was computed; the head records where it is. A consumer fetches it straight from that worker. | Map outputs reach the reduce without passing through the head. | ~700 |
| 5 | **A shared-memory store per node.** Workers on one node share results through memory-region instead of sockets. | Two workers on one node pass a 100 MB result without a socket copy. | ~600 |
| 6 | **Kubernetes.** A container image, manifests (a head StatefulSet, a worker Deployment) and a smoke test on a kind cluster in CI. | The fan-out runs on a 3-node kind cluster. | ~400 |

**Data track** (arrow-mlake.mojo, then dista.mojo):

| # | PR | Gate | Size |
|---|---|---|---|
| 7 | **Arrow IPC in arrow-mlake.** Move the writer from flight.mojo, add a reader, and cover every `ArrayData` type, nested ones included. | pyarrow reads what it writes, and it reads what pyarrow writes. | ~1,500 |
| 8 | **`RecordBatch` results in dista.** Columnar results travel as IPC, in the node store and between nodes. | A task returns a batch read by parquet.mojo, and another task consumes it. | ~500 |
| 9 | **The Iceberg scan.** One task per data file, results gathered as Arrow. | Phase 2's gate. | ~400 |

PR 7 changes a published tin, and needs your sign-off before it's merged.

## Phase 3 plan

Phase 3 adds actors: long-lived structs that keep state between calls. Its gate, from the vision: a long-lived actor survives its worker restarting, with its state restored.

It starts with two things the cluster runs need before they're measured again: results are freed, and the head can restart. Both sit on the hot path, so the next AWS runs should include them.

### The head, version 0

Today the head is one process holding everything: the queue, where each result lives, which workers exist. If it dies, the job dies with it.

**Version 0 makes the head's state rebuildable instead of durable.** Nothing it holds is written to disk. When Kubernetes restarts it:
- **Workers rebuild the directory.** Each worker reconnects (they already retry for `DISTA_CONNECT_TIMEOUT`) and reports the results it holds and, once actors exist, the actor it hosts with its last call number. The head rebuilds the directory from those reports.
- **The driver rebuilds the queue.** It reconnects and resubmits every task it hasn't got a result for. Tasks with a result on a worker aren't run again.
- **Running tasks may run twice.** A task that was running when the head died can be resubmitted and run again, so tasks have to be safe to repeat, which they are for a pure function.

That covers a head restart, not a head that's lost for good with the driver. Replicating the head is later work.

**Throughput.** The head is a single-threaded `poll()` loop. `bench/fanout.mojo` grows a mode that measures tasks per second through the head with 16, 64 and 192 workers, empty tasks and 1 MB results. That shows whether the head limits the 12-node runs, before the actor work adds traffic to it.

### How actors work

**Declared at compile time, like tasks.** dista can't load code at runtime, so an actor is a state type and its constructor, and each method is declared once:

```mojo
comptime Counter = Actor[CounterState, CounterArgs, new_counter]
comptime Add = Method[Counter, Int, Int, add]           # def add(mut s: CounterState, n: Int) raises -> Int

var c = rt.create[Counter](CounterArgs(start=0))        # an ActorHandle
var r = rt.call[Add](c, 5)                               # an ObjectRef[Int], as for a task
```

Actors and methods go in `Runtime[...]` with the tasks, so the registry hash still proves every process runs the same code.

**One actor, one worker process.** The head places an actor on a worker that hosts no other, the one with the fewest tasks in flight, and that worker serves only that actor until the actor is killed. An actor created when every worker hosts one waits for a worker to join or another actor to be killed. This is Ray's model. It keeps a single-threaded worker simple: the actor's state is owned by one process, and nothing else runs beside it.

**Calls run one at a time, in order.** The head sends each actor's calls to its worker as they arrive, on one connection, so they keep the driver's order. The worker runs them one after another. A method's result is an `ObjectRef` like a task's: it stays on the worker if it's large, and it can be another task's argument.

**Handles are values.** An `ActorHandle` goes through the codec, so a task or another actor can call methods. Calls from workers go through the head, which keeps the order.

**Restart.** When an actor's worker goes away, the head marks the actor restarting and places it on another worker. Kubernetes restarts the pod, which rejoins as a new worker. The actor comes back from its last checkpoint, or from its constructor if it has none, and the calls since then are replayed.

**Checkpoints.** A checkpoint is the actor's state encoded by the codec, with the number of the last call it reflects. A state type can implement `Codec` itself to save only what it can't rebuild: a cache can save nothing, and a loaded model can save its path. Checkpoints are written through objectstore to `DISTA_CHECKPOINT_URI`, a local directory or `s3://`, so they outlive the node. The head keeps each call's arguments until a checkpoint covers it.

### Defaults to confirm

| Question | Default | Alternative |
|---|---|---|
| When to checkpoint | Every N calls (per actor, `checkpoint_every`), plus an explicit `rt.checkpoint(handle)` | Only explicit, or only on a timer |
| Calls in flight when the worker dies | Replayed after the restore: at-least-once, so a method should be safe to run twice | Failed with an error: at-most-once |
| Concurrency | One call at a time per actor | Concurrent read-only methods |
| Freeing results | Reference counts at the head: the driver's hold plus the pending tasks that take a result. `rt.release(ref)` drops the driver's hold. | Free on `ObjectRef` destruction, which needs a non-copyable ref |

### PRs

Stacked, at most about 2,000 lines each, in order. Through PR 7 a head restart loses the actors: the resuming driver fails their unfinished calls, and a rejoining worker destroys its actor. A worker's death doesn't, from PR 6. Surviving a head restart comes with PR 7b: the call log that rebuilds an actor is the head's, so it goes with the head, and the creating driver only knows its own calls, not those from tasks and other actors. A checkpoint in object storage, with the calls since it, is what a new head can rebuild from:

| # | PR | Gate | Size |
|---|---|---|---|
| 1 | **Freeing results.** Reference counts at the head; `rt.release`; workers delete a freed result from memory or the node store. | A loop of 10,000 tasks with 1 MB results finishes with the stores empty. | ~500 |
| 2 | **Head restart, version 0.** Workers re-register what they hold; the driver resubmits what it hasn't got; head throughput in `bench/fanout.mojo`. | The head is killed mid-fan-out on kind; the job finishes with every answer right. Tasks per second with 16, 64 and 192 workers are recorded. | ~600 |
| — | **Fuzzing and a chaos test** (#35, done). mozz harnesses for frames, the codec, payloads and the head's protocol; `tests/chaos.sh` kills the head at random under a random task graph. Five parser bugs and four restart races found and fixed. | — | ~1,300 |
| 3 | **A task timeline.** Head and worker timestamps per task; `rt.timeline(path)` writes Chrome's trace format. | A fan-out's trace opens in Perfetto with one track per worker, and every task's span sits inside its submit and settle. | ~400 |
| 4 | **Actors: declare, place, call.** `Actor`, `Method`, `rt.create`, `rt.call`, `rt.kill`; one worker per actor; ordered calls; results as `ObjectRef`s. | A counter actor takes 1,000 calls from the driver and returns 1,000 in order; a task uses a method's result. | ~900 |
| 5 | **Handles as values** (done). `ActorHandle` through the codec; `CtxTask` and `CtxMethod` get a `Client`, the driver's side of the protocol, which a worker opens to the head as `ROLE_CLIENT`; calls from tasks and other actors go through the head like a driver's. | 100 tasks each call one shared actor; the actor sees every call once. | ~400 |
| 6 | **Restart from the constructor** (done). The head keeps every call made to an actor; when its worker goes away it places the actor elsewhere, builds it again and replays every call in order. Each call is answered once: a second answer is dropped. | The counter's worker is killed after 500 of 1,000 calls; the final count is still 1,000. | ~600 |
| 7 | **Checkpoints** (done). `checkpoint_every` on `rt.create`, and `rt.checkpoint`, whose result is the calls covered; state through the codec, through objectstore to `DISTA_CHECKPOINT_URI`; a restart restores from the last checkpoint and replays the calls since; the head drops calls a checkpoint covers, which bounds the call log. The worker hosting an actor deletes the checkpoints behind the one the head would restore from. | Phase 3's gate: on kind, the counter's pod is deleted mid-run; the actor restores from its checkpoint on another node and the count is exact. | ~700 |
| 7b | **Actors across a head restart.** A rejoining worker keeps the actor it hosts and reports it with its call count; a driver that resumes sends back its actors, with their checkpoint, and resends their unfinished calls. A call a checkpoint covers whose answer died with the head fails rather than run twice. | The head is killed during the counter's calls; the count is exact. | ~500 |
| 8 | **Prometheus metrics.** `/metrics` on the head and every worker: task counters, queue depth, objects and store bytes, actors, a task-duration histogram. | `curl` on each pod's `/metrics` during a kind run shows tasks done climbing to the total. | ~500 |

### Telemetry

Ray exports Prometheus metrics from every node, runs a dashboard on the head, and dumps a task timeline in Chrome's trace format (`ray timeline`). dista has stdout and `Runtime.stats`. Two pieces, in the order they pay off:

- **A task timeline** (PR 3), before the actor work, so the next AWS runs have it. The head stamps each task's submit, dispatch and settle times, worker and node; the worker adds its start and end, split into fetching inputs and running. `rt.timeline(path)` writes Chrome's trace format, which Perfetto and the browser's trace viewer open. It shows where a slow run spends its time: dispatch at the head, waiting on inputs, S3 reads inside a task, or stragglers.
- **Prometheus metrics** (PR 8), once actors exist. A `/metrics` endpoint on the head and each worker. Counters: tasks submitted, done and failed; bytes fetched. Gauges: queue depth, objects held, node-store bytes, actors. A histogram: task duration. It's plain text over a minimal HTTP responder on dista's sockets, and Kubernetes monitoring scrapes it as it is.

OpenTelemetry tracing waits for the pipeline work, where one trace should cover the orchestrator, the driver and its tasks. OTLP over HTTP with JSON needs no gRPC, and can go out through objectstore's libcurl client.

Restate stays out of phase 3. It's the candidate for durable workflows built on actors later, and for phase 4's recovery.

## Order of work, from 2026-11-01

1. **Measure on one host and on many**, to check the performance is where it should be. The one-process references run on 16 threads and on 1 thread with best settings (each framework one decoding thread with 8 files' reads in flight). Then the 3, 6 and 12 node matrix. Every number goes into `bench/cloud/results/NUMBERS.md`.
2. **Fix what those runs turn up**: bugs and performance regressions.
3. **Finish the actors** (PRs 5 to 7 above) and metrics (PR 8), then the remaining phases.

### To do: what a shuffle needs

The taxi benchmark is mostly reading Parquet from S3. A second benchmark should spend its time in user code and in moving data between tasks: per-trip features in each framework's own code, a group-by on pickup zone × dropoff zone × hour (about 1.7 million groups, too many to send to the driver) as map tasks partitioning by key and reduce tasks taking their partition from every map task, then a top-k. dista doesn't have what that needs yet. Ray has all of it:

| | What's missing | Why a shuffle needs it |
|---|---|---|
| 1 | **Several results from one task.** A `Task` that declares N results; `remote` returns N `ObjectRef`s, each stored and fetched on its own (Ray's `num_returns`). | A map task makes one partition per reducer. With one result per task, each reducer fetches every map task's whole output, or each partition is a separate task that re-reads its input. |
| 2 | **Fetching part of an object.** A reducer fetches element *j* of a stored `Seq` without the rest. | The alternative to 1 when a task's partitions are one value. |
| 3 | **Refs inside arguments.** A task's argument can mix values and `ObjectRef`s, and the refs resolve before it runs. Today a task takes either one value, one ref or a list of refs. | "Reduce partition *j* over these map outputs" is a number and a list of refs. |
| 4 | **Placement by data location.** A task goes to the node holding most of its inputs' bytes, rather than round-robin. | Reducers read megabytes from every node; sending them where the bytes are halves the traffic or better. |
| 5 | **Spilling.** Results that don't fit in memory or `/dev/shm` go to local disk or object storage. | A shuffle's intermediate data can be larger than memory. |

1 and 3 come first, before the second benchmark; 4 and 5 once it shows they matter.

### To do: a `dista` command for operating a cluster

With the metrics (PR 8), as a binary of its own. Today one binary runs as head, worker or driver, but only the workers and drivers run the user's code: the tasks and actors, and whatever those depend on (Arrow, Iceberg, a model's library). The head routes frames and checks registry hashes. So the head and the command line go into a generic `dista` binary that depends on dista's core alone, released and versioned on its own, while the user's binary stays the driver and the worker. One head can then serve jobs built from different code, told apart by their registry hashes.

| Command | What it does |
|---|---|
| `dista head` | The head, as `DISTA_ROLE=head` runs it today from the user's binary. |
| `dista status` | The head's view: workers, actors, objects held, queue depth (the stats frame, extended). |
| `dista timeline -o trace.json` | Every driver's finished tasks, not only the caller's own. |
| `dista stop` | Asks the head to shut down, and its workers with it. |

`status` and `timeline` need an observer role: a peer that says hello to the head and asks, without being a driver that owns objects, and without a registry hash to check. A head that serves several jobs checks each driver's hash against the workers it hands that driver's tasks to, rather than against its own. The benchmark's own command, `bench/cloud/bench`, is separate: it runs the AWS clusters and the frameworks on them.

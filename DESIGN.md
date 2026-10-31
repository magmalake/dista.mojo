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
| Same node | IPC message written once into the node's shared-memory store; readers map it | 2 |
| Between nodes | Arrow Flight, sending the same bytes | 2 |
| Spill to object storage | the same bytes as `.arrow` files, readable by pyarrow, DuckDB, Polars | 4 |
| Phase 1 (one machine) | IPC bytes over the task socket | 1 |

- **One object per batch**, roughly one per Parquet row group, so a scan task returns one `ObjectRef` per batch.
- **Work needed in the tins:**
  - Move `ipc.mojo` from flight.mojo into arrow-mlake, add a reader, and cover every `ArrayData` type, including nested types.
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
- **A worker advertises the address it reaches the head from** (`getsockname` on that connection), or `DISTA_ADVERTISE` if set. In Kubernetes that will be the pod IP.
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

Phase 2 takes dista from one machine to a cluster. Its gate, from the vision: a distributed Iceberg scan of the taxi data on a 3-node cluster returns the same answer as one machine. It lands as stacked PRs of at most about 2,000 lines, each with its own gate, in two tracks that can run in parallel.

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

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
- **Gap: `List[T]` and other generic containers.** Reflection can't recover a `List`'s element type, so phase 1 doesn't encode lists. Workarounds: a `Codec` wrapper type, or Arrow for anything large. Revisit if Mojo adds conditional conformance on `List`.
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

### Phase 1: one machine

- **The driver is the head.** `Runtime(...)` binds a TCP listener on `127.0.0.1` with an OS-assigned port, then forks the workers. Forking before any thread exists is safe, and it means a worker has the registry without an exec.
- **A worker** connects back, then loops: read a task frame, dispatch, write a result frame. It exits on end-of-file.
- **Scheduling** is a FIFO queue. `remote` only enqueues. `get` pumps: hand queued tasks to idle workers, then read the result of the oldest in-flight task. Workers compute in parallel while the driver waits on one of them, because finished results wait in socket buffers. No `poll` and no threads in phase 1.
- **Transport** is loopback TCP over libc (`socket`, `bind`, `connect`, `send`, `recv`) in `src/dista/socket.mojo`, with `TCP_NODELAY` set. dista has no dependencies. flare's TCP would work from source, but it brings flare's native shims and its threads and json dependencies for something a hundred lines of libc covers.
- **Frames** are a 4-byte little-endian length, then a fixed 17-byte header and a payload:

| Offset | Field |
|---|---|
| 0 | kind: 1 task, 2 result, 3 error |
| 1 | object ID, u64 |
| 9 | task ID, u64 (0 except on a task frame) |
| 17 | payload: the encoded argument, the encoded result, or a UTF-8 error message |

- **Measured** on an M4 MacBook with 4 workers: start-up 2.4 ms, one task round trip about 0.05 ms once warm, and 1,000 tasks of `square` in 10.8 ms, about 11 µs each. That's well under the vision's 1 ms per-task target. The phase-1 gate test passes on both Mojo 1.2.0 and the 1.3 nightly.

### Phase 2: a cluster

- **A separate head process** (a Kubernetes StatefulSet) owns membership, the task queue and the object directory. Workers (a Deployment) find it through `DISTA_HEAD=host:port`; the same binary acts as head, worker or driver depending on its environment.
- **The scheduler moves onto a `poll` event loop**, over the same libc socket layer, so the head serves many workers without blocking on one, and so tasks can be pipelined per worker.
- **Ownership** follows Ray: the submitter owns a result and holds its location. The head knows where objects live but never holds their bytes.
- **Locality:** a task goes to the node that holds the most bytes of its arguments, falling back to the least-loaded node.

## Concurrency

A `poll` event loop, plus threads where work must overlap inside one process, not async. Mojo's async is unfinished: on the 1.3 nightly, `async`/`await` were renamed `__async`/`__await` (modular `b1bb00ad53`, October 2, 2026) to mark them unstable, while the compiler's coroutine lowering is being rebuilt. Task execution stays behind dista's own interface, so coroutines can replace threads later without changing the user API.

## Failure handling

- **Phase 1:** a worker that dies closes its socket. `get` on any of its in-flight tasks raises. No retry.
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
| `src/dista/runtime.mojo` | `Runtime`, `ObjectRef`, the worker loop |
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

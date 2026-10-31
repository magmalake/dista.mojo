# dista vs Ray vs Rust on the taxi gate

The same program three times: taxibench's eight queries over the NYC taxi
Iceberg table (79.5M trips, 24 monthly files, 1.3 GB). Each query runs as one
task per data file, folded where it's read, and is then checked against the
same scan in one process.

| | program | Iceberg reader | distribution |
|---|---|---|---|
| dista | [deploy/taxi.mojo](../deploy/taxi.mojo) | iceberg.mojo | dista: a head, workers, `ObjectRef`s |
| Ray | [ray/taxi_ray.py](ray/taxi_ray.py) | PyIceberg + pyarrow, with taxibench's own folds | Ray 2.59 tasks |
| Rust | [rust/src/main.rs](rust/src/main.rs) | iceberg-rust 0.10.1 + arrow 58 | tokio TCP, driver to workers directly |

All three run on the same kind cluster in one session: three worker nodes,
one worker per node, the warehouse copied to each node. dista and Ray also
have a head that runs no tasks; the Rust version has no head at all. Every
node is a container in one colima VM on an Apple-silicon Mac (arm64, 8
CPUs), so the times measure parallelism and per-task overhead, not a
network. These are single runs on 2026-10-31.

```
tests/kind_taxi.sh                          # dista
TAXI_THREADS=1 bench/ray/run_kind.sh        # Ray, 1 pyarrow thread per process
TAXI_THREADS= bench/ray/run_kind.sh         # Ray, pyarrow's default threads
TAXI_THREADS=1 bench/rust/run_kind.sh       # Rust, single-threaded runtimes
TAXI_THREADS= bench/rust/run_kind.sh        # Rust, default runtimes and limits
```

## Distributed, 3 nodes (ms)

| query | dista | Rust, 1 thread | Rust, default | Ray, 1 thread | Ray, default |
|---|---:|---:|---:|---:|---:|
| q1 count, full scan | 135 | 325 | 318 | 1,631 | 1,719 |
| q2 one quarter | 20 | 48 | 54 | 91 | 91 |
| q3 cash fares | 173 | 237 | 236 | 453 | 444 |
| q4 tip ratio | 285 | 367 | 374 | 563 | 611 |
| q5 zone counts | 312 | 366 | 362 | 582 | 673 |
| q6 zone revenue | 176 | 220 | 220 | 432 | 479 |
| q7 one month, all columns | 217 | 241 | 265 | 455 | 468 |
| q8 selective | 302 | 340 | 352 | 543 | 633 |
| **total** | **1,620** | **2,144** | **2,181** | **4,749** | **5,117** |
| total without q1 | 1,485 | 1,819 | 1,863 | 3,118 | 3,399 |
| q2 as Arrow batches | 171 | 200 | 212 | 321 | 217 |

## One process (ms)

The same scans run in the driver, with no distribution at all.

| query | iceberg.mojo | iceberg-rust, 1 thread | iceberg-rust, default | PyIceberg, 1 thread | PyIceberg, default |
|---|---:|---:|---:|---:|---:|
| **total** | 3,858 | 4,883 | 5,123 | 5,886 | **2,931** |

## Image size

| image | on disk | compressed (pushed / pulled) |
|---|---:|---:|
| `dista:smoke`: dista alone ([deploy/Dockerfile](../deploy/Dockerfile)) | 140 MB | 29 MB |
| `dista-rust:taxi`: the Rust program | 159 MB | 35 MB |
| `dista:taxi`: dista + iceberg + parquet + codecs + SQL shims | 266 MB | 65 MB |
| `dista-ray:taxi`: Ray + PyIceberg + pyarrow, trimmed | 840 MB | 206 MB |
| `rayproject/ray:2.59.0-py312-cpu`, arm64, for reference | | 921 MB |

All but the Ray images are `debian:bookworm-slim` (about 28 MB compressed)
plus the program. The Rust binary is 17 MB and links only libc, libm and
libgcc_s, which the base image has. `dista:taxi` is a 3 MB binary plus 87 MB of shared libraries it
links or loads; the largest are ICU, libstdc++ and OpenSSL, which libpq and
libcurl bring in, and most of that could go. `dista-ray:taxi` is
`python:3.12-slim` plus a venv in which Ray takes 200 MB and pyarrow 140 MB.
Tests, headers and bytecode are removed, the way taxibench builds its
PyIceberg image.

## What the numbers say

- **dista is fastest distributed**, about 1.3× ahead of Rust and 2× to 3×
  ahead of Ray. Most of the gap to Rust is in the reader, not the
  distribution. In one process, iceberg.mojo takes 3.9 s and iceberg-rust
  4.9 s, and the distributed totals keep roughly that ratio.
- **Ray pays for starting workers and for Python.** Ray starts a worker
  process lazily, on the first task, and that process imports pyarrow and
  PyIceberg; that's most of q1's 1.6 s. Without q1, Ray is about 2.1× dista.
  The rest is per-task Python: reopening the table and re-planning the scan
  in every task costs more in PyIceberg than in Mojo or Rust.
- **Threads change only PyIceberg.** One pyarrow process with its default
  threads is the fastest single-process run (2.9 s), because pyarrow decodes
  columns in parallel. iceberg-rust's defaults don't speed up a single
  process here, and iceberg.mojo's scan runs one thread by default
  (`ScanOptions.num_workers = 1`). In the distributed runs every worker does
  one task at a time either way, so threads matter much less there.
- **The answers agree everywhere.** Counts are identical. Sums differ in the
  tenth significant digit (299443390.9194437, 299443390.9199999,
  299443390.91999996), because the terms are added in different orders.

## Differences between the three

- **Pipelining:** dista sends a worker up to two tasks at a time; Ray
  schedules one per CPU; the Rust driver keeps one in flight per worker.
- **Paths:** the dista pods mount the warehouse at `/data/wh` and rebase the
  table's file locations onto it. The Ray and Rust pods mount it at the path
  it was written at, because PyIceberg and iceberg-rust read manifests by
  absolute location.
- **Shared memory:** the Ray pods get a 2 GiB in-memory `/dev/shm` for Ray's
  object store. The dista and Rust pods use the default 64 MB.

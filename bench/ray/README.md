# dista vs Ray on the taxi gate

The same program twice: taxibench's eight queries over the NYC taxi Iceberg
table (79.5M trips, 24 monthly files, 1.3 GB). Each query runs as one task
per data file, folded where it's read, and is then checked against the same
scan in one process. [deploy/taxi.mojo](../../deploy/taxi.mojo) is the dista
version, built on iceberg.mojo. [taxi_ray.py](taxi_ray.py) is the Ray
version, built on PyIceberg and pyarrow, with taxibench's own Python folds.

Both run on the same kind cluster: three worker nodes, one worker pod per
node, the warehouse copied to each node, and a head that runs no tasks.
Every node is in one colima VM on an Apple-silicon Mac (arm64, 8 CPUs), so
the times measure parallelism and per-task overhead, not a network. These
are single runs on 2026-10-31.

```
tests/kind_taxi.sh                          # dista
TAXI_THREADS=1 bench/ray/run_kind.sh        # Ray, one pyarrow thread per process
TAXI_THREADS= bench/ray/run_kind.sh         # Ray, pyarrow's default threads
```

## Duration (ms)

| query | dista, 3 nodes | Ray, 3 nodes, 1 thread | Ray, 3 nodes, default threads | iceberg.mojo, 1 process | PyIceberg, 1 process, 1 thread | PyIceberg, 1 process, default threads |
|---|---:|---:|---:|---:|---:|---:|
| q1 count, full scan | 142 | 1,714 | 1,686 | 297 | 711 | 320 |
| q2 one quarter | 23 | 123 | 100 | 48 | 152 | 58 |
| q3 cash fares | 181 | 468 | 481 | 448 | 848 | 316 |
| q4 tip ratio | 288 | 641 | 579 | 715 | 1,122 | 370 |
| q5 zone counts | 313 | 676 | 697 | 835 | 1,121 | 776 |
| q6 zone revenue | 224 | 502 | 477 | 475 | 1,109 | 564 |
| q7 one month, all columns | 214 | 548 | 518 | 207 | 383 | 172 |
| q8 selective | 316 | 632 | 635 | 761 | 1,140 | 387 |
| **total** | **1,701** | **5,304** | **5,175** | **3,786** | **6,586** | **2,964** |
| total without q1 | 1,559 | 3,590 | 3,489 | 3,489 | 5,875 | 2,644 |
| q2 as Arrow batches | 163 | 312 | 222 | | | |

- **Ray's q1 includes starting its worker processes.** Ray starts a worker
  process lazily, on the first task, and that process then imports pyarrow and
  PyIceberg. "Total without q1" leaves that out. dista's workers are the
  long-running pods themselves, so there is nothing to start.
- **One task at a time per node in both.** Ray workers have `--num-cpus=1`,
  and a dista worker runs its tasks one after another. With one pyarrow
  thread, each task is single-threaded in both. With pyarrow's default
  threads, Ray's tasks can use more cores than dista's.
- **In one process, PyIceberg with threads is the fastest.** pyarrow decodes
  Parquet columns in parallel, and iceberg.mojo's `ScanOptions.num_workers`
  defaults to 1, so it runs one thread here. Held to one thread, PyIceberg
  takes 6.6 s against iceberg.mojo's 3.8 s.
- **Answers agree everywhere.** Counts are identical. Sums differ in the
  tenth significant digit (299443390.9194437 vs 299443390.91999996), because
  the terms are added in different orders.

## Image size

| image | on disk | compressed (pushed / pulled) |
|---|---:|---:|
| `dista:taxi`: dista + iceberg + parquet + codecs + SQL shims | 266 MB | 65 MB |
| `dista:smoke`: dista alone (deploy/Dockerfile) | 140 MB | 29 MB |
| `dista-ray:taxi`: Ray + PyIceberg + pyarrow, trimmed | 840 MB | 206 MB |
| `rayproject/ray:2.59.0-py312-cpu`, arm64, for reference | | 921 MB |

`dista:taxi` is `debian:bookworm-slim` plus one 3 MB binary and 87 MB of the
shared libraries it links or loads. The largest are ICU, libstdc++ and
OpenSSL, which come in with libpq and libcurl. `dista-ray:taxi` is
`python:3.12-slim` plus a venv in which Ray takes 200 MB and pyarrow 140 MB,
with tests, headers and bytecode removed, the same way taxibench builds its
PyIceberg image.

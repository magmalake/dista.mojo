# The taxi benchmark on AWS

[../README.md](../README.md) compares dista, Ray and Rust on a kind cluster:
three containers in one VM, a 1.3 GB table copied to each, two seconds of
work. That shows per-task overhead and little else. This directory runs the
same three programs where a real deployment would: separate EC2 machines,
the table in S3, and enough data that a run takes minutes rather than
seconds. Numbers worth publishing come from here, not from kind.

## What's measured

**The table.** NYC yellow taxi trips 2011–2024: about 1.5 billion rows and
20 GB of Parquet, written to S3 by PyIceberg with its default layout, one
append per month (168 snapshots). [load_table.py](load_table.py) is
taxibench's loader pointed at S3 and at more years, with the same schema and
the same rules. 2011 is where TLC's files start using today's column names.

**The queries.** taxibench's eight, unchanged. Five scan all 14 years; q2, q6
and q7 select months of 2024, so partition pruning keeps them small.

**The cluster.** k3s on Ubuntu 24.04, in one subnet of the default VPC in
us-east-2. One head node runs the dista head, the Ray head, every driver and
the image builds. 3, 6 or 12 worker nodes run tasks. Every node is the same
Graviton instance, c7g.4xlarge by default (16 vCPU, 32 GiB, up to 15 Gbit/s).

**Equal parallelism.** Each worker node runs `SLOTS` single-threaded tasks at
once (default: its vCPUs). That's `SLOTS` dista worker pods, `SLOTS` Rust
worker pods, or one Ray worker with `--num-cpus=SLOTS`. Every reader runs with
one thread: pyarrow's CPU and I/O pools and iceberg-rust's limits are 1.

**Planning happens once.** The driver plans each query and ships every task
its planned file: dista and Ray send the file's planned task, and Rust sends
iceberg-rust's serialized `FileScanTask`. A dista or Ray task reads the
table's metadata file, then its data file; a Rust task reads only its data
file, because iceberg-rust's task carries the schema. No task reads a
manifest. On kind every task re-planned, which was cheap with local files.
On S3, with 168 manifests, it would be a request per manifest per task, and
that would swamp everything else.

**Repetitions.** Every query runs `PASSES` times (default 5) on each
framework and node count. The tables report each query's median, and the
median and slowest of the per-pass totals. Pass 1 is checked against a scan
in one process on the head node; later passes are checked against pass 1.
Ray's pass 1 includes starting its worker processes.

**Building.** [build.sh](build.sh) builds each image on the head node from
an empty Docker cache, base images included, and records the whole build,
the dependency step, the compile step and the image size:

| image | dependency step | compile step |
|---|---|---|
| dista | `pixi install` (Mojo toolchain and the data environment) | `mojo build` |
| Rust | `cargo build` of the dependencies, against a stub crate | `cargo build` of the crate |
| Ray | `pip install` of Ray and PyIceberg | none |

## Running it

The `dista-bench` AWS profile has to be signed in (`aws login --profile
dista-bench`), with a vCPU quota for the instances: 13 × 16 = 208 vCPUs of
"Running On-Demand Standard instances" in us-east-2 for the full matrix, or
of spot instances with `SPOT=1`.

```sh
bench/cloud/aws.sh up 0              # the head node alone: ~3 minutes
bench/cloud/data.sh load             # the table, once: it stays in S3
bench/cloud/aws.sh up 12             # add the workers
bench/cloud/build.sh                 # timed builds, loaded into every node
bench/cloud/run.sh                   # 3 x 3 runs, 5 passes each
bench/cloud/aws.sh down              # terminate everything but the bucket
```

`NODES`, `FRAMEWORKS`, `PASSES` and `SLOTS` narrow or change the matrix. For
example, `NODES=3 FRAMEWORKS=dista PASSES=1 bench/cloud/run.sh` is a smoke
test. `data.sh drop` deletes the bucket.

The pods read S3 with the static `AWS_*` variables. run.sh copies this
machine's current credentials into a Kubernetes Secret before each run, and
`aws login` credentials last 12 hours. Instances have no IAM role:
objectstore.mojo doesn't read instance credentials yet.

## Reproducing a published run

**What gets built is pinned.** build.sh builds dista.mojo at the commit
that's checked out, and refuses to build one that's uncommitted or not
pushed. Its siblings are built at the commits in [revs.txt](revs.txt): the
Mojo libraries, and taxibench.example for Ray's queries. The head node
clones them all fresh before any timing starts, so nothing from the machine
running the scripts goes into an image. The Rust crates are pinned by
`bench/rust/Cargo.lock`, Ray and PyIceberg by version in
`bench/ray/Dockerfile`, and the Mojo toolchain by dista's `pixi.lock`. The
base images are pinned by tag.

**Every run is kept.** build.sh, data.sh and run.sh write into
`bench/cloud/results/<UTC date>/`, or `$RESULTS`. That folder is committed
with the post that uses it:

| file | what |
|---|---|
| `revs.txt` | every repository and commit that was built |
| `meta.txt` | date, Region, the nodes with their instance types, kubelet and kernel versions, the table's metadata location, passes, slots |
| `load.log` | the table load, month by month, with row counts |
| `build.tsv`, `build-*.log.gz` | the timed builds and their full BuildKit logs |
| `<framework>-<nodes>.log` | each run's output, every `TIME` line included |
| `summary.md` | the tables, from summarize.py |

To check a published number, rebuild at that folder's `revs.txt` and run the
same matrix. To rework the tables from the raw logs, run
`python3 bench/cloud/summarize.py bench/cloud/results/<date>`.

## What it costs

c7g.4xlarge is about $0.58 an hour on demand in us-east-2. The full matrix
keeps 13 instances up for roughly an hour, so builds and runs cost about
$7.50. Loading the table keeps the head node up for about an hour, about
$0.60. 20 GB in S3 costs about $0.50 a month. Traffic between EC2 and S3 in
one Region is free. Spot instances usually cost a third of that or less, but
can be reclaimed mid-run.

Every instance powers itself off after `TTL_MINUTES` (default 240) and is
terminated when it does, so a cluster nobody remembered to take down stops
billing on its own.

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-11-13

### Added

- **Adaptive in flight under `DISTA_WORKER_SLOTS=auto`** ([#123](https://github.com/morca-dev/dista/issues/123)):
  a slow start from one task, back-off on PSI memory pressure, spills or
  slower tasks, and task sizes learned from tasks run alone.
- **`DISTA_WORKER_SLOTS=auto`** ([#123](https://github.com/morca-dev/dista/issues/123)):
  slots and a memory budget from the run's grant (`MORCA_GRANT_CPUS`,
  `MORCA_GRANT_MEMORY_MB`); past the budget, tasks wait at the head.

### Changed

- The head breaks dispatch ties by the most room, then by connection order.

## [0.4.0] - 2026-11-11

### Added

- **A cluster token** ([#124](https://github.com/magmalake/dista.mojo/issues/124)):
  with `DISTA_TOKEN` (or `DISTA_TOKEN_FILE`) set, the head and every worker
  refuse a connection that doesn't open with it. Unset, nothing changes.

## [0.3.0] - 2026-11-05

### Added

- **`DISTA_WORKER_SLOTS`: a worker runs several tasks at once**
  ([#120](https://github.com/magmalake/dista.mojo/issues/120)). With N above
  1 (0 for one per core) a worker runs up to N tasks on threads of its own
  while its event loop keeps serving fetches, inputs and results; the head
  sends it N plus `pipeline - 1` tasks. Default 1: no threads, and the same
  behaviour and hello as before. A dead worker's running tasks all run
  again, cancel, timeouts and drain hold per task, and an actor's calls
  still run one at a time. `/metrics` shows `dista_worker_slots`,
  `dista_worker_tasks_running` and the head's `dista_slots`;
  `dista status --json` each worker's `slots`. N slots hold N tasks' inputs.
  dista now depends on threads-mojo (0.6.1).

## [0.2.2] - 2026-11-04

The merge trains #106 and #109 (#107 large results in part frames, #108
keepalive and `DISTA_TASK_TIMEOUT`, and the purge/cancel fixes), #110 (one
copy per hop on the fetch path), and the S3 multipart fix.

### Changed

- Built against objectstore-mojo 0.4.2 (objectstore-shim 0.2.1), the S3
  multipart fix. The stable, nightly and data environments and the CI
  checkouts pin the same revision.

### Fixed

- **A task fetching an input from a worker that dies fails** with
  `input lost: the worker holding it (<ip:port>) went away`, instead of
  waiting forever. The fetching worker drops the dead connection: it used to
  poll it at 100% CPU, or die of the reset itself
  ([#89](https://github.com/magmalake/dista.mojo/issues/89)).
  `tests/peer_loss.sh` kills a worker while two others fetch from it.
### Added

- **Silent peers are noticed.** Every socket has TCP keepalive: a worker,
  head or driver whose host froze or dropped off the network fails its
  connections after about 25 s (`DISTA_KEEPALIVE_SECONDS`, default 10 s of
  silence, then 3 probes; 0 turns it off). The head then drops the worker
  and runs its tasks again, and a worker rejoins `DISTA_HEAD`, a
  replacement head included. **`DISTA_TASK_TIMEOUT`** (seconds; unset, no
  limit) fails a task still running that long after it was sent, and
  `/metrics` counts them in `dista_tasks_timed_out_total`
  ([#90](https://github.com/magmalake/dista.mojo/issues/90)).
- **`rt.take(ref)`**: `get`, then `release`, for a result read once. A
  small result is decoded from the bytes it drops rather than a copy;
  `rt.take(refs)` takes a list in order. `get` and `release` are unchanged
  ([#98](https://github.com/magmalake/dista.mojo/issues/98)).
- **What each driver holds.** `dista status` prints the objects each driver
  holds, `--json` adds `driver_objects` (driver, held, client), `/metrics`
  adds `dista_driver_held_objects_max`, and the head logs a driver holding
  `DISTA_WARN_HELD` objects (default 100,000; 0, never), and again at each
  doubling.

### Changed

- **Large results are copied once per hop, and served a window at a time.**
  A frame's payload is read straight into a buffer of its own, and the
  driver keeps it as the result instead of copying it. A worker serving a
  fetch sends from its node-store file's mapping, or from memory, 1 MiB at a
  time, instead of copying the whole result into each fetcher's buffer, so
  N fetchers of one result no longer cost N copies. A task's inputs are
  copied once into its arguments, as it starts; a lone fetched input
  becomes them without a copy. `/metrics` adds
  `dista_worker_bytes_copied_total` and `dista_worker_fetch_buffer_bytes`.
  `bench/fetch.mojo` (`pixi run bench-fetch`): 12 readers of a 256 MB
  result over sockets take about half as long, and the largest worker's
  peak memory drops by a third
  ([#95](https://github.com/magmalake/dista.mojo/issues/95)).
- The README, `examples/mapreduce.mojo` and the docstrings read results with
  `take`: the README's loop of `get`s kept every result in the driver and the
  head until the program ended.
### Added

- **`rt.cancel(ref)`** (and a list overload): a task that hasn't run never
  does, and a running one's result is dropped; `get` raises `cancelled`, as
  do tasks waiting on it. An actor call is answered `cancelled` but still
  runs. A new frame, `KIND_CANCEL` (32), carries it.
- **`rt.get(ref, timeout=seconds)`** raises an error starting with `timed
  out` once the timeout passes; the ref stays valid. `get(ref)` is
  unchanged.

### Fixed

- A driver that disconnects no longer leaves its queued and waiting tasks
  to run on the workers: the head drops them (#97).
- **Driver numbers no longer restart at 1 when the head does** (#91). A head
  numbers new drivers from a random start and sends an epoch in its welcome;
  only a driver resuming under its old number claims results the workers
  report, so a new driver can't be handed another job's results. A resuming
  driver whose number this head gave someone else is refused with a reason,
  and its `Client` raises instead of retrying for ever; one that comes back
  before the head noticed its old connection drop replaces it. Results no
  driver comes back for are freed after `DISTA_ORPHAN_SECONDS` (default
  120).
- **The head's ready queue holds only what's queued.** It cleared its list
  only once every task in it had been handed out, so under backpressure,
  with the queue full for a whole job, every task dispatched stayed in
  memory with its arguments, uncounted by `dista_queued_bytes`. A task
  handed out now moves into the running set, and the queue compacts once
  half its list is spent. Dispatch no longer copies a task's arguments
  twice. `bench/fanout.mojo` gains a backpressure run: the head's peak
  memory there went from 176 MB to about 95 MB.
- **Connects time out, and a worker's peer connects don't block it.** A
  worker connects to the peers it fetches from inside its event loop, and
  keeps running tasks and serving fetches meanwhile. After
  `DISTA_PEER_CONNECT_TIMEOUT` seconds (new, default 5) it fails the tasks
  waiting on that peer, as unreachable. Before, a black-holed address
  stalled the whole worker for the kernel's two minutes. A driver's fetch
  waits the same time, `dista status` and the other observers wait
  `DISTA_CONNECT_TIMEOUT`, and each try of `connect_head` waits only for
  what is left of that budget
  ([#94](https://github.com/magmalake/dista.mojo/issues/94)).
  `tests/peer_connect.sh` fills a stopped worker's backlog and fetches from
  it.
- **Results larger than 1 GiB cross hosts.** A fetched result over 64 MiB
  now comes in part frames (new kind 33), which the fetcher assembles into
  one buffer. Before, a result between 1 and 4 GiB was refused as a `bad
  frame length`, and past 4 GiB its u32 length wrapped and garbled the
  connection. A fetch says it takes parts, so an older fetcher still gets
  one result frame, or a clear error if the result doesn't fit. Writing any
  frame too large for the receiver raises at the sender: a task argument
  past 1 GiB fails at `remote`
  ([#93](https://github.com/magmalake/dista.mojo/issues/93)).
### Changed

- **The head loop's cost follows its events.** Waiting tasks are indexed by
  their inputs, so an iteration looks only at the tasks whose inputs just
  settled instead of checking and copying every waiting task: 384 reducers
  gathering 2,000 map outputs finish in 0.07 s instead of 1.1 s
  (`bench/fanout.mojo`, which gains this run). A closed connection's slot
  and buffers are reused and freed, so a long-lived head polled by
  `dista status` no longer scans every connection it ever had. Each
  worker's held bytes are counted as results come and go, not recomputed
  for every `dista status --json`.

## [0.2.1] - 2026-11-03

### Changed

- Built against objectstore-mojo 0.4.1, whose shim is found without
  `CONDA_PREFIX` and raises an Error instead of aborting when missing. The
  stable, nightly and data environments (and the CI checkouts) pin the same
  release revisions of objectstore, zstd, lz4, brotli, sqlite and postgres.

## [0.2.0] - 2026-11-03

Autoscaling: the head reports its demand, a policy decides, and whatever
owns the machines starts and drains workers.

### Added

- **Demand, for autoscaling.** `dista status --json` prints the head's tasks
  ready, waiting for inputs and running, its actors waiting for a worker,
  and each worker's tasks in flight, seconds idle and bytes held. `/metrics`
  adds `dista_tasks_ready`, `dista_tasks_waiting`, `dista_tasks_running` and
  `dista_workers_idle`.
- **`dista.scale`**: `decide(demand, policy, now, pending, short_since)`
  says how many workers to start and which to drain, from
  `Demand.parse(status_json)` and a `Policy` (`min_workers`, which may be 0,
  `max_workers`, `tasks_per_worker`, `up_after`, `idle_before_down`,
  `max_step`). It has no clock or I/O of its own.
- **`dista scale --once`** prints the decision for the head's current
  demand; `tests/autoscale.sh` drives a job from 0 workers to 6 and back to
  0 with it.
- **`dista drain <ip:port>`** drains one worker from an observer, the same
  drain as SIGTERM's, for a provider that can't signal the worker's process.

## [0.1.0] - 2026-11-03

The first release: phases 1 to 4 of [VISION.md](VISION.md). Mojo 1.2.0, and
the 1.3 nightly in CI.

### Added

- **Tasks.** `Task[A, R, f]`, `Task2` and `Task3` declare a function's
  argument and result types; `rt.remote[T](...)` runs it and returns an
  `ObjectRef`, and `rt.get` waits for it. A task can take another's
  `ObjectRef`, gather a list of them into a `Seq`, or take a value beside
  them (`TaskWith`); a `TaskMany` returns several results, each its own
  object. Arguments and results are scalars, strings, plain structs of
  those, `Seq`s, `Codec` types, and arrow-mlake `RecordBatch`es as Arrow IPC.
- **One binary, three roles.** Run alone, a program forks a head and
  workers; with `DISTA_ROLE` and `DISTA_HEAD` it's a cluster's head, worker
  or driver. The `dista` command is a head for any program, and shows
  `status`, writes a `timeline` and `stop`s a cluster.
- **Data on the workers.** Large results stay where they were made: mapped
  from shared memory on the same host, fetched from the worker across
  hosts, and spilled to `DISTA_SPILL_DIR` past `DISTA_STORE_BYTES`. A task
  goes to the node holding most of its inputs' bytes.
- **Actors.** State on a worker of its own, with ordered method calls from
  drivers, tasks and other actors; checkpoints to `DISTA_CHECKPOINT_URI`, a
  directory or `s3://`; restarts from the last checkpoint when their worker
  dies, and restores across a head restart.
- **Failures.** A dead worker's tasks run again elsewhere, up to
  `DISTA_TASK_RETRIES`. A head that restarts gets its state back from its
  workers and drivers. Results lost with a worker aren't rebuilt: the job
  fails, naming them, and an orchestrator such as orca runs it again.
- **Draining.** A worker sent SIGTERM, as Kubernetes stops a pod, finishes
  its tasks while the others adopt its results and its actor, restored from
  a checkpoint, then leaves: nothing fails or runs twice.
- **Backpressure.** Past `DISTA_MAX_QUEUED` tasks or
  `DISTA_MAX_QUEUED_BYTES` of arguments, `remote` blocks until workers catch
  up; past `DISTA_MAX_DRIVER_BUFFER` bytes waiting for a driver that isn't
  reading, its tasks wait.
- **Operations.** Prometheus metrics with `DISTA_METRICS_PORT`, a Chrome
  trace timeline, one container image for every role, and Kubernetes
  manifests for a head StatefulSet and a worker Deployment.
- **Tests** of each: a fuzzer for the head's frames and the codec, chaos
  runs that kill the head at random, and on kind, the NYC taxi queries and a
  shuffle on 3 nodes against one process, with a worker killed and with one
  drained.

[Unreleased]: https://github.com/magmalake/dista.mojo/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/magmalake/dista.mojo/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/magmalake/dista.mojo/releases/tag/v0.1.0

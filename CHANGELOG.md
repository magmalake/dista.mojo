# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

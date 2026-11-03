# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/magmalake/dista.mojo/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/magmalake/dista.mojo/releases/tag/v0.1.0

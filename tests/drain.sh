#!/bin/bash
# A worker told to stop drains: a head, three workers and
# tests/drain_driver.mojo, as separate processes. Once every result is
# stored and slow tasks are running, one worker gets SIGTERM. It must leave by
# itself, having finished its tasks, and the others adopt what it held: the
# driver reads every result and tasks use each, with nothing failed and no
# task run again. Twice: with the node store, where an adopting worker maps
# the file, and with it off (DISTA_SHM=0), where it fetches over a socket.
set -euo pipefail
mkdir -p build
mojo build -I src ${DEP_INC:--I src} tests/drain_driver.mojo -o build/drain-driver

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

port_in() {
  for _ in $(seq 100); do
    p=$(sed -n "s/^$2 on port //p" "$1" | head -1)
    [ -n "$p" ] && { echo "$p"; return; }
    sleep 0.1
  done
  echo "no '$2' in $1" >&2; cat "$1" >&2; exit 1
}

value() { curl -sf "http://127.0.0.1:$1/metrics" | awk -v m="$2" '$1 == m {print $2}'; }

run() {
  local name=$1 shm=$2
  local log=build/drain-$name
  rm -f "$log"-*.log "$log.go"
  DISTA_SHM=$shm DISTA_ROLE=head DISTA_PORT=0 DISTA_METRICS_PORT=0 ./build/drain-driver > "$log-head.log" 2>&1 &
  local head=$!; pids+=($head)
  local port metrics
  port=$(port_in "$log-head.log" "dista head listening")
  metrics=$(port_in "$log-head.log" "dista metrics")
  local workers=()
  for w in 1 2 3; do
    DISTA_SHM=$shm DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/drain-driver > "$log-worker$w.log" 2>&1 &
    workers+=($!); pids+=($!)
  done
  DISTA_SHM=$shm DRAIN_GO="$log.go" DISTA_HEAD=localhost:$port ./build/drain-driver > "$log-driver.log" 2>&1 &
  local driver=$!; pids+=($driver)
  for _ in $(seq 300); do grep -q '^READY' "$log-driver.log" && break; sleep 0.1; done
  grep -q '^READY' "$log-driver.log" || { echo "FAIL: the driver never got ready"; cat "$log-driver.log"; exit 1; }

  local victim=${workers[0]}
  kill -TERM "$victim"
  # It finishes its slow tasks (1.5 s) and moves its results, well inside
  # DISTA_DRAIN_SECONDS.
  local code=""
  for _ in $(seq 200); do kill -0 "$victim" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$victim" 2>/dev/null; then echo "FAIL ($name): the worker didn't leave"; cat "$log-worker1.log"; exit 1; fi
  wait "$victim" && code=0 || code=$?
  grep -q 'dista worker: drained' "$log-worker1.log" || { echo "FAIL ($name): the worker didn't drain"; cat "$log-worker1.log"; exit 1; }
  touch "$log.go"

  wait "$driver" || { echo "FAIL ($name): the driver"; cat "$log-driver.log"; exit 1; }
  grep -q '^PASS drain driver' "$log-driver.log" || { echo "FAIL ($name): the driver"; cat "$log-driver.log"; exit 1; }

  local retried moved failed left
  retried=$(value "$metrics" dista_tasks_retried_total)
  moved=$(value "$metrics" dista_results_moved_total)
  failed=$(value "$metrics" 'dista_tasks_finished_total{outcome="error"}')
  left=$(value "$metrics" dista_workers)
  echo "$name: worker exited $code; head: $moved results moved, $retried tasks retried, $failed failed, $left workers left"
  if [ "$code" != 0 ] || [ "${retried:-1}" != 0 ] || [ "${moved:-0}" -lt 1 ] || [ "${failed:-1}" != 0 ] || [ "$left" != 2 ]; then
    echo "FAIL ($name): the drain lost or redid work"; cat "$log-head.log"; exit 1
  fi
  kill -9 "$head" "${workers[@]:1}" 2>/dev/null || true
  wait "$head" "${workers[@]:1}" 2>/dev/null || true
}

run store 1
run sockets 0
echo "PASS drain: a worker given SIGTERM finished its tasks, its results moved, nothing failed or ran again"

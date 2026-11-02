#!/bin/bash
# PR 8's local gate: a head and two workers with DISTA_METRICS_PORT set, as
# separate processes, run the smoke driver. Every process's /metrics must
# answer and count the tasks, and each must exit promptly on SIGTERM, as a
# pod being deleted sends.
set -euo pipefail
mkdir -p build
mojo build -I src ${DEP_INC:--I src} deploy/smoke.mojo -o build/metrics-smoke

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

# Waits for "<what> on port N" in a log, and prints N.
port_in() {
  for _ in $(seq 100); do
    p=$(sed -n "s/^$2 on port //p" "$1" | head -1)
    [ -n "$p" ] && { echo "$p"; return; }
    sleep 0.1
  done
  echo "no '$2' in $1" >&2; cat "$1" >&2; exit 1
}

DISTA_ROLE=head DISTA_PORT=0 DISTA_METRICS_PORT=0 ./build/metrics-smoke > build/metrics-head.log 2>&1 &
head=$!; pids+=($head)
port=$(port_in build/metrics-head.log "dista head listening")
head_metrics=$(port_in build/metrics-head.log "dista metrics")
workers=()
for w in 1 2; do
  DISTA_ROLE=worker DISTA_HEAD=localhost:$port DISTA_METRICS_PORT=0 ./build/metrics-smoke > build/metrics-worker$w.log 2>&1 &
  workers+=($!); pids+=($!)
done
w1=$(port_in build/metrics-worker1.log "dista metrics")
w2=$(port_in build/metrics-worker2.log "dista metrics")

DISTA_HEAD=localhost:$port ./build/metrics-smoke > build/metrics-driver.log 2>&1
grep -q '^PASS smoke' build/metrics-driver.log || { echo "FAIL: the driver"; cat build/metrics-driver.log; exit 1; }

value() { curl -sf "http://127.0.0.1:$1/metrics" | awk -v m="$2" '$1 == m {print $2}'; }
done_=$(value "$head_metrics" 'dista_tasks_finished_total{outcome="ok"}')
n_workers=$(value "$head_metrics" dista_workers)
ran1=$(value "$w1" 'dista_worker_tasks_finished_total{outcome="ok"}')
ran2=$(value "$w2" 'dista_worker_tasks_finished_total{outcome="ok"}')
echo "head: $done_ tasks finished, $n_workers workers; workers: $ran1 and $ran2"
if [ "${done_:-0}" -lt 1000 ] || [ "$n_workers" != 2 ] || [ "${ran1:-0}" -lt 1 ] || [ "${ran2:-0}" -lt 1 ] \
   || [ $((ran1 + ran2)) -lt 1000 ]; then
  echo "FAIL: the metrics don't add up"; curl -s "http://127.0.0.1:$head_metrics/metrics"; exit 1
fi
curl -s "http://127.0.0.1:$head_metrics/metrics" | grep -q '^dista_task_duration_seconds_count [1-9]' \
  || { echo "FAIL: no task durations"; exit 1; }

# SIGTERM: each process exits within five seconds.
for p in "${workers[@]}" "$head"; do
  kill -TERM "$p"
  for _ in $(seq 50); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$p" 2>/dev/null; then echo "FAIL: process $p ignored SIGTERM"; exit 1; fi
done
echo "PASS metrics: head and workers count the tasks, and exit on SIGTERM"

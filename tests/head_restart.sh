#!/bin/bash
# Phase 3's head-restart gate: a head, four workers and a driver as separate
# processes; the head is killed partway through the driver's work and started
# again on the same port. Workers rejoin with their results, the driver
# resumes, and its answers must be exact.
set -euo pipefail
mkdir -p build
mojo build -I src ${DEP_INC:--I src} tests/restart_driver.mojo -o build/restart-driver

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

start_head() {
  DISTA_ROLE=head DISTA_PORT=$1 ./build/restart-driver >> build/restart-head.log 2>&1 &
  head=$!
  pids+=($head)
}
: > build/restart-head.log
start_head 0
port=""
for _ in $(seq 100); do
  port=$(sed -n 's/^dista head listening on port //p' build/restart-head.log | head -1)
  [ -n "$port" ] && break
  sleep 0.1
done
[ -n "$port" ] || { echo "head never started"; cat build/restart-head.log; exit 1; }

for _ in 1 2 3 4; do
  DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/restart-driver >> build/restart-workers.log 2>&1 &
  pids+=($!)
done

DISTA_HEAD=localhost:$port ./build/restart-driver > build/restart-driver.log 2>&1 &
driver=$!
pids+=($driver)
sleep 1
kill -9 "$head"
sleep 0.5
start_head "$port"

status=0
wait "$driver" || status=$?
out=$(grep '^lengths' build/restart-driver.log || true)
echo "driver: $out"
# 400 blobs of 100,000 + i bytes; squares of 0..399.
want="lengths $((400 * 100000 + 399 * 400 / 2)) squares $((399 * 400 * 799 / 6))"
if [ $status != 0 ] || [ "$out" != "$want" ]; then
  echo "FAIL: wanted '$want' (driver exit $status)"
  cat build/restart-driver.log; tail -20 build/restart-head.log build/restart-workers.log
  exit 1
fi
grep -q "resuming" build/restart-driver.log || { echo "FAIL: the driver never lost its head"; exit 1; }
echo "PASS head restart: killed mid-run, workers rejoined, the driver resumed, answers exact"

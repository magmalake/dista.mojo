#!/bin/bash
# Randomized end-to-end test: for each seed in CHAOS_SEEDS (default "1 2 3"),
# a head, four workers and tests/chaos_driver.mojo's random task graph, with
# the head killed (-9) and restarted at random moments. The driver checks
# every result it still holds against the same functions run in-process.
#
# Only the head is killed for now: a worker's death fails its tasks until
# actors bring restarts (phase 3, PR 6), and then workers join the kill list.
set -euo pipefail
mkdir -p build
mojo build -I src ${DEP_INC:--I src} tests/chaos_driver.mojo -o build/chaos-driver

for seed in ${CHAOS_SEEDS:-1 2 3}; do
  pids=()
  cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
  trap cleanup EXIT
  : > build/chaos-head.log
  DISTA_ROLE=head DISTA_PORT=0 ./build/chaos-driver >> build/chaos-head.log 2>&1 &
  head=$!
  pids+=($head)
  port=""
  for _ in $(seq 100); do
    port=$(sed -n 's/^dista head listening on port //p' build/chaos-head.log | head -1)
    [ -n "$port" ] && break
    sleep 0.1
  done
  [ -n "$port" ] || { echo "head never started"; exit 1; }
  for _ in 1 2 3 4; do
    DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/chaos-driver >> build/chaos-workers.log 2>&1 &
    pids+=($!)
  done
  CHAOS_SEED=$seed DISTA_HEAD=localhost:$port ./build/chaos-driver > build/chaos-driver.log 2>&1 &
  driver=$!
  pids+=($driver)

  # Kill the head every 0.3-1.2 s (seeded) while the driver runs.
  RANDOM=$seed
  kills=0
  while kill -0 "$driver" 2>/dev/null; do
    sleep "0.$((3 + RANDOM % 9))"
    kill -0 "$driver" 2>/dev/null || break
    kill -9 "$head" 2>/dev/null || true
    sleep 0.2
    DISTA_ROLE=head DISTA_PORT=$port ./build/chaos-driver >> build/chaos-head.log 2>&1 &
    head=$!
    pids+=($head)
    kills=$((kills + 1))
  done
  status=0
  wait "$driver" || status=$?
  if [ $status != 0 ] || ! grep -q '^PASS chaos' build/chaos-driver.log; then
    echo "FAIL: chaos seed $seed after $kills head kills"
    tail -20 build/chaos-driver.log build/chaos-head.log build/chaos-workers.log
    exit 1
  fi
  echo "$(grep '^PASS chaos' build/chaos-driver.log), $kills head kills"
  cleanup
  trap - EXIT
  sleep 0.5
done

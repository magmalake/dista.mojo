#!/bin/bash
# Placement by data location: a head, four workers as two nodes (two each,
# named by DISTA_NODE) and tests/locality_driver.mojo. Most tasks taking a
# result that lives on a worker must run on that worker's node; without
# placement about half do.
set -euo pipefail
mkdir -p build
mojo build -I src ${DEP_INC:--I src} tests/locality_driver.mojo -o build/locality-driver

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

DISTA_ROLE=head DISTA_PORT=0 ./build/locality-driver > build/locality-head.log 2>&1 &
pids+=($!)
port=""
for _ in $(seq 100); do
  port=$(sed -n 's/^dista head listening on port //p' build/locality-head.log)
  [ -n "$port" ] && break
  sleep 0.1
done
[ -n "$port" ] || { echo "head never started"; cat build/locality-head.log; exit 1; }

for node in a a b b; do
  DISTA_NODE=node-$node DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/locality-driver >> build/locality-workers.log 2>&1 &
  pids+=($!)
done

out=$(DISTA_HEAD=localhost:$port ./build/locality-driver)
echo "driver: $out"
n=$(echo "$out" | sed -n 's/^local \([0-9]*\) of 40$/\1/p')
[ -n "$n" ] || { echo "FAIL: no answer"; exit 1; }
[ "$n" -ge 36 ] || { echo "FAIL: only $n of 40 tasks ran beside their input"; exit 1; }
echo "PASS locality: $n of 40 tasks ran on the node holding their input"

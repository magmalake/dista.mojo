#!/bin/bash
# Runs the map-reduce example as a real cluster on this machine: a head, four
# workers and a driver, each its own process, found by host name. Then checks
# that a binary built with different tasks is refused.
set -euo pipefail
mkdir -p build
mojo build -I src examples/mapreduce.mojo -o build/mapreduce
mojo build -I src tests/other_registry.mojo -o build/other-registry

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

DISTA_ROLE=head DISTA_PORT=0 ./build/mapreduce > build/head.log 2>&1 &
pids+=($!)
port=""
for _ in $(seq 100); do
  port=$(sed -n 's/^dista head listening on port //p' build/head.log)
  [ -n "$port" ] && break
  sleep 0.1
done
[ -n "$port" ] || { echo "head never started"; cat build/head.log; exit 1; }

for _ in 1 2 3 4; do
  DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/mapreduce &
  pids+=($!)
done

out=$(DISTA_HEAD=localhost:$port ./build/mapreduce)
echo "driver: $out"
[ "$out" = "total 332833500" ] || { echo "FAIL: wrong total"; exit 1; }

if err=$(DISTA_HEAD=localhost:$port ./build/other-registry 2>&1); then
  echo "FAIL: a driver with a different registry was accepted"; exit 1
fi
echo "$err" | grep -q "registry mismatch" || { echo "FAIL: $err"; exit 1; }
echo "PASS cluster: head, 4 workers and a driver as separate processes; mismatched registry refused"

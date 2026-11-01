#!/bin/bash
# Randomized end-to-end test: for each seed in CHAOS_SEEDS (default "1 2 3"),
# a head, four workers and tests/chaos_driver.mojo's random task graph, with
# the head killed (-9) and restarted at random moments. The driver checks
# every result it still holds against the same functions run in-process.
#
# A seed that runs past CHAOS_TIMEOUT seconds (default 180) fails, after
# saving the driver's stack to build/chaos-hang-<seed>.txt (macOS `sample`,
# Linux gdb), so a hang can't stall CI and leaves something to read.
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
  progress=build/chaos-progress
  : > "$progress"
  CHAOS_SEED=$seed CHAOS_PROGRESS=$progress DISTA_HEAD=localhost:$port \
    ./build/chaos-driver > build/chaos-driver.log 2>&1 &
  driver=$!
  pids+=($driver)

  # Kill the head every 0.3-1.2 s (seeded) while the driver runs;
  # CHAOS_FAST=1 makes it every 0.1-0.5 s, CHAOS_FAST=2 every 0.05-0.2 s.
  RANDOM=$seed
  kills=0
  last_progress=
  started=$SECONDS
  lo=3 spread=9
  [ "${CHAOS_FAST:-0}" = 1 ] && lo=1 spread=5
  while kill -0 "$driver" 2>/dev/null; do
    if [ $((SECONDS - started)) -gt "${CHAOS_TIMEOUT:-180}" ]; then
      echo "FAIL: chaos seed $seed hung after $kills head kills; stack in build/chaos-hang-$seed.txt"
      if command -v sample >/dev/null; then
        sample "$driver" 2 -file "build/chaos-hang-$seed.txt" >/dev/null 2>&1 || true
      elif command -v gdb >/dev/null; then
        gdb -p "$driver" -batch -ex "thread apply all bt" > "build/chaos-hang-$seed.txt" 2>&1 || true
      fi
      grep -E "dista::" "build/chaos-hang-$seed.txt" | head -30 || true
      echo "== who holds port $port"
      lsof -nP -iTCP:"$port" 2>/dev/null || true
      echo "== processes"
      ps -o pid,stat,etime,command -p "$(IFS=,; echo "${pids[*]}")" 2>/dev/null | grep -v " Z " || true
      echo "== driver log"
      tail -20 build/chaos-driver.log
      echo "== head log"
      tail -20 build/chaos-head.log
      echo "== workers log"
      grep -v "head gone, rejoining" build/chaos-workers.log | tail -20 || true
      exit 1
    fi
    if [ "${CHAOS_FAST:-0}" = 2 ]; then
      sleep "$(printf '0.%02d' $((5 + RANDOM % 15)))"
    else
      sleep "0.$((lo + RANDOM % spread))"
    fi
    kill -0 "$driver" 2>/dev/null || break
    # Kill only once the driver has got further since the last kill (or 2 s
    # on): heads killed faster than the driver can recover would keep it
    # from ever finishing, a livelock of the test rather than a bug. A real
    # deadlock still makes no progress and runs into CHAOS_TIMEOUT.
    for _ in $(seq 20); do
      [ "$(cat "$progress" 2>/dev/null)" != "${last_progress:-}" ] && break
      kill -0 "$driver" 2>/dev/null || break
      sleep 0.1
    done
    last_progress=$(cat "$progress" 2>/dev/null)
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

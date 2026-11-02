#!/bin/bash
# The `dista` command's gate: one generic head (`dista head`) serves two
# different programs at once, each with workers of its own: the smoke test
# and the head-restart driver, actor included. Both must pass, `dista status`
# must show the four workers under two registries, `dista timeline` must
# write their tasks, and `dista stop` must end the head and every worker.
set -euo pipefail
mkdir -p build
mojo build -I src ${DEP_INC:--I src} cli/main.mojo -o build/dista
mojo build -I src ${DEP_INC:--I src} deploy/smoke.mojo -o build/cli-smoke
mojo build -I src ${DEP_INC:--I src} tests/restart_driver.mojo -o build/cli-restart
export DISTA_CHECKPOINT_URI=$PWD/build/cli-checkpoints
rm -rf "$DISTA_CHECKPOINT_URI"

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

./build/dista head --port 0 > build/cli-head.log 2>&1 &
head=$!; pids+=($head)
port=""
for _ in $(seq 100); do
  port=$(sed -n 's/^dista head listening on port //p' build/cli-head.log | head -1)
  [ -n "$port" ] && break
  sleep 0.1
done
[ -n "$port" ] || { echo "FAIL: dista head never started"; cat build/cli-head.log; exit 1; }
export DISTA_HEAD=localhost:$port

workers=()
for prog in cli-smoke cli-smoke cli-restart cli-restart; do
  DISTA_ROLE=worker ./build/$prog >> build/cli-workers.log 2>&1 &
  workers+=($!); pids+=($!)
done

./build/cli-smoke > build/cli-smoke.log 2>&1 &
smoke=$!; pids+=($smoke)
./build/cli-restart > build/cli-restart.log 2>&1 &
restart=$!; pids+=($restart)
sleep 1
./build/dista status | tee build/cli-status.log
status=0
wait "$smoke" || status=1
wait "$restart" || status=1
grep -q '^PASS smoke' build/cli-smoke.log || status=1
want="lengths $((400 * 100000 + 399 * 400 / 2)) squares $((399 * 400 * 799 / 6)) counter 300"
[ "$(grep '^lengths' build/cli-restart.log || true)" = "$want" ] || status=1
if [ $status != 0 ]; then
  echo "FAIL: a driver"; tail -5 build/cli-smoke.log build/cli-restart.log build/cli-head.log; exit 1
fi
echo "both programs passed on one head"

[ "$(grep -c 'registry 0x' build/cli-status.log)" -ge 4 ] || { echo "FAIL: status lists too few workers"; exit 1; }
[ "$(grep '  registry 0x' build/cli-status.log | awk '{print $3}' | sort -u | wc -l)" -ge 2 ] \
  || { echo "FAIL: status shows one registry"; exit 1; }

./build/dista timeline -o build/cli-trace.json | tee build/cli-timeline.log
n=$(awk '{print $2}' build/cli-timeline.log)
[ "${n:-0}" -ge 1000 ] || { echo "FAIL: the timeline holds $n tasks"; exit 1; }

./build/dista stop
for p in "$head" "${workers[@]}"; do
  for _ in $(seq 50); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$p" 2>/dev/null; then echo "FAIL: process $p outlived dista stop"; exit 1; fi
done
echo "PASS cli: one dista head served two programs; status, timeline and stop work"

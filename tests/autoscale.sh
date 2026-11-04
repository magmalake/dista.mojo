#!/bin/bash
# Autoscaling, end to end on one machine: a `dista head` with no workers, a
# driver submitting 200 tasks of 0.5 s, and a provider loop here that runs
# `dista scale --once` twice a second, starts the workers it asks for as
# processes and drains the ones it names with `dista drain`. The job must
# grow to max_workers (6), in steps of at most max_step, and once it's done
# shrink back to zero; every result exact, no task retried, nothing failed,
# and every drained worker exiting 0.
set -euo pipefail
mkdir -p build
scripts/mojo-build.sh -I src ${DEP_INC:--I src} cli/main.mojo -o build/dista
scripts/mojo-build.sh -I src ${DEP_INC:--I src} tests/autoscale_driver.mojo -o build/autoscale-driver

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

log=build/autoscale
rm -f "$log"-*.log
DISTA_METRICS_PORT=0 ./build/dista head --port 0 > "$log-head.log" 2>&1 &
head=$!; pids+=($head)
port_in() {
  for _ in $(seq 100); do
    p=$(sed -n "s/^$1 on port //p" "$log-head.log" | head -1)
    [ -n "$p" ] && { echo "$p"; return; }
    sleep 0.1
  done
  echo "no '$1' in the head's log" >&2; cat "$log-head.log" >&2; exit 1
}
port=$(port_in "dista head listening")
metrics=$(port_in "dista metrics")
export DISTA_HEAD=localhost:$port
value() { curl -sf "http://127.0.0.1:$metrics/metrics" | awk -v m="$1" '$1 == m {print $2}'; }
joined() { ./build/dista status --json | python3 -c 'import json, sys; print(len(json.load(sys.stdin)["workers"]))'; }

./build/autoscale-driver > "$log-driver.log" 2>&1 &
driver=$!; pids+=($driver)

workers=()
since=0
most=0
step=0
started=0
drained=0
driver_done=""
for tick in $(seq 600); do
  alive=0
  for w in "${workers[@]+"${workers[@]}"}"; do kill -0 "$w" 2>/dev/null && alive=$((alive + 1)); done
  n=$(joined)
  pending=$((alive > n ? alive - n : 0))
  out=$(./build/dista scale --once --min 0 --max 6 --step 4 --up-after 1 --idle 2 \
    --pending "$pending" --short-since "$since")
  since=$(awk '$1 == "short_since" {print $2}' <<< "$out")
  start=$(awk '$1 == "start" {print $2}' <<< "$out")
  [ "$start" -gt "$step" ] && step=$start
  for ((k = 0; k < start; k++)); do
    DISTA_ROLE=worker ./build/autoscale-driver >> "$log-workers.log" 2>&1 &
    workers+=($!); pids+=($!)
    started=$((started + 1))
  done
  for a in $(awk '$1 == "drain" {print $2}' <<< "$out"); do
    ./build/dista drain "$a" > /dev/null
    drained=$((drained + 1))
  done
  echo "tick $tick alive $alive joined $n pending $pending: $(tr '\n' ' ' <<< "$out")" >> "$log-ticks.log"
  total=$((alive + start))
  [ "$total" -gt "$most" ] && most=$total
  if [ -z "$driver_done" ] && ! kill -0 "$driver" 2>/dev/null; then
    driver_done=$tick
  fi
  if [ -n "$driver_done" ] && [ "$alive" = 0 ] && [ "$n" = 0 ]; then
    break
  fi
  sleep 0.5
done

wait "$driver" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
grep -q '^PASS autoscale driver' "$log-driver.log" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
codes=0
for w in "${workers[@]}"; do wait "$w" || codes=$((codes + 1)); done
retried=$(value dista_tasks_retried_total)
failed=$(value 'dista_tasks_finished_total{outcome="error"}')
left=$(value dista_workers)
echo "autoscale: started $started workers (at most $most at once, $step in one step), drained $drained; $retried retried, $failed failed, $left left, $codes exited non-zero"
if [ "$most" != 6 ] || [ "$step" -gt 4 ] || [ "$step" -lt 1 ] || [ "${left:-1}" != 0 ] \
  || [ "${retried:-1}" != 0 ] || [ "${failed:-1}" != 0 ] || [ "$codes" != 0 ] || [ "$drained" != "$started" ]; then
  echo "FAIL: autoscaling"; tail -20 "$log-head.log"; exit 1
fi
echo "PASS autoscale: grew from 0 to 6 workers and back to 0, every result exact, nothing retried or lost"

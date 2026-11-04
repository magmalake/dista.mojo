#!/bin/bash
# A worker dies while others fetch its result: a head, three workers and
# tests/peer_loss_driver.mojo, as separate processes, with the node store off
# (DISTA_SHM=0) so inputs travel over sockets. The worker holding a result is
# stopped, tasks taking that result go out, and the holder is killed while
# the other workers wait on their fetches. Every task must fail with its
# input lost, within seconds, and the other workers must stay up. Then a
# worker that has served a fetch is killed while idle: the last one must not
# spin on the connection it closed.
set -euo pipefail
mkdir -p build
scripts/mojo-build.sh -I src ${DEP_INC:--I src} tests/peer_loss_driver.mojo -o build/peer-loss-driver

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

# CPU time a process has used, in hundredths of a second.
cpu() {
  if [ -r "/proc/$1/stat" ]; then
    # utime and stime, in clock ticks: 100 a second on Linux.
    awk '{print $14 + $15}' "/proc/$1/stat"
  else
    ps -o time= -p "$1" | awk -F'[:.]' '{print ($1 * 60 + $2) * 100 + $3}'
  fi
}

log=build/peer-loss
rm -f "$log"-*.log "$log.go" "$log.done"
export DISTA_SHM=0
DISTA_ROLE=head DISTA_PORT=0 DISTA_METRICS_PORT=0 ./build/peer-loss-driver > "$log-head.log" 2>&1 &
head=$!; pids+=($head)
port=$(port_in "$log-head.log" "dista head listening")
metrics=$(port_in "$log-head.log" "dista metrics")
workers=()
for w in 1 2 3; do
  DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/peer-loss-driver > "$log-worker$w.log" 2>&1 &
  workers+=($!); pids+=($!)
done
PEER_LOSS_GO="$log.go" PEER_LOSS_DONE="$log.done" DISTA_HEAD=localhost:$port ./build/peer-loss-driver > "$log-driver.log" 2>&1 &
driver=$!; pids+=($driver)

holder=""
for _ in $(seq 300); do
  holder=$(sed -n 's/^HOLDER //p' "$log-driver.log")
  [ -n "$holder" ] && break
  sleep 0.1
done
[ -n "$holder" ] || { echo "FAIL: the driver never said which worker holds its result"; cat "$log-driver.log"; exit 1; }
others=()
for w in "${workers[@]}"; do [ "$w" = "$holder" ] || others+=("$w"); done
[ "${#others[@]}" = 2 ] || { echo "FAIL: the holder $holder isn't one of the workers ${workers[*]}"; exit 1; }

kill -STOP "$holder"
touch "$log.go"
for _ in $(seq 100); do grep -q '^SUBMITTED' "$log-driver.log" && break; sleep 0.1; done
sleep 1  # the tasks reach the workers, which ask the stopped holder
kill -9 "$holder"
for _ in $(seq 150); do grep -q '^HOLDER2' "$log-driver.log" && break; sleep 0.1; done
grep -q '^HOLDER2' "$log-driver.log" || {
  echo "FAIL: the tasks still wait 15 s after the worker holding their input died"
  cat "$log-driver.log"; cat "$log-head.log"; exit 1
}
for w in "${others[@]}"; do
  kill -0 "$w" 2>/dev/null || { echo "FAIL: worker $w died with its peer"; cat "$log"-worker*.log; exit 1; }
done
left=$(value "$metrics" dista_workers)
[ "$left" = 2 ] || { echo "FAIL: $left workers left, not 2"; exit 1; }

for _ in $(seq 100); do grep -q '^FETCHED' "$log-driver.log" && break; sleep 0.1; done
grep -q '^FETCHED' "$log-driver.log" || { echo "FAIL: the second result's tasks"; cat "$log-driver.log"; exit 1; }
holder2=$(sed -n 's/^HOLDER2 //p' "$log-driver.log")
last=""
for w in "${others[@]}"; do [ "$w" = "$holder2" ] || last=$w; done
[ -n "$last" ] || { echo "FAIL: the second holder $holder2 isn't one of ${others[*]}"; exit 1; }
kill -9 "$holder2"
sleep 0.5
before=$(cpu "$last")
sleep 2
spent=$(($(cpu "$last") - before))
echo "the last worker used $((spent * 10)) ms of CPU in 2 s after its peer closed"
kill -0 "$last" 2>/dev/null || { echo "FAIL: the last worker died with its peer"; cat "$log"-worker*.log; exit 1; }
[ "$spent" -le 20 ] || { echo "FAIL: the last worker spins on the closed connection"; exit 1; }
touch "$log.done"
wait "$driver" || { echo "FAIL: the driver"; cat "$log-driver.log"; cat "$log-head.log"; exit 1; }
grep -q '^PASS peer loss driver' "$log-driver.log" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
echo "PASS peer loss: tasks whose input's worker died failed fast, its peers stayed up, and none spun on a closed connection"

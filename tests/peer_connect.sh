#!/bin/bash
# A connect to a peer that never answers doesn't stall a worker: a head, two
# workers and tests/peer_connect_driver.mojo, with the node store off
# (DISTA_SHM=0) so inputs travel over sockets. The worker holding a result is
# stopped and its listener's backlog filled, so new connects to it get no
# answer, as with a node that vanished or a firewall dropping SYNs. The other
# worker, asked to fetch from it, must keep serving meanwhile, and fail the
# task once DISTA_PEER_CONNECT_TIMEOUT (2 s here) passes, not after the
# kernel's two minutes.
set -euo pipefail
mkdir -p build
scripts/mojo-build.sh -I src ${DEP_INC:--I src} tests/peer_connect_driver.mojo -o build/peer-connect-driver

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

log=build/peer-connect
rm -f "$log"-*.log "$log.go"
export DISTA_SHM=0 DISTA_PEER_CONNECT_TIMEOUT=2
DISTA_ROLE=head DISTA_PORT=0 ./build/peer-connect-driver > "$log-head.log" 2>&1 &
pids+=($!)
port=""
for _ in $(seq 100); do
  port=$(sed -n 's/^dista head listening on port //p' "$log-head.log")
  [ -n "$port" ] && break
  sleep 0.1
done
[ -n "$port" ] || { echo "head never started"; cat "$log-head.log"; exit 1; }
for w in 1 2; do
  DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/peer-connect-driver > "$log-worker$w.log" 2>&1 &
  pids+=($!)
done
PEER_CONNECT_GO="$log.go" DISTA_HEAD=localhost:$port ./build/peer-connect-driver > "$log-driver.log" 2>&1 &
driver=$!; pids+=($driver)

holder=""
for _ in $(seq 300); do
  holder=$(sed -n 's/^HOLDER //p' "$log-driver.log")
  [ -n "$holder" ] && break
  sleep 0.1
done
[ -n "$holder" ] || { echo "FAIL: the driver never said where its result is"; cat "$log-driver.log"; exit 1; }
pid=${holder%% *}
peer_port=${holder##*:}
kill -STOP "$pid"
# Connect to the stopped worker until its backlog is full: the kernel then
# drops further SYNs. The sockets stay open while the test runs.
python3 -c '
import socket, sys, time
port = int(sys.argv[1])
held = []
for _ in range(1024):
    s = socket.socket()
    s.setblocking(False)
    s.connect_ex(("127.0.0.1", port))
    held.append(s)
print("stuffed", flush=True)
time.sleep(120)
' "$peer_port" > "$log-stuff.log" 2>&1 &
pids+=($!)
for _ in $(seq 50); do grep -q stuffed "$log-stuff.log" && break; sleep 0.1; done
sleep 0.5
touch "$log.go"
for _ in $(seq 200); do kill -0 "$driver" 2>/dev/null || break; sleep 0.1; done
if kill -0 "$driver" 2>/dev/null; then
  echo "FAIL: the driver still waits 20 s after the holder went silent"
  cat "$log-driver.log"; exit 1
fi
wait "$driver" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
grep '^SERVED\|^FAILED' "$log-driver.log"
grep -q '^PASS peer connect driver' "$log-driver.log" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
echo "PASS peer connect: a worker connecting to a silent peer kept serving, and failed the task at DISTA_PEER_CONNECT_TIMEOUT"

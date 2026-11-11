#!/bin/bash
# A connect to a peer that never answers doesn't stall a worker: a head, two
# workers and tests/peer_connect_driver.mojo, with the node store off
# (DISTA_SHM=0) so inputs travel over sockets. One worker advertises
# 192.0.2.1 (TEST-NET-1, RFC 5737, never routed), so connects to it get no
# answer, as with a node that vanished or a firewall dropping SYNs; it's
# stopped once it holds a result. (A full listen backlog stood in for this
# until macOS 27, which resets those connects: #126.) The other
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
DISTA_ADVERTISE=192.0.2.1 DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/peer-connect-driver > "$log-worker1.log" 2>&1 &
silent=$!; pids+=($silent)
DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/peer-connect-driver > "$log-worker2.log" 2>&1 &
pids+=($!)
PEER_CONNECT_GO="$log.go" DISTA_HEAD=localhost:$port ./build/peer-connect-driver > "$log-driver.log" 2>&1 &
driver=$!; pids+=($driver)

holder=""
for _ in $(seq 300); do
  holder=$(sed -n 's/^HOLDER //p' "$log-driver.log")
  [ -n "$holder" ] && break
  sleep 0.1
done
[ -n "$holder" ] || { echo "FAIL: the driver never said where its result is"; cat "$log-driver.log"; exit 1; }
kill -STOP "$silent"
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

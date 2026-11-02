#!/bin/bash
# The phase-2 gate on a kind cluster of three worker nodes: taxibench's
# queries over the NYC taxi Iceberg table, one task per data file, checked
# against the same scans in one process (deploy/taxi.mojo). Then the shuffle
# benchmark's job on the same cluster (deploy/shuffle.mojo, bench/shuffle):
# trips by zone pair and hour as map and reduce tasks, checked against one
# process too. Then phase 4's gate: the queries again, many passes, with a
# worker pod killed partway; every answer must still be exact, and the head
# must have run the dead worker's tasks again.
#
# Needs docker, kind, kubectl, the taxi warehouse (taxibench.example's
# scripts/load.sh builds it), and the data environment's siblings checked out
# next to this repository. Not run in CI: the table is 1.3 GB.
#
#   TAXI_WAREHOUSE  the warehouse directory (default: taxibench.example's)
#   KEEP_CLUSTER=1  leave the cluster up afterwards
set -euo pipefail
cd "$(dirname "$0")/.."

cluster=${KIND_CLUSTER:-dista}
# The physical path: the table names its files by where they really are, and
# a symlinked parent (~/dev here) would make the rebase miss.
warehouse=$(cd "${TAXI_WAREHOUSE:-../taxibench.example/build/warehouse}" && pwd -P)
metadata=$(cat "$warehouse/metadata_location.txt")
prefix="file://$warehouse"
case "$metadata" in
  "$prefix"/*) ;;
  *) echo "the table's metadata ($metadata) isn't under $prefix"; exit 1 ;;
esac

# The build context: dista.mojo and the siblings its data environment
# compiles, side by side, without build output.
stage=build/taxi-context
rm -rf "$stage" && mkdir -p "$stage"
for repo in dista memory-region arrow-mlake parquet iceberg threads thrift \
    hashes snappy deflate avro roaring objectstore zstd lz4 brotli sqlite \
    postgres; do
  rsync -a --exclude .pixi --exclude build --exclude .git \
    "../$repo.mojo/" "$stage/$repo.mojo/"
done
docker build -f deploy/Dockerfile.taxi -t dista:taxi "$stage"

if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --name "$cluster" --config deploy/kind.yaml --wait 120s
fi
[ "${KEEP_CLUSTER:-0}" = 1 ] || trap 'kind delete cluster --name "$cluster"' EXIT
kind load docker-image dista:taxi --name "$cluster"

# Every node gets its own copy of the warehouse at /data/wh, as if it were a
# shared filesystem. The table names its files by $prefix; the pods rebase
# that onto /data/wh.
for node in $(kind get nodes --name "$cluster" | grep -v control-plane); do
  if ! docker exec "$node" test -f /data/wh/metadata_location.txt; then
    echo "copying the warehouse to $node"
    docker exec "$node" mkdir -p /data/wh
    docker cp "$warehouse/." "$node:/data/wh/"
  fi
done

ctx=(--context "kind-$cluster")

# Runs program $1 (in /opt/dista/bin) as the workers and the driver, and
# checks the driver's log has a line starting with $2. With $3 = kill, it
# runs 20 passes and kills a worker pod once the first query is done.
run_program() {
  local program=$1 pass=$2 kill=${3:-}
  local passes=1
  [ "$kill" = kill ] && passes=20
  sed -e "s|@TAXI_METADATA@|$metadata|" -e "s|@TAXI_PREFIX@|$prefix|" \
    -e "s|@PROGRAM@|/opt/dista/bin/$program|" -e "s|@PASSES@|$passes|" \
    deploy/k8s/taxi.yaml > build/taxi.yaml
  # Start from nothing: a rolling update can't place new workers beside the
  # old ones while the spread allows only one per node.
  kubectl "${ctx[@]}" delete -f build/taxi.yaml --ignore-not-found --wait
  kubectl "${ctx[@]}" apply -f build/taxi.yaml
  kubectl "${ctx[@]}" rollout status statefulset/dista-head --timeout=120s
  kubectl "${ctx[@]}" rollout status deployment/dista-worker --timeout=120s
  kubectl "${ctx[@]}" get pods -o wide

  if [ "$kill" = kill ]; then
    # Once the first query's first pass is done, kill a worker outright: no
    # grace period, as a node failure would. Its replacement joins the head.
    for _ in $(seq 300); do
      kubectl "${ctx[@]}" logs job/dista-taxi 2>/dev/null | grep -q '^TIME ' && break
      sleep 1
    done
    local victim
    victim=$(kubectl "${ctx[@]}" get pods -l app=dista-taxi,role=worker -o name | head -1)
    echo "killing $victim"
    kubectl "${ctx[@]}" delete "$victim" --grace-period=0 --force --wait=false
  fi

  # Wait for the job to succeed or fail, whichever comes first.
  local status=1 done_
  for _ in $(seq 600); do
    done_=$(kubectl "${ctx[@]}" get job dista-taxi -o jsonpath='{.status.succeeded}{.status.failed}')
    [ -n "$done_" ] && break
    sleep 2
  done
  [ "$(kubectl "${ctx[@]}" get job dista-taxi -o jsonpath='{.status.succeeded}')" = 1 ] && status=0
  kubectl "${ctx[@]}" logs job/dista-taxi || true
  if [ $status != 0 ] || ! kubectl "${ctx[@]}" logs job/dista-taxi | grep -q "^$pass"; then
    echo "FAIL: the $program job did not pass"
    kubectl "${ctx[@]}" get pods -o wide
    kubectl "${ctx[@]}" logs -l app=dista-taxi --prefix --tail=40 || true
    exit 1
  fi
  if [ "$kill" = kill ]; then
    # The head's metrics: the dead worker's tasks must have run again.
    kubectl "${ctx[@]}" port-forward pod/dista-head-0 19090:9090 >/dev/null 2>&1 &
    local fwd=$!
    local retried=""
    for _ in $(seq 30); do
      retried=$(curl -s localhost:19090/metrics | sed -n 's/^dista_tasks_retried_total //p')
      [ -n "$retried" ] && break
      sleep 1
    done
    kill $fwd 2>/dev/null || true
    echo "tasks retried: ${retried:-unknown}"
    if [ -z "$retried" ] || [ "$retried" -lt 1 ]; then
      echo "FAIL: the killed worker's tasks were never run again"
      exit 1
    fi
  fi
}

run_program app "PASS taxi"
run_program shuffle "PASS shuffle"
run_program app "PASS taxi" kill
echo "PASS kind taxi: the queries and the shuffle on 3 nodes, each equal to one process; the queries again with a worker killed, still exact"

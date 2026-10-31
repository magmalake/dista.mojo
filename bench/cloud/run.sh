#!/bin/bash
# The benchmark matrix on the EC2 cluster: each framework on each node count,
# every query TAXI_PASSES times, one run after another on the same nodes.
#
#   bench/cloud/run.sh                      everything below
#   NODES="3 6 12" FRAMEWORKS="dista rust ray" PASSES=5 SLOTS=16 bench/cloud/run.sh
#
# SLOTS is how many single-threaded tasks each node runs at once: that many
# dista workers, that many Rust workers, or one Ray worker with that many
# CPUs. It defaults to the worker nodes' vCPUs. Each run's log goes to
# build/cloud/results/<framework>-<nodes>.log, and summarize.py turns the
# TIME lines into tables. Needs aws.sh up, build.sh and data.sh load first.
set -euo pipefail
cd "$(dirname "$0")/../.."
. bench/cloud/common.sh

metadata=$(cat "$state/metadata.txt")
passes=${PASSES:-5}
slots=${SLOTS:-$(kubectl get nodes -l dista-bench/role=worker -o jsonpath='{.items[0].status.capacity.cpu}')}
results=$state/results
mkdir -p "$results"

for nodes in ${NODES:-3 6 12}; do
  for fw in ${FRAMEWORKS:-dista rust ray}; do
    echo "== $fw on $nodes nodes, $slots slots each"
    aws_secret
    sed -e "s|@NODES@|$nodes|g" -e "s|@SLOTS@|$slots|g" \
      -e "s|@REPLICAS@|$((nodes * slots))|g" -e "s|@PASSES@|$passes|g" \
      -e "s|@TAXI_METADATA@|$metadata|g" "bench/cloud/k8s/$fw.yaml" > "$state/$fw.yaml"
    kubectl delete -f "$state/$fw.yaml" --ignore-not-found --wait >/dev/null
    kubectl apply -f "$state/$fw.yaml" >/dev/null
    for w in $(kubectl get -f "$state/$fw.yaml" -o name | grep -E 'deployment|statefulset'); do
      kubectl rollout status "$w" --timeout=600s >/dev/null
    done
    log=$results/$fw-$nodes.log
    { echo "# $fw nodes=$nodes slots=$slots passes=$passes"
      echo "# metadata=$metadata"; } > "$log"
    ok=0
    wait_job taxi && ok=1
    kubectl logs job/taxi >> "$log" || true
    grep -E '^(PASS|tasks ran)' "$log" || true
    if [ $ok = 0 ] || ! grep -q '^PASS' "$log"; then
      echo "FAIL: $fw on $nodes nodes"; tail -20 "$log"
      kubectl logs -l "app=$fw-taxi" --prefix --tail=20 || true
    fi
    kubectl delete -f "$state/$fw.yaml" --wait >/dev/null
  done
done
python3 bench/cloud/summarize.py "$results"

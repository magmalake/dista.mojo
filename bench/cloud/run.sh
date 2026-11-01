#!/bin/bash
# The benchmark on the EC2 cluster, in two parts.
#
#   bench/cloud/run.sh local    the reference: each framework scans the table
#                               in one process on the head node, no workers
#   bench/cloud/run.sh          the matrix: each framework on each node count,
#                               every query TAXI_PASSES times
#   NODES="3 6 12" FRAMEWORKS="dista rust ray" PASSES=5 SLOTS=16 bench/cloud/run.sh
#
# The one-process scan takes from minutes (dista) to most of an hour (Rust,
# Ray) on 1.4 billion rows, so it runs once, before the workers exist, and
# the matrix skips it (TAXI_MODE=distributed). Every run prints its answers,
# and summarize.py checks them against the references, which also have to
# agree with each other.
#
# SLOTS is how many single-threaded tasks each node runs at once: that many
# dista workers, that many Rust workers, or one Ray worker with that many
# CPUs. It defaults to the worker nodes' vCPUs.
#
# Everything lands in $RESULTS (default bench/cloud/results/<UTC date>), which
# is meant to be committed: <framework>-local.log and <framework>-<nodes>.log,
# meta.txt with the cluster and the table, and summary.md from summarize.py.
# Needs aws.sh up, build.sh and data.sh load first.
set -euo pipefail
cd "$(dirname "$0")/../.."
. bench/cloud/common.sh

metadata=$(cat "$state/metadata.txt")
passes=${PASSES:-5}
results=${RESULTS:-bench/cloud/results/$(date -u +%F)}
mkdir -p "$results"

# Runs job/taxi from $1 and appends its log to $2; says whether it passed.
run_job() {
  local manifest=$1 log=$2 app=$3
  kubectl delete -f "$manifest" --ignore-not-found --wait >/dev/null
  kubectl apply -f "$manifest" >/dev/null
  local ok=0 ready=1
  for w in $(kubectl get -f "$manifest" -o name | grep -E 'deployment|statefulset' || true); do
    if ! kubectl rollout status "$w" --timeout=600s >/dev/null; then
      # One stuck rollout fails this run, not the rest of the matrix.
      echo "FAIL: $w didn't become ready" | tee -a "$log"
      kubectl get pods -o wide | grep -v Running | head -20 >> "$log"
      ready=0
    fi
  done
  [ $ready = 1 ] && wait_job taxi && ok=1
  kubectl logs job/taxi >> "$log" || true
  grep -E '^(PASS|tasks ran)' "$log" || true
  if [ $ok = 0 ] || ! grep -q '^PASS' "$log"; then
    echo "FAIL: see $log"; tail -20 "$log"
    kubectl logs -l "app=$app" --prefix --tail=20 || true
  fi
  kubectl delete -f "$manifest" --wait >/dev/null
}

if [ "${1:-}" = local ]; then
  aws_secret
  for fw in ${FRAMEWORKS:-dista rust ray}; do
    case $fw in
      dista) image=dista:taxi; command='["/opt/dista/bin/dista"]' ;;
      rust)  image=dista-rust:taxi; command='["/usr/local/bin/taxi-rs"]' ;;
      ray)   image=dista-ray:taxi; command='["python", "/app/taxi_ray.py"]' ;;
    esac
    echo "== $fw, one process on the head node"
    sed -e "s|@APP@|$fw-taxi|g" -e "s|@IMAGE@|$image|g" -e "s|@COMMAND@|$command|g" \
      -e "s|@TAXI_METADATA@|$metadata|g" bench/cloud/k8s/local.yaml > "$state/$fw-local.yaml"
    log=$results/$fw-local.log
    { echo "# $fw local"; echo "# metadata=$metadata"; } > "$log"
    run_job "$state/$fw-local.yaml" "$log" "$fw-taxi"
  done
  python3 bench/cloud/summarize.py "$results" | tee "$results/summary.md"
  exit 0
fi

slots=${SLOTS:-$(kubectl get nodes -l dista-bench/role=worker -o jsonpath='{.items[0].status.capacity.cpu}')}
{ echo "date: $(date -u +%FT%TZ)"
  echo "region: $AWS_REGION"
  echo "table: $metadata"
  echo "passes: $passes"
  echo "slots per node: $slots"
  echo "instances: $(aws ec2 describe-instances --filters Name=tag-key,Values=dista-bench \
    Name=instance-state-name,Values=running --query 'Reservations[].Instances[].InstanceType' \
    --output text | tr '\t' '\n' | sort | uniq -c | xargs)"
  kubectl get nodes -o wide -L dista-bench/role,dista-bench/rank
} > "$results/meta.txt"

for nodes in ${NODES:-3 6 12}; do
  for fw in ${FRAMEWORKS:-dista rust ray}; do
    echo "== $fw on $nodes nodes, $slots slots each"
    aws_secret
    sed -e "s|@NODES@|$nodes|g" -e "s|@SLOTS@|$slots|g" \
      -e "s|@REPLICAS@|$((nodes * slots))|g" -e "s|@PASSES@|$passes|g" \
      -e "s|@TAXI_METADATA@|$metadata|g" "bench/cloud/k8s/$fw.yaml" > "$state/$fw.yaml"
    log=$results/$fw-$nodes.log
    { echo "# $fw nodes=$nodes slots=$slots passes=$passes"
      echo "# metadata=$metadata"; } > "$log"
    run_job "$state/$fw.yaml" "$log" "$fw-taxi"
  done
done
python3 bench/cloud/summarize.py "$results" | tee "$results/summary.md"

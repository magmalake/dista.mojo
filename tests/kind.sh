#!/bin/bash
# Runs the smoke test on a kind cluster of three worker nodes: builds the
# image, loads it into the cluster, starts a head and three workers, and runs
# a driver job against them. Then the actor gate: a second driver job starts
# a checkpointed counter, and this script deletes the pod hosting it mid-run;
# the driver checks the count comes out exact. Needs docker, kind and kubectl.
#
# KEEP_CLUSTER=1 leaves the cluster up afterwards for poking at.
set -euo pipefail
cd "$(dirname "$0")/.."
cluster=${KIND_CLUSTER:-dista}

docker build -f deploy/Dockerfile -t dista:smoke .
mkdir -p /tmp/dista-checkpoints
if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --name "$cluster" --config deploy/kind.yaml --wait 120s
fi
[ "${KEEP_CLUSTER:-0}" = 1 ] || trap 'kind delete cluster --name "$cluster"' EXIT
kind load docker-image dista:smoke --name "$cluster"

ctx=(--context "kind-$cluster")
kubectl "${ctx[@]}" delete job dista-driver dista-actor-driver --ignore-not-found
kubectl "${ctx[@]}" apply -f deploy/k8s/cluster.yaml
kubectl "${ctx[@]}" rollout status statefulset/dista-head --timeout=120s
kubectl "${ctx[@]}" rollout status deployment/dista-worker --timeout=120s
kubectl "${ctx[@]}" get pods -o wide

kubectl "${ctx[@]}" apply -f deploy/k8s/driver-job.yaml
status=0
kubectl "${ctx[@]}" wait --for=condition=complete job/dista-driver --timeout=300s || status=1
kubectl "${ctx[@]}" logs job/dista-driver || true
if [ $status != 0 ] || ! kubectl "${ctx[@]}" logs job/dista-driver | grep -q '^PASS smoke on 3 nodes'; then
  echo "FAIL: the driver job did not pass"
  kubectl "${ctx[@]}" get pods -o wide
  kubectl "${ctx[@]}" logs -l app=dista --prefix --tail=50 || true
  exit 1
fi
echo "PASS kind: head, 3 workers on 3 nodes and a driver job"

# The actor gate. The driver prints the counter's pod once it's up; delete
# that pod a couple of seconds into its 2,000 calls.
kubectl "${ctx[@]}" apply -f deploy/k8s/actor-job.yaml
pod=""
for _ in $(seq 120); do
  pod=$(kubectl "${ctx[@]}" logs job/dista-actor-driver 2>/dev/null | awk '/^ACTOR_POD/ {print $2}' || true)
  [ -n "$pod" ] && break
  sleep 1
done
if [ -z "$pod" ]; then
  echo "FAIL: the actor driver never said where the counter is"
  kubectl "${ctx[@]}" logs job/dista-actor-driver || true
  exit 1
fi
sleep 2
# Without grace: the worker is its container's PID 1, which ignores SIGTERM,
# so a plain delete would leave it serving for the 30 s grace period.
echo "deleting the counter's pod $pod"
kubectl "${ctx[@]}" delete pod "$pod" --grace-period=0 --force --wait=false
status=1
for _ in $(seq 300); do
  done_=$(kubectl "${ctx[@]}" get job dista-actor-driver -o jsonpath='{.status.succeeded}/{.status.failed}')
  case "$done_" in
    1/*) status=0; break ;;
    */1) break ;;
  esac
  sleep 1
done
kubectl "${ctx[@]}" logs job/dista-actor-driver || true
if [ $status != 0 ] || ! kubectl "${ctx[@]}" logs job/dista-actor-driver | grep -q '^PASS actor restored'; then
  echo "FAIL: the actor driver did not pass"
  kubectl "${ctx[@]}" get pods -o wide
  kubectl "${ctx[@]}" logs -l app=dista --prefix --tail=50 || true
  exit 1
fi
echo "PASS kind: the counter's pod was deleted mid-run and the count is exact"

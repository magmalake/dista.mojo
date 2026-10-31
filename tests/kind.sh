#!/bin/bash
# Runs the smoke test on a kind cluster of three worker nodes: builds the
# image, loads it into the cluster, starts a head and three workers, and runs
# a driver job against them. Needs docker, kind and kubectl.
#
# KEEP_CLUSTER=1 leaves the cluster up afterwards for poking at.
set -euo pipefail
cd "$(dirname "$0")/.."
cluster=${KIND_CLUSTER:-dista}

docker build -f deploy/Dockerfile -t dista:smoke .
if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --name "$cluster" --config deploy/kind.yaml --wait 120s
fi
[ "${KEEP_CLUSTER:-0}" = 1 ] || trap 'kind delete cluster --name "$cluster"' EXIT
kind load docker-image dista:smoke --name "$cluster"

ctx=(--context "kind-$cluster")
kubectl "${ctx[@]}" delete job dista-driver --ignore-not-found
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

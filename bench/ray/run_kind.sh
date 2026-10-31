#!/bin/bash
# bench/ray/taxi_ray.py on the kind cluster tests/kind_taxi.sh uses: the same
# nodes, the same warehouse copy at /data/wh, one worker per node. Run
# tests/kind_taxi.sh with KEEP_CLUSTER=1 first, or this creates the cluster
# and copies the warehouse itself.
#
#   TAXI_THREADS    pyarrow threads per process (default 1, like a dista
#                   task); "" for pyarrow's default
#   KEEP_CLUSTER=1  leave the cluster up afterwards
set -euo pipefail
cd "$(dirname "$0")/../.."
cluster=${KIND_CLUSTER:-dista}
threads=${TAXI_THREADS-1}
warehouse=$(cd "${TAXI_WAREHOUSE:-../taxibench.example/build/warehouse}" && pwd -P)
metadata=$(cat "$warehouse/metadata_location.txt")

stage=build/ray-context
rm -rf "$stage" && mkdir -p "$stage/dista.mojo/bench/ray" "$stage/taxibench.example/python"
cp bench/ray/taxi_ray.py "$stage/dista.mojo/bench/ray/"
rsync -a --exclude __pycache__ ../taxibench.example/python/taxibench "$stage/taxibench.example/python/"
docker build -f bench/ray/Dockerfile -t dista-ray:taxi "$stage"

if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --name "$cluster" --config deploy/kind.yaml --wait 120s
fi
[ "${KEEP_CLUSTER:-0}" = 1 ] || trap 'kind delete cluster --name "$cluster"' EXIT
kind load docker-image dista-ray:taxi --name "$cluster"
for node in $(kind get nodes --name "$cluster" | grep -v control-plane); do
  if ! docker exec "$node" test -f /data/wh/metadata_location.txt; then
    docker exec "$node" mkdir -p /data/wh
    docker cp "$warehouse/." "$node:/data/wh/"
  fi
done

ctx=(--context "kind-$cluster")
sed -e "s|@TAXI_METADATA@|$metadata|" -e "s|@WAREHOUSE@|$warehouse|" \
  -e "s|@TAXI_THREADS@|$threads|" bench/ray/k8s.yaml > build/ray.yaml
kubectl "${ctx[@]}" delete -f build/ray.yaml --ignore-not-found --wait
kubectl "${ctx[@]}" apply -f build/ray.yaml
kubectl "${ctx[@]}" rollout status statefulset/ray-head --timeout=180s
kubectl "${ctx[@]}" rollout status deployment/ray-worker --timeout=180s
for _ in $(seq 600); do
  [ -n "$(kubectl "${ctx[@]}" get job ray-taxi -o jsonpath='{.status.succeeded}{.status.failed}')" ] && break
  sleep 2
done
kubectl "${ctx[@]}" logs job/ray-taxi | grep -v "WARNING\|^\s*$" || true
[ "$(kubectl "${ctx[@]}" get job ray-taxi -o jsonpath='{.status.succeeded}')" = 1 ] || {
  echo "FAIL: the ray job did not pass"
  kubectl "${ctx[@]}" get pods -o wide
  kubectl "${ctx[@]}" logs -l app=ray-taxi --prefix --tail=30 || true
  exit 1
}

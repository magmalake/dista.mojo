#!/bin/bash
# Runs the smoke test on a kind cluster of three worker nodes: builds the
# image, loads it into the cluster, starts a head and three workers, and runs
# a driver job against them. Then the actor gate: a second driver job starts
# a checkpointed counter, and this script deletes the pod hosting it mid-run;
# the driver checks the count comes out exact. Every pod's /metrics is read
# along the way: the head's count of finished tasks climbs, and every worker
# reports the tasks it ran. Needs docker, kind and kubectl.
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

# The head is `dista head`; its status lists the three workers.
status_log=$(mktemp)
kubectl "${ctx[@]}" exec dista-head-0 -- /opt/dista/bin/dista status --head localhost:6390 \
  | tee "$status_log"
grep -q '^head: 3 workers' "$status_log" || { echo "FAIL: dista status"; exit 1; }

# A pod's metric, read through the API server's proxy to its port 9090.
metric() {
  kubectl "${ctx[@]}" get --raw "/api/v1/namespaces/default/pods/$1:9090/proxy/metrics" \
    | awk -v m="$2" '$1 == m {print $2}'
}
ok='dista_tasks_finished_total{outcome="ok"}'
before=$(metric dista-head-0 "$ok")
echo "the head has finished $before tasks"
for p in $(kubectl "${ctx[@]}" get pods -l role=worker -o name | sed 's|pod/||'); do
  ran=$(metric "$p" 'dista_worker_tasks_finished_total{outcome="ok"}')
  echo "worker $p ran $ran tasks"
  [ "${ran:-0}" -gt 0 ] || { echo "FAIL: worker $p reports no tasks"; exit 1; }
done

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
# A plain delete: the worker exits on the SIGTERM it sends.
echo "deleting the counter's pod $pod"
kubectl "${ctx[@]}" delete pod "$pod" --wait=false
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

after=$(metric dista-head-0 "$ok")
restarts=$(metric dista-head-0 dista_actor_restarts_total)
echo "the head has finished $after tasks, and restarted $restarts actors"
if [ "${after:-0}" -le "${before:-0}" ] || [ "${restarts:-0}" -lt 1 ]; then
  echo "FAIL: the head's metrics didn't move"; exit 1
fi
echo "PASS kind: every pod's /metrics answers, and the head's count climbed"

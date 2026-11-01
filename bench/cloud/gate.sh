#!/bin/bash
# The 5-minute AWS gate: head + 3 workers, timed builds, 3 frameworks on 3
# nodes, 2 passes each, then teardown whatever happens.
set -uo pipefail
cd "$(dirname "$0")/../.."
export AWS_PROFILE=${AWS_PROFILE:-dista-bench} AWS_REGION=${AWS_REGION:-us-east-2}
export KUBECONFIG=build/cloud/kubeconfig
export RESULTS=${RESULTS:-bench/cloud/results/gate-$(date -u +%Y-%m-%d)}
trap 'echo "== teardown $(date -u +%T)"; bench/cloud/aws.sh down' EXIT
echo "== up $(date -u +%T)"
bench/cloud/aws.sh up 3 || exit 1
echo "== build $(date -u +%T)"
bench/cloud/build.sh || exit 1
echo "== run $(date -u +%T)"
NODES=3 PASSES=2 bench/cloud/run.sh
echo "== done $(date -u +%T)"

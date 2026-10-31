# Shared by data.sh and run.sh: the AWS settings, the cluster's kubeconfig,
# and the `aws` Secret that hands the pods this machine's credentials.
export AWS_PROFILE=${AWS_PROFILE:-dista-bench}
export AWS_REGION=${AWS_REGION:-us-east-2}
state=build/cloud
export KUBECONFIG=$state/kubeconfig
account=$(aws sts get-caller-identity --query Account --output text)
bucket=${BENCH_BUCKET:-dista-bench-$account-$AWS_REGION}

# The pods read S3 with the static AWS_* variables every reader understands;
# `aws login` credentials are temporary, so the Secret is refreshed per run.
aws_secret() {
  aws configure export-credentials --format env-no-export \
    | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > "$state/aws.env"
  echo "AWS_REGION=$AWS_REGION" >> "$state/aws.env"
  kubectl create secret generic aws --from-env-file="$state/aws.env" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  rm -f "$state/aws.env"
}

# Waits for job/$1 to finish and says whether it succeeded.
wait_job() {
  while true; do
    local s
    s=$(kubectl get "job/$1" -o jsonpath='{.status.succeeded}{.status.failed}')
    [ -n "$s" ] && break
    sleep 5
  done
  [ "$(kubectl get "job/$1" -o jsonpath='{.status.succeeded}')" = 1 ]
}

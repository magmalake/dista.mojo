# Shared by data.sh and run.sh: the AWS settings, the cluster's kubeconfig,
# and the `aws` Secret that hands the pods this machine's credentials.
export AWS_PROFILE=${AWS_PROFILE:-dista-bench}
export AWS_REGION=${AWS_REGION:-us-east-2}
state=build/cloud
export KUBECONFIG=$state/kubeconfig
account=$(aws sts get-caller-identity --query Account --output text)
bucket=${BENCH_BUCKET:-dista-bench-$account-$AWS_REGION}

# The pods take S3 credentials from the node's IAM role (aws.sh's
# dista-bench-node), so the `aws` Secret only names the Region. It used to
# carry this machine's `aws login` credentials, which last 15 minutes: less
# than a load or a run.
aws_secret() {
  kubectl create secret generic aws --from-literal="AWS_REGION=$AWS_REGION" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# Copies $1 from the head node to $2.
from_head() {
  local ip
  ip=$(aws ec2 describe-instances --filters "Name=tag:dista-bench,Values=head" \
    "Name=instance-state-name,Values=running" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  scp -q -i "$state/key.pem" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o LogLevel=ERROR "ubuntu@$ip:$1" "$2"
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

#!/bin/bash
# A k3s cluster on EC2 for the cloud benchmark: one head node (the dista and
# Ray heads, every driver, the image builds) and N worker nodes, all the same
# Graviton instance type, in one subnet of the default VPC.
#
#   bench/cloud/aws.sh up 12     start the head and 12 workers (or add workers)
#   bench/cloud/aws.sh status    instances, nodes, and the hourly price
#   bench/cloud/aws.sh ssh [n]   a shell on the head, or on worker n
#   bench/cloud/aws.sh down      terminate everything; keeps the S3 bucket
#
# Every instance powers itself off after $TTL_MINUTES (default 240) and is
# terminated when it does, so a forgotten cluster stops billing on its own.
#
#   AWS_PROFILE    default dista-bench
#   AWS_REGION     default us-east-2 (the project's Region)
#   INSTANCE_TYPE  default c7g.4xlarge (16 vCPU, 32 GiB)
#   SPOT=1         spot instances instead of on-demand
set -euo pipefail
cd "$(dirname "$0")/../.."

export AWS_PROFILE=${AWS_PROFILE:-dista-bench}
export AWS_REGION=${AWS_REGION:-us-east-2}
type=${INSTANCE_TYPE:-c7g.4xlarge}
ttl=${TTL_MINUTES:-240}
tag=dista-bench
state=build/cloud
mkdir -p "$state"
key="$state/key.pem"
ssh_opts=(-i "$key" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)

instances() {  # id, private ip, public ip, role, for running instances
  aws ec2 describe-instances \
    --filters "Name=tag:$tag,Values=*" "Name=instance-state-name,Values=pending,running" \
    --query "Reservations[].Instances[].[InstanceId,PrivateIpAddress,PublicIpAddress,Tags[?Key=='$tag']|[0].Value]" \
    --output text | sort -k4
}

head_ip() { instances | awk '$4=="head" {print $3}'; }

head_ssh() { ssh "${ssh_opts[@]}" -A "ubuntu@$(head_ip)" "$@"; }

network() {
  if ! aws ec2 describe-key-pairs --key-names "$tag" >/dev/null 2>&1; then
    aws ec2 create-key-pair --key-name "$tag" --key-type ed25519 \
      --query KeyMaterial --output text > "$key"
    chmod 600 "$key"
  fi
  [ -f "$key" ] || { echo "key pair $tag exists but $key is missing; run down first"; exit 1; }
  vpc=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
  # One subnet, so node-to-node traffic never crosses zones.
  subnet=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" "Name=default-for-az,Values=true" \
    --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
  sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$tag" "Name=vpc-id,Values=$vpc" \
    --query 'SecurityGroups[0].GroupId' --output text)
  if [ "$sg" = None ]; then
    sg=$(aws ec2 create-security-group --group-name "$tag" --vpc-id "$vpc" \
      --description "dista cloud benchmark" --query GroupId --output text)
    aws ec2 authorize-security-group-ingress --group-id "$sg" \
      --ip-permissions "IpProtocol=-1,UserIdGroupPairs=[{GroupId=$sg}]" >/dev/null
  fi
  # SSH and the Kubernetes API from this machine only.
  me=$(curl -s https://checkip.amazonaws.com)/32
  for port in 22 6443; do
    aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp \
      --port $port --cidr "$me" >/dev/null 2>&1 || true
  done
}

launch() {  # role count user-data-file
  local market=()
  [ "${SPOT:-0}" = 1 ] && market=(--instance-market-options 'MarketType=spot')
  local ami
  ami=$(aws ssm get-parameter \
    --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id \
    --query Parameter.Value --output text)
  aws ec2 run-instances --image-id "$ami" --instance-type "$type" --count "$2" \
    --key-name "$tag" --security-group-ids "$sg" --subnet-id "$subnet" \
    --instance-initiated-shutdown-behavior terminate "${market[@]}" \
    --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$([ "$1" = head ] && echo 200 || echo 40),VolumeType=gp3,DeleteOnTermination=true}" \
    --metadata-options HttpTokens=required \
    --tag-specifications "ResourceType=instance,Tags=[{Key=$tag,Value=$1},{Key=Name,Value=$tag-$1}]" \
    --user-data "file://$3" --query 'Instances[].InstanceId' --output text
}

up() {
  local want=${1:?how many workers}
  network
  [ -f "$state/token" ] || openssl rand -hex 24 > "$state/token"
  local token
  token=$(cat "$state/token")
  if [ -z "$(head_ip)" ]; then
    cat > "$state/head-init.sh" <<EOF
#!/bin/bash
shutdown -P +$ttl
apt-get update && apt-get install -y docker.io rsync
usermod -aG docker ubuntu
pub=\$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' | \
  xargs -I{} curl -s -H 'X-aws-ec2-metadata-token: {}' http://169.254.169.254/latest/meta-data/public-ipv4)
curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=v1.33 K3S_TOKEN=$token sh -s - server \
  --disable traefik --disable servicelb --write-kubeconfig-mode 644 \
  --tls-san "\$pub" --node-label $tag/role=head
EOF
    echo "launching the head ($type)"
    launch head 1 "$state/head-init.sh" >/dev/null
    until [ -n "$(instances | awk '$4=="head" && $3!="None" {print $3}')" ]; do sleep 5; done
  fi
  local head_private
  head_private=$(instances | awk '$4=="head" {print $2}')
  local have
  have=$(instances | awk '$4=="worker"' | wc -l | tr -d ' ')
  if [ "$have" -lt "$want" ]; then
    cat > "$state/worker-init.sh" <<EOF
#!/bin/bash
shutdown -P +$ttl
curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=v1.33 K3S_TOKEN=$token \
  K3S_URL=https://$head_private:6443 sh -s - agent --node-label $tag/role=worker
EOF
    echo "launching $((want - have)) workers ($type)"
    launch worker $((want - have)) "$state/worker-init.sh" >/dev/null
  fi

  echo "waiting for the head's API"
  until head_ssh test -f /etc/rancher/k3s/k3s.yaml 2>/dev/null; do sleep 5; done
  head_ssh cat /etc/rancher/k3s/k3s.yaml | sed "s/127.0.0.1/$(head_ip)/" > "$state/kubeconfig"
  export KUBECONFIG=$state/kubeconfig
  echo "waiting for $want workers to join"
  until [ "$(kubectl get nodes -l $tag/role=worker --no-headers 2>/dev/null | grep -cw Ready)" -ge "$want" ]; do
    sleep 5
  done
  # Ranks 0..N-1, so a run on n nodes can select `rank < n`.
  local i=0
  for node in $(kubectl get nodes -l $tag/role=worker -o name | sort); do
    kubectl label --overwrite "$node" "$tag/rank=$i" >/dev/null
    i=$((i + 1))
  done
  until head_ssh docker info >/dev/null 2>&1; do sleep 5; done
  kubectl get nodes -L $tag/role,$tag/rank
  echo "KUBECONFIG=$state/kubeconfig"
}

case "${1:-}" in
  up) up "${2:-}" ;;
  status)
    instances
    n=$(instances | wc -l | tr -d ' ')
    echo "$n x $type"
    [ -f "$state/kubeconfig" ] && KUBECONFIG=$state/kubeconfig kubectl get nodes -L $tag/role,$tag/rank || true
    ;;
  ssh)
    if [ -z "${2:-}" ]; then
      head_ssh
    else
      ip=$(KUBECONFIG=$state/kubeconfig kubectl get nodes -l "$tag/rank=$2" \
        -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
      head_ssh -t ssh -o StrictHostKeyChecking=no "ubuntu@$ip"
    fi
    ;;
  down)
    ids=$(instances | awk '{print $1}')
    if [ -n "$ids" ]; then
      # shellcheck disable=SC2086
      aws ec2 terminate-instances --instance-ids $ids >/dev/null
      # shellcheck disable=SC2086
      aws ec2 wait instance-terminated --instance-ids $ids
    fi
    sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$tag" \
      --query 'SecurityGroups[0].GroupId' --output text)
    [ "$sg" != None ] && aws ec2 delete-security-group --group-id "$sg"
    aws ec2 delete-key-pair --key-name "$tag" >/dev/null
    rm -f "$key" "$state/kubeconfig" "$state/token"
    echo "terminated; the bucket stays (bench/cloud/data.sh drop removes it)"
    ;;
  *) sed -n '2,17p' "$0"; exit 2 ;;
esac

#!/bin/bash
# Builds the three benchmark images on the cluster's head node, timed, and
# loads them into every node's k3s. Each build starts from an empty Docker
# cache, base images included, so the times are what a fresh machine pays.
#
#   bench/cloud/build.sh [dista|ray|rust ...]   (default: all three)
#
# Writes build/cloud/results/build.tsv: per image, the whole build's wall
# time, the dependency step (pixi install / cargo building the dependencies /
# pip install), the compile step (mojo build / cargo building the crate; Ray
# has none), and the image's size on disk and gzip-compressed. Needs `bench/cloud/aws.sh up` first. The sibling
# checkouts are staged from ../ as tests/kind_taxi.sh does, so they must be
# at the revisions under test (iceberg.mojo with to_batches_for_tasks).
set -euo pipefail
cd "$(dirname "$0")/../.."
state=build/cloud
key="$state/key.pem"
ssh_opts=(-i "$key" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
export AWS_PROFILE=${AWS_PROFILE:-dista-bench} AWS_REGION=${AWS_REGION:-us-east-2}
export KUBECONFIG=$state/kubeconfig
head=$(aws ec2 describe-instances \
  --filters "Name=tag:dista-bench,Values=head" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
on_head() { ssh "${ssh_opts[@]}" -A "ubuntu@$head" "$@"; }
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(dista ray rust)

# The three build contexts, as the kind scripts stage them.
stage=build/cloud/context
rm -rf "$stage" && mkdir -p "$stage/dista" "$stage/ray/dista.mojo/bench/ray" \
  "$stage/ray/taxibench.example/python" "$stage/rust"
for repo in dista memory-region arrow-mlake parquet iceberg threads thrift \
    hashes snappy deflate avro roaring objectstore zstd lz4 brotli sqlite \
    postgres; do
  rsync -a --exclude .pixi --exclude build --exclude .git --exclude target \
    "../$repo.mojo/" "$stage/dista/$repo.mojo/"
done
cp bench/ray/taxi_ray.py "$stage/ray/dista.mojo/bench/ray/"
cp bench/ray/Dockerfile "$stage/ray/Dockerfile"
rsync -a --exclude __pycache__ ../taxibench.example/python/taxibench "$stage/ray/taxibench.example/python/"
rsync -a --exclude target bench/rust/ "$stage/rust/"
rsync -a --delete -e "ssh ${ssh_opts[*]}" "$stage/" "ubuntu@$head:ctx/"

mkdir -p "$state/results"
out="$state/results/build.tsv"
[ -f "$out" ] || printf 'image\ttotal_s\tdeps_s\tcompile_s\tdisk_mb\tgzip_mb\tinstance\n' > "$out"
instance=$(on_head 'curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60" | xargs -I{} curl -s -H "X-aws-ec2-metadata-token: {}" http://169.254.169.254/latest/meta-data/instance-type')

for image in "${images[@]}"; do
  case $image in
    dista) tagname=dista:taxi; dockerfile=dista/dista.mojo/deploy/Dockerfile.taxi; ctx=dista
           deps='pixi install'; compile='mojo build' ;;
    ray)   tagname=dista-ray:taxi; dockerfile=ray/Dockerfile; ctx=ray
           deps='pip install'; compile='' ;;
    rust)  tagname=dista-rust:taxi; dockerfile=rust/Dockerfile; ctx=rust
           deps='fn main'; compile='touch src/main.rs' ;;
    *) echo "unknown image $image"; exit 2 ;;
  esac
  echo "== building $tagname on $instance"
  on_head "docker system prune -af >/dev/null && docker builder prune -af >/dev/null"
  t0=$(date +%s)
  on_head "cd ctx && DOCKER_BUILDKIT=1 docker build --no-cache --progress=plain \
    -f $dockerfile -t $tagname $ctx" > "$state/results/build-$image.log" 2>&1
  total=$(( $(date +%s) - t0 ))
  # BuildKit's plain log names each step (`#7 [build 5/7] RUN ...`) and ends
  # it with `#7 DONE 41.2s`; sum the steps whose command matches.
  step_s() {
    awk -v pat="$1" '
      /^#[0-9]+ \[/ && index($0, "RUN") && index($0, pat) { want[$1] = 1 }
      /^#[0-9]+ DONE/ && want[$1] { sub("s", "", $3); s += $3 }
      END { printf "%.1f", s }' "$state/results/build-$image.log"
  }
  deps_s=$(step_s "$deps")
  compile_s=-
  [ -n "$compile" ] && compile_s=$(step_s "$compile")
  disk=$(on_head "docker image inspect $tagname --format '{{.Size}}'")
  gz=$(on_head "docker save $tagname | gzip -1 | wc -c")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$image" "$total" "$deps_s" "$compile_s" \
    "$((disk / 1000000))" "$((gz / 1000000))" "$instance" | tee -a "$out"

  echo "== loading $tagname into every node"
  on_head "docker save $tagname | sudo k3s ctr images import - >/dev/null"
  for ip in $(kubectl get nodes -l dista-bench/role=worker \
      -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}'); do
    on_head "docker save $tagname | ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR ubuntu@$ip 'sudo k3s ctr images import - >/dev/null'" &
  done
  wait
done
column -t -s $'\t' "$out"

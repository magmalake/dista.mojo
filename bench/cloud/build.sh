#!/bin/bash
# Builds the three benchmark images on the cluster's head node, timed, and
# loads them into every node's k3s. Each build starts from an empty Docker
# cache, base images included, so the times are what a fresh machine pays.
#
#   bench/cloud/build.sh [dista|ray|rust ...]   (default: all three)
#
# What gets built is pinned: dista.mojo at this checkout's commit, which must
# be committed and pushed, and its siblings at the commits in revs.txt. The
# head node clones them itself, before any timing starts.
#
# Writes to $RESULTS (default bench/cloud/results/<UTC date>): build.tsv with,
# per image, the whole build's wall time, the dependency step (pixi install /
# cargo building the dependencies / pip install), the compile step (mojo build
# / cargo building the crate; Ray has none) and the image's size on disk and
# gzip-compressed; each build's log, gzipped; and revs.txt, everything that
# was built. Needs `bench/cloud/aws.sh up` first.
set -euo pipefail
cd "$(dirname "$0")/../.."
state=build/cloud
key="$state/key.pem"
ssh_opts=(-i "$key" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
export AWS_PROFILE=${AWS_PROFILE:-dista-bench} AWS_REGION=${AWS_REGION:-us-east-2}
export KUBECONFIG=$state/kubeconfig
results=${RESULTS:-bench/cloud/results/$(date -u +%F)}
mkdir -p "$results"
head=$(aws ec2 describe-instances \
  --filters "Name=tag:dista-bench,Values=head" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
on_head() { ssh "${ssh_opts[@]}" "ubuntu@$head" "$@"; }
images="${*:-dista ray rust}"

dista_rev=$(git rev-parse HEAD)
if ! git diff --quiet HEAD -- . ":(exclude)bench/cloud/results"; then
  echo "dista.mojo has uncommitted changes; build.sh builds commits"; exit 1
fi
if [ -z "$(git branch -r --contains "$dista_rev")" ]; then
  echo "dista.mojo $dista_rev isn't pushed; the head node clones it"; exit 1
fi
{ echo "dista.mojo https://github.com/magmalake/dista.mojo.git $dista_rev"
  grep -v '^#' bench/cloud/revs.txt | grep .; } > "$results/revs.txt"

# The three build contexts, as the kind scripts stage them, from fresh clones.
on_head 'cat > revs.txt' < "$results/revs.txt"
on_head 'bash -s' <<'EOF'
set -euo pipefail
rm -rf src ctx && mkdir -p src ctx/dista ctx/rust ctx/ray/dista.mojo/bench/ray ctx/ray/taxibench.example/python
while read -r dir url rev; do
  git init -q "src/$dir"
  git -C "src/$dir" fetch -q --depth 1 "$url" "$rev"
  git -C "src/$dir" checkout -q FETCH_HEAD
  case $dir in
    *.mojo) rsync -a --exclude .git "src/$dir/" "ctx/dista/$dir/" ;;
  esac
done < revs.txt
cp src/dista.mojo/bench/ray/taxi_ray.py ctx/ray/dista.mojo/bench/ray/
cp src/dista.mojo/bench/ray/Dockerfile ctx/ray/Dockerfile
rsync -a --exclude __pycache__ src/taxibench.example/python/taxibench ctx/ray/taxibench.example/python/
rsync -a --exclude .git src/dista.mojo/bench/rust/ ctx/rust/
EOF

out="$results/build.tsv"
[ -f "$out" ] || printf 'image\ttotal_s\tdeps_s\tcompile_s\tdisk_mb\tgzip_mb\tinstance\n' > "$out"
instance=$(on_head 'curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60" | xargs -I{} curl -s -H "X-aws-ec2-metadata-token: {}" http://169.254.169.254/latest/meta-data/instance-type')

# BuildKit's plain log names each step (`#7 [build 5/7] RUN ...`) and ends it
# with `#7 DONE 41.2s`; this sums the steps whose command matches.
step_s() {
  awk -v pat="$1" '
    /^#[0-9]+ \[/ && index($0, "RUN") && index($0, pat) { want[$1] = 1 }
    /^#[0-9]+ DONE/ && want[$1] { sub("s", "", $3); s += $3 }
    END { printf "%.1f", s }' "$2"
}

for image in $images; do
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
  log=$state/build-$image.log
  on_head "docker system prune -af >/dev/null && docker builder prune -af >/dev/null"
  t0=$(date +%s)
  on_head "cd ctx && DOCKER_BUILDKIT=1 docker build --no-cache --progress=plain \
    -f $dockerfile -t $tagname $ctx" > "$log" 2>&1
  total=$(( $(date +%s) - t0 ))
  gzip -c "$log" > "$results/build-$image.log.gz"
  deps_s=$(step_s "$deps" "$log")
  compile_s=-
  [ -n "$compile" ] && compile_s=$(step_s "$compile" "$log")
  # The image as an uncompressed tar: with BuildKit's image store, `docker
  # image inspect` reports the compressed size instead.
  disk=$(on_head "docker save $tagname | wc -c")
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

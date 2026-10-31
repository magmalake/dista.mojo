#!/bin/bash
# The benchmark table in S3.
#
#   bench/cloud/data.sh load [2011-2024]   create the bucket, load the years
#   bench/cloud/data.sh drop               delete the bucket and everything in it
#
# The load runs on the cluster's head node (bench/cloud/aws.sh up 0 is
# enough), so the TLC downloads and the writes stay inside AWS. It ends by
# writing the table's metadata location to build/cloud/metadata.txt, which
# run.sh reads.
set -euo pipefail
cd "$(dirname "$0")/../.."
. bench/cloud/common.sh

case "${1:-}" in
  load)
    years=${2:-2011-2024}
    if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
      aws s3api create-bucket --bucket "$bucket" \
        --create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
      aws s3api put-public-access-block --bucket "$bucket" --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    fi
    aws_secret
    kubectl create configmap load-table --from-file=bench/cloud/load_table.py \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    kubectl delete job load --ignore-not-found --wait >/dev/null
    sed -e "s|@WAREHOUSE@|s3://$bucket/warehouse|" -e "s|@YEARS@|$years|" \
      bench/cloud/k8s/load.yaml | kubectl apply -f - >/dev/null
    until kubectl logs job/load >/dev/null 2>&1; do sleep 5; done
    kubectl logs -f job/load | tee "$state/load.log"
    wait_job load || { echo "FAIL: the load did not finish"; exit 1; }
    sed -n 's/^metadata: //p' "$state/load.log" > "$state/metadata.txt"
    echo "table: $(cat "$state/metadata.txt")"
    ;;
  drop)
    aws s3 rb "s3://$bucket" --force
    ;;
  *) sed -n '2,10p' "$0"; exit 2 ;;
esac

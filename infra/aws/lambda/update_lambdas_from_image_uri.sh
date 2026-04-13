#!/usr/bin/env bash
# Update all three JMI image Lambdas to a new ECR image URI (same handlers + env as deploy_ecr_create_update.sh).
# Usage (from repo root):
#   export AWS_REGION=ap-south-1
#   export BUCKET_NAME=jmi-dakshyadav-job-market-intelligence
#   ./infra/aws/lambda/update_lambdas_from_image_uri.sh 470441577506.dkr.ecr.ap-south-1.amazonaws.com/jmi-lambda:v20-abc123
#
# Used by: GitHub Actions workflow, CodeBuild post_build, or any environment with aws CLI (no Docker required here).
set -euo pipefail

IMAGE_URI="${1:-}"
if [[ -z "$IMAGE_URI" ]]; then
  echo "Usage: $0 <full-ecr-image-uri>" >&2
  exit 1
fi

AWS_REGION="${AWS_REGION:-ap-south-1}"
BUCKET_NAME="${BUCKET_NAME:-jmi-dakshyadav-job-market-intelligence}"

INGEST_FN="jmi-ingest-live"
SILVER_FN="jmi-transform-silver"
GOLD_FN="jmi-transform-gold"

wait_ready() {
  local fn="$1"
  local i st
  for i in $(seq 1 60); do
    st=$(aws lambda get-function-configuration --function-name "$fn" --region "$AWS_REGION" --query 'LastUpdateStatus' --output text 2>/dev/null || echo Unknown)
    if [[ "$st" == "Successful" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timeout waiting for $fn" >&2
  return 1
}

update_fn() {
  local fn_name="$1" cmd="$2" timeout="$3" memory="$4"
  aws lambda update-function-code --function-name "$fn_name" --image-uri "$IMAGE_URI" --region "$AWS_REGION" >/dev/null
  wait_ready "$fn_name"
  local ic
  ic=$(printf '{"Command":["%s"]}' "$cmd")
  aws lambda update-function-configuration --function-name "$fn_name" \
    --image-config "$ic" \
    --timeout "$timeout" --memory-size "$memory" \
    --region "$AWS_REGION" >/dev/null
  wait_ready "$fn_name"
}

update_fn "$INGEST_FN" "handlers.ingest_handler.handler" 120 512
update_fn "$SILVER_FN" "handlers.silver_handler.handler" 180 1024
update_fn "$GOLD_FN" "handlers.gold_handler.handler" 180 1024

aws lambda update-function-configuration --function-name "$INGEST_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME},JMI_SILVER_FUNCTION_NAME=${SILVER_FN}}" \
  --region "$AWS_REGION" >/dev/null
wait_ready "$INGEST_FN"

aws lambda update-function-configuration --function-name "$SILVER_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME},JMI_GOLD_FUNCTION_NAME=${GOLD_FN}}" \
  --region "$AWS_REGION" >/dev/null
wait_ready "$SILVER_FN"

aws lambda update-function-configuration --function-name "$GOLD_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME}}" \
  --region "$AWS_REGION" >/dev/null
wait_ready "$GOLD_FN"

echo "Lambdas updated to: $IMAGE_URI"

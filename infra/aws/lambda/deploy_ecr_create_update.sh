#!/usr/bin/env bash
# Build the Lambda container from infra/aws/lambda/Dockerfile, push to ECR, update all three functions.
# Usage (from repo root): ./infra/aws/lambda/deploy_ecr_create_update.sh <tag>
# Example: ./infra/aws/lambda/deploy_ecr_create_update.sh v20-$(date +%Y%m%d)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT_DIR"

AWS_REGION="${AWS_REGION:-ap-south-1}"
ACCOUNT_ID="${AWS_ACCOUNT_ID:-470441577506}"
BUCKET_NAME="${JMI_BUCKET:-jmi-dakshyadav-job-market-intelligence}"
REPO_NAME="jmi-lambda"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/jmi-lambda-exec-role"

INGEST_FN="jmi-ingest-live"
SILVER_FN="jmi-transform-silver"
GOLD_FN="jmi-transform-gold"

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  echo "Usage: $0 <ecr-image-tag>" >&2
  echo "Example: $0 v20-\$(date +%Y%m%d)" >&2
  exit 1
fi

ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_URI="${ECR_REGISTRY}/${REPO_NAME}:${TAG}"

echo "Logging in to ECR ${ECR_REGISTRY}..."
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY"

echo "Building ${IMAGE_URI} (linux/amd64)..."
docker build --platform linux/amd64 -f infra/aws/lambda/Dockerfile -t "${REPO_NAME}:${TAG}" .

docker tag "${REPO_NAME}:${TAG}" "$IMAGE_URI"

echo "Pushing ${IMAGE_URI}..."
docker push "$IMAGE_URI"

wait_for_ready () {
  local fn="$1"
  local status=""
  for _ in $(seq 1 60); do
    status=$(aws lambda get-function-configuration --function-name "$fn" --region "$AWS_REGION" --query 'LastUpdateStatus' --output text 2>/dev/null || echo "Unknown")
    if [[ "$status" == "Successful" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timeout waiting for $fn to become Successful (last=$status)" >&2
  return 1
}

create_image_function () {
  local fn_name="$1"
  local cmd="$2"
  local timeout="$3"
  local memory="$4"
  echo "Creating function: $fn_name"
  aws lambda create-function \
    --function-name "$fn_name" \
    --package-type Image \
    --code "ImageUri=${IMAGE_URI}" \
    --role "$ROLE_ARN" \
    --timeout "$timeout" \
    --memory-size "$memory" \
    --region "$AWS_REGION" \
    --image-config "{\"Command\":[\"${cmd}\"]}" >/dev/null
  wait_for_ready "$fn_name"
}

update_image_function () {
  local fn_name="$1"
  local timeout="$2"
  local memory="$3"
  echo "Updating function code: $fn_name -> $IMAGE_URI"
  aws lambda update-function-code \
    --function-name "$fn_name" \
    --image-uri "$IMAGE_URI" \
    --region "$AWS_REGION" >/dev/null
  wait_for_ready "$fn_name"
  echo "Updating timeout/memory: $fn_name"
  aws lambda update-function-configuration \
    --function-name "$fn_name" \
    --timeout "$timeout" \
    --memory-size "$memory" \
    --region "$AWS_REGION" >/dev/null
  wait_for_ready "$fn_name"
}

ensure_function () {
  local fn_name="$1"
  local cmd="$2"
  local timeout="$3"
  local memory="$4"
  if aws lambda get-function --function-name "$fn_name" --region "$AWS_REGION" >/dev/null 2>&1; then
    update_image_function "$fn_name" "$timeout" "$memory"
  else
    create_image_function "$fn_name" "$cmd" "$timeout" "$memory"
  fi
}

ensure_function "$INGEST_FN" "handlers.ingest_handler.handler" 120 512
ensure_function "$SILVER_FN" "handlers.silver_handler.handler" 180 1024
ensure_function "$GOLD_FN" "handlers.gold_handler.handler" 180 1024

echo "Setting environment variables..."
aws lambda update-function-configuration \
  --function-name "$INGEST_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME},JMI_SILVER_FUNCTION_NAME=${SILVER_FN}}" \
  --region "$AWS_REGION" >/dev/null
wait_for_ready "$INGEST_FN"

aws lambda update-function-configuration \
  --function-name "$SILVER_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME},JMI_GOLD_FUNCTION_NAME=${GOLD_FN}}" \
  --region "$AWS_REGION" >/dev/null
wait_for_ready "$SILVER_FN"

aws lambda update-function-configuration \
  --function-name "$GOLD_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME}}" \
  --region "$AWS_REGION" >/dev/null
wait_for_ready "$GOLD_FN"

echo "ECR deploy complete. Image: $IMAGE_URI"

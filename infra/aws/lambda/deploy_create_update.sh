#!/usr/bin/env bash
# Zip-based Lambda deploy. Use ONLY when functions use PackageType Zip.
# Current production functions are container images — use deploy_ecr_create_update.sh instead.
set -euo pipefail

AWS_REGION="ap-south-1"
ACCOUNT_ID="470441577506"
BUCKET_NAME="jmi-dakshyadav-job-market-intelligence"
ROLE_NAME="jmi-lambda-exec-role"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
ZIP_PATH="infra/aws/lambda/dist/jmi-lambda.zip"

INGEST_FN="jmi-ingest-live"
SILVER_FN="jmi-transform-silver"
GOLD_FN="jmi-transform-gold"

for fn in "$INGEST_FN" "$SILVER_FN" "$GOLD_FN"; do
  pkg=$(aws lambda get-function-configuration --function-name "$fn" --region "$AWS_REGION" --query 'PackageType' --output text 2>/dev/null || echo "Missing")
  if [[ "$pkg" == "Image" ]]; then
    echo "ERROR: $fn uses PackageType Image. Zip deploy is invalid for this function." >&2
    echo "Build and deploy with: ./infra/aws/lambda/deploy_ecr_create_update.sh <tag>" >&2
    echo "See infra/aws/lambda/README.md" >&2
    exit 1
  fi
done

create_or_update_function () {
  local fn_name="$1"
  local handler="$2"
  local timeout="$3"
  local memory="$4"

  if aws lambda get-function --function-name "$fn_name" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "Updating existing function: $fn_name"
    aws lambda update-function-code \
      --function-name "$fn_name" \
      --zip-file "fileb://${ZIP_PATH}" \
      --region "$AWS_REGION" >/dev/null

    aws lambda update-function-configuration \
      --function-name "$fn_name" \
      --handler "$handler" \
      --runtime python3.11 \
      --timeout "$timeout" \
      --memory-size "$memory" \
      --region "$AWS_REGION" >/dev/null
  else
    echo "Creating function: $fn_name"
    aws lambda create-function \
      --function-name "$fn_name" \
      --runtime python3.11 \
      --handler "$handler" \
      --role "$ROLE_ARN" \
      --timeout "$timeout" \
      --memory-size "$memory" \
      --zip-file "fileb://${ZIP_PATH}" \
      --region "$AWS_REGION" >/dev/null
  fi
}

create_or_update_function "$INGEST_FN" "handlers.ingest_handler.handler" 120 512
create_or_update_function "$SILVER_FN" "handlers.silver_handler.handler" 180 1024
create_or_update_function "$GOLD_FN" "handlers.gold_handler.handler" 180 1024

aws lambda update-function-configuration \
  --function-name "$INGEST_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME},JMI_SILVER_FUNCTION_NAME=${SILVER_FN}}" \
  --region "$AWS_REGION" >/dev/null

aws lambda update-function-configuration \
  --function-name "$SILVER_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME},JMI_GOLD_FUNCTION_NAME=${GOLD_FN}}" \
  --region "$AWS_REGION" >/dev/null

aws lambda update-function-configuration \
  --function-name "$GOLD_FN" \
  --environment "Variables={JMI_DATA_ROOT=s3://${BUCKET_NAME},JMI_BUCKET=${BUCKET_NAME}}" \
  --region "$AWS_REGION" >/dev/null

echo "Create+update flow complete."


#!/usr/bin/env bash
# Refreshes optional zip archive under lambda_legacy/ (audit/download only — image Lambdas do not use it).
# Requires Docker (see package_and_zip.sh).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT_DIR"

AWS_REGION="${AWS_REGION:-ap-south-1}"
BUCKET_NAME="${JMI_BUCKET:-jmi-dakshyadav-job-market-intelligence}"
S3_KEY="${JMI_LAMBDA_ZIP_S3_KEY:-lambda_legacy/jmi-lambda.zip}"

./infra/aws/lambda/package_and_zip.sh
aws s3 cp infra/aws/lambda/dist/jmi-lambda.zip "s3://${BUCKET_NAME}/${S3_KEY}" --region "$AWS_REGION"
echo "Uploaded s3://${BUCKET_NAME}/${S3_KEY}"

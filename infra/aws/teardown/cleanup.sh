#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="ap-south-1"
ACCOUNT_ID="470441577506"
BUCKET_NAME="jmi-dakshyadav-job-market-intelligence"

aws scheduler delete-schedule --name jmi-ingest-10min --group-name default --region "$AWS_REGION" || true

aws lambda delete-function --function-name jmi-ingest-live --region "$AWS_REGION" || true
aws lambda delete-function --function-name jmi-transform-silver --region "$AWS_REGION" || true
aws lambda delete-function --function-name jmi-transform-gold --region "$AWS_REGION" || true

aws s3 rm "s3://${BUCKET_NAME}/bronze/" --recursive || true
aws s3 rm "s3://${BUCKET_NAME}/silver/" --recursive || true
aws s3 rm "s3://${BUCKET_NAME}/gold/" --recursive || true
aws s3 rm "s3://${BUCKET_NAME}/quality/" --recursive || true
aws s3 rm "s3://${BUCKET_NAME}/health/" --recursive || true

aws iam detach-role-policy --role-name jmi-lambda-exec-role --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/jmi-lambda-exec-policy" || true
aws iam delete-policy --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/jmi-lambda-exec-policy" || true
aws iam delete-role --role-name jmi-lambda-exec-role || true

aws iam delete-role-policy --role-name jmi-eventbridge-invoke-lambda-role --policy-name jmi-eventbridge-invoke-policy || true
aws iam delete-role --role-name jmi-eventbridge-invoke-lambda-role || true


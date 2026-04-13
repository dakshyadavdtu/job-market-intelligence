# AWS Deployment Boundary

This directory holds AWS-only assets (IAM JSON, deployment scripts, EventBridge setup, Athena DDL execution scripts).

No AWS commands should be auto-executed without explicit approval.

## Lambda

See **`lambda/README.md`**: ECR image deploy vs optional zip/S3 archive; scripts `deploy_ecr_create_update.sh`, `package_and_zip.sh`, `deploy_create_update.sh` (zip-only).

## Planned Files (next phase)

- `iam/lambda-ingest-policy.json`
- `iam/lambda-transform-policy.json`
- `lambda/packaging.sh`
- `eventbridge/schedule.json`
- `athena/bootstrap.sql`
- `teardown/cleanup.sh`

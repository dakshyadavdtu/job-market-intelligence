# JMI Lambda packaging and deployment

## What actually runs in AWS (current account/region)

The three functions **`jmi-ingest-live`**, **`jmi-transform-silver`**, and **`jmi-transform-gold`** are deployed as **container images** (`PackageType: Image`) from **Amazon ECR**, not from a `.zip` on S3.

- **Active artifact:** ECR image URI on each function (e.g. `…/jmi-lambda:<tag>`).
- **Optional zip archive:** `s3://<data-bucket>/lambda_legacy/jmi-lambda.zip` (audit/download only). **`lambda/`** under the bucket should stay empty; live Lambdas use **ECR images**, not this zip.

## Canonical deploy (ECR → Lambda)

From the **repository root**, with Docker **daemon** running and AWS CLI configured for the target account:

```bash
chmod +x infra/aws/lambda/deploy_ecr_create_update.sh
./infra/aws/lambda/deploy_ecr_create_update.sh v20-your-tag
```

This builds `infra/aws/lambda/Dockerfile` (includes full `src/` — Arbeitnow, Adzuna, shared pipelines) and `handlers/`, pushes to ECR, then updates all three functions to the new image.

### If local Docker is not available (recommended cloud paths)

- **GitHub Actions:** workflow `JMI Lambda ECR deploy` (`.github/workflows/jmi-lambda-ecr-deploy.yml`) — run **Workflow dispatch** from the Actions tab; requires repo secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The runner provides Docker; no local daemon needed.
- **AWS CodeBuild:** use `infra/aws/lambda/codebuild/buildspec.yml` with a **privileged** project (`aws/codebuild/standard:7.0` or similar) so `docker build` runs in AWS. **AWS CloudShell does not support Docker** — do not use CloudShell for image builds.

Updating Lambdas after a push (same image URI on all three functions) can also be done **without Docker** via `infra/aws/lambda/update_lambdas_from_image_uri.sh` if you already have an image in ECR.

Environment variables for the functions are re-applied by the script (same as the historical zip flow).

## Optional: zip package (local / legacy)

`package_and_zip.sh` builds **`infra/aws/lambda/dist/jmi-lambda.zip`** using Docker (Linux deps) and copies `src/` + `handlers/`. It does **not** deploy to Lambda if your functions are image-based.

`deploy_create_update.sh` uses **`aws lambda update-function-code --zip-file`**, which **only applies to Zip-based functions**. It **exits with an error** if the target functions are `PackageType: Image` (see guard at top of that script).

## Optional: refresh the S3 zip (audit / human download only)

Does **not** change Lambda code unless you separately switch functions to S3-based zip deployment (not the current design).

```bash
./infra/aws/lambda/package_and_zip.sh
aws s3 cp infra/aws/lambda/dist/jmi-lambda.zip s3://jmi-dakshyadav-job-market-intelligence/lambda_legacy/jmi-lambda.zip --region ap-south-1
```

Or use `sync_lambda_zip_to_s3.sh` after `package_and_zip.sh`.

## Adzuna code in the image

The Dockerfile **`COPY src ./src`**. Anything under `src/jmi/` (including Adzuna connector and `ingest_adzuna` pipeline) is in the image. Scheduled **`jmi-ingest-live`** still invokes **`ingest_live`** (Arbeitnow) only; Adzuna is a separate manual/module entrypoint unless you add another function or change the handler.

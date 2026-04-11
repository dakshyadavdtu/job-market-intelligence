# Build from repository root:
#   docker build --platform linux/amd64 -f infra/aws/lambda/Dockerfile -t jmi-lambda:local .
# Per-function handler is set via Lambda ImageConfig Command (ingest / silver / gold).
FROM public.ecr.aws/lambda/python:3.11
WORKDIR ${LAMBDA_TASK_ROOT}
COPY infra/aws/lambda/requirements-lambda.txt ./requirements-lambda.txt
RUN pip install --no-cache-dir -r requirements-lambda.txt
COPY src ./src
COPY infra/aws/lambda/handlers ./handlers

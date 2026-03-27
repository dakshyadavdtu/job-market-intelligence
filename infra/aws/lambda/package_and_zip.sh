#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
DIST_DIR="$ROOT_DIR/infra/aws/lambda/dist"
BUILD_DIR="$ROOT_DIR/infra/aws/lambda/build"
ZIP_PATH="$DIST_DIR/jmi-lambda.zip"

rm -rf "$BUILD_DIR" "$DIST_DIR"
mkdir -p "$BUILD_DIR" "$DIST_DIR"

# Build Linux-compatible package for Lambda using Docker.
docker run --rm \
  --platform linux/amd64 \
  -v "$ROOT_DIR":/var/task \
  -w /var/task \
  --entrypoint /bin/sh \
  public.ecr.aws/lambda/python:3.11 \
  -c "pip install --only-binary=:all: -r infra/aws/lambda/requirements-lambda.txt -t infra/aws/lambda/build"

cp -R "$ROOT_DIR/src" "$BUILD_DIR/src"
cp -R "$ROOT_DIR/infra/aws/lambda/handlers" "$BUILD_DIR/handlers"

cd "$BUILD_DIR"
zip -r "$ZIP_PATH" .
echo "Created: $ZIP_PATH"


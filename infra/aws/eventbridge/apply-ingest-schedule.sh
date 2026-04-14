#!/usr/bin/env bash
# Apply or update EventBridge Scheduler schedule for jmi-ingest-live (24-hour cadence).
# Requires: aws CLI v2, iam:PassRole on the target role, scheduler:* on the schedule.
# Region: ap-south-1 (override with AWS_REGION).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REGION="${AWS_REGION:-ap-south-1}"
NAME="jmi-ingest-10min"
GROUP="${JMI_SCHEDULER_GROUP:-default}"
TARGET_FILE="$HERE/scheduler-target.json"

if [[ ! -f "$TARGET_FILE" ]]; then
  echo "Missing $TARGET_FILE" >&2
  exit 1
fi

if aws scheduler get-schedule --name "$NAME" --group-name "$GROUP" --region "$REGION" &>/dev/null; then
  echo "Updating existing schedule $GROUP/$NAME ..."
  aws scheduler update-schedule \
    --name "$NAME" \
    --group-name "$GROUP" \
    --region "$REGION" \
    --schedule-expression "rate(24 hours)" \
    --state ENABLED \
    --flexible-time-window Mode=OFF \
    --target "file://$TARGET_FILE"
else
  echo "Creating schedule $GROUP/$NAME ..."
  aws scheduler create-schedule \
    --name "$NAME" \
    --group-name "$GROUP" \
    --region "$REGION" \
    --schedule-expression "rate(24 hours)" \
    --state ENABLED \
    --flexible-time-window Mode=OFF \
    --target "file://$TARGET_FILE" \
    --description "JMI pipeline (ingest→silver→gold) every 24 hours via EventBridge Scheduler"
fi

echo "OK: $GROUP/$NAME is rate(24 hours) in $REGION"

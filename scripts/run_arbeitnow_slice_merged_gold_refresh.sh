#!/usr/bin/env bash
# Arbeitnow slice only: rebuild merged Silver from the full history union (slice batches + legacy
# modular source=arbeitnow + silver_legacy), then Gold with all posted_months present. Does not touch Adzuna.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT"
: "${JMI_DATA_ROOT:?Set JMI_DATA_ROOT (e.g. s3://your-bucket or local data root)}"
export JMI_ARBEITNOW_SLICE="${JMI_ARBEITNOW_SLICE:-arbeitnow_2026_q1_focus}"
python scripts/rebuild_merged_silver_from_union.py --source arbeitnow
python -m src.jmi.pipelines.transform_gold --full-posted-months

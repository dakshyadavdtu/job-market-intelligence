#!/usr/bin/env bash
# Rebuild main jmi_gold_v2 Arbeitnow facts: merged Silver = modular source=arbeitnow (no slice) + silver_legacy,
# then Gold under gold/<table>/source=arbeitnow/... Requires JMI_DATA_ROOT. Does not touch Adzuna.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT"
: "${JMI_DATA_ROOT:?Set JMI_DATA_ROOT (e.g. s3://your-bucket or local data root)}"
unset JMI_ARBEITNOW_SLICE
python scripts/rebuild_merged_silver_from_union.py --source arbeitnow
python -m src.jmi.pipelines.transform_gold --full-posted-months

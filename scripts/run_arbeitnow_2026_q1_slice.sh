#!/usr/bin/env bash
# Arbeitnow only: fetch with API min_created_at from 2025-01-01 UTC (still-listed 2025 + 2026),
# isolate Bronze/Silver/Gold under slice=arbeitnow_2026_q1_focus. Does not touch Adzuna.
# Requires: network; optional .env for proxy. From repo root with venv active.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export JMI_ARBEITNOW_SLICE="${JMI_ARBEITNOW_SLICE:-arbeitnow_2026_q1_focus}"
export JMI_ARBEITNOW_USE_MIN_CREATED_AT_PARAM=true
export JMI_ARBEITNOW_MIN_CREATED_AT="${JMI_ARBEITNOW_MIN_CREATED_AT:-1735689600}"

python -m src.jmi.pipelines.ingest_live
python -m src.jmi.pipelines.transform_silver
python -m src.jmi.pipelines.transform_gold --full-posted-months

echo "OK: Arbeitnow slice pipeline finished. Gold under gold/slice=${JMI_ARBEITNOW_SLICE}/"

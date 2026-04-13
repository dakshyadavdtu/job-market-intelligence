#!/usr/bin/env bash
# Local full pipeline: Bronze → Silver → Gold (writes only under data/).
# For ingest + sync + Athena v2 live update in one step, use:
#   ./scripts/run_pipeline_live_sync.sh arbeitnow|adzuna_in
# Usage:
#   ./scripts/run_pipeline_local.sh              # default: arbeitnow
#   ./scripts/run_pipeline_local.sh adzuna_in    # needs ADZUNA_APP_ID + ADZUNA_APP_KEY
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# Always write under repo data/ so paths match `aws s3 sync data/...` (ignore inherited JMI_DATA_ROOT=s3://...).
export JMI_DATA_ROOT="${ROOT}/data"
if [[ ! -d .venv ]]; then
  echo "Create venv first: python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi
# shellcheck source=/dev/null
source .venv/bin/activate

SOURCE="${1:-arbeitnow}"
if [[ "$SOURCE" == "adzuna_in" || "$SOURCE" == "adzuna" ]]; then
  python -m src.jmi.pipelines.ingest_adzuna
  python -m src.jmi.pipelines.transform_silver --source adzuna_in
  python -m src.jmi.pipelines.transform_gold --source adzuna_in
else
  python -m src.jmi.pipelines.ingest_live
  python -m src.jmi.pipelines.transform_silver
  python -m src.jmi.pipelines.transform_gold
fi
echo "Done. Data under ${JMI_DATA_ROOT:-data}/"

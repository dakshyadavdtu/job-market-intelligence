#!/usr/bin/env bash
# Local Adzuna India: Bronze → Silver → Gold (no AWS).
# Requires: ADZUNA_APP_ID, ADZUNA_APP_KEY, venv with requirements.txt
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  echo "Use .venv: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi
: "${ADZUNA_APP_ID:?export ADZUNA_APP_ID}"
: "${ADZUNA_APP_KEY:?export ADZUNA_APP_KEY}"
export JMI_ADZUNA_MAX_PAGES="${JMI_ADZUNA_MAX_PAGES:-2}"
"$PY" -m src.jmi.pipelines.ingest_adzuna
"$PY" -m src.jmi.pipelines.transform_silver --source adzuna_in
"$PY" -m src.jmi.pipelines.transform_gold --source adzuna_in
echo "Adzuna E2E OK (see data/quality/*.json and docs/adzuna_india_runbook.md)"

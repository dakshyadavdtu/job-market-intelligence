#!/usr/bin/env bash
# One command: local ingest → silver → gold → S3 sync → Athena v2 run_id projection → verify.
# Usage:
#   ./scripts/run_pipeline_live_sync.sh arbeitnow
#   ./scripts/run_pipeline_live_sync.sh adzuna_in
# Optional: --skip-sync-s3  --skip-athena  --skip-verify
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ ! -d .venv ]]; then
  echo "Create venv first: python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi
# shellcheck source=/dev/null
source .venv/bin/activate
export PYTHONPATH="$ROOT"
exec python3 "$ROOT/scripts/pipeline_live_sync.py" "$@"

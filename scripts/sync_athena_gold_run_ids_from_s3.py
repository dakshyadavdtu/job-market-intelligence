#!/usr/bin/env python3
"""Merge S3 gold run_ids into jmi_gold_v2 partition projection (Glue TBLPROPERTIES)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.jmi.aws.athena_projection import sync_gold_run_id_projection_from_s3  # noqa: E402


def main() -> int:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
    csv = sync_gold_run_id_projection_from_s3(region=region, workgroup="primary")
    print(f"Updated projection with {len(csv.split(','))} run_id(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Refresh jmi_gold_arbeitnow_slice partition projection.run_id.values from S3 slice prefixes."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.jmi.aws.athena_projection import sync_arbeitnow_slice_gold_projection_from_s3  # noqa: E402


def main() -> int:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
    csv = sync_arbeitnow_slice_gold_projection_from_s3(region=region, workgroup="primary")
    print(f"Updated {len(csv.split(','))} run_id(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

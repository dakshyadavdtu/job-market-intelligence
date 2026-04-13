#!/usr/bin/env python3
"""Verify active S3 layout: no ingest_month= or root latest_run_metadata under gold/."""
from __future__ import annotations

import os
import sys

BUCKET = os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip() or "jmi-dakshyadav-job-market-intelligence"
REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"


def main() -> int:
    import boto3  # type: ignore

    client = boto3.client("s3", region_name=REGION)
    bad_ingest: list[str] = []
    bad_meta: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="gold/"):
        for obj in page.get("Contents") or []:
            k = obj["Key"]
            if "ingest_month=" in k:
                bad_ingest.append(k)
            if k.startswith("gold/latest_run_metadata/"):
                bad_meta.append(k)

    print(f"bucket={BUCKET} region={REGION}")
    print(f"gold/ keys with ingest_month=: {len(bad_ingest)}")
    print(f"gold/latest_run_metadata/ (root, invalid): {len(bad_meta)}")

    ok = not bad_ingest and not bad_meta
    if bad_ingest[:10]:
        print("ingest_month samples:", *bad_ingest[:5], sep="\n  ")
    if not ok:
        return 1
    print("OK active layout checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

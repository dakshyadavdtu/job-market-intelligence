#!/usr/bin/env python3
"""
Delete legacy Silver keys: silver/jobs/ingest_date=... (flat layout, no source= segment).

Does NOT delete:
  silver/jobs/source=*/ingest_date=.../...

Requires: boto3, s3:ListBucket, s3:DeleteObject on the bucket.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Iterator

try:
    import boto3  # type: ignore
except ImportError:
    boto3 = None  # type: ignore


def is_legacy_flat_silver_key(key: str) -> bool:
    """True for keys like silver/jobs/ingest_date=YYYY-MM-DD/... but not silver/jobs/source=..."""
    if not key.startswith("silver/jobs/"):
        return False
    rest = key[len("silver/jobs/") :]
    return rest.startswith("ingest_date=")


def iter_legacy_flat_keys(bucket: str, *, prefix: str = "silver/jobs/") -> Iterator[str]:
    if boto3 is None:
        raise RuntimeError("boto3 is required")
    cli = boto3.client("s3")
    paginator = cli.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            k = obj.get("Key") or ""
            if k and is_legacy_flat_silver_key(k):
                yield k


def main() -> int:
    p = argparse.ArgumentParser(description="Purge legacy flat silver/jobs/ingest_date=* keys on S3.")
    p.add_argument("--bucket", default=os.environ.get("JMI_BUCKET", "").strip(), help="S3 bucket (default: JMI_BUCKET)")
    p.add_argument("--dry-run", action="store_true", help="List only; do not delete")
    args = p.parse_args()
    bucket = args.bucket or "jmi-dakshyadav-job-market-intelligence"
    if boto3 is None:
        print("boto3 required", file=sys.stderr)
        return 1

    keys = sorted(iter_legacy_flat_keys(bucket))
    out = {"bucket": bucket, "legacy_flat_key_count": len(keys), "sample_keys": keys[:40]}
    if args.dry_run:
        print(json.dumps(out, indent=2))
        return 0
    if not keys:
        print(json.dumps({**out, "deleted": 0}, indent=2))
        return 0

    cli = boto3.client("s3")
    deleted = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        cli.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )
        deleted += len(chunk)
    print(json.dumps({**out, "deleted": deleted}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

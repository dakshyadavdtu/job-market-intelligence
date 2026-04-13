#!/usr/bin/env python3
"""
Delete legacy junk keys under the active `gold/` prefix on S3.

Removes (only):
  - `gold/latest_run_metadata/...` (top-level; per-source lives under `gold/source=<slug>/latest_run_metadata/`)
  - any key whose path contains `/ingest_month=` (legacy fact partitions)

Does NOT delete:
  - `gold/source=<slug>/posted_month=...` or `gold/<table>/source=<slug>/posted_month=...`
  - `gold/source=<slug>/latest_run_metadata/...`

Requires: boto3, AWS credentials with s3:DeleteObject + s3:ListBucket on the bucket.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Iterator

try:
    import boto3  # type: ignore
except ImportError:
    boto3 = None  # type: ignore


def is_legacy_gold_s3_key(key: str) -> bool:
    if not key.startswith("gold/"):
        return False
    if key.startswith("gold/latest_run_metadata/"):
        return True
    if "/ingest_month=" in key:
        return True
    return False


def iter_legacy_gold_keys(bucket: str, *, prefix: str = "gold/") -> Iterator[str]:
    if boto3 is None:
        raise RuntimeError("boto3 is required for S3 purge")
    cli = boto3.client("s3")
    paginator = cli.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            k = obj.get("Key") or ""
            if k and is_legacy_gold_s3_key(k):
                yield k


def delete_keys_batch(cli, bucket: str, keys: list[str]) -> None:
    if not keys:
        return
    # delete_objects accepts up to 1000 keys
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        cli.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )


def purge_legacy_gold_active(
    bucket: str,
    *,
    dry_run: bool = False,
    prefix: str = "gold/",
) -> dict:
    """List and optionally delete legacy keys. Returns counts and sample keys."""
    if boto3 is None:
        raise RuntimeError("boto3 is required for S3 purge")
    cli = boto3.client("s3")
    keys = sorted(iter_legacy_gold_keys(bucket, prefix=prefix))
    out: dict = {
        "bucket": bucket,
        "prefix": prefix,
        "dry_run": dry_run,
        "legacy_key_count": len(keys),
        "sample_keys": keys[:30],
    }
    if dry_run or not keys:
        out["deleted"] = 0
        return out
    delete_keys_batch(cli, bucket, keys)
    out["deleted"] = len(keys)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Purge legacy gold/ keys on S3 (ingest_month=, top-level latest_run_metadata).")
    p.add_argument("--bucket", default=os.environ.get("JMI_BUCKET", "").strip(), help="S3 bucket (default: JMI_BUCKET)")
    p.add_argument("--prefix", default="gold/", help="List prefix (default gold/)")
    p.add_argument("--dry-run", action="store_true", help="List only; do not delete")
    args = p.parse_args()
    bucket = args.bucket or "jmi-dakshyadav-job-market-intelligence"
    result = purge_legacy_gold_active(bucket, dry_run=args.dry_run, prefix=args.prefix)
    import json

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

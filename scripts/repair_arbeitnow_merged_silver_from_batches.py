#!/usr/bin/env python3
"""Rewrite Arbeitnow merged Silver (merged/latest.parquet) from union of all per-run batch Parquets.

Uses the same logic as transform_silver.load_silver_jobs_history_union (excludes merged/).
Honest: dedupes by job_id keeping the latest bronze/ingest ordering (same as pipeline).

Requires JMI_DATA_ROOT pointing at the data root (local or s3://bucket/...).

Exit 0 if merged written; prints JSON with posted_month distribution (from assign_posted_month_and_time_axis) for audit.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import replace

from src.jmi.config import AppConfig
from src.jmi.paths import silver_jobs_merged_latest
from src.jmi.pipelines.gold_time import assign_posted_month_and_time_axis
from src.jmi.pipelines.transform_silver import load_silver_jobs_history_union
from src.jmi.utils.io import write_parquet


def main() -> int:
    if not os.environ.get("JMI_DATA_ROOT"):
        print("Set JMI_DATA_ROOT to your data root (e.g. s3://bucket/ or local data/).", file=sys.stderr)
        return 2
    cfg = replace(AppConfig(), source_name="arbeitnow")
    df = load_silver_jobs_history_union(cfg)
    if df is None or df.empty:
        print(json.dumps({"ok": False, "error": "no_batch_parquet_union", "rows": 0}))
        return 1
    audit = assign_posted_month_and_time_axis(df.copy())
    pm_counts = (
        audit["posted_month"]
        .astype(str)
        .replace("", "(empty)")
        .value_counts()
        .head(50)
        .to_dict()
    )
    out_path = silver_jobs_merged_latest(cfg)
    write_parquet(out_path, df)
    print(
        json.dumps(
            {
                "ok": True,
                "merged_path": str(out_path),
                "rows_written": int(len(df)),
                "posted_month_top": pm_counts,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

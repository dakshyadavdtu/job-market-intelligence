#!/usr/bin/env python3
"""
Same-run Silver proof for Arbeitnow: distinct valid posted_month for rows matching
latest Gold pipeline_run_id (bronze_run_id in Silver).
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.jmi.config import AppConfig  # noqa: E402
from src.jmi.paths import silver_jobs_merged_latest  # noqa: E402
from src.jmi.pipelines.gold_time import assign_posted_month_and_time_axis  # noqa: E402


def _latest_gold_run_id(cfg: AppConfig) -> str | None:
    p = cfg.gold_root.as_path() / "source=arbeitnow" / "latest_run_metadata" / "part-00001.parquet"
    if not p.is_file():
        return None
    df = pd.read_parquet(p)
    if df.empty or "run_id" not in df.columns:
        return None
    return str(df["run_id"].iloc[0]).strip()


def main() -> int:
    os.environ.setdefault("JMI_DATA_ROOT", str(REPO_ROOT / "data"))
    cfg = replace(AppConfig(), source_name="arbeitnow")
    rid = _latest_gold_run_id(cfg)
    merged = silver_jobs_merged_latest(cfg)
    path = merged.as_path()
    if not path.is_file():
        print(json.dumps({"error": "merged silver missing", "path": str(path)}))
        return 1
    df = pd.read_parquet(path)
    if rid:
        if "bronze_run_id" in df.columns:
            sub = df[df["bronze_run_id"].astype(str) == rid]
        else:
            sub = df
    else:
        sub = df
    d = assign_posted_month_and_time_axis(sub.copy())
    valid = d[d["posted_month"].astype(str).str.match(r"^\d{4}-\d{2}$", na=False)]
    months = sorted(valid["posted_month"].astype(str).unique().tolist())
    out = {
        "gold_latest_run_id": rid,
        "merged_silver_path": str(path.resolve()),
        "rows_same_run": int(len(sub)),
        "distinct_valid_posted_month": months,
        "n_distinct_valid_posted_month": len(months),
        "gold_single_month_is_silver_truth": len(months) <= 1,
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

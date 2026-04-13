#!/usr/bin/env python3
"""Rewrite silver/.../merged/latest.parquet from the full per-batch union (honest retained history)."""
from __future__ import annotations

import argparse
import json
import sys

from dataclasses import replace as dc_replace

from src.jmi.config import AppConfig

from src.jmi.pipelines.transform_silver import load_silver_jobs_history_union, project_silver_to_contract
from src.jmi.paths import silver_jobs_merged_latest
from src.jmi.utils.io import write_parquet


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, choices=("arbeitnow", "adzuna_in"))
    args = p.parse_args()
    cfg = dc_replace(AppConfig(), source_name=args.source)
    df = load_silver_jobs_history_union(cfg)
    if df is None or df.empty:
        print(f"No silver batch files found for source={args.source}", file=sys.stderr)
        return 1
    df = project_silver_to_contract(df)
    out = silver_jobs_merged_latest(cfg)
    write_parquet(out, df)
    print(
        json.dumps(
            {
                "source": args.source,
                "rows": int(len(df)),
                "merged_silver_file": str(out),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

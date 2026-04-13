"""Exploratory calendar-year rollup from latest Gold role_demand_monthly (per source; not strict-intersection filtered)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd

from src.jmi.config import AppConfig
from src.jmi.paths import derived_yearly_exploratory_source_year_totals_parquet, derived_yearly_manifest_parquet
from src.jmi.pipelines.transform_derived_strict_common import (
    ADZUNA_IN,
    ARBEITNOW,
    _latest_run_id,
    _load_fact_with_posted_month,
    _months_with_role_partition_for_run,
)
from src.jmi.utils.io import ensure_dir, write_parquet

LAYER = "exploratory_latest_run_calendar_year"
ALIGNMENT = "per_source_latest_gold_run_not_strict_month_intersection"


def run_derived_yearly_exploratory(cfg: AppConfig | None = None) -> dict:
    cfg = cfg or AppConfig()
    rows: list[dict] = []
    all_years: set[int] = set()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for src in (ARBEITNOW, ADZUNA_IN):
        rid = _latest_run_id(cfg, src)
        if not rid:
            continue
        months = _months_with_role_partition_for_run(cfg, src, rid)
        if not months:
            continue
        df = _load_fact_with_posted_month(cfg, "role_demand_monthly", src, rid, months)
        if df.empty or "posted_month" not in df.columns:
            continue
        df = df.copy()
        df["calendar_year"] = df["posted_month"].astype(str).str[:4].astype(int)
        for yr, g in df.groupby("calendar_year", sort=True):
            y = int(yr)
            all_years.add(y)
            pm_in_y = g["posted_month"].astype(str).nunique()
            tot = int(g["job_count"].sum()) if "job_count" in g.columns else 0
            rows.append(
                {
                    "source": src,
                    "calendar_year": y,
                    "total_postings": tot,
                    "months_present_in_year": int(pm_in_y),
                    "run_id": rid,
                    "layer_scope": LAYER,
                    "data_alignment": ALIGNMENT,
                    "exploratory_only": True,
                    "materialized_at_utc": ts,
                }
            )

    out = pd.DataFrame(rows)
    if not rows:
        out = pd.DataFrame(
            columns=[
                "source",
                "calendar_year",
                "total_postings",
                "months_present_in_year",
                "run_id",
                "layer_scope",
                "data_alignment",
                "exploratory_only",
                "materialized_at_utc",
            ]
        )

    distinct_union = sorted(all_years)
    multi_year = len(distinct_union) >= 2
    manifest = pd.DataFrame(
        [
            {
                "layer_scope": LAYER,
                "exploratory_only": True,
                "distinct_calendar_years_union_csv": ",".join(str(y) for y in distinct_union),
                "distinct_year_count_union": len(distinct_union),
                "multi_calendar_year_data_present": multi_year,
                "headline_multi_year_narrative_worthy": multi_year,
                "note": (
                    "Rollup uses each source's latest Gold run only; months are not filtered to "
                    "strict cross-source intersection. Do not treat as apples-to-apples annual benchmark "
                    "across sources unless policy views agree."
                ),
                "materialized_at_utc": ts,
            }
        ]
    )

    dest = derived_yearly_exploratory_source_year_totals_parquet(cfg)
    ensure_dir(dest.parent)
    write_parquet(dest, out.sort_values(["source", "calendar_year"]) if not out.empty else out)

    mpath = derived_yearly_manifest_parquet(cfg)
    ensure_dir(mpath.parent)
    write_parquet(mpath, manifest)

    meta = cfg.quality_root / "derived_yearly_exploratory.json"
    ensure_dir(meta.parent)
    meta.write_text(
        json.dumps(
            {
                "stage": "derived_yearly_exploratory",
                "distinct_years_union": distinct_union,
                "row_count": int(len(out)),
                "paths": {"totals": str(dest), "manifest": str(mpath)},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "status": "OK",
        "distinct_calendar_years_union": distinct_union,
        "row_count": int(len(out)),
        "multi_year_data": multi_year,
        "paths": {"totals": str(dest), "manifest": str(mpath)},
    }


if __name__ == "__main__":
    print(json.dumps(run_derived_yearly_exploratory(), indent=2))

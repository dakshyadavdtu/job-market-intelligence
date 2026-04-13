"""
Build normalized Gold v2 presentation layer under gold_v2/presentation/.

Reads source-truth gold facts (latest run_id per source from gold/source=<slug>/latest_run_metadata/).
Writes the same visible tree for arbeitnow and adzuna_in:
  monthly/ — one Parquet per posted_month present in source gold (no padding)
  yearly/  — rolled up by calendar year from those months only

Does not invent months or years. Comparison-only gold/comparison_* remains separate (legacy).
"""
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import replace
from datetime import datetime, timezone
import pandas as pd

from src.jmi.config import AppConfig, new_run_id, split_s3_uri
from src.jmi.paths import gold_fact_partition, gold_latest_run_metadata_file, gold_v2_presentation_monthly_parquet, gold_v2_presentation_yearly_parquet
from src.jmi.utils.io import write_parquet

POSTED_MONTH_RE = re.compile(r"posted_month=(\d{4}-\d{2})/")

# (gold fact table name, presentation folder name)
FACT_SPECS: tuple[tuple[str, str], ...] = (
    ("skill_demand_monthly", "v2_skill_demand"),
    ("role_demand_monthly", "v2_role_demand"),
    ("location_demand_monthly", "v2_location_demand"),
    ("company_hiring_monthly", "v2_company_hiring"),
    ("pipeline_run_summary", "v2_pipeline_run_summary"),
)


def _read_latest_run_id(cfg: AppConfig, source_slug: str) -> str:
    cfg_m = replace(cfg, source_name=source_slug)
    meta = gold_latest_run_metadata_file(cfg_m)
    df = pd.read_parquet(str(meta))
    if df.empty or "run_id" not in df.columns:
        raise RuntimeError(f"Missing run_id in {meta}")
    return str(df["run_id"].iloc[0])


def _list_posted_months_for_run(cfg: AppConfig, table: str, source_slug: str, run_id: str) -> list[str]:
    """Months that exist in source gold for this fact table, source, and pipeline run_id."""
    gold_root = cfg.gold_root
    months: set[str] = set()
    if gold_root.is_s3:
        bucket, base_key = split_s3_uri(str(gold_root).rstrip("/"))
        pfx = f"{base_key}/{table}/source={source_slug}/"
        client = __import__("boto3").client("s3")
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=pfx):
            for o in page.get("Contents") or []:
                k = o["Key"]
                if f"run_id={run_id}/" not in k:
                    continue
                m = POSTED_MONTH_RE.search(k)
                if m:
                    months.add(m.group(1))
    else:
        base = gold_root.as_path() / table / f"source={source_slug}"
        if not base.is_dir():
            return []
        for d in sorted(base.glob("posted_month=*")):
            if not d.is_dir():
                continue
            pm = d.name.split("=", 1)[1]
            part = d / f"run_id={run_id}" / "part-00001.parquet"
            if part.is_file():
                months.add(pm)
    return sorted(months)


def _read_fact(cfg: AppConfig, table: str, source_slug: str, posted_month: str, run_id: str) -> pd.DataFrame:
    cfg_m = replace(cfg, source_name=source_slug)
    path = gold_fact_partition(cfg_m, table, posted_month=posted_month, pipeline_run_id=run_id)
    return pd.read_parquet(str(path))


def _add_lineage(
    df: pd.DataFrame,
    *,
    presentation_build_id: str,
    source_gold_run_id: str,
) -> pd.DataFrame:
    out = df.copy()
    out["presentation_build_id"] = presentation_build_id
    out["source_gold_run_id"] = source_gold_run_id
    return out


def _process_dimension_fact(
    cfg: AppConfig,
    *,
    gold_table: str,
    pres_name: str,
    source_slug: str,
    run_id: str,
    presentation_build_id: str,
    key_col: str,
) -> dict:
    months = _list_posted_months_for_run(cfg, gold_table, source_slug, run_id)
    monthly_frames: list[pd.DataFrame] = []
    for pm in months:
        df = _read_fact(cfg, gold_table, source_slug, pm, run_id)
        df["posted_month_ref"] = pm
        monthly_frames.append(df)
    if not monthly_frames:
        return {"gold_table": gold_table, "months": [], "yearly_years": []}

    all_m = pd.concat(monthly_frames, ignore_index=True)
    all_m = all_m.drop(columns=["source"], errors="ignore")
    all_m = _add_lineage(all_m, presentation_build_id=presentation_build_id, source_gold_run_id=run_id)
    # monthly writes: one file per posted_month
    out_m: list[str] = []
    for pm in months:
        sub = all_m[all_m["posted_month_ref"] == pm].drop(columns=["posted_month_ref"], errors="ignore")
        dest = gold_v2_presentation_monthly_parquet(
            cfg, pres_name, source_slug=source_slug, posted_month=pm
        )
        write_parquet(dest, sub)
        out_m.append(str(dest))

    all_m["calendar_year"] = all_m["posted_month_ref"].astype(str).str[:4]
    years = sorted(all_m["calendar_year"].unique())
    out_y: list[str] = []
    for yr in years:
        sub = all_m[all_m["calendar_year"] == yr].drop(
            columns=["posted_month_ref", "presentation_build_id", "source_gold_run_id"],
            errors="ignore",
        )
        g = (
            sub.groupby(key_col, as_index=False)
            .agg(
                {
                    "job_count": "sum",
                    "bronze_ingest_date": "max",
                    "time_axis": lambda s: s.mode().iloc[0] if len(s.mode()) else s.iloc[0],
                }
            )
        )
        g = _add_lineage(g, presentation_build_id=presentation_build_id, source_gold_run_id=run_id)
        g["calendar_year"] = yr
        dest = gold_v2_presentation_yearly_parquet(cfg, pres_name, source_slug=source_slug, calendar_year=yr)
        write_parquet(dest, g)
        out_y.append(str(dest))

    return {"gold_table": gold_table, "months": months, "yearly_years": years, "monthly_paths": out_m, "yearly_paths": out_y}


def _process_pipeline_summary(
    cfg: AppConfig,
    *,
    source_slug: str,
    run_id: str,
    presentation_build_id: str,
) -> dict:
    gold_table = "pipeline_run_summary"
    pres_name = "v2_pipeline_run_summary"
    months = _list_posted_months_for_run(cfg, gold_table, source_slug, run_id)
    if not months:
        return {"gold_table": gold_table, "months": [], "yearly_years": []}

    monthly_frames: list[pd.DataFrame] = []
    for pm in months:
        df = _read_fact(cfg, gold_table, source_slug, pm, run_id)
        df["posted_month_ref"] = pm
        monthly_frames.append(df)
    all_m = pd.concat(monthly_frames, ignore_index=True)
    all_m = _add_lineage(all_m, presentation_build_id=presentation_build_id, source_gold_run_id=run_id)
    # Avoid duplicate `source` with partition column in Athena.
    all_m = all_m.drop(columns=["source"], errors="ignore")

    out_m: list[str] = []
    for pm in months:
        sub = all_m[all_m["posted_month_ref"] == pm].drop(columns=["posted_month_ref"], errors="ignore")
        dest = gold_v2_presentation_monthly_parquet(
            cfg, pres_name, source_slug=source_slug, posted_month=pm
        )
        write_parquet(dest, sub)
        out_m.append(str(dest))

    all_m["calendar_year"] = all_m["posted_month_ref"].astype(str).str[:4]
    years = sorted(all_m["calendar_year"].unique())
    out_y: list[str] = []
    for yr in years:
        sub = all_m[all_m["calendar_year"] == yr].drop(
            columns=["posted_month_ref", "presentation_build_id", "source_gold_run_id"],
            errors="ignore",
        )
        g = sub.groupby("calendar_year", as_index=False).agg(
            {
                "skill_row_count": "sum",
                "role_row_count": "sum",
                "location_row_count": "sum",
                "company_row_count": "sum",
                "bronze_ingest_date": "max",
                "time_axis": lambda s: s.mode().iloc[0] if len(s.mode()) else s.iloc[0],
                "status": lambda s: s.mode().iloc[0] if len(s.mode()) else s.iloc[0],
            }
        )
        g = _add_lineage(g, presentation_build_id=presentation_build_id, source_gold_run_id=run_id)
        dest = gold_v2_presentation_yearly_parquet(cfg, pres_name, source_slug=source_slug, calendar_year=yr)
        write_parquet(dest, g)
        out_y.append(str(dest))

    return {"gold_table": gold_table, "months": months, "yearly_years": years, "monthly_paths": out_m, "yearly_paths": out_y}


def _required_months_from_env() -> list[str]:
    raw = (os.environ.get("JMI_PRESENTATION_REQUIRED_MONTHS") or "").strip()
    if not raw:
        return []
    return [m.strip() for m in raw.split(",") if m.strip()]


def run(
    *,
    cfg: AppConfig | None = None,
    sources: tuple[str, ...] | None = None,
    presentation_build_id: str | None = None,
) -> dict:
    cfg = cfg or AppConfig()
    sources = sources or ("arbeitnow", "adzuna_in")
    pbid = presentation_build_id or os.environ.get("JMI_PRESENTATION_BUILD_ID") or new_run_id()
    built_at = datetime.now(timezone.utc).isoformat()
    required_months = _required_months_from_env()

    per_source: list[dict] = []
    month_gaps: list[str] = []
    for src in sources:
        run_id = _read_latest_run_id(cfg, src)
        key_cols = {
            "skill_demand_monthly": "skill",
            "role_demand_monthly": "role",
            "location_demand_monthly": "location",
            "company_hiring_monthly": "company_name",
        }
        facts_out: list[dict] = []
        for gold_table, pres_name in FACT_SPECS:
            if gold_table == "pipeline_run_summary":
                facts_out.append(
                    _process_pipeline_summary(
                        cfg,
                        source_slug=src,
                        run_id=run_id,
                        presentation_build_id=pbid,
                    )
                )
            else:
                kc = key_cols[gold_table]
                facts_out.append(
                    _process_dimension_fact(
                        cfg,
                        gold_table=gold_table,
                        pres_name=pres_name,
                        source_slug=src,
                        run_id=run_id,
                        presentation_build_id=pbid,
                        key_col=kc,
                    )
                )
        if required_months:
            months_skill = set(
                _list_posted_months_for_run(cfg, "skill_demand_monthly", src, run_id)
            )
            for m in required_months:
                if m not in months_skill:
                    gap = f"{src}: gold source-truth missing posted_month={m} (skill_demand_monthly)"
                    month_gaps.append(gap)

        per_source.append(
            {
                "source": src,
                "source_gold_run_id": run_id,
                "facts": facts_out,
            }
        )

    manifest_body = {
        "presentation_build_id": pbid,
        "built_at": built_at,
        "required_months_checked": required_months,
        "month_coverage_gaps": month_gaps,
        "sources": per_source,
    }

    manifest_path = cfg.gold_v2_root / "presentation" / "_manifest" / "presentation_build.json"
    if manifest_path.is_s3:
        manifest_path.write_text(json.dumps(manifest_body, indent=2))
    else:
        mp = manifest_path.as_path()
        mp.parent.mkdir(parents=True, exist_ok=True)
        mp.write_text(json.dumps(manifest_body, indent=2), encoding="utf-8")

    return {
        "stage": "gold_v2_presentation",
        "presentation_build_id": pbid,
        "built_at": built_at,
        "sources": per_source,
        "manifest": str(manifest_path),
        "required_months_checked": required_months,
        "month_coverage_gaps": month_gaps,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Build gold_v2/presentation from source-truth gold facts.")
    p.add_argument(
        "--sources",
        default="arbeitnow,adzuna_in",
        help="Comma-separated source slugs (default: arbeitnow,adzuna_in)",
    )
    args = p.parse_args()
    sources = tuple(s.strip() for s in args.sources.split(",") if s.strip())
    out = run(sources=sources)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace
from pathlib import Path

import pandas as pd

from src.jmi.config import AppConfig, DataPath
from src.jmi.paths import gold_fact_partition, gold_latest_run_metadata_file
from src.jmi.pipelines.gold_time import assign_posted_month_and_time_axis, dominant_time_axis
from src.jmi.pipelines.silver_schema import normalize_location_raw, skills_json_to_list
from src.jmi.utils.io import write_parquet


def _merged_silver_path(cfg: AppConfig) -> DataPath:
    from src.jmi.paths import silver_jobs_merged_latest

    return silver_jobs_merged_latest(cfg)


def _latest_silver_file(cfg: AppConfig) -> Path:
    if cfg.silver_root.is_s3:
        raise FileNotFoundError("Pass silver_file or merged_silver_file when JMI_DATA_ROOT is S3.")
    base = cfg.silver_root.as_path() / "jobs"
    sub = base / f"source={cfg.source_name}"
    files = sorted(sub.glob("ingest_date=*/run_id=*/part-*.parquet"), key=lambda p: p.stat().st_mtime)
    if not files and cfg.source_name == "arbeitnow":
        files = sorted(base.glob("ingest_date=*/run_id=*/part-*.parquet"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError("No silver files found. Run silver transform first.")
    return files[-1]


def _silver_month_span_metrics(df: pd.DataFrame | None) -> tuple[int, int]:
    if df is None or df.empty:
        return (0, 0)
    d = assign_posted_month_and_time_axis(df.copy())
    d = d[d["posted_month"].astype(str).str.match(r"^\d{4}-\d{2}$", na=False)]
    if d.empty:
        return (0, 0)
    return (int(d["posted_month"].nunique()), int(len(d)))


def _resolve_silver_dataframe(
    cfg: AppConfig,
    silver_file: str | None,
    merged_silver_file: str | None,
) -> tuple[pd.DataFrame, str]:
    """Prefer the broadest Silver snapshot by valid posted_month span (fixes truncated merged/latest)."""
    from src.jmi.pipelines.transform_silver import load_silver_jobs_history_union

    seen: set[str] = set()
    for c in (merged_silver_file, os.environ.get("JMI_MERGED_SILVER_FILE")):
        if not c or c in seen:
            continue
        seen.add(c)
        try:
            frame = pd.read_parquet(c)
            if frame is not None and not frame.empty:
                return frame, c
        except Exception:
            continue

    merged_path = str(_merged_silver_path(cfg))
    df_merged: pd.DataFrame | None = None
    if merged_path not in seen:
        seen.add(merged_path)
        try:
            df_merged = pd.read_parquet(merged_path)
            if df_merged is None or df_merged.empty:
                df_merged = None
        except Exception:
            df_merged = None

    df_union = load_silver_jobs_history_union(cfg)
    mm, rm = _silver_month_span_metrics(df_merged)
    mu, ru = _silver_month_span_metrics(df_union)

    if df_union is not None and not df_union.empty:
        if df_merged is None or mu > mm or (mu == mm and ru > rm):
            return df_union, f"<silver_jobs_history_union source={cfg.source_name}>"
    if df_merged is not None and not df_merged.empty:
        return df_merged, merged_path

    if silver_file and silver_file not in seen:
        seen.add(silver_file)
        try:
            frame = pd.read_parquet(silver_file)
            if frame is not None and not frame.empty:
                return frame, silver_file
        except Exception:
            pass

    if not cfg.silver_root.is_s3:
        latest = str(_latest_silver_file(cfg))
        if latest not in seen:
            frame = pd.read_parquet(latest)
            if frame is not None and not frame.empty:
                return frame, latest

    raise FileNotFoundError("No readable non-empty silver parquet found.")


def _build_monthly_skill(sub: pd.DataFrame, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    skills_col = sub["skills"].map(skills_json_to_list)
    skill_df = pd.DataFrame({"job_id": sub["job_id"], "skills": skills_col}).explode("skills").dropna(subset=["skills"])
    skill_df = skill_df[skill_df["skills"].astype(str).str.strip() != ""]
    skill_agg = (
        skill_df.groupby("skills", as_index=False)["job_id"]
        .nunique()
        .rename(columns={"skills": "skill", "job_id": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    # `source` is added in `run()` so live Athena tables (legacy path) can filter `WHERE source = ...`.
    skill_agg["bronze_ingest_date"] = rep_date
    skill_agg["bronze_run_id"] = bronze_run_id
    skill_agg["time_axis"] = dominant_time_axis(sub["time_axis"]) if "time_axis" in sub.columns else "posted"
    return skill_agg


def _role_series(sub: pd.DataFrame) -> pd.Series:
    if "title_norm" in sub.columns:
        return sub["title_norm"]
    if "title_clean" in sub.columns:
        return sub["title_clean"]
    if "title" in sub.columns:
        return sub["title"]
    return pd.Series([""] * len(sub), index=sub.index)


def _location_series(sub: pd.DataFrame) -> pd.Series:
    if "location_raw" in sub.columns:
        return sub["location_raw"]
    if "location" in sub.columns:
        return sub["location"]
    return pd.Series([""] * len(sub), index=sub.index)


def _company_series(sub: pd.DataFrame) -> pd.Series:
    if "company_norm" in sub.columns:
        return sub["company_norm"]
    if "company_name" in sub.columns:
        return sub["company_name"]
    return pd.Series([""] * len(sub), index=sub.index)


def _build_monthly_role(sub: pd.DataFrame, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    role_source_series = _role_series(sub)
    role_df = pd.DataFrame({"role": role_source_series.fillna("").astype(str)})
    role_df["role"] = role_df["role"].str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    role_df = role_df[role_df["role"] != ""]
    role_agg = (
        role_df.groupby("role", as_index=False)
        .size()
        .rename(columns={"size": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    role_agg["bronze_ingest_date"] = rep_date
    role_agg["bronze_run_id"] = bronze_run_id
    role_agg["time_axis"] = dominant_time_axis(sub["time_axis"]) if "time_axis" in sub.columns else "posted"
    return role_agg


def _build_monthly_location(sub: pd.DataFrame, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    location_source_series = _location_series(sub)
    location_df = pd.DataFrame({"location": location_source_series.fillna("").astype(str)})
    location_df["location"] = location_df["location"].map(normalize_location_raw)
    location_df = location_df[location_df["location"] != ""]
    location_agg = (
        location_df.groupby("location", as_index=False)
        .size()
        .rename(columns={"size": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    location_agg["bronze_ingest_date"] = rep_date
    location_agg["bronze_run_id"] = bronze_run_id
    location_agg["time_axis"] = dominant_time_axis(sub["time_axis"]) if "time_axis" in sub.columns else "posted"
    return location_agg


def _build_monthly_company(sub: pd.DataFrame, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    company_source_series = _company_series(sub)
    company_df = pd.DataFrame({"company_name": company_source_series.fillna("").astype(str)})
    company_df["company_name"] = (
        company_df["company_name"].str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    )
    company_df = company_df[company_df["company_name"] != ""]
    company_agg = (
        company_df.groupby("company_name", as_index=False)
        .size()
        .rename(columns={"size": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    company_agg["bronze_ingest_date"] = rep_date
    company_agg["bronze_run_id"] = bronze_run_id
    company_agg["time_axis"] = dominant_time_axis(sub["time_axis"]) if "time_axis" in sub.columns else "posted"
    return company_agg


def run(
    silver_file: str | None = None,
    merged_silver_file: str | None = None,
    pipeline_run_id: str | None = None,
    *,
    cfg: AppConfig | None = None,
) -> dict:
    cfg = cfg or AppConfig()
    df, resolved_path = _resolve_silver_dataframe(cfg, silver_file, merged_silver_file)
    if df.empty:
        raise RuntimeError("Silver dataset is empty.")
    required_cols = {"bronze_ingest_date", "bronze_run_id", "source"}
    missing = required_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Silver file missing lineage columns: {sorted(missing)}")

    source = str(df["source"].iloc[0])
    if str(cfg.source_name) != source:
        raise RuntimeError(f"Silver source {source!r} does not match pipeline source_name={cfg.source_name!r}")
    df = assign_posted_month_and_time_axis(df)
    df = df[df["posted_month"].astype(str).str.match(r"^\d{4}-\d{2}$", na=False)]
    if df.empty:
        raise RuntimeError("No Silver rows with a valid posted_month (check posted_at and bronze_ingest_date).")

    prid = pipeline_run_id or os.environ.get("JMI_PIPELINE_RUN_ID")
    if not prid:
        ordered = df.sort_values(by=["bronze_ingest_date", "bronze_run_id", "ingested_at"])
        prid = str(ordered["bronze_run_id"].iloc[-1])

    months = sorted(df["posted_month"].unique())
    skill_outputs: list[str] = []
    role_outputs: list[str] = []
    location_outputs: list[str] = []
    company_outputs: list[str] = []
    summary_outputs: list[str] = []
    summary_rows: list[dict] = []

    for posted_month in months:
        sub = df[df["posted_month"] == posted_month]
        rep_date = str(sub["bronze_ingest_date"].max())

        skill_agg = _build_monthly_skill(sub, rep_date, prid)
        role_agg = _build_monthly_role(sub, rep_date, prid)
        location_agg = _build_monthly_location(sub, rep_date, prid)
        company_agg = _build_monthly_company(sub, rep_date, prid)
        # Live Athena/Glue tables expect `source` in the Parquet body (path may omit source= prefix).
        skill_agg["source"] = source
        role_agg["source"] = source
        location_agg["source"] = source
        company_agg["source"] = source

        skill_out_path = gold_fact_partition(cfg, "skill_demand_monthly", posted_month=posted_month, pipeline_run_id=prid)
        role_out_path = gold_fact_partition(cfg, "role_demand_monthly", posted_month=posted_month, pipeline_run_id=prid)
        location_out_path = gold_fact_partition(cfg, "location_demand_monthly", posted_month=posted_month, pipeline_run_id=prid)
        company_out_path = gold_fact_partition(cfg, "company_hiring_monthly", posted_month=posted_month, pipeline_run_id=prid)

        write_parquet(skill_out_path, skill_agg)
        write_parquet(role_out_path, role_agg)
        write_parquet(location_out_path, location_agg)
        write_parquet(company_out_path, company_agg)

        skill_row_count = int(len(skill_agg))
        role_row_count = int(len(role_agg))
        location_row_count = int(len(location_agg))
        company_row_count = int(len(company_agg))
        status = "PASS"

        summary_df = pd.DataFrame(
            [
                {
                    "source": source,
                    "bronze_ingest_date": rep_date,
                    "bronze_run_id": prid,
                    "skill_row_count": skill_row_count,
                    "role_row_count": role_row_count,
                    "location_row_count": location_row_count,
                    "company_row_count": company_row_count,
                    "status": status,
                    "time_axis": dominant_time_axis(sub["time_axis"]) if "time_axis" in sub.columns else "posted",
                }
            ]
        )

        summary_out_path = gold_fact_partition(cfg, "pipeline_run_summary", posted_month=posted_month, pipeline_run_id=prid)
        write_parquet(summary_out_path, summary_df)

        skill_outputs.append(str(skill_out_path))
        role_outputs.append(str(role_out_path))
        location_outputs.append(str(location_out_path))
        company_outputs.append(str(company_out_path))
        summary_outputs.append(str(summary_out_path))
        summary_rows.append(
            {
                "posted_month": posted_month,
                "bronze_ingest_date": rep_date,
                "skill_row_count": skill_row_count,
                "role_row_count": role_row_count,
                "location_row_count": location_row_count,
                "company_row_count": company_row_count,
            }
        )

    # One pointer per source run (paths: gold/source=<slug>/latest_run_metadata/); sources do not overwrite each other.
    latest_meta_path = gold_latest_run_metadata_file(cfg)
    write_parquet(latest_meta_path, pd.DataFrame([{"run_id": prid}]))

    payload = {
        "stage": "gold",
        "pipeline_run_id": prid,
        "source": source,
        "posted_months_rebuilt": months,
        "time_grain": "posted_month",
        "summary_by_posted_month": summary_rows,
        "source_silver_file": resolved_path,
        "skill_output_files": skill_outputs,
        "role_output_files": role_outputs,
        "location_output_files": location_outputs,
        "company_output_files": company_outputs,
        "pipeline_run_summary_output_files": summary_outputs,
        "latest_run_metadata_file": str(latest_meta_path),
        "latest_run_metadata_skipped": False,
    }
    try:
        from src.jmi.pipelines.transform_derived_comparison import run_derived_comparison

        payload["derived_comparison"] = run_derived_comparison(cfg)
    except Exception as exc:
        payload["derived_comparison"] = {"status": "ERROR", "error": str(exc)}
    try:
        from src.jmi.pipelines.transform_derived_strict_common import run_derived_strict_common

        payload["derived_strict_common"] = run_derived_strict_common(cfg)
    except Exception as exc:
        payload["derived_strict_common"] = {"status": "ERROR", "error": str(exc)}
    (cfg.quality_root / f"gold_quality_{prid}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver → Gold aggregates.")
    parser.add_argument(
        "--source",
        default=None,
        metavar="NAME",
        help="Silver source partition for merged/latest path (e.g. adzuna_in). Default: arbeitnow.",
    )
    parser.add_argument("--silver-file", default=None, metavar="PATH")
    parser.add_argument("--merged-silver-file", default=None, metavar="PATH")
    args = parser.parse_args()
    base_cfg = AppConfig()
    if args.source:
        base_cfg = replace(base_cfg, source_name=args.source)
    print(
        json.dumps(
            run(
                args.silver_file,
                args.merged_silver_file,
                cfg=base_cfg,
            ),
            indent=2,
        )
    )

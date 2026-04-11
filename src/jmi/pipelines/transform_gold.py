from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pandas as pd

from src.jmi.config import AppConfig, DataPath
from src.jmi.utils.io import write_parquet

# Conservative cleanup for location group-by keys (whitespace, commas, obvious duplicates).
_WS_RUN = re.compile(r"\s+")
_COMMA_RUN = re.compile(r",+")
_SEGMENT_EDGE_PUNCT = re.compile(r"^[\s.,;:|/\\-]+|[\s.,;:|/\\-]+$")

_CANONICAL_LOCATION_ALIASES: dict[str, str] = {
    "frankfurt": "frankfurt am main",
}


def _clean_location_segment(raw: str) -> str:
    seg = _SEGMENT_EDGE_PUNCT.sub("", _WS_RUN.sub(" ", raw.strip()))
    return seg


def _normalize_location_label(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = _WS_RUN.sub(" ", text)
    text = _COMMA_RUN.sub(",", text)
    parts: list[str] = []
    for raw_seg in text.split(","):
        seg = _clean_location_segment(raw_seg)
        if seg:
            parts.append(seg)
    if not parts:
        return ""
    if len(parts) >= 3 and parts[0] == parts[1]:
        parts = [parts[0]]
    deduped: list[str] = [parts[0]]
    for seg in parts[1:]:
        if seg != deduped[-1]:
            deduped.append(seg)
    if len(deduped) == 1:
        out = deduped[0]
    elif len(deduped) == 2 and deduped[0] == "berlin" and deduped[1] == "germany":
        out = "berlin"
    else:
        out = ", ".join(deduped)
    return _CANONICAL_LOCATION_ALIASES.get(out, out)


def _merged_silver_path(cfg: AppConfig) -> DataPath:
    return cfg.silver_root / "jobs" / f"source={cfg.source_name}" / "merged" / "latest.parquet"


def _latest_silver_file(cfg: AppConfig) -> Path:
    if cfg.silver_root.is_s3:
        raise FileNotFoundError("Pass silver_file or merged_silver_file when JMI_DATA_ROOT is S3.")
    files = sorted(
        cfg.silver_root.as_path().glob("jobs/ingest_date=*/run_id=*/part-*.parquet"),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise FileNotFoundError("No silver files found. Run silver transform first.")
    return files[-1]


def _resolve_silver_dataframe(
    cfg: AppConfig,
    silver_file: str | None,
    merged_silver_file: str | None,
) -> tuple[pd.DataFrame, str]:
    candidates: list[str] = []
    if merged_silver_file:
        candidates.append(merged_silver_file)
    env_m = os.environ.get("JMI_MERGED_SILVER_FILE")
    if env_m:
        candidates.append(env_m)
    candidates.append(str(_merged_silver_path(cfg)))
    if silver_file:
        candidates.append(silver_file)
    candidates.append(str(_latest_silver_file(cfg)))

    seen: set[str] = set()
    for c in candidates:
        if not c or c in seen:
            continue
        seen.add(c)
        try:
            frame = pd.read_parquet(c)
            if frame is not None and not frame.empty:
                return frame, c
        except Exception:
            continue
    raise FileNotFoundError("No readable non-empty silver parquet found.")


def _build_monthly_skill(sub: pd.DataFrame, source: str, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    skill_df = sub[["job_id", "skills"]].explode("skills").dropna(subset=["skills"])
    skill_agg = (
        skill_df.groupby("skills", as_index=False)["job_id"]
        .nunique()
        .rename(columns={"skills": "skill", "job_id": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    skill_agg["source"] = source
    skill_agg["bronze_ingest_date"] = rep_date
    skill_agg["bronze_run_id"] = bronze_run_id
    return skill_agg


def _build_monthly_role(sub: pd.DataFrame, source: str, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    role_source_series = sub["title_clean"] if "title_clean" in sub.columns else sub["title"]
    role_df = pd.DataFrame({"role": role_source_series.fillna("").astype(str)})
    role_df["role"] = role_df["role"].str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    role_df = role_df[role_df["role"] != ""]
    role_agg = (
        role_df.groupby("role", as_index=False)
        .size()
        .rename(columns={"size": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    role_agg["source"] = source
    role_agg["bronze_ingest_date"] = rep_date
    role_agg["bronze_run_id"] = bronze_run_id
    return role_agg


def _build_monthly_location(sub: pd.DataFrame, source: str, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    location_source_series = sub["location"] if "location" in sub.columns else pd.Series([], dtype="object")
    location_df = pd.DataFrame({"location": location_source_series.fillna("").astype(str)})
    location_df["location"] = location_df["location"].map(_normalize_location_label)
    location_df = location_df[location_df["location"] != ""]
    location_agg = (
        location_df.groupby("location", as_index=False)
        .size()
        .rename(columns={"size": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    location_agg["source"] = source
    location_agg["bronze_ingest_date"] = rep_date
    location_agg["bronze_run_id"] = bronze_run_id
    return location_agg


def _build_monthly_company(sub: pd.DataFrame, source: str, rep_date: str, bronze_run_id: str) -> pd.DataFrame:
    company_source_series = (
        sub["company_name"] if "company_name" in sub.columns else pd.Series([], dtype="object")
    )
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
    company_agg["source"] = source
    company_agg["bronze_ingest_date"] = rep_date
    company_agg["bronze_run_id"] = bronze_run_id
    return company_agg


def run(
    silver_file: str | None = None,
    merged_silver_file: str | None = None,
    pipeline_run_id: str | None = None,
) -> dict:
    cfg = AppConfig()
    df, resolved_path = _resolve_silver_dataframe(cfg, silver_file, merged_silver_file)
    if df.empty:
        raise RuntimeError("Silver dataset is empty.")
    required_cols = {"bronze_ingest_date", "bronze_run_id", "source"}
    missing = required_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Silver file missing lineage columns: {sorted(missing)}")

    source = str(df["source"].iloc[0])
    df = df.copy()
    df["ingest_month"] = df["bronze_ingest_date"].astype(str).str[:7]
    bad = df[df["ingest_month"].str.len() != 7]
    if not bad.empty:
        raise RuntimeError("Invalid bronze_ingest_date values in silver data.")

    prid = pipeline_run_id or os.environ.get("JMI_PIPELINE_RUN_ID")
    if not prid:
        ordered = df.sort_values(by=["bronze_ingest_date", "bronze_run_id", "ingested_at"])
        prid = str(ordered["bronze_run_id"].iloc[-1])

    months = sorted(df["ingest_month"].unique())
    skill_outputs: list[str] = []
    role_outputs: list[str] = []
    location_outputs: list[str] = []
    company_outputs: list[str] = []
    summary_outputs: list[str] = []
    summary_rows: list[dict] = []

    for ingest_month in months:
        sub = df[df["ingest_month"] == ingest_month]
        rep_date = str(sub["bronze_ingest_date"].max())

        skill_agg = _build_monthly_skill(sub, source, rep_date, prid)
        role_agg = _build_monthly_role(sub, source, rep_date, prid)
        location_agg = _build_monthly_location(sub, source, rep_date, prid)
        company_agg = _build_monthly_company(sub, source, rep_date, prid)

        skill_out_path = (
            cfg.gold_root
            / "skill_demand_monthly"
            / f"ingest_month={ingest_month}"
            / f"run_id={prid}"
            / "part-00001.parquet"
        )
        role_out_path = (
            cfg.gold_root
            / "role_demand_monthly"
            / f"ingest_month={ingest_month}"
            / f"run_id={prid}"
            / "part-00001.parquet"
        )
        location_out_path = (
            cfg.gold_root
            / "location_demand_monthly"
            / f"ingest_month={ingest_month}"
            / f"run_id={prid}"
            / "part-00001.parquet"
        )
        company_out_path = (
            cfg.gold_root
            / "company_hiring_monthly"
            / f"ingest_month={ingest_month}"
            / f"run_id={prid}"
            / "part-00001.parquet"
        )

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
                }
            ]
        )

        summary_out_path = (
            cfg.gold_root
            / "pipeline_run_summary"
            / f"ingest_month={ingest_month}"
            / f"run_id={prid}"
            / "part-00001.parquet"
        )
        write_parquet(summary_out_path, summary_df)

        skill_outputs.append(str(skill_out_path))
        role_outputs.append(str(role_out_path))
        location_outputs.append(str(location_out_path))
        company_outputs.append(str(company_out_path))
        summary_outputs.append(str(summary_out_path))
        summary_rows.append(
            {
                "ingest_month": ingest_month,
                "bronze_ingest_date": rep_date,
                "skill_row_count": skill_row_count,
                "role_row_count": role_row_count,
                "location_row_count": location_row_count,
                "company_row_count": company_row_count,
            }
        )

    payload = {
        "stage": "gold",
        "pipeline_run_id": prid,
        "source": source,
        "ingest_months_rebuilt": months,
        "summary_by_month": summary_rows,
        "source_silver_file": resolved_path,
        "skill_output_files": skill_outputs,
        "role_output_files": role_outputs,
        "location_output_files": location_outputs,
        "company_output_files": company_outputs,
        "pipeline_run_summary_output_files": summary_outputs,
    }
    (cfg.quality_root / f"gold_quality_{prid}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))

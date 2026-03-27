from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.jmi.config import AppConfig
from src.jmi.utils.io import write_parquet


def _latest_silver_file(cfg: AppConfig) -> Path:
    files = sorted(
        cfg.silver_root.as_path().glob("jobs/ingest_date=*/run_id=*/part-*.parquet"),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise FileNotFoundError("No silver files found. Run silver transform first.")
    return files[-1]


def run(silver_file: str | None = None) -> dict:
    cfg = AppConfig()
    silver_file_str = silver_file or str(_latest_silver_file(cfg))
    df = pd.read_parquet(silver_file_str)
    if df.empty:
        raise RuntimeError("Silver dataset is empty.")
    required_cols = {"bronze_ingest_date", "bronze_run_id", "source"}
    missing = required_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Silver file missing lineage columns: {sorted(missing)}")

    bronze_ingest_date = str(df["bronze_ingest_date"].iloc[0])
    bronze_run_id = str(df["bronze_run_id"].iloc[0])
    source = str(df["source"].iloc[0])
    ingest_month = bronze_ingest_date[:7]
    if len(ingest_month) != 7 or ingest_month[4] != "-":
        raise RuntimeError(f"Invalid bronze_ingest_date value: {bronze_ingest_date}")

    # Explode skill arrays for dashboard-friendly aggregates.
    skill_df = df[["job_id", "skills"]].explode("skills").dropna(subset=["skills"])
    skill_agg = (
        skill_df.groupby("skills", as_index=False)["job_id"]
        .nunique()
        .rename(columns={"skills": "skill", "job_id": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    skill_agg["source"] = source
    skill_agg["bronze_ingest_date"] = bronze_ingest_date
    skill_agg["bronze_run_id"] = bronze_run_id

    skill_out_path = (
        cfg.gold_root
        / "skill_demand_monthly"
        / f"ingest_month={ingest_month}"
        / f"run_id={bronze_run_id}"
        / "part-00001.parquet"
    )
    write_parquet(skill_out_path, skill_agg)

    role_source_series = df["title_clean"] if "title_clean" in df.columns else df["title"]
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
    role_agg["bronze_ingest_date"] = bronze_ingest_date
    role_agg["bronze_run_id"] = bronze_run_id

    role_out_path = (
        cfg.gold_root
        / "role_demand_monthly"
        / f"ingest_month={ingest_month}"
        / f"run_id={bronze_run_id}"
        / "part-00001.parquet"
    )
    write_parquet(role_out_path, role_agg)

    location_source_series = df["location"] if "location" in df.columns else pd.Series([], dtype="object")
    location_df = pd.DataFrame({"location": location_source_series.fillna("").astype(str)})
    location_df["location"] = (
        location_df["location"].str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    )
    location_df = location_df[location_df["location"] != ""]
    location_agg = (
        location_df.groupby("location", as_index=False)
        .size()
        .rename(columns={"size": "job_count"})
        .sort_values("job_count", ascending=False)
    )
    location_agg["source"] = source
    location_agg["bronze_ingest_date"] = bronze_ingest_date
    location_agg["bronze_run_id"] = bronze_run_id

    location_out_path = (
        cfg.gold_root
        / "location_demand_monthly"
        / f"ingest_month={ingest_month}"
        / f"run_id={bronze_run_id}"
        / "part-00001.parquet"
    )
    write_parquet(location_out_path, location_agg)

    payload = {
        "stage": "gold",
        "ingest_month": ingest_month,
        "bronze_ingest_date": bronze_ingest_date,
        "bronze_run_id": bronze_run_id,
        "source": source,
        "skill_row_count": int(len(skill_agg)),
        "role_row_count": int(len(role_agg)),
        "location_row_count": int(len(location_agg)),
        "source_silver_file": silver_file_str,
        "skill_output_file": str(skill_out_path),
        "role_output_file": str(role_out_path),
        "location_output_file": str(location_out_path),
    }
    (cfg.quality_root / f"gold_quality_{ingest_month}_{bronze_run_id}.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    return payload


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))

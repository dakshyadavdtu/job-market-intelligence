from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from src.jmi.config import AppConfig, DataPath, split_s3_uri
from src.jmi.connectors.skill_extract import extract_silver_skills
from src.jmi.pipelines.silver_schema import (
    CANONICAL_SILVER_COLUMN_ORDER,
    align_silver_dataframe_to_canonical,
    employment_type_from_arbeitnow_payload,
    normalize_company_norm,
    normalize_title_norm,
    posted_at_iso_utc,
    remote_type_from_arbeitnow_payload,
    strip_html_description,
)
from src.jmi.utils.io import read_jsonl_gz, write_parquet
from src.jmi.utils.quality import run_silver_checks


def _latest_bronze_file(cfg: AppConfig) -> Path:
    if cfg.bronze_root.is_s3:
        raise FileNotFoundError("Pass bronze_file when JMI_DATA_ROOT is S3.")
    files = sorted(
        cfg.bronze_root.as_path().glob(f"source={cfg.source_name}/ingest_date=*/run_id=*/raw.jsonl.gz"),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise FileNotFoundError("No bronze files found. Run ingest first.")
    return files[-1]


def _merged_silver_path(cfg: AppConfig) -> DataPath:
    return cfg.silver_root / "jobs" / f"source={cfg.source_name}" / "merged" / "latest.parquet"


_RUN_RE = re.compile(r"(?:^|/)run_id=([^/]+)(?:/|$)")
_DATE_RE = re.compile(r"(?:^|/)ingest_date=([^/]+)(?:/|$)")


def _extract_lineage_from_bronze_path(path: str) -> tuple[str, str]:
    run_match = _RUN_RE.search(path)
    date_match = _DATE_RE.search(path)
    run_id = run_match.group(1) if run_match else ""
    bronze_ingest_date = date_match.group(1) if date_match else ""
    if not run_id or not bronze_ingest_date:
        raise RuntimeError(f"Cannot extract lineage from path: {path}")
    return run_id, bronze_ingest_date


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _source_job_id_from_arbeitnow(slug: str) -> str | None:
    return slug if slug else None


def _prior_partition_silver_frames(cfg: AppConfig) -> pd.DataFrame | None:
    """When merged/latest.parquet is missing, rebuild prior state from historical per-run partitions."""
    jobs_root = cfg.silver_root / "jobs"
    paths: list[str] = []
    if jobs_root.is_s3:
        import boto3  # type: ignore

        bucket, prefix = split_s3_uri(str(jobs_root).rstrip("/") + "/")
        client = boto3.client("s3")
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents") or []:
                k = obj["Key"]
                if "/merged/" in k:
                    continue
                if k.endswith(".parquet") and "part-" in k:
                    paths.append(f"s3://{bucket}/{k}")
    else:
        base = jobs_root.as_path()
        paths = sorted(str(p) for p in base.glob("ingest_date=*/run_id=*/part-*.parquet"))
    if not paths:
        return None
    frames = [align_silver_dataframe_to_canonical(pd.read_parquet(p)) for p in paths]
    combined = pd.concat(frames, ignore_index=True)
    combined["_sd"] = combined["bronze_ingest_date"].astype(str)
    combined["_sr"] = combined["bronze_run_id"].astype(str)
    combined["_si"] = combined["ingested_at"].astype(str)
    combined = combined.sort_values(by=["_sd", "_sr", "_si"])
    combined = combined.drop_duplicates(subset=["job_id"], keep="last")
    return combined.drop(columns=["_sd", "_sr", "_si"])


def _merge_with_prior_silver(cfg: AppConfig, df_batch: pd.DataFrame) -> pd.DataFrame:
    merged_path = _merged_silver_path(cfg)
    path_str = str(merged_path)
    try:
        df_old = align_silver_dataframe_to_canonical(pd.read_parquet(path_str))
    except Exception:
        df_old = _prior_partition_silver_frames(cfg)
    if df_old is None or df_old.empty:
        return df_batch
    combined = pd.concat([df_old, df_batch], ignore_index=True)
    combined["_sd"] = combined["bronze_ingest_date"].astype(str)
    combined["_sr"] = combined["bronze_run_id"].astype(str)
    combined["_si"] = combined["ingested_at"].astype(str)
    combined = combined.sort_values(by=["_sd", "_sr", "_si"])
    combined = combined.drop_duplicates(subset=["job_id"], keep="last")
    return combined.drop(columns=["_sd", "_sr", "_si"])


def run(bronze_file: str | None = None) -> dict:
    cfg = AppConfig()
    bronze_file_str = bronze_file or str(_latest_bronze_file(cfg))
    bronze_run_id, bronze_ingest_date = _extract_lineage_from_bronze_path(bronze_file_str)
    bronze_rows = read_jsonl_gz(bronze_file_str)
    if not bronze_rows:
        raise RuntimeError("Bronze file is empty.")

    flattened: list[dict] = []
    for row in bronze_rows:
        payload = row.get("raw_payload", {})
        title = _clean_text(payload.get("title"))
        company = _clean_text(payload.get("company_name"))
        location = _clean_text(payload.get("location"))
        slug = _clean_text(row.get("source_slug") or payload.get("slug"))
        desc_stripped = strip_html_description(_clean_text(payload.get("description")))
        skills = extract_silver_skills(payload.get("tags"), title, desc_stripped)
        rid = row.get("run_id", bronze_run_id)

        flattened.append(
            {
                "job_id": row.get("job_id"),
                "job_id_strategy": row.get("job_id_strategy", ""),
                "source": row.get("source"),
                "source_job_id": _source_job_id_from_arbeitnow(slug),
                "title_norm": normalize_title_norm(title),
                "company_norm": normalize_company_norm(company),
                "location_raw": location,
                "remote_type": remote_type_from_arbeitnow_payload(payload),
                "employment_type": employment_type_from_arbeitnow_payload(payload),
                "skills": skills,
                "posted_at": posted_at_iso_utc(payload),
                "ingested_at": row.get("ingested_at"),
                "bronze_run_id": rid,
                "bronze_ingest_date": row.get("bronze_ingest_date", bronze_ingest_date),
                "bronze_data_file": bronze_file_str,
            }
        )

    raw_df = pd.DataFrame(flattened)
    if raw_df.empty:
        raise RuntimeError("No rows could be flattened from bronze data.")

    raw_df = align_silver_dataframe_to_canonical(raw_df)
    pre_dedup_count = int(len(raw_df))
    df_batch = raw_df.drop_duplicates(subset=["job_id"], keep="first").copy()
    post_dedup_count = int(len(df_batch))
    dedup_removed = pre_dedup_count - post_dedup_count

    df_batch = df_batch[[c for c in CANONICAL_SILVER_COLUMN_ORDER if c in df_batch.columns]]

    report = run_silver_checks(df_batch, bronze_row_count=len(bronze_rows))
    if report.status != "PASS":
        raise RuntimeError(
            f"Silver quality checks failed: missing_title={report.missing_title}, "
            f"missing_company={report.missing_company}, duplicate_job_id={report.duplicate_job_id}, "
            f"duplicate_source_key={report.duplicate_source_key}"
        )

    df_merged = _merge_with_prior_silver(cfg, df_batch)
    df_merged = df_merged[[c for c in CANONICAL_SILVER_COLUMN_ORDER if c in df_merged.columns]]

    out_path = (
        cfg.silver_root
        / "jobs"
        / f"ingest_date={bronze_ingest_date}"
        / f"run_id={bronze_run_id}"
        / "part-00001.parquet"
    )
    write_parquet(out_path, df_batch)

    merged_path = _merged_silver_path(cfg)
    write_parquet(merged_path, df_merged)

    quality_payload = {
        "stage": "silver",
        "status": report.status,
        "checks_passed": report.checks_passed,
        "checks_failed": report.checks_failed,
        "bronze_run_id": bronze_run_id,
        "bronze_ingest_date": bronze_ingest_date,
        "bronze_row_count": len(bronze_rows),
        "silver_row_count_before_dedup": pre_dedup_count,
        "silver_row_count_after_dedup": post_dedup_count,
        "dedup_rows_removed": dedup_removed,
        "silver_merged_row_count": int(len(df_merged)),
        "row_count": report.row_count,
        "missing_title": report.missing_title,
        "missing_company": report.missing_company,
        "duplicate_job_id": report.duplicate_job_id,
        "duplicate_source_key": report.duplicate_source_key,
        "source_bronze_file": bronze_file_str,
        "output_file": str(out_path),
        "merged_silver_file": str(merged_path),
    }
    quality_file = cfg.quality_root / f"silver_quality_{bronze_ingest_date}_{bronze_run_id}.json"
    quality_file.write_text(json.dumps(quality_payload, indent=2), encoding="utf-8")
    return quality_payload


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))

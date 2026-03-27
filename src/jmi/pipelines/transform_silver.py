from __future__ import annotations

import json
from pathlib import Path
import re

import pandas as pd

from src.jmi.config import AppConfig
from src.jmi.connectors.arbeitnow import normalize_skill_tokens
from src.jmi.utils.io import read_jsonl_gz, write_parquet
from src.jmi.utils.quality import run_silver_checks


def _latest_bronze_file(cfg: AppConfig) -> Path:
    files = sorted(
        cfg.bronze_root.as_path().glob(f"source={cfg.source_name}/ingest_date=*/run_id=*/raw.jsonl.gz"),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise FileNotFoundError("No bronze files found. Run ingest first.")
    return files[-1]


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
        remote = bool(payload.get("remote", False))
        url = _clean_text(payload.get("url"))
        created = str(payload.get("created_at", "") or "")
        skills = normalize_skill_tokens(payload.get("tags"))
        source_slug = _clean_text(row.get("source_slug") or payload.get("slug"))
        source_record_key = source_slug or url or _clean_text(row.get("job_id"))

        flattened.append(
            {
                "job_id": row.get("job_id"),
                "job_id_strategy": row.get("job_id_strategy", ""),
                "source_record_key": source_record_key,
                "source": row.get("source"),
                "schema_version": row.get("schema_version"),
                "title": title.lower(),
                "title_clean": title,
                "company_name": company,
                "location": location,
                "is_remote": remote,
                "published_at_raw": created,
                "skills": skills,
                "posting_url": url,
                "ingested_at": row.get("ingested_at"),
                "bronze_run_id": row.get("run_id", bronze_run_id),
                "bronze_ingest_date": row.get("bronze_ingest_date", bronze_ingest_date),
                "bronze_data_file": bronze_file_str,
            }
        )

    raw_df = pd.DataFrame(flattened)
    if raw_df.empty:
        raise RuntimeError("No rows could be flattened from bronze data.")

    pre_dedup_count = int(len(raw_df))
    df = raw_df.drop_duplicates(subset=["job_id"], keep="first").copy()
    post_dedup_count = int(len(df))
    dedup_removed = pre_dedup_count - post_dedup_count

    report = run_silver_checks(df, bronze_row_count=len(bronze_rows))
    if report.status != "PASS":
        raise RuntimeError(
            f"Silver quality checks failed: missing_title={report.missing_title}, "
            f"missing_company={report.missing_company}, duplicate_job_id={report.duplicate_job_id}, "
            f"duplicate_source_key={report.duplicate_source_key}"
        )

    out_path = (
        cfg.silver_root
        / "jobs"
        / f"ingest_date={bronze_ingest_date}"
        / f"run_id={bronze_run_id}"
        / "part-00001.parquet"
    )
    write_parquet(out_path, df)

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
        "row_count": report.row_count,
        "missing_title": report.missing_title,
        "missing_company": report.missing_company,
        "duplicate_job_id": report.duplicate_job_id,
        "duplicate_source_key": report.duplicate_source_key,
        "source_bronze_file": bronze_file_str,
        "output_file": str(out_path),
    }
    quality_file = cfg.quality_root / f"silver_quality_{bronze_ingest_date}_{bronze_run_id}.json"
    quality_file.write_text(json.dumps(quality_payload, indent=2), encoding="utf-8")
    return quality_payload


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))

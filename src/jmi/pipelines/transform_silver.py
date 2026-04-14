from __future__ import annotations

import argparse
import json
import re
from dataclasses import replace
from pathlib import Path

import pandas as pd

from src.jmi.config import AppConfig, DataPath, split_s3_uri
from src.jmi.paths import arbeitnow_slice_tag, silver_jobs_batch_part, silver_jobs_merged_latest, silver_legacy_flat_jobs_root
from src.jmi.connectors.adzuna import ADZUNA_SOURCE_SLUG
from src.jmi.connectors.skill_extract import adzuna_enrich_weak_skills, extract_silver_skills
from src.jmi.pipelines.gold_time import assign_posted_month_and_time_axis
from src.jmi.pipelines.silver_schema import (
    adzuna_location_for_silver,
    adzuna_skill_blob_context,
    adzuna_title_norm_for_silver,
    align_silver_dataframe_to_canonical,
    normalize_company_norm,
    normalize_location_raw,
    normalize_title_norm,
    posted_at_iso_from_payload,
    project_silver_to_contract,
    remote_type_for_silver,
    strip_html_description,
)
from src.jmi.utils.io import read_jsonl_gz, write_parquet
from src.jmi.utils.quality import run_silver_checks


def _silver_month_span_metrics(df: pd.DataFrame | None) -> tuple[int, int]:
    """(n_distinct_valid_posted_month, n_rows_with_valid_posted_month)."""
    if df is None or df.empty:
        return (0, 0)
    d = assign_posted_month_and_time_axis(df.copy())
    d = d[d["posted_month"].astype(str).str.match(r"^\d{4}-\d{2}$", na=False)]
    if d.empty:
        return (0, 0)
    return (int(d["posted_month"].nunique()), int(len(d)))


def _latest_bronze_file(cfg: AppConfig) -> Path:
    if cfg.bronze_root.is_s3:
        raise FileNotFoundError("Pass bronze_file when JMI_DATA_ROOT is S3.")
    slice_tag = arbeitnow_slice_tag()
    if cfg.source_name == "arbeitnow" and slice_tag:
        pat = f"source=arbeitnow/slice={slice_tag}/ingest_date=*/run_id=*/raw.jsonl.gz"
    else:
        pat = f"source={cfg.source_name}/ingest_date=*/run_id=*/raw.jsonl.gz"
    files = sorted(
        cfg.bronze_root.as_path().glob(pat),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise FileNotFoundError("No bronze files found. Run ingest first.")
    return files[-1]


def _merged_silver_path(cfg: AppConfig) -> DataPath:
    return silver_jobs_merged_latest(cfg)


def _silver_batch_out_path(cfg: AppConfig, bronze_ingest_date: str, bronze_run_id: str) -> DataPath:
    """Modular layout: silver/jobs/source=<slug>/ingest_date=.../run_id=.../part-00001.parquet (all sources)."""
    return silver_jobs_batch_part(cfg, bronze_ingest_date, bronze_run_id)


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


def _source_job_id_from_row(row: dict, slug: str) -> str | None:
    if str(row.get("source") or "") == ADZUNA_SOURCE_SLUG:
        sji = _clean_text(row.get("source_job_id"))
        return sji if sji else None
    return _source_job_id_from_arbeitnow(slug)


def _flat_payload_fields(source: str, payload: dict) -> tuple[str, str, str, object | None]:
    """Return (title, company_name, location, tags_or_none) for skill extraction."""
    if source == ADZUNA_SOURCE_SLUG:
        title = _clean_text(payload.get("title"))
        company = ""
        comp = payload.get("company")
        if isinstance(comp, dict):
            company = _clean_text(comp.get("display_name"))
        loc = adzuna_location_for_silver(payload)
        return title, company, loc, None

    title = _clean_text(payload.get("title"))
    company = _clean_text(payload.get("company_name"))
    location = _clean_text(payload.get("location"))
    return title, company, location, payload.get("tags")


def load_silver_jobs_history_union(cfg: AppConfig) -> pd.DataFrame | None:
    """All per-run Silver batch Parquet files for this source (excludes merged/), deduped by job_id."""
    jobs_root = cfg.silver_root / "jobs"
    paths: list[str] = []
    slice_tag = arbeitnow_slice_tag()
    if jobs_root.is_s3:
        import boto3  # type: ignore

        client = boto3.client("s3")

        def collect_parts(bucket: str, pfx: str, require_source: str | None) -> None:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=pfx):
                for obj in page.get("Contents") or []:
                    k = obj["Key"]
                    if "/merged/" in k:
                        continue
                    if not (k.endswith(".parquet") and "part-" in k):
                        continue
                    if require_source:
                        if f"/source={require_source}/" not in k:
                            continue
                    paths.append(f"s3://{bucket}/{k}")

        bucket, prefix = split_s3_uri(str(jobs_root).rstrip("/") + "/")
        collect_parts(bucket, prefix, require_source=cfg.source_name)

        if cfg.source_name == "arbeitnow" and not slice_tag:
            leg = silver_legacy_flat_jobs_root(cfg)
            if leg.is_s3:
                lb, lp = split_s3_uri(str(leg).rstrip("/") + "/")
                collect_parts(lb, lp, require_source=None)
                paths = [
                    u
                    for u in paths
                    if "/source=arbeitnow/" in u
                    or ("/silver_legacy/jobs/" in u and "/ingest_date=" in u and "/source=" not in u)
                ]
            else:
                paths = [u for u in paths if "/source=arbeitnow/" in u]
        elif cfg.source_name == "arbeitnow" and slice_tag:
            paths = [u for u in paths if f"/slice={slice_tag}/" in u]
        elif cfg.source_name == "arbeitnow" and not slice_tag:
            paths = [u for u in paths if "/slice=" not in u]
    else:
        base = jobs_root.as_path()
        if cfg.source_name == "arbeitnow" and slice_tag:
            sub = base / f"source={cfg.source_name}" / f"slice={slice_tag}"
            paths = sorted(str(p) for p in sub.glob("ingest_date=*/run_id=*/part-*.parquet"))
        else:
            sub = base / f"source={cfg.source_name}"
            paths = sorted(str(p) for p in sub.glob("ingest_date=*/run_id=*/part-*.parquet"))
            if cfg.source_name == "arbeitnow":
                leg = silver_legacy_flat_jobs_root(cfg).as_path()
                if leg.exists():
                    paths.extend(sorted(str(p) for p in leg.glob("ingest_date=*/run_id=*/part-*.parquet")))
        paths = sorted(set(paths))
    if not paths:
        return None
    frames = [align_silver_dataframe_to_canonical(pd.read_parquet(p)) for p in paths]
    combined = pd.concat(frames, ignore_index=True)
    combined["_sd"] = combined["bronze_ingest_date"].astype(str)
    combined["_sr"] = combined["bronze_run_id"].astype(str)
    combined["_si"] = combined["ingested_at"].astype(str)
    combined = combined.sort_values(by=["_sd", "_sr", "_si"])
    combined = combined.drop_duplicates(subset=["job_id"], keep="last")
    combined = combined.drop(columns=["_sd", "_sr", "_si"])
    return project_silver_to_contract(combined)


def _prior_partition_silver_frames(cfg: AppConfig) -> pd.DataFrame | None:
    """Backward-compatible name for Gold / merge repair."""
    return load_silver_jobs_history_union(cfg)


def _merge_with_prior_silver(cfg: AppConfig, df_batch: pd.DataFrame) -> pd.DataFrame:
    merged_path = _merged_silver_path(cfg)
    path_str = str(merged_path)
    df_old: pd.DataFrame | None = None
    try:
        df_old = align_silver_dataframe_to_canonical(pd.read_parquet(path_str))
        if df_old.empty:
            df_old = None
    except Exception:
        df_old = None

    df_union = load_silver_jobs_history_union(cfg)
    mu, ru = _silver_month_span_metrics(df_union)
    mm, rm = _silver_month_span_metrics(df_old)
    if df_union is not None and not df_union.empty:
        if df_old is None or mu > mm or (mu == mm and ru > rm):
            df_old = df_union

    if df_old is None or df_old.empty:
        return df_batch
    combined = pd.concat([df_old, df_batch], ignore_index=True)
    combined["_sd"] = combined["bronze_ingest_date"].astype(str)
    combined["_sr"] = combined["bronze_run_id"].astype(str)
    combined["_si"] = combined["ingested_at"].astype(str)
    combined = combined.sort_values(by=["_sd", "_sr", "_si"])
    combined = combined.drop_duplicates(subset=["job_id"], keep="last")
    combined = combined.drop(columns=["_sd", "_sr", "_si"])
    return project_silver_to_contract(combined)


def run(bronze_file: str | None = None, *, cfg: AppConfig | None = None) -> dict:
    cfg = cfg or AppConfig()
    bronze_file_str = bronze_file or str(_latest_bronze_file(cfg))
    bronze_run_id, bronze_ingest_date = _extract_lineage_from_bronze_path(bronze_file_str)
    bronze_rows = read_jsonl_gz(bronze_file_str)
    if not bronze_rows:
        raise RuntimeError("Bronze file is empty.")

    flattened: list[dict] = []
    for row in bronze_rows:
        payload = row.get("raw_payload", {})
        source = str(row.get("source") or cfg.source_name or "")
        title, company, location, tags = _flat_payload_fields(source, payload)
        slug = _clean_text(row.get("source_slug") or payload.get("slug"))
        desc_stripped = strip_html_description(_clean_text(payload.get("description")))
        extra_skill_ctx = adzuna_skill_blob_context(payload) if source == ADZUNA_SOURCE_SLUG else ""
        skills = extract_silver_skills(tags, title, desc_stripped, extra_context=extra_skill_ctx)
        if source == ADZUNA_SOURCE_SLUG:
            skills = adzuna_enrich_weak_skills(
                skills, title, desc_stripped, extra_context=extra_skill_ctx
            )
        rid = row.get("run_id", bronze_run_id)

        title_norm = (
            adzuna_title_norm_for_silver(title, payload)
            if source == ADZUNA_SOURCE_SLUG
            else normalize_title_norm(title)
        )

        flattened.append(
            {
                "job_id": row.get("job_id"),
                "job_id_strategy": row.get("job_id_strategy", ""),
                "source": row.get("source"),
                "source_job_id": _source_job_id_from_row(row, slug),
                "title_norm": title_norm,
                "company_norm": normalize_company_norm(company),
                "location_raw": normalize_location_raw(location),
                "remote_type": remote_type_for_silver(
                    source, payload, title=title, description_plain=desc_stripped
                ),
                "skills": skills,
                "posted_at": posted_at_iso_from_payload(payload),
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

    df_batch = project_silver_to_contract(df_batch)

    report = run_silver_checks(df_batch, bronze_row_count=len(bronze_rows))
    if report.status != "PASS":
        raise RuntimeError(
            f"Silver quality checks failed: missing_title={report.missing_title}, "
            f"missing_company={report.missing_company}, duplicate_job_id={report.duplicate_job_id}, "
            f"duplicate_source_key={report.duplicate_source_key}"
        )

    df_merged = project_silver_to_contract(_merge_with_prior_silver(cfg, df_batch))

    out_path = _silver_batch_out_path(cfg, bronze_ingest_date, bronze_run_id)
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
    parser = argparse.ArgumentParser(description="Bronze → Silver transform.")
    parser.add_argument(
        "--source",
        default=None,
        metavar="NAME",
        help="Bronze source partition (e.g. adzuna_in). Default: arbeitnow from AppConfig.",
    )
    parser.add_argument(
        "--bronze-file",
        default=None,
        metavar="PATH",
        help="Explicit path to raw.jsonl.gz (optional).",
    )
    args = parser.parse_args()
    base_cfg = AppConfig()
    if args.source:
        base_cfg = replace(base_cfg, source_name=args.source)
    print(json.dumps(run(args.bronze_file, cfg=base_cfg), indent=2))

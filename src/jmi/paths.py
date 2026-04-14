"""
Canonical data lake path layout (local + S3).

Medallion layers are source-partitioned under Bronze/Silver/Gold so Arbeitnow, Adzuna,
and future sources do not share ambiguous prefixes.

Layout (examples):
  bronze/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<id>/raw.jsonl.gz
  silver/jobs/source=<slug>/ingest_date=.../run_id=.../part-00001.parquet
  silver/jobs/source=<slug>/merged/latest.parquet
  silver_legacy/jobs/ingest_date=.../run_id=.../part-00001.parquet  (Arbeitnow flat batches archived here)
  gold/<table>/source=<slug>/posted_month=YYYY-MM/run_id=<id>/part-00001.parquet
  gold/slice=<arbeitnow_tag>/<table>/source=arbeitnow/...  (only when JMI_ARBEITNOW_SLICE is set)
  gold/source=<slug>/latest_run_metadata/part-00001.parquet
  gold_legacy/<table>/...  (archived ingest_month= partitions only; not written by current pipeline)

Legacy comparison parquet prefixes (orphaned if present; not written by the pipeline — use Athena
``jmi_analytics_v2.comparison_*`` and ``v2_*`` views; see ``infra/aws/athena/legacy_comparison_gold_parquet_paths.txt``):
  gold/comparison_strict_common_month/…
  gold/comparison_yearly/…
  gold/comparison_march_only/…
"""

from __future__ import annotations

import os

from src.jmi.config import AppConfig, DataPath


def arbeitnow_slice_tag() -> str | None:
    """When set, Bronze/Silver/Gold for Arbeitnow use isolated paths (see docs in paths module)."""
    v = os.getenv("JMI_ARBEITNOW_SLICE", "").strip()
    return v if v else None


def _arbeitnow_bronze_source_segment(cfg: AppConfig) -> str:
    tag = arbeitnow_slice_tag()
    if cfg.source_name == "arbeitnow" and tag:
        return f"source=arbeitnow/slice={tag}"
    return f"source={cfg.source_name}"


def bronze_raw_gz(cfg: AppConfig, ingest_date: str, run_id: str) -> DataPath:
    return (
        cfg.bronze_root
        / _arbeitnow_bronze_source_segment(cfg)
        / f"ingest_date={ingest_date}"
        / f"run_id={run_id}"
        / "raw.jsonl.gz"
    )


def silver_jobs_merged_latest(cfg: AppConfig) -> DataPath:
    tag = arbeitnow_slice_tag()
    if cfg.source_name == "arbeitnow" and tag:
        return cfg.silver_root / "jobs" / f"source={cfg.source_name}" / f"slice={tag}" / "merged" / "latest.parquet"
    return cfg.silver_root / "jobs" / f"source={cfg.source_name}" / "merged" / "latest.parquet"


def silver_legacy_flat_jobs_root(cfg: AppConfig) -> DataPath:
    """Pre–source-prefix Arbeitnow batches (flat ingest_date/run_id), kept out of silver/jobs/."""
    return cfg.data_root / "silver_legacy" / "jobs"


def silver_jobs_batch_part(cfg: AppConfig, ingest_date: str, bronze_run_id: str) -> DataPath:
    """Per-batch Silver Parquet (canonical modular layout for all sources)."""
    tag = arbeitnow_slice_tag()
    if cfg.source_name == "arbeitnow" and tag:
        return (
            cfg.silver_root
            / "jobs"
            / f"source={cfg.source_name}"
            / f"slice={tag}"
            / f"ingest_date={ingest_date}"
            / f"run_id={bronze_run_id}"
            / "part-00001.parquet"
        )
    return (
        cfg.silver_root
        / "jobs"
        / f"source={cfg.source_name}"
        / f"ingest_date={ingest_date}"
        / f"run_id={bronze_run_id}"
        / "part-00001.parquet"
    )


def gold_root_effective(cfg: AppConfig) -> DataPath:
    """Main Gold under `gold/`; Arbeitnow slice runs under `gold/slice=<tag>/` (does not replace main)."""
    tag = arbeitnow_slice_tag()
    if cfg.source_name == "arbeitnow" and tag:
        return cfg.gold_root / f"slice={tag}"
    return cfg.gold_root


def gold_fact_partition(
    cfg: AppConfig,
    table_name: str,
    *,
    posted_month: str,
    pipeline_run_id: str,
) -> DataPath:
    """Gold fact path: analysis grain is **posted_month** (job posting month from Silver `posted_at`)."""
    return (
        gold_root_effective(cfg)
        / table_name
        / f"source={cfg.source_name}"
        / f"posted_month={posted_month}"
        / f"run_id={pipeline_run_id}"
        / "part-00001.parquet"
    )


def gold_latest_run_metadata_file(cfg: AppConfig) -> DataPath:
    """Single-row pointer Parquet for one pipeline source (Athena ``latest_run_metadata`` helpers).

    Always ``gold/source=<slug>/latest_run_metadata/part-00001.parquet``. Never a legacy top-level
    ``gold/latest_run_metadata/`` (no ``source=`` segment).

    Arbeitnow slice runs: ``gold/slice=<tag>/source=<slug>/latest_run_metadata/...`` (isolated from main).
    """
    slug = str(cfg.source_name).strip()
    if not slug:
        raise ValueError("AppConfig.source_name must be non-empty to write latest_run_metadata")
    source_seg = f"source={slug}"
    base = gold_root_effective(cfg)
    out = base / source_seg / "latest_run_metadata" / "part-00001.parquet"
    normalized = str(out).replace("\\", "/")
    if f"/{source_seg}/latest_run_metadata/" not in normalized:
        raise RuntimeError(f"refusing non-source-scoped latest_run_metadata path: {out}")
    return out
